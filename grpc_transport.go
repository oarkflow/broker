package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// GRPCConfig configures the legacy native JSON/TCP RPC server.
// Deprecated: use NativeRPCConfig. This transport is not gRPC.
// Uses a custom protocol over TCP for environments without gRPC dependencies.
type GRPCConfig struct {
	// ListenAddr is the TCP address. Default: ":50051".
	ListenAddr string
	// MaxConnections limits concurrent connections. Default: 1000.
	MaxConnections int
	// MaxMessageSize limits incoming message size. Default: 4MB.
	MaxMessageSize int
	// KeepAlive is the keep-alive interval. Default: 60s.
	KeepAlive time.Duration
	// TLSCertFile is the path to the TLS certificate file.
	TLSCertFile string
	// TLSKeyFile is the path to the TLS private key file.
	TLSKeyFile string
	// ClientCAFile is the path to the CA certificate for client verification (mTLS).
	ClientCAFile string
	// RequireClientCert requires client certificate verification (mTLS).
	RequireClientCert bool
	// BrokerToken is the bearer token for authentication.
	// If set, every request must include this token.
	BrokerToken string
}

// GRPCServer provides the legacy native JSON/TCP transport.
// Deprecated: use NativeRPCServer. This transport is not gRPC.
// Uses length-prefixed JSON frames over TCP.
type GRPCServer struct {
	broker *Broker
	cfg    GRPCConfig
	ln     net.Listener
	conns  map[string]*grpcConn
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type grpcConn struct {
	id     string
	conn   net.Conn
	closed chan struct{}
	// closeOnce guards closed, which more than one shutdown path can reach.
	closeOnce sync.Once
}

type grpcRequest struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Body    json.RawMessage   `json:"body"`
	Headers map[string]string `json:"headers,omitempty"`
	// Context is an optional client deadline as Unix nanoseconds, matching the
	// gRPC deadline convention. The server used to run every request on a
	// background context, so a client could not cancel work it had started.
	Context int64 `json:"context,omitempty"`
}

type grpcResponse struct {
	ID     string          `json:"id"`
	Error  string          `json:"error,omitempty"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
}

const (
	GRPCMethodPublish       = "broker.Publish"
	GRPCMethodSubscribe     = "broker.Subscribe"
	GRPCMethodAck           = "broker.Ack"
	GRPCMethodNack          = "broker.Nack"
	GRPCMethodPoll          = "broker.Poll"
	GRPCMethodRequest       = "broker.Request"
	GRPCMethodStats         = "broker.Stats"
	GRPCMethodHealth        = "broker.Health"
	GRPCMethodDeclareQueue  = "broker.DeclareQueue"
	GRPCMethodDeclareTopic  = "broker.DeclareTopic"
	GRPCMethodDeclareStream = "broker.DeclareStream"
	GRPCMethodReadStream    = "broker.ReadStream"
	GRPCMethodCommitOffset  = "broker.CommitOffset"
	GRPCMethodMove          = "broker.Move"
	GRPCMethodPauseQueue    = "broker.PauseQueue"
	GRPCMethodResumeQueue   = "broker.ResumeQueue"
	GRPCMethodPurgeQueue    = "broker.PurgeQueue"
)

// NewGRPCServer creates the legacy native JSON/TCP server.
func NewGRPCServer(b *Broker, cfg GRPCConfig) *GRPCServer {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":50051"
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1000
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 4 << 20
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 60 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &GRPCServer{
		broker: b,
		cfg:    cfg,
		conns:  make(map[string]*grpcConn),
		ctx:    ctx,
		cancel: cancel,
	}
}

// NativeRPCConfig accurately names the legacy JSON/TCP RPC configuration.
type NativeRPCConfig = GRPCConfig

// NativeRPCServer accurately names the legacy JSON/TCP RPC server.
type NativeRPCServer = GRPCServer

func NewNativeRPCServer(b *Broker, cfg NativeRPCConfig) *NativeRPCServer {
	return NewGRPCServer(b, cfg)
}

// ListenAndServe starts the legacy native JSON/TCP server.
func (gs *GRPCServer) ListenAndServe() error {
	var err error
	// Set up TLS if configured
	if gs.cfg.TLSCertFile != "" && gs.cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(gs.cfg.TLSCertFile, gs.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("grpc: load TLS keypair: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		if gs.cfg.ClientCAFile != "" {
			pem, err := os.ReadFile(gs.cfg.ClientCAFile)
			if err != nil {
				return fmt.Errorf("grpc: load client CA: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("grpc: no valid certs in client CA")
			}
			tlsCfg.ClientCAs = pool
		}
		if gs.cfg.RequireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		ln, err := tls.Listen("tcp", gs.cfg.ListenAddr, tlsCfg)
		if err != nil {
			return err
		}
		gs.ln = ln
		log.Printf("[grpc] listening on %s (TLS)", gs.cfg.ListenAddr)
	} else {
		gs.ln, err = net.Listen("tcp", gs.cfg.ListenAddr)
		if err != nil {
			return err
		}
		log.Printf("[grpc] listening on %s", gs.cfg.ListenAddr)
	}
	for {
		conn, err := gs.ln.Accept()
		if err != nil {
			select {
			case <-gs.ctx.Done():
				return nil
			default:
				log.Printf("[grpc] accept error: %v", err)
				continue
			}
		}
		gs.wg.Add(1)
		go gs.handleConn(conn)
	}
}

// Stop shuts down the server.
func (gs *GRPCServer) Stop() {
	gs.cancel()
	if gs.ln != nil {
		gs.ln.Close()
	}
	// Closing the socket is enough to unblock the connection goroutine, which then
	// removes itself and closes its own closed channel. Stop used to close that
	// channel too, so shutting down with any client connected panicked with
	// "close of closed channel" from the goroutine's deferred cleanup.
	gs.mu.Lock()
	for id, c := range gs.conns {
		c.conn.Close()
		delete(gs.conns, id)
	}
	gs.mu.Unlock()
	gs.wg.Wait()
}

func (gs *GRPCServer) handleConn(conn net.Conn) {
	defer gs.wg.Done()
	id := fmt.Sprintf("grpc-%d", time.Now().UnixNano())
	gc := &grpcConn{id: id, conn: conn, closed: make(chan struct{})}
	gs.mu.Lock()
	if len(gs.conns) >= gs.cfg.MaxConnections {
		gs.mu.Unlock()
		conn.Close()
		return
	}
	gs.conns[id] = gc
	gs.mu.Unlock()
	defer func() {
		gs.mu.Lock()
		delete(gs.conns, id)
		gs.mu.Unlock()
		conn.Close()
		gc.closeOnce.Do(func() { close(gc.closed) })
	}()

	// Bound the bytes a single connection can make the broker buffer. The
	// decoder previously read straight from the socket, so one client could
	// stream an unbounded frame and exhaust memory. HTTP bounds this with
	// MaxBytesReader and gRPC with MaxRecvMsgSize; this transport did neither.
	decoder := json.NewDecoder(&maxBytesReader{conn: conn, limit: gs.cfg.MaxMessageSize})
	encoder := json.NewEncoder(conn)
	for {
		select {
		case <-gs.ctx.Done():
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(gs.cfg.KeepAlive * 2))
		var req grpcRequest
		if err := decoder.Decode(&req); err != nil {
			// A closed socket is the normal shutdown path, not a protocol error.
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && err.Error() != "EOF" {
				log.Printf("[grpc] decode error: %v", err)
			}
			return
		}
		resp := gs.handleMethod(req)
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := encoder.Encode(resp); err != nil {
			return
		}
	}
}

func (gs *GRPCServer) handleMethod(req grpcRequest) grpcResponse {
	// Check authentication if token is configured
	if gs.cfg.BrokerToken != "" {
		if req.Headers == nil {
			return grpcResponse{ID: req.ID, Error: "unauthorized: missing auth token", Status: 401}
		}
		token := req.Headers["Authorization"]
		if token == "" {
			token = req.Headers["authorization"]
		}
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
		if token != gs.cfg.BrokerToken {
			return grpcResponse{ID: req.ID, Error: "unauthorized: invalid token", Status: 401}
		}
	}
	// A background context ignored broker shutdown and client disconnect
	// entirely, so an in-flight request could not be cancelled. It also meant the
	// tenant resolved for the request never reached the store.
	ctx := gs.requestContext(req)
	switch req.Method {
	case GRPCMethodPublish:
		return gs.handlePublish(ctx, req)
	case GRPCMethodAck:
		return gs.handleAck(ctx, req)
	case GRPCMethodNack:
		return gs.handleNack(ctx, req)
	case GRPCMethodPoll:
		return gs.handlePoll(ctx, req)
	case GRPCMethodSubscribe:
		return gs.handleSubscribe(ctx, req)
	case GRPCMethodRequest:
		return gs.handleRequest(ctx, req)
	case GRPCMethodStats:
		return gs.handleStats(ctx, req)
	case GRPCMethodHealth:
		return gs.handleHealth(ctx, req)
	case GRPCMethodDeclareQueue:
		return gs.handleDeclareQueue(ctx, req)
	case GRPCMethodDeclareTopic:
		return gs.handleDeclareTopic(ctx, req)
	case GRPCMethodDeclareStream:
		return gs.handleDeclareStream(ctx, req)
	case GRPCMethodReadStream:
		return gs.handleReadStream(ctx, req)
	case GRPCMethodCommitOffset:
		return gs.handleCommitOffset(ctx, req)
	case GRPCMethodMove:
		return gs.handleMove(ctx, req)
	case GRPCMethodPauseQueue:
		return gs.handlePauseQueue(ctx, req)
	case GRPCMethodResumeQueue:
		return gs.handleResumeQueue(ctx, req)
	case GRPCMethodPurgeQueue:
		return gs.handlePurgeQueue(ctx, req)
	default:
		return grpcResponse{ID: req.ID, Error: "unknown method: " + req.Method, Status: 404}
	}
}

func (gs *GRPCServer) handlePublish(ctx context.Context, req grpcRequest) grpcResponse {
	var p struct {
		Topic           string            `json:"topic"`
		Queue           string            `json:"queue,omitempty"`
		Stream          string            `json:"stream,omitempty"`
		Payload         json.RawMessage   `json:"payload"`
		Type            string            `json:"type,omitempty"`
		ContentType     string            `json:"content_type,omitempty"`
		Headers         map[string]string `json:"headers,omitempty"`
		Metadata        map[string]string `json:"metadata,omitempty"`
		Key             string            `json:"key,omitempty"`
		RoutingKey      string            `json:"routing_key,omitempty"`
		TenantID        string            `json:"tenant_id,omitempty"`
		CorrelationID   string            `json:"correlation_id,omitempty"`
		CausationID     string            `json:"causation_id,omitempty"`
		TraceID         string            `json:"trace_id,omitempty"`
		IdempotencyKey  string            `json:"idempotency_key,omitempty"`
		MessageID       string            `json:"message_id,omitempty"`
		Priority        int               `json:"priority,omitempty"`
		MaxAttempts     int               `json:"max_attempts,omitempty"`
		Schema          string            `json:"schema,omitempty"`
		SchemaVersion   string            `json:"schema_version,omitempty"`
		PayloadEncoding string            `json:"payload_encoding,omitempty"`
		TTLNS           int64             `json:"ttl_ns,omitempty"`
	}
	if err := json.Unmarshal(req.Body, &p); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	payload, perr := decodeNativePayload(p.Payload, p.PayloadEncoding)
	if perr != nil {
		return grpcResponse{ID: req.ID, Error: perr.Error(), Status: 400}
	}
	opts := []PublishOption{}
	for _, o := range []PublishOption{
		WithQueue(p.Queue), WithStream(p.Stream), WithType(p.Type),
		WithContentType(p.ContentType), WithHeaders(p.Headers), WithMetadata(p.Metadata),
		WithKey(p.Key), WithRoutingKey(p.RoutingKey), WithTenantID(p.TenantID),
		WithCorrelationID(p.CorrelationID), WithCausationID(p.CausationID),
		WithTraceID(p.TraceID), WithIdempotencyKey(p.IdempotencyKey),
		WithMessageID(p.MessageID), WithPriority(p.Priority), WithMaxAttempts(p.MaxAttempts),
	} {
		opts = append(opts, o)
	}
	if p.Schema != "" || p.SchemaVersion != "" {
		opts = append(opts, WithSchema(p.Schema, p.SchemaVersion))
	}
	if p.TTLNS > 0 {
		opts = append(opts, WithTTL(time.Duration(p.TTLNS)))
	}
	res, err := gs.broker.Publisher().Publish(ctx, p.Topic, payload, opts...)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	body, _ := json.Marshal(res)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleAck(ctx context.Context, req grpcRequest) grpcResponse {
	var a struct {
		MessageID  string `json:"message_id"`
		Queue      string `json:"queue"`
		ConsumerID string `json:"consumer_id,omitempty"`
		LeaseToken string `json:"lease_token,omitempty"`
	}
	if err := json.Unmarshal(req.Body, &a); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	// The lease token is mandatory for a leased message: ackLocked rejects a
	// missing or mismatched token with ErrLeaseLost. Omitting it made every ack
	// over this transport fail.
	err := gs.broker.Store().Ack(ctx, AckRequest{
		MessageID:  a.MessageID,
		Queue:      a.Queue,
		ConsumerID: a.ConsumerID,
		LeaseToken: a.LeaseToken,
	})
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleNack(ctx context.Context, req grpcRequest) grpcResponse {
	var n struct {
		MessageID  string `json:"message_id"`
		Queue      string `json:"queue"`
		Action     string `json:"action"`
		ConsumerID string `json:"consumer_id,omitempty"`
		LeaseToken string `json:"lease_token,omitempty"`
	}
	if err := json.Unmarshal(req.Body, &n); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	action := NackRequeue
	switch n.Action {
	case "retry":
		action = NackRetry
	case "dlq":
		action = NackDLQ
	case "reject":
		action = NackReject
	}
	// IncrementAttempt must be set. Without it Attempts stayed at zero, the
	// store's retry ceiling never tripped, and a failing message was redelivered
	// forever. Every other transport (embedded, HTTP, gRPC) sets it.
	err := gs.broker.Store().Nack(ctx, NackRequest{
		MessageID:        n.MessageID,
		Queue:            n.Queue,
		Action:           action,
		ConsumerID:       n.ConsumerID,
		LeaseToken:       n.LeaseToken,
		IncrementAttempt: true,
	})
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handlePoll(ctx context.Context, req grpcRequest) grpcResponse {
	var p struct {
		Queue               string `json:"queue"`
		Limit               int    `json:"limit"`
		Consumer            string `json:"consumer"`
		ConsumerGroup       string `json:"consumer_group,omitempty"`
		VisibilityTimeoutNS int64  `json:"visibility_timeout_ns,omitempty"`
		TenantID            string `json:"tenant_id,omitempty"`
	}
	if err := json.Unmarshal(req.Body, &p); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	if p.Limit <= 0 {
		p.Limit = 1
	}
	// ConsumerGroup, VisibilityTimeout and TenantID were all missing here. Without
	// a group every consumer in a competing set leased from the same implicit
	// group, the default 30s visibility timeout ignored the caller's choice, and
	// TenantID never reached the store so tenant scoping could not apply.
	leaseReq := LeaseRequest{
		Queue:             p.Queue,
		ConsumerID:        p.Consumer,
		ConsumerGroup:     firstNonEmpty(p.ConsumerGroup, p.Consumer),
		Limit:             p.Limit,
		VisibilityTimeout: time.Duration(p.VisibilityTimeoutNS),
		TenantID:          p.TenantID,
	}
	msgs, err := gs.broker.Store().Lease(ctx, leaseReq)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	body, _ := json.Marshal(msgs)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleRequest(ctx context.Context, req grpcRequest) grpcResponse {
	var r struct {
		Subject string          `json:"subject"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(req.Body, &r); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	var out json.RawMessage
	err := gs.broker.Publisher().Request(ctx, r.Subject, r.Payload, &out, WithTimeout(30*time.Second))
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200, Body: out}
}

func (gs *GRPCServer) handleStats(ctx context.Context, req grpcRequest) grpcResponse {
	stats, err := gs.broker.Store().Stats(ctx)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 500}
	}
	body, _ := json.Marshal(stats)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleHealth(ctx context.Context, req grpcRequest) grpcResponse {
	health := gs.broker.Health()
	body, _ := json.Marshal(health)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleDeclareQueue(ctx context.Context, req grpcRequest) grpcResponse {
	var cfg QueueConfig
	if err := json.Unmarshal(req.Body, &cfg); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.DeclareQueue(ctx, cfg)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleDeclareTopic(ctx context.Context, req grpcRequest) grpcResponse {
	var cfg TopicConfig
	if err := json.Unmarshal(req.Body, &cfg); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.DeclareTopic(ctx, cfg)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleDeclareStream(ctx context.Context, req grpcRequest) grpcResponse {
	var cfg StreamConfig
	if err := json.Unmarshal(req.Body, &cfg); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.DeclareStream(ctx, cfg)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleReadStream(ctx context.Context, req grpcRequest) grpcResponse {
	var r StreamReadRequest
	if err := json.Unmarshal(req.Body, &r); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	msgs, err := gs.broker.ReadStream(ctx, r)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 404}
	}
	body, _ := json.Marshal(msgs)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleCommitOffset(ctx context.Context, req grpcRequest) grpcResponse {
	var c OffsetCommit
	if err := json.Unmarshal(req.Body, &c); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.CommitOffset(ctx, c)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleMove(ctx context.Context, req grpcRequest) grpcResponse {
	var m MoveRequest
	if err := json.Unmarshal(req.Body, &m); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	res, err := gs.broker.Store().Move(ctx, m)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	body, _ := json.Marshal(res)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handlePauseQueue(ctx context.Context, req grpcRequest) grpcResponse {
	var q struct{ Name string }
	if err := json.Unmarshal(req.Body, &q); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.PauseQueue(ctx, q.Name)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleResumeQueue(ctx context.Context, req grpcRequest) grpcResponse {
	var q struct{ Name string }
	if err := json.Unmarshal(req.Body, &q); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.ResumeQueue(ctx, q.Name)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handlePurgeQueue(ctx context.Context, req grpcRequest) grpcResponse {
	var q struct{ Name string }
	if err := json.Unmarshal(req.Body, &q); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	n, err := gs.broker.PurgeQueue(ctx, q.Name)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	body, _ := json.Marshal(map[string]int{"purged": n})
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

// maxBytesReader fails a read once more than limit bytes have been consumed on a
// connection. GRPCConfig documents MaxMessageSize as limiting incoming message
// size, but nothing enforced it: the JSON decoder read straight from the socket,
// so a single client could make the broker buffer an unbounded frame.
type maxBytesReader struct {
	conn  net.Conn
	limit int
	read  int
}

var errNativeFrameTooLarge = errors.New("broker: native rpc frame exceeds max message size")

func (r *maxBytesReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, errNativeFrameTooLarge
	}
	if remaining := r.limit - r.read; len(p) > remaining+1 {
		// Read one byte past the limit so an oversized frame is detected rather
		// than silently truncated into a confusing decode error.
		p = p[:remaining+1]
	}
	n, err := r.conn.Read(p)
	r.read += n
	if r.read > r.limit {
		return n, errNativeFrameTooLarge
	}
	return n, err
}

// requestContext derives the context for one native RPC request. It is rooted in
// the server context so shutdown and keep-alive deadlines cancel in-flight work,
// and it carries the request deadline when the client sent one.
//
// This transport authenticates with a single static bearer token and has no ACL,
// so there is no per-request principal to attach. Tenant scoping therefore has
// to come from the request body, which the handlers now pass through. Adding a
// real per-principal identity here would need an ACL on GRPCConfig.
func (gs *GRPCServer) requestContext(req grpcRequest) context.Context {
	ctx := gs.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if req.Context != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, req.Context))
		// The handler runs synchronously and returns before the response is
		// written, so releasing here bounds the cancel to the request.
		defer cancel()
	}
	return ctx
}

// decodeNativePayload resolves a publish payload, honouring an explicit
// base64 encoding so binary payloads survive the JSON transport.
func decodeNativePayload(payload json.RawMessage, encoding string) ([]byte, error) {
	if encoding == "" || encoding == "json" {
		return []byte(payload), nil
	}
	var wire PublishHTTPPayload
	wire.Payload = payload
	wire.PayloadEncoding = encoding
	return wire.PayloadBytes()
}

// handleSubscribe registers a remote consumer subscription. GRPCMethodSubscribe
// was exported as a constant but never routed, so any client using it got a
// permanent 404 from a server that advertised the method.
func (gs *GRPCServer) handleSubscribe(ctx context.Context, req grpcRequest) grpcResponse {
	var body struct {
		ConsumerID string            `json:"consumer_id"`
		Group      string            `json:"group,omitempty"`
		Queue      string            `json:"queue,omitempty"`
		Pattern    string            `json:"pattern,omitempty"`
		Name       string            `json:"name,omitempty"`
		TenantID   string            `json:"tenant_id,omitempty"`
		AutoCreate *bool             `json:"auto_create,omitempty"`
		Visibility time.Duration     `json:"visibility_timeout_ns,omitempty"`
		Headers    map[string]string `json:"-"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	if body.ConsumerID == "" {
		return grpcResponse{ID: req.ID, Error: "consumer_id required", Status: 400}
	}
	if body.TenantID != "" {
		if tenant, terr := resolveTenant(ctx, body.TenantID); terr == nil {
			body.TenantID = tenant
		} else {
			return grpcResponse{ID: req.ID, Error: terr.Error(), Status: 403}
		}
	}
	sub := RemoteSubscription{
		Queue:   body.Queue,
		Pattern: body.Pattern,
		Name:    body.Name,
	}
	if sub.Name == "" {
		sub.Name = firstNonEmpty(firstNonEmpty(body.Pattern, body.Queue), body.ConsumerID)
	}
	if body.AutoCreate != nil {
		sub.AutoCreate = *body.AutoCreate
	}
	info, err := gs.broker.RemoteConsumerSubscribe(ctx, body.ConsumerID, sub)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: grpcStatusFor(err)}
	}
	out, _ := json.Marshal(info)
	return grpcResponse{ID: req.ID, Status: 200, Body: out}
}

// grpcStatusFor maps a broker error onto a status code, so a native RPC client
// can distinguish an authorization failure from a conflict. Everything else keeps
// the historical 409.
func grpcStatusFor(err error) int {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return 403
	case errors.Is(err, ErrInvalidMessage):
		return 400
	case errors.Is(err, ErrQueueNotFound), errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrStreamNotFound), errors.Is(err, ErrMessageNotFound):
		return 404
	case errors.Is(err, ErrPaused), errors.Is(err, ErrStopped), errors.Is(err, ErrClosed):
		return 409
	default:
		return 409
	}
}
