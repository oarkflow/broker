package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
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
}

type grpcRequest struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Body    json.RawMessage   `json:"body"`
	Headers map[string]string `json:"headers,omitempty"`
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
	gs.mu.Lock()
	for id, c := range gs.conns {
		c.conn.Close()
		close(c.closed)
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
		close(gc.closed)
	}()

	decoder := json.NewDecoder(conn)
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
			if err.Error() != "EOF" {
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
	ctx := context.Background()
	switch req.Method {
	case GRPCMethodPublish:
		return gs.handlePublish(ctx, req)
	case GRPCMethodAck:
		return gs.handleAck(ctx, req)
	case GRPCMethodNack:
		return gs.handleNack(ctx, req)
	case GRPCMethodPoll:
		return gs.handlePoll(ctx, req)
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
		Topic   string            `json:"topic"`
		Queue   string            `json:"queue,omitempty"`
		Payload json.RawMessage   `json:"payload"`
		Type    string            `json:"type,omitempty"`
		Headers map[string]string `json:"headers,omitempty"`
	}
	if err := json.Unmarshal(req.Body, &p); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	opts := []PublishOption{}
	if p.Queue != "" {
		opts = append(opts, WithQueue(p.Queue))
	}
	if p.Type != "" {
		opts = append(opts, WithType(p.Type))
	}
	if p.Headers != nil {
		opts = append(opts, WithHeaders(p.Headers))
	}
	res, err := gs.broker.Publisher().Publish(ctx, p.Topic, p.Payload, opts...)
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	body, _ := json.Marshal(res)
	return grpcResponse{ID: req.ID, Status: 200, Body: body}
}

func (gs *GRPCServer) handleAck(ctx context.Context, req grpcRequest) grpcResponse {
	var a struct {
		MessageID string `json:"message_id"`
		Queue     string `json:"queue"`
	}
	if err := json.Unmarshal(req.Body, &a); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	err := gs.broker.Store().Ack(ctx, AckRequest{MessageID: a.MessageID, Queue: a.Queue})
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handleNack(ctx context.Context, req grpcRequest) grpcResponse {
	var n struct {
		MessageID string `json:"message_id"`
		Queue     string `json:"queue"`
		Action    string `json:"action"`
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
	err := gs.broker.Store().Nack(ctx, NackRequest{MessageID: n.MessageID, Queue: n.Queue, Action: action})
	if err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 409}
	}
	return grpcResponse{ID: req.ID, Status: 200}
}

func (gs *GRPCServer) handlePoll(ctx context.Context, req grpcRequest) grpcResponse {
	var p struct {
		Queue    string `json:"queue"`
		Limit    int    `json:"limit"`
		Consumer string `json:"consumer"`
	}
	if err := json.Unmarshal(req.Body, &p); err != nil {
		return grpcResponse{ID: req.ID, Error: err.Error(), Status: 400}
	}
	if p.Limit <= 0 {
		p.Limit = 1
	}
	msgs, err := gs.broker.Store().Lease(ctx, LeaseRequest{Queue: p.Queue, ConsumerID: p.Consumer, Limit: p.Limit})
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
