package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// RFC6455Config configures the real WebSocket data-plane listener. It is
// deliberately separate from SSEConfig.
type RFC6455Config struct {
	Path           string
	OriginPatterns []string
	MaxConnections int
	MaxMessageSize int64
	ACL            *ACL
	InsecureDev    bool
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type RFC6455Server struct {
	broker  *Broker
	cfg     RFC6455Config
	mu      sync.Mutex
	conns   map[*websocket.Conn]struct{}
	pending int
}

func NewRFC6455Server(b *Broker, cfg RFC6455Config) *RFC6455Server {
	if cfg.Path == "" {
		cfg.Path = "/ws"
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1000
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 4 << 20
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	return &RFC6455Server{broker: b, cfg: cfg, conns: make(map[*websocket.Conn]struct{})}
}

type webSocketRequest struct {
	ID     string          `json:"id,omitempty"`
	Action string          `json:"action"`
	Body   json.RawMessage `json:"body,omitempty"`
}

type webSocketResponse struct {
	ID    string `json:"id,omitempty"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

func (s *RFC6455Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != s.cfg.Path {
			http.NotFound(w, r)
			return
		}
		principal, ok := AdminPrincipal{Name: "development", Permissions: []string{PermissionAdminAll}}, s.cfg.InsecureDev
		if !ok {
			if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
				principal, ok = s.cfg.ACL.AuthenticateCertificate(r.TLS.VerifiedChains[0][0])
			}
			if !ok {
				principal, ok = s.cfg.ACL.Authenticate(bearerToken(r))
			}
		}
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		release, err := s.cfg.ACL.Admit(principal)
		if err != nil {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		defer release()
		s.mu.Lock()
		if len(s.conns)+s.pending >= s.cfg.MaxConnections {
			s.mu.Unlock()
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}
		s.pending++
		s.mu.Unlock()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.cfg.OriginPatterns})
		s.mu.Lock()
		s.pending--
		if err != nil {
			s.mu.Unlock()
			return
		}
		conn.SetReadLimit(s.cfg.MaxMessageSize)
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			_ = conn.Close(websocket.StatusNormalClosure, "")
		}()
		s.serve(r.Context(), conn, principal)
	})
}

func (s *RFC6455Server) serve(parent context.Context, conn *websocket.Conn, principal AdminPrincipal) {
	for {
		readCtx, cancel := context.WithTimeout(parent, s.cfg.ReadTimeout)
		messageType, data, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return
		}
		if messageType != websocket.MessageText {
			s.write(conn, webSocketResponse{OK: false, Error: "binary WebSocket messages are not supported"})
			continue
		}
		var req webSocketRequest
		if err := json.Unmarshal(data, &req); err != nil {
			s.write(conn, webSocketResponse{OK: false, Error: "invalid JSON request"})
			continue
		}
		ctx := context.WithValue(parent, adminPrincipalKey{}, principal)
		resp := s.dispatch(ctx, principal, req)
		if err := s.write(conn, resp); err != nil {
			return
		}
	}
}

func (s *RFC6455Server) write(conn *websocket.Conn, response webSocketResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.WriteTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, data)
}

func (s *RFC6455Server) dispatch(ctx context.Context, principal AdminPrincipal, req webSocketRequest) webSocketResponse {
	permission := map[string]string{
		"publish":       PermissionPublish,
		"poll":          PermissionConsumerPoll,
		"ack":           PermissionMessageAck,
		"nack":          PermissionMessageNack,
		"read_stream":   PermissionStreamRead,
		"commit_offset": PermissionStreamCommit,
		"stats":         PermissionBrokerRead,
		"health":        PermissionBrokerRead,
	}[req.Action]
	if permission == "" {
		return webSocketResponse{ID: req.ID, OK: false, Error: "unknown action"}
	}
	if !s.cfg.InsecureDev && !s.cfg.ACL.Authorize(principal, permission) {
		return webSocketResponse{ID: req.ID, OK: false, Error: "forbidden"}
	}
	var data any
	var err error
	switch req.Action {
	case "publish":
		var in struct {
			Topic    string            `json:"topic"`
			Queue    string            `json:"queue,omitempty"`
			Stream   string            `json:"stream,omitempty"`
			TenantID string            `json:"tenant_id,omitempty"`
			Payload  json.RawMessage   `json:"payload"`
			Headers  map[string]string `json:"headers,omitempty"`
		}
		if err = json.Unmarshal(req.Body, &in); err == nil {
			data, err = s.broker.Publisher().Publish(ctx, in.Topic, in.Payload, WithQueue(in.Queue), WithStream(in.Stream), WithTenantID(in.TenantID), WithHeaders(in.Headers))
		}
	case "poll":
		var in LeaseRequest
		if err = json.Unmarshal(req.Body, &in); err == nil {
			in.TenantID, err = resolveTenant(ctx, in.TenantID)
			if err == nil {
				data, err = s.broker.Store().Lease(ctx, in)
			}
		}
	case "ack":
		var in AckRequest
		if err = json.Unmarshal(req.Body, &in); err == nil {
			err = s.broker.Store().Ack(ctx, in)
		}
	case "nack":
		var in NackRequest
		if err = json.Unmarshal(req.Body, &in); err == nil {
			err = s.broker.Store().Nack(ctx, in)
		}
	case "read_stream":
		var in StreamReadRequest
		if err = json.Unmarshal(req.Body, &in); err == nil {
			data, err = s.broker.ReadStream(ctx, in)
		}
	case "commit_offset":
		var in OffsetCommit
		if err = json.Unmarshal(req.Body, &in); err == nil {
			err = s.broker.CommitOffset(ctx, in)
		}
	case "stats":
		data, err = s.broker.Store().Stats(ctx)
	case "health":
		data = s.broker.Health()
	}
	if err != nil {
		return webSocketResponse{ID: req.ID, OK: false, Error: err.Error()}
	}
	return webSocketResponse{ID: req.ID, OK: true, Data: data}
}

func (s *RFC6455Server) Stop() {
	s.mu.Lock()
	connections := make([]*websocket.Conn, 0, len(s.conns))
	for conn := range s.conns {
		connections = append(connections, conn)
	}
	s.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
	}
}

func (s *RFC6455Server) ConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
