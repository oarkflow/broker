package broker

import (
	"context"
	"encoding/json"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/websocket"
)

// RFC6455Config configures the real WebSocket data-plane listener. It is
// deliberately separate from SSEConfig.
type RFC6455Config struct {
	Path string
	// OriginPatterns lists host patterns (path.Match syntax, e.g.
	// "*.example.com") whose browser Origin may connect. Same-host origins and
	// requests without an Origin header are always accepted.
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
	actions map[string]webSocketAction
	mu      sync.Mutex
	conns   map[*websocket.Conn]struct{}
	pending int
}

// webSocketAction is one routable WebSocket request action.
type webSocketAction struct {
	permission string
	handle     func(ctx context.Context, body json.RawMessage) (any, error)
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
	s := &RFC6455Server{broker: b, cfg: cfg, conns: make(map[*websocket.Conn]struct{})}
	s.actions = s.actionRoutes()
	return s
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

// App returns the WebSocket application, serving cfg.Path.
func (s *RFC6455Server) App() *fh.App {
	app := NewHTTPApp(HTTPServerOptions{ReadHeaderTimeout: 5 * time.Second}, fh.WithNotFoundHandler(plainNotFound), fh.WithMethodNotAllowedHandler(plainMethodNotAllowed))
	s.Register(app)
	return app
}

// Register mounts the WebSocket endpoint on app at cfg.Path. Authentication
// and connection admission run as middleware before the upgrade.
func (s *RFC6455Server) Register(app *fh.App) {
	app.Get(s.cfg.Path, s.admit, s.upgrade())
}

const (
	webSocketPrincipalLocal = "broker.ws.principal"
	webSocketPendingLocal   = "broker.ws.pending"
)

// admit authenticates the upgrade request and reserves a connection slot.
func (s *RFC6455Server) admit(c fh.Ctx) error {
	if !s.cfg.InsecureDev && s.cfg.ACL == nil {
		return httpError(c, "authentication is not configured", fh.StatusServiceUnavailable)
	}
	principal, ok := AdminPrincipal{Name: "development", Permissions: []string{PermissionAdminAll}}, s.cfg.InsecureDev
	if !ok {
		principal, ok = authenticateAdminCredential(s.cfg.ACL, bearerToken(c), verifiedPeerCertificate(c))
	}
	if !ok {
		return httpError(c, "unauthorized", fh.StatusUnauthorized)
	}
	release := func() {}
	if s.cfg.ACL != nil {
		var err error
		release, err = s.cfg.ACL.AdmitConnection(principal)
		if err != nil {
			return httpError(c, err.Error(), fh.StatusTooManyRequests)
		}
	}
	defer release()
	s.mu.Lock()
	if len(s.conns)+s.pending >= s.cfg.MaxConnections {
		s.mu.Unlock()
		return httpError(c, "too many connections", fh.StatusServiceUnavailable)
	}
	s.pending++
	s.mu.Unlock()
	// The upgrade runs synchronously inside Next, so the admission release and
	// the pending slot cover the whole connection. track() converts the
	// pending slot into a live connection once the handshake succeeds.
	tracked := false
	c.Locals(webSocketPendingLocal, &tracked)
	defer func() {
		if !tracked {
			s.mu.Lock()
			s.pending--
			s.mu.Unlock()
		}
	}()
	c.Locals(webSocketPrincipalLocal, principal)
	return c.Next()
}

func (s *RFC6455Server) upgrade() fh.HandlerFunc {
	cfg := websocket.DefaultConfig()
	cfg.MaxMessageSize = int(s.cfg.MaxMessageSize)
	cfg.MaxFrameSize = int(s.cfg.MaxMessageSize)
	cfg.MaxFragments = 1 << 10
	cfg.ReadTimeout = s.cfg.ReadTimeout
	cfg.WriteTimeout = s.cfg.WriteTimeout
	// Request throughput is governed by the ACL's per-principal rate limit.
	cfg.MaxMessagesPerSecond = 1 << 20
	cfg.EnableHeartbeat = false
	cfg.CheckOrigin = s.checkOrigin
	return func(c fh.Ctx) error {
		principal, _ := c.Locals(webSocketPrincipalLocal).(AdminPrincipal)
		tracked, _ := c.Locals(webSocketPendingLocal).(*bool)
		// The upgrade outlives the request Ctx, so capture its context now.
		parent := context.WithoutCancel(c.Context())
		return websocket.NewWithConfig(cfg, func(conn *websocket.Conn) error {
			s.track(conn, tracked)
			defer s.untrack(conn)
			s.serve(parent, conn, principal)
			return nil
		})(c)
	}
}

// checkOrigin accepts non-browser clients, same-host origins and origins whose
// host matches one of cfg.OriginPatterns.
func (s *RFC6455Server) checkOrigin(c fh.Ctx) bool {
	origin := c.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, c.Hostname()) || strings.EqualFold(u.Host, c.Get("Host")) {
		return true
	}
	for _, pattern := range s.cfg.OriginPatterns {
		if ok, _ := path.Match(strings.ToLower(pattern), strings.ToLower(u.Host)); ok {
			return true
		}
	}
	return false
}

func (s *RFC6455Server) track(conn *websocket.Conn, pending *bool) {
	s.mu.Lock()
	if pending != nil && !*pending {
		*pending = true
		s.pending--
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *RFC6455Server) untrack(conn *websocket.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	_ = conn.CloseWithStatus(websocket.CloseNormalClosure, "")
}

func (s *RFC6455Server) serve(parent context.Context, conn *websocket.Conn, principal AdminPrincipal) {
	ctx := context.WithValue(parent, adminPrincipalKey{}, principal)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		opcode, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if opcode != websocket.Text {
			if s.write(conn, webSocketResponse{OK: false, Error: "binary WebSocket messages are not supported"}) != nil {
				return
			}
			continue
		}
		var req webSocketRequest
		if err := json.Unmarshal(data, &req); err != nil {
			if s.write(conn, webSocketResponse{OK: false, Error: "invalid JSON request"}) != nil {
				return
			}
			continue
		}
		if s.cfg.ACL != nil {
			if err := s.cfg.ACL.AdmitRate(principal); err != nil {
				if s.write(conn, webSocketResponse{ID: req.ID, OK: false, Error: err.Error()}) != nil {
					return
				}
				continue
			}
		}
		if err := s.write(conn, s.dispatch(ctx, principal, req)); err != nil {
			return
		}
	}
}

func (s *RFC6455Server) write(conn *websocket.Conn, response webSocketResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	return conn.WriteMessage(websocket.Text, data)
}

// actionRoutes is the WebSocket action router: each action declares the
// permission it requires and decodes its own body.
func (s *RFC6455Server) actionRoutes() map[string]webSocketAction {
	b := s.broker
	return map[string]webSocketAction{
		"publish": {PermissionPublish, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in struct {
				Topic    string            `json:"topic"`
				Queue    string            `json:"queue,omitempty"`
				Stream   string            `json:"stream,omitempty"`
				TenantID string            `json:"tenant_id,omitempty"`
				Payload  json.RawMessage   `json:"payload"`
				Headers  map[string]string `json:"headers,omitempty"`
			}
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			return b.Publisher().Publish(ctx, in.Topic, in.Payload, WithQueue(in.Queue), WithStream(in.Stream), WithTenantID(in.TenantID), WithHeaders(in.Headers))
		}},
		"poll": {PermissionConsumerPoll, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in LeaseRequest
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			tenant, err := resolveTenant(ctx, in.TenantID)
			if err != nil {
				return nil, err
			}
			in.TenantID = tenant
			return b.Store().Lease(ctx, in)
		}},
		"ack": {PermissionMessageAck, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in AckRequest
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			return nil, b.Store().Ack(ctx, in)
		}},
		"nack": {PermissionMessageNack, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in NackRequest
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			return nil, b.Store().Nack(ctx, in)
		}},
		"read_stream": {PermissionStreamRead, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in StreamReadRequest
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			return b.ReadStream(ctx, in)
		}},
		"commit_offset": {PermissionStreamCommit, func(ctx context.Context, body json.RawMessage) (any, error) {
			var in OffsetCommit
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}
			return nil, b.CommitOffset(ctx, in)
		}},
		"stats": {PermissionBrokerRead, func(ctx context.Context, _ json.RawMessage) (any, error) {
			return b.Store().Stats(ctx)
		}},
		"health": {PermissionBrokerRead, func(context.Context, json.RawMessage) (any, error) {
			return b.Health(), nil
		}},
	}
}

func (s *RFC6455Server) dispatch(ctx context.Context, principal AdminPrincipal, req webSocketRequest) webSocketResponse {
	action, ok := s.actions[req.Action]
	if !ok {
		return webSocketResponse{ID: req.ID, OK: false, Error: "unknown action"}
	}
	if !s.cfg.InsecureDev && !s.cfg.ACL.Authorize(principal, action.permission) {
		return webSocketResponse{ID: req.ID, OK: false, Error: "forbidden"}
	}
	data, err := action.handle(ctx, req.Body)
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
		_ = conn.CloseWithStatus(websocket.CloseGoingAway, "server shutting down")
	}
}

func (s *RFC6455Server) ConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
