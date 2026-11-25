package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// WebSocketConfig configures the legacy SSE transport.
// Deprecated: use SSEConfig. This implementation is not RFC 6455 WebSocket.
type WebSocketConfig struct {
	// Path is the WebSocket endpoint path. Default: "/ws".
	Path string
	// MaxConnections limits concurrent connections. Default: 1000.
	MaxConnections int
	// HeartbeatInterval is the ping/pong interval. Default: 30s.
	HeartbeatInterval time.Duration
	ACL               *ACL
	InsecureDev       bool
}

// WebSocketServer provides the legacy SSE transport for the broker data plane.
// Deprecated: use SSEServer. This implementation is not RFC 6455 WebSocket.
type WebSocketServer struct {
	broker  *Broker
	cfg     WebSocketConfig
	clients map[string]*wsClient
	mu      sync.RWMutex
	wg      sync.WaitGroup
	token   string
}

type wsClient struct {
	id        string
	send      chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	principal AdminPrincipal
}

// NewWebSocketServer creates a new WebSocket server.
func NewWebSocketServer(b *Broker, cfg WebSocketConfig) *WebSocketServer {
	if cfg.Path == "" {
		cfg.Path = "/ws"
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1000
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	return &WebSocketServer{broker: b, cfg: cfg, clients: make(map[string]*wsClient)}
}

type SSEConfig = WebSocketConfig
type SSEServer = WebSocketServer

func NewSSEServer(b *Broker, cfg SSEConfig) *SSEServer {
	return NewWebSocketServer(b, cfg)
}

// SetToken sets the authentication token for WebSocket connections.
func (ws *WebSocketServer) SetToken(token string) {
	ws.token = token
}

// App returns the SSE transport application, serving cfg.Path.
func (ws *WebSocketServer) App() *fh.App {
	app := NewHTTPApp(HTTPServerOptions{ReadHeaderTimeout: 5 * time.Second}, fh.WithNotFoundHandler(plainNotFound), fh.WithMethodNotAllowedHandler(plainMethodNotAllowed))
	ws.Register(app)
	return app
}

// Register mounts the SSE stream on app at cfg.Path.
func (ws *WebSocketServer) Register(app *fh.App) {
	app.Get(ws.cfg.Path, ws.handleStream)
}

func (ws *WebSocketServer) authenticate(c fh.Ctx) (AdminPrincipal, bool) {
	if ws.cfg.InsecureDev {
		return AdminPrincipal{Name: "development", Permissions: []string{PermissionAdminAll}}, true
	}
	token := bearerToken(c)
	if ws.cfg.ACL != nil {
		if cert := verifiedPeerCertificate(c); cert != nil {
			if principal, ok := ws.cfg.ACL.AuthenticateCertificate(cert); ok {
				return principal, true
			}
		}
		if principal, ok := ws.cfg.ACL.Authenticate(token); ok {
			return principal, true
		}
	}
	if ws.token != "" && constantTimeEqual(token, ws.token) {
		return AdminPrincipal{Name: "legacy-token", Permissions: []string{PermissionNotification}}, true
	}
	return AdminPrincipal{}, false
}

func (ws *WebSocketServer) handleStream(c fh.Ctx) error {
	principal, ok := ws.authenticate(c)
	if !ok {
		return httpError(c, "unauthorized", fh.StatusUnauthorized)
	}
	if ws.cfg.ACL != nil && !ws.cfg.InsecureDev && !ws.cfg.ACL.Authorize(principal, PermissionNotification) {
		return httpError(c, "forbidden", fh.StatusForbidden)
	}
	release := func() {}
	if ws.cfg.ACL != nil {
		var err error
		release, err = ws.cfg.ACL.Admit(principal)
		if err != nil {
			return httpError(c, err.Error(), fh.StatusTooManyRequests)
		}
	}
	defer release()
	ws.mu.RLock()
	count := len(ws.clients)
	ws.mu.RUnlock()
	if count >= ws.cfg.MaxConnections {
		return httpError(c, "too many connections", fh.StatusServiceUnavailable)
	}

	clientID := fmt.Sprintf("ws-%d", time.Now().UnixNano())
	client := &wsClient{id: clientID, send: make(chan []byte, 256), closed: make(chan struct{}), principal: principal}
	ws.mu.Lock()
	ws.clients[clientID] = client
	ws.mu.Unlock()
	defer func() {
		ws.mu.Lock()
		delete(ws.clients, clientID)
		ws.mu.Unlock()
		client.closeOnce.Do(func() { close(client.closed) })
	}()

	ctx := c.Context()
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("X-Accel-Buffering", "no")
	return c.Stream(func(sw *fh.StreamWriter) error {
		if _, err := fmt.Fprintf(sw, "event: connected\ndata: {\"client_id\":\"%s\"}\n\n", clientID); err != nil {
			return err
		}
		if err := sw.Flush(); err != nil {
			return err
		}
		heartbeat := time.NewTicker(ws.cfg.HeartbeatInterval)
		defer heartbeat.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg, ok := <-client.send:
				if !ok {
					return nil
				}
				if _, err := sw.Write(msg); err != nil {
					return err
				}
			case <-heartbeat.C:
				if _, err := io.WriteString(sw, ": ping\n\n"); err != nil {
					return err
				}
			}
			if err := sw.Flush(); err != nil {
				return err
			}
		}
	})
}

type wsResponse struct {
	ID     string `json:"id"`
	Action string `json:"action,omitempty"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Data   any    `json:"data,omitempty"`
}

// Broadcast sends a message to all connected clients.
func (ws *WebSocketServer) Broadcast(msgType string, data any) {
	ws.mu.RLock()
	clients := make([]*wsClient, 0, len(ws.clients))
	for _, c := range ws.clients {
		clients = append(clients, c)
	}
	ws.mu.RUnlock()
	resp := wsResponse{Action: msgType, OK: true, Data: data}
	payload, _ := json.Marshal(resp)
	event := fmt.Sprintf("event: %s\ndata: %s\n\n", msgType, payload)
	for _, c := range clients {
		if !broadcastAllowed(c.principal, data) {
			continue
		}
		select {
		case c.send <- []byte(event):
		default:
		}
	}
}

func broadcastAllowed(principal AdminPrincipal, data any) bool {
	ctx := context.WithValue(context.Background(), adminPrincipalKey{}, principal)
	switch value := data.(type) {
	case Notification:
		return tenantAllowed(ctx, value.TenantID)
	case Message:
		return tenantAllowed(ctx, value.TenantID)
	case TaskInfo:
		return tenantAllowed(ctx, value.TenantID)
	case SagaInstance:
		return tenantAllowed(ctx, value.TenantID)
	default:
		return !hasRestrictedTenantScope(ctx)
	}
}

// ConnectionCount returns the number of active connections.
func (ws *WebSocketServer) ConnectionCount() int {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return len(ws.clients)
}

// Stop closes all connections.
func (ws *WebSocketServer) Stop() {
	ws.mu.Lock()
	for id, c := range ws.clients {
		close(c.send)
		c.closeOnce.Do(func() { close(c.closed) })
		delete(ws.clients, id)
	}
	ws.mu.Unlock()
	ws.wg.Wait()
}
