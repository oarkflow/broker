package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
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

// Handler returns an HTTP handler for WebSocket connections.
func (ws *WebSocketServer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ws.cfg.Path {
			http.NotFound(w, r)
			return
		}
		ws.handleUpgrade(w, r)
	})
}

func (ws *WebSocketServer) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	// Check authentication if token is configured
	if ws.token != "" {
		token := r.Header.Get("Authorization")
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
		if token != ws.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	ws.mu.RLock()
	count := len(ws.clients)
	ws.mu.RUnlock()
	if count >= ws.cfg.MaxConnections {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	clientID := fmt.Sprintf("ws-%d", time.Now().UnixNano())
	client := &wsClient{id: clientID, send: make(chan []byte, 256), closed: make(chan struct{})}
	ws.mu.Lock()
	ws.clients[clientID] = client
	ws.mu.Unlock()

	ctx := r.Context()
	defer func() {
		ws.mu.Lock()
		delete(ws.clients, clientID)
		ws.mu.Unlock()
		client.closeOnce.Do(func() { close(client.closed) })
	}()

	// Send client ID
	fmt.Fprintf(w, "event: connected\ndata: {\"client_id\":\"%s\"}\n\n", clientID)
	flusher.Flush()

	// Register with broker for consuming
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-client.send:
			if !ok {
				return
			}
			_, _ = w.Write(msg)
			flusher.Flush()
		}
	}
}

func (ws *WebSocketServer) handleHTTPRequest(w http.ResponseWriter, r *http.Request, clientID string) {
	action := r.URL.Query().Get("action")
	msg := wsMessage{ID: r.URL.Query().Get("id"), Action: action}
	switch action {
	case "publish":
		msg.Topic = r.URL.Query().Get("topic")
		msg.Queue = r.URL.Query().Get("queue")
		if r.Body != nil {
			msg.Payload, _ = json.Marshal(map[string]any{})
		}
	case "subscribe":
		msg.Queue = r.URL.Query().Get("queue")
	case "poll":
		msg.Queue = r.URL.Query().Get("queue")
	case "stats":
		msg.Action = "stats"
	case "health":
		msg.Action = "health"
	}
	ws.handleAction(w, msg, clientID)
}

func (ws *WebSocketServer) handleAction(w http.ResponseWriter, msg wsMessage, clientID string) {
	ctx := context.Background()
	switch msg.Action {
	case "publish":
		opts := []PublishOption{}
		if msg.Queue != "" {
			opts = append(opts, WithQueue(msg.Queue))
		}
		if msg.Type != "" {
			opts = append(opts, WithType(msg.Type))
		}
		res, err := ws.broker.Publisher().Publish(ctx, msg.Topic, msg.Payload, opts...)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"%s","ok":false,"error":"%s"}`, msg.ID, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(wsResponse{ID: msg.ID, OK: true, Data: res})
	case "stats":
		stats, _ := ws.broker.Store().Stats(ctx)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(wsResponse{ID: msg.ID, OK: true, Data: stats})
	case "health":
		health := ws.broker.Health()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(wsResponse{ID: msg.ID, OK: true, Data: health})
	}
}

type wsMessage struct {
	ID      string            `json:"id"`
	Action  string            `json:"action"`
	Topic   string            `json:"topic,omitempty"`
	Queue   string            `json:"queue,omitempty"`
	Payload json.RawMessage   `json:"payload,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Type    string            `json:"type,omitempty"`
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
		select {
		case c.send <- []byte(event):
		default:
		}
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

func init() {
	_ = log.Printf
}
