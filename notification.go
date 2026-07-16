package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

type Severity string

const (
	SeverityDebug Severity = "debug"
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
)

type Notification struct {
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	Source        string         `json:"source"`
	Severity      Severity       `json:"severity"`
	MessageID     string         `json:"message_id,omitempty"`
	TaskID        string         `json:"task_id,omitempty"`
	Topic         string         `json:"topic,omitempty"`
	Queue         string         `json:"queue,omitempty"`
	Stream        string         `json:"stream,omitempty"`
	Consumer      string         `json:"consumer,omitempty"`
	ConsumerGroup string         `json:"consumer_group,omitempty"`
	TenantID      string         `json:"tenant_id,omitempty"`
	TraceID       string         `json:"trace_id,omitempty"`
	Timestamp     time.Time      `json:"timestamp"`
	Data          map[string]any `json:"data,omitempty"`
}

type Callback interface {
	HandleNotification(context.Context, Notification) error
}
type CallbackFunc func(context.Context, Notification) error

func (f CallbackFunc) HandleNotification(ctx context.Context, n Notification) error { return f(ctx, n) }

type NotificationBus struct {
	mu      sync.RWMutex
	subs    map[string]Callback
	history []Notification
	cap     int
	closed  bool
	workers chan struct{}
}

const notificationWorkerPoolSize = 64

func NewNotificationBus(capacity int) *NotificationBus {
	if capacity <= 0 {
		capacity = 1024
	}
	return &NotificationBus{subs: map[string]Callback{}, cap: capacity, workers: make(chan struct{}, notificationWorkerPoolSize)}
}
func (b *NotificationBus) Subscribe(id string, cb Callback) {
	if id == "" || cb == nil {
		return
	}
	b.mu.Lock()
	b.subs[id] = cb
	b.mu.Unlock()
}
func (b *NotificationBus) Unsubscribe(id string) { b.mu.Lock(); delete(b.subs, id); b.mu.Unlock() }
func (b *NotificationBus) Publish(n Notification) {
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now()
	}
	if n.ID == "" {
		n.ID = n.Type + "-" + newIDFromTime()
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.history = append(b.history, n)
	if len(b.history) > b.cap {
		copy(b.history, b.history[len(b.history)-b.cap:])
		b.history = b.history[:b.cap]
	}
	subs := make([]Callback, 0, len(b.subs))
	for _, cb := range b.subs {
		subs = append(subs, cb)
	}
	b.mu.Unlock()
	for _, cb := range subs {
		cb := cb
		// Use bounded worker pool to prevent goroutine explosion
		select {
		case b.workers <- struct{}{}:
			go func() {
				defer func() { <-b.workers }()
				_ = cb.HandleNotification(context.Background(), n)
			}()
		default:
			// Pool full, execute synchronously as fallback
			_ = cb.HandleNotification(context.Background(), n)
		}
	}
}
func (b *NotificationBus) History(limit int) []Notification {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if limit <= 0 || limit > len(b.history) {
		limit = len(b.history)
	}
	out := make([]Notification, limit)
	copy(out, b.history[len(b.history)-limit:])
	return out
}
func (b *NotificationBus) Close() { b.mu.Lock(); b.closed = true; b.subs = nil; b.mu.Unlock() }

type WebhookCallback struct {
	URL         string
	Token       string
	Client      *http.Client
	Timeout     time.Duration
	MaxAttempts int
	RetryDelay  time.Duration
}

func (w WebhookCallback) HandleNotification(ctx context.Context, n Notification) error {
	if w.URL == "" {
		return nil
	}
	body, err := json.Marshal(n)
	if err != nil {
		return err
	}
	if w.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.Timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.Token)
	}
	c := w.Client
	if c == nil {
		c = http.DefaultClient
	}
	attempts := w.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	delay := w.RetryDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
		req2 := req.Clone(ctx)
		req2.Body = io.NopCloser(bytes.NewReader(body))
		resp, err := c.Do(req2)
		if err != nil {
			last = err
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		last = errf(ErrCallbackFailed, "webhook returned status %d", resp.StatusCode)
	}
	return last
}
