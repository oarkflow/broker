package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
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
	subMu     sync.RWMutex
	historyMu sync.RWMutex
	subs      map[string]Callback
	history   []Notification
	cap       int
	workers   chan struct{}
	enabled   atomic.Bool
	closed    atomic.Bool
}

const notificationWorkerPoolSize = 64

func NewNotificationBus(capacity int) *NotificationBus {
	if capacity < 0 {
		capacity = 0
	}
	return &NotificationBus{subs: map[string]Callback{}, cap: capacity, workers: make(chan struct{}, notificationWorkerPoolSize)}
}
func (b *NotificationBus) Subscribe(id string, cb Callback) {
	if b == nil || id == "" || cb == nil || b.closed.Load() {
		return
	}
	b.subMu.Lock()
	b.subs[id] = cb
	b.enabled.Store(true)
	b.subMu.Unlock()
}
func (b *NotificationBus) Unsubscribe(id string) {
	if b == nil {
		return
	}
	b.subMu.Lock()
	delete(b.subs, id)
	b.enabled.Store(len(b.subs) > 0 || b.cap > 0)
	b.subMu.Unlock()
}
func (b *NotificationBus) Publish(n Notification) {
	if b == nil || b.closed.Load() || !b.enabled.Load() {
		return
	}
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now()
	}
	if n.ID == "" {
		n.ID = n.Type + "-" + newIDFromTime()
	}
	if b.cap > 0 {
		b.historyMu.Lock()
		b.history = append(b.history, n)
		if len(b.history) > b.cap {
			copy(b.history, b.history[len(b.history)-b.cap:])
			b.history = b.history[:b.cap]
		}
		b.historyMu.Unlock()
	}
	b.subMu.RLock()
	subs := make([]Callback, 0, len(b.subs))
	for _, cb := range b.subs {
		subs = append(subs, cb)
	}
	b.subMu.RUnlock()
	for _, cb := range subs {
		cb := cb
		select {
		case b.workers <- struct{}{}:
			go func() {
				defer func() { <-b.workers }()
				_ = cb.HandleNotification(context.Background(), n)
			}()
		default:
			_ = cb.HandleNotification(context.Background(), n)
		}
	}
}
func (b *NotificationBus) History(limit int) []Notification {
	if b == nil || b.cap <= 0 {
		return nil
	}
	b.historyMu.RLock()
	defer b.historyMu.RUnlock()
	if limit <= 0 || limit > len(b.history) {
		limit = len(b.history)
	}
	out := make([]Notification, limit)
	copy(out, b.history[len(b.history)-limit:])
	return out
}
func (b *NotificationBus) Enabled() bool { return b != nil && b.enabled.Load() && !b.closed.Load() }
func (b *NotificationBus) Close() {
	if b == nil || !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.subMu.Lock()
	b.subs = nil
	b.enabled.Store(false)
	b.subMu.Unlock()
}

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
