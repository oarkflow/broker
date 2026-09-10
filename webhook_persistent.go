package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// WebhookAttempt records an individual delivery attempt for audit and replay.
type WebhookAttempt struct {
	ID             string    `json:"id"`
	URL            string    `json:"url"`
	NotificationID string    `json:"notification_id"`
	Payload        []byte    `json:"payload,omitempty"`
	Attempt        int       `json:"attempt"`
	StatusCode     int       `json:"status_code,omitempty"`
	Error          string    `json:"error,omitempty"`
	Success        bool      `json:"success"`
	Timestamp      time.Time `json:"timestamp"`
}

// PersistentWebhookConfig configures the PersistentWebhookDispatcher.
type PersistentWebhookConfig struct {
	HTTPClient       *http.Client
	MaxAttempts      int
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	FailureThreshold int
	CooldownDuration time.Duration
	MaxHistoryPerURL int
}

// PersistentWebhookDispatcher manages reliable webhook deliveries with retry and circuit breakers.
type PersistentWebhookDispatcher struct {
	cfg      PersistentWebhookConfig
	breakers map[string]*CircuitBreaker
	history  map[string][]*WebhookAttempt
	payloads map[string]Notification
	mu       sync.RWMutex
	rand     *rand.Rand
}

// NewPersistentWebhookDispatcher creates a new dispatcher.
func NewPersistentWebhookDispatcher(cfg PersistentWebhookConfig) *PersistentWebhookDispatcher {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 50 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.CooldownDuration <= 0 {
		cfg.CooldownDuration = 30 * time.Second
	}
	if cfg.MaxHistoryPerURL <= 0 {
		cfg.MaxHistoryPerURL = 100
	}

	return &PersistentWebhookDispatcher{
		cfg:      cfg,
		breakers: make(map[string]*CircuitBreaker),
		history:  make(map[string][]*WebhookAttempt),
		payloads: make(map[string]Notification),
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (d *PersistentWebhookDispatcher) getCircuitBreaker(url string) *CircuitBreaker {
	d.mu.Lock()
	defer d.mu.Unlock()
	cb, ok := d.breakers[url]
	if !ok {
		cb = NewCircuitBreaker(CircuitBreakerConfig{
			Threshold: d.cfg.FailureThreshold,
			Timeout:   d.cfg.CooldownDuration,
		})
		d.breakers[url] = cb
	}
	return cb
}

// Dispatch executes durable webhook delivery with backoff and circuit breaking.
func (d *PersistentWebhookDispatcher) Dispatch(ctx context.Context, url string, n Notification) error {
	cb := d.getCircuitBreaker(url)
	if !cb.Allow() {
		return ErrCircuitOpen
	}

	d.mu.Lock()
	d.payloads[n.ID] = n
	d.mu.Unlock()

	body, err := json.Marshal(n)
	if err != nil {
		return err
	}

	backoff := d.cfg.InitialBackoff
	var lastErr error

	for attempt := 1; attempt <= d.cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			jitter := time.Duration(d.rand.Int63n(int64(backoff) / 2))
			sleepDur := backoff + jitter
			select {
			case <-time.After(sleepDur):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
			if backoff > d.cfg.MaxBackoff {
				backoff = d.cfg.MaxBackoff
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Notification-ID", n.ID)
		req.Header.Set("X-Delivery-Attempt", fmt.Sprint(attempt))

		resp, err := d.cfg.HTTPClient.Do(req)
		att := &WebhookAttempt{
			ID:             fmt.Sprintf("att-%d-%s", time.Now().UnixNano(), n.ID),
			URL:            url,
			NotificationID: n.ID,
			Payload:        body,
			Attempt:        attempt,
			Timestamp:      time.Now(),
		}

		if err != nil {
			att.Error = err.Error()
			att.Success = false
			lastErr = err
			d.recordAttempt(url, att)
			cb.RecordFailure()
			continue
		}

		att.StatusCode = resp.StatusCode
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			att.Success = true
			d.recordAttempt(url, att)
			cb.RecordSuccess()
			return nil
		}

		att.Success = false
		att.Error = fmt.Sprintf("status code: %d", resp.StatusCode)
		lastErr = errors.New(att.Error)
		d.recordAttempt(url, att)
		cb.RecordFailure()
	}

	return lastErr
}

func (d *PersistentWebhookDispatcher) recordAttempt(url string, att *WebhookAttempt) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hist := d.history[url]
	hist = append(hist, att)
	if len(hist) > d.cfg.MaxHistoryPerURL {
		hist = hist[len(hist)-d.cfg.MaxHistoryPerURL:]
	}
	d.history[url] = hist
}

// History returns the delivery attempts for a given webhook URL.
func (d *PersistentWebhookDispatcher) History(url string) []*WebhookAttempt {
	d.mu.RLock()
	defer d.mu.RUnlock()
	list := d.history[url]
	out := make([]*WebhookAttempt, len(list))
	copy(out, list)
	return out
}

// ReTrigger replays a previously saved notification by ID.
func (d *PersistentWebhookDispatcher) ReTrigger(ctx context.Context, url string, notificationID string) error {
	d.mu.RLock()
	n, ok := d.payloads[notificationID]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("notification %s not found in dispatcher cache", notificationID)
	}
	return d.Dispatch(ctx, url, n)
}

// CircuitState returns the state of the circuit breaker for a URL.
func (d *PersistentWebhookDispatcher) CircuitState(url string) CircuitState {
	cb := d.getCircuitBreaker(url)
	return cb.State()
}

// ResetCircuit manually resets the circuit breaker for a URL.
func (d *PersistentWebhookDispatcher) ResetCircuit(url string) {
	cb := d.getCircuitBreaker(url)
	cb.Reset()
}
