package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DurableOutboxStatus tracks the dispatch lifecycle of an outbox message.
type DurableOutboxStatus string

const (
	OutboxPending    DurableOutboxStatus = "pending"
	OutboxInFlight   DurableOutboxStatus = "in_flight"
	OutboxPublished  DurableOutboxStatus = "published"
	OutboxFailed     DurableOutboxStatus = "failed"
	OutboxDeadLetter DurableOutboxStatus = "dead_lettered"
)

// DurableOutboxEntry represents a durable message pending or completed dispatch.
type DurableOutboxEntry struct {
	ID           string              `json:"id"`
	Message      Message             `json:"message"`
	Destinations []Destination       `json:"destinations"`
	Status       DurableOutboxStatus `json:"status"`
	Attempts     int                 `json:"attempts"`
	MaxAttempts  int                 `json:"max_attempts"`
	LastError    string              `json:"last_error,omitempty"`
	CreatedAt    time.Time           `json:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at"`
	PublishedAt  *time.Time          `json:"published_at,omitempty"`
}

// DurableOutboxConfig configures the legacy in-memory dispatch worker.
type DurableOutboxConfig struct {
	Broker          *Broker
	PollInterval    time.Duration
	BatchSize       int
	MaxRetries      int
	DeadLetterQueue string
	DispatchTimeout time.Duration
}

// DurableOutbox is a legacy in-memory dispatch worker. Its entries do not survive
// process restart and are not atomic with an application database transaction.
// Deprecated: use contrib/sqlitestore.NewOutbox for a transactional SQL outbox.
type DurableOutbox struct {
	broker    *Broker
	interval  time.Duration
	batchSize int
	maxRetry  int
	dlq       string
	timeout   time.Duration

	mu      sync.RWMutex
	lifeMu  sync.Mutex
	entries map[string]*DurableOutboxEntry
	stopCh  chan struct{}
	doneCh  chan struct{}
	running atomic.Bool
}

// NewDurableOutbox initializes the legacy in-memory dispatch worker.
func NewDurableOutbox(cfg DurableOutboxConfig) (*DurableOutbox, error) {
	if cfg.Broker == nil {
		return nil, errors.New("durable outbox: Broker is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.DispatchTimeout <= 0 {
		cfg.DispatchTimeout = 5 * time.Second
	}
	if cfg.DeadLetterQueue == "" {
		cfg.DeadLetterQueue = "outbox.dlq"
	}

	return &DurableOutbox{
		broker:    cfg.Broker,
		interval:  cfg.PollInterval,
		batchSize: cfg.BatchSize,
		maxRetry:  cfg.MaxRetries,
		dlq:       cfg.DeadLetterQueue,
		timeout:   cfg.DispatchTimeout,
		entries:   make(map[string]*DurableOutboxEntry),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}, nil
}

// Enqueue stores a message in memory for background dispatch.
func (o *DurableOutbox) Enqueue(ctx context.Context, msg Message, dests []Destination) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if msg.ID == "" {
		msg.ID = o.broker.nextID("msg")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	entry := &DurableOutboxEntry{
		ID:           msg.ID,
		Message:      cloneMessage(msg),
		Destinations: append([]Destination(nil), dests...),
		Status:       OutboxPending,
		Attempts:     0,
		MaxAttempts:  o.maxRetry,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	o.mu.Lock()
	if _, exists := o.entries[msg.ID]; exists {
		o.mu.Unlock()
		return "", ErrDuplicateMessage
	}
	o.entries[msg.ID] = entry
	o.mu.Unlock()

	return msg.ID, nil
}

// Start launches the background dispatch worker loop.
func (o *DurableOutbox) Start() error {
	o.lifeMu.Lock()
	defer o.lifeMu.Unlock()
	if !o.running.CompareAndSwap(false, true) {
		return nil
	}
	o.stopCh = make(chan struct{})
	o.doneCh = make(chan struct{})
	go o.dispatchLoop()
	return nil
}

// Stop cleanly terminates the outbox worker.
func (o *DurableOutbox) Stop() error {
	o.lifeMu.Lock()
	defer o.lifeMu.Unlock()
	if !o.running.CompareAndSwap(true, false) {
		return nil
	}
	close(o.stopCh)
	<-o.doneCh
	return nil
}

func (o *DurableOutbox) dispatchLoop() {
	defer close(o.doneCh)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopCh:
			return
		case <-ticker.C:
			_ = o.flushPending(context.Background())
		}
	}
}

// Flush synchronously dispatches up to BatchSize entries and returns failures.
func (o *DurableOutbox) Flush(ctx context.Context) error {
	return o.flushPending(ctx)
}

func (o *DurableOutbox) flushPending(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	batch := make([]*DurableOutboxEntry, 0, o.batchSize)
	for _, e := range o.entries {
		if e.Status == OutboxPending || e.Status == OutboxFailed {
			e.Status = OutboxInFlight
			e.Attempts++
			batch = append(batch, e)
			if len(batch) >= o.batchSize {
				break
			}
		}
	}
	o.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	var result error
	for _, entry := range batch {
		ctx, cancel := context.WithTimeout(parent, o.timeout)

		// Append message to store
		_, err := o.broker.store.Append(ctx, entry.Message, entry.Destinations)
		cancel()

		o.mu.Lock()
		entry.UpdatedAt = time.Now()
		if err == nil {
			entry.Status = OutboxPublished
			now := time.Now()
			entry.PublishedAt = &now
		} else {
			result = errors.Join(result, err)
			entry.LastError = err.Error()
			if entry.Attempts >= entry.MaxAttempts {
				entry.Status = OutboxDeadLetter
				// Route to DLQ if configured
				if o.dlq != "" {
					dlqMsg := entry.Message
					dlqMsg.ID = fmt.Sprintf("%s.dlq", entry.ID)
					dlqMsg.DeadLetterReason = err.Error()
					dlqCtx, dlqCancel := context.WithTimeout(context.Background(), o.timeout)
					_, _ = o.broker.store.Append(dlqCtx, dlqMsg, []Destination{{Kind: DestinationDLQ, Name: o.dlq}})
					dlqCancel()
				}
			} else {
				entry.Status = OutboxFailed
			}
		}
		o.mu.Unlock()
	}
	return result
}

// PendingCount returns the number of outbox entries currently waiting to be published.
func (o *DurableOutbox) PendingCount() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	count := 0
	for _, e := range o.entries {
		if e.Status == OutboxPending || e.Status == OutboxFailed || e.Status == OutboxInFlight {
			count++
		}
	}
	return count
}

// GetEntry returns a copy of an outbox entry by ID.
func (o *DurableOutbox) GetEntry(id string) (*DurableOutboxEntry, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	entry, ok := o.entries[id]
	if !ok {
		return nil, false
	}
	copyEntry := *entry
	copyEntry.Message = cloneMessage(entry.Message)
	copyEntry.Destinations = append([]Destination(nil), entry.Destinations...)
	if entry.PublishedAt != nil {
		at := *entry.PublishedAt
		copyEntry.PublishedAt = &at
	}
	return &copyEntry, true
}
