package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
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
	ID                string              `json:"id"`
	Message           Message             `json:"message"`
	Destinations      []Destination       `json:"destinations"`
	Status            DurableOutboxStatus `json:"status"`
	Attempts          int                 `json:"attempts"`
	DLQAttempts       int                 `json:"dlq_attempts,omitempty"`
	MaxAttempts       int                 `json:"max_attempts"`
	DeadLetterPending bool                `json:"dead_letter_pending,omitempty"`
	LastError         string              `json:"last_error,omitempty"`
	DLQError          string              `json:"dlq_error,omitempty"`
	CreatedAt         time.Time           `json:"created_at"`
	UpdatedAt         time.Time           `json:"updated_at"`
	PublishedAt       *time.Time          `json:"published_at,omitempty"`
	ClaimToken        string              `json:"claim_token,omitempty"`
	ClaimUntil        time.Time           `json:"claim_until,omitempty"`
	NextAttemptAt     time.Time           `json:"next_attempt_at,omitempty"`
}

// DurableOutboxConfig configures the background transactional outbox engine.
type DurableOutboxConfig struct {
	Broker *Broker
	// Store overrides the broker's StateStore for outbox records. If nil, the
	// broker StateStore is used when it implements OutboxStateStore.
	Store        OutboxStateStore
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
	// RetryPolicy controls exponential backoff for broker and DLQ failures.
	// Defaults to a one second initial delay, one minute cap, and jitter.
	RetryPolicy RetryPolicy
	// ClaimLease controls how long a dispatch claim remains owned before another
	// dispatcher may recover it. It must cover the sequential batch dispatch
	// budget plus one minute; shorter values are rejected to avoid reclaiming
	// entries while their original dispatcher may still be publishing them.
	ClaimLease      time.Duration
	DeadLetterQueue string
	DispatchTimeout time.Duration
}

// DurableOutbox provides at-least-once dispatch. Entries survive process
// restarts only when its OutboxStateStore is backed by durable storage; the
// default MemoryStateStore is process-local. A crash after message append but
// before marking the entry published can cause a duplicate append. Stores that
// implement OutboxClaimStore coordinate multiple dispatcher instances.
type DurableOutbox struct {
	broker      *Broker
	interval    time.Duration
	batchSize   int
	maxRetry    int
	dlq         string
	timeout     time.Duration
	claimLease  time.Duration
	retryPolicy RetryPolicy
	store       OutboxStateStore

	mu         sync.RWMutex
	dispatchMu sync.Mutex
	entries    map[string]*DurableOutboxEntry
	stopCh     chan struct{}
	doneCh     chan struct{}

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
}

// NewDurableOutbox initializes an outbox worker and restores persisted entries
// when the configured state store implements OutboxStateStore.
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
	if cfg.RetryPolicy == (RetryPolicy{}) {
		cfg.RetryPolicy = RetryPolicy{Initial: time.Second, Max: time.Minute, Factor: 2, Jitter: true}
	}
	if cfg.RetryPolicy.Initial <= 0 {
		cfg.RetryPolicy.Initial = time.Second
	}
	if cfg.RetryPolicy.Max <= 0 {
		cfg.RetryPolicy.Max = time.Minute
	}
	if cfg.RetryPolicy.Max < cfg.RetryPolicy.Initial {
		cfg.RetryPolicy.Max = cfg.RetryPolicy.Initial
	}
	if cfg.RetryPolicy.Factor <= 0 {
		cfg.RetryPolicy.Factor = 2
	}
	if cfg.DispatchTimeout <= 0 {
		cfg.DispatchTimeout = 5 * time.Second
	}
	requiredClaimLease, err := minimumOutboxClaimLease(cfg.DispatchTimeout, cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	if cfg.ClaimLease <= 0 {
		cfg.ClaimLease = requiredClaimLease
	} else if cfg.ClaimLease < requiredClaimLease {
		return nil, fmt.Errorf("durable outbox: claim lease %s is shorter than required batch budget %s", cfg.ClaimLease, requiredClaimLease)
	}
	if cfg.DeadLetterQueue == "" {
		cfg.DeadLetterQueue = "outbox.dlq"
	}

	stateStore := cfg.Store
	if stateStore == nil {
		stateStore, _ = cfg.Broker.stateStore.(OutboxStateStore)
	}
	outbox := &DurableOutbox{
		broker:      cfg.Broker,
		interval:    cfg.PollInterval,
		batchSize:   cfg.BatchSize,
		maxRetry:    cfg.MaxRetries,
		dlq:         cfg.DeadLetterQueue,
		timeout:     cfg.DispatchTimeout,
		claimLease:  cfg.ClaimLease,
		retryPolicy: cfg.RetryPolicy,
		store:       stateStore,
		entries:     make(map[string]*DurableOutboxEntry),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	if stateStore != nil {
		entries, err := stateStore.ListOutboxEntries(context.Background(), 0)
		if err != nil {
			return nil, fmt.Errorf("durable outbox: load entries: %w", err)
		}
		for i := range entries {
			entry := cloneOutboxEntry(entries[i])
			// A process may stop after recording in-flight but before recording
			// the outcome. Re-dispatch on recovery; delivery is at least once.
			_, supportsClaims := stateStore.(OutboxClaimStore)
			if entry.Status == OutboxInFlight && !supportsClaims {
				entry.Status = OutboxPending
				entry.ClaimToken = ""
				entry.ClaimUntil = time.Time{}
				entry.UpdatedAt = time.Now()
				if err := stateStore.PutOutboxEntry(context.Background(), entry); err != nil {
					return nil, fmt.Errorf("durable outbox: recover entry %q: %w", entry.ID, err)
				}
			}
			outbox.entries[entry.ID] = &entry
		}
	}
	return outbox, nil
}

func minimumOutboxClaimLease(dispatchTimeout time.Duration, batchSize int) (time.Duration, error) {
	if dispatchTimeout <= 0 || batchSize <= 0 {
		return 0, ErrInvalidMessage
	}
	maxDuration := time.Duration(1<<63 - 1)
	if uint64(batchSize) > uint64(maxDuration/dispatchTimeout) {
		return 0, errors.New("durable outbox: batch dispatch budget overflows time.Duration")
	}
	budget := dispatchTimeout * time.Duration(batchSize)
	const grace = time.Minute
	if budget > maxDuration-grace {
		return 0, errors.New("durable outbox: batch dispatch budget plus claim grace overflows time.Duration")
	}
	return budget + grace, nil
}

// Enqueue stores a message into the outbox for background at-least-once
// dispatch. If a durable OutboxStateStore is configured, the entry is persisted
// before Enqueue returns.
func (o *DurableOutbox) Enqueue(ctx context.Context, msg Message, dests []Destination) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	entry := o.newEntry(msg, dests)
	o.mu.Lock()
	if _, exists := o.entries[entry.ID]; exists {
		o.mu.Unlock()
		return "", fmt.Errorf("durable outbox: entry %q already exists", entry.ID)
	}
	if o.store != nil {
		if err := o.store.PutOutboxEntry(ctx, entry); err != nil {
			o.mu.Unlock()
			return "", fmt.Errorf("durable outbox: persist entry: %w", err)
		}
	}
	o.entries[entry.ID] = &entry
	o.mu.Unlock()
	return entry.ID, nil
}

// EnqueueTx writes the outbox record into a caller-owned database transaction.
// Commit that transaction only after all related business writes succeed. The
// configured OutboxStateStore must read from the same database; the dispatcher
// discovers the committed entry on its next poll. This method does not commit
// the caller's transaction.
func (o *DurableOutbox) EnqueueTx(ctx context.Context, tx OutboxEntryTransaction, msg Message, dests []Destination) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if tx == nil {
		return "", errors.New("durable outbox: transaction is required")
	}
	if o.store == nil {
		return "", errors.New("durable outbox: persistent OutboxStateStore is required for transactional enqueue")
	}
	entry := o.newEntry(msg, dests)
	o.mu.RLock()
	_, exists := o.entries[entry.ID]
	o.mu.RUnlock()
	if exists {
		return "", fmt.Errorf("durable outbox: entry %q already exists", entry.ID)
	}
	if err := tx.InsertOutboxEntry(ctx, entry); err != nil {
		return "", fmt.Errorf("durable outbox: insert entry in transaction: %w", err)
	}
	return entry.ID, nil
}

func (o *DurableOutbox) newEntry(msg Message, dests []Destination) DurableOutboxEntry {
	if msg.ID == "" {
		msg.ID = o.broker.nextID("msg")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	return DurableOutboxEntry{
		ID:           msg.ID,
		Message:      cloneMessage(msg),
		Destinations: append([]Destination(nil), dests...),
		Status:       OutboxPending,
		Attempts:     0,
		MaxAttempts:  o.maxRetry,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

// Start launches the background dispatch worker loop. An outbox instance can
// be started once; after Stop, create a new instance to resume persisted work.
func (o *DurableOutbox) Start() error {
	o.lifecycleMu.Lock()
	defer o.lifecycleMu.Unlock()
	if o.stopped {
		return ErrClosed
	}
	if o.started {
		return nil
	}
	o.started = true
	go o.dispatchLoop()
	return nil
}

// Stop cleanly terminates the outbox worker.
func (o *DurableOutbox) Stop() error {
	o.lifecycleMu.Lock()
	if !o.started {
		o.lifecycleMu.Unlock()
		return nil
	}
	if !o.stopped {
		o.stopped = true
		close(o.stopCh)
	}
	done := o.doneCh
	o.lifecycleMu.Unlock()
	<-done
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
			ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
			_ = o.flushPending(ctx)
			cancel()
		}
	}
}

// Flush forces an immediate synchronous dispatch of all pending outbox entries.
func (o *DurableOutbox) Flush(ctx context.Context) error {
	return o.flushPending(ctx)
}

func (o *DurableOutbox) flushPending(parent context.Context) error {
	o.dispatchMu.Lock()
	defer o.dispatchMu.Unlock()
	if claims, ok := o.store.(OutboxClaimStore); ok {
		return o.flushClaimed(parent, claims)
	}
	if err := o.refreshDispatchable(parent); err != nil {
		return err
	}
	o.mu.Lock()
	due := make([]*DurableOutboxEntry, 0, len(o.entries))
	now := time.Now()
	for _, e := range o.entries {
		if outboxEntryDue(*e, now) {
			due = append(due, e)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].CreatedAt.Equal(due[j].CreatedAt) {
			return due[i].ID < due[j].ID
		}
		return due[i].CreatedAt.Before(due[j].CreatedAt)
	})
	if len(due) > o.batchSize {
		due = due[:o.batchSize]
	}
	batch := make([]DurableOutboxEntry, 0, len(due))
	for _, entry := range due {
		entry.Status = OutboxInFlight
		batch = append(batch, cloneOutboxEntry(*entry))
	}
	o.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	for i := range batch {
		entry := &batch[i]
		if err := parent.Err(); err != nil {
			return errors.Join(err, o.restorePending(batch[i:]))
		}
		save := func(ctx context.Context, entry DurableOutboxEntry) error {
			return o.persist(ctx, entry)
		}
		if err := o.dispatchEntry(parent, entry, save); err != nil {
			return errors.Join(err, o.restorePending(batch[i:]))
		}
		o.mu.Lock()
		current := o.entries[entry.ID]
		if current != nil {
			updated := cloneOutboxEntry(*entry)
			*current = updated
		}
		o.mu.Unlock()
	}
	return nil
}

func (o *DurableOutbox) flushClaimed(ctx context.Context, store OutboxClaimStore) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	token := o.broker.nextID("outbox-claim")
	entries, err := store.ClaimOutboxEntries(ctx, token, o.batchSize, o.claimLease)
	if err != nil {
		return fmt.Errorf("durable outbox: claim entries: %w", err)
	}
	for _, claimed := range entries {
		entry := cloneOutboxEntry(claimed)
		o.mu.Lock()
		cached := cloneOutboxEntry(entry)
		o.entries[entry.ID] = &cached
		o.mu.Unlock()

		if err := ctx.Err(); err != nil {
			return err // the claim expires and another worker can recover it
		}
		save := func(ctx context.Context, entry DurableOutboxEntry) error {
			return store.UpdateClaimedOutboxEntry(ctx, token, entry)
		}
		if err := o.dispatchEntry(ctx, &entry, save); err != nil {
			return fmt.Errorf("durable outbox: dispatch %q: %w", entry.ID, err)
		}
		o.mu.Lock()
		cached = cloneOutboxEntry(entry)
		o.entries[entry.ID] = &cached
		o.mu.Unlock()
	}
	return nil
}

func (o *DurableOutbox) dispatchEntry(ctx context.Context, entry *DurableOutboxEntry, save func(context.Context, DurableOutboxEntry) error) error {
	if entry.DeadLetterPending {
		return o.dispatchDeadLetter(ctx, entry, save)
	}
	entry.Attempts++
	entry.UpdatedAt = time.Now()
	if err := save(ctx, *entry); err != nil {
		return err
	}
	if metrics := o.broker.promMetrics; metrics != nil {
		metrics.RecordOutboxDispatchAttempt()
	}

	dispatchCtx, cancel := context.WithTimeout(ctx, o.timeout)
	_, dispatchErr := o.broker.store.Append(dispatchCtx, entry.Message, entry.Destinations)
	cancel()
	entry.UpdatedAt = time.Now()
	if dispatchErr == nil {
		entry.Status = OutboxPublished
		entry.LastError = ""
		entry.NextAttemptAt = time.Time{}
		now := time.Now()
		entry.PublishedAt = &now
		if err := o.finishEntry(ctx, entry, save); err != nil {
			return err
		}
		if metrics := o.broker.promMetrics; metrics != nil {
			metrics.RecordOutboxPublished()
		}
		return nil
	}

	if metrics := o.broker.promMetrics; metrics != nil {
		metrics.RecordOutboxDispatchFailure()
	}
	entry.LastError = dispatchErr.Error()
	entry.PublishedAt = nil
	if entry.Attempts >= entry.MaxAttempts {
		if o.dlq == "" {
			entry.Status = OutboxDeadLetter
			entry.NextAttemptAt = time.Time{}
			return o.finishEntry(ctx, entry, save)
		}
		entry.DeadLetterPending = true
		entry.DLQError = ""
		// Persist the phase change before writing the DLQ so recovery retries the
		// DLQ action instead of publishing the original message again.
		if err := save(ctx, *entry); err != nil {
			return err
		}
		return o.dispatchDeadLetter(ctx, entry, save)
	}
	entry.Status = OutboxFailed
	entry.NextAttemptAt = time.Now().Add(backoffDelay(o.retryPolicy, entry.Attempts))
	return o.finishEntry(ctx, entry, save)
}

func (o *DurableOutbox) dispatchDeadLetter(ctx context.Context, entry *DurableOutboxEntry, save func(context.Context, DurableOutboxEntry) error) error {
	entry.DLQAttempts++
	entry.UpdatedAt = time.Now()
	if err := save(ctx, *entry); err != nil {
		return err
	}

	dlqMsg := entry.Message
	dlqMsg.ID = fmt.Sprintf("%s.dlq", entry.ID)
	dlqMsg.DeadLetterReason = entry.LastError
	if _, exists, err := o.broker.store.Get(ctx, dlqMsg.ID); err != nil {
		entry.DLQError = err.Error()
		return o.failDeadLetter(ctx, entry, save)
	} else if exists {
		entry.Status = OutboxDeadLetter
		entry.DeadLetterPending = false
		entry.DLQError = ""
		entry.NextAttemptAt = time.Time{}
		if err := o.finishEntry(ctx, entry, save); err != nil {
			return err
		}
		if metrics := o.broker.promMetrics; metrics != nil {
			metrics.RecordOutboxDeadLettered()
		}
		return nil
	}

	dlqCtx, cancel := context.WithTimeout(ctx, o.timeout)
	_, err := o.broker.store.Append(dlqCtx, dlqMsg, []Destination{{Kind: DestinationDLQ, Name: o.dlq}})
	cancel()
	if err != nil {
		entry.DLQError = err.Error()
		return o.failDeadLetter(ctx, entry, save)
	}
	entry.Status = OutboxDeadLetter
	entry.DeadLetterPending = false
	entry.DLQError = ""
	entry.NextAttemptAt = time.Time{}
	if err := o.finishEntry(ctx, entry, save); err != nil {
		return err
	}
	if metrics := o.broker.promMetrics; metrics != nil {
		metrics.RecordOutboxDeadLettered()
	}
	return nil
}

func (o *DurableOutbox) failDeadLetter(ctx context.Context, entry *DurableOutboxEntry, save func(context.Context, DurableOutboxEntry) error) error {
	entry.Status = OutboxFailed
	entry.NextAttemptAt = time.Now().Add(backoffDelay(o.retryPolicy, entry.DLQAttempts))
	return o.finishEntry(ctx, entry, save)
}

func (o *DurableOutbox) finishEntry(ctx context.Context, entry *DurableOutboxEntry, save func(context.Context, DurableOutboxEntry) error) error {
	entry.UpdatedAt = time.Now()
	entry.ClaimToken = ""
	entry.ClaimUntil = time.Time{}
	return save(ctx, *entry)
}

func (o *DurableOutbox) refreshDispatchable(ctx context.Context) error {
	if o.store == nil {
		return nil
	}
	entries, err := o.store.ListDispatchableOutboxEntries(ctx, o.batchSize)
	if err != nil {
		return fmt.Errorf("durable outbox: refresh entries: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range entries {
		entry := cloneOutboxEntry(entries[i])
		if _, exists := o.entries[entry.ID]; !exists {
			o.entries[entry.ID] = &entry
		}
	}
	return nil
}

func (o *DurableOutbox) persist(ctx context.Context, entry DurableOutboxEntry) error {
	if o.store == nil {
		return nil
	}
	if err := o.store.PutOutboxEntry(ctx, entry); err != nil {
		return fmt.Errorf("durable outbox: persist entry %q: %w", entry.ID, err)
	}
	return nil
}

func (o *DurableOutbox) restorePending(entries []DurableOutboxEntry) error {
	var restoreErrors []error
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	for _, entry := range entries {
		o.mu.Lock()
		current := o.entries[entry.ID]
		if current != nil && current.Status == OutboxInFlight {
			current.Status = OutboxPending
			current.UpdatedAt = time.Now()
			entry = cloneOutboxEntry(*current)
		}
		o.mu.Unlock()
		if err := o.persist(ctx, entry); err != nil {
			restoreErrors = append(restoreErrors, err)
		}
	}
	return errors.Join(restoreErrors...)
}

func cloneOutboxEntry(in DurableOutboxEntry) DurableOutboxEntry {
	out := in
	out.Message = cloneMessage(in.Message)
	out.Destinations = append([]Destination(nil), in.Destinations...)
	if in.PublishedAt != nil {
		publishedAt := *in.PublishedAt
		out.PublishedAt = &publishedAt
	}
	return out
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
	copyEntry = cloneOutboxEntry(copyEntry)
	return &copyEntry, true
}

// Prune removes at most limit published or dead-lettered entries whose last
// update is older than before. Pending, failed, and in-flight entries are never
// removed. Pruning also ends GetEntry retention and permits reuse of a message
// ID after its outbox record has expired.
func (o *DurableOutbox) Prune(ctx context.Context, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() || limit <= 0 {
		return 0, ErrInvalidMessage
	}
	o.dispatchMu.Lock()
	defer o.dispatchMu.Unlock()
	var ids []string
	if o.store != nil {
		retention, ok := o.store.(OutboxRetentionStore)
		if !ok {
			return 0, ErrUnsupportedMode
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		var err error
		ids, err = retention.PruneCompletedOutboxEntries(ctx, before, limit)
		if err != nil {
			return 0, fmt.Errorf("durable outbox: prune entries: %w", err)
		}
	} else {
		o.mu.Lock()
		candidates := make([]DurableOutboxEntry, 0)
		for _, entry := range o.entries {
			if (entry.Status == OutboxPublished || entry.Status == OutboxDeadLetter) && entry.UpdatedAt.Before(before) {
				candidates = append(candidates, cloneOutboxEntry(*entry))
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].UpdatedAt.Equal(candidates[j].UpdatedAt) {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].UpdatedAt.Before(candidates[j].UpdatedAt)
		})
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}
		for _, entry := range candidates {
			delete(o.entries, entry.ID)
			ids = append(ids, entry.ID)
		}
		o.mu.Unlock()
		return len(ids), nil
	}

	for _, id := range ids {
		delete(o.entries, id)
	}
	return len(ids), nil
}
