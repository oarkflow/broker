package broker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker/pkg/pool"
)

type consumerStrategyKey struct {
	queue string
	group string
	type_ StrategyType
}

type Delivery struct {
	Message Message
	ack     *ackHandle
}

func (d Delivery) Ack(ctx context.Context) error { return d.ack.Ack(ctx) }
func (d Delivery) Nack(ctx context.Context, opts ...NackOption) error {
	return d.ack.Nack(ctx, opts...)
}
func (d Delivery) Reject(ctx context.Context, reason string) error   { return d.ack.Reject(ctx, reason) }
func (d Delivery) Extend(ctx context.Context, v time.Duration) error { return d.ack.Extend(ctx, v) }
func (d Delivery) Progress(ctx context.Context, p Progress) error    { return d.ack.Progress(ctx, p) }
func (d Delivery) Respond(ctx context.Context, response any, opts ...ResponseOption) error {
	return d.ack.Respond(ctx, response, opts...)
}
func (d Delivery) RespondError(ctx context.Context, err error, opts ...ResponseOption) error {
	return d.ack.RespondError(ctx, err, opts...)
}
func (d Delivery) Decode(out any) error { return d.ack.b.codec.Unmarshal(d.Message.Payload, out) }

type Progress struct {
	Current   int64     `json:"current"`
	Total     int64     `json:"total"`
	Percent   float64   `json:"percent"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type NackOptions struct {
	Action NackAction
	Delay  time.Duration
	DLQ    string
	Reason string
	Error  string
}
type NackOption func(*NackOptions)

func NackWithDelay(v time.Duration) NackOption { return func(o *NackOptions) { o.Delay = v } }
func NackToDLQ(v string) NackOption            { return func(o *NackOptions) { o.Action = NackDLQ; o.DLQ = v } }
func NackReason(v string) NackOption           { return func(o *NackOptions) { o.Reason = v } }
func NackError(v string) NackOption            { return func(o *NackOptions) { o.Error = v } }

type ResponseOptions struct {
	Headers   map[string]string
	Status    ResponseStatus
	EndStream bool
}
type ResponseOption func(*ResponseOptions)

func WithResponseHeader(k, v string) ResponseOption {
	return func(o *ResponseOptions) {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers[k] = v
	}
}
func WithResponseStatus(v ResponseStatus) ResponseOption {
	return func(o *ResponseOptions) { o.Status = v }
}
func WithEndStream(v bool) ResponseOption { return func(o *ResponseOptions) { o.EndStream = v } }

type ackHandle struct {
	b          *Broker
	msg        Message
	queue      string
	consumer   string
	settleMu   sync.Mutex
	settled    bool
	taskResult []byte
}

func (a *ackHandle) Ack(ctx context.Context) error {
	a.settleMu.Lock()
	if a.settled {
		a.settleMu.Unlock()
		return nil
	}
	err := a.b.store.Ack(ctx, AckRequest{MessageID: a.msg.ID, Queue: a.queue, LeaseToken: a.msg.LeaseToken, ConsumerID: a.consumer})
	if err != nil {
		a.settleMu.Unlock()
		return err
	}
	taskResult := append([]byte(nil), a.taskResult...)
	a.settled = true
	a.settleMu.Unlock()
	a.b.releaseDispatchAssignment(a.msg.ID)
	if a.b.cfg.Hooks.OnAck != nil {
		a.b.cfg.Hooks.OnAck(ctx, MessageEvent{Message: a.msg, Consumer: a.consumer, Queue: a.queue, Time: time.Now()})
	}
	a.b.notify.Publish(Notification{Type: "message.acked", Source: a.consumer, Severity: SeverityInfo, MessageID: a.msg.ID, Queue: a.queue, TenantID: a.msg.TenantID, Timestamp: time.Now()})
	if err := a.b.completeTask(ctx, a.msg, taskResult, nil); err != nil {
		return fmt.Errorf("message %s was acknowledged but task completion could not be persisted: %w", a.msg.ID, err)
	}
	return nil
}
func (a *ackHandle) Nack(ctx context.Context, opts ...NackOption) error {
	var o NackOptions
	o.Action = NackRequeue
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	a.settleMu.Lock()
	if a.settled {
		a.settleMu.Unlock()
		return nil
	}
	err := a.b.store.Nack(ctx, NackRequest{MessageID: a.msg.ID, Queue: a.queue, LeaseToken: a.msg.LeaseToken, ConsumerID: a.consumer, Action: o.Action, Delay: o.Delay, DLQ: o.DLQ, Reason: o.Reason, Error: o.Error, IncrementAttempt: true})
	if err != nil {
		a.settleMu.Unlock()
		return err
	}
	a.settled = true
	a.settleMu.Unlock()
	a.b.releaseDispatchAssignment(a.msg.ID)
	return nil
}
func (a *ackHandle) Reject(ctx context.Context, reason string) error {
	return a.Nack(ctx, NackToDLQ(""), NackReason(reason))
}
func (a *ackHandle) Extend(ctx context.Context, v time.Duration) error {
	err := a.b.store.ExtendLease(ctx, LeaseRequest{MessageID: a.msg.ID, LeaseToken: a.msg.LeaseToken, ConsumerID: a.consumer, VisibilityTimeout: v})
	if err == nil && a.b.cfg.Hooks.OnLeaseExtended != nil {
		a.b.cfg.Hooks.OnLeaseExtended(ctx, MessageEvent{Message: a.msg, Consumer: a.consumer, Queue: a.queue, Time: time.Now()})
	}
	return err
}
func (a *ackHandle) Progress(ctx context.Context, p Progress) error {
	m := a.msg
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = time.Now()
	}
	if m.TaskID == "" && m.Headers != nil {
		m.TaskID = m.Headers["x-task-id"]
		m.TaskName = m.Headers["x-task-name"]
	}
	if m.Metadata == nil {
		m.Metadata = map[string]string{}
	}
	m.Status = MessageProgressing
	if err := a.b.store.Update(ctx, m); err != nil {
		return fmt.Errorf("persist progress for message %s: %w", m.ID, err)
	}
	if m.TaskID != "" {
		if err := a.b.updateTaskProgress(ctx, m.TaskID, p); err != nil {
			return fmt.Errorf("persist progress for task %s: %w", m.TaskID, err)
		}
	}
	a.b.notify.Publish(Notification{Type: "message.progress", Source: a.consumer, Severity: SeverityInfo, MessageID: m.ID, TaskID: m.TaskID, Queue: a.queue, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"progress": p}})
	return nil
}
func (a *ackHandle) Respond(ctx context.Context, response any, opts ...ResponseOption) error {
	var o ResponseOptions
	o.Status = ResponseOK
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	body, err := a.b.codec.Marshal(response)
	if err != nil {
		return err
	}
	if a.msg.TaskID != "" || a.msg.Headers["x-task-id"] != "" {
		a.settleMu.Lock()
		if !a.settled {
			a.taskResult = append([]byte(nil), body...)
		}
		a.settleMu.Unlock()
	}
	if o.EndStream {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers["x-stream-end"] = "true"
	}
	resp := Response{MessageID: a.b.nextID("resp"), CorrelationID: a.msg.CorrelationID, Payload: body, Headers: o.Headers, Status: o.Status}
	a.b.completeResponse(resp)
	return nil
}
func (a *ackHandle) RespondError(ctx context.Context, err error, opts ...ResponseOption) error {
	if err == nil {
		return a.Respond(ctx, nil, opts...)
	}
	var o ResponseOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.EndStream {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers["x-stream-end"] = "true"
	}
	resp := Response{MessageID: a.b.nextID("resp"), CorrelationID: a.msg.CorrelationID, Headers: o.Headers, Error: &BrokerError{Code: "handler_error", Message: err.Error(), Retryable: !IsPermanent(err)}, Status: ResponseError}
	a.b.completeResponse(resp)
	return nil
}

type Subscription struct {
	id                       string
	routeName                string
	b                        *Broker
	cfg                      ConsumerConfig
	handler                  Handler
	batchHandler             BatchHandler
	ctx                      context.Context
	cancel                   context.CancelFunc
	stopped                  chan struct{}
	paused                   atomic.Bool
	inFlight                 atomic.Int64
	batchWaitNanos           atomic.Int64
	batchWaitFloor           time.Duration
	batchWaitGrowthDivisor   int
	batchWaitDecreaseDivisor int
	strategy                 ConsumerStrategy
	dedup                    *ConsumerDedup
	filter                   ConsumerFilter
}

type dispatchAssignment struct {
	ConsumerID string
	Queue      string
	Group      string
}

// ConsumerDedup provides idempotent consumer processing by deduplicating messages
// based on idempotency key or deduplication ID.
type ConsumerDedup struct {
	mu     sync.Mutex
	seen   map[string]time.Time
	window time.Duration
}

// NewConsumerDedup creates a new consumer deduplication tracker.
func NewConsumerDedup(window time.Duration) *ConsumerDedup {
	if window <= 0 {
		window = 5 * time.Minute
	}
	return &ConsumerDedup{
		seen:   make(map[string]time.Time),
		window: window,
	}
}

// IsDuplicate returns true if the message has already been processed within the dedup window.
func (d *ConsumerDedup) IsDuplicate(msg Message) bool {
	key := d.dedupKey(msg)
	if key == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.seen[key]; ok && time.Since(t) < d.window {
		return true
	}
	d.seen[key] = time.Now()
	return false
}

// Cleanup removes expired entries from the dedup tracker.
func (d *ConsumerDedup) Cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()
	cutoff := time.Now().Add(-d.window)
	for k, t := range d.seen {
		if t.Before(cutoff) {
			delete(d.seen, k)
		}
	}
}

func (d *ConsumerDedup) dedupKey(msg Message) string {
	if msg.IdempotencyKey != "" {
		return "idemp:" + msg.IdempotencyKey
	}
	if msg.DeduplicationID != "" {
		return "dedup:" + msg.DeduplicationID
	}
	return ""
}

func (s *Subscription) ID() string { return s.id }
func (s *Subscription) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	<-s.stopped
}
func (s *Subscription) Pause() {
	s.paused.Store(true)
	if s.b.cfg.Hooks.OnConsumerPaused != nil {
		s.b.cfg.Hooks.OnConsumerPaused(context.Background(), ConsumerEvent{Consumer: s.cfg.Name, Group: s.cfg.Group, Queue: s.cfg.Queue, State: ConsumerPaused, Time: time.Now()})
	}
}
func (s *Subscription) Resume() {
	s.paused.Store(false)
	if s.b.cfg.Hooks.OnConsumerResumed != nil {
		s.b.cfg.Hooks.OnConsumerResumed(context.Background(), ConsumerEvent{Consumer: s.cfg.Name, Group: s.cfg.Group, Queue: s.cfg.Queue, State: ConsumerReady, Time: time.Now()})
	}
}

func (b *Broker) SubscribeBatch(ctx context.Context, cfg ConsumerConfig, handler BatchHandler) (*Subscription, error) {
	if handler == nil {
		return nil, ErrNoHandler
	}
	if cfg.BatchSize <= 0 {
		profile, err := DefaultsForProfile(b.cfg.PerformanceProfile)
		if err != nil {
			return nil, err
		}
		cfg.BatchSize = profile.ConsumerBatchSize
	}
	return b.subscribe(ctx, cfg, nil, handler)
}

func (b *Broker) Subscribe(ctx context.Context, cfg ConsumerConfig, handler Handler) (*Subscription, error) {
	if handler == nil {
		return nil, ErrNoHandler
	}
	return b.subscribe(ctx, cfg, handler, nil)
}

func (b *Broker) subscribe(ctx context.Context, cfg ConsumerConfig, handler Handler, batchHandler BatchHandler) (*Subscription, error) {
	if cfg.Name == "" {
		cfg.Name = b.nextID("consumer")
	}
	if cfg.Group == "" {
		cfg.Group = cfg.Name
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = b.cfg.DefaultVisibility
	}
	if cfg.BatchWait > 0 && cfg.BatchWait >= cfg.VisibilityTimeout {
		cfg.BatchWait = cfg.VisibilityTimeout / 2
	}
	if cfg.RetryPolicy.MaxAttempts <= 0 {
		cfg.RetryPolicy = b.cfg.DefaultRetry
	}
	if cfg.AckMode == "" {
		cfg.AckMode = AckAuto
	}
	routePattern := ""
	if cfg.Queue == "" && (cfg.Topic != "" || cfg.Pattern != "") {
		if hasRestrictedTenantScope(ctx) {
			return nil, errf(ErrUnauthorized, "tenant-scoped credentials cannot create shared subscription routes")
		}
		routePattern = firstNonEmpty(cfg.Pattern, cfg.Topic)
		cfg.Queue = "sub." + cfg.Group + "." + sanitize(routePattern)
	}
	if cfg.Queue == "" {
		cfg.Queue = b.cfg.DefaultQueue
	}
	if cfg.DLQ == "" {
		cfg.DLQ = cfg.Queue + ".dlq"
	}
	queueConfig := QueueConfig{Name: cfg.Queue, Workers: cfg.Concurrency, VisibilityTimeout: cfg.VisibilityTimeout, RetryPolicy: cfg.RetryPolicy, DLQ: cfg.DLQ}
	if hasRestrictedTenantScope(ctx) {
		// A tenant-scoped subscriber may consume an existing shared queue, but
		// cannot create it or change its shared configuration.
		if !b.queueDeclared(cfg.Queue) {
			return nil, errf(ErrUnauthorized, "tenant-scoped credentials cannot create or configure shared queues")
		}
	} else if err := b.DeclareQueue(ctx, queueConfig); err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(b.ctx)
	if principal, ok := scopedPrincipalFromContext(ctx); ok {
		cctx = WithAdminPrincipal(cctx, principal)
	}
	var strategy ConsumerStrategy
	if cfg.Strategy != "" {
		strategy = b.sharedConsumerStrategy(cfg)
	}
	s := &Subscription{id: b.nextID("sub"), b: b, cfg: cfg, handler: handler, batchHandler: batchHandler, ctx: cctx, cancel: cancel, stopped: make(chan struct{}), strategy: strategy}
	if routePattern != "" {
		s.routeName = "subscription." + s.id
		b.AddRoute(Route{Name: s.routeName, Mode: RouteTopic, Match: routePattern, To: []Destination{{Kind: DestinationQueue, Name: cfg.Queue}}, Enabled: true})
	}
	if batchHandler != nil && cfg.BatchWait > 0 {
		profile, _ := DefaultsForProfile(b.cfg.PerformanceProfile)
		initialWait := minDuration(cfg.BatchWait, profile.ConsumerBatchWaitInitial)
		s.batchWaitFloor = minDuration(cfg.BatchWait, profile.ConsumerBatchWaitFloor)
		s.batchWaitGrowthDivisor = profile.ConsumerBatchWaitGrowthDivisor
		s.batchWaitDecreaseDivisor = profile.ConsumerBatchWaitDecreaseDivisor
		s.batchWaitNanos.Store(initialWait.Nanoseconds())
	}
	if batchHandler != nil && b.promMetrics != nil {
		b.promMetrics.SetConsumerBatchWait(s, time.Duration(s.batchWaitNanos.Load()))
	}
	if f, err := NewConsumerFilter(cfg); err == nil && f != nil {
		s.filter = f
	}
	if cfg.DeduplicationWindow > 0 || cfg.Idempotent {
		s.dedup = NewConsumerDedup(cfg.DeduplicationWindow)
	}
	b.subsMu.Lock()
	b.subs[s.id] = s
	b.subsMu.Unlock()
	if b.cfg.Hooks.OnConsumerRegistered != nil {
		b.cfg.Hooks.OnConsumerRegistered(ctx, ConsumerEvent{Consumer: cfg.Name, Group: cfg.Group, Queue: cfg.Queue, State: ConsumerRegistered, Time: time.Now()})
	}
	b.wg.Add(1)
	go s.loop()
	return s, nil
}

func (s *Subscription) loop() {
	defer func() {
		if s.b.promMetrics != nil && s.batchHandler != nil {
			s.b.promMetrics.RemoveConsumerBatchWait(s)
		}
		if s.routeName != "" {
			s.b.RemoveRoute(s.routeName)
		}
		s.b.subsMu.Lock()
		delete(s.b.subs, s.id)
		s.b.subsMu.Unlock()
		s.b.releaseConsumerAssignments(s.cfg.Name)
		if s.b.cfg.Hooks.OnConsumerStopped != nil {
			s.b.cfg.Hooks.OnConsumerStopped(context.Background(), ConsumerEvent{Consumer: s.cfg.Name, Group: s.cfg.Group, Queue: s.cfg.Queue, State: ConsumerStopped, Time: time.Now()})
		}
		close(s.stopped)
		s.b.wg.Done()
	}()
	t := time.NewTicker(s.b.cfg.PollInterval)
	defer t.Stop()
	// Wake as soon as the queue gains work instead of waiting out the poll
	// interval. The ticker stays as a fallback for state the signal cannot cover:
	// lease expiry, queue resume, and messages promoted out of cold storage.
	var ready <-chan struct{}
	if signaler, ok := s.b.store.(QueueSignaler); ok {
		ready = signaler.QueueReady(s.cfg.Queue)
	}
	// Cleanup dedup map periodically
	var dedupTicker *time.Ticker
	var dedupTick <-chan time.Time
	if s.dedup != nil {
		dedupTicker = time.NewTicker(60 * time.Second)
		dedupTick = dedupTicker.C
		defer dedupTicker.Stop()
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-dedupTick:
			if s.dedup != nil {
				s.dedup.Cleanup()
			}
		case <-ready:
		case <-t.C:
		}
		if ready == nil {
			// The queue may not have existed when the subscription started. Retry
			// the lookup so a later append is not missed; a nil channel simply
			// parks on the ticker until then.
			if signaler, ok := s.b.store.(QueueSignaler); ok {
				ready = signaler.QueueReady(s.cfg.Queue)
			}
		}
		if s.paused.Load() {
			continue
		}
		if s.cfg.MaxInFlight > 0 && int(s.inFlight.Load()) >= s.cfg.MaxInFlight {
			continue
		}
		limit := s.cfg.BatchSize
		if limit <= 0 {
			limit = 1
		}
		if s.cfg.MaxInFlight > 0 {
			available := s.cfg.MaxInFlight - int(s.inFlight.Load())
			if available <= 0 {
				continue
			}
			if limit > available {
				limit = available
			}
		}
		var accept func(Message) bool
		if s.strategy != nil {
			candidates := s.b.QueueCandidates(s.cfg.Queue, s.cfg.Group)
			accept = func(msg Message) bool {
				return s.b.assignConsumer(s.strategy, msg, candidates) == s.cfg.Name
			}
		}
		msgs, err := s.b.store.Lease(s.ctx, LeaseRequest{Queue: s.cfg.Queue, ConsumerID: s.cfg.Name, ConsumerGroup: s.cfg.Group, Limit: limit, VisibilityTimeout: s.cfg.VisibilityTimeout, Accept: accept})
		if err != nil || len(msgs) == 0 {
			continue
		}
		if s.filter != nil && len(msgs) > 0 {
			msgs = s.filterMessages(msgs)
			if len(msgs) == 0 {
				continue
			}
		}
		if s.batchHandler != nil {
			var collectWait time.Duration
			if s.cfg.BatchWait > 0 && len(msgs) < limit {
				collectStarted := time.Now()
				msgs = s.collectBatch(msgs, limit, accept, ready, t.C)
				collectWait = time.Since(collectStarted)
			}
			batch := append([]Message(nil), msgs...)
			s.inFlight.Add(int64(len(batch)))
			batchID := s.id + ":batch"
			if len(batch) > 0 {
				batchID = batch[0].ID + ":batch"
			}
			if err := s.b.pool.SubmitFunc(func(ctx context.Context) error {
				defer s.inFlight.Add(-int64(len(batch)))
				return s.handleBatch(ctx, batch)
			}, pool.JobOptions{ID: batchID, Queue: s.cfg.Queue, Timeout: s.cfg.Timeout, MaxAttempts: 1, ConcurrencyKey: s.batchOrderKey(batch), MaxConcurrentPerKey: s.orderLimit()}); err != nil {
				s.inFlight.Add(-int64(len(batch)))
				s.requeueLeased(batch)
			} else if s.b.promMetrics != nil {
				s.b.promMetrics.RecordConsumerBatch(s, len(batch), collectWait, time.Duration(s.batchWaitNanos.Load()))
			}
			continue
		}
		s.inFlight.Add(int64(len(msgs)))
		for _, m := range msgs {
			m := m
			if err := s.b.pool.SubmitFunc(func(ctx context.Context) error { defer s.inFlight.Add(-1); return s.handle(ctx, m) }, pool.JobOptions{ID: m.ID, Queue: s.cfg.Queue, Priority: pool.Priority(m.Priority), Timeout: s.cfg.Timeout, MaxAttempts: 1, ConcurrencyKey: s.orderKey(m), MaxConcurrentPerKey: s.orderLimit()}); err != nil {
				s.inFlight.Add(-1)
				s.requeueLeased([]Message{m})
			}
		}
	}
}

func (s *Subscription) requeueLeased(messages []Message) {
	for _, msg := range messages {
		err := s.b.store.Nack(s.ctx, NackRequest{
			MessageID: msg.ID, Queue: s.cfg.Queue, ConsumerID: s.cfg.Name,
			LeaseToken: msg.LeaseToken, Action: NackRequeue,
		})
		if err == nil {
			s.b.releaseDispatchAssignment(msg.ID)
		}
	}
}

func (s *Subscription) collectBatch(batch []Message, limit int, accept func(Message) bool, ready <-chan struct{}, poll <-chan time.Time) []Message {
	wait := time.Duration(s.batchWaitNanos.Load())
	if wait <= 0 || wait > s.cfg.BatchWait {
		wait = s.cfg.BatchWait
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	readyCh := ready
	for len(batch) < limit {
		more, err := s.b.store.Lease(s.ctx, LeaseRequest{
			Queue: s.cfg.Queue, ConsumerID: s.cfg.Name, ConsumerGroup: s.cfg.Group,
			Limit: limit - len(batch), VisibilityTimeout: s.cfg.VisibilityTimeout, Accept: accept,
		})
		if err == nil && len(more) > 0 {
			if s.filter != nil {
				more = s.filterMessages(more)
			}
			batch = append(batch, more...)
			continue
		}
		select {
		case <-readyCh:
		case <-poll:
			if signaler, ok := s.b.store.(QueueSignaler); ok {
				readyCh = signaler.QueueReady(s.cfg.Queue)
			}
		case <-timer.C:
			s.adjustBatchWait(false)
			return batch
		case <-s.ctx.Done():
			return batch
		}
	}
	s.adjustBatchWait(true)
	return batch
}

func (s *Subscription) adjustBatchWait(filled bool) {
	current := time.Duration(s.batchWaitNanos.Load())
	if current <= 0 {
		return
	}
	minWait := s.batchWaitFloor
	decreaseDivisor := s.batchWaitDecreaseDivisor
	if decreaseDivisor < 2 {
		decreaseDivisor = 4
	}
	growthDivisor := s.batchWaitGrowthDivisor
	if growthDivisor < 1 {
		growthDivisor = 8
	}
	step := s.cfg.BatchWait / time.Duration(growthDivisor)
	if step <= 0 {
		step = time.Nanosecond
	}
	if filled {
		current -= current / time.Duration(decreaseDivisor)
		if current < minWait {
			current = minWait
		}
	} else {
		if current >= s.cfg.BatchWait {
			current -= current / time.Duration(decreaseDivisor)
			if current < minWait {
				current = minWait
			}
		} else {
			current += step
			if current > s.cfg.BatchWait {
				current = s.cfg.BatchWait
			}
		}
	}
	s.batchWaitNanos.Store(current.Nanoseconds())
	if s.b.promMetrics != nil {
		s.b.promMetrics.SetConsumerBatchWait(s, current)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (s *Subscription) orderKey(m Message) string {
	if !s.cfg.Ordered {
		return ""
	}
	switch s.cfg.OrderBy {
	case OrderTenant:
		return m.TenantID
	case OrderPartition:
		return fmt.Sprint(m.Partition)
	case OrderGlobal:
		return s.cfg.Queue
	default:
		return m.Key
	}
}
func (s *Subscription) orderLimit() int {
	if s.cfg.Ordered {
		return 1
	}
	return 0
}

func (s *Subscription) batchOrderKey(batch []Message) string {
	if len(batch) == 0 || !s.cfg.Ordered {
		return ""
	}
	return s.orderKey(batch[0])
}

func (s *Subscription) handleBatch(ctx context.Context, msgs []Message) (err error) {
	deliveries := make([]Delivery, 0, len(msgs))
	handles := make([]*ackHandle, 0, len(msgs))
	activeMessages := make([]Message, 0, len(msgs))
	for i, m := range msgs {
		if m.TaskID == "" && m.Headers != nil {
			m.TaskID = m.Headers["x-task-id"]
			m.TaskName = m.Headers["x-task-name"]
			msgs[i] = m
		}
		m.Status = MessageProcessing
		if err := s.b.store.Update(ctx, m); err != nil {
			s.requeueLeased(msgs)
			return fmt.Errorf("mark message %s processing: %w", m.ID, err)
		}
		h := &ackHandle{b: s.b, msg: m, queue: s.cfg.Queue, consumer: s.cfg.Name}
		if m.TaskID != "" {
			run, err := s.b.startTask(ctx, m.TaskID)
			if err != nil {
				s.requeueLeased(msgs)
				return fmt.Errorf("start task %s: %w", m.TaskID, err)
			}
			if !run {
				if err := h.Ack(ctx); err != nil {
					s.requeueLeased(msgs)
					return fmt.Errorf("ack cancelled task message %s: %w", m.ID, err)
				}
				continue
			}
		}
		handles = append(handles, h)
		activeMessages = append(activeMessages, m)
		deliveries = append(deliveries, Delivery{Message: m, ack: h})
	}
	msgs = activeMessages
	if len(msgs) == 0 {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			var settlementErr error
			for i, h := range handles {
				_ = h.RespondError(ctx, panicErr)
				settlementErr = errors.Join(settlementErr, s.fail(ctx, h, msgs[i], panicErr))
			}
			err = errors.Join(panicErr, settlementErr)
		}
	}()
	if s.b.cfg.Hooks.BeforeHandle != nil {
		for _, m := range msgs {
			if hookErr := s.b.cfg.Hooks.BeforeHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()}); hookErr != nil {
				var settlementErr error
				for i, h := range handles {
					settlementErr = errors.Join(settlementErr, s.fail(ctx, h, msgs[i], hookErr))
				}
				return errors.Join(hookErr, settlementErr)
			}
		}
	}
	err = s.batchHandler(ctx, deliveries)
	if err != nil {
		handlerErr := err
		var settlementErr error
		for i, h := range handles {
			_ = h.RespondError(ctx, handlerErr)
			settlementErr = errors.Join(settlementErr, s.fail(ctx, h, msgs[i], handlerErr))
		}
		return errors.Join(handlerErr, settlementErr)
	}
	var ackErr error
	if s.cfg.AckMode == AckAuto {
		for _, h := range handles {
			if err := h.Ack(ctx); err != nil && ackErr == nil {
				ackErr = err
			}
		}
	}
	if s.b.cfg.Hooks.AfterHandle != nil {
		for _, m := range msgs {
			s.b.cfg.Hooks.AfterHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()})
		}
	}
	if ackErr != nil {
		return fmt.Errorf("ack batch after handler success: %w", ackErr)
	}
	return nil
}

func (s *Subscription) handle(ctx context.Context, m Message) (err error) {
	if s.dedup != nil && s.dedup.IsDuplicate(m) {
		if err := s.b.store.Ack(ctx, AckRequest{MessageID: m.ID, Queue: s.cfg.Queue, LeaseToken: m.LeaseToken, ConsumerID: s.cfg.Name}); err != nil {
			return fmt.Errorf("ack duplicate message %s: %w", m.ID, err)
		}
		s.b.releaseDispatchAssignment(m.ID)
		return nil
	}
	m.Status = MessageProcessing
	if err := s.b.store.Update(ctx, m); err != nil {
		s.requeueLeased([]Message{m})
		return fmt.Errorf("mark message %s processing: %w", m.ID, err)
	}
	h := &ackHandle{b: s.b, msg: m, queue: s.cfg.Queue, consumer: s.cfg.Name}
	d := Delivery{Message: m, ack: h}
	if m.TaskID == "" && m.Headers != nil {
		m.TaskID = m.Headers["x-task-id"]
		m.TaskName = m.Headers["x-task-name"]
		h.msg = m
		d.Message = m
	}
	if m.TaskID != "" {
		run, err := s.b.startTask(ctx, m.TaskID)
		if err != nil {
			s.requeueLeased([]Message{m})
			return fmt.Errorf("start task %s: %w", m.TaskID, err)
		}
		if !run {
			if err := h.Ack(ctx); err != nil {
				return fmt.Errorf("ack cancelled task message %s: %w", m.ID, err)
			}
			return nil
		}
	}
	if s.b.cfg.Hooks.BeforeHandle != nil {
		if err := s.b.cfg.Hooks.BeforeHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()}); err != nil {
			return errors.Join(err, s.fail(ctx, h, m, err))
		}
	}
	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			if s.b.cfg.Hooks.OnPanic != nil {
				s.b.cfg.Hooks.OnPanic(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Error: panicErr, Time: time.Now()})
			}
			_ = h.RespondError(ctx, panicErr)
			err = errors.Join(panicErr, s.fail(ctx, h, m, panicErr))
		}
	}()
	err = s.handler(ctx, d)
	if err != nil {
		_ = h.RespondError(ctx, err)
		return errors.Join(err, s.fail(ctx, h, m, err))
	}
	var ackErr error
	if s.cfg.AckMode == AckAuto {
		ackErr = h.Ack(ctx)
	}
	if s.b.cfg.Hooks.AfterHandle != nil {
		s.b.cfg.Hooks.AfterHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()})
	}
	if ackErr != nil {
		return fmt.Errorf("ack message %s after handler success: %w", m.ID, ackErr)
	}
	return nil
}
func (s *Subscription) fail(ctx context.Context, h *ackHandle, m Message, err error) error {
	if s.b.cfg.Hooks.OnHandlerError != nil {
		s.b.cfg.Hooks.OnHandlerError(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Error: err, Time: time.Now()})
	}
	maxAttempts := firstPositive(m.MaxAttempts, s.cfg.RetryPolicy.MaxAttempts, s.b.cfg.DefaultRetry.MaxAttempts)
	if IsPermanent(err) || m.Attempts+1 >= maxAttempts {
		if nackErr := h.Nack(ctx, NackToDLQ(s.cfg.DLQ), NackError(err.Error()), NackReason("retries_exhausted")); nackErr != nil {
			return fmt.Errorf("dead-letter message %s after handler failure: %w", m.ID, nackErr)
		}
		if m.TaskID != "" {
			if taskErr := s.b.failTask(ctx, m.TaskID, err); taskErr != nil {
				return fmt.Errorf("dead-lettered message %s but task failure could not be persisted: %w", m.ID, taskErr)
			}
		}
		if s.b.cfg.Hooks.OnDeadLettered != nil {
			s.b.cfg.Hooks.OnDeadLettered(ctx, DLQEvent{Message: m, Queue: s.cfg.DLQ, Reason: "retries_exhausted", Error: err, Time: time.Now()})
		}
		if s.b.cfg.Hooks.OnMaxRedeliveryExceeded != nil {
			s.b.cfg.Hooks.OnMaxRedeliveryExceeded(ctx, DLQEvent{Message: m, Queue: s.cfg.DLQ, Reason: "max_redelivery_exceeded", Error: err, Time: time.Now()})
		}
		s.b.notify.Publish(Notification{
			Type:      "message.max_redelivery_exceeded",
			Source:    s.cfg.Name,
			Severity:  SeverityWarn,
			MessageID: m.ID,
			Queue:     m.Queue,
			TenantID:  m.TenantID,
			Timestamp: time.Now(),
			Data:      map[string]any{"attempts": m.Attempts, "max_attempts": maxAttempts, "error": err.Error()},
		})
		return nil
	}
	delay := backoffDelay(s.cfg.RetryPolicy, m.Attempts+1)
	if nackErr := h.Nack(ctx, func(o *NackOptions) {
		o.Action = NackRetry
		o.Delay = delay
		o.Error = err.Error()
		o.Reason = "handler_error"
	}); nackErr != nil {
		return fmt.Errorf("schedule retry for message %s after handler failure: %w", m.ID, nackErr)
	}
	if m.TaskID != "" {
		if taskErr := s.b.retryTask(ctx, m.TaskID, err); taskErr != nil {
			return fmt.Errorf("scheduled retry for message %s but task retry state could not be persisted: %w", m.ID, taskErr)
		}
	}
	if s.b.cfg.Hooks.OnRetryScheduled != nil {
		s.b.cfg.Hooks.OnRetryScheduled(ctx, RetryEvent{Message: m, Delay: delay, Error: err, Time: time.Now()})
	}
	return nil
}

func backoffDelay(p RetryPolicy, attempt int) time.Duration {
	if p.Initial <= 0 {
		p.Initial = 50 * time.Millisecond
	}
	if p.Max <= 0 {
		p.Max = 5 * time.Second
	}
	if p.Initial > p.Max {
		p.Initial = p.Max
	}
	if p.Factor <= 0 {
		p.Factor = 2
	}
	if math.IsNaN(p.Factor) || math.IsInf(p.Factor, 0) {
		p.Factor = 2
	}
	if p.Factor < 1 {
		p.Factor = 1
	}
	d := p.Initial
	if attempt < 1 {
		attempt = 1
	}
	for i := 1; i < attempt && d < p.Max; i++ {
		// Compare before converting the product back to time.Duration so very
		// large factors cannot overflow into a negative, immediately-due delay.
		if float64(d) >= float64(p.Max)/p.Factor {
			d = p.Max
			break
		}
		d = time.Duration(float64(d) * p.Factor)
		if d > p.Max {
			d = p.Max
		}
	}
	// Apply full jitter [0, d) to prevent thundering herd.
	if p.Jitter && d > 0 {
		d = time.Duration(rand.Int63n(int64(d)))
	}
	return d
}
func firstPositive(v ...int) int {
	for _, x := range v {
		if x > 0 {
			return x
		}
	}
	return 1
}
func sanitize(s string) string {
	s = strings.NewReplacer("#", "all", "*", "star", "/", "_", " ", "_", ":", "_").Replace(s)
	return s
}

func (s *Subscription) buildCandidate() ConsumerCandidate {
	return ConsumerCandidate{
		ConsumerID:  s.cfg.Name,
		Queue:       s.cfg.Queue,
		Group:       s.cfg.Group,
		Weight:      s.cfg.Priority,
		MaxInFlight: s.cfg.MaxInFlight,
		InFlight:    int(s.inFlight.Load()),
		Healthy:     true,
	}
}

func (b *Broker) sharedConsumerStrategy(cfg ConsumerConfig) ConsumerStrategy {
	key := consumerStrategyKey{queue: cfg.Queue, group: cfg.Group, type_: cfg.Strategy}
	b.dispatchMu.Lock()
	defer b.dispatchMu.Unlock()
	if strategy := b.consumerStrategies[key]; strategy != nil {
		return strategy
	}
	strategyCfg := StrategyConfig{Type: cfg.Strategy}
	if cfg.StrategyConfig != nil {
		strategyCfg = *cfg.StrategyConfig
		if strategyCfg.Type == "" {
			strategyCfg.Type = cfg.Strategy
		}
	}
	strategy := NewStrategy(strategyCfg)
	b.consumerStrategies[key] = strategy
	return strategy
}

// assignConsumer makes strategy selection a broker-level decision and caches
// it by message ID. Every competing subscription therefore observes the same
// owner before any of them mutates the message lease.
func (b *Broker) assignConsumer(strategy ConsumerStrategy, msg Message, candidates []ConsumerCandidate) string {
	b.dispatchMu.Lock()
	defer b.dispatchMu.Unlock()
	if assigned := b.dispatchAssignments[msg.ID]; assigned.ConsumerID != "" {
		for _, candidate := range candidates {
			if candidate.ConsumerID == assigned.ConsumerID {
				return assigned.ConsumerID
			}
		}
		delete(b.dispatchAssignments, msg.ID)
	}
	if len(candidates) == 0 {
		return ""
	}
	b.refreshAssignedCounts(candidates)
	selected, err := strategy.Select(b.ctx, msg, candidates)
	if err != nil || len(selected) == 0 {
		selected = candidates[:1]
	}
	owner := selected[0].ConsumerID
	b.dispatchAssignments[msg.ID] = dispatchAssignment{ConsumerID: owner, Queue: selected[0].Queue, Group: selected[0].Group}
	return owner
}

func (b *Broker) refreshAssignedCounts(candidates []ConsumerCandidate) {
	type candidateKey struct{ queue, group, consumer string }
	indexes := make(map[candidateKey]int, len(candidates))
	for i := range candidates {
		candidates[i].Assigned = 0
		indexes[candidateKey{candidates[i].Queue, candidates[i].Group, candidates[i].ConsumerID}] = i
	}
	for _, assignment := range b.dispatchAssignments {
		key := candidateKey{assignment.Queue, assignment.Group, assignment.ConsumerID}
		if i, ok := indexes[key]; ok {
			candidates[i].Assigned++
		}
	}
}

func (b *Broker) releaseDispatchAssignment(messageID string) {
	b.dispatchMu.Lock()
	delete(b.dispatchAssignments, messageID)
	b.dispatchMu.Unlock()
}

func (b *Broker) releaseConsumerAssignments(consumerID string) {
	b.dispatchMu.Lock()
	for messageID, assigned := range b.dispatchAssignments {
		if assigned.ConsumerID == consumerID {
			delete(b.dispatchAssignments, messageID)
		}
	}
	b.dispatchMu.Unlock()
}

func (s *Subscription) filterByStrategy(msgs []Message) []Message {
	if s.strategy == nil || len(msgs) == 0 {
		return msgs
	}
	candidates := s.b.QueueCandidates(s.cfg.Queue, s.cfg.Group)
	if len(candidates) <= 1 {
		return msgs
	}
	filtered := make([]Message, 0, len(msgs))
	for _, msg := range msgs {
		result, err := s.strategy.Select(s.ctx, msg, candidates)
		if err != nil || len(result) == 0 {
			filtered = append(filtered, msg)
			continue
		}
		for _, c := range result {
			if c.ConsumerID == s.cfg.Name {
				filtered = append(filtered, msg)
				break
			}
		}
	}
	return filtered
}

func (s *Subscription) filterMessages(msgs []Message) []Message {
	if s.filter == nil || len(msgs) == 0 {
		return msgs
	}
	matched := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		if s.filter.Match(m) {
			matched = append(matched, m)
		} else {
			if err := s.b.store.Ack(s.ctx, AckRequest{Queue: s.cfg.Queue, MessageID: m.ID, ConsumerID: s.cfg.Name, LeaseToken: m.LeaseToken}); err == nil {
				s.b.releaseDispatchAssignment(m.ID)
			}
		}
	}
	return matched
}

func (b *Broker) QueueCandidates(queue, group string) []ConsumerCandidate {
	b.subsMu.RLock()
	candidates := make([]ConsumerCandidate, 0, 4)
	for _, sub := range b.subs {
		if sub.cfg.Queue == queue && sub.cfg.Group == group {
			candidates = append(candidates, sub.buildCandidate())
		}
	}
	if len(candidates) == 0 {
		for _, sub := range b.subs {
			if sub.cfg.Queue == queue {
				candidates = append(candidates, sub.buildCandidate())
			}
		}
	}
	b.subsMu.RUnlock()
	if len(candidates) == 0 {
		return candidates
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ConsumerID < candidates[j].ConsumerID
	})
	indexes := make(map[string]int, len(candidates))
	for i := range candidates {
		indexes[candidates[i].Queue+"\x00"+candidates[i].Group+"\x00"+candidates[i].ConsumerID] = i
	}
	b.dispatchMu.Lock()
	for _, assignment := range b.dispatchAssignments {
		if i, ok := indexes[assignment.Queue+"\x00"+assignment.Group+"\x00"+assignment.ConsumerID]; ok {
			candidates[i].Assigned++
		}
	}
	b.dispatchMu.Unlock()
	return candidates
}
