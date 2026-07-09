package broker

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	fastworker "github.com/oarkflow/fastworker"
)

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
	b        *Broker
	msg      Message
	queue    string
	consumer string
	once     atomic.Bool
}

func (a *ackHandle) Ack(ctx context.Context) error {
	if !a.once.CompareAndSwap(false, true) {
		return nil
	}
	err := a.b.store.Ack(ctx, AckRequest{MessageID: a.msg.ID, Queue: a.queue, LeaseToken: a.msg.LeaseToken, ConsumerID: a.consumer})
	if err == nil {
		if a.b.cfg.Hooks.OnAck != nil {
			a.b.cfg.Hooks.OnAck(ctx, MessageEvent{Message: a.msg, Consumer: a.consumer, Queue: a.queue, Time: time.Now()})
		}
		a.b.notify.Publish(Notification{Type: "message.acked", Source: a.consumer, Severity: SeverityInfo, MessageID: a.msg.ID, Queue: a.queue, Timestamp: time.Now()})
		a.b.completeTask(ctx, a.msg, nil, nil)
	}
	return err
}
func (a *ackHandle) Nack(ctx context.Context, opts ...NackOption) error {
	var o NackOptions
	o.Action = NackRequeue
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if !a.once.CompareAndSwap(false, true) {
		return nil
	}
	return a.b.store.Nack(ctx, NackRequest{MessageID: a.msg.ID, Queue: a.queue, LeaseToken: a.msg.LeaseToken, ConsumerID: a.consumer, Action: o.Action, Delay: o.Delay, DLQ: o.DLQ, Reason: o.Reason, Error: o.Error, IncrementAttempt: true})
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
	_ = a.b.store.Update(ctx, m)
	a.b.notify.Publish(Notification{Type: "message.progress", Source: a.consumer, Severity: SeverityInfo, MessageID: m.ID, TaskID: m.TaskID, Queue: a.queue, Timestamp: time.Now(), Data: map[string]any{"progress": p}})
	if m.TaskID != "" {
		a.b.updateTaskProgress(ctx, m.TaskID, p)
	}
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
	if o.EndStream {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers["x-stream-end"] = "true"
	}
	resp := Response{MessageID: a.b.nextID("resp"), CorrelationID: a.msg.CorrelationID, Payload: body, Headers: o.Headers, Status: o.Status}
	a.b.completeResponse(resp)
	a.b.completeTask(ctx, a.msg, body, nil)
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
	a.b.completeTask(ctx, a.msg, nil, err)
	return nil
}

type Subscription struct {
	id           string
	b            *Broker
	cfg          ConsumerConfig
	handler      Handler
	batchHandler BatchHandler
	ctx          context.Context
	cancel       context.CancelFunc
	stopped      chan struct{}
	paused       atomic.Bool
	inFlight     atomic.Int64
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
		cfg.BatchSize = 32
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
	if cfg.RetryPolicy.MaxAttempts <= 0 {
		cfg.RetryPolicy = b.cfg.DefaultRetry
	}
	if cfg.AckMode == "" {
		cfg.AckMode = AckAuto
	}
	if cfg.Queue == "" && (cfg.Topic != "" || cfg.Pattern != "") {
		pattern := firstNonEmpty(cfg.Pattern, cfg.Topic)
		cfg.Queue = "sub." + cfg.Group + "." + sanitize(pattern)
		_ = b.DeclareQueue(ctx, QueueConfig{Name: cfg.Queue, VisibilityTimeout: cfg.VisibilityTimeout, RetryPolicy: cfg.RetryPolicy, DLQ: firstNonEmpty(cfg.DLQ, cfg.Queue+".dlq")})
		b.AddRoute(Route{Name: "subscription." + cfg.Name, Mode: RouteTopic, Match: pattern, To: []Destination{{Kind: DestinationQueue, Name: cfg.Queue}}, Enabled: true})
	}
	if cfg.Queue == "" {
		cfg.Queue = b.cfg.DefaultQueue
	}
	if cfg.DLQ == "" {
		cfg.DLQ = cfg.Queue + ".dlq"
	}
	if err := b.DeclareQueue(ctx, QueueConfig{Name: cfg.Queue, Workers: cfg.Concurrency, VisibilityTimeout: cfg.VisibilityTimeout, RetryPolicy: cfg.RetryPolicy, DLQ: cfg.DLQ}); err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(b.ctx)
	s := &Subscription{id: b.nextID("sub"), b: b, cfg: cfg, handler: handler, batchHandler: batchHandler, ctx: cctx, cancel: cancel, stopped: make(chan struct{})}
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
		s.b.subsMu.Lock()
		delete(s.b.subs, s.id)
		s.b.subsMu.Unlock()
		if s.b.cfg.Hooks.OnConsumerStopped != nil {
			s.b.cfg.Hooks.OnConsumerStopped(context.Background(), ConsumerEvent{Consumer: s.cfg.Name, Group: s.cfg.Group, Queue: s.cfg.Queue, State: ConsumerStopped, Time: time.Now()})
		}
		close(s.stopped)
		s.b.wg.Done()
	}()
	t := time.NewTicker(s.b.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if s.paused.Load() {
				continue
			}
			if s.cfg.MaxInFlight > 0 && int(s.inFlight.Load()) >= s.cfg.MaxInFlight {
				continue
			}
			n := s.cfg.BatchSize
			if n <= 0 {
				n = 1
			}
			msgs, err := s.b.store.Lease(s.ctx, LeaseRequest{Queue: s.cfg.Queue, ConsumerID: s.cfg.Name, ConsumerGroup: s.cfg.Group, Limit: n, VisibilityTimeout: s.cfg.VisibilityTimeout})
			if err != nil || len(msgs) == 0 {
				continue
			}
			if s.batchHandler != nil {
				batch := append([]Message(nil), msgs...)
				s.inFlight.Add(int64(len(batch)))
				batchID := s.id + ":batch"
				if len(batch) > 0 {
					batchID = batch[0].ID + ":batch"
				}
				_ = s.b.pool.SubmitFunc(func(ctx context.Context) error {
					defer s.inFlight.Add(-int64(len(batch)))
					return s.handleBatch(ctx, batch)
				}, fastworker.JobOptions{ID: batchID, Queue: s.cfg.Queue, Timeout: s.cfg.Timeout, MaxAttempts: 1, ConcurrencyKey: s.batchOrderKey(batch), MaxConcurrentPerKey: s.orderLimit()})
				continue
			}
			for _, m := range msgs {
				m := m
				s.inFlight.Add(1)
				_ = s.b.pool.SubmitFunc(func(ctx context.Context) error { defer s.inFlight.Add(-1); return s.handle(ctx, m) }, fastworker.JobOptions{ID: m.ID, Queue: s.cfg.Queue, Priority: fastworker.Priority(m.Priority), Timeout: s.cfg.Timeout, MaxAttempts: 1, ConcurrencyKey: s.orderKey(m), MaxConcurrentPerKey: s.orderLimit()})
			}
		}
	}
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
	for _, m := range msgs {
		m.Status = MessageProcessing
		_ = s.b.store.Update(ctx, m)
		h := &ackHandle{b: s.b, msg: m, queue: s.cfg.Queue, consumer: s.cfg.Name}
		handles = append(handles, h)
		deliveries = append(deliveries, Delivery{Message: m, ack: h})
		if m.TaskID != "" {
			s.b.startTask(ctx, m.TaskID)
		}
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			for i, h := range handles {
				_ = h.RespondError(ctx, err)
				s.fail(ctx, h, msgs[i], err)
			}
		}
	}()
	if s.b.cfg.Hooks.BeforeHandle != nil {
		for _, m := range msgs {
			if hookErr := s.b.cfg.Hooks.BeforeHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()}); hookErr != nil {
				for i, h := range handles {
					_ = h.Nack(ctx, NackError(hookErr.Error()), NackReason("before_handle"))
					if msgs[i].TaskID != "" {
						s.b.failTask(ctx, msgs[i].TaskID, hookErr)
					}
				}
				return hookErr
			}
		}
	}
	err = s.batchHandler(ctx, deliveries)
	if err != nil {
		for i, h := range handles {
			_ = h.RespondError(ctx, err)
			s.fail(ctx, h, msgs[i], err)
		}
		return err
	}
	if s.cfg.AckMode == AckAuto {
		for _, h := range handles {
			_ = h.Ack(ctx)
		}
	}
	if s.b.cfg.Hooks.AfterHandle != nil {
		for _, m := range msgs {
			s.b.cfg.Hooks.AfterHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()})
		}
	}
	return nil
}

func (s *Subscription) handle(ctx context.Context, m Message) (err error) {
	m.Status = MessageProcessing
	_ = s.b.store.Update(ctx, m)
	h := &ackHandle{b: s.b, msg: m, queue: s.cfg.Queue, consumer: s.cfg.Name}
	d := Delivery{Message: m, ack: h}
	if s.b.cfg.Hooks.BeforeHandle != nil {
		if err := s.b.cfg.Hooks.BeforeHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()}); err != nil {
			_ = h.Nack(ctx, NackError(err.Error()), NackReason("before_handle"))
			return err
		}
	}
	if m.TaskID == "" && m.Headers != nil {
		m.TaskID = m.Headers["x-task-id"]
		m.TaskName = m.Headers["x-task-name"]
	}
	if m.TaskID == "" && m.Headers != nil {
		m.TaskID = m.Headers["x-task-id"]
		m.TaskName = m.Headers["x-task-name"]
	}
	if m.TaskID != "" {
		s.b.startTask(ctx, m.TaskID)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			if s.b.cfg.Hooks.OnPanic != nil {
				s.b.cfg.Hooks.OnPanic(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Error: err, Time: time.Now()})
			}
			_ = h.RespondError(ctx, err)
			s.fail(ctx, h, m, err)
		}
	}()
	err = s.handler(ctx, d)
	if err != nil {
		_ = h.RespondError(ctx, err)
		s.fail(ctx, h, m, err)
		return err
	}
	if s.cfg.AckMode == AckAuto {
		_ = h.Ack(ctx)
	}
	if s.b.cfg.Hooks.AfterHandle != nil {
		s.b.cfg.Hooks.AfterHandle(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Time: time.Now()})
	}
	return nil
}
func (s *Subscription) fail(ctx context.Context, h *ackHandle, m Message, err error) {
	if s.b.cfg.Hooks.OnHandlerError != nil {
		s.b.cfg.Hooks.OnHandlerError(ctx, MessageEvent{Message: m, Consumer: s.cfg.Name, Queue: s.cfg.Queue, Error: err, Time: time.Now()})
	}
	if IsPermanent(err) || m.Attempts+1 >= firstPositive(m.MaxAttempts, s.cfg.RetryPolicy.MaxAttempts, s.b.cfg.DefaultRetry.MaxAttempts) {
		_ = h.Nack(ctx, NackToDLQ(s.cfg.DLQ), NackError(err.Error()), NackReason("retries_exhausted"))
		if s.b.cfg.Hooks.OnDeadLettered != nil {
			s.b.cfg.Hooks.OnDeadLettered(ctx, DLQEvent{Message: m, Queue: s.cfg.DLQ, Reason: "retries_exhausted", Error: err, Time: time.Now()})
		}
		if m.TaskID != "" {
			s.b.failTask(ctx, m.TaskID, err)
		}
		return
	}
	delay := backoffDelay(s.cfg.RetryPolicy, m.Attempts+1)
	if s.b.cfg.Hooks.OnRetryScheduled != nil {
		s.b.cfg.Hooks.OnRetryScheduled(ctx, RetryEvent{Message: m, Delay: delay, Error: err, Time: time.Now()})
	}
	_ = h.Nack(ctx, func(o *NackOptions) {
		o.Action = NackRetry
		o.Delay = delay
		o.Error = err.Error()
		o.Reason = "handler_error"
	})
	if m.TaskID != "" {
		s.b.retryTask(ctx, m.TaskID, err)
	}
}

func backoffDelay(p RetryPolicy, attempt int) time.Duration {
	if p.Initial <= 0 {
		p.Initial = 50 * time.Millisecond
	}
	if p.Max <= 0 {
		p.Max = 5 * time.Second
	}
	if p.Factor <= 0 {
		p.Factor = 2
	}
	d := p.Initial
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Factor)
		if d > p.Max {
			d = p.Max
			break
		}
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
