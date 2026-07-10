package broker

import (
	"context"
	"fmt"
	"time"
)

type Publisher struct{ b *Broker }

func (p *Publisher) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	return p.publish(ctx, topic, payload, opts...)
}
func (p *Publisher) PublishConfirm(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	opts = append(opts, WithConfirmLevel(ConfirmPersisted))
	return p.publish(ctx, topic, payload, opts...)
}
func (p *Publisher) PublishAsync(ctx context.Context, topic string, payload any, opts ...PublishOption) PublishFuture {
	f := newFutureResult[PublishResult](p.b.nextID("pubf"))
	cctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				f.complete(PublishResult{}, fmt.Errorf("publish panic: %v", r))
			}
		}()
		r, err := p.publish(cctx, topic, payload, opts...)
		f.complete(r, err)
	}()
	return f
}
func (p *Publisher) PublishBatch(ctx context.Context, topic string, payloads []any, opts ...PublishOption) (BatchPublishResult, error) {
	res := BatchPublishResult{Results: make([]PublishResult, 0, len(payloads))}
	var first error
	for _, payload := range payloads {
		r, err := p.publish(ctx, topic, payload, opts...)
		if err != nil {
			if first == nil {
				first = err
			}
			res.Errors = append(res.Errors, err.Error())
		} else {
			res.Results = append(res.Results, r)
		}
	}
	return res, first
}
func (p *Publisher) PublishBatchAsync(ctx context.Context, topic string, payloads []any, opts ...PublishOption) BatchPublishFuture {
	f := newFutureResult[BatchPublishResult](p.b.nextID("batchf"))
	cctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				f.complete(BatchPublishResult{}, fmt.Errorf("batch publish panic: %v", r))
			}
		}()
		r, err := p.PublishBatch(cctx, topic, payloads, opts...)
		f.complete(r, err)
	}()
	return f
}

func (p *Publisher) Broadcast(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	return p.publish(ctx, topic, payload, opts...)
}
func (p *Publisher) Command(ctx context.Context, command string, request any, response any, opts ...RequestOption) error {
	return p.Request(ctx, command, request, response, opts...)
}

func (p *Publisher) PublishAfter(ctx context.Context, topic string, payload any, delay time.Duration, opts ...PublishOption) (ScheduleResult, error) {
	opts = append(opts, WithDelay(delay))
	r, err := p.publish(ctx, topic, payload, opts...)
	if err != nil {
		return ScheduleResult{}, err
	}
	return ScheduleResult{MessageID: r.MessageID, ScheduleID: r.MessageID, RunAt: time.Now().Add(delay).Format(time.RFC3339Nano)}, nil
}
func (p *Publisher) PublishAt(ctx context.Context, topic string, payload any, at time.Time, opts ...PublishOption) (ScheduleResult, error) {
	opts = append(opts, WithRunAt(at))
	r, err := p.publish(ctx, topic, payload, opts...)
	if err != nil {
		return ScheduleResult{}, err
	}
	return ScheduleResult{MessageID: r.MessageID, ScheduleID: r.MessageID, RunAt: at.Format(time.RFC3339Nano)}, nil
}

type preparedPublish struct {
	msg   Message
	dests []Destination
	event PublishEvent
	opts  PublishOptions
}

func (p *Publisher) publish(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	prep, err := p.preparePublish(ctx, topic, payload, opts...)
	if err != nil {
		return PublishResult{}, err
	}
	stored, err := p.b.store.Append(ctx, prep.msg, prep.dests)
	if err != nil {
		if p.b.cfg.Hooks.OnPublishFailed != nil {
			prep.event.Error = err
			p.b.cfg.Hooks.OnPublishFailed(ctx, prep.event)
		}
		p.b.recordPublisher(ctx, PublisherRecord{ID: prep.msg.ID, Operation: "publish", Topic: prep.msg.Topic, Queue: prep.msg.Queue, Stream: prep.msg.Stream, MessageID: prep.msg.ID, Status: "failed", Error: err.Error(), CreatedAt: prep.msg.CreatedAt, UpdatedAt: time.Now()})
		return PublishResult{}, err
	}
	res := PublishResult{MessageID: prep.msg.ID, Messages: stored, Destinations: prep.dests, Confirmed: firstConfirm(prep.opts.ConfirmLevel, ConfirmPersisted)}
	p.afterPublish(ctx, prep, res)
	return res, nil
}

func (p *Publisher) preparePublish(ctx context.Context, topic string, payload any, opts ...PublishOption) (preparedPublish, error) {
	if p == nil || p.b == nil {
		return preparedPublish{}, ErrClosed
	}
	if p.b.State() == BrokerPaused {
		return preparedPublish{}, ErrPaused
	}
	var o PublishOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	body, err := p.b.codec.Marshal(payload)
	if err != nil {
		return preparedPublish{}, err
	}
	if len(body) > p.b.cfg.MaxMessageSize {
		return preparedPublish{}, ErrMessageTooLarge
	}
	now := time.Now()
	id := o.MessageID
	if id == "" {
		id = p.b.nextID("msg")
	}
	available := now
	if o.Delay > 0 {
		available = now.Add(o.Delay)
	}
	if !o.RunAt.IsZero() && o.RunAt.After(available) {
		available = o.RunAt
	}
	status := MessageAccepted
	if available.After(now) {
		status = MessageScheduled
	}
	meta := cloneMap(o.Metadata)
	if o.DeduplicationWindow > 0 {
		if meta == nil {
			meta = map[string]string{}
		}
		meta["_dedup_window_ns"] = fmt.Sprintf("%d", o.DeduplicationWindow.Nanoseconds())
	}
	msg := Message{ID: id, Topic: topic, Queue: o.Queue, Stream: o.Stream, Key: o.Key, RoutingKey: o.RoutingKey, Type: o.Type, ContentType: firstNonEmpty(o.ContentType, p.b.codec.ContentType()), Schema: o.Schema, SchemaVersion: o.SchemaVersion, Payload: body, Headers: cloneMap(o.Headers), Metadata: meta, TraceID: o.TraceID, CorrelationID: o.CorrelationID, CausationID: o.CausationID, IdempotencyKey: o.IdempotencyKey, DeduplicationID: o.DeduplicationID, Priority: o.Priority, CreatedAt: now, AvailableAt: available, TenantID: o.TenantID, ProducerID: o.ProducerID, Status: status, MaxAttempts: o.MaxAttempts}
	if msg.Headers != nil {
		msg.TaskID = msg.Headers["x-task-id"]
		msg.TaskName = msg.Headers["x-task-name"]
	}
	if o.TTL > 0 {
		msg.ExpiresAt = now.Add(o.TTL)
	}
	if msg.MaxAttempts <= 0 {
		msg.MaxAttempts = p.b.cfg.DefaultRetry.MaxAttempts
	}
	if o.Strategy != "" {
		if msg.Headers == nil {
			msg.Headers = map[string]string{}
		}
		msg.Headers["x-routing-strategy"] = string(o.Strategy)
	}
	if p.b.cfg.SchemaRegistry != nil {
		if err := p.b.cfg.SchemaRegistry.Validate(ctx, msg); err != nil {
			return preparedPublish{}, err
		}
	}
	ev := PublishEvent{Message: msg, Time: now}
	if p.b.cfg.Hooks.BeforePublish != nil {
		if err := p.b.cfg.Hooks.BeforePublish(ctx, ev); err != nil {
			if p.b.cfg.Hooks.OnPublishRejected != nil {
				ev.Error = err
				p.b.cfg.Hooks.OnPublishRejected(ctx, ev)
			}
			return preparedPublish{}, err
		}
	}
	dests := make([]Destination, 0, 4)
	if o.Queue != "" {
		dests = append(dests, Destination{Kind: DestinationQueue, Name: o.Queue})
	}
	if o.Stream != "" {
		dests = append(dests, Destination{Kind: DestinationStream, Name: o.Stream})
	}
	if o.Strategy != "" {
		routed, routes := p.b.router.Route(ctx, msg)
		_ = routes
		dests = append(dests, routed...)
	} else {
		rd, routes := p.b.router.Route(ctx, msg)
		_ = routes
		dests = append(dests, rd...)
	}
	if len(dests) == 0 && topic != "" {
		dests = append(dests, Destination{Kind: DestinationTopic, Name: topic})
	}
	if o.RequireDestination && len(dests) == 0 {
		return preparedPublish{}, ErrNoDestination
	}
	return preparedPublish{msg: msg, dests: dests, event: ev, opts: o}, nil
}

func (p *Publisher) afterPublish(ctx context.Context, prep preparedPublish, res PublishResult) {
	prep.event.Result = res
	prep.event.Destinations = prep.dests
	if p.b.cfg.Hooks.AfterPublish != nil {
		p.b.cfg.Hooks.AfterPublish(ctx, prep.event)
	}
	if p.b.cfg.Hooks.OnPublishConfirmed != nil {
		p.b.cfg.Hooks.OnPublishConfirmed(ctx, prep.event)
	}
	p.b.notify.Publish(Notification{Type: "message.published", Source: "publisher", Severity: SeverityInfo, MessageID: prep.msg.ID, Topic: prep.msg.Topic, Queue: prep.msg.Queue, TenantID: prep.msg.TenantID, TraceID: prep.msg.TraceID, Timestamp: time.Now()})
	p.b.recordPublisher(ctx, PublisherRecord{ID: prep.msg.ID, Operation: "publish", Topic: prep.msg.Topic, Queue: prep.msg.Queue, Stream: prep.msg.Stream, MessageID: prep.msg.ID, Status: "succeeded", CreatedAt: prep.msg.CreatedAt, UpdatedAt: time.Now()})
}

func firstConfirm(v, d ConfirmLevel) ConfirmLevel {
	if v != "" {
		return v
	}
	return d
}

func (b *Broker) recordPublisher(ctx context.Context, rec PublisherRecord) {
	if b != nil && b.stateStore != nil {
		_ = b.stateStore.PutPublisherRecord(ctx, rec)
	}
}

func (p *Publisher) Request(ctx context.Context, subject string, request any, response any, opts ...RequestOption) error {
	resp, err := p.request(ctx, subject, request, opts...)
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	if response != nil {
		return p.b.codec.Unmarshal(resp.Payload, response)
	}
	return nil
}
func (p *Publisher) RequestAsync(ctx context.Context, subject string, request any, opts ...RequestOption) ResponseFuture {
	var o RequestOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.CorrelationID == "" {
		o.CorrelationID = p.b.nextID("corr")
	}
	f := newFutureResult[Response](p.b.nextID("reqf"))
	f.corr = o.CorrelationID
	cctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	go func() { r, err := p.requestWithOptions(cctx, subject, request, o); f.complete(r, err) }()
	return f
}
func (p *Publisher) RequestStream(ctx context.Context, subject string, request any, opts ...RequestOption) (ResponseStream, error) {
	var o RequestOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.CorrelationID == "" {
		o.CorrelationID = p.b.nextID("corr")
	}
	ch := make(chan Response, 64)
	p.b.registerWaiter(o.CorrelationID, ch)
	_, err := p.Publish(ctx, subject, request, WithCorrelationID(o.CorrelationID), WithHeader("x-request-stream", "true"), WithHeaders(o.Headers), WithTenantID(o.TenantID))
	if err != nil {
		p.b.unregisterWaiter(o.CorrelationID)
		return nil, err
	}
	return &responseStream{ctx: ctx, ch: ch, cancel: func() { p.b.unregisterWaiter(o.CorrelationID) }}, nil
}

func (p *Publisher) request(ctx context.Context, subject string, request any, opts ...RequestOption) (Response, error) {
	var o RequestOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return p.requestWithOptions(ctx, subject, request, o)
}

func (p *Publisher) requestWithOptions(ctx context.Context, subject string, request any, o RequestOptions) (Response, error) {
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	if o.CorrelationID == "" {
		o.CorrelationID = p.b.nextID("corr")
	}
	if o.ReplyTo == "" {
		o.ReplyTo = "_reply." + o.CorrelationID
	}
	ch := make(chan Response, 1)
	p.b.registerWaiter(o.CorrelationID, ch)
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	defer p.b.unregisterWaiter(o.CorrelationID)
	pubOpts := []PublishOption{WithHeaders(o.Headers), WithCorrelationID(o.CorrelationID), WithHeader("reply_to", o.ReplyTo), WithIdempotencyKey(o.IdempotencyKey), WithTenantID(o.TenantID), WithRequireDestination(o.RequireHandler)}
	_, err := p.Publish(ctx, subject, request, pubOpts...)
	if err != nil {
		p.b.recordPublisher(ctx, PublisherRecord{ID: o.CorrelationID, Operation: "request", Subject: subject, CorrelationID: o.CorrelationID, Status: "failed", Error: err.Error(), CreatedAt: time.Now(), UpdatedAt: time.Now()})
		return Response{}, err
	}
	select {
	case r := <-ch:
		p.b.recordPublisher(ctx, PublisherRecord{ID: o.CorrelationID, Operation: "request", Subject: subject, CorrelationID: o.CorrelationID, Status: "succeeded", CreatedAt: time.Now(), UpdatedAt: time.Now()})
		return r, nil
	case <-ctx.Done():
		p.b.recordPublisher(ctx, PublisherRecord{ID: o.CorrelationID, Operation: "request", Subject: subject, CorrelationID: o.CorrelationID, Status: "timeout", Error: ErrRequestTimeout.Error(), CreatedAt: time.Now(), UpdatedAt: time.Now()})
		return Response{CorrelationID: o.CorrelationID, Status: ResponseTimeout, Error: &BrokerError{Code: "request_timeout", Message: ErrRequestTimeout.Error(), Temporary: true, Retryable: true}}, ErrRequestTimeout
	}
}

func (b *Broker) registerWaiter(corr string, ch chan Response) {
	b.waitMu.Lock()
	b.waiters[corr] = ch
	b.waitMu.Unlock()
}
func (b *Broker) unregisterWaiter(corr string) {
	b.waitMu.Lock()
	delete(b.waiters, corr)
	b.waitMu.Unlock()
}
func (b *Broker) completeResponse(resp Response) bool {
	b.waitMu.Lock()
	ch := b.waiters[resp.CorrelationID]
	b.waitMu.Unlock()
	if ch == nil {
		return false
	}
	sent := false
	select {
	case ch <- resp:
		sent = true
	default:
	}
	if resp.Headers != nil && resp.Headers["x-stream-end"] == "true" {
		b.waitMu.Lock()
		if b.waiters[resp.CorrelationID] == ch {
			delete(b.waiters, resp.CorrelationID)
			// Use recover to safely handle double-close if another goroutine
			// already closed the channel between the check and this close.
			func() {
				defer func() { recover() }()
				close(ch)
			}()
		}
		b.waitMu.Unlock()
	}
	return sent
}

type ResponseStream interface {
	Next() bool
	Message() Response
	Err() error
	Close() error
}
type responseStream struct {
	ctx    context.Context
	ch     <-chan Response
	cur    Response
	err    error
	cancel func()
}

func (s *responseStream) Next() bool {
	select {
	case r, ok := <-s.ch:
		if !ok {
			return false
		}
		s.cur = r
		return true
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
		return false
	}
}
func (s *responseStream) Message() Response { return s.cur }
func (s *responseStream) Err() error        { return s.err }
func (s *responseStream) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (p *Publisher) BeginTx(ctx context.Context, opts ...TxOption) (PublishTx, error) {
	return &publishTx{p: p}, nil
}

type PublishTx interface {
	Publish(topic string, payload any, opts ...PublishOption) error
	Commit(context.Context) ([]PublishResult, error)
	Rollback(context.Context) error
}
type txMsg struct {
	topic   string
	payload any
	opts    []PublishOption
}
type publishTx struct {
	p      *Publisher
	closed bool
	msgs   []txMsg
}

func (tx *publishTx) Publish(topic string, payload any, opts ...PublishOption) error {
	if tx.closed {
		return ErrTransactionClosed
	}
	tx.msgs = append(tx.msgs, txMsg{topic: topic, payload: payload, opts: opts})
	return nil
}
func (tx *publishTx) Commit(ctx context.Context) ([]PublishResult, error) {
	if tx.closed {
		return nil, ErrTransactionClosed
	}
	tx.closed = true
	preps := make([]preparedPublish, 0, len(tx.msgs))
	entries := make([]AppendEntry, 0, len(tx.msgs))
	for _, m := range tx.msgs {
		prep, err := tx.p.preparePublish(ctx, m.topic, m.payload, m.opts...)
		if err != nil {
			return nil, err
		}
		preps = append(preps, prep)
		entries = append(entries, AppendEntry{Message: prep.msg, Destinations: prep.dests})
	}
	atomicStore, ok := tx.p.b.store.(AtomicStore)
	if !ok {
		return nil, errf(ErrInvalidMessage, "store does not support atomic append")
	}
	stored, err := atomicStore.AppendAtomic(ctx, entries)
	if err != nil {
		for _, prep := range preps {
			if tx.p.b.cfg.Hooks.OnPublishFailed != nil {
				prep.event.Error = err
				tx.p.b.cfg.Hooks.OnPublishFailed(ctx, prep.event)
			}
		}
		return nil, err
	}
	results := make([]PublishResult, 0, len(preps))
	pos := 0
	for _, prep := range preps {
		n := len(prep.dests)
		if n == 0 {
			n = 1
		}
		end := pos + n
		if end > len(stored) {
			end = len(stored)
		}
		res := PublishResult{MessageID: prep.msg.ID, Messages: append([]Message(nil), stored[pos:end]...), Destinations: prep.dests, Confirmed: firstConfirm(prep.opts.ConfirmLevel, ConfirmCommitted)}
		tx.p.afterPublish(ctx, prep, res)
		results = append(results, res)
		pos = end
	}
	return results, nil
}
func (tx *publishTx) Rollback(ctx context.Context) error { tx.closed = true; tx.msgs = nil; return nil }
