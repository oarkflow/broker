package client

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fw "github.com/oarkflow/fastworker"
	"github.com/oarkflow/fastworker/broker"
)

type Delivery struct {
	Message broker.Message
	client  *Client
}

func (d Delivery) Decode(out any) error          { return json.Unmarshal(d.Message.Payload, out) }
func (d Delivery) Ack(ctx context.Context) error { return d.client.Ack(ctx, d.Message) }
func (d Delivery) Nack(ctx context.Context, opts ...NackOption) error {
	return d.client.Nack(ctx, d.Message, opts...)
}
func (d Delivery) Reject(ctx context.Context, reason string) error {
	return d.client.Reject(ctx, d.Message, reason)
}
func (d Delivery) Extend(ctx context.Context, v time.Duration) error {
	return d.client.Extend(ctx, d.Message, v)
}
func (d Delivery) Progress(ctx context.Context, p broker.Progress) error {
	return d.client.Progress(ctx, d.Message, p)
}
func (d Delivery) Respond(ctx context.Context, response any, ack bool) error {
	return d.client.Respond(ctx, d.Message, response, ack)
}
func (d Delivery) RespondError(ctx context.Context, err error, ack bool) error {
	return d.client.RespondError(ctx, d.Message, err, ack)
}
func (d Delivery) Complete(ctx context.Context, result any) error {
	return d.client.Complete(ctx, d.Message, result)
}
func (d Delivery) Fail(ctx context.Context, err error) error {
	return d.client.Fail(ctx, d.Message, err)
}

type Handler func(context.Context, Delivery) error
type RequestHandler func(context.Context, Delivery) (any, error)
type TaskHandler func(context.Context, Delivery) (any, error)
type Worker struct {
	client          *Client
	pool            *fw.Pool
	ownPool         bool
	mu              sync.RWMutex
	queueHandlers   map[string]Handler
	topicHandlers   map[string]Handler
	patternHandlers map[string]Handler
	taskHandlers    map[string]TaskHandler
	requestHandlers map[string]RequestHandler
	commandHandlers map[string]RequestHandler
	pollQueues      map[string]struct{}
	inflight        chan struct{}
	activeMu        sync.Mutex
	active          map[string]context.CancelFunc
	stopping        atomic.Bool
}

func NewWorker(cfg Config) (*Worker, error) {
	c := New(cfg)
	pool := cfg.Pool
	own := false
	if pool == nil {
		pool = fw.MustNew(fw.Config{MinWorkers: cfg.Concurrency, MaxWorkers: maxInt(cfg.Concurrency*8, cfg.Concurrency), QueueSize: maxInt(cfg.MaxInFlight*8, 1024), RecoverPanics: true})
		own = true
	}
	return &Worker{client: c, pool: pool, ownPool: own, queueHandlers: map[string]Handler{}, topicHandlers: map[string]Handler{}, patternHandlers: map[string]Handler{}, taskHandlers: map[string]TaskHandler{}, requestHandlers: map[string]RequestHandler{}, commandHandlers: map[string]RequestHandler{}, pollQueues: map[string]struct{}{}, inflight: make(chan struct{}, c.cfg.MaxInFlight), active: map[string]context.CancelFunc{}}, nil
}
func (w *Worker) Client() *Client { return w.client }
func (w *Worker) HandleQueue(q string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.queueHandlers[q] = h
	w.pollQueues[q] = struct{}{}
}
func (w *Worker) HandleTopic(t string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.topicHandlers[t] = h
	w.pollQueues["sub."+w.client.cfg.ConsumerID+"."+sanitize(t)] = struct{}{}
}
func (w *Worker) HandlePattern(p string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.patternHandlers[p] = h
	w.pollQueues["sub."+w.client.cfg.ConsumerID+"."+sanitize(p)] = struct{}{}
}
func (w *Worker) HandleTask(n string, h TaskHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.taskHandlers[n] = h
	w.pollQueues["tasks."+n] = struct{}{}
}
func (w *Worker) HandleRequest(s string, h RequestHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.requestHandlers[s] = h
	w.pollQueues["request."+sanitize(s)] = struct{}{}
}
func (w *Worker) HandleCommand(s string, h RequestHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.commandHandlers[s] = h
	w.pollQueues["command."+sanitize(s)] = struct{}{}
}
func (w *Worker) Run(ctx context.Context) error {
	if w.ownPool {
		if err := w.pool.Start(); err != nil {
			return err
		}
	}
	queues, topics, patterns, tasks, requests, commands := w.snapshotNames()
	reg := broker.ConsumerRegistration{ConsumerID: w.client.cfg.ConsumerID, Group: w.client.cfg.Group, Name: w.client.cfg.Name, Version: w.client.cfg.Version, Queues: queues, Topics: topics, Patterns: patterns, TaskTypes: tasks, RequestSubjects: requests, CommandTypes: commands, Concurrency: w.client.cfg.Concurrency, MaxInFlight: w.client.cfg.MaxInFlight, AckMode: broker.AckManual, HeartbeatEveryNS: w.client.cfg.HeartbeatInterval.Nanoseconds(), VisibilityTimeout: w.client.cfg.VisibilityTimeout.Nanoseconds(), Capabilities: []string{"poll", "ack", "nack", "progress", "respond", "task", "request", "command", "dlq", "auto_extend"}}
	if _, err := w.client.Register(ctx, reg); err != nil {
		return err
	}
	for _, q := range queues {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "queue", Name: q, Queue: q, AutoCreate: true})
	}
	for _, t := range topics {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "topic", Name: t, AutoCreate: true})
	}
	for _, p := range patterns {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "pattern", Name: p, Pattern: p, AutoCreate: true})
	}
	for _, t := range tasks {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "task", Name: t, Queue: "tasks." + t, AutoCreate: true})
	}
	for _, r := range requests {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "request", Name: r, Queue: "request." + sanitize(r), AutoCreate: true})
	}
	for _, c := range commands {
		_, _ = w.client.Subscribe(ctx, broker.RemoteSubscription{Type: "command", Name: c, Queue: "command." + sanitize(c), AutoCreate: true})
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.heartbeatLoop(runCtx) }()
	for q := range w.snapshotPollQueues() {
		queue := q
		wg.Add(1)
		go func() { defer wg.Done(); w.pollLoop(runCtx, queue) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); w.eventLoop(runCtx, cancel) }()
	<-runCtx.Done()
	w.stopping.Store(true)
	wg.Wait()
	_ = w.client.doJSON(context.Background(), "POST", "/v1/consumers/"+w.client.cfg.ConsumerID+"/unregister", nil, nil)
	if w.ownPool {
		_ = w.pool.Shutdown(context.Background())
	}
	if w.client.cfg.Hooks.OnStopped != nil {
		w.client.cfg.Hooks.OnStopped(Event{ConsumerID: w.client.cfg.ConsumerID, Group: w.client.cfg.Group, State: broker.ConsumerStopped, Time: time.Now()})
	}
	return ctx.Err()
}
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.client.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_, _ = w.client.Heartbeat(ctx)
		case <-ctx.Done():
			return
		}
	}
}
func (w *Worker) eventLoop(ctx context.Context, cancel context.CancelFunc) {
	var after string
	for {
		evs, err := w.client.Events(ctx, after, w.client.cfg.HeartbeatInterval)
		if err == nil {
			for _, e := range evs {
				after = e.ID
				if w.client.cfg.Hooks.OnNotification != nil {
					w.client.cfg.Hooks.OnNotification(NotificationEvent{Notification: e, Time: time.Now()})
				}
				if e.Type == "task.cancelled" {
					w.cancelMessage(firstNonEmpty(e.MessageID, asString(e.Data["message_id"])))
				}
				if e.Type == "consumer.draining" || e.Type == "broker.stopping" || e.Type == "broker.draining" {
					if w.client.cfg.Hooks.OnDrain != nil {
						w.client.cfg.Hooks.OnDrain(Event{ConsumerID: w.client.cfg.ConsumerID, Group: w.client.cfg.Group, State: broker.ConsumerDraining, Time: time.Now()})
					}
					cancel()
					return
				}
			}
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}
func (w *Worker) pollLoop(ctx context.Context, queue string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := w.client.Poll(ctx, queue, w.client.cfg.PollLimit)
		if err != nil {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		for _, m := range msgs {
			m := m
			select {
			case w.inflight <- struct{}{}:
			case <-ctx.Done():
				return
			}
			if w.client.cfg.Hooks.OnMessageReceived != nil {
				w.client.cfg.Hooks.OnMessageReceived(MessageEvent{Message: m, Queue: queue, Time: time.Now()})
			}
			if err := w.pool.SubmitFunc(func(jobCtx context.Context) error {
				defer func() { <-w.inflight }()
				return w.handle(jobCtx, m)
			}, fw.JobOptions{ID: m.ID, Queue: queue, Priority: fw.Priority(m.Priority), MaxAttempts: 1}); err != nil {
				<-w.inflight
			}
		}
	}
}
func (w *Worker) handle(ctx context.Context, m broker.Message) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.trackMessage(m.ID, cancel)
	defer w.untrackMessage(m.ID)
	stopExtend := w.startLeaseExtender(ctx, m)
	defer stopExtend()
	d := Delivery{Message: m, client: w.client}
	if w.client.cfg.Hooks.BeforeHandle != nil {
		if err := w.client.cfg.Hooks.BeforeHandle(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()}); err != nil {
			_ = d.Nack(ctx, NackError(err.Error()), NackReason("before_handle"))
			return err
		}
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			_ = d.RespondError(ctx, err, false)
			_ = w.nackOrDLQ(ctx, d, err)
			if w.client.cfg.Hooks.OnHandlerError != nil {
				w.client.cfg.Hooks.OnHandlerError(MessageEvent{Message: m, Queue: m.Queue, Error: err, Time: time.Now()})
			}
		}
	}()
	h, kind := w.findHandler(m)
	if h == nil {
		err = fmt.Errorf("no remote handler for message type=%s topic=%s queue=%s", m.Type, m.Topic, m.Queue)
		_ = w.nackOrDLQ(ctx, d, err)
		return err
	}
	switch kind {
	case "request":
		res, e := h.(RequestHandler)(ctx, d)
		if e != nil {
			_ = d.RespondError(ctx, e, false)
			err = e
		} else {
			_ = d.Respond(ctx, res, false)
		}
	case "task":
		res, e := h.(TaskHandler)(ctx, d)
		if e != nil {
			err = e
		} else {
			if err = d.Complete(ctx, res); err != nil {
				return err
			}
			if w.client.cfg.Hooks.AfterHandle != nil {
				w.client.cfg.Hooks.AfterHandle(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
			}
			return nil
		}
	default:
		err = h.(Handler)(ctx, d)
	}
	if err != nil {
		_ = w.nackOrDLQ(ctx, d, err)
		if w.client.cfg.Hooks.OnHandlerError != nil {
			w.client.cfg.Hooks.OnHandlerError(MessageEvent{Message: m, Queue: m.Queue, Error: err, Time: time.Now()})
		}
		return err
	}
	if err := d.Ack(ctx); err != nil {
		return err
	}
	if w.client.cfg.Hooks.AfterHandle != nil {
		w.client.cfg.Hooks.AfterHandle(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
	}
	return nil
}
func (w *Worker) nackOrDLQ(ctx context.Context, d Delivery, err error) error {
	max := d.Message.MaxAttempts
	if max <= 0 {
		max = 3
	}
	if d.Message.Attempts+1 >= max {
		return d.Reject(ctx, err.Error())
	}
	return d.Nack(ctx, NackError(err.Error()), NackReason("handler_error"), NackDelay(time.Second*time.Duration(d.Message.Attempts+1)))
}
func (w *Worker) findHandler(m broker.Message) (any, string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if h := w.taskHandlers[firstNonEmpty(m.TaskName, m.Type, m.Headers["x-task-name"])]; h != nil {
		return h, "task"
	}
	if m.CorrelationID != "" {
		if h := w.commandHandlers[m.Topic]; h != nil {
			return h, "request"
		}
		if h := w.commandHandlers[m.Type]; h != nil {
			return h, "request"
		}
		if h := w.requestHandlers[m.Topic]; h != nil {
			return h, "request"
		}
		if h := w.requestHandlers[m.Type]; h != nil {
			return h, "request"
		}
	}
	if h := w.queueHandlers[m.Queue]; h != nil {
		return h, "handler"
	}
	if h := w.topicHandlers[m.Topic]; h != nil {
		return h, "handler"
	}
	for p, h := range w.patternHandlers {
		if broker.TopicMatch(p, m.Topic) || broker.TopicMatch(p, m.RoutingKey) {
			return h, "handler"
		}
	}
	return nil, ""
}
func (w *Worker) snapshotNames() (queues, topics, patterns, tasks, requests, commands []string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for k := range w.queueHandlers {
		queues = append(queues, k)
	}
	for k := range w.topicHandlers {
		topics = append(topics, k)
	}
	for k := range w.patternHandlers {
		patterns = append(patterns, k)
	}
	for k := range w.taskHandlers {
		tasks = append(tasks, k)
	}
	for k := range w.requestHandlers {
		requests = append(requests, k)
	}
	for k := range w.commandHandlers {
		commands = append(commands, k)
	}
	return
}
func (w *Worker) snapshotPollQueues() map[string]struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := map[string]struct{}{}
	for k := range w.pollQueues {
		out[k] = struct{}{}
	}
	return out
}

func (w *Worker) trackMessage(id string, cancel context.CancelFunc) {
	if id == "" || cancel == nil {
		return
	}
	w.activeMu.Lock()
	w.active[id] = cancel
	w.activeMu.Unlock()
}
func (w *Worker) untrackMessage(id string) {
	if id == "" {
		return
	}
	w.activeMu.Lock()
	delete(w.active, id)
	w.activeMu.Unlock()
}
func (w *Worker) cancelMessage(id string) {
	if id == "" {
		return
	}
	w.activeMu.Lock()
	cancel := w.active[id]
	w.activeMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func (w *Worker) startLeaseExtender(ctx context.Context, m broker.Message) func() {
	if w.client.cfg.DisableAutoExtendLeases || m.LeaseToken == "" {
		return func() {}
	}
	interval := w.client.cfg.LeaseExtensionInterval
	if interval <= 0 {
		interval = w.client.cfg.VisibilityTimeout / 2
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		extended := 0
		for {
			select {
			case <-t.C:
				if w.client.cfg.MaxLeaseExtensions > 0 && extended >= w.client.cfg.MaxLeaseExtensions {
					return
				}
				_ = w.client.Extend(ctx, m, w.client.cfg.VisibilityTimeout)
				extended++
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() { close(stop) }
}
func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func sanitize(s string) string {
	return strings.NewReplacer("#", "all", "*", "star", "/", "_", " ", "_", ":", "_").Replace(s)
}
func firstNonEmpty(v ...string) string {
	for _, x := range v {
		if x != "" {
			return x
		}
	}
	return ""
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
