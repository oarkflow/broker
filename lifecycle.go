package fastworker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EventType describes a lifecycle transition emitted by the pool. The values
// are stable strings so users can log, audit, filter and expose them directly.
type EventType string

const (
	EventPoolCreated     EventType = "pool.created"
	EventPoolStarting    EventType = "pool.starting"
	EventPoolStarted     EventType = "pool.started"
	EventPoolPausing     EventType = "pool.pausing"
	EventPoolPaused      EventType = "pool.paused"
	EventPoolResuming    EventType = "pool.resuming"
	EventPoolResumed     EventType = "pool.resumed"
	EventPoolStopping    EventType = "pool.stopping"
	EventPoolStopped     EventType = "pool.stopped"
	EventPoolTerminating EventType = "pool.terminating"
	EventPoolTerminated  EventType = "pool.terminated"
	EventDrainStarted    EventType = "drain.started"
	EventDrainCompleted  EventType = "drain.completed"

	EventQueuePaused  EventType = "queue.paused"
	EventQueueResumed EventType = "queue.resumed"
	EventQueueFull    EventType = "queue.full"
	EventQueueSpill   EventType = "queue.spill"

	EventWorkerStarting EventType = "worker.starting"
	EventWorkerStarted  EventType = "worker.started"
	EventWorkerStopping EventType = "worker.stopping"
	EventWorkerStopped  EventType = "worker.stopped"
	EventWorkerIdle     EventType = "worker.idle"

	EventJobSubmitting         EventType = "job.submitting"
	EventJobSubmitted          EventType = "job.submitted"
	EventJobAccepted           EventType = "job.accepted"
	EventJobRejected           EventType = "job.rejected"
	EventJobQueued             EventType = "job.queued"
	EventJobDequeued           EventType = "job.dequeued"
	EventJobStarting           EventType = "job.starting"
	EventJobStarted            EventType = "job.started"
	EventJobSucceeded          EventType = "job.succeeded"
	EventJobFailed             EventType = "job.failed"
	EventJobRetrying           EventType = "job.retrying"
	EventJobRetried            EventType = "job.retried"
	EventJobDeadLettered       EventType = "job.dead_lettered"
	EventJobCancelled          EventType = "job.cancelled"
	EventJobProgress           EventType = "job.progress"
	EventJobPanic              EventType = "job.panic"
	EventJobTimeout            EventType = "job.timeout"
	EventJobRateDelayed        EventType = "job.rate_delayed"
	EventJobConcurrencyDelayed EventType = "job.concurrency_delayed"

	EventHookPanic EventType = "hook.panic"
)

// Event is emitted for pool, worker, queue and job lifecycle transitions.
type Event struct {
	Type      EventType         `json:"type"`
	Time      time.Time         `json:"time"`
	Pool      string            `json:"pool,omitempty"`
	State     string            `json:"state,omitempty"`
	Queue     string            `json:"queue,omitempty"`
	WorkerID  int               `json:"worker_id,omitempty"`
	Job       JobInfo           `json:"job,omitempty"`
	Options   JobOptions        `json:"-"`
	Attempt   int               `json:"attempt,omitempty"`
	Duration  time.Duration     `json:"duration,omitempty"`
	Error     error             `json:"-"`
	ErrorText string            `json:"error,omitempty"`
	Panic     any               `json:"-"`
	Message   string            `json:"message,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// LifecycleHandler receives lifecycle events. Handlers must be fast. For slow
// operations use Lifecycle.SetAsync(true) or enqueue work into another system.
type LifecycleHandler func(context.Context, Event)

type handlerEntry struct {
	id string
	fn LifecycleHandler
}

// Lifecycle is a small safe event bus. It is disabled unless configured, so the
// hot path remains allocation-light for benchmark/low-latency mode.
type Lifecycle struct {
	mu        sync.RWMutex
	seq       atomic.Uint64
	all       []handlerEntry
	typed     map[EventType][]handlerEntry
	async     atomic.Bool
	recovered atomic.Uint64
}

func NewLifecycle() *Lifecycle { return &Lifecycle{typed: make(map[EventType][]handlerEntry)} }

func (l *Lifecycle) SetAsync(v bool) {
	if l != nil {
		l.async.Store(v)
	}
}
func (l *Lifecycle) RecoveredPanics() uint64 {
	if l == nil {
		return 0
	}
	return l.recovered.Load()
}

// On registers a handler for one or more event types. With no types it receives all events.
// It returns an unsubscribe function.
func (l *Lifecycle) On(fn LifecycleHandler, types ...EventType) func() {
	if l == nil || fn == nil {
		return func() {}
	}
	id := fmt.Sprintf("h-%d", l.seq.Add(1))
	e := handlerEntry{id: id, fn: fn}
	l.mu.Lock()
	if len(types) == 0 {
		l.all = append(l.all, e)
	} else {
		for _, t := range types {
			l.typed[t] = append(l.typed[t], e)
		}
	}
	l.mu.Unlock()
	return func() { l.off(id) }
}

func (l *Lifecycle) off(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all = removeEntry(l.all, id)
	for t, arr := range l.typed {
		l.typed[t] = removeEntry(arr, id)
	}
}

func removeEntry(in []handlerEntry, id string) []handlerEntry {
	out := in[:0]
	for _, e := range in {
		if e.id != id {
			out = append(out, e)
		}
	}
	return out
}

func (l *Lifecycle) emit(ctx context.Context, ev Event) {
	if l == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	l.mu.RLock()
	n := len(l.all) + len(l.typed[ev.Type])
	if n == 0 {
		l.mu.RUnlock()
		return
	}
	handlers := make([]handlerEntry, 0, n)
	handlers = append(handlers, l.all...)
	handlers = append(handlers, l.typed[ev.Type]...)
	l.mu.RUnlock()
	for _, h := range handlers {
		h := h
		run := func() {
			defer func() {
				if recover() != nil {
					l.recovered.Add(1)
				}
			}()
			h.fn(ctx, ev)
		}
		if l.async.Load() {
			go run()
		} else {
			run()
		}
	}
}

func (p *Pool) Lifecycle() *Lifecycle {
	if p == nil {
		return nil
	}
	if p.lifecycle == nil {
		p.lifecycle = NewLifecycle()
	}
	return p.lifecycle
}

func (p *Pool) On(fn LifecycleHandler, types ...EventType) func() {
	return p.Lifecycle().On(fn, types...)
}

func WithLifecycle(l *Lifecycle) Option {
	return func(p *Pool) {
		if l == nil {
			l = NewLifecycle()
		}
		p.lifecycle = l
	}
}
func WithLifecycleHook(fn LifecycleHandler, types ...EventType) Option {
	return func(p *Pool) { p.Lifecycle().On(fn, types...) }
}
func WithAsyncLifecycle(enabled bool) Option {
	return func(p *Pool) { p.Lifecycle().SetAsync(enabled) }
}

func (p *Pool) emit(ev Event) {
	if p == nil || p.lifecycle == nil {
		return
	}
	ev.Pool = p.cfg.Name
	ev.State = p.State().String()
	if ev.Error != nil && ev.ErrorText == "" {
		ev.ErrorText = ev.Error.Error()
	}
	p.lifecycle.emit(context.Background(), ev)
}
func (p *Pool) emitJob(t EventType, qj *queuedJob, attempt int, err error, msg string) {
	if p == nil || p.lifecycle == nil {
		return
	}
	ev := Event{Type: t, Attempt: attempt, Error: err, Message: msg}
	if qj != nil {
		ev.Job = qj.info()
		ev.Options = qj.opts
		ev.Queue = qj.opts.Queue
		if !qj.startedAt.IsZero() && !qj.finishedAt.IsZero() {
			ev.Duration = qj.finishedAt.Sub(qj.startedAt)
		}
	}
	p.emit(ev)
}
