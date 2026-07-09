package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker/pkg/queue"
)

type State uint32

const (
	stateNew State = iota
	stateRunning
	statePaused
	stateStopping
	stateStopped
	stateTerminated
)

// Store records job state transitions and dead-letter events. It is intentionally
// tiny so production users can plug in file, database, Redis, NATS, Kafka, or
// custom audit backends without affecting the hot path when disabled.
type Store interface {
	SaveJob(context.Context, JobInfo) error
	SaveDeadLetter(context.Context, JobInfo) error
	Close() error
}

type noopStore struct{}

func (noopStore) SaveJob(context.Context, JobInfo) error        { return nil }
func (noopStore) SaveDeadLetter(context.Context, JobInfo) error { return nil }
func (noopStore) Close() error                                  { return nil }

type Pool struct {
	cfg            Config
	q              *queue.PriorityQueue[*queuedJob]
	basicq         *queue.FIFO[Job]
	fastq          *queue.FIFO[*queuedJob]
	queueWake      chan struct{}
	closeOnce      sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	workers        atomic.Int64
	state          atomic.Uint32
	seq            atomic.Uint64
	started        time.Time
	c              counters
	hooks          Hooks
	middleware     []Middleware
	idemp          sync.Map
	ratesMu        sync.RWMutex
	rates          map[string]*tokenBucket
	sem            *keyedSemaphore
	dlqMu          sync.Mutex
	dlq            []*queuedJob
	jobs           sync.Map // job id -> *queuedJob
	scaleStop      chan struct{}
	store          Store
	lifecycle      *Lifecycle
	jobPool        sync.Pool
	queueMu        sync.RWMutex
	queueDefaults  map[string]QueuePolicy
	pausedQueues   map[string]bool
	defaultMu      sync.RWMutex
	defaultOptions JobOptions
	workerInit     WorkerInitFunc
	workerClose    WorkerCloseFunc
	spillMu        sync.Mutex
	spill          []*queuedJob
	spillPath      string
	spillMax       int
}

func New(c Config, opts ...Option) (*Pool, error) {
	cfg, err := normalizeConfig(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{cfg: cfg, q: queue.NewPriorityQueue[*queuedJob](cfg.QueueSize), basicq: queue.NewFIFO[Job](cfg.QueueSize), fastq: queue.NewFIFO[*queuedJob](cfg.QueueSize), queueWake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, sem: newKeyedSemaphore(), rates: make(map[string]*tokenBucket), scaleStop: make(chan struct{}), queueDefaults: make(map[string]QueuePolicy), pausedQueues: make(map[string]bool)}
	p.state.Store(uint32(stateNew))
	p.jobPool.New = func() any { return new(queuedJob) }
	for _, o := range opts {
		o(p)
	}
	p.emit(Event{Type: EventPoolCreated})
	return p, nil
}

func MustNew(c Config, opts ...Option) *Pool {
	p, err := New(c, opts...)
	if err != nil {
		panic(err)
	}
	return p
}

type Option func(*Pool)

func WithHooks(h Hooks) Option { return func(p *Pool) { p.hooks = h } }
func WithMiddleware(m ...Middleware) Option {
	return func(p *Pool) { p.middleware = append(p.middleware, m...) }
}
func WithStore(s Store) Option {
	return func(p *Pool) {
		if s != nil {
			p.store = s
		}
	}
}

func (p *Pool) Start() error {
	p.emit(Event{Type: EventPoolStarting})
	if !p.state.CompareAndSwap(uint32(stateNew), uint32(stateRunning)) {
		return nil
	}
	p.started = time.Now()
	for i := 0; i < p.cfg.MinWorkers; i++ {
		p.spawnWorker()
	}
	if p.cfg.EnableAutoScale && p.cfg.MaxWorkers > p.cfg.MinWorkers {
		go p.autoscale()
	}
	if p.hooks.OnPoolStart != nil {
		p.hooks.OnPoolStart(p)
	}
	p.emit(Event{Type: EventPoolStarted})
	return nil
}
func (p *Pool) spawnWorker() { id := int(p.workers.Add(1)); p.wg.Add(1); go p.worker(id) }
func (p *Pool) autoscale() {
	t := time.NewTicker(p.cfg.ScaleInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if State(p.state.Load()) != stateRunning {
				continue
			}
			depth := p.q.Len()
			workers := int(p.workers.Load())
			busy := int(p.c.busy.Load())
			if depth > workers && workers < p.cfg.MaxWorkers {
				p.spawnWorker()
			}
			if depth == 0 && busy == 0 && workers > p.cfg.MinWorkers { /* workers self exit on idle */
			}
		case <-p.scaleStop:
			return
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) Submit(job Job, opts ...JobOptions) error {
	_, err := p.submit(job, nil, opts...)
	return err
}

// SubmitFast is the minimal zero-allocation hot path for immediate jobs.
// It intentionally bypasses options, hooks, middleware, tracking, retries, rate
// limiting, idempotency, and stats. Use Submit for the full-featured path.
func (p *Pool) SubmitFast(job Job) error {
	if job == nil {
		return ErrInvalidConfig
	}
	st := State(p.state.Load())
	if st == stateTerminated {
		return ErrTerminated
	}
	if st == stateStopping || st == stateStopped {
		return ErrClosed
	}
	if p.pushBasic(job) {
		return nil
	}
	return ErrQueueFull
}

func (p *Pool) SubmitFunc(fn func(context.Context) error, opts ...JobOptions) error {
	return p.Submit(JobFunc(fn), opts...)
}

// SubmitTask submits stateful work without allocating a capturing closure.
func SubmitTask[T any](p *Pool, arg *T, fn TaskFunc[T], opts ...JobOptions) error {
	return p.Submit(Task[T]{Arg: arg, Fn: fn}, opts...)
}

// SubmitTask submits stateful work without allocating a capturing closure.
func (p *Pool) SubmitTask(arg any, fn func(context.Context, any) error, opts ...JobOptions) error {
	return p.Submit(AnyJob{Payload: arg, Handler: fn}, opts...)
}

// SubmitTaskFast submits stateful work through the minimal hot path.
func SubmitTaskFast[T any](p *Pool, arg *T, fn TaskFunc[T]) error {
	return p.SubmitFast(Task[T]{Arg: arg, Fn: fn})
}

// SubmitAny accepts any request/payload type and queues it for background
// processing by handler. It is useful for HTTP/event ingestion where admission
// should be fast while workers handle smoothing, rate limits and concurrency.
func (p *Pool) SubmitAny(payload any, handler AnyHandler, opts ...JobOptions) error {
	return p.Submit(AnyJob{Payload: payload, Handler: handler}, opts...)
}
func SubmitResult[T any](p *Pool, fn func(context.Context) (T, error), opts ...JobOptions) (Future[T], error) {
	f := newFutureAny()
	j := resultJob[T]{fn: fn, f: f}
	_, err := p.submit(j, f, opts...)
	return futureTyped[T]{inner: f}, err
}
func (p *Pool) SubmitResult(fn func(context.Context) (any, error), opts ...JobOptions) (Future[any], error) {
	return SubmitResult[any](p, fn, opts...)
}

func (p *Pool) submit(job Job, f *futureAny, opts ...JobOptions) (*queuedJob, error) {
	if !p.cfg.DisableStats {
		p.c.submitted.Add(1)
	}
	p.emit(Event{Type: EventJobSubmitting})
	st := State(p.state.Load())
	if st == stateTerminated {
		p.c.rejected.Add(1)
		p.emit(Event{Type: EventJobRejected, Error: ErrTerminated})
		return nil, ErrTerminated
	}
	if st == stateStopping || st == stateStopped {
		p.c.rejected.Add(1)
		p.emit(Event{Type: EventJobRejected, Error: ErrClosed})
		return nil, ErrClosed
	}
	if job == nil {
		p.c.rejected.Add(1)
		err := fmt.Errorf("%w: nil job", ErrInvalidConfig)
		p.emit(Event{Type: EventJobRejected, Error: err})
		return nil, err
	}
	var opt JobOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	p.applyDefaults(&opt)
	if opt.ID == "" && (p.cfg.EnableJobTracking || p.cfg.EnableJobIDs || opt.IdempotencyKey != "") {
		opt.ID = "job-" + strconv.FormatUint(p.seq.Add(1), 10)
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = p.cfg.DefaultMaxAttempts
	}
	if opt.Backoff == nil {
		opt.Backoff = p.cfg.DefaultBackoff
	}
	if opt.Timeout <= 0 {
		opt.Timeout = p.cfg.DefaultTimeout
	}
	if p.useBasicFastPath(job, opt, f) {
		if p.pushBasic(job) {
			if !p.cfg.DisableStats {
				p.c.accepted.Add(1)
			}
			return nil, nil
		}
		p.c.rejected.Add(1)
		return nil, ErrQueueFull
	}
	if opt.IdempotencyKey != "" {
		if _, loaded := p.idemp.LoadOrStore(opt.IdempotencyKey, opt.ID); loaded {
			p.c.rejected.Add(1)
			p.emit(Event{Type: EventJobRejected, Error: ErrDuplicate, Options: opt, Queue: opt.Queue})
			return nil, ErrDuplicate
		}
	}
	if err := p.admitRate(opt.RateLimitKey); err != nil {
		p.c.rejected.Add(1)
		p.emit(Event{Type: EventJobRejected, Error: err, Options: opt, Queue: opt.Queue})
		return nil, err
	}
	now := time.Now()
	runAt := now
	if opt.Delay > 0 {
		runAt = runAt.Add(opt.Delay)
	}
	if !opt.RunAt.IsZero() && opt.RunAt.After(runAt) {
		runAt = opt.RunAt
	}
	ctx := p.ctx
	var cancel context.CancelFunc
	if f != nil || p.cfg.EnableJobTracking || opt.ID != "" {
		ctx, cancel = context.WithCancel(p.ctx)
	}
	if len(p.middleware) > 0 {
		job = Chain(job, p.middleware...)
	}
	qj := p.jobPool.Get().(*queuedJob)
	*qj = queuedJob{seq: p.seq.Add(1), job: job, opts: opt, createdAt: now, runAt: runAt, future: f, ctx: ctx, cancel: cancel, pool: p}
	qj.state.Store(uint32(JobQueued))
	if f != nil {
		f.cancel = cancel
	}
	if p.hooks.OnJobSubmit != nil {
		p.hooks.OnJobSubmit(opt)
	}
	p.emitJob(EventJobSubmitted, qj, 0, nil, "")
	if opt.Callback != nil && opt.Callback.OnQueued != nil {
		opt.Callback.OnQueued(context.Background(), qj.info())
	}
	if p.cfg.EnableJobTracking && opt.ID != "" {
		p.jobs.Store(opt.ID, qj)
	}
	if p.store != nil {
		_ = p.store.SaveJob(context.Background(), qj.info())
	}
	ok := false
	if p.useFastQueue(opt, now, runAt) {
		ok = p.pushFast(qj)
	} else {
		item := queue.Item[*queuedJob]{Value: qj, Priority: int(opt.Priority), Seq: qj.seq, RunAt: runAt}
		switch p.cfg.Backpressure {
		case BackpressureReject:
			ok = p.q.Push(item)
		case BackpressureDropOldest:
			ok = p.q.DropOldestPush(item)
		case BackpressureDropNewest:
			ok = p.q.Push(item)
		default:
			ok = p.pushBlock(item)
		}
		if ok {
			p.signalQueueWake()
		}
	}
	if !ok {
		if p.trySpill(qj) {
			p.c.accepted.Add(1)
			p.emitJob(EventJobAccepted, qj, 0, nil, "spilled")
			return qj, nil
		}
		p.c.rejected.Add(1)
		p.emitJob(EventJobRejected, qj, 0, ErrQueueFull, "")
		if p.cfg.EnableJobTracking && opt.ID != "" {
			p.jobs.Delete(opt.ID)
		}
		if f != nil {
			f.complete(nil, ErrQueueFull)
		}
		p.releaseJob(qj)
		return nil, ErrQueueFull
	}
	if !p.cfg.DisableStats {
		p.c.accepted.Add(1)
	}
	p.emitJob(EventJobAccepted, qj, 0, nil, "")
	p.emitJob(EventJobQueued, qj, 0, nil, "")
	return qj, nil
}
func (p *Pool) useBasicFastPath(job Job, opt JobOptions, f *futureAny) bool {
	return f == nil && p.store == nil && !p.cfg.EnableJobTracking && !p.cfg.EnableJobIDs && len(p.middleware) == 0 &&
		p.lifecycle == nil && p.hooks.OnJobSubmit == nil && p.hooks.OnJobStart == nil && p.hooks.OnJobSuccess == nil && p.hooks.OnJobError == nil && p.hooks.OnJobPanic == nil && p.spillPath == "" &&
		opt.Queue == "" && opt.Priority == PriorityNormal && opt.Delay == 0 && opt.RunAt.IsZero() && opt.Timeout == 0 &&
		opt.MaxAttempts == 1 && opt.Backoff == p.cfg.DefaultBackoff && len(opt.Metadata) == 0 && opt.IdempotencyKey == "" && opt.ConcurrencyKey == "" && opt.RateLimitKey == ""
}

func (p *Pool) pushBasic(job Job) bool {
	switch p.cfg.Backpressure {
	case BackpressureReject, BackpressureDropNewest:
		return p.basicq.TryPush(job)
	default:
		if p.cfg.SubmitTimeout > 0 {
			deadline := time.Now().Add(p.cfg.SubmitTimeout)
			for {
				if p.basicq.TryPush(job) {
					return true
				}
				if time.Now().After(deadline) || State(p.state.Load()) >= stateStopping {
					return false
				}
				runtime.Gosched()
			}
		}
		return p.basicq.Push(job)
	}
}

func (p *Pool) useFastQueue(opt JobOptions, now, runAt time.Time) bool {
	return opt.Priority == PriorityNormal && opt.Delay == 0 && opt.RunAt.IsZero() && runAt.Equal(now)
}

func (p *Pool) pushFast(qj *queuedJob) bool {
	switch p.cfg.Backpressure {
	case BackpressureReject, BackpressureDropNewest:
		return p.fastq.TryPush(qj)
	default:
		if p.cfg.SubmitTimeout > 0 {
			deadline := time.Now().Add(p.cfg.SubmitTimeout)
			for {
				if p.fastq.TryPush(qj) {
					return true
				}
				if time.Now().After(deadline) || State(p.state.Load()) >= stateStopping {
					return false
				}
				runtime.Gosched()
			}
		}
		return p.fastq.Push(qj)
	}
}

func (p *Pool) signalQueueWake() {
	select {
	case p.queueWake <- struct{}{}:
	default:
	}
}

func (p *Pool) pushBlock(item queue.Item[*queuedJob]) bool {
	deadline := time.Time{}
	if p.cfg.SubmitTimeout > 0 {
		deadline = time.Now().Add(p.cfg.SubmitTimeout)
	}
	for {
		if p.q.Push(item) {
			return true
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return false
		}
		if State(p.state.Load()) >= stateStopping {
			return false
		}
		runtime.Gosched()
		time.Sleep(time.Microsecond)
	}
}

func (p *Pool) worker(id int) {
	p.emit(Event{Type: EventWorkerStarting, WorkerID: id})
	defer func() {
		p.workers.Add(-1)
		p.wg.Done()
		p.emit(Event{Type: EventWorkerStopping, WorkerID: id})
		if p.hooks.OnWorkerStop != nil {
			p.hooks.OnWorkerStop(id)
		}
		p.emit(Event{Type: EventWorkerStopped, WorkerID: id})
	}()
	workerState := &Worker{ID: id}
	if p.workerInit != nil {
		_ = p.workerInit(p.ctx, workerState)
	}
	defer func() {
		if p.workerClose != nil {
			_ = p.workerClose(context.Background(), workerState)
		}
	}()
	if p.hooks.OnWorkerStart != nil {
		p.hooks.OnWorkerStart(id)
	}
	p.emit(Event{Type: EventWorkerStarted, WorkerID: id})
	for {
		if State(p.state.Load()) == stateTerminated {
			return
		}
		for State(p.state.Load()) == statePaused {
			select {
			case <-p.ctx.Done():
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
		if job, ok := p.basicq.TryPop(); ok {
			p.executeBasic(job)
			continue
		}
		if qj, ok := p.fastq.TryPop(); ok {
			p.execute(id, qj)
			continue
		}
		if qj, ok := p.popSpill(); ok {
			p.execute(id, qj)
			continue
		}
		if it, ok := p.q.TryPop(); ok {
			p.execute(id, it.Value)
			continue
		}
		if p.q.Len() > 0 {
			if it, ok := p.q.PopTimeout(10 * time.Millisecond); ok {
				p.execute(id, it.Value)
				continue
			}
		}
		if State(p.state.Load()) == stateStopping && p.basicq.Len() == 0 && p.fastq.Len() == 0 && p.q.Len() == 0 {
			return
		}
		if p.cfg.IdleTimeout > 0 && int(p.workers.Load()) > p.cfg.MinWorkers {
			if job, ok := p.basicq.PopTimeout(p.cfg.IdleTimeout); ok {
				p.executeBasic(job)
				continue
			}
			if qj, ok := p.fastq.PopTimeout(time.Millisecond); ok {
				p.execute(id, qj)
				continue
			}
			if int(p.workers.Load()) > p.cfg.MinWorkers {
				return
			}
			continue
		}
		if job, ok := p.basicq.PopTimeout(10 * time.Millisecond); ok {
			p.executeBasic(job)
			continue
		}
		if qj, ok := p.fastq.TryPop(); ok {
			p.execute(id, qj)
			continue
		}
		if it, ok := p.q.PopTimeout(10 * time.Millisecond); ok {
			p.execute(id, it.Value)
			continue
		}
		select {
		case <-p.queueWake:
		case <-p.ctx.Done():
			return
		default:
			runtime.Gosched()
		}
	}
}

func (p *Pool) executeBasic(job Job) {
	if !p.cfg.DisableStats {
		p.c.started.Add(1)
		p.c.busy.Add(1)
		defer p.c.busy.Add(-1)
	}
	if !p.cfg.RecoverPanics {
		if err := job.Run(p.ctx); err != nil {
			if !p.cfg.DisableStats {
				p.c.failed.Add(1)
				p.c.dlq.Add(1)
			}
			return
		}
		if !p.cfg.DisableStats {
			p.c.completed.Add(1)
		}
		return
	}
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if !p.cfg.DisableStats {
					p.c.panicked.Add(1)
				}
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		err = job.Run(p.ctx)
	}()
	if err != nil {
		if !p.cfg.DisableStats {
			p.c.failed.Add(1)
			p.c.dlq.Add(1)
		}
		return
	}
	if !p.cfg.DisableStats {
		p.c.completed.Add(1)
	}
}

func (p *Pool) requeue(qj *queuedJob) bool {
	if qj == nil {
		return false
	}
	item := queue.Item[*queuedJob]{Value: qj, Priority: int(qj.opts.Priority), Seq: qj.seq, RunAt: qj.runAt}
	ok := p.pushBlock(item)
	if ok {
		p.signalQueueWake()
	}
	return ok
}

func (p *Pool) execute(id int, qj *queuedJob) {
	if qj == nil {
		return
	}
	p.emitJob(EventJobDequeued, qj, qj.attempt, nil, "")
	if qj.ctx != nil && qj.ctx.Err() != nil {
		qj.state.Store(uint32(JobCancelled))
		qj.finishedAt = time.Now()
		if p.store != nil {
			_ = p.store.SaveJob(context.Background(), qj.info())
		}
		if p.cfg.EnableJobTracking && qj.opts.ID != "" {
			p.jobs.Delete(qj.opts.ID)
		}
		if qj.future != nil {
			qj.future.complete(nil, ErrCancelled)
		}
		if qj.opts.Callback != nil && qj.opts.Callback.OnCancel != nil {
			qj.opts.Callback.OnCancel(context.Background(), qj.info())
		}
		if qj.opts.Callback != nil && qj.opts.Callback.OnFinally != nil {
			qj.opts.Callback.OnFinally(context.Background(), qj.info(), ErrCancelled)
		}
		p.emitJob(EventJobCancelled, qj, qj.attempt, ErrCancelled, "")
		p.releaseJob(qj)
		return
	}
	if p.isQueuedPaused(qj) {
		qj.runAt = time.Now().Add(50 * time.Millisecond)
		qj.state.Store(uint32(JobQueued))
		if !p.requeue(qj) {
			p.deadletter(qj, ErrQueueFull)
		}
		return
	}
	if p.delayForRateLimit(qj) {
		return
	}
	if !p.sem.tryAcquire(qj.opts.ConcurrencyKey, qj.opts.MaxConcurrentPerKey) {
		p.emitJob(EventJobConcurrencyDelayed, qj, qj.attempt, nil, "concurrency limit")
		qj.runAt = time.Now().Add(5 * time.Millisecond)
		qj.state.Store(uint32(JobQueued))
		if !p.requeue(qj) {
			p.deadletter(qj, ErrQueueFull)
		}
		return
	}
	defer p.sem.release(qj.opts.ConcurrencyKey)
	ctx := qj.ctx
	if ctx == nil {
		ctx = p.ctx
	}
	var cancel context.CancelFunc
	if qj.opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, qj.opts.Timeout)
		defer cancel()
	}
	qj.attempt++
	qj.startedAt = time.Now()
	qj.state.Store(uint32(JobRunning))
	p.emitJob(EventJobStarting, qj, qj.attempt, nil, "")
	if qj.opts.Callback != nil && qj.opts.Callback.OnStart != nil {
		qj.opts.Callback.OnStart(ctx, qj.info())
	}
	if p.store != nil {
		_ = p.store.SaveJob(context.Background(), qj.info())
	}
	p.c.started.Add(1)
	p.c.busy.Add(1)
	defer p.c.busy.Add(-1)
	if p.hooks.OnJobStart != nil {
		p.hooks.OnJobStart(ctx, qj.opts, qj.attempt)
	}
	p.emitJob(EventJobStarted, qj, qj.attempt, nil, "")
	var err error
	var panicVal any
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicVal = r
				p.c.panicked.Add(1)
				if p.hooks.OnJobPanic != nil {
					p.hooks.OnJobPanic(ctx, qj.opts, qj.attempt, r)
				}
				if qj.opts.Callback != nil && qj.opts.Callback.OnPanic != nil {
					qj.opts.Callback.OnPanic(ctx, qj.info(), r)
				}
				p.emit(Event{Type: EventJobPanic, WorkerID: id, Job: qj.info(), Options: qj.opts, Queue: qj.opts.Queue, Attempt: qj.attempt, Panic: r, Message: fmt.Sprint(r)})
				if !p.cfg.RecoverPanics {
					panic(r)
				}
				err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			}
		}()
		ctx = contextWithProgress(ctx, qj)
		err = qj.job.Run(ctx)
	}()
	if ctx.Err() != nil && err == nil {
		err = ctx.Err()
		p.c.timedout.Add(1)
		p.emitJob(EventJobTimeout, qj, qj.attempt, err, "")
	}
	if err == nil && panicVal == nil {
		p.c.completed.Add(1)
		qj.state.Store(uint32(JobSucceeded))
		qj.finishedAt = time.Now()
		if p.store != nil {
			_ = p.store.SaveJob(context.Background(), qj.info())
		}
		if p.cfg.EnableJobTracking && qj.opts.ID != "" {
			p.jobs.Delete(qj.opts.ID)
		}
		if qj.future != nil {
			qj.future.complete(nil, nil)
		}
		if qj.opts.Callback != nil && qj.opts.Callback.OnSuccess != nil {
			qj.opts.Callback.OnSuccess(ctx, qj.info())
		}
		if qj.opts.Callback != nil && qj.opts.Callback.OnFinally != nil {
			qj.opts.Callback.OnFinally(ctx, qj.info(), nil)
		}
		if p.hooks.OnJobSuccess != nil {
			p.hooks.OnJobSuccess(ctx, qj.opts, qj.attempt)
		}
		p.emitJob(EventJobSucceeded, qj, qj.attempt, nil, "")
		if qj.opts.IdempotencyKey != "" {
			p.idemp.Delete(qj.opts.IdempotencyKey)
		}
		p.releaseJob(qj)
		return
	}
	p.c.failed.Add(1)
	qj.state.Store(uint32(JobFailed))
	if err != nil {
		qj.lastErr.Store(err.Error())
	}
	if p.hooks.OnJobError != nil {
		p.hooks.OnJobError(ctx, qj.opts, qj.attempt, err)
	}
	p.emitJob(EventJobFailed, qj, qj.attempt, err, "")
	if qj.opts.Callback != nil && qj.opts.Callback.OnError != nil {
		qj.opts.Callback.OnError(ctx, qj.info(), err)
	}
	if qj.attempt < qj.opts.MaxAttempts && !IsPermanent(err) && State(p.state.Load()) < stateStopping {
		p.c.retried.Add(1)
		if p.hooks.OnJobRetry != nil {
			p.hooks.OnJobRetry(qj.opts, qj.attempt, err)
		}
		if qj.opts.Callback != nil && qj.opts.Callback.OnRetry != nil {
			qj.opts.Callback.OnRetry(ctx, qj.info(), qj.attempt, err)
		}
		p.emitJob(EventJobRetrying, qj, qj.attempt, err, "")
		qj.state.Store(uint32(JobRetrying))
		delay := qj.opts.Backoff.Delay(qj.attempt)
		qj.runAt = time.Now().Add(delay)
		if p.store != nil {
			_ = p.store.SaveJob(context.Background(), qj.info())
		}
		if !p.requeue(qj) {
			p.deadletter(qj, ErrQueueFull)
		}
		return
	}
	p.deadletter(qj, err)
}
func (p *Pool) deadletter(qj *queuedJob, err error) {
	p.c.dlq.Add(1)
	qj.state.Store(uint32(JobDeadLettered))
	qj.finishedAt = time.Now()
	if p.cfg.EnableJobTracking && qj.opts.ID != "" {
		p.jobs.Delete(qj.opts.ID)
	}
	if p.store != nil {
		_ = p.store.SaveJob(context.Background(), qj.info())
		_ = p.store.SaveDeadLetter(context.Background(), qj.info())
	}
	p.dlqMu.Lock()
	p.dlq = append(p.dlq, qj)
	p.dlqMu.Unlock()
	if qj.future != nil {
		qj.future.complete(nil, err)
	}
	if qj.opts.Callback != nil && qj.opts.Callback.OnDeadLetter != nil {
		qj.opts.Callback.OnDeadLetter(context.Background(), qj.info(), err)
	}
	if qj.opts.Callback != nil && qj.opts.Callback.OnFinally != nil {
		qj.opts.Callback.OnFinally(context.Background(), qj.info(), err)
	}
	if p.hooks.OnJobDeadLetter != nil {
		p.hooks.OnJobDeadLetter(qj.opts, qj.attempt, err)
	}
	p.emitJob(EventJobDeadLettered, qj, qj.attempt, err, "")
	if qj.opts.IdempotencyKey != "" {
		p.idemp.Delete(qj.opts.IdempotencyKey)
	}
}

func (p *Pool) releaseJob(qj *queuedJob) {
	if qj == nil || qj.future != nil || JobState(qj.state.Load()) == JobDeadLettered {
		return
	}
	*qj = queuedJob{}
	p.jobPool.Put(qj)
}

func (p *Pool) Pause() error {
	for {
		s := State(p.state.Load())
		if s == stateRunning {
			p.emit(Event{Type: EventPoolPausing})
			if p.state.CompareAndSwap(uint32(s), uint32(statePaused)) {
				p.emit(Event{Type: EventPoolPaused})
				return nil
			}
			continue
		}
		if s == statePaused {
			return nil
		}
		return ErrClosed
	}
}
func (p *Pool) Resume() error {
	for {
		s := State(p.state.Load())
		if s == statePaused {
			p.emit(Event{Type: EventPoolResuming})
			if p.state.CompareAndSwap(uint32(s), uint32(stateRunning)) {
				p.emit(Event{Type: EventPoolResumed})
				return nil
			}
			continue
		}
		if s == stateRunning {
			return nil
		}
		return ErrClosed
	}
}
func (p *Pool) Stop(ctx context.Context) error { return p.Shutdown(ctx) }
func (p *Pool) Shutdown(ctx context.Context) error {
	s := State(p.state.Load())
	if s == stateTerminated {
		return ErrTerminated
	}
	if s < stateStopping {
		p.emit(Event{Type: EventPoolStopping})
		p.state.Store(uint32(stateStopping))
		close(p.scaleStop)
		p.closeOnce.Do(func() { p.basicq.Close(); p.fastq.Close() })
		p.q.Close()
		if p.hooks.OnPoolStop != nil {
			p.hooks.OnPoolStop(p)
		}
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		p.state.Store(uint32(stateStopped))
		p.emit(Event{Type: EventPoolStopped})
		p.cancel()
		if p.store != nil {
			_ = p.store.Close()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *Pool) Terminate() error {
	p.emit(Event{Type: EventPoolTerminating})
	s := State(p.state.Swap(uint32(stateTerminated)))
	if s == stateTerminated {
		return nil
	}
	p.cancel()
	p.closeOnce.Do(func() { p.basicq.Close(); p.fastq.Close() })
	p.q.Close()
	if p.store != nil {
		_ = p.store.Close()
	}
	p.emit(Event{Type: EventPoolTerminated})
	return nil
}

func (p *Pool) Stats() Stats {
	s := State(p.state.Load())
	return Stats{Submitted: p.c.submitted.Load(), Accepted: p.c.accepted.Load(), Rejected: p.c.rejected.Load(), Started: p.c.started.Load(), Completed: p.c.completed.Load(), Failed: p.c.failed.Load(), Retried: p.c.retried.Load(), Panicked: p.c.panicked.Load(), TimedOut: p.c.timedout.Load(), DeadLettered: p.c.dlq.Load(), RateDelayed: p.c.rateDelayed.Load(), QueueDepth: p.q.Len() + p.fastq.Len() + p.basicq.Len() + p.SpillDepth(), Workers: int(p.workers.Load()), BusyWorkers: int(p.c.busy.Load()), Paused: s == statePaused, Stopped: s == stateStopped, Terminated: s == stateTerminated, Uptime: time.Since(p.started)}
}
func (p *Pool) DeadLetters() []JobOptions {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	out := make([]JobOptions, 0, len(p.dlq))
	for _, j := range p.dlq {
		out = append(out, j.opts)
	}
	return out
}
func (p *Pool) SetRateLimit(key string, r RateLimit) {
	p.ratesMu.Lock()
	p.rates[key] = newBucket(r)
	p.ratesMu.Unlock()
}

func (p *Pool) rateBucket(key string) *tokenBucket {
	if key == "" {
		return nil
	}
	p.ratesMu.RLock()
	b := p.rates[key]
	p.ratesMu.RUnlock()
	return b
}

func (p *Pool) admitRate(key string) error {
	b := p.rateBucket(key)
	if b == nil {
		return nil
	}
	switch b.mode {
	case RateLimitReject:
		if !b.allow() {
			return ErrRateLimited
		}
	case RateLimitBlock:
		deadline := time.Time{}
		if p.cfg.SubmitTimeout > 0 {
			deadline = time.Now().Add(p.cfg.SubmitTimeout)
		}
		for {
			if b.allow() {
				return nil
			}
			if State(p.state.Load()) >= stateStopping {
				return ErrClosed
			}
			if !deadline.IsZero() && time.Now().After(deadline) {
				return ErrRateLimited
			}
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}

func (p *Pool) delayForRateLimit(qj *queuedJob) bool {
	if qj == nil || qj.opts.RateLimitKey == "" {
		return false
	}
	if !qj.rateReadyAt.IsZero() {
		if time.Now().Before(qj.rateReadyAt) {
			qj.runAt = qj.rateReadyAt
			qj.state.Store(uint32(JobQueued))
			if !p.requeue(qj) {
				p.deadletter(qj, ErrQueueFull)
			}
			return true
		}
		qj.rateReadyAt = time.Time{}
		return false
	}
	b := p.rateBucket(qj.opts.RateLimitKey)
	if b == nil || b.mode != RateLimitQueue {
		return false
	}
	delay, ok := b.reserveDelay(true)
	if !ok {
		return false
	}
	if delay <= 0 {
		return false
	}
	qj.runAt = time.Now().Add(delay)
	qj.rateReadyAt = qj.runAt
	qj.state.Store(uint32(JobQueued))
	p.c.rateDelayed.Add(1)
	p.emitJob(EventJobRateDelayed, qj, qj.attempt, nil, "rate limited")
	if !p.requeue(qj) {
		p.deadletter(qj, ErrQueueFull)
	}
	return true
}

func (p *Pool) allowRate(key string) bool {
	b := p.rateBucket(key)
	if b == nil {
		return true
	}
	return b.allow()
}

func (p *Pool) resetQueues(n int) {
	p.q = queue.NewPriorityQueue[*queuedJob](n)
	p.basicq = queue.NewFIFO[Job](n)
	p.fastq = queue.NewFIFO[*queuedJob](n)
}

func (p *Pool) isQueuedPaused(qj *queuedJob) bool {
	if qj == nil || qj.opts.Queue == "" {
		return false
	}
	p.queueMu.RLock()
	paused := p.pausedQueues[qj.opts.Queue]
	p.queueMu.RUnlock()
	return paused
}

func (p *Pool) trySpill(qj *queuedJob) bool {
	if p.spillPath == "" || qj == nil {
		return false
	}
	p.spillMu.Lock()
	defer p.spillMu.Unlock()
	if p.spillMax > 0 && len(p.spill) >= p.spillMax {
		return false
	}
	p.spill = append(p.spill, qj)
	p.emitJob(EventQueueSpill, qj, qj.attempt, nil, "spilled to overflow")
	if p.spillPath != "" {
		_ = os.MkdirAll(p.spillPath, 0o755)
		f, err := os.OpenFile(filepath.Join(p.spillPath, "spill.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			_ = json.NewEncoder(f).Encode(qj.info())
			_ = f.Close()
		}
	}
	p.signalQueueWake()
	return true
}

func (p *Pool) popSpill() (*queuedJob, bool) {
	p.spillMu.Lock()
	defer p.spillMu.Unlock()
	if len(p.spill) == 0 {
		return nil, false
	}
	qj := p.spill[0]
	copy(p.spill, p.spill[1:])
	p.spill[len(p.spill)-1] = nil
	p.spill = p.spill[:len(p.spill)-1]
	return qj, true
}

func (p *Pool) SpillDepth() int { p.spillMu.Lock(); n := len(p.spill); p.spillMu.Unlock(); return n }
