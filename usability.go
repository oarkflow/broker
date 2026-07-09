package fastworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// QueuePolicy defines ergonomic per-queue defaults. It intentionally maps to
// JobOptions so queue groups can stay allocation-light and share the same pool.
type QueuePolicy struct {
	Name                string
	Priority            Priority
	Timeout             time.Duration
	MaxAttempts         int
	Backoff             Backoff
	RateLimitKey        string
	ConcurrencyKey      string
	MaxConcurrentPerKey int
	Metadata            map[string]string
}

// Queue returns a named queue handle. The handle is cheap and can be kept by callers.
func (p *Pool) Queue(name string) QueueHandle { return QueueHandle{pool: p, name: name} }

type QueueHandle struct {
	pool *Pool
	name string
}

func (q QueueHandle) Submit(job Job, opts ...JobOptions) error {
	opt := firstOptions(opts)
	opt.Queue = q.name
	return q.pool.Submit(job, opt)
}
func (q QueueHandle) SubmitFunc(fn func(context.Context) error, opts ...JobOptions) error {
	return q.Submit(JobFunc(fn), opts...)
}
func (q QueueHandle) SubmitAny(payload any, h AnyHandler, opts ...JobOptions) error {
	opt := firstOptions(opts)
	opt.Queue = q.name
	return q.pool.SubmitAny(payload, h, opt)
}
func (q QueueHandle) SubmitResult(fn func(context.Context) (any, error), opts ...JobOptions) (Future[any], error) {
	opt := firstOptions(opts)
	opt.Queue = q.name
	return q.pool.SubmitResult(fn, opt)
}
func (q QueueHandle) Pause()  { q.pool.PauseQueue(q.name) }
func (q QueueHandle) Resume() { q.pool.ResumeQueue(q.name) }

func firstOptions(opts []JobOptions) JobOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	return JobOptions{}
}

// SetQueuePolicy updates defaults applied to jobs submitted with opts.Queue/name.
func (p *Pool) SetQueuePolicy(name string, policy QueuePolicy) {
	if name == "" {
		return
	}
	policy.Name = name
	p.queueMu.Lock()
	p.queueDefaults[name] = policy
	p.queueMu.Unlock()
}
func (p *Pool) QueuePolicy(name string) (QueuePolicy, bool) {
	p.queueMu.RLock()
	v, ok := p.queueDefaults[name]
	p.queueMu.RUnlock()
	return v, ok
}
func (p *Pool) PauseQueue(name string) {
	if name != "" {
		p.queueMu.Lock()
		p.pausedQueues[name] = true
		p.queueMu.Unlock()
		p.emit(Event{Type: EventQueuePaused, Queue: name})
		p.signalQueueWake()
	}
}
func (p *Pool) ResumeQueue(name string) {
	if name != "" {
		p.queueMu.Lock()
		delete(p.pausedQueues, name)
		p.queueMu.Unlock()
		p.emit(Event{Type: EventQueueResumed, Queue: name})
		p.signalQueueWake()
	}
}
func (p *Pool) IsQueuePaused(name string) bool {
	p.queueMu.RLock()
	v := p.pausedQueues[name]
	p.queueMu.RUnlock()
	return v
}

func (p *Pool) applyDefaults(opt *JobOptions) {
	if opt == nil {
		return
	}
	p.defaultMu.RLock()
	def := p.defaultOptions
	p.defaultMu.RUnlock()
	mergeJobOptions(opt, def)
	if opt.Queue != "" {
		p.queueMu.RLock()
		qp, ok := p.queueDefaults[opt.Queue]
		paused := p.pausedQueues[opt.Queue]
		p.queueMu.RUnlock()
		if ok {
			if opt.Priority == PriorityNormal {
				opt.Priority = qp.Priority
			}
			if opt.Timeout <= 0 {
				opt.Timeout = qp.Timeout
			}
			if opt.MaxAttempts <= 0 {
				opt.MaxAttempts = qp.MaxAttempts
			}
			if opt.Backoff == nil {
				opt.Backoff = qp.Backoff
			}
			if opt.RateLimitKey == "" {
				opt.RateLimitKey = qp.RateLimitKey
			}
			if opt.ConcurrencyKey == "" {
				opt.ConcurrencyKey = qp.ConcurrencyKey
			}
			if opt.MaxConcurrentPerKey <= 0 {
				opt.MaxConcurrentPerKey = qp.MaxConcurrentPerKey
			}
			opt.Metadata = mergeMetadata(qp.Metadata, opt.Metadata)
		}
		if paused && opt.Delay <= 0 && opt.RunAt.IsZero() {
			opt.Delay = 50 * time.Millisecond
		}
	}
}
func mergeJobOptions(opt *JobOptions, def JobOptions) {
	if opt.ID == "" {
		opt.ID = def.ID
	}
	if opt.Queue == "" {
		opt.Queue = def.Queue
	}
	if opt.Priority == PriorityNormal {
		opt.Priority = def.Priority
	}
	if opt.Delay <= 0 {
		opt.Delay = def.Delay
	}
	if opt.RunAt.IsZero() {
		opt.RunAt = def.RunAt
	}
	if opt.Timeout <= 0 {
		opt.Timeout = def.Timeout
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = def.MaxAttempts
	}
	if opt.Backoff == nil {
		opt.Backoff = def.Backoff
	}
	if opt.IdempotencyKey == "" {
		opt.IdempotencyKey = def.IdempotencyKey
	}
	if opt.ConcurrencyKey == "" {
		opt.ConcurrencyKey = def.ConcurrencyKey
	}
	if opt.MaxConcurrentPerKey <= 0 {
		opt.MaxConcurrentPerKey = def.MaxConcurrentPerKey
	}
	if opt.RateLimitKey == "" {
		opt.RateLimitKey = def.RateLimitKey
	}
	if opt.Callback == nil {
		opt.Callback = def.Callback
	}
	opt.Metadata = mergeMetadata(def.Metadata, opt.Metadata)
}
func mergeMetadata(base, over map[string]string) map[string]string {
	if len(base) == 0 {
		return over
	}
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}
func (p *Pool) SetDefaultOptions(opt JobOptions) {
	p.defaultMu.Lock()
	p.defaultOptions = opt
	p.defaultMu.Unlock()
}

// Functional options for ergonomic construction.
func NewPool(opts ...Option) (*Pool, error) { return New(DefaultConfig(), opts...) }
func MustNewPool(opts ...Option) *Pool      { return MustNew(DefaultConfig(), opts...) }
func WithName(name string) Option {
	return func(p *Pool) {
		if name != "" {
			p.cfg.Name = name
		}
	}
}
func WithWorkers(n int) Option {
	return func(p *Pool) {
		if n > 0 {
			p.cfg.MinWorkers = n
			if p.cfg.MaxWorkers < n {
				p.cfg.MaxWorkers = n
			}
		}
	}
}
func WithWorkerRange(min, max int) Option {
	return func(p *Pool) {
		if min > 0 {
			p.cfg.MinWorkers = min
		}
		if max > 0 {
			p.cfg.MaxWorkers = max
		}
		if p.cfg.MaxWorkers < p.cfg.MinWorkers {
			p.cfg.MaxWorkers = p.cfg.MinWorkers
		}
	}
}
func WithQueueSize(n int) Option {
	return func(p *Pool) {
		if n > 0 && p.State() == stateNew {
			p.cfg.QueueSize = n
			p.resetQueues(n)
		}
	}
}
func WithBackpressure(b BackpressurePolicy) Option { return func(p *Pool) { p.cfg.Backpressure = b } }
func WithSubmitTimeout(d time.Duration) Option     { return func(p *Pool) { p.cfg.SubmitTimeout = d } }
func WithDefaultOptions(opt JobOptions) Option     { return func(p *Pool) { p.defaultOptions = opt } }
func WithRateLimit(key string, r RateLimit) Option { return func(p *Pool) { p.SetRateLimit(key, r) } }
func WithQueuePolicy(name string, policy QueuePolicy) Option {
	return func(p *Pool) { p.SetQueuePolicy(name, policy) }
}
func WithJobTracking(enabled bool) Option     { return func(p *Pool) { p.cfg.EnableJobTracking = enabled } }
func WithJobIDs(enabled bool) Option          { return func(p *Pool) { p.cfg.EnableJobIDs = enabled } }
func WithRecoverPanics(enabled bool) Option   { return func(p *Pool) { p.cfg.RecoverPanics = enabled } }
func WithAutoScale(enabled bool) Option       { return func(p *Pool) { p.cfg.EnableAutoScale = enabled } }
func WithWorkerInit(fn WorkerInitFunc) Option { return func(p *Pool) { p.workerInit = fn } }
func WithSpillover(dir string, maxJobs int) Option {
	return func(p *Pool) { p.spillPath = dir; p.spillMax = maxJobs }
}
func WithWorkerClose(fn WorkerCloseFunc) Option { return func(p *Pool) { p.workerClose = fn } }

// Presets.
func PresetLowLatency() Option {
	return func(p *Pool) {
		p.cfg.MinWorkers = runtimeWorkers()
		p.cfg.MaxWorkers = p.cfg.MinWorkers
		p.cfg.QueueSize = 8192
		p.resetQueues(p.cfg.QueueSize)
		p.cfg.EnableAutoScale = false
		p.cfg.EnableJobTracking = false
		p.cfg.EnableJobIDs = false
		p.cfg.Backpressure = BackpressureReject
	}
}
func PresetHighThroughput() Option {
	return func(p *Pool) {
		p.cfg.MinWorkers = runtimeWorkers() * 2
		p.cfg.MaxWorkers = p.cfg.MinWorkers * 4
		p.cfg.QueueSize = 262144
		p.resetQueues(p.cfg.QueueSize)
		p.cfg.EnableAutoScale = true
		p.cfg.EnableJobTracking = false
	}
}
func PresetReliable() Option {
	return func(p *Pool) {
		p.cfg.MinWorkers = 4
		p.cfg.MaxWorkers = 64
		p.cfg.QueueSize = 100000
		p.resetQueues(p.cfg.QueueSize)
		p.cfg.EnableJobTracking = true
		p.cfg.EnableJobIDs = true
		p.cfg.DefaultMaxAttempts = 3
	}
}
func PresetBackgroundQueue() Option {
	return func(p *Pool) {
		p.cfg.MinWorkers = 2
		p.cfg.MaxWorkers = 32
		p.cfg.QueueSize = 100000
		p.resetQueues(p.cfg.QueueSize)
		p.cfg.Backpressure = BackpressureBlock
	}
}
func PresetAPIBuffer() Option {
	return func(p *Pool) {
		p.cfg.MinWorkers = 4
		p.cfg.MaxWorkers = 128
		p.cfg.QueueSize = 250000
		p.resetQueues(p.cfg.QueueSize)
		p.cfg.EnableJobTracking = true
		p.cfg.EnableJobIDs = true
		p.cfg.Backpressure = BackpressureReject
	}
}

func runtimeWorkers() int {
	n := 4
	if v := os.Getenv("GOMAXPROCS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			n = i
		}
	}
	return n
}

// Request adapters.
func (p *Pool) SubmitBytes(data []byte, h func(context.Context, []byte) error, opts ...JobOptions) error {
	b := append([]byte(nil), data...)
	return p.SubmitFunc(func(ctx context.Context) error { return h(ctx, b) }, opts...)
}
func (p *Pool) SubmitJSON(data []byte, dst any, h func(context.Context, any) error, opts ...JobOptions) error {
	b := append([]byte(nil), data...)
	return p.SubmitFunc(func(ctx context.Context) error {
		if dst != nil {
			if err := json.Unmarshal(b, dst); err != nil {
				return Permanent(err)
			}
		}
		return h(ctx, dst)
	}, opts...)
}
func (p *Pool) SubmitMap(m map[string]any, h func(context.Context, map[string]any) error, opts ...JobOptions) error {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return p.SubmitFunc(func(ctx context.Context) error { return h(ctx, cp) }, opts...)
}
func (p *Pool) SubmitStruct(v any, h func(context.Context, any) error, opts ...JobOptions) error {
	return p.SubmitAny(v, h, opts...)
}
func SubmitChannel[T any](p *Pool, ctx context.Context, ch <-chan T, h func(context.Context, T) error, opts ...JobOptions) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			vv := v
			if err := p.SubmitFunc(func(c context.Context) error { return h(c, vv) }, opts...); err != nil {
				return err
			}
		}
	}
}

// HTTP async adapter.
type HTTPOptions struct {
	Queue          string
	AcceptedCode   int
	MaxBodySize    int64
	CaptureHeaders []string
	RateLimitKey   string
	Timeout        time.Duration
	MaxAttempts    int
	RespondJobID   bool
	Handler        func(context.Context, HTTPRequest) error
}
type HTTPRequest struct {
	Method     string      `json:"method"`
	URL        string      `json:"url"`
	Header     http.Header `json:"header,omitempty"`
	Body       []byte      `json:"body,omitempty"`
	RemoteAddr string      `json:"remote_addr,omitempty"`
	ReceivedAt time.Time   `json:"received_at"`
}

func HTTPMiddleware(p *Pool, opt HTTPOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		max := opt.MaxBodySize
		if max <= 0 {
			max = 1 << 20
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, max+1))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if int64(len(body)) > max {
			http.Error(w, "request body too large", 413)
			return
		}
		hdr := http.Header{}
		for _, k := range opt.CaptureHeaders {
			if v := r.Header.Values(k); len(v) > 0 {
				hdr[k] = append([]string(nil), v...)
			}
		}
		req := HTTPRequest{Method: r.Method, URL: r.URL.String(), Header: hdr, Body: body, RemoteAddr: r.RemoteAddr, ReceivedAt: time.Now()}
		handler := opt.Handler
		if handler == nil {
			handler = func(context.Context, HTTPRequest) error { return nil }
		}
		jo := JobOptions{Queue: opt.Queue, RateLimitKey: opt.RateLimitKey, Timeout: opt.Timeout, MaxAttempts: opt.MaxAttempts}
		if opt.RespondJobID {
			jo.ID = "job-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		err = p.SubmitFunc(func(ctx context.Context) error { return handler(ctx, req) }, jo)
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		code := opt.AcceptedCode
		if code == 0 {
			code = http.StatusAccepted
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "job_id": jo.ID})
	})
}

// Job builder.
type JobBuilder struct {
	pool *Pool
	job  Job
	opt  JobOptions
}

func (p *Pool) Job(job Job) *JobBuilder                            { return &JobBuilder{pool: p, job: job} }
func (p *Pool) JobFunc(fn func(context.Context) error) *JobBuilder { return p.Job(JobFunc(fn)) }
func (b *JobBuilder) Queue(name string) *JobBuilder                { b.opt.Queue = name; return b }
func (b *JobBuilder) Priority(v Priority) *JobBuilder              { b.opt.Priority = v; return b }
func (b *JobBuilder) Timeout(d time.Duration) *JobBuilder          { b.opt.Timeout = d; return b }
func (b *JobBuilder) Retry(n int) *JobBuilder                      { b.opt.MaxAttempts = n; return b }
func (b *JobBuilder) Backoff(v Backoff) *JobBuilder                { b.opt.Backoff = v; return b }
func (b *JobBuilder) Delay(d time.Duration) *JobBuilder            { b.opt.Delay = d; return b }
func (b *JobBuilder) At(t time.Time) *JobBuilder                   { b.opt.RunAt = t; return b }
func (b *JobBuilder) RateLimit(k string) *JobBuilder               { b.opt.RateLimitKey = k; return b }
func (b *JobBuilder) ConcurrencyKey(k string, max int) *JobBuilder {
	b.opt.ConcurrencyKey = k
	b.opt.MaxConcurrentPerKey = max
	return b
}
func (b *JobBuilder) ID(id string) *JobBuilder            { b.opt.ID = id; return b }
func (b *JobBuilder) IdempotencyKey(k string) *JobBuilder { b.opt.IdempotencyKey = k; return b }
func (b *JobBuilder) Metadata(k, v string) *JobBuilder {
	if b.opt.Metadata == nil {
		b.opt.Metadata = map[string]string{}
	}
	b.opt.Metadata[k] = v
	return b
}
func (b *JobBuilder) Callback(cb *Callback) *JobBuilder { b.opt.Callback = cb; return b }
func (b *JobBuilder) Submit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return b.pool.Submit(b.job, b.opt)
	}
}

// Idempotency conveniences.
func (p *Pool) Once(key string, job Job, opts ...JobOptions) error {
	opt := firstOptions(opts)
	opt.IdempotencyKey = key
	return p.Submit(job, opt)
}
func (p *Pool) OncePer(key string, ttl time.Duration, job Job, opts ...JobOptions) error {
	if ttl <= 0 {
		ttl = time.Minute
	}
	expKey := fmt.Sprintf("%s:%d", key, time.Now().UnixNano()/int64(ttl))
	return p.Once(expKey, job, opts...)
}
func (p *Pool) Replace(key string, job Job, opts ...JobOptions) error {
	p.idemp.Delete(key)
	return p.Once(key, job, opts...)
}
func (p *Pool) JoinExisting(key string, job Job, opts ...JobOptions) error {
	return p.Once(key, job, opts...)
}

// Runtime reconfiguration.
func (p *Pool) UpdateRateLimit(key string, r RateLimit) { p.SetRateLimit(key, r) }
func (p *Pool) Resize(workers int) {
	if workers <= 0 {
		return
	}
	p.cfg.MinWorkers = workers
	if p.cfg.MaxWorkers < workers {
		p.cfg.MaxWorkers = workers
	}
	for int(p.workers.Load()) < workers && p.State() == stateRunning {
		p.spawnWorker()
	}
}
func (p *Pool) UpdateQueueWeight(name string, priority Priority) {
	p.queueMu.Lock()
	q := p.queueDefaults[name]
	q.Priority = priority
	q.Name = name
	p.queueDefaults[name] = q
	p.queueMu.Unlock()
}

// Diagnostics.
type Diagnosis struct {
	Healthy         bool   `json:"healthy"`
	Saturated       bool   `json:"saturated"`
	State           string `json:"state"`
	QueueDepth      int    `json:"queue_depth"`
	QueueCapacity   int    `json:"queue_capacity"`
	Workers         int    `json:"workers"`
	BusyWorkers     int    `json:"busy_workers"`
	OldestQueuedAge string `json:"oldest_queued_age,omitempty"`
	Recommendation  string `json:"recommendation,omitempty"`
}

func (p *Pool) Diagnose() Diagnosis {
	st := p.Stats()
	d := Diagnosis{Healthy: p.IsRunning() || p.IsPaused(), State: p.State().String(), QueueDepth: st.QueueDepth, QueueCapacity: p.cfg.QueueSize, Workers: st.Workers, BusyWorkers: st.BusyWorkers}
	if p.cfg.QueueSize > 0 && st.QueueDepth*100/p.cfg.QueueSize >= 80 {
		d.Saturated = true
		d.Recommendation = "queue is near capacity; increase QueueSize, add workers, or use queued rate limits/spillover"
	} else if st.BusyWorkers >= st.Workers && st.QueueDepth > 0 {
		d.Saturated = true
		d.Recommendation = "all workers are busy; increase workers or reduce job latency"
	} else {
		d.Recommendation = "pool is healthy"
	}
	return d
}
func (p *Pool) ExplainSaturation() string {
	d := p.Diagnose()
	if !d.Saturated {
		return "Pool is healthy: " + d.Recommendation
	}
	return fmt.Sprintf("Pool is saturated: state=%s queue=%d/%d busy=%d/%d. Recommendation: %s", d.State, d.QueueDepth, d.QueueCapacity, d.BusyWorkers, d.Workers, d.Recommendation)
}
func (p *Pool) Dump() string {
	b, _ := json.MarshalIndent(map[string]any{"stats": p.Stats(), "diagnosis": p.Diagnose(), "jobs": p.ActiveJobs()}, "", "  ")
	return string(b)
}

// Config file support. JSON is fully supported; YAML supports simple key: value nesting used in README examples.
type FileConfig struct {
	Name    string `json:"name"`
	Workers struct {
		Min int `json:"min"`
		Max int `json:"max"`
	} `json:"workers"`
	Queue struct {
		Size         int    `json:"size"`
		Backpressure string `json:"backpressure"`
	} `json:"queue"`
	Tracking bool `json:"tracking"`
}

func FromConfigFile(path string, opts ...Option) (*Pool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fc := FileConfig{}
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(b, &fc); err != nil {
			return nil, err
		}
	} else {
		fc = parseSimpleYAML(string(b))
	}
	c := DefaultConfig()
	if fc.Name != "" {
		c.Name = fc.Name
	}
	if fc.Workers.Min > 0 {
		c.MinWorkers = fc.Workers.Min
	}
	if fc.Workers.Max > 0 {
		c.MaxWorkers = fc.Workers.Max
	}
	if fc.Queue.Size > 0 {
		c.QueueSize = fc.Queue.Size
	}
	if fc.Tracking {
		c.EnableJobTracking = true
		c.EnableJobIDs = true
	}
	switch strings.ToLower(fc.Queue.Backpressure) {
	case "reject":
		c.Backpressure = BackpressureReject
	case "drop_oldest":
		c.Backpressure = BackpressureDropOldest
	case "drop_newest":
		c.Backpressure = BackpressureDropNewest
	}
	return New(c, opts...)
}
func parseSimpleYAML(s string) FileConfig {
	var fc FileConfig
	section := ""
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.Split(line, "#")[0])
		if line == "" {
			continue
		}
		if strings.HasSuffix(line, ":") {
			section = strings.TrimSuffix(line, ":")
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), "\"")
		atoi := func() int { i, _ := strconv.Atoi(v); return i }
		switch section + "." + k {
		case ".name":
			fc.Name = v
		case "workers.min":
			fc.Workers.Min = atoi()
		case "workers.max":
			fc.Workers.Max = atoi()
		case "queue.size":
			fc.Queue.Size = atoi()
		case "queue.backpressure":
			fc.Queue.Backpressure = v
		case ".tracking":
			fc.Tracking = v == "true"
		}
	}
	return fc
}

// Application integration.
func ShutdownOnSignal(p *Pool, timeout time.Duration, sigs ...os.Signal) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if len(sigs) == 0 {
		sigs = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigs...)
	<-ch
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.Shutdown(ctx)
}
func RunUntilSignal(p *Pool) error {
	if err := p.Start(); err != nil {
		return err
	}
	return ShutdownOnSignal(p, 30*time.Second)
}
func MustShutdown(p *Pool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		panic(err)
	}
}

// Testing helpers.
type TestPool struct{ *Pool }

func NewTestPool() *TestPool {
	p := MustNewPool(WithWorkers(1), WithQueueSize(1024), WithAutoScale(false), WithJobTracking(true))
	_ = p.Start()
	return &TestPool{Pool: p}
}
func (t *TestPool) RunAll(ctx context.Context) error { return t.WaitIdle(ctx) }
func (t *TestPool) WaitForJobs(ctx context.Context, n uint64) error {
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if t.Stats().Completed >= n {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// Worker local state.
type WorkerInitFunc func(context.Context, *Worker) error
type WorkerCloseFunc func(context.Context, *Worker) error
type Worker struct {
	ID     int
	values sync.Map
}

func (w *Worker) Set(key string, value any)  { w.values.Store(key, value) }
func (w *Worker) Get(key string) (any, bool) { return w.values.Load(key) }
