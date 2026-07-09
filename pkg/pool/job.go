package pool

import (
	"context"
	"sync/atomic"
	"time"
)

type Job interface{ Run(context.Context) error }

type JobFunc func(context.Context) error

func (fn JobFunc) Run(ctx context.Context) error { return fn(ctx) }

// TaskFunc processes a reusable argument without creating a per-submit closure.
// It is the zero-allocation alternative to SubmitFunc when the work needs state.
type TaskFunc[T any] func(context.Context, *T) error

// Task is a small value-type Job. Passing Task[T] to Submit/SubmitFast avoids
// closure heap allocation while still carrying state through Arg.
type Task[T any] struct {
	Arg *T
	Fn  TaskFunc[T]
}

func (t Task[T]) Run(ctx context.Context) error {
	if t.Fn == nil {
		return nil
	}
	return t.Fn(ctx, t.Arg)
}

// AnyHandler can process arbitrary request payloads accepted by SubmitAny.
type AnyHandler func(context.Context, any) error

// AnyJob adapts an arbitrary payload and handler into a Job.
type AnyJob struct {
	Payload any
	Handler AnyHandler
}

func (j AnyJob) Run(ctx context.Context) error {
	if j.Handler == nil {
		return nil
	}
	return j.Handler(ctx, j.Payload)
}

type Priority int

const (
	PriorityLow      Priority = -10
	PriorityNormal   Priority = 0
	PriorityHigh     Priority = 10
	PriorityCritical Priority = 100
)

type JobOptions struct {
	ID                  string
	Queue               string
	Priority            Priority
	Delay               time.Duration
	RunAt               time.Time
	Timeout             time.Duration
	MaxAttempts         int
	Backoff             Backoff
	Metadata            map[string]string
	IdempotencyKey      string
	ConcurrencyKey      string
	MaxConcurrentPerKey int
	RateLimitKey        string
	Result              any
	Callback            *Callback
	LifecycleMetadata   map[string]string
}

type JobState uint32

const (
	JobQueued JobState = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobRetrying
	JobDeadLettered
	JobCancelled
)

func (s JobState) String() string {
	switch s {
	case JobQueued:
		return "queued"
	case JobRunning:
		return "running"
	case JobSucceeded:
		return "succeeded"
	case JobFailed:
		return "failed"
	case JobRetrying:
		return "retrying"
	case JobDeadLettered:
		return "dead_lettered"
	case JobCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

type JobInfo struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue,omitempty"`
	State       string            `json:"state"`
	Priority    Priority          `json:"priority"`
	Attempts    int               `json:"attempts"`
	MaxAttempts int               `json:"max_attempts"`
	CreatedAt   time.Time         `json:"created_at"`
	RunAt       time.Time         `json:"run_at"`
	StartedAt   time.Time         `json:"started_at,omitempty"`
	FinishedAt  time.Time         `json:"finished_at,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Progress    Progress          `json:"progress,omitempty"`
}

type queuedJob struct {
	seq         uint64
	job         Job
	opts        JobOptions
	createdAt   time.Time
	runAt       time.Time
	startedAt   time.Time
	finishedAt  time.Time
	attempt     int
	future      *futureAny
	ctx         context.Context
	cancel      context.CancelFunc
	state       atomic.Uint32
	lastErr     atomic.Value // string
	rateReadyAt time.Time
	progress    atomic.Value // Progress
	pool        *Pool
}

func (q *queuedJob) info() JobInfo {
	errText, _ := q.lastErr.Load().(string)
	pr, _ := q.progress.Load().(Progress)
	return JobInfo{ID: q.opts.ID, Queue: q.opts.Queue, State: JobState(q.state.Load()).String(), Priority: q.opts.Priority, Attempts: q.attempt, MaxAttempts: q.opts.MaxAttempts, CreatedAt: q.createdAt, RunAt: q.runAt, StartedAt: q.startedAt, FinishedAt: q.finishedAt, LastError: errText, Metadata: cloneMetadata(q.opts.Metadata), Progress: pr}
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
