package broker

import (
	"context"
	"sync"
)

type PublishResult struct {
	MessageID    string        `json:"message_id"`
	Messages     []Message     `json:"messages,omitempty"`
	Destinations []Destination `json:"destinations,omitempty"`
	Confirmed    ConfirmLevel  `json:"confirmed"`
	Duplicate    bool          `json:"duplicate,omitempty"`
}
type BatchPublishResult struct {
	Results []PublishResult `json:"results"`
	Errors  []string        `json:"errors,omitempty"`
}
type ScheduleResult struct {
	MessageID  string `json:"message_id"`
	ScheduleID string `json:"schedule_id"`
	RunAt      string `json:"run_at"`
}

type ResponseStatus string

const (
	ResponseOK      ResponseStatus = "ok"
	ResponseError   ResponseStatus = "error"
	ResponseTimeout ResponseStatus = "timeout"
)

type Response struct {
	MessageID     string            `json:"message_id"`
	CorrelationID string            `json:"correlation_id"`
	Payload       []byte            `json:"payload,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Error         *BrokerError      `json:"error,omitempty"`
	Status        ResponseStatus    `json:"status"`
}

type PublishFuture interface {
	ID() string
	Done() <-chan struct{}
	Wait(context.Context) (PublishResult, error)
	Cancel(context.Context) error
}
type BatchPublishFuture interface {
	Done() <-chan struct{}
	Wait(context.Context) (BatchPublishResult, error)
	Cancel(context.Context) error
}
type ResponseFuture interface {
	RequestID() string
	CorrelationID() string
	Done() <-chan struct{}
	Wait(context.Context) (Response, error)
	Cancel(context.Context) error
}

type futureResult[T any] struct {
	id     string
	corr   string
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	val    T
	err    error
	cancel context.CancelFunc
}

func newFutureResult[T any](id string) *futureResult[T] {
	return &futureResult[T]{id: id, done: make(chan struct{})}
}
func (f *futureResult[T]) complete(v T, err error) {
	f.once.Do(func() { f.mu.Lock(); f.val = v; f.err = err; f.mu.Unlock(); close(f.done) })
}
func (f *futureResult[T]) ID() string            { return f.id }
func (f *futureResult[T]) RequestID() string     { return f.id }
func (f *futureResult[T]) CorrelationID() string { return f.corr }
func (f *futureResult[T]) Done() <-chan struct{} { return f.done }
func (f *futureResult[T]) Wait(ctx context.Context) (T, error) {
	var zero T
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.val, f.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
func (f *futureResult[T]) Cancel(ctx context.Context) error {
	if f.cancel != nil {
		f.cancel()
	}
	var zero T
	f.complete(zero, ctx.Err())
	return nil
}
