package fastworker

import (
	"context"
	"sync"
)

type Future[T any] interface {
	Get(context.Context) (T, error)
	Done() <-chan struct{}
	Cancel() bool
}

type futureAny struct {
	once   sync.Once
	done   chan struct{}
	mu     sync.Mutex
	val    any
	err    error
	cancel context.CancelFunc
}

func newFutureAny() *futureAny { return &futureAny{done: make(chan struct{})} }

func (f *futureAny) setValue(v any) {
	f.mu.Lock()
	f.val = v
	f.mu.Unlock()
}

func (f *futureAny) complete(v any, err error) {
	f.once.Do(func() {
		f.mu.Lock()
		if v != nil {
			f.val = v
		}
		f.err = err
		f.mu.Unlock()
		close(f.done)
	})
}
func (f *futureAny) Done() <-chan struct{} { return f.done }
func (f *futureAny) Cancel() bool {
	if f.cancel != nil {
		f.cancel()
	}
	var ok bool
	f.once.Do(func() {
		ok = true
		f.mu.Lock()
		f.err = ErrCancelled
		f.mu.Unlock()
		close(f.done)
	})
	return ok
}

type futureTyped[T any] struct{ inner *futureAny }

func (f futureTyped[T]) Done() <-chan struct{} { return f.inner.Done() }
func (f futureTyped[T]) Cancel() bool          { return f.inner.Cancel() }
func (f futureTyped[T]) Get(ctx context.Context) (T, error) {
	var zero T
	select {
	case <-f.inner.done:
		f.inner.mu.Lock()
		defer f.inner.mu.Unlock()
		if f.inner.err != nil {
			return zero, f.inner.err
		}
		if f.inner.val == nil {
			return zero, nil
		}
		v, ok := f.inner.val.(T)
		if !ok {
			return zero, nil
		}
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type resultJob[T any] struct {
	fn func(context.Context) (T, error)
	f  *futureAny
}

func (r resultJob[T]) Run(ctx context.Context) error {
	v, err := r.fn(ctx)
	if err == nil && r.f != nil {
		r.f.setValue(v)
	}
	return err
}
