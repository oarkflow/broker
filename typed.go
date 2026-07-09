package fastworker

import "context"

type Handler[I any, O any] func(context.Context, I) (O, error)

type TypedPool[I any, O any] struct {
	pool    *Pool
	handler Handler[I, O]
}

func NewTyped[I any, O any](cfg Config, h Handler[I, O], opts ...Option) (*TypedPool[I, O], error) {
	p, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	return &TypedPool[I, O]{pool: p, handler: h}, nil
}
func MustNewTyped[I any, O any](cfg Config, h Handler[I, O], opts ...Option) *TypedPool[I, O] {
	tp, err := NewTyped[I, O](cfg, h, opts...)
	if err != nil {
		panic(err)
	}
	return tp
}
func (t *TypedPool[I, O]) Start() error                   { return t.pool.Start() }
func (t *TypedPool[I, O]) Pause() error                   { return t.pool.Pause() }
func (t *TypedPool[I, O]) Resume() error                  { return t.pool.Resume() }
func (t *TypedPool[I, O]) Stop(ctx context.Context) error { return t.pool.Stop(ctx) }
func (t *TypedPool[I, O]) Terminate() error               { return t.pool.Terminate() }
func (t *TypedPool[I, O]) Stats() Stats                   { return t.pool.Stats() }
func (t *TypedPool[I, O]) Submit(ctx context.Context, in I, opts ...JobOptions) (Future[O], error) {
	return SubmitResult[O](t.pool, func(c context.Context) (O, error) {
		select {
		case <-ctx.Done():
			var z O
			return z, ctx.Err()
		default:
		}
		return t.handler(c, in)
	}, opts...)
}
func (t *TypedPool[I, O]) Pool() *Pool { return t.pool }
