package fastworker

import (
	"context"
	"errors"
	"sync"
)

type BatchResult[T any] struct {
	Values []T
	Errors []error
}

func Map[I any, O any](ctx context.Context, p *Pool, items []I, fn func(context.Context, I) (O, error), opts ...JobOptions) (BatchResult[O], error) {
	res := BatchResult[O]{Values: make([]O, len(items)), Errors: make([]error, len(items))}
	var wg sync.WaitGroup
	for i, item := range items {
		i, item := i, item
		wg.Add(1)
		err := p.SubmitFunc(func(c context.Context) error {
			defer wg.Done()
			v, err := fn(c, item)
			res.Values[i] = v
			res.Errors[i] = err
			return err
		}, opts...)
		if err != nil {
			wg.Done()
			res.Errors[i] = err
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return res, errors.Join(res.Errors...)
	case <-ctx.Done():
		return res, ctx.Err()
	}
}
