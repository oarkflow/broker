package main

import (
	"context"
	"fmt"
	"time"
)

func receiveN[T any](ctx context.Context, ch <-chan T, n int) ([]T, error) {
	values := make([]T, 0, n)
	for len(values) < n {
		select {
		case value := <-ch:
			values = append(values, value)
		case <-ctx.Done():
			return nil, fmt.Errorf("received %d/%d values: %w", len(values), n, ctx.Err())
		}
	}
	return values, nil
}

func stopContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
