package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	opBatchMax   = 256
	opBatchDelay = 500 * time.Microsecond
)

type opBatchResult struct {
	messages []Message
	err      error
}

type opBatchRequest struct {
	ctx     context.Context
	payload any
	result  chan opBatchResult
}

// opBatcher coalesces independent durable operations. Each caller still waits
// for its own WAL fsync, but one group-commit fsync can now confirm many
// concurrent operations. The store callback owns ordering and applies the
// prepared batch under writeMu.
type opBatcher struct {
	mu       sync.Mutex
	closed   bool
	requests chan *opBatchRequest
	process  func([]*opBatchRequest)
	done     chan struct{}
}

func newOpBatcher(process func([]*opBatchRequest)) *opBatcher {
	b := &opBatcher{
		requests: make(chan *opBatchRequest, 4096),
		process:  process,
		done:     make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *opBatcher) submit(ctx context.Context, payload any) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req := &opBatchRequest{
		ctx:     ctx,
		payload: payload,
		result:  make(chan opBatchResult, 1),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	select {
	case b.requests <- req:
		b.mu.Unlock()
	case <-ctx.Done():
		b.mu.Unlock()
		return nil, ctx.Err()
	}
	select {
	case result := <-req.result:
		return result.messages, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *opBatcher) run() {
	defer close(b.done)
	for first := range b.requests {
		batch := make([]*opBatchRequest, 1, opBatchMax)
		batch[0] = first
		timer := time.NewTimer(opBatchDelay)
	collect:
		for len(batch) < opBatchMax {
			select {
			case req, ok := <-b.requests:
				if !ok {
					break collect
				}
				batch = append(batch, req)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.process(batch)
	}
}

func (b *opBatcher) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.requests)
	}
	b.mu.Unlock()
	<-b.done
}

func deliverOpBatchError(batch []*opBatchRequest, err error) {
	if err == nil {
		err = errors.New("op batch failed")
	}
	for _, req := range batch {
		req.result <- opBatchResult{err: err}
	}
}
