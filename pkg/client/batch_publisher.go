package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/oarkflow/broker"
)

// ErrBatchPublisherClosed is returned by Publish after Close.
var ErrBatchPublisherClosed = errors.New("client: batch publisher closed")

// BatchPublisherConfig tunes request coalescing. Zero values pick defaults.
type BatchPublisherConfig struct {
	// MaxBatch caps messages per request. Default 512.
	MaxBatch int
	// MaxInFlight is how many batch requests may be outstanding at once, so a
	// new batch fills while earlier ones wait on the broker's fsync. Default 4.
	MaxInFlight int
	// QueueSize bounds messages waiting to be batched; Publish blocks (or
	// honors ctx) when it is full. Default 8192.
	QueueSize int
}

type batchItem struct {
	msg  broker.Message
	done chan error
}

// BatchPublisher turns many concurrent single-message Publish calls into a few
// frame-batch requests. Each caller still returns only after the broker has
// durably accepted its own message, with its own error, so durability and
// error semantics match one request per message while the request, header and
// fsync overhead is shared. There is no timer: while a batch is in flight,
// later messages accumulate and go out together, exactly like WAL group commit.
// A lone caller is sent immediately and never waits for company.
type BatchPublisher struct {
	c      *Client
	cfg    BatchPublisherConfig
	in     chan *batchItem
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

// NewBatchPublisher starts a coalescing publisher on top of c.
func (c *Client) NewBatchPublisher(cfg BatchPublisherConfig) *BatchPublisher {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 512
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 4
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
	p := &BatchPublisher{c: c, cfg: cfg, in: make(chan *batchItem, cfg.QueueSize)}
	for i := 0; i < cfg.MaxInFlight; i++ {
		p.wg.Add(1)
		go p.run()
	}
	return p
}

// Publish sends one message and waits for the broker's durable acknowledgement
// of that message. Set msg.ID to make retries idempotent; with return=minimal
// the broker does not echo ids back. If ctx ends while waiting, the message may
// still be accepted by the broker.
func (p *BatchPublisher) Publish(ctx context.Context, msg broker.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	item := &batchItem{msg: msg, done: make(chan error, 1)}
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrBatchPublisherClosed
	}
	select {
	case p.in <- item:
		p.mu.RUnlock()
	case <-ctx.Done():
		p.mu.RUnlock()
		return ctx.Err()
	}
	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting messages, sends everything already queued and waits.
func (p *BatchPublisher) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.in)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *BatchPublisher) run() {
	defer p.wg.Done()
	batch := make([]*batchItem, 0, p.cfg.MaxBatch)
	for first := range p.in {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < p.cfg.MaxBatch {
			select {
			case next, ok := <-p.in:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}
		p.send(batch)
	}
}

func (p *BatchPublisher) send(batch []*batchItem) {
	msgs := make([]broker.Message, len(batch))
	for i, it := range batch {
		msgs[i] = it.msg
	}
	body, err := broker.EncodeMessageFrameBatch(nil, p.c.cfg.FrameCodec, msgs)
	if err != nil {
		for _, it := range batch {
			it.done <- err
		}
		return
	}
	data, status, err := p.c.doFrameBatch(context.Background(), body)
	if err != nil {
		for _, it := range batch {
			it.done <- err
		}
		return
	}
	if status == http.StatusNoContent || len(data) == 0 {
		for _, it := range batch {
			it.done <- nil
		}
		return
	}
	var out struct {
		Errors []string `json:"errors"`
		Error  string   `json:"error"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		for _, it := range batch {
			it.done <- fmt.Errorf("client: unreadable batch response: %w", err)
		}
		return
	}
	for i, it := range batch {
		switch {
		case i < len(out.Errors) && out.Errors[i] != "":
			it.done <- errors.New(out.Errors[i])
		case len(out.Errors) == 0 && out.Error != "":
			it.done <- errors.New(out.Error)
		default:
			it.done <- nil
		}
	}
}

// doFrameBatch posts a frame batch with minimal responses and returns the body
// and status so per-message failures (207) can be mapped to callers.
func (c *Client) doFrameBatch(ctx context.Context, body []byte) ([]byte, int, error) {
	data, status, err := c.doRawStatus(ctx, http.MethodPost, "/v1/publish/frame/batch", "application/vnd.pool.broker.frames", body, "return=minimal")
	return data, status, err
}
