package broker

import (
	"context"
	"sync"
	"time"
)

// publishCoalescer lets every caller keep publishing one message at a time
// through the normal Publish API — no client-side batching, no -batch flag
// — while the broker transparently coalesces whatever concurrent single
// publishes land at the same instant into one store.AppendBatch call (one
// lock, one WAL fsync per shard) and one NotificationBus.PublishMany call,
// instead of paying for those per message. It's the same opportunistic
// group-commit idea as groupCommitWriter, applied one level up the publish
// pipeline. Each caller still gets its own per-message success/error;
// nothing about the public Publish/PublishBatch semantics changes.
type publishCoalescer struct {
	store  Store
	notify *NotificationBus

	closeMu sync.RWMutex
	closed  bool
	reqCh   chan *publishReq
	doneCh  chan struct{}
}

type publishReq struct {
	msg   Message
	dests []Destination
	done  chan publishOutcome
}

type publishOutcome struct {
	messages []Message
	err      error
}

const publishCoalesceMaxBatch = 4096

func newPublishCoalescer(store Store, notify *NotificationBus) *publishCoalescer {
	c := &publishCoalescer{
		store:  store,
		notify: notify,
		reqCh:  make(chan *publishReq, 4096),
		doneCh: make(chan struct{}),
	}
	go c.run()
	return c
}

func (c *publishCoalescer) run() {
	defer close(c.doneCh)
	for req := range c.reqCh {
		batch := make([]*publishReq, 0, 64)
		batch = append(batch, req)
	drain:
		for len(batch) < publishCoalesceMaxBatch {
			select {
			case r2, ok := <-c.reqCh:
				if !ok {
					break drain
				}
				batch = append(batch, r2)
			default:
				break drain
			}
		}
		c.processBatch(batch)
	}
}

func (c *publishCoalescer) processBatch(batch []*publishReq) {
	if bs, ok := c.store.(BatchAppendStore); ok && len(batch) > 1 {
		entries := make([]AppendEntry, len(batch))
		for i, r := range batch {
			entries[i] = AppendEntry{Message: r.msg, Destinations: r.dests}
		}
		results := bs.AppendBatch(context.Background(), entries)
		notifs := make([]Notification, 0, len(batch))
		for i, r := range batch {
			res := results[i]
			if res.Err == nil {
				notifs = append(notifs, publishedNotification(r.msg))
			}
			r.done <- publishOutcome{messages: res.Messages, err: res.Err}
		}
		if c.notify != nil && len(notifs) > 0 {
			c.notify.PublishMany(notifs)
		}
		return
	}
	// Fallback for stores without batch append (custom Store
	// implementations) or a lone request: behaves exactly like calling
	// Append directly, just routed through the same code path.
	for _, r := range batch {
		out, err := c.store.Append(context.Background(), r.msg, r.dests)
		if err == nil && c.notify != nil {
			c.notify.Publish(publishedNotification(r.msg))
		}
		r.done <- publishOutcome{messages: out, err: err}
	}
}

func publishedNotification(msg Message) Notification {
	return Notification{
		Type: "message.published", Source: "publisher", Severity: SeverityInfo,
		MessageID: msg.ID, Topic: msg.Topic, Queue: msg.Queue,
		TenantID: msg.TenantID, TraceID: msg.TraceID, Timestamp: time.Now(),
	}
}

func (c *publishCoalescer) submit(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	c.closeMu.RLock()
	if c.closed {
		c.closeMu.RUnlock()
		return nil, ErrClosed
	}
	req := &publishReq{msg: msg, dests: dests, done: make(chan publishOutcome, 1)}
	c.reqCh <- req
	c.closeMu.RUnlock()
	select {
	case out := <-req.done:
		return out.messages, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *publishCoalescer) close() error {
	c.closeMu.Lock()
	if !c.closed {
		c.closed = true
		close(c.reqCh)
	}
	c.closeMu.Unlock()
	<-c.doneCh
	return nil
}
