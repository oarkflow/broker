package broker

import (
	"context"
	"sync"
	"time"
)

// TTLSweeperConfig configures queue message TTL sweeping.
type TTLSweeperConfig struct {
	Interval    time.Duration `json:"interval"`
	ExpireToDLQ bool          `json:"expire_to_dlq"`
	BatchSize   int           `json:"batch_size"`
}

// TTLSweepResult summarizes the results of a TTL sweep iteration.
type TTLSweepResult struct {
	Inspected int `json:"inspected"`
	Expired   int `json:"expired"`
	DeadLettered int `json:"dead_lettered"`
	Purged    int `json:"purged"`
}

// TTLSweeper inspects queue messages and cleans up expired messages.
type TTLSweeper struct {
	b       *Broker
	cfg     TTLSweeperConfig
	mu      sync.Mutex
	stopCh  chan struct{}
	running bool
}

// NewTTLSweeper creates a new queue message TTL sweeper.
func NewTTLSweeper(b *Broker, cfg TTLSweeperConfig) *TTLSweeper {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &TTLSweeper{
		b:      b,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start runs the TTL sweeper loop in the background.
func (ts *TTLSweeper) Start(ctx context.Context) {
	ts.mu.Lock()
	if ts.running {
		ts.mu.Unlock()
		return
	}
	ts.running = true
	ts.mu.Unlock()

	go func() {
		ticker := time.NewTicker(ts.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ts.stopCh:
				return
			case <-ticker.C:
				_, _ = ts.Sweep(ctx)
			}
		}
	}()
}

// Stop stops the background sweeper.
func (ts *TTLSweeper) Stop() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if !ts.running {
		return
	}
	close(ts.stopCh)
	ts.running = false
}

// Sweep inspects all queues for expired messages and moves them to DLQ or purges them.
func (ts *TTLSweeper) Sweep(ctx context.Context) (TTLSweepResult, error) {
	stats, err := ts.b.Store().Stats(ctx)
	if err != nil {
		return TTLSweepResult{}, err
	}

	res := TTLSweepResult{}
	now := time.Now()

	for queue := range stats.Queues {
		// Do not sweep DLQs themselves to prevent loops
		if isDLQName(queue) {
			continue
		}

		msgs, err := ts.b.Store().ListQueue(ctx, queue, ts.cfg.BatchSize)
		if err != nil {
			continue
		}

		for _, m := range msgs {
			res.Inspected++
			if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
				res.Expired++
				dlq := queue + ".dlq"
				if ts.cfg.ExpireToDLQ {
					// Move to DLQ
					_, err := ts.b.Store().Move(ctx, MoveRequest{
						SourceType:      DestinationQueue,
						Source:          queue,
						DestinationType: DestinationQueue,
						Destination:     dlq,
						MessageIDs:      []string{m.ID},
						Mode:            MoveRemoveSource,
						PreserveID:      true,
						Reason:          "ttl_expired",
					})
					if err == nil {
						res.DeadLettered++
					} else {
						// Fallback: Nack to DLQ directly
						_ = ts.b.Store().Nack(ctx, NackRequest{
							Queue:            queue,
							MessageID:        m.ID,
							Action:           NackDLQ,
							DLQ:              dlq,
							Reason:           "ttl_expired",
							Error:            "message TTL expired",
							IncrementAttempt: false,
						})
						res.DeadLettered++
					}

					if ts.b.notify != nil {
						ts.b.notify.Publish(Notification{
							Type:      "message.expired",
							Topic:     "message.expired",
							Queue:     queue,
							MessageID: m.ID,
							Timestamp: now,
							Data: map[string]any{
								"reason": "ttl_expired",
								"dlq":    dlq,
							},
						})
					}
				} else {
					// Purge expired message
					_ = ts.b.Store().Ack(ctx, AckRequest{
						Queue:     queue,
						MessageID: m.ID,
					})
					res.Purged++

					if ts.b.notify != nil {
						ts.b.notify.Publish(Notification{
							Type:      "message.expired",
							Topic:     "message.expired",
							Queue:     queue,
							MessageID: m.ID,
							Timestamp: now,
							Data: map[string]any{
								"reason": "ttl_expired",
								"action": "purged",
							},
						})
					}
				}
			}
		}
	}

	return res, nil
}

func isDLQName(name string) bool {
	return len(name) >= 4 && (name[len(name)-4:] == ".dlq" || name[len(name)-4:] == "-dlq")
}
