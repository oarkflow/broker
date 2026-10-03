package broker

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

type reopenableStore interface {
	Store
	Close() error
}

// TestPipelinedWALNoAcknowledgedLoss hammers a WAL store with concurrent
// appends, leases and acks (which now share fsyncs instead of serializing),
// then reopens it from disk. Every acknowledged append must be present, every
// acknowledged ack must stay completed, and every leased-but-unacked message
// must be deliverable again.
func TestPipelinedWALNoAcknowledgedLoss(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(dir string) (reopenableStore, error)
	}{
		{"binary", func(dir string) (reopenableStore, error) { return NewBinaryFileWALStore(dir) }},
		{"json", func(dir string) (reopenableStore, error) { return NewFileWALStore(dir) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
				t.Fatal(err)
			}
			const producers, perProducer = 16, 100
			var wg sync.WaitGroup
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for i := 0; i < perProducer; i++ {
						id := "m-" + strconv.Itoa(p) + "-" + strconv.Itoa(i)
						if _, err := store.Append(ctx, Message{ID: id, Topic: "t", Payload: []byte(id)}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
							t.Errorf("append %s: %v", id, err)
							return
						}
					}
				}(p)
			}
			wg.Wait()

			// Lease everything concurrently; ack roughly half, leave the rest in flight.
			var mu sync.Mutex
			acked := map[string]bool{}
			total := producers * perProducer
			var leased int
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					consumer := "c" + strconv.Itoa(w)
					for {
						mu.Lock()
						if leased >= total {
							mu.Unlock()
							return
						}
						mu.Unlock()
						msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: consumer, Limit: 5, VisibilityTimeout: time.Hour})
						if err != nil {
							t.Errorf("lease: %v", err)
							return
						}
						if len(msgs) == 0 {
							time.Sleep(time.Millisecond)
							continue
						}
						mu.Lock()
						leased += len(msgs)
						mu.Unlock()
						for i, m := range msgs {
							if i%2 == 1 {
								continue // stays leased, unacked
							}
							if err := store.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "q", LeaseToken: m.LeaseToken, ConsumerID: consumer}); err != nil {
								t.Errorf("ack %s: %v", m.ID, err)
								return
							}
							mu.Lock()
							acked[m.ID] = true
							mu.Unlock()
						}
					}
				}(w)
			}
			wg.Wait()
			if t.Failed() {
				return
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			completed, other := 0, 0
			for p := 0; p < producers; p++ {
				for i := 0; i < perProducer; i++ {
					id := "m-" + strconv.Itoa(p) + "-" + strconv.Itoa(i)
					m, ok, err := reopened.Get(ctx, id)
					if err != nil || !ok {
						t.Fatalf("acknowledged append %s lost after reopen: ok=%t err=%v", id, ok, err)
					}
					if acked[id] {
						if m.Status != MessageCompleted {
							t.Fatalf("acknowledged ack %s not durable: status=%s", id, m.Status)
						}
						completed++
					} else {
						if m.Status == MessageCompleted {
							t.Fatalf("%s completed without an ack", id)
						}
						other++
					}
				}
			}
			if completed+other != total || len(acked) != completed {
				t.Fatalf("accounting mismatch: completed=%d other=%d acked=%d total=%d", completed, other, len(acked), total)
			}
			// Unacked in-flight work must be redeliverable (at-least-once).
			redelivered := 0
			for redelivered < other {
				msgs, err := reopened.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "recover", Limit: 50, VisibilityTimeout: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) == 0 {
					break
				}
				redelivered += len(msgs)
			}
			if redelivered != other {
				t.Fatalf("redelivered %d of %d unacked messages", redelivered, other)
			}
		})
	}
}
