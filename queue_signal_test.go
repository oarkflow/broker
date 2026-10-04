package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A waiting consumer used to learn about newly available work only on its next
// poll tick, which put a floor on pickup latency equal to that interval: 25ms over
// HTTP long-poll, PollInterval for an embedded subscription. The store now signals
// the queue so a waiting consumer is handed work as soon as it arrives.

// pickupBudget is generous for CI but far below the 25ms long-poll floor, so a
// regression to polling shows up as a failure rather than as a slower test.
const pickupBudget = 15 * time.Millisecond

func TestEmbeddedConsumerIsWokenOnPublish(t *testing.T) {
	for _, pollInterval := range []time.Duration{time.Millisecond, 50 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(pollInterval.String(), func(t *testing.T) {
			ctx := context.Background()
			b, err := New(Config{PollInterval: pollInterval})
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer b.Stop(ctx)
			if err := b.DeclareQueue(ctx, QueueConfig{Name: "wake"}); err != nil {
				t.Fatal(err)
			}

			received := make(chan time.Duration, 1)
			started := time.Now()
			if _, err := b.Subscribe(ctx, ConsumerConfig{
				Name: "c1", Queue: "wake", Group: "g1", Concurrency: 1,
			}, func(cctx context.Context, d Delivery) error {
				received <- time.Since(started)
				return d.Ack(cctx)
			}); err != nil {
				t.Fatal(err)
			}

			// Let the subscription reach its select.
			time.Sleep(20 * time.Millisecond)
			started = time.Now()
			if _, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("wake")); err != nil {
				t.Fatal(err)
			}

			select {
			case d := <-received:
				if d > pickupBudget {
					t.Fatalf("pickup took %s with a %s poll interval; the consumer is still waiting out the ticker", d, pollInterval)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("consumer never received the message (poll interval %s)", pollInterval)
			}
		})
	}
}

func TestLongPollIsWokenOnPublish(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	admin := AdminServer{Broker: b, Token: "secret"}

	// Publish after the long poll has parked, so the measurement covers the wakeup
	// rather than a message that was already available.
	parked := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(parked)
		if _, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("wake")); err != nil {
			t.Errorf("publish: %v", err)
		}
	}()
	<-parked
	// Give the request time to reach the store and park on the signal.
	time.Sleep(60 * time.Millisecond)

	start := time.Now()
	body, err := json.Marshal(map[string]any{
		"queue": "wake", "consumer_id": "c1", "consumer_group": "g1", "limit": 10, "wait_ns": int64(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/poll", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	admin.dataPlane(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var msgs []Message
	if err := json.Unmarshal(rec.Body.Bytes(), &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("polled %d messages, want 1", len(msgs))
	}
	if elapsed > pickupBudget {
		t.Fatalf("long poll returned after %s; it waited out the 25ms retry tick instead of the store signal", elapsed)
	}
}

// The signal is advisory: it can be coalesced or missed, so the poll fallback has
// to remain. A message published while no consumer is waiting must still be found.
func TestPollFallbackStillFindsMessages(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}

	// Publish before any consumer exists, then subscribe. The signal fired with
	// nobody listening, so only the fallback can find it.
	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte(fmt.Sprintf("m%d", i)), WithQueue("q")); err != nil {
			t.Fatal(err)
		}
	}
	// Buffered for every message and sent without dropping: a size-one channel
	// with a non-blocking send silently discards a second delivery that arrives
	// before the reader drains the first, which made this test flaky.
	got := make(chan struct{}, 8)
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "late", Queue: "q", Group: "g1", Concurrency: 1, BatchSize: 3,
	}, func(cctx context.Context, d Delivery) error {
		got <- struct{}{}
		return d.Ack(cctx)
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	seen := 0
	for seen < 3 {
		select {
		case <-got:
			seen++
		case <-deadline:
			t.Fatalf("only received %d of 3 messages published before subscribing", seen)
		}
	}
}

// A queued notification must never block a publisher, however many consumers are
// attached, and must not accumulate.
func TestQueueSignalDoesNotBlockOrAccumulate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	ready := store.QueueReady("q")
	if ready == nil {
		t.Fatal("QueueReady returned nil for a declared queue")
	}
	// Nobody is receiving: repeated publishes must not block, and only one
	// notification may be pending.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			if _, err := store.Append(ctx, Message{
				ID: fmt.Sprintf("m-%d", i), Queue: "q", Payload: []byte("p"),
			}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishing blocked with no consumer draining the signal")
	}
	select {
	case <-ready:
	default:
		t.Fatal("expected one pending notification")
	}
	select {
	case <-ready:
		t.Fatal("more than one notification accumulated; the signal is not coalescing")
	default:
	}
}

// An undeclared queue has nothing to signal, and the caller must be able to tell.
func TestQueueReadyOnUndeclaredQueue(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()
	if ch := store.QueueReady("nope"); ch != nil {
		t.Fatal("QueueReady should return nil for an undeclared queue")
	}
}

// Nacking returns a message to the queue, which must wake a waiter too.
func TestNackWakesWaiter(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := store.Append(ctx, Message{ID: "m1", Queue: "q", Payload: []byte("p")},
		[]Destination{{Kind: DestinationQueue, Name: "q"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = res
	// Drain the notification from the append.
	<-store.QueueReady("q")

	msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c1", Limit: 1, VisibilityTimeout: time.Hour})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("lease: n=%d err=%v", len(msgs), err)
	}
	select {
	case <-store.QueueReady("q"):
		t.Fatal("lease should not signal; the message is not available")
	default:
	}
	if err := store.Nack(ctx, NackRequest{
		MessageID: msgs[0].ID, Queue: "q", ConsumerID: "c1",
		LeaseToken: msgs[0].LeaseToken, Action: NackRetry, IncrementAttempt: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.QueueReady("q"):
	case <-time.After(2 * time.Second):
		t.Fatal("a nack must wake a waiter, since the message became leasable again")
	}
}

// Durable stores embed *MemoryStore, so the signal has to reach them too.
func TestQueueSignalerIsSatisfiedByDurableStores(t *testing.T) {
	if _, ok := interface{}(NewMemoryStore()).(QueueSignaler); !ok {
		t.Fatal("MemoryStore must satisfy QueueSignaler")
	}
	if _, ok := interface{}(&BinaryFileWALStore{}).(QueueSignaler); !ok {
		t.Fatal("BinaryFileWALStore must satisfy QueueSignaler")
	}
	if _, ok := interface{}(&SegmentedWALStore{}).(QueueSignaler); !ok {
		t.Fatal("SegmentedWALStore must satisfy QueueSignaler")
	}
}
