package broker

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestDisabledRouteDoesNotRoute(t *testing.T) {
	b := MustNew(Config{})
	ctx := context.Background()
	b.AddRoute(Route{Name: "off", Match: "orders.*", Disabled: true, To: []Destination{{Kind: DestinationQueue, Name: "orders"}}})
	if _, err := b.Publisher().Publish(ctx, "orders.created", map[string]string{"id": "1"}); err != nil {
		t.Fatal(err)
	}
	st, err := b.Store().Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Queues["orders"]; got != 0 {
		t.Fatalf("disabled route delivered %d messages", got)
	}
}

func TestSubscribeBatchAutoAck(t *testing.T) {
	b := MustNew(Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "batch"}); err != nil {
		t.Fatal(err)
	}
	var seen atomic.Int64
	_, err := b.SubscribeBatch(ctx, ConsumerConfig{Name: "batcher", Queue: "batch", BatchSize: 3, AckMode: AckAuto}, func(ctx context.Context, ds []Delivery) error {
		seen.Add(int64(len(ds)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "", map[string]int{"i": i}, WithQueue("batch")); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, time.Second, func() bool {
		msgs, err := b.Store().ListQueue(ctx, "batch", 10)
		return err == nil && seen.Load() == 3 && len(msgs) == 0
	})
}

func TestAtomicTransactionDoesNotPartiallyCommit(t *testing.T) {
	b := MustNew(Config{})
	ctx := context.Background()
	p := b.Publisher()
	tx, err := p.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Publish("", map[string]string{"ok": "1"}, WithQueue("tx"), WithIdempotencyKey("same")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Publish("", map[string]string{"ok": "2"}, WithQueue("tx"), WithIdempotencyKey("same")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	msgs, err := b.Store().ListQueue(ctx, "tx", 10)
	if err != nil {
		// Queue is not created when the transaction aborts before commit; that is acceptable.
		return
	}
	if len(msgs) != 0 {
		t.Fatalf("transaction partially committed %d messages", len(msgs))
	}
}

func TestBackpressureDropOldestAndPauseResume(t *testing.T) {
	b := MustNew(Config{})
	ctx := context.Background()
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "bp", MaxDepth: 1, Backpressure: BackpressureDropOldest}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", "old", WithQueue("bp"), WithMessageID("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", "new", WithQueue("bp"), WithMessageID("new")); err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().ListQueue(ctx, "bp", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "new" {
		t.Fatalf("expected newest message to remain, got %#v", msgs)
	}
	if err := b.PauseQueue(ctx, "bp"); err != nil {
		t.Fatal(err)
	}
	leased, err := b.Store().Lease(ctx, LeaseRequest{Queue: "bp", ConsumerID: "c", Limit: 1, VisibilityTimeout: time.Second})
	if !errors.Is(err, ErrPaused) || len(leased) != 0 {
		t.Fatalf("expected paused queue lease failure, leased=%d err=%v", len(leased), err)
	}
	if err := b.ResumeQueue(ctx, "bp"); err != nil {
		t.Fatal(err)
	}
	leased, err = b.Store().Lease(ctx, LeaseRequest{Queue: "bp", ConsumerID: "c", Limit: 1, VisibilityTimeout: time.Second})
	if err != nil || len(leased) != 1 {
		t.Fatalf("expected resume lease success, leased=%d err=%v", len(leased), err)
	}
}

func TestFileWALRecoversInflightAsQueued(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "wal")
	st, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	b := MustNew(Config{Store: st})
	if _, err := b.Publisher().Publish(ctx, "", "payload", WithQueue("recover")); err != nil {
		t.Fatal(err)
	}
	leased, err := b.Store().Lease(ctx, LeaseRequest{Queue: "recover", ConsumerID: "c1", Limit: 1, VisibilityTimeout: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease failed len=%d err=%v", len(leased), err)
	}
	if err := b.Store().Update(ctx, Message{ID: leased[0].ID, Queue: "recover", Payload: leased[0].Payload, Status: MessageProcessing}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	leased2, err := st2.Lease(ctx, LeaseRequest{Queue: "recover", ConsumerID: "c2", Limit: 1, VisibilityTimeout: time.Second})
	if err != nil || len(leased2) != 1 {
		t.Fatalf("expected recovered message to be leaseable, len=%d err=%v", len(leased2), err)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
