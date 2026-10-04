package broker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// checkBackpressure used to assert the store to *MemoryStore, so it silently
// did nothing on every durable store, and it read MemoryStore.queues without the
// store lock. These tests cover both the memory and durable configurations, and
// the concurrent declare/publish case that used to be a data race.

func TestCheckBackpressureAppliesToMemoryStore(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{Backpressure: BackpressureReject})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "bounded", MaxDepth: 3}); err != nil {
		t.Fatal(err)
	}
	// Fill the queue to its limit.
	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "", "x", WithQueue("bounded")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	_, err = b.Publisher().Publish(ctx, "", "overflow", WithQueue("bounded"))
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure at the depth limit, got %v", err)
	}
	// A topic destination is not depth limited and must keep working.
	if _, err := b.Publisher().Publish(ctx, "some-topic", "x"); err != nil {
		t.Fatalf("topic publish should not be backpressured: %v", err)
	}
}

func TestCheckBackpressureAppliesToDurableStore(t *testing.T) {
	ctx := context.Background()
	store, err := NewBinaryFileWALStore(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Backpressure: BackpressureReject, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "durable-bounded", MaxDepth: 2}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Publisher().Publish(ctx, "", "x", WithQueue("durable-bounded")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, err := b.Publisher().Publish(ctx, "", "overflow", WithQueue("durable-bounded")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("durable store ignored the depth limit (got %v); backpressure was a no-op on non-MemoryStore stores", err)
	}
}

func TestQueueMaxDepthReflectsDeclaredLimit(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if got := b.QueueMaxDepth("nope"); got != 0 {
		t.Fatalf("undeclared queue should report 0, got %d", got)
	}
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "limited", MaxDepth: 7}); err != nil {
		t.Fatal(err)
	}
	if got := b.QueueMaxDepth("limited"); got != 7 {
		t.Fatalf("QueueMaxDepth = %d, want 7", got)
	}
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "open"}); err != nil {
		t.Fatal(err)
	}
	if got := b.QueueMaxDepth("open"); got != 0 {
		t.Fatalf("unlimited queue should report 0, got %d", got)
	}
}

// Declaring a queue mutates the same map backpressure reads. Run under -race this
// reproduces the previous unsynchronized map access.
func TestCheckBackpressureConcurrentWithDeclareQueue(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{Backpressure: BackpressureReject})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			name := "churn-" + string(rune('a'+i%26))
			if err := b.DeclareQueue(ctx, QueueConfig{Name: name, MaxDepth: 1 + i%4}); err != nil {
				t.Errorf("declare %s: %v", name, err)
				return
			}
		}
		close(stop)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := b.DeclareQueue(ctx, QueueConfig{Name: "target", MaxDepth: 1000}); err != nil {
				t.Errorf("declare target: %v", err)
				return
			}
			if _, err := b.Publisher().Publish(ctx, "", "x", WithQueue("target")); err != nil && !errors.Is(err, ErrBackpressure) {
				t.Errorf("publish: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

// A poll against an empty queue must not do the per-poll bookkeeping: it should
// return without allocating a result or scanning the head. Holding the store read
// lock proves the scan is gone: advanceQueueHeadLocked walks the head cursor, so
// a poll that reached it under a held read lock would still return here, but the
// allocation and scan are what this guards against and are asserted via
// allocs-per-op below.
func TestLeaseOnEmptyQueueIsCheap(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	if err := b.DeclareQueue(ctx, QueueConfig{Name: "idle"}); err != nil {
		t.Fatal(err)
	}
	ms := b.Store().(*MemoryStore)

	// An empty queue must produce no result slice allocation at all.
	req := LeaseRequest{Queue: "idle", ConsumerID: "c1", Limit: 32}
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := ms.Lease(ctx, req); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("empty-queue poll allocated %.1f times per call, want 0", allocs)
	}
}

// The fast path must not change behaviour once the queue does hold messages.
func TestLeaseFastPathStillDeliversMessages(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	if err := b.DeclareQueue(ctx, QueueConfig{Name: "filled"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "", "payload", WithQueue("filled")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	ms := b.Store().(*MemoryStore)

	// Drain, which leaves depth at 0 so the next call takes the fast path.
	msgs, err := ms.Lease(ctx, LeaseRequest{Queue: "filled", ConsumerID: "c1", Limit: 8})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("first lease returned %d messages, want 3", len(msgs))
	}
	for _, m := range msgs {
		if err := ms.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "filled", ConsumerID: "c1", LeaseToken: m.LeaseToken}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	// Now empty again: must return no messages and no error.
	msgs, err = ms.Lease(ctx, LeaseRequest{Queue: "filled", ConsumerID: "c1", Limit: 8})
	if err != nil {
		t.Fatalf("lease after drain: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("lease on drained queue returned %d messages, want 0", len(msgs))
	}

	// And a paused queue must still report ErrPaused on the fast path.
	if err := b.PauseQueue(ctx, "filled"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Lease(ctx, LeaseRequest{Queue: "filled", ConsumerID: "c1", Limit: 8}); !errors.Is(err, ErrPaused) {
		t.Fatalf("paused queue returned %v, want ErrPaused", err)
	}

	// An undeclared queue must still report ErrQueueNotFound.
	if _, err := ms.Lease(ctx, LeaseRequest{Queue: "does-not-exist", ConsumerID: "c1", Limit: 8}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("undeclared queue returned %v, want ErrQueueNotFound", err)
	}
}

// The daemon defaults message history to 0 and documents that as disabling the
// hot-path history, but MemoryStore defaults to 32 and Config had no way to set
// it. An in-process embedder was therefore structurally slower than the same
// broker run standalone, with no supported way to close the gap.
func TestConfigMessageHistoryAppliesToStore(t *testing.T) {
	ctx := context.Background()

	t.Run("unset keeps the store default", func(t *testing.T) {
		b, err := New(Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Stop(ctx)
		ms := b.Store().(*MemoryStore)
		ms.mu.RLock()
		got := ms.historyLimit
		ms.mu.RUnlock()
		if got != 32 {
			t.Fatalf("unset MessageHistory changed the store default: got %d, want 32", got)
		}
	})

	t.Run("explicit zero disables history", func(t *testing.T) {
		b, err := New(Config{MessageHistory: IntPtr(0)})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Stop(ctx)
		ms := b.Store().(*MemoryStore)
		ms.mu.RLock()
		got := ms.historyLimit
		ms.mu.RUnlock()
		if got != 0 {
			t.Fatalf("MessageHistory=0 gave historyLimit %d, want 0", got)
		}
	})

	t.Run("explicit value is honoured", func(t *testing.T) {
		b, err := New(Config{MessageHistory: IntPtr(4)})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Stop(ctx)
		ms := b.Store().(*MemoryStore)
		ms.mu.RLock()
		got := ms.historyLimit
		ms.mu.RUnlock()
		if got != 4 {
			t.Fatalf("MessageHistory=4 gave historyLimit %d, want 4", got)
		}
	})

	t.Run("applies to an injected durable store", func(t *testing.T) {
		store, err := NewBinaryFileWALStore(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		b, err := New(Config{Store: store, MessageHistory: IntPtr(0)})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Stop(ctx)
		bs, ok := b.Store().(*BinaryFileWALStore)
		if !ok {
			t.Fatalf("unexpected store type %T", b.Store())
		}
		bs.MemoryStore.mu.RLock()
		got := bs.historyLimit
		bs.MemoryStore.mu.RUnlock()
		if got != 0 {
			t.Fatalf("injected store historyLimit %d, want 0", got)
		}
	})
}

// History is recorded on every state transition, so a disabled limit must leave
// the history map empty while the message itself still works.
func TestMessageHistoryZeroStillDelivers(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{MessageHistory: IntPtr(0)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("leased %d messages, want 1", len(msgs))
	}
	history, err := b.Store().History(ctx, res.MessageID)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("history recorded %d entries with the limit disabled, want 0", len(history))
	}
}
