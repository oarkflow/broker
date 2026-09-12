package broker

import (
	"context"
	"errors"
	"testing"
)

type failOnceProducerStore struct {
	*MemoryStore
	fail bool
}

func (s *failOnceProducerStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	if s.fail {
		s.fail = false
		return nil, ErrStorageUnavailable
	}
	return s.MemoryStore.Append(ctx, msg, dests)
}

func TestProducerRegistryEpochFencingAndSequencing(t *testing.T) {
	reg := NewProducerRegistry()

	// 1. Initial sequence 0
	isDup, _, err := reg.CheckAndRecord("prod-1", 1, 0, "msg-0")
	if err != nil || isDup {
		t.Fatalf("expected first publish ok, got dup=%v, err=%v", isDup, err)
	}

	// 2. Duplicate sequence 0
	isDup, prevID, err := reg.CheckAndRecord("prod-1", 1, 0, "msg-0-retry")
	if err != nil || !isDup || prevID != "msg-0" {
		t.Fatalf("expected duplicate recognized, got dup=%v prevID=%s err=%v", isDup, prevID, err)
	}

	// 3. In-order sequence 1
	isDup, _, err = reg.CheckAndRecord("prod-1", 1, 1, "msg-1")
	if err != nil || isDup {
		t.Fatalf("expected seq 1 ok, got dup=%v err=%v", isDup, err)
	}

	// 4. Out-of-order sequence (gap: expecting 2, got 4)
	isDup, _, err = reg.CheckAndRecord("prod-1", 1, 4, "msg-4")
	if err != ErrOutOfOrderSequence {
		t.Fatalf("expected ErrOutOfOrderSequence, got %v", err)
	}

	// 5. Epoch fencing (producer with epoch 0 tries to publish)
	isDup, _, err = reg.CheckAndRecord("prod-1", 0, 2, "msg-fenced")
	if err != ErrProducerFenced {
		t.Fatalf("expected ErrProducerFenced, got %v", err)
	}

	// 6. New epoch bumps epoch and resets sequence
	isDup, _, err = reg.CheckAndRecord("prod-1", 2, 0, "msg-epoch2-0")
	if err != nil || isDup {
		t.Fatalf("expected new epoch sequence 0 ok, got dup=%v err=%v", isDup, err)
	}

	state, ok := reg.GetProducer("prod-1")
	if !ok || state.Epoch != 2 || state.LastSequence != 0 {
		t.Fatalf("unexpected producer state: %+v", state)
	}
}

func TestBrokerPublishIdempotentProducer(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()

	// 1. Initial publish seq 0
	res1, err := b.Publisher().Publish(ctx, "events", "order-1",
		WithQueue("orders"),
		WithProducer("checkout-service", 1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res1.MessageID == "" {
		t.Fatal("expected message ID")
	}

	// 2. Retry publish seq 0 (idempotent duplicate)
	res1Retry, err := b.Publisher().Publish(ctx, "events", "order-1",
		WithQueue("orders"),
		WithProducer("checkout-service", 1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res1Retry.MessageID != res1.MessageID {
		t.Fatalf("expected retry to return same message ID %s, got %s", res1.MessageID, res1Retry.MessageID)
	}

	// Verify only 1 message is in queue
	msgs, err := b.Store().ListQueue(ctx, "orders", 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected 1 message in queue, got %d (err=%v)", len(msgs), err)
	}

	// 3. Publish seq 1
	res2, err := b.Publisher().Publish(ctx, "events", "order-2",
		WithQueue("orders"),
		WithProducer("checkout-service", 1, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res2.MessageID == res1.MessageID {
		t.Fatal("expected different message ID for seq 1")
	}

	// 4. Out of order publish seq 5 -> ErrOutOfOrderSequence
	_, err = b.Publisher().Publish(ctx, "events", "order-5",
		WithQueue("orders"),
		WithProducer("checkout-service", 1, 5),
	)
	if err != ErrOutOfOrderSequence {
		t.Fatalf("expected ErrOutOfOrderSequence, got %v", err)
	}

	// 5. Fenced epoch publish -> ErrProducerFenced
	_, err = b.Publisher().Publish(ctx, "events", "order-fenced",
		WithQueue("orders"),
		WithProducer("checkout-service", 0, 2),
	)
	if err != ErrProducerFenced {
		t.Fatalf("expected ErrProducerFenced, got %v", err)
	}
}

func TestProducerSequenceIsNotConsumedByFailedAppend(t *testing.T) {
	store := &failOnceProducerStore{MemoryStore: NewMemoryStore(), fail: true}
	b := MustNew(Config{Store: store})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	if _, err := b.Publisher().Publish(context.Background(), "events", "payload", WithQueue("orders"), WithProducer("producer", 1, 0)); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("expected first append to fail, got %v", err)
	}
	result, err := b.Publisher().Publish(context.Background(), "events", "payload", WithQueue("orders"), WithProducer("producer", 1, 0))
	if err != nil {
		t.Fatalf("retry after failed append: %v", err)
	}
	if result.Duplicate {
		t.Fatal("failed append consumed the producer sequence")
	}
}

func TestProducerSequenceSurvivesWALRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		t.Fatal(err)
	}
	b := MustNew(Config{Store: store})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := b.Publisher().Publish(context.Background(), "events", "payload", WithQueue("orders"), WithProducer("producer", 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		t.Fatal(err)
	}
	b2 := MustNew(Config{Store: reopened})
	if err := b2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b2.Stop(context.Background())
	retry, err := b2.Publisher().Publish(context.Background(), "events", "payload", WithQueue("orders"), WithProducer("producer", 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Duplicate || retry.MessageID != first.MessageID {
		t.Fatalf("restart retry=%+v, want duplicate message %q", retry, first.MessageID)
	}
}
