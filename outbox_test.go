package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDurableOutboxDispatchAndLifecycle(t *testing.T) {
	b := MustNew(Config{
		Store: NewMemoryStore(),
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer b.Stop(context.Background())

	outbox, err := NewDurableOutbox(DurableOutboxConfig{
		Broker:       b,
		PollInterval: 10 * time.Millisecond,
		MaxRetries:   3,
	})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}

	if err := outbox.Start(); err != nil {
		t.Fatalf("start outbox: %v", err)
	}
	defer outbox.Stop()

	// Enqueue 2 messages
	msg1 := Message{
		ID:      "outbox-msg-1",
		Queue:   "jobs",
		Payload: []byte("job-1-payload"),
	}
	msg2 := Message{
		ID:      "outbox-msg-2",
		Queue:   "jobs",
		Payload: []byte("job-2-payload"),
	}

	id1, err := outbox.Enqueue(context.Background(), msg1, []Destination{{Kind: DestinationQueue, Name: "jobs"}})
	if err != nil || id1 != "outbox-msg-1" {
		t.Fatalf("enqueue msg1: %v", err)
	}
	_, err = outbox.Enqueue(context.Background(), msg2, []Destination{{Kind: DestinationQueue, Name: "jobs"}})
	if err != nil {
		t.Fatalf("enqueue msg2: %v", err)
	}

	// Wait for background worker to flush
	deadline := time.Now().Add(2 * time.Second)
	for outbox.PendingCount() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for outbox flush; pending=%d", outbox.PendingCount())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify entries marked published
	e1, ok := outbox.GetEntry("outbox-msg-1")
	if !ok || e1.Status != OutboxPublished || e1.PublishedAt == nil {
		t.Fatalf("unexpected entry 1: %+v", e1)
	}

	// Verify messages exist in broker store
	storedMsg, ok, err := b.store.Get(context.Background(), "outbox-msg-1")
	if err != nil || !ok {
		t.Fatalf("stored message 1 missing: %v", err)
	}
	if string(storedMsg.Payload) != "job-1-payload" {
		t.Fatalf("payload mismatch: %s", string(storedMsg.Payload))
	}
}

func TestLegacyOutboxRestartOwnershipAndFlushFailure(t *testing.T) {
	store := &failOnceProducerStore{MemoryStore: NewMemoryStore(), fail: true}
	b := MustNew(Config{Store: store})
	defer b.Stop(context.Background())
	o, err := NewDurableOutbox(DurableOutboxConfig{Broker: b})
	if err != nil {
		t.Fatal(err)
	}
	message := Message{ID: "owned", Payload: []byte("original")}
	dests := []Destination{{Kind: DestinationQueue, Name: "jobs"}}
	if _, err := o.Enqueue(context.Background(), message, dests); err != nil {
		t.Fatal(err)
	}
	message.Payload[0] = 'X'
	dests[0].Name = "changed"
	if err := o.Flush(context.Background()); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("flush hid append failure: %v", err)
	}
	entry, _ := o.GetEntry("owned")
	if entry.Status != OutboxFailed || entry.Attempts != 1 {
		t.Fatalf("failed state: %+v", entry)
	}
	entry.Message.Payload[0] = 'Y'
	if err := o.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := store.Get(context.Background(), "owned")
	if err != nil || !ok || string(stored.Payload) != "original" || stored.Queue != "jobs" {
		t.Fatalf("caller mutated queued entry: %+v, %v", stored, err)
	}
	for i := 0; i < 3; i++ {
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		if err := o.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}
