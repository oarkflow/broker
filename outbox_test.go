package broker

import (
	"context"
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
