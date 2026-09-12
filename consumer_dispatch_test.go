package broker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLeaseAcceptRejectsBeforeMutation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "jobs"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "job-1"}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	msgs, err := store.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "wrong", Limit: 1, Accept: func(Message) bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("rejected selector leased %d messages", len(msgs))
	}
	msg, ok, err := store.Get(ctx, "job-1")
	if err != nil || !ok || msg.Status != MessageQueued || msg.LeaseToken != "" {
		t.Fatalf("rejected message was mutated: ok=%v message=%+v err=%v", ok, msg, err)
	}
}

func TestCompetingStrategyConsumersDoNotStrandLeases(t *testing.T) {
	b := MustNew(Config{PollInterval: time.Millisecond, DefaultVisibility: 2 * time.Second})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	const total = 20
	var mu sync.Mutex
	seen := make(map[string]int, total)
	done := make(chan struct{}, total)
	handler := func(_ context.Context, delivery Delivery) error {
		mu.Lock()
		seen[delivery.Message.ID]++
		mu.Unlock()
		done <- struct{}{}
		return nil
	}
	for _, name := range []string{"worker-a", "worker-b"} {
		if _, err := b.Subscribe(context.Background(), ConsumerConfig{Name: name, Group: "workers", Queue: "jobs", Strategy: StrategyRoundRobin}, handler); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(context.Background(), "jobs.created", i, WithQueue("jobs")); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(750 * time.Millisecond)
	for i := 0; i < total; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d/%d messages completed before visibility timeout", i, total)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("handled %d unique messages, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("message %s handled %d times", id, count)
		}
	}
}
