package broker

import (
	"context"
	"testing"
	"time"
)

func TestTTLSweeperExpireToDLQ(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "short-lived"})
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "short-lived.dlq"})

	// Publish message with very short TTL
	res1, err := b.Publisher().Publish(ctx, "", "will-expire",
		WithQueue("short-lived"),
		WithTTL(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Publish message with long TTL
	res2, err := b.Publisher().Publish(ctx, "", "will-live",
		WithQueue("short-lived"),
		WithTTL(10*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for message 1 to expire
	time.Sleep(30 * time.Millisecond)

	sweeper := NewTTLSweeper(b, TTLSweeperConfig{
		ExpireToDLQ: true,
		BatchSize:   50,
	})

	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep error: %v", err)
	}

	if result.Expired != 1 || result.DeadLettered != 1 {
		t.Fatalf("expected 1 expired and dead lettered, got %+v", result)
	}

	// Verify short-lived has only message 2
	liveMsgs, err := b.Store().ListQueue(ctx, "short-lived", 10)
	if err != nil || len(liveMsgs) != 1 || liveMsgs[0].ID != res2.MessageID {
		t.Fatalf("expected only message 2 in queue, got %+v", liveMsgs)
	}

	// Verify short-lived.dlq has message 1
	dlqMsgs, err := b.Store().ListQueue(ctx, "short-lived.dlq", 10)
	if err != nil || len(dlqMsgs) != 1 || dlqMsgs[0].ID != res1.MessageID {
		t.Fatalf("expected message 1 in DLQ, got %+v", dlqMsgs)
	}
}

func TestTTLSweeperPurge(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "purge-test"})

	_, err := b.Publisher().Publish(ctx, "", "purge-me",
		WithQueue("purge-test"),
		WithTTL(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)

	sweeper := NewTTLSweeper(b, TTLSweeperConfig{
		ExpireToDLQ: false,
		BatchSize:   50,
	})

	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep error: %v", err)
	}

	if result.Expired != 1 || result.Purged != 1 {
		t.Fatalf("expected 1 expired and purged, got %+v", result)
	}

	msgs, err := b.Store().ListQueue(ctx, "purge-test", 10)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("expected empty queue, got %d msgs", len(msgs))
	}
}
