package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 1: Idempotent Consumers
// Demonstrates deduplication of messages using idempotency keys.
// Messages with the same idempotency key are automatically deduplicated
// within the configured window.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:             "idempotent-example",
		DefaultQueue:     "orders",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	var processedCount atomic.Int32

	// Subscribe with idempotent consumer enabled.
	// Messages with the same idempotency_key will be deduplicated within the window.
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:                "idempotent-worker",
		Queue:               "orders",
		AckMode:             broker.AckAuto,
		Concurrency:         4,
		Idempotent:          true,
		DeduplicationWindow: 5 * time.Minute,
	}, func(ctx context.Context, d broker.Delivery) error {
		count := processedCount.Add(1)
		log.Printf("[worker] processing message %s (count=%d)", d.Message.ID, count)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Publish 5 messages: 2 with same idempotency key, 3 with unique keys.
	// Only 4 unique messages should be processed (2 with same key = 1 published).
	for i := 0; i < 5; i++ {
		idempKey := "order-123" // same key for first two
		if i >= 2 {
			idempKey = fmt.Sprintf("order-%d", i) // unique keys for rest
		}
		res, err := pub.Publish(ctx, "orders.created", map[string]any{
			"order_id": fmt.Sprintf("order-%d", i),
			"amount":   (i + 1) * 100,
		},
			broker.WithQueue("orders"),
			broker.WithIdempotencyKey(idempKey),
			broker.WithType("order"),
		)
		if err != nil {
			log.Printf("[publisher] publish error: %v", err)
			continue
		}
		log.Printf("[publisher] published msg=%s idemp_key=%s", res.MessageID, idempKey)
	}

	time.Sleep(2 * time.Second)
	log.Printf("[result] processed %d messages (expected: 4 unique)", processedCount.Load())
}
