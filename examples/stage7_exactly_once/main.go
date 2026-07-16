package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 7: Exactly-Once Delivery
// Demonstrates transactional outbox pattern and idempotent consumers
// for exactly-once delivery semantics.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "exactly-once-example",
		DefaultQueue:      "transactions",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Create transactional outbox
	outbox := b.TransactionalOutbox()

	// Subscribe with idempotent consumer
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:                "transaction-worker",
		Queue:               "transactions",
		AckMode:             broker.AckManual,
		Idempotent:          true,
		DeduplicationWindow: 10 * time.Minute,
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] processing transaction %s", d.Message.ID)
		// Simulate processing
		time.Sleep(50 * time.Millisecond)
		log.Printf("[worker] acked transaction %s", d.Message.ID)
		return d.Ack(ctx)
	})
	if err != nil {
		log.Fatal(err)
	}

	// Publish via transactional outbox
	for i := 0; i < 5; i++ {
		_, err := outbox.Publish(ctx, "payment.processed", map[string]any{
			"payment_id": i,
			"amount":     (i + 1) * 50,
			"currency":   "USD",
		}, broker.WithQueue("transactions"))
		if err != nil {
			log.Printf("[outbox] publish error: %v", err)
		} else {
			log.Printf("[outbox] published payment %d", i)
		}
	}

	time.Sleep(3 * time.Second)
	log.Printf("[result] pending outbox events: %d", len(outbox.Pending()))
}
