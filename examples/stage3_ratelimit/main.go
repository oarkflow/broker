package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 3: Rate Limiting and Circuit Breaker
// Demonstrates per-queue rate limiting and circuit breaker protection.
// Messages exceeding the rate limit are rejected.
// The circuit breaker opens after consecutive failures.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "ratelimit-example",
		DefaultQueue:      "api-requests",
		DefaultVisibility: 30 * time.Second,
		RateLimit: &broker.RateLimitConfig{
			PerQueue: map[string]int{
				"api-requests": 10, // 10 messages/sec per queue
			},
			DefaultRate: 50,
		},
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to process messages
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "api-worker",
		Queue: "api-requests",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] processing %s", d.Message.ID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Publish messages - some will be rate limited
	successCount := 0
	rateLimitedCount := 0
	for i := 0; i < 50; i++ {
		_, err := pub.Publish(ctx, "api.request", map[string]any{
			"request_id": i,
			"endpoint":   "/api/users",
		}, broker.WithQueue("api-requests"))
		if err != nil {
			rateLimitedCount++
		} else {
			successCount++
		}
	}

	time.Sleep(2 * time.Second)
	log.Printf("[result] published=%d rate_limited=%d", successCount, rateLimitedCount)
}
