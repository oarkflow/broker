package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 8: Partitioning and Ordering
// Demonstrates key-based partitioning and message ordering guarantees.
// Messages with the same key are routed to the same partition and processed in order.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "partitioning-example",
		DefaultQueue:      "events",
		DefaultVisibility: 30 * time.Second,
		Partition: &broker.PartitionConfig{
			Partitions: 4,
			Strategy:   broker.PartitionHash,
		},
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to partition-specific queues
	for i := 0; i < 4; i++ {
		partition := i
		_, err := b.Subscribe(ctx, broker.ConsumerConfig{
			Name:    fmt.Sprintf("partition-worker-%d", partition),
			Queue:   fmt.Sprintf("events.p%d", partition),
			AckMode: broker.AckAuto,
		}, func(ctx context.Context, d broker.Delivery) error {
			log.Printf("[partition-%d] processing %s key=%s", partition, d.Message.ID, d.Message.Key)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create a partition router to determine which partition to use
	router := broker.NewPartitionRouter(broker.PartitionConfig{Partitions: 4, Strategy: broker.PartitionHash})

	// Publish ordered messages per user using explicit partition queues
	for userID := 1; userID <= 3; userID++ {
		for step := 1; step <= 5; step++ {
			key := fmt.Sprintf("user-%d", userID)
			msg := broker.Message{Key: key, ID: fmt.Sprintf("msg-%d-%d", userID, step)}
			partitionIdx := router.SelectPartition(msg)
			queueName := fmt.Sprintf("events.p%d", partitionIdx)
			_, err := b.Publisher().Publish(ctx, "events.user", map[string]any{
				"user_id": userID,
				"step":    step,
				"action":  fmt.Sprintf("step-%d", step),
			}, broker.WithQueue(queueName), broker.WithKey(key))
			if err != nil {
				log.Printf("[publisher] error: %v", err)
			}
		}
	}

	// Add partition reassignment rule
	b.PartitionReassignment().AddRule(broker.ReassignmentRule{
		SourceTopic: "events.old",
		DestTopic:   "events.new",
		BatchSize:   50,
		Enabled:     true,
	})

	// Get partition stats
	stats, _ := b.TopicPartitionStats(ctx, "events", 4)
	log.Printf("[partitioning] topic events has %d partitions with %d total messages",
		len(stats.Partitions), stats.TotalMsgs)

	time.Sleep(3 * time.Second)
}
