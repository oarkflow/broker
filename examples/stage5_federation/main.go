package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 5: Federation and Shovel
// Demonstrates federation between broker instances and message shoveling.
// A shovel moves messages from one queue to another automatically.

func main() {
	ctx := context.Background()

	// Create a single broker with federation
	b := broker.MustNew(broker.Config{
		Name:              "federation-example",
		DefaultQueue:      "source-queue",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Declare source and destination queues
	_ = b.DeclareQueue(ctx, broker.QueueConfig{Name: "source-queue", VisibilityTimeout: 30 * time.Second})
	_ = b.DeclareQueue(ctx, broker.QueueConfig{Name: "dest-queue", VisibilityTimeout: 30 * time.Second})
	_ = b.DeclareQueue(ctx, broker.QueueConfig{Name: "audit-queue", VisibilityTimeout: 30 * time.Second})

	// Subscribe to destination queue
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "dest-worker",
		Queue: "dest-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[dest-worker] received: %s", d.Message.ID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start a shovel from source to destination
	shovelMgr := b.ShovelManager()
	shovelID := shovelMgr.StartShovel(broker.ShovelConfig{
		Source:      "source-queue",
		Destination: "dest-queue",
		BatchSize:   10,
	})
	log.Printf("[shovel] started shovel %s", shovelID)

	// Start federation manager (in a real setup, this connects to an upstream broker)
	fedMgr := b.FederationManager()
	link := fedMgr.AddLink(broker.FederationConfig{
		Queues:       []string{"source-queue"},
		PollInterval: 500 * time.Millisecond,
		BatchSize:    10,
	})
	log.Printf("[federation] added link %s", link.ID)

	// Publish messages to source queue
	pub := b.Publisher()
	for i := 0; i < 10; i++ {
		_, _ = pub.Publish(ctx, "test.event", map[string]any{
			"event_id": i,
			"type":     "shovel-test",
		}, broker.WithQueue("source-queue"))
	}

	time.Sleep(3 * time.Second)
	log.Println("[result] messages should have been shoveled from source to destination")
}
