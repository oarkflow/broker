package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 6: Multi-Tenancy
// Demonstrates tenant isolation using queue prefixes.
// Each tenant gets isolated queues while sharing the same broker.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "tenancy-example",
		DefaultQueue:      "default",
		DefaultVisibility: 30 * time.Second,
		Tenancy: &broker.TenancyConfig{
			Enabled:        true,
			DefaultTenant:  "default",
			IsolationLevel: broker.IsolationQueuePrefix,
			QueuePrefix:    "t.",
		},
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	tenantMgr := b.TenantManager()

	// Create tenants
	tenantMgr.EnsureTenant("tenant-a")
	tenantMgr.EnsureTenant("tenant-b")
	log.Println("[tenancy] created tenants: tenant-a, tenant-b")

	// Subscribe for tenant-a
	_, err := tenantMgr.TenantAwareSubscribe(ctx, "tenant-a", broker.ConsumerConfig{
		Name:  "worker-a",
		Queue: "orders",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[tenant-a] received: %s payload=%s", d.Message.ID, string(d.Message.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe for tenant-b
	_, err = tenantMgr.TenantAwareSubscribe(ctx, "tenant-b", broker.ConsumerConfig{
		Name:  "worker-b",
		Queue: "orders",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[tenant-b] received: %s payload=%s", d.Message.ID, string(d.Message.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Give subscriptions time to start polling
	time.Sleep(500 * time.Millisecond)

	// Publish to each tenant using explicit queue names
	pub := b.Publisher()
	_, _ = pub.Publish(ctx, "orders.created", map[string]string{
		"order": "order-a-1", "tenant": "tenant-a",
	}, broker.WithQueue(tenantMgr.QueueName("tenant-a", "orders")))
	_, _ = pub.Publish(ctx, "orders.created", map[string]string{
		"order": "order-b-1", "tenant": "tenant-b",
	}, broker.WithQueue(tenantMgr.QueueName("tenant-b", "orders")))

	time.Sleep(2 * time.Second)
	log.Println("[result] each tenant should only receive its own messages")
}
