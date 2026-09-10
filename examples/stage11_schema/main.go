package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 11: Schema Evolution
// Demonstrates schema versioning and compatibility checking.
// Register schemas with different versions and validate messages.

func main() {
	ctx := context.Background()

	// Create schema registry
	registry := broker.NewMemorySchemaRegistry(
		broker.SchemaDefinition{
			Name:         "invoice.created",
			Version:      "v1",
			Required:     []string{"invoice_id", "amount"},
			AllowUnknown: true,
		},
		broker.SchemaDefinition{
			Name:           "invoice.created",
			Version:        "v2",
			Required:       []string{"invoice_id", "amount", "currency"},
			AllowUnknown:   true,
			CompatibleWith: "v1",
		},
		broker.SchemaDefinition{
			Name:           "invoice.created",
			Version:        "v3",
			Required:       []string{"invoice_id", "amount", "currency", "tax_rate"},
			AllowUnknown:   true,
			CompatibleWith: "v2",
		},
	)

	b := broker.MustNew(broker.Config{
		Name:              "schema-example",
		DefaultQueue:      "invoices",
		DefaultVisibility: 30 * time.Second,
		SchemaRegistry:    registry,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "invoice-worker",
		Queue: "invoices",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] invoice: schema=%s version=%s payload=%s",
			d.Message.Schema, d.Message.SchemaVersion, string(d.Message.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Publish v1 message (valid)
	_, err = pub.Publish(ctx, "invoice.created", map[string]any{
		"invoice_id": "INV-001",
		"amount":     100.00,
	}, broker.WithSchema("invoice.created", "v1"), broker.WithQueue("invoices"))
	if err != nil {
		log.Printf("[v1] publish error: %v", err)
	} else {
		log.Println("[v1] published successfully")
	}

	// Publish v2 message (valid)
	_, err = pub.Publish(ctx, "invoice.created", map[string]any{
		"invoice_id": "INV-002",
		"amount":     200.00,
		"currency":   "USD",
	}, broker.WithSchema("invoice.created", "v2"), broker.WithQueue("invoices"))
	if err != nil {
		log.Printf("[v2] publish error: %v", err)
	} else {
		log.Println("[v2] published successfully")
	}

	// Publish v3 message (valid)
	_, err = pub.Publish(ctx, "invoice.created", map[string]any{
		"invoice_id": "INV-003",
		"amount":     300.00,
		"currency":   "EUR",
		"tax_rate":   0.19,
	}, broker.WithSchema("invoice.created", "v3"), broker.WithQueue("invoices"))
	if err != nil {
		log.Printf("[v3] publish error: %v", err)
	} else {
		log.Println("[v3] published successfully")
	}

	// Check compatibility
	compat := registry.CheckCompatibility("invoice.created", "v2", "v1")
	log.Printf("[compat] v2 vs v1: %s", compat)

	latest, _ := registry.LatestVersion("invoice.created")
	log.Printf("[schema] latest version: %s", latest)

	time.Sleep(2 * time.Second)
}
