package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/fh"
)

// Stage 12: Audit Logging
// Demonstrates structured audit logging with multiple sinks.
// All broker operations are logged with principal, action, and result.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "audit-example",
		DefaultQueue:      "audit-queue",
		DefaultVisibility: 30 * time.Second,
		AuditEnabled:      true,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to process messages
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "audit-worker",
		Queue: "audit-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] processing %s", d.Message.ID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Perform operations that will be audited
	for i := 0; i < 5; i++ {
		_, _ = pub.Publish(ctx, "audit.test", map[string]any{
			"event": "test-operation",
			"id":    i,
		}, broker.WithQueue("audit-queue"))
	}

	time.Sleep(2 * time.Second)

	// Start admin server to view audit logs
	log.Println("[server] audit logs available at http://localhost:9091/audit")
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Get("/audit", func(c fh.Ctx) error {
		auditLogger := b.AuditLogger()
		if auditLogger == nil {
			return c.SendString("audit logging not enabled")
		}
		var out strings.Builder
		for _, e := range auditLogger.History(100) {
			out.WriteString(e.Timestamp.Format(time.RFC3339))
			out.WriteString(" ")
			out.WriteString(e.Principal)
			out.WriteString(" ")
			out.WriteString(e.Action)
			out.WriteString(" ")
			out.WriteString(e.Resource)
			out.WriteString(" ")
			out.WriteString(e.Status)
			out.WriteString("\n")
		}
		return c.SendString(out.String())
	})
	log.Fatal(app.Listen(":9091"))
}
