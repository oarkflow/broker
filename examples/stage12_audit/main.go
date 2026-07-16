package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/broker"
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
	http.Handle("/audit", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		auditLogger := b.AuditLogger()
		if auditLogger != nil {
			events := auditLogger.History(100)
			for _, e := range events {
				w.Write([]byte(e.Timestamp.Format(time.RFC3339) + " " + e.Principal + " " + e.Action + " " + e.Resource + " " + e.Status + "\n"))
			}
		} else {
			w.Write([]byte("audit logging not enabled"))
		}
	}))
	log.Fatal(http.ListenAndServe(":9091", nil))
}
