package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 2: Metrics and Tracing
// Demonstrates Prometheus metrics endpoint and distributed tracing.
// Access metrics at http://localhost:9090/metrics

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "metrics-example",
		DefaultQueue:      "events",
		DefaultVisibility: 30 * time.Second,
		EnablePrometheus:  true,
		EnableTracing:     true,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to process messages
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "metrics-worker",
		Queue: "events",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] processing %s", d.Message.ID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Publish some messages to generate metrics
	for i := 0; i < 100; i++ {
		_, _ = pub.Publish(ctx, "events.test", map[string]string{
			"event": "test",
			"index": string(rune('A' + i%26)),
		}, broker.WithQueue("events"))
	}

	time.Sleep(2 * time.Second)

	// Start metrics HTTP server
	log.Println("[server] metrics available at http://localhost:9090/metrics")
	log.Println("[server] health available at http://localhost:9090/healthz")
	http.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	log.Fatal(http.ListenAndServe(":9090", nil))
}
