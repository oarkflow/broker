package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 4: WebSocket Transport
// Demonstrates WebSocket-like transport for real-time broker communication.
// Clients connect via HTTP SSE and receive real-time updates.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "websocket-example",
		DefaultQueue:      "notifications",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to process messages
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "notification-worker",
		Queue: "notifications",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] notification: %s", d.Message.ID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	pub := b.Publisher()

	// Start WebSocket/SSE server
	wsServer := broker.NewWebSocketServer(b, broker.WebSocketConfig{
		Path:              "/ws",
		MaxConnections:    100,
		HeartbeatInterval: 30 * time.Second,
	})

	mux := http.NewServeMux()
	mux.Handle("/ws", wsServer.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	// Publish a message to demonstrate real-time delivery
	go func() {
		time.Sleep(2 * time.Second)
		for i := 0; i < 5; i++ {
			_, _ = pub.Publish(ctx, "notifications.test", map[string]string{
				"type":    "info",
				"message": "Hello from WebSocket example",
			}, broker.WithQueue("notifications"))
			time.Sleep(500 * time.Millisecond)
		}
	}()

	log.Println("[server] WebSocket/SSE available at http://localhost:8080/ws")
	log.Println("[server] Health available at http://localhost:8080/health")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
