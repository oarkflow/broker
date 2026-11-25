package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/fh"
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

	// Start the legacy SSE server (not RFC 6455 WebSocket).
	wsServer := broker.NewWebSocketServer(b, broker.WebSocketConfig{
		Path:              "/ws",
		MaxConnections:    100,
		HeartbeatInterval: 30 * time.Second,
		InsecureDev:       true,
	})

	app := fh.New(fh.WithStartupBannerDisabled(true))
	wsServer.Register(app)
	app.Get("/health", func(c fh.Ctx) error { return c.SendString("ok") })

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

	log.Println("[server] SSE available at http://localhost:8080/ws")
	log.Println("[server] Health available at http://localhost:8080/health")
	log.Fatal(app.Listen(":8080"))
}
