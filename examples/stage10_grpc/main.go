package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 10: gRPC Transport
// Demonstrates the gRPC-like transport server.
// Uses length-prefixed JSON over TCP for high-performance communication.

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "grpc-example",
		DefaultQueue:      "rpc-queue",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to process RPC requests
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "rpc-worker",
		Queue: "rpc-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[worker] RPC request: %s payload=%s", d.Message.ID, string(d.Message.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start gRPC-like server
	grpcServer := broker.NewGRPCServer(b, broker.GRPCConfig{
		ListenAddr:     ":50051",
		MaxConnections: 100,
		MaxMessageSize: 4 << 20,
		KeepAlive:      60 * time.Second,
	})
	go func() {
		log.Println("[grpc] server starting on :50051")
		if err := grpcServer.ListenAndServe(); err != nil {
			log.Printf("[grpc] server error: %v", err)
		}
	}()

	// Publish via broker (simulating gRPC publish)
	pub := b.Publisher()
	for i := 0; i < 5; i++ {
		_, _ = pub.Publish(ctx, "rpc.request", map[string]any{
			"method": "GetUser",
			"params": map[string]string{"id": "123"},
		}, broker.WithQueue("rpc-queue"))
	}

	time.Sleep(3 * time.Second)
	log.Println("[result] gRPC server running, connect via TCP on :50051")
}
