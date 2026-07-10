package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/amqp"
)

func main() {
	// Create a new broker instance
	b, err := broker.New(broker.Config{
		Name:                  "amqp-demo",
		Mode:                  broker.ModeStandalone,
		DefaultQueue:          "default",
		DefaultVisibility:     30 * time.Second,
		DefaultRetry:          broker.DefaultRetryPolicy(),
		DrainTimeout:          15 * time.Second,
		MaxMessageSize:        64 << 20,
		LeaseSweepInterval:    time.Second,
		RemoteConsumerTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// Start the broker
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
	defer b.Stop(ctx)

	// Create AMQP server
	amqpServer := amqp.NewServer(amqp.ServerConfig{
		ListenAddr:     ":5672",
		Broker:         b,
		MaxConnections: 1000,
		FrameMaxSize:   131072,
		Heartbeat:      60 * time.Second,
	})

	// Start AMQP server
	if err := amqpServer.Listen(); err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		amqpServer.Stop(stopCtx)
		b.Stop(stopCtx)
		os.Exit(0)
	}()

	fmt.Println("Fastworker AMQP Server")
	fmt.Println("======================")
	fmt.Println("Listening on :5672")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()
	fmt.Println("Connect with any AMQP 0.9.1 client:")
	fmt.Println("  amqp-host: localhost")
	fmt.Println("  amqp-port: 5672")
	fmt.Println("  username: (any)")
	fmt.Println("  password: (any)")
	fmt.Println()
	fmt.Println("Example using 'rake' CLI tool:")
	fmt.Println("  rake publish --exchange demo --routing-key test --body 'Hello AMQP'")
	fmt.Println()
	fmt.Println("Example using Python pika:")
	fmt.Println("  import pika")
	fmt.Println("  conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))")
	fmt.Println("  ch = conn.channel()")
	fmt.Println("  ch.basic_publish(exchange='demo', routing_key='test', body=b'Hello AMQP')")

	// Run forever
	select {}
}
