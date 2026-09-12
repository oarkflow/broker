package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/oarkflow/broker/pkg/client"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:18091", "broker URL")
	token := flag.String("token", "complete-example-token", "API bearer token")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker, err := client.NewWorker(client.Config{
		BrokerURL: *url, Token: *token, ConsumerID: "complete-consumer",
		Group: "complete-consumers", Concurrency: 4, MaxInFlight: 16,
		PollWait: time.Second, HeartbeatInterval: time.Second, VisibilityTimeout: 30 * time.Second,
	})
	if err != nil {
		fatal(err)
	}
	worker.HandleQueue("complete.jobs", func(ctx context.Context, delivery client.Delivery) error {
		fmt.Printf("consumer queue message=%s payload=%s\n", delivery.Message.ID, delivery.Message.Payload)
		return delivery.Ack(ctx)
	})
	worker.HandleTopic("complete.events", func(ctx context.Context, delivery client.Delivery) error {
		fmt.Printf("consumer topic message=%s topic=%s\n", delivery.Message.ID, delivery.Message.Topic)
		return delivery.Ack(ctx)
	})
	worker.HandleRequest("complete.lookup", func(_ context.Context, delivery client.Delivery) (any, error) {
		fmt.Printf("consumer request message=%s\n", delivery.Message.ID)
		return map[string]any{"found": true, "service": "complete-consumer"}, nil
	})
	worker.HandleTask("complete.render", func(_ context.Context, delivery client.Delivery) (any, error) {
		fmt.Printf("consumer task message=%s task=%s\n", delivery.Message.ID, delivery.Message.TaskID)
		return map[string]any{"rendered": true}, nil
	})
	fmt.Println("complete consumer starting")
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "consumer failed:", err)
	os.Exit(1)
}
