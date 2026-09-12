package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/oarkflow/broker"
	client "github.com/oarkflow/broker/pkg/client"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:18091", "broker URL")
	token := flag.String("token", "complete-example-token", "API bearer token")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := run(ctx, *url, *token); err != nil {
		fmt.Fprintln(os.Stderr, "publisher failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url, token string) error {
	c := client.New(client.Config{BrokerURL: url, Token: token, PollWait: time.Second})
	for {
		consumers, err := c.Consumers(ctx)
		if err == nil && len(consumers) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for consumer registration: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	queueResult, err := c.Publish(ctx, "", map[string]any{"kind": "queue", "value": 1}, client.WithQueue("complete.jobs"))
	if err != nil {
		return fmt.Errorf("publish queue: %w", err)
	}
	topicResult, err := c.Publish(ctx, "complete.events", map[string]any{"kind": "topic", "value": 2})
	if err != nil {
		return fmt.Errorf("publish topic: %w", err)
	}
	streamResult, err := c.Publish(ctx, "", map[string]any{"kind": "stream", "value": 3}, client.WithStream("complete.stream"))
	if err != nil {
		return fmt.Errorf("publish stream: %w", err)
	}
	var response map[string]any
	if err := c.Request(ctx, "complete.lookup", map[string]string{"id": "customer-7"}, &response, 5*time.Second); err != nil {
		return fmt.Errorf("request/response: %w", err)
	}
	task, err := c.SubmitTask(ctx, "complete.render", map[string]string{"document": "invoice-101"})
	if err != nil {
		return fmt.Errorf("submit task: %w", err)
	}
	for task.Status != broker.TaskSucceeded {
		if task.Status == broker.TaskFailed || task.Status == broker.TaskDeadLettered || task.Status == broker.TaskCancelled {
			return fmt.Errorf("task ended in %s: %s", task.Status, task.LastError)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for task: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		task, err = c.Task(ctx, task.ID)
		if err != nil {
			return err
		}
	}
	stream, err := c.ReadStream(ctx, "complete.stream", "complete-publisher", 0, 10)
	if err != nil || len(stream) != 1 {
		return fmt.Errorf("read stream: messages=%d err=%w", len(stream), err)
	}
	if err := c.CommitStreamOffset(ctx, "complete.stream", "complete-publisher", stream[0].Offset); err != nil {
		return fmt.Errorf("commit stream offset: %w", err)
	}
	encodedResponse, _ := json.Marshal(response)
	fmt.Printf("complete publisher finished queue=%s topic=%s stream=%s offset=%d response=%s task=%s\n", queueResult.MessageID, topicResult.MessageID, streamResult.MessageID, stream[0].Offset, encodedResponse, task.Status)
	return nil
}
