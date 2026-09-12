package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/oarkflow/broker"
)

// runEmbedded demonstrates the library API without a network server:
// competing queue workers, manual acknowledgement, topic fan-out, wildcard
// subscriptions, batch delivery, streams and committed offsets, atomic
// publishing, idempotent producers, and tracked tasks.
func runEmbedded(args []string) error {
	flags := flag.NewFlagSet("embedded", flag.ContinueOnError)
	timeout := flags.Duration("timeout", 10*time.Second, "maximum example runtime")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	b, err := broker.New(broker.Config{
		Name:                  "complete-embedded",
		PerformanceProfile:    broker.ProfileDurable,
		PollInterval:          2 * time.Millisecond,
		EnableTaskTracking:    true,
		EnableRequestResponse: true,
	})
	if err != nil {
		return err
	}
	if err := b.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, stop := stopContext()
		defer stop()
		_ = b.Stop(stopCtx)
	}()

	for _, queue := range []broker.QueueConfig{
		{Name: "jobs", AckMode: broker.AckManual, VisibilityTimeout: time.Second},
		{Name: "batches", AckMode: broker.AckAuto},
	} {
		if err := b.DeclareQueue(ctx, queue); err != nil {
			return fmt.Errorf("declare queue %s: %w", queue.Name, err)
		}
	}
	if err := b.DeclareTopic(ctx, broker.TopicConfig{Name: "orders.created", Partitions: 4}); err != nil {
		return fmt.Errorf("declare topic: %w", err)
	}
	if err := b.DeclareStream(ctx, broker.StreamConfig{Name: "orders.events", Partitions: 4}); err != nil {
		return fmt.Errorf("declare stream: %w", err)
	}

	jobs := make(chan string, 8)
	for _, worker := range []string{"worker-a", "worker-b"} {
		worker := worker
		_, err := b.Subscribe(ctx, broker.ConsumerConfig{
			Name: worker, Group: "job-workers", Queue: "jobs",
			Concurrency: 2, MaxInFlight: 4, AckMode: broker.AckManual,
			Strategy: broker.StrategyRoundRobin,
		}, func(handlerCtx context.Context, delivery broker.Delivery) error {
			if err := delivery.Ack(handlerCtx); err != nil {
				return err
			}
			jobs <- worker
			return nil
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", worker, err)
		}
	}

	topicEvents := make(chan string, 2)
	_, err = b.Subscribe(ctx, broker.ConsumerConfig{Name: "billing", Group: "billing", Topic: "orders.created"}, func(_ context.Context, delivery broker.Delivery) error {
		topicEvents <- "billing:" + delivery.Message.Topic
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe exact topic: %w", err)
	}
	_, err = b.Subscribe(ctx, broker.ConsumerConfig{Name: "analytics", Group: "analytics", Pattern: "orders.*"}, func(_ context.Context, delivery broker.Delivery) error {
		topicEvents <- "analytics:" + delivery.Message.Topic
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe wildcard topic: %w", err)
	}

	batchSizes := make(chan int, 1)
	_, err = b.SubscribeBatch(ctx, broker.ConsumerConfig{Name: "batch-worker", Group: "batch-workers", Queue: "batches", BatchSize: 3}, func(_ context.Context, deliveries []broker.Delivery) error {
		batchSizes <- len(deliveries)
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe batch: %w", err)
	}

	_, err = b.RegisterTask(ctx, "invoice.render", func(handlerCtx context.Context, delivery broker.TaskDelivery) (any, error) {
		var request map[string]string
		if err := delivery.Decode(&request); err != nil {
			return nil, err
		}
		_ = delivery.Progress(handlerCtx, broker.Progress{Percent: 50, Message: "rendering", UpdatedAt: time.Now()})
		return map[string]string{"invoice": request["invoice"], "status": "rendered"}, nil
	})
	if err != nil {
		return fmt.Errorf("register task: %w", err)
	}

	pub := b.Publisher()
	for sequence := uint64(0); sequence < 4; sequence++ {
		if _, err := pub.Publish(ctx, "", map[string]uint64{"sequence": sequence}, broker.WithQueue("jobs"), broker.WithProducer("complete-producer", 1, sequence)); err != nil {
			return fmt.Errorf("publish job %d: %w", sequence, err)
		}
	}
	duplicate, err := pub.Publish(ctx, "", map[string]uint64{"sequence": 3}, broker.WithQueue("jobs"), broker.WithProducer("complete-producer", 1, 3))
	if err != nil || !duplicate.Duplicate {
		return fmt.Errorf("idempotent retry: duplicate=%t err=%w", duplicate.Duplicate, err)
	}

	tx, err := pub.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	if err := tx.Publish("", map[string]int{"transaction": 1}, broker.WithQueue("jobs")); err != nil {
		return err
	}
	if err := tx.Publish("", map[string]int{"transaction": 2}, broker.WithQueue("jobs")); err != nil {
		return err
	}
	if results, err := tx.Commit(ctx); err != nil || len(results) != 2 {
		return fmt.Errorf("commit transaction: results=%d err=%w", len(results), err)
	}

	if _, err := pub.PublishBatch(ctx, "", []any{"one", "two", "three"}, broker.WithQueue("batches")); err != nil {
		return fmt.Errorf("publish batch: %w", err)
	}
	order, err := pub.Publish(ctx, "orders.created", map[string]string{"order_id": "order-101"}, broker.WithStream("orders.events"), broker.WithKey("customer-7"))
	if err != nil {
		return fmt.Errorf("publish topic and stream: %w", err)
	}

	task, err := pub.SubmitTask(ctx, "invoice.render", map[string]string{"invoice": "INV-101"})
	if err != nil {
		return fmt.Errorf("submit task: %w", err)
	}
	taskResult, err := task.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait task: %w", err)
	}
	var rendered map[string]string
	if err := json.Unmarshal(taskResult.Payload, &rendered); err != nil {
		return fmt.Errorf("decode task result: %w", err)
	}

	workers, err := receiveN(ctx, jobs, 6)
	if err != nil {
		return fmt.Errorf("queue workers: %w", err)
	}
	fanout, err := receiveN(ctx, topicEvents, 2)
	if err != nil {
		return fmt.Errorf("topic fan-out: %w", err)
	}
	batches, err := receiveN(ctx, batchSizes, 1)
	if err != nil || batches[0] != 3 {
		return fmt.Errorf("batch delivery: sizes=%v err=%w", batches, err)
	}

	streamMessages, err := b.ReadStream(ctx, broker.StreamReadRequest{Stream: "orders.events", ConsumerGroup: "projection", Limit: 10})
	if err != nil || len(streamMessages) != 1 {
		return fmt.Errorf("read stream: messages=%d err=%w", len(streamMessages), err)
	}
	lastOffset := streamMessages[len(streamMessages)-1].Offset
	if err := b.CommitOffset(ctx, broker.OffsetCommit{Stream: "orders.events", ConsumerGroup: "projection", Offset: lastOffset}); err != nil {
		return fmt.Errorf("commit stream offset: %w", err)
	}

	counts := map[string]int{}
	for _, worker := range workers {
		counts[worker]++
	}
	sort.Strings(fanout)
	fmt.Printf("embedded complete: jobs=%d distribution=%v fanout=%v batch=%d stream_message=%s offset=%d task=%s\n", len(workers), counts, fanout, batches[0], order.MessageID, lastOffset, rendered["status"])
	return nil
}
