package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/broker"
)

func runDurable(args []string) error {
	flags := flag.NewFlagSet("durable", flag.ContinueOnError)
	dataDir := flags.String("data", "", "WAL directory; empty uses a temporary directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	root := *dataDir
	remove := false
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "broker-complete-durable-")
		if err != nil {
			return err
		}
		remove = true
	}
	if remove {
		defer os.RemoveAll(root)
	}
	walDir := filepath.Join(root, "wal")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	open := func() (*broker.Broker, error) {
		store, err := broker.NewFileWALStore(walDir)
		if err != nil {
			return nil, err
		}
		return broker.New(broker.Config{Name: "complete-durable", Store: store, PollInterval: 2 * time.Millisecond})
	}

	first, err := open()
	if err != nil {
		return fmt.Errorf("open first broker: %w", err)
	}
	if err := first.Start(ctx); err != nil {
		return err
	}
	if err := first.DeclareQueue(ctx, broker.QueueConfig{Name: "durable.jobs"}); err != nil {
		return err
	}
	if err := first.DeclareStream(ctx, broker.StreamConfig{Name: "durable.events", Partitions: 1}); err != nil {
		return err
	}
	result, err := first.Publisher().Publish(ctx, "", []byte("survives restart"), broker.WithMessageID("durable-message-1"), broker.WithQueue("durable.jobs"), broker.WithProducer("durable-producer", 1, 0), broker.WithConfirmLevel(broker.ConfirmPersisted))
	if err != nil {
		return fmt.Errorf("persist message: %w", err)
	}
	if _, err := first.Publisher().Publish(ctx, "", []byte("stream survives restart"), broker.WithMessageID("durable-stream-1"), broker.WithStream("durable.events"), broker.WithConfirmLevel(broker.ConfirmPersisted)); err != nil {
		return fmt.Errorf("persist stream message: %w", err)
	}
	stopCtx, stop := stopContext()
	if err := first.Stop(stopCtx); err != nil {
		stop()
		return fmt.Errorf("stop first broker: %w", err)
	}
	stop()

	second, err := open()
	if err != nil {
		return fmt.Errorf("reopen broker: %w", err)
	}
	if err := second.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, stop := stopContext()
		defer stop()
		_ = second.Stop(stopCtx)
	}()
	recovered, ok, err := second.Store().Get(ctx, "durable-message-1")
	if err != nil || !ok || string(recovered.Payload) != "survives restart" {
		return fmt.Errorf("recover message: found=%t payload=%q err=%w", ok, recovered.Payload, err)
	}
	stream, err := second.ReadStream(ctx, broker.StreamReadRequest{Stream: "durable.events", ConsumerGroup: "recovery-check", Limit: 10})
	if err != nil || len(stream) != 1 {
		return fmt.Errorf("recover stream: messages=%d err=%w", len(stream), err)
	}
	retry, err := second.Publisher().Publish(ctx, "", []byte("survives restart"), broker.WithQueue("durable.jobs"), broker.WithProducer("durable-producer", 1, 0))
	if err != nil || !retry.Duplicate || retry.MessageID != result.MessageID {
		return fmt.Errorf("recover producer sequence: result=%+v err=%w", retry, err)
	}
	fmt.Printf("durable complete: message=%s stream_offset=%d producer_retry_duplicate=%t wal=%s\n", recovered.ID, stream[0].Offset, retry.Duplicate, walDir)
	return nil
}
