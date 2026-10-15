package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/broker"
)

func requestReplyExample(ctx context.Context, _ string) error {
	if err := withBroker(ctx, broker.NewMemoryStore(), func(b *broker.Broker) error {
		_, err := b.Subscribe(ctx, broker.ConsumerConfig{Name: "echo", Topic: "rpc.echo", AckMode: broker.AckAuto}, func(ctx context.Context, d broker.Delivery) error {
			if d.Message.Headers["x-request-stream"] == "true" {
				if err := d.Respond(ctx, "part-1"); err != nil {
					return err
				}
				return d.Respond(ctx, "part-2", broker.WithEndStream(true))
			}
			var request string
			if err := d.Decode(&request); err != nil {
				return err
			}
			return d.Respond(ctx, "echo:"+request)
		})
		if err != nil {
			return err
		}
		var reply string
		if err := b.Publisher().Request(ctx, "rpc.echo", "hello", &reply, broker.WithTimeout(time.Second)); err != nil {
			return err
		}
		stream, err := b.Publisher().RequestStream(ctx, "rpc.echo", "stream", broker.WithTimeout(time.Second))
		if err != nil {
			return err
		}
		defer stream.Close()
		parts := []string{}
		for stream.Next() {
			var part string
			if err := json.Unmarshal(stream.Message().Payload, &part); err != nil {
				return err
			}
			parts = append(parts, part)
		}
		if err := stream.Err(); err != nil {
			return err
		}
		if len(parts) != 2 {
			return fmt.Errorf("expected two response parts, got %v", parts)
		}
		fmt.Printf("  Request: %s; stream: %v followed by clean EOF\n", reply, parts)

		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		first, err := b.Publisher().RequestStream(cctx, "rpc.unhandled", nil, broker.WithRequestCorrelationID("owned"))
		if err != nil {
			return err
		}
		defer first.Close()
		_, err = b.Publisher().RequestStream(ctx, "rpc.unhandled", nil, broker.WithRequestCorrelationID("owned"))
		if err := expectError(err, broker.ErrDuplicateMessage); err != nil {
			return err
		}
		cancel()
		if first.Next() {
			return fmt.Errorf("canceled stream returned a response")
		}
		if err := expectError(first.Err(), context.Canceled); err != nil {
			return err
		}
		replacement, err := b.Publisher().RequestStream(ctx, "rpc.unhandled", nil, broker.WithRequestCorrelationID("owned"), broker.WithTimeout(50*time.Millisecond))
		if err != nil {
			return err
		}
		defer replacement.Close()
		first.Close() // Closing the old stream cannot remove the replacement's waiter.
		_, err = b.Publisher().RequestStream(ctx, "rpc.unhandled", nil, broker.WithRequestCorrelationID("owned"))
		if err := expectError(err, broker.ErrDuplicateMessage); err != nil {
			return err
		}
		if replacement.Next() {
			return fmt.Errorf("unhandled request unexpectedly responded")
		}
		if err := expectError(replacement.Err(), context.DeadlineExceeded); err != nil {
			return err
		}
		fmt.Println("  Active correlation rejected; cancellation, safe reuse and explicit timeout handled")
		return nil
	}); err != nil {
		return err
	}
	b, err := broker.New(broker.Config{})
	if err != nil {
		return err
	}
	stream, err := b.Publisher().RequestStream(ctx, "rpc.unhandled", nil)
	if err != nil {
		b.Stop(context.Background())
		return err
	}
	if err := b.Stop(context.Background()); err != nil {
		return err
	}
	defer stream.Close()
	if stream.Next() {
		return fmt.Errorf("stopped broker unexpectedly responded")
	}
	fmt.Println("  Broker shutdown releases outstanding request streams")
	return nil
}
