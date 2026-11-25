package client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/fh"
)

// The request-path benchmarks measure the client's own per-call cost: the
// server answers every route with a canned body so broker work does not
// dominate the profile.

func newCannedServer(b *testing.B) (string, broker.FrameCodec) {
	b.Helper()
	codec := broker.NewFrameCodec(broker.FrameCodecConfig{})
	msgs := make([]broker.Message, 32)
	for i := range msgs {
		msgs[i] = broker.Message{ID: "m-" + string(rune('a'+i%26)), Topic: "bench", Queue: "q", Payload: []byte(`{"order":123,"status":"paid"}`), LeaseToken: "lease-token", Attempts: 1, CreatedAt: time.Now()}
	}
	pollJSON, _ := json.Marshal(msgs)
	pollFrames, err := broker.EncodeMessageFrameBatch(nil, codec, msgs)
	if err != nil {
		b.Fatal(err)
	}
	publishResult, _ := json.Marshal(broker.PublishResult{MessageID: "m-1", Confirmed: broker.ConfirmPersisted})
	app := fh.New(fh.WithStartupBannerDisabled(true), fh.WithLogger(fh.NewNoopLogger()))
	app.Post("/v1/poll", func(c fh.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.Send(pollJSON)
	})
	app.Post("/v1/poll/frame", func(c fh.Ctx) error {
		c.Set("Content-Type", "application/vnd.pool.broker.frames")
		return c.Send(pollFrames)
	})
	app.Post("/v1/messages/:id/ack", func(c fh.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.SendString("{\"ok\":true}\n")
	})
	app.Post("/v1/publish", func(c fh.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.Send(publishResult)
	})
	app.Post("/v1/publish/frame", func(c fh.Ctx) error {
		c.Set("Content-Type", "application/json")
		return c.Send(publishResult)
	})
	srv := newTestServer(b, app)
	return srv.URL, codec
}

func benchClient(b *testing.B) (*Client, broker.FrameCodec) {
	url, codec := newCannedServer(b)
	return New(Config{BrokerURL: url, Token: "bench-token", ConsumerID: "c", Group: "g", FrameCodec: codec, PollWait: time.Second}), codec
}

func BenchmarkRequestPathPollJSON(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if msgs, err := c.Poll(ctx, "q", 32); err != nil || len(msgs) != 32 {
			b.Fatalf("poll: %d %v", len(msgs), err)
		}
	}
}

func BenchmarkRequestPathPollFrames(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if msgs, err := c.PollFrames(ctx, "q", 32); err != nil || len(msgs) != 32 {
			b.Fatalf("poll frames: %d %v", len(msgs), err)
		}
	}
}

func BenchmarkRequestPathAck(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	m := broker.Message{ID: "m-1", Queue: "q", LeaseToken: "lease-token"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Ack(ctx, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestPathPublishJSON(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	payload := map[string]any{"order": 123, "status": "paid"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Publish(ctx, "bench", payload, WithQueue("q")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestPathPublishFrame(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	msg := broker.Message{Topic: "bench", Queue: "q", Payload: []byte(`{"order":123,"status":"paid"}`)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.PublishFrame(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRequestPathParallelAck exercises connection reuse and pooling under
// concurrency, as a busy worker pool acknowledging messages does.
func BenchmarkRequestPathParallelAck(b *testing.B) {
	c, _ := benchClient(b)
	ctx := context.Background()
	m := broker.Message{ID: "m-1", Queue: "q", LeaseToken: "lease-token"}
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Ack(ctx, m); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
