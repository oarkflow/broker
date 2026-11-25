package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestResponseDeliveryConcurrentCancellation(t *testing.T) {
	b := MustNew(Config{})
	defer b.Stop(context.Background())
	for i := 0; i < 200; i++ {
		corr := fmt.Sprint(i)
		ch := make(chan Response, 1)
		if err := b.registerWaiter(corr, ch); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			b.completeResponse(Response{CorrelationID: corr, Headers: map[string]string{"x-stream-end": "true"}})
		})
		wg.Go(func() { b.unregisterWaiter(corr, ch) })
		wg.Wait()
	}
}

func TestRequestStreamCorrelationOwnership(t *testing.T) {
	b := MustNew(Config{})
	defer b.Stop(context.Background())
	p := b.Publisher()
	first, err := p.RequestStream(context.Background(), "jobs", "request", WithRequestCorrelationID("same"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := p.RequestStream(context.Background(), "jobs", "collision", WithRequestCorrelationID("same")); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("active correlation overwritten: %v", err)
	}
	if !b.completeResponse(Response{CorrelationID: "same", Headers: map[string]string{"x-stream-end": "true"}}) {
		t.Fatal("stream end was not delivered")
	}
	if !first.Next() || first.Next() || first.Next() || first.Err() != nil {
		t.Fatal("completed stream did not retain a clean EOF")
	}
	second, err := p.RequestStream(context.Background(), "jobs", "new request", WithRequestCorrelationID("same"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	first.Close()
	if !b.completeResponse(Response{CorrelationID: "same", Payload: []byte("new response")}) {
		t.Fatal("closing old stream removed new waiter")
	}
	if !second.Next() || string(second.Message().Payload) != "new response" {
		t.Fatal("new stream did not receive its response")
	}
}

func TestRequestStreamContextCleanupAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			b := MustNew(Config{})
			defer b.Stop(context.Background())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := []RequestOption{WithRequestCorrelationID("cancel")}
			if timeout {
				opts = append(opts, WithTimeout(20*time.Millisecond))
			}
			stream, err := b.Publisher().RequestStream(ctx, "jobs", "request", opts...)
			if err != nil {
				t.Fatal(err)
			}
			if !timeout {
				cancel()
			}
			// Cleanup must happen even if the caller never calls Next or Close.
			select {
			case _, open := <-stream.(*responseStream).ch:
				if open {
					t.Fatal("unexpected response")
				}
			case <-time.After(time.Second):
				t.Fatal("canceled stream retained waiter")
			}
			if stream.Next() || stream.Err() == nil {
				t.Fatal("canceled stream did not report its context error")
			}
		})
	}
}

func TestStreamEndCanBeRetriedAfterBufferFull(t *testing.T) {
	b := MustNew(Config{})
	defer b.Stop(context.Background())
	ch := make(chan Response, 1)
	if err := b.registerWaiter("full", ch); err != nil {
		t.Fatal(err)
	}
	if !b.completeResponse(Response{CorrelationID: "full"}) {
		t.Fatal("first response rejected")
	}
	end := Response{CorrelationID: "full", Headers: map[string]string{"x-stream-end": "true"}}
	if b.completeResponse(end) {
		t.Fatal("full buffer accepted response")
	}
	<-ch
	if !b.completeResponse(end) {
		t.Fatal("dropped end marker closed stream prematurely")
	}
	<-ch
	if _, open := <-ch; open {
		t.Fatal("stream not closed after accepted end marker")
	}
}

func TestBrokerStopReleasesRequestWaiters(t *testing.T) {
	b := MustNew(Config{})
	stream, err := b.Publisher().RequestStream(context.Background(), "jobs", "request")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-stream.(*responseStream).ch:
		if open {
			t.Fatal("unexpected response")
		}
	case <-time.After(time.Second):
		t.Fatal("broker stop retained request waiter")
	}
}

func TestRequestStreamPreservesHeadersAndStreamMarker(t *testing.T) {
	b := MustNew(Config{})
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	_, err := b.Subscribe(ctx, ConsumerConfig{Name: "stream-responder", Topic: "rpc.stream"}, func(ctx context.Context, d Delivery) error {
		if d.Message.Headers["x-request-stream"] != "true" || d.Message.Headers["x-client"] != "example" {
			return d.RespondError(ctx, ErrInvalidMessage, WithEndStream(true))
		}
		return d.Respond(ctx, "complete", WithEndStream(true))
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := b.Publisher().RequestStream(ctx, "rpc.stream", nil, WithTimeout(time.Second), WithRequestHeader("x-client", "example"), WithRequestHeader("x-request-stream", "false"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !stream.Next() || stream.Message().Error != nil {
		t.Fatalf("stream marker or user header lost: response=%+v, error=%v", stream.Message(), stream.Err())
	}
	if stream.Next() || stream.Err() != nil {
		t.Fatalf("stream did not end cleanly: %v", stream.Err())
	}
}
