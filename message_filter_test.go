package broker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMessageFilterExpressions(t *testing.T) {
	// 1. Header equality
	f1, err := ParseFilterExpression("headers[\"region\"] == \"us-west\"")
	if err != nil {
		t.Fatal(err)
	}
	if !f1.Match(Message{Headers: map[string]string{"region": "us-west"}}) {
		t.Fatal("expected match for region us-west")
	}
	if f1.Match(Message{Headers: map[string]string{"region": "eu-central"}}) {
		t.Fatal("expected no match for eu-central")
	}

	// 2. Header inequality & has
	f2, err := ParseFilterExpression("headers[\"env\"] != \"prod\"")
	if err != nil {
		t.Fatal(err)
	}
	if !f2.Match(Message{Headers: map[string]string{"env": "staging"}}) {
		t.Fatal("expected match for env staging")
	}
	if f2.Match(Message{Headers: map[string]string{"env": "prod"}}) {
		t.Fatal("expected no match for env prod")
	}

	fHas, err := ParseFilterExpression("headers.has(\"tenant_id\")")
	if err != nil {
		t.Fatal(err)
	}
	if !fHas.Match(Message{Headers: map[string]string{"tenant_id": "acme"}}) {
		t.Fatal("expected match for headers.has(tenant_id)")
	}
	if fHas.Match(Message{Headers: map[string]string{"other": "1"}}) {
		t.Fatal("expected no match for headers without tenant_id")
	}

	// 3. Type and Topic matching
	fType, err := ParseFilterExpression("type == \"order.completed\"")
	if err != nil {
		t.Fatal(err)
	}
	if !fType.Match(Message{Type: "order.completed"}) {
		t.Fatal("expected match for order.completed")
	}

	// 4. Priority comparison
	fPrio, err := ParseFilterExpression("priority >= 5")
	if err != nil {
		t.Fatal(err)
	}
	if !fPrio.Match(Message{Priority: 7}) {
		t.Fatal("expected match for priority 7 >= 5")
	}
	if fPrio.Match(Message{Priority: 3}) {
		t.Fatal("expected no match for priority 3 >= 5")
	}

	// 5. AND / OR conjunctions
	fAnd, err := ParseFilterExpression("headers[\"region\"] == \"us-west\" && priority >= 5")
	if err != nil {
		t.Fatal(err)
	}
	if !fAnd.Match(Message{Headers: map[string]string{"region": "us-west"}, Priority: 5}) {
		t.Fatal("expected match for AND")
	}
	if fAnd.Match(Message{Headers: map[string]string{"region": "us-west"}, Priority: 4}) {
		t.Fatal("expected no match for AND with priority 4")
	}

	fOr, err := ParseFilterExpression("headers[\"region\"] == \"us-west\" || headers[\"region\"] == \"eu-central\"")
	if err != nil {
		t.Fatal(err)
	}
	if !fOr.Match(Message{Headers: map[string]string{"region": "eu-central"}}) {
		t.Fatal("expected match for OR")
	}
	if fOr.Match(Message{Headers: map[string]string{"region": "ap-south"}}) {
		t.Fatal("expected no match for ap-south")
	}
}

func TestConsumerMessageFiltering(t *testing.T) {
	b := MustNew(Config{
		Store:        NewMemoryStore(),
		PollInterval: 10 * time.Millisecond,
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()
	received := make([]string, 0)
	var mu sync.Mutex
	done := make(chan struct{}, 2)

	// Subscribe with filter: only receive messages with region == "us-east"
	sub, err := b.Subscribe(ctx, ConsumerConfig{
		Queue:            "filtered-orders",
		FilterExpression: "headers[\"region\"] == \"us-east\"",
		AckMode:          AckAuto,
	}, func(ctx context.Context, d Delivery) error {
		mu.Lock()
		received = append(received, string(d.Message.Payload))
		mu.Unlock()
		done <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()

	// Publish message 1: region = "us-west" (should be filtered out and auto-acked)
	_, err = b.Publisher().Publish(ctx, "orders", "order-west",
		WithQueue("filtered-orders"),
		WithHeader("region", "us-west"),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Publish message 2: region = "us-east" (should be delivered)
	_, err = b.Publisher().Publish(ctx, "orders", "order-east",
		WithQueue("filtered-orders"),
		WithHeader("region", "us-east"),
	)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for filtered message delivery")
	}

	// Wait a moment to ensure no unexpected messages arrive
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != "\"order-east\"" {
		t.Fatalf("expected only order-east to be delivered, got: %v", received)
	}
}
