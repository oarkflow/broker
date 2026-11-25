package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/broker"
)

func TestClientStreamTransactionBinaryPayloadAndConflict(t *testing.T) {
	ctx := context.Background()
	b := broker.MustNew(broker.Config{})
	defer b.Stop(ctx)
	if err := b.DeclareStream(ctx, broker.StreamConfig{Name: "input"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", "input", broker.WithStream("input")); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((broker.AdminServer{Broker: b, Token: "secret"}).Handler())
	defer server.Close()
	c := New(Config{BrokerURL: server.URL, Token: "secret"})
	data := []byte{0, 255, 1, 254}
	raw, err := json.Marshal(base64.StdEncoding.EncodeToString(data))
	if err != nil {
		t.Fatal(err)
	}
	tx := broker.StreamTransactionHTTPPayload{ConsumerGroup: "workers", Offset: 1, Messages: []broker.PublishHTTPPayload{{Topic: "processed", Queue: "jobs", Payload: raw, PayloadEncoding: "base64"}}}
	results, err := c.CommitStreamTransaction(ctx, "input", tx)
	if err != nil || len(results) != 1 {
		t.Fatalf("results=%v, error=%v", results, err)
	}
	message, ok, err := b.Store().Get(ctx, results[0].MessageID)
	if err != nil || !ok || string(message.Payload) != string(data) {
		t.Fatalf("binary output=%v, exists=%v, error=%v", message.Payload, ok, err)
	}
	if _, err := c.CommitStreamTransaction(ctx, "input", tx); !errors.Is(err, broker.ErrOffsetConflict) {
		t.Fatalf("remote conflict is not typed: %v", err)
	}
	if offset, err := c.StreamOffset(ctx, "input", "workers"); err != nil || offset != 1 {
		t.Fatalf("offset=%d, error=%v", offset, err)
	}
}
