package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

// This is a differential harness: it runs the same logical operation through the
// embedded API and through the HTTP data plane, then compares what the broker
// actually stored. Code review found several places where the two paths disagree
// silently, which is the failure mode that matters most because neither side
// reports an error. Rather than assert each known case, this enumerates the
// fields a caller can set and reports every field that differs, so a new
// divergence shows up as a new line in the failure output.

// fieldCase is one field a publisher can set. The same value is applied to both
// sides so any difference in what the broker stored is a real divergence rather
// than a difference in the inputs.
type fieldCase struct {
	name  string
	value any
	// remoteKey is the JSON key the HTTP data plane expects, when it differs
	// from the field name.
	remoteKey string
	// extra adds wire fields the case needs beyond the primary one, for example
	// a deduplication window alongside its id.
	extra map[string]any
}

func newParityBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	if err := b.DeclareQueue(context.Background(), QueueConfig{Name: "diff"}); err != nil {
		t.Fatal(err)
	}
	return b
}

// applyEmbedded sets tc's field on the in-process Publisher.
func applyEmbedded(tc fieldCase, opts []PublishOption) []PublishOption {
	v := func() string { return fmt.Sprint(tc.value) }
	switch tc.name {
	case "routing_key":
		return append(opts, WithRoutingKey(v()))
	case "key":
		return append(opts, WithKey(v()))
	case "tenant_id":
		return append(opts, WithTenantID(v()))
	case "correlation_id":
		return append(opts, WithCorrelationID(v()))
	case "idempotency_key":
		return append(opts, WithIdempotencyKey(v()))
	case "priority":
		return append(opts, WithPriority(4))
	case "type":
		return append(opts, WithType(v()))
	case "headers":
		return append(opts, WithHeaders(map[string]string{"h": "1"}))
	case "metadata":
		return append(opts, WithMetadata(map[string]string{"m": "2"}))
	case "content_type":
		return append(opts, WithContentType(v()))
	case "schema":
		return append(opts, WithSchema("order", "9"))
	case "deduplication":
		return append(opts, WithDeduplication(v(), time.Minute))
	case "delay":
		return append(opts, WithDelay(time.Duration(tc.value.(int64))))
	case "trace_id":
		return append(opts, WithTraceID(v()))
	case "ttl":
		return append(opts, WithTTL(time.Duration(tc.value.(int64))))
	case "max_attempts":
		return append(opts, WithMaxAttempts(int(tc.value.(int))))
	case "producer_id":
		return append(opts, WithProducerID(v()))
	case "message_id":
		return append(opts, WithMessageID(v()))
	}
	return opts
}

func publishEmbedded(t *testing.T, b *Broker, tc fieldCase) string {
	t.Helper()
	opts := applyEmbedded(tc, []PublishOption{WithQueue("diff")})
	res, err := b.Publisher().Publish(context.Background(), "", []byte(`{"v":1}`), opts...)
	if err != nil {
		t.Fatalf("embedded publish: %v", err)
	}
	return res.MessageID
}

func publishRemote(t *testing.T, a AdminServer, body map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/publish", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.dataPlane(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("remote publish: status %d body %s", rec.Code, rec.Body.String())
	}
	var res PublishResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	if res.MessageID == "" {
		t.Fatalf("remote publish returned no message_id: %s", rec.Body.String())
	}
	return res.MessageID
}

// TestEmbeddedVsRemotePublishFieldParity is the differential test. Each case
// names a field a caller might set and reports whether the value survives the
// HTTP hop. Run it with -v to see the full matrix.
func TestEmbeddedVsRemotePublishFieldParity(t *testing.T) {
	cases := []fieldCase{
		{name: "routing_key", value: "rk-1"},
		{name: "key", value: "key-1"},
		{name: "tenant_id", value: "tenant-1"},
		{name: "correlation_id", value: "corr-1"},
		{name: "idempotency_key", value: "idem-1"},
		{name: "priority", value: 4},
		{name: "type", value: "order.v1"},
		{name: "headers", value: map[string]any{"h": "1"}},
		{name: "metadata", value: map[string]any{"m": "2"}},
		{name: "content_type", value: "application/x-parity"},
		{name: "schema", value: "order", extra: map[string]any{"schema_version": "9"}},
		{name: "deduplication", value: "dedup-1", remoteKey: "deduplication_id", extra: map[string]any{"deduplication_window_ns": int64(time.Minute)}},
		{name: "trace_id", value: "trace-1"},
		{name: "ttl", value: int64(time.Hour), remoteKey: "ttl_ns"},
		{name: "message_id", value: "producer-chosen-id"},
		{name: "max_attempts", value: 7},
		{name: "producer_id", value: "producer-1"},
		{name: "causation_id", value: "cause-1"},
		{name: "delay", value: int64(time.Hour), remoteKey: "delay_ns"},
	}

	var diverged []string
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			embeddedBroker := newParityBroker(t)
			embeddedID := publishEmbedded(t, embeddedBroker, tc)
			embeddedMsg, _, err := embeddedBroker.Store().Get(ctx, embeddedID)
			if err != nil {
				t.Fatal(err)
			}

			remoteBroker := newParityBroker(t)
			admin := AdminServer{Broker: remoteBroker, Token: "t", InsecureDev: true}
			remoteBody := map[string]any{"queue": "diff", "payload": json.RawMessage(`{"v":1}`)}
			key := tc.remoteKey
			if key == "" {
				key = tc.name
			}
			remoteBody[key] = tc.value
			for k, v := range tc.extra {
				remoteBody[k] = v
			}
			remoteID := publishRemote(t, admin, remoteBody)
			remoteMsg, _, err := remoteBroker.Store().Get(ctx, remoteID)
			if err != nil {
				t.Fatal(err)
			}

			diffs := compareMessage(embeddedMsg, remoteMsg)
			for _, d := range diffs {
				t.Logf("DIVERGES %-16s embedded=%q remote=%q", d.field, d.embedded, d.remote)
				diverged = append(diverged, tc.name+":"+d.field)
			}
		})
	}
	if len(diverged) > 0 {
		sort.Strings(diverged)
		t.Errorf("%d embedded/remote divergences:\n  %v", len(diverged), diverged)
	}
}

type fieldDiff struct {
	field    string
	embedded string
	remote   string
}

// compareMessage reports every field where the embedded and remote results
// disagree. It compares only fields the case is expected to populate, so a
// server-assigned field like ID never shows up as noise.
func compareMessage(embedded, remote Message) []fieldDiff {
	get := func(m Message) map[string]string {
		return map[string]string{
			"routing_key":      m.RoutingKey,
			"key":              m.Key,
			"tenant_id":        m.TenantID,
			"correlation_id":   m.CorrelationID,
			"idempotency_key":  m.IdempotencyKey,
			"type":             m.Type,
			"content_type":     m.ContentType,
			"schema":           m.Schema,
			"schema_version":   m.SchemaVersion,
			"trace_id":         m.TraceID,
			"deduplication_id": m.DeduplicationID,
			"producer_id":      m.ProducerID,
			"max_attempts":     fmt.Sprint(m.MaxAttempts),
			"priority":         fmt.Sprint(m.Priority),
			"headers":          fmt.Sprint(m.Headers),
			"metadata":         fmt.Sprint(m.Metadata),
			"has_expiry":       fmt.Sprint(!m.ExpiresAt.IsZero()),
			"payload":          string(m.Payload),
		}
	}
	le, lr := get(embedded), get(remote)
	var diffs []fieldDiff
	for _, f := range []string{
		"routing_key", "key", "tenant_id", "correlation_id", "idempotency_key",
		"type", "content_type", "schema", "schema_version", "trace_id",
		"deduplication_id", "producer_id", "max_attempts", "priority", "headers",
		"metadata", "has_expiry", "payload",
	} {
		if le[f] != lr[f] {
			diffs = append(diffs, fieldDiff{field: f, embedded: le[f], remote: lr[f]})
		}
	}
	return diffs
}

// TestEmbeddedVsRemoteBinaryPayload pins the payload encoding contract. The Go
// client used to let encoding/json turn a []byte into a base64 JSON string and
// the broker stored the literal characters, so six arbitrary bytes arrived as
// eight ASCII bytes while the embedded path stored them untouched.
func TestEmbeddedVsRemoteBinaryPayload(t *testing.T) {
	ctx := context.Background()
	raw := []byte{0x00, 0x01, 0xfe, 0xff, 'h', 'i'}

	embeddedBroker := newParityBroker(t)
	res, err := embeddedBroker.Publisher().Publish(ctx, "", raw, WithQueue("diff"))
	if err != nil {
		t.Fatal(err)
	}
	embeddedMsg, _, err := embeddedBroker.Store().Get(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}

	remoteBroker := newParityBroker(t)
	admin := AdminServer{Broker: remoteBroker, Token: "t", InsecureDev: true}
	remoteID := publishRemote(t, admin, map[string]any{
		"queue":            "diff",
		"payload":          base64.StdEncoding.EncodeToString(raw),
		"payload_encoding": "base64",
	})
	remoteMsg, _, err := remoteBroker.Store().Get(ctx, remoteID)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(embeddedMsg.Payload, remoteMsg.Payload) {
		t.Fatalf("payload diverges:\n  embedded=%q\n  remote =%q", embeddedMsg.Payload, remoteMsg.Payload)
	}
}

// TestPublishPayloadEncoding pins the encoding switch itself: the default stays
// untouched so existing clients keep working, base64 round trips exactly, and an
// unknown encoding is rejected rather than silently storing the wrong bytes.
func TestPublishPayloadEncoding(t *testing.T) {
	t.Run("default is passthrough json", func(t *testing.T) {
		p := PublishHTTPPayload{Payload: json.RawMessage(`{"a":1}`)}
		got, err := p.PayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != `{"a":1}` {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("explicit json is passthrough", func(t *testing.T) {
		p := PublishHTTPPayload{Payload: json.RawMessage(`{"a":1}`), PayloadEncoding: "json"}
		got, err := p.PayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != `{"a":1}` {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("base64 round trips arbitrary bytes", func(t *testing.T) {
		want := []byte{0x00, 0xff, 0x10, 0x80, 'a'}
		quoted, err := json.Marshal(base64.StdEncoding.EncodeToString(want))
		if err != nil {
			t.Fatal(err)
		}
		p := PublishHTTPPayload{Payload: json.RawMessage(quoted), PayloadEncoding: "base64"}
		got, err := p.PayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("got %q want %q", got, want)
		}
	})

	t.Run("unknown encoding is rejected", func(t *testing.T) {
		p := PublishHTTPPayload{Payload: json.RawMessage(`x`), PayloadEncoding: "rot13"}
		if _, err := p.PayloadBytes(); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("got %v, want ErrInvalidMessage", err)
		}
	})

	t.Run("bad base64 is rejected", func(t *testing.T) {
		bad, err := json.Marshal("!!!not base64!!!")
		if err != nil {
			t.Fatal(err)
		}
		p := PublishHTTPPayload{Payload: json.RawMessage(bad), PayloadEncoding: "base64"}
		if _, err := p.PayloadBytes(); err == nil {
			t.Fatal("expected an error for malformed base64")
		}
	})
}

// TestEmbeddedVsRemoteAckLeaseToken pins the lease token contract, which decides
// whether a remote consumer can ack at all.
func TestEmbeddedVsRemoteAckLeaseToken(t *testing.T) {
	ctx := context.Background()
	b := newParityBroker(t)
	defer b.Stop(ctx)

	res, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("diff"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "diff", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("leased %d, want 1", len(msgs))
	}
	if msgs[0].LeaseToken == "" {
		t.Fatal("embedded lease returned no lease token")
	}
	// Acking without the token must fail, which is what the HTTP handlers rely on.
	if err := b.Store().Ack(ctx, AckRequest{MessageID: res.MessageID, Queue: "diff", ConsumerID: "c1"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ack without lease token returned %v, want ErrLeaseLost", err)
	}
	if err := b.Store().Ack(ctx, AckRequest{MessageID: res.MessageID, Queue: "diff", ConsumerID: "c1", LeaseToken: msgs[0].LeaseToken}); err != nil {
		t.Fatalf("ack with lease token: %v", err)
	}
}
