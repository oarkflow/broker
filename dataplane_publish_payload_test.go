package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// PublishHTTPPayload used to carry no JSON tags, so encoding/json matched Go
// field names only. Go's fallback is case-insensitive, which accidentally
// accepted "Topic", but the underscore in the documented snake_case names broke
// it entirely: tenant_id, routing_key, correlation_id, idempotency_key and
// timeout_ns were all silently dropped. These tests pin both the documented
// format and backward compatibility with the old Go field names.

func TestPublishHTTPPayloadAcceptsDocumentedSnakeCase(t *testing.T) {
	body := []byte(`{
		"topic":"orders.created",
		"queue":"q1",
		"stream":"s1",
		"type":"order.v2",
		"payload":{"id":7},
		"headers":{"h":"1"},
		"metadata":{"m":"2"},
		"key":"k1",
		"routing_key":"rk1",
		"priority":5,
		"tenant_id":"tenant-a",
		"correlation_id":"corr-1",
		"timeout_ns":1234,
		"idempotency_key":"idem-1"
	}`)
	var got PublishHTTPPayload
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Topic != "orders.created" || got.Queue != "q1" || got.Stream != "s1" || got.Type != "order.v2" {
		t.Fatalf("destination fields lost: %+v", got)
	}
	if got.RoutingKey != "rk1" {
		t.Fatalf("routing_key lost: %+v", got)
	}
	if got.TenantID != "tenant-a" {
		t.Fatalf("tenant_id lost: %+v", got)
	}
	if got.CorrelationID != "corr-1" {
		t.Fatalf("correlation_id lost: %+v", got)
	}
	if got.IdempotencyKey != "idem-1" {
		t.Fatalf("idempotency_key lost: %+v", got)
	}
	if got.TimeoutNS != 1234 {
		t.Fatalf("timeout_ns lost: %+v", got)
	}
	if got.Key != "k1" || got.Priority != 5 {
		t.Fatalf("key/priority lost: %+v", got)
	}
	if got.Headers["h"] != "1" || got.Metadata["m"] != "2" {
		t.Fatalf("headers/metadata lost: %+v", got)
	}
	if string(got.Payload) != `{"id":7}` {
		t.Fatalf("payload lost: %s", got.Payload)
	}
}

func TestPublishHTTPPayloadStillAcceptsGoFieldNames(t *testing.T) {
	body := []byte(`{
		"Topic":"orders.created",
		"Queue":"q1",
		"Type":"order.v2",
		"Payload":{"id":7},
		"Key":"k1",
		"RoutingKey":"rk1",
		"Priority":5,
		"TenantID":"tenant-a",
		"CorrelationID":"corr-1",
		"TimeoutNS":1234,
		"IdempotencyKey":"idem-1"
	}`)
	var got PublishHTTPPayload
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Topic != "orders.created" || got.Queue != "q1" || got.Type != "order.v2" {
		t.Fatalf("destination fields lost: %+v", got)
	}
	if got.RoutingKey != "rk1" || got.TenantID != "tenant-a" {
		t.Fatalf("multi-word fields lost: %+v", got)
	}
	if got.CorrelationID != "corr-1" || got.IdempotencyKey != "idem-1" || got.TimeoutNS != 1234 {
		t.Fatalf("multi-word fields lost: %+v", got)
	}
	if got.Key != "k1" || got.Priority != 5 || string(got.Payload) != `{"id":7}` {
		t.Fatalf("single-word fields lost: %+v", got)
	}
}

// A client that sends both spellings must get the documented snake_case value;
// the compatibility pass only fills fields the primary decode left empty.
func TestPublishHTTPPayloadPrefersSnakeCaseOverLegacyKeys(t *testing.T) {
	body := []byte(`{"topic":"t","routing_key":"new","RoutingKey":"old","tenant_id":"new-tenant","TenantID":"old-tenant"}`)
	var got PublishHTTPPayload
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.RoutingKey != "new" {
		t.Fatalf("routing_key should win, got %q", got.RoutingKey)
	}
	if got.TenantID != "new-tenant" {
		t.Fatalf("tenant_id should win, got %q", got.TenantID)
	}
}

// The in-repo client marshals this same struct, so both ends must agree.
func TestPublishHTTPPayloadMarshalsSnakeCase(t *testing.T) {
	out, err := json.Marshal(PublishHTTPPayload{
		Topic:          "t",
		RoutingKey:     "rk",
		TenantID:       "tenant-a",
		CorrelationID:  "corr",
		IdempotencyKey: "idem",
		TimeoutNS:      9,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"topic":"t"`, `"routing_key":"rk"`, `"tenant_id":"tenant-a"`,
		`"correlation_id":"corr"`, `"idempotency_key":"idem"`, `"timeout_ns":9`,
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("missing %s in %s", want, out)
		}
	}
}

func TestPublishHTTPPayloadRejectsInvalidJSON(t *testing.T) {
	var got PublishHTTPPayload
	if err := json.Unmarshal([]byte(`{"topic":`), &got); err == nil {
		t.Fatal("expected an error for malformed json")
	}
}

// End-to-end through the real handler: the tenant must survive the round trip
// and reach the message the broker stored.
func TestV1PublishPreservesTenantFromSnakeCaseBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"snake_case", `{"topic":"t","tenant_id":"tenant-a","routing_key":"rk","payload":{"a":1}}`},
		{"go_field_names", `{"Topic":"t","TenantID":"tenant-a","RoutingKey":"rk","Payload":{"a":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Stop(context.Background())
			a := AdminServer{Broker: b, Token: "t", InsecureDev: true}

			req := httptest.NewRequest(http.MethodPost, "/v1/publish", bytes.NewReader([]byte(tc.body)))
			rec := httptest.NewRecorder()
			a.dataPlane(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
			}

			var res PublishResult
			if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if len(res.Messages) == 0 {
				t.Fatalf("no message in response: %s", rec.Body.String())
			}
			stored, ok, err := b.Store().Get(context.Background(), res.Messages[0].ID)
			if err != nil || !ok {
				t.Fatalf("get message: ok=%v err=%v", ok, err)
			}
			if stored.TenantID != "tenant-a" {
				t.Fatalf("tenant_id lost in transit: %q", stored.TenantID)
			}
			if stored.RoutingKey != "rk" {
				t.Fatalf("routing_key lost in transit: %q", stored.RoutingKey)
			}
		})
	}
}

// The batch endpoint wraps PublishHTTPPayload in an outer struct, so this proves
// encoding/json dispatches UnmarshalJSON for slice elements too.
func TestV1PublishBatchPreservesTenantFromSnakeCaseBody(t *testing.T) {
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	a := AdminServer{Broker: b, Token: "t", InsecureDev: true}

	body := `{"messages":[{"topic":"t1","tenant_id":"tenant-a","payload":{"a":1}},{"topic":"t2","tenant_id":"tenant-b","payload":{"b":2}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/publish/batch", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	a.dataPlane(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var res BatchPublishResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %d: %s", len(res.Results), rec.Body.String())
	}
	for i, want := range []string{"tenant-a", "tenant-b"} {
		id := res.Results[i].MessageID
		if id == "" {
			t.Fatalf("result %d missing message_id: %+v", i, res.Results[i])
		}
		stored, ok, err := b.Store().Get(context.Background(), id)
		if err != nil || !ok {
			t.Fatalf("get message %d: ok=%v err=%v", i, ok, err)
		}
		if stored.TenantID != want {
			t.Fatalf("message %d tenant = %q, want %q", i, stored.TenantID, want)
		}
	}
}
