package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The console's create dialogs are the only place these request shapes are
// written down. /publish/batch already shipped documented as {"messages":[...]}
// when the API wanted a bare array, so each shape the console sends is pinned
// here rather than left to be discovered in a browser.
func consoleCreateServer(t *testing.T) (*Broker, *httptest.Server) {
	t.Helper()
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true}.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = b.Stop(t.Context())
	})
	return b, srv
}

// post sends exactly the body the console's dialog builds for this endpoint.
func consolePost(t *testing.T, srv *httptest.Server, path string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.Bytes()
}

// A queue declared from the console must exist with the settings that were asked
// for, including the nanosecond duration the dialog converts "30s" into.
func TestConsoleCreateQueue(t *testing.T) {
	_, srv := consoleCreateServer(t)

	code, body := consolePost(t, srv, "/queues", map[string]any{
		"name":               "orders",
		"max_inflight":       10,
		"max_depth":          500,
		"visibility_timeout": int64(30 * 1e9), // what durationToNs("30s") produces
		"ack_mode":           "manual",
		"dlq":                "orders.dlq",
	})
	if code != http.StatusOK {
		t.Fatalf("declare queue: %d %s", code, body)
	}

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st struct {
		Store struct {
			Queues map[string]int `json:"queues"`
		} `json:"store"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Store.Queues["orders"]; !ok {
		t.Fatalf("queue not created: %v", st.Store.Queues)
	}

	// There is no endpoint that returns a stored QueueConfig, so assert what is
	// observable: the queue is declared and immediately usable.
	resp2, err := http.Get(srv.URL + "/queues/orders/messages?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("declared queue is not usable: %d", resp2.StatusCode)
	}
}

func TestConsoleCreateTopicAndStream(t *testing.T) {
	_, srv := consoleCreateServer(t)

	code, body := consolePost(t, srv, "/topics", map[string]any{
		"name":       "orders.created",
		"partitions": 2,
		"retention":  int64(24 * 3600 * 1e9),
	})
	if code != http.StatusOK {
		t.Fatalf("declare topic: %d %s", code, body)
	}

	code, body = consolePost(t, srv, "/streams", map[string]any{
		"name":       "audit",
		"partitions": 1,
		"retention":  int64(7 * 24 * 3600 * 1e9),
		"compact":    true,
	})
	if code != http.StatusOK {
		t.Fatalf("create stream: %d %s", code, body)
	}

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st struct {
		Store struct {
			Topics  map[string]int `json:"topics"`
			Streams map[string]int `json:"streams"`
		} `json:"store"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Store.Topics["orders.created"]; !ok {
		t.Errorf("topic not created: %v", st.Store.Topics)
	}
	if _, ok := st.Store.Streams["audit"]; !ok {
		t.Errorf("stream not created: %v", st.Store.Streams)
	}
}

// Registering a consumer from the console must yield a usable id, and the
// comma-separated queue list must become an array.
func TestConsoleCreateConsumer(t *testing.T) {
	_, srv := consoleCreateServer(t)
	consolePost(t, srv, "/queues", map[string]any{"name": "orders"})
	consolePost(t, srv, "/queues", map[string]any{"name": "invoices"})

	code, body := consolePost(t, srv, "/v1/consumers/register", map[string]any{
		"consumer_id":  "ui-consumer",
		"group":        "workers",
		"queues":       []string{"orders", "invoices"}, // what kind:"list" produces
		"topics":       []string{},
		"concurrency":  2,
		"max_inflight": 8,
		"ack_mode":     "manual",
	})
	if code != http.StatusOK {
		t.Fatalf("register consumer: %d %s", code, body)
	}
	var info ConsumerInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatal(err)
	}
	if info.ConsumerID == "" {
		t.Fatal("no consumer_id returned; the console cannot show what it created")
	}
	if len(info.Queues) != 2 {
		t.Errorf("queues = %v, want two entries", info.Queues)
	}
	if info.Concurrency != 2 || info.MaxInFlight != 8 {
		t.Errorf("concurrency=%d max_inflight=%d, want 2 and 8", info.Concurrency, info.MaxInFlight)
	}

	// It must then be controllable through the actions the console renders.
	for _, action := range []string{"pause", "drain", "resume"} {
		resp, err := http.Post(srv.URL+"/v1/consumers/"+info.ConsumerID+"/"+action, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("consumer %s: %d", action, resp.StatusCode)
		}
	}
}

// Leaving the id blank must still work: the broker assigns one and the console
// displays the returned value.
func TestConsoleCreateConsumerWithoutID(t *testing.T) {
	_, srv := consoleCreateServer(t)
	code, body := consolePost(t, srv, "/v1/consumers/register", map[string]any{
		"group":  "workers",
		"queues": []string{"default"},
	})
	if code != http.StatusOK {
		t.Fatalf("register: %d %s", code, body)
	}
	var info ConsumerInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatal(err)
	}
	if info.ConsumerID == "" {
		t.Fatal("broker did not assign a consumer_id")
	}
}

// The publish dialog nests the payload as parsed JSON, and the destination key is
// chosen from queue/topic/stream.
func TestConsolePublish(t *testing.T) {
	b, srv := consoleCreateServer(t)
	consolePost(t, srv, "/queues", map[string]any{"name": "orders"})

	for _, dest := range []struct {
		key  string
		name string
	}{{"queue", "orders"}, {"topic", "events"}, {"stream", "audit"}} {
		body := map[string]any{
			dest.key:  dest.name,
			"type":    "test",
			"payload": map[string]any{"hello": "world"},
		}
		code, out := consolePost(t, srv, "/publish", body)
		if code != http.StatusOK {
			t.Fatalf("publish to %s: %d %s", dest.key, code, out)
		}
		var res struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		if res.MessageID == "" {
			t.Errorf("publish to %s returned no message_id: %s", dest.key, out)
		}
	}

	// The queue publish must be readable back with its payload intact.
	msgs, err := b.Store().ListQueue(t.Context(), "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("queue depth = %d, want 1", len(msgs))
	}
	var got map[string]any
	if err := json.Unmarshal(msgs[0].Payload, &got); err != nil {
		t.Fatalf("payload is not the JSON that was sent: %v", err)
	}
	if got["hello"] != "world" {
		t.Errorf("payload = %v, want hello=world", got)
	}
}

// Routes need structured destinations, so the dialog builds a `to` array from
// the comma-separated queue field.
func TestConsoleCreateRoute(t *testing.T) {
	_, srv := consoleCreateServer(t)
	code, body := consolePost(t, srv, "/routes", map[string]any{
		"name":    "orders-to-queue",
		"match":   "orders.*",
		"mode":    "publish",
		"to":      []map[string]string{{"type": "queue", "name": "orders"}, {"type": "queue", "name": "invoices"}},
		"enabled": true,
	})
	if code != http.StatusOK {
		t.Fatalf("add route: %d %s", code, body)
	}

	resp, err := http.Get(srv.URL + "/routes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var routes []Route
	if err := json.NewDecoder(resp.Body).Decode(&routes); err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("got %d routes, want 1", len(routes))
	}
	if routes[0].Name != "orders-to-queue" || routes[0].Match != "orders.*" {
		t.Errorf("route = %+v", routes[0])
	}
	if len(routes[0].To) != 2 {
		t.Errorf("route has %d destinations, want 2", len(routes[0].To))
	}
	if !routes[0].Enabled {
		t.Error("route should be enabled")
	}
}

// DLQ replay is offered per queue in the Dead letters view.
func TestConsoleDLQReplay(t *testing.T) {
	_, srv := consoleCreateServer(t)
	consolePost(t, srv, "/queues", map[string]any{"name": "risky", "max_inflight": 1})

	code, body := consolePost(t, srv, "/dlq/risky.dlq/replay-all", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("replay-all: %d %s", code, body)
	}
}
