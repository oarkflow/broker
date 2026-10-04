package broker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func managementServer(t *testing.T) (*Broker, *httptest.Server) {
	t.Helper()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	admin := AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = b.Stop(ctx)
	})
	return b, srv
}

func mgmtPost(t *testing.T, srv *httptest.Server, path, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func mgmtGet(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// Broker-wide pause and resume had no HTTP route at all, so an operator holding
// only the API could drain a queue but never the broker.
func TestManagementBrokerPauseResumeOverHTTP(t *testing.T) {
	_, srv := managementServer(t)

	code, body := mgmtGet(t, srv, "/v1/broker")
	if code != http.StatusOK {
		t.Fatalf("get broker state: %d %s", code, body)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got["state"] != "running" {
		t.Fatalf("want running, got %v", got["state"])
	}

	code, body = mgmtPost(t, srv, "/v1/broker", `{"action":"pause","reason":"maintenance"}`)
	if code != http.StatusOK {
		t.Fatalf("pause: %d %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got["state"] != "paused" {
		t.Fatalf("want paused, got %v", got["state"])
	}

	code, body = mgmtGet(t, srv, "/v1/broker")
	if code != http.StatusOK {
		t.Fatalf("get paused state: %d %s", code, body)
	}
	_ = json.Unmarshal([]byte(body), &got)
	if got["state"] != "paused" {
		t.Fatalf("want paused, got %v", got["state"])
	}

	code, body = mgmtPost(t, srv, "/v1/broker", `{"action":"resume"}`)
	if code != http.StatusOK {
		t.Fatalf("resume: %d %s", code, body)
	}
	_ = json.Unmarshal([]byte(body), &got)
	if got["state"] != "running" {
		t.Fatalf("want running, got %v", got["state"])
	}
}

func TestManagementBrokerLifecycleRejectsUnknownAction(t *testing.T) {
	_, srv := managementServer(t)
	code, body := mgmtPost(t, srv, "/v1/broker", `{"action":"explode"}`)
	if code < 400 {
		t.Fatalf("want error status, got %d %s", code, body)
	}
	if !strings.Contains(body, "pause or resume") {
		t.Fatalf("want actionable message, got %s", body)
	}
	// A rejected request must not have changed broker state.
	code, body = mgmtGet(t, srv, "/v1/broker")
	if code != http.StatusOK || !strings.Contains(body, "running") {
		t.Fatalf("broker state changed after bad action: %d %s", code, body)
	}
}

// Topics exposed counts but no retention read, so an operator could not see what
// had been published.
func TestManagementTopicReadOverHTTP(t *testing.T) {
	b, srv := managementServer(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "orders", []byte("m"), nil); err != nil {
			t.Fatal(err)
		}
	}
	code, body := mgmtGet(t, srv, "/v1/topics/orders")
	if code != http.StatusOK {
		t.Fatalf("read topic: %d %s", code, body)
	}
	var msgs []Message
	if err := json.Unmarshal([]byte(body), &msgs); err != nil {
		t.Fatalf("decode: %v body=%s", err, body)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 retained, got %d", len(msgs))
	}
	for i, m := range msgs {
		if string(m.Payload) != "m" {
			t.Fatalf("msg %d payload = %q", i, m.Payload)
		}
	}
}

// The console resumes a topic scroll with a cursor, so paging must not repeat or
// skip messages.
func TestManagementTopicReadCursorPaging(t *testing.T) {
	b, srv := managementServer(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := b.Publisher().Publish(ctx, "t", []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	code, body := mgmtGet(t, srv, "/v1/topics/t?limit=2")
	if code != http.StatusOK {
		t.Fatalf("page 1: %d %s", code, body)
	}
	var page1 []Message
	if err := json.Unmarshal([]byte(body), &page1); err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("page 1 want 2, got %d", len(page1))
	}
	last := page1[1].ID

	code, body = mgmtGet(t, srv, "/v1/topics/t?after="+last+"&limit=2")
	if code != http.StatusOK {
		t.Fatalf("page 2: %d %s", code, body)
	}
	var page2 []Message
	if err := json.Unmarshal([]byte(body), &page2); err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("page 2 want 2, got %d", len(page2))
	}
	for _, m := range page2 {
		if m.ID == last {
			t.Fatalf("cursor message repeated on page 2")
		}
	}
}

func TestManagementTopicReadUnknownTopic(t *testing.T) {
	_, srv := managementServer(t)
	code, body := mgmtGet(t, srv, "/v1/topics/nope")
	if code < 400 {
		t.Fatalf("want error, got %d %s", code, body)
	}
	if !strings.Contains(body, "nope") {
		t.Fatalf("error should name the topic: %s", body)
	}
}

func TestManagementTopicReadRejectsWrite(t *testing.T) {
	_, srv := managementServer(t)
	code, _ := mgmtPost(t, srv, "/v1/topics/orders", `{}`)
	if code < 400 {
		t.Fatalf("want 405-ish, got %d", code)
	}
}

// The console polls these; a stalled broker must not hang the UI forever.
func TestManagementRoutesRespondWhileBrokerPaused(t *testing.T) {
	b, srv := managementServer(t)
	if err := b.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/broker", "/stats", "/queues", "/consumers", "/v1/tasks"} {
		done := make(chan int, 1)
		go func(p string) {
			code, _ := mgmtGet(t, srv, p)
			done <- code
		}(path)
		select {
		case code := <-done:
			if code >= 500 {
				t.Fatalf("%s returned %d while paused", path, code)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s hung while broker paused", path)
		}
	}
}
