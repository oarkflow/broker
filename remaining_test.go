package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func remainingTimeoutCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestStreamReadAndCommitOffset(t *testing.T) {
	ctx := context.Background()
	b := MustNew(Config{PollInterval: time.Millisecond})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	if err := b.DeclareStream(ctx, StreamConfig{Name: "events"}); err != nil {
		t.Fatal(err)
	}
	pub := b.Publisher()
	if _, err := pub.Publish(ctx, "events.created", map[string]string{"n": "1"}, WithStream("events")); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, "events.created", map[string]string{"n": "2"}, WithStream("events")); err != nil {
		t.Fatal(err)
	}
	msgs, err := b.ReadStream(ctx, StreamReadRequest{Stream: "events", ConsumerGroup: "g1", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 stream messages, got %d", len(msgs))
	}
	if msgs[0].Offset == 0 || msgs[1].Offset != msgs[0].Offset+1 {
		t.Fatalf("unexpected offsets: %+v", msgs)
	}
	if err := b.CommitOffset(ctx, OffsetCommit{Stream: "events", ConsumerGroup: "g1", Offset: msgs[0].Offset}); err != nil {
		t.Fatal(err)
	}
	msgs, err = b.ReadStream(ctx, StreamReadRequest{Stream: "events", ConsumerGroup: "g1", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Offset != 2 {
		t.Fatalf("expected one message after committed offset, got %+v", msgs)
	}
	off, err := b.ConsumerOffset(ctx, OffsetCommit{Stream: "events", ConsumerGroup: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	if off != 1 {
		t.Fatalf("expected committed offset 1, got %d", off)
	}
}

func TestTaskProgressUsesTaskHeaders(t *testing.T) {
	ctx := context.Background()
	b := MustNew(Config{PollInterval: time.Millisecond})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	_, err := b.RegisterTask(ctx, "progress", func(ctx context.Context, d TaskDelivery) (any, error) {
		if err := d.Progress(ctx, Progress{Current: 1, Total: 2, Percent: 50, Message: "half"}); err != nil {
			return nil, err
		}
		return map[string]string{"ok": "true"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := b.Publisher().SubmitTask(ctx, "progress", map[string]string{"x": "y"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.Wait(remainingTimeoutCtx(t, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if res.Task.Status != TaskSucceeded {
		t.Fatalf("expected success, got %+v", res.Task)
	}
	info, err := h.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Progress.Percent != 50 || info.Progress.Message != "half" {
		t.Fatalf("progress not stored from task headers: %+v", info.Progress)
	}
}

func TestAdminPullAckStreamAndSSEEndpoints(t *testing.T) {
	ctx := context.Background()
	b := MustNew(Config{})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true}.Handler())
	defer srv.Close()
	if _, err := b.Publisher().Publish(ctx, "jobs", []byte(`{"x":1}`), WithQueue("adminq")); err != nil {
		t.Fatal(err)
	}
	pullBody := bytes.NewBufferString(`{"consumer_id":"admin-test","consumer_group":"admin-test","limit":1}`)
	resp, err := http.Post(srv.URL+"/queues/adminq/pull", "application/json", pullBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pull status %d", resp.StatusCode)
	}
	var pulled []Message
	if err := json.NewDecoder(resp.Body).Decode(&pulled); err != nil {
		t.Fatal(err)
	}
	if len(pulled) != 1 || pulled[0].LeaseToken == "" {
		t.Fatalf("bad pull: %+v", pulled)
	}
	ackBody, _ := json.Marshal(AckRequest{Queue: "adminq", LeaseToken: pulled[0].LeaseToken, ConsumerID: "admin-test"})
	resp2, err := http.Post(srv.URL+"/messages/"+pulled[0].ID+"/ack", "application/json", bytes.NewReader(ackBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("ack status %d", resp2.StatusCode)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte(`{"e":1}`), WithStream("admins")); err != nil {
		t.Fatal(err)
	}
	resp3, err := http.Get(srv.URL + "/streams/admins/messages?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("stream status %d", resp3.StatusCode)
	}
	var streamMsgs []Message
	if err := json.NewDecoder(resp3.Body).Decode(&streamMsgs); err != nil {
		t.Fatal(err)
	}
	if len(streamMsgs) != 1 || streamMsgs[0].Offset == 0 {
		t.Fatalf("bad stream read: %+v", streamMsgs)
	}
}

func TestWebhookCallbackRetriesAndFailsOnNon2xx(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 2 {
			http.Error(w, "try again", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	cb := WebhookCallback{URL: srv.URL, MaxAttempts: 3, RetryDelay: time.Millisecond, Timeout: time.Second}
	if err := cb.HandleNotification(context.Background(), Notification{Type: "test", Source: "test"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 callback attempts, got %d", calls)
	}
}

func TestFileWALStreamOffsetRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	b := MustNew(Config{Store: st})
	if err := b.DeclareStream(ctx, StreamConfig{Name: "wal-events"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "wal", []byte(`1`), WithStream("wal-events")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "wal", []byte(`2`), WithStream("wal-events")); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset(ctx, OffsetCommit{Stream: "wal-events", ConsumerGroup: "g", Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	msgs, err := st2.ReadStream(ctx, StreamReadRequest{Stream: "wal-events", ConsumerGroup: "g", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Offset != 2 {
		t.Fatalf("expected recovered stream after offset, got %+v", msgs)
	}
}
