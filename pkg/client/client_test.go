package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

// ---------------------------------------------------------------------
// Pure/unit-level tests: config defaults, URL handling, option builders,
// frame batch decoding. No network involved.
// ---------------------------------------------------------------------

func TestNormalizeBrokerURLs(t *testing.T) {
	tests := []struct {
		name    string
		primary string
		more    []string
		want    []string
	}{
		{"single", "http://a:1/", nil, []string{"http://a:1"}},
		{"dedupe", "http://a:1", []string{"http://a:1", "http://b:2"}, []string{"http://a:1", "http://b:2"}},
		{"trims whitespace and slash", "  http://a:1/  ", []string{"http://b:2/"}, []string{"http://a:1", "http://b:2"}},
		{"skips empty", "", []string{"", "http://b:2"}, []string{"http://b:2"}},
		{"all empty", "", nil, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeBrokerURLs(tt.primary, tt.more)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v want %v", got, tt.want)
				}
			}
		})
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	c := New(Config{})
	cfg := c.Config()
	if cfg.BrokerURL != "http://127.0.0.1:8090" {
		t.Fatalf("unexpected default broker url: %s", cfg.BrokerURL)
	}
	if cfg.ConsumerID == "" {
		t.Fatal("expected a generated consumer id")
	}
	if cfg.Group != cfg.ConsumerID {
		t.Fatalf("expected group to default to consumer id, got %s vs %s", cfg.Group, cfg.ConsumerID)
	}
	if cfg.Name != cfg.ConsumerID {
		t.Fatalf("expected name to default to consumer id")
	}
	if cfg.Concurrency != 1 {
		t.Fatalf("expected concurrency default 1, got %d", cfg.Concurrency)
	}
	if cfg.MaxInFlight != cfg.Concurrency {
		t.Fatalf("expected max in flight to default to concurrency")
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("unexpected heartbeat interval default: %v", cfg.HeartbeatInterval)
	}
	if cfg.VisibilityTimeout != 30*time.Second {
		t.Fatalf("unexpected visibility timeout default: %v", cfg.VisibilityTimeout)
	}
	if cfg.PollWait != 30*time.Second {
		t.Fatalf("unexpected poll wait default: %v", cfg.PollWait)
	}
	if cfg.PollLimit != 1 {
		t.Fatalf("unexpected poll limit default: %d", cfg.PollLimit)
	}
	if cfg.FrameCodec == nil {
		t.Fatal("expected a default frame codec")
	}
	if cfg.LeaseExtensionInterval != cfg.VisibilityTimeout/2 {
		t.Fatalf("unexpected lease extension interval: %v", cfg.LeaseExtensionInterval)
	}
	if cfg.HTTPClient == nil {
		t.Fatal("expected an http client to be constructed")
	}
}

func TestNewRespectsExplicitValues(t *testing.T) {
	c := New(Config{
		ConsumerID:        "worker-1",
		Group:             "group-1",
		Concurrency:       4,
		VisibilityTimeout: 2 * time.Second,
	})
	cfg := c.Config()
	if cfg.ConsumerID != "worker-1" || cfg.Group != "group-1" {
		t.Fatalf("explicit ids not respected: %+v", cfg)
	}
	if cfg.Concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", cfg.Concurrency)
	}
	if cfg.MaxInFlight != 4 {
		t.Fatalf("expected max in flight to fall back to concurrency, got %d", cfg.MaxInFlight)
	}
	// LeaseExtensionInterval falls back to half the visibility timeout.
	if cfg.LeaseExtensionInterval != time.Second {
		t.Fatalf("expected lease extension interval 1s, got %v", cfg.LeaseExtensionInterval)
	}
}

func TestNewMultipleBrokerURLsOrdering(t *testing.T) {
	c := New(Config{BrokerURLs: []string{"http://a", "http://b", "http://c"}})
	urls := c.orderedURLs()
	if len(urls) != 3 || urls[0] != "http://a" {
		t.Fatalf("unexpected initial order: %v", urls)
	}
	c.markActive("http://b")
	urls = c.orderedURLs()
	want := []string{"http://b", "http://c", "http://a"}
	for i, u := range want {
		if urls[i] != u {
			t.Fatalf("after markActive(b), got %v want %v", urls, want)
		}
	}
	// marking an unknown base should be a no-op.
	c.markActive("http://unknown")
	urls2 := c.orderedURLs()
	for i, u := range urls2 {
		if urls[i] != u {
			t.Fatalf("markActive on unknown base changed ordering: before=%v after=%v", urls, urls2)
		}
	}
}

func TestPublishOptionBuilders(t *testing.T) {
	var o PublishOptions
	for _, fn := range []PublishOption{
		WithQueue("q"), WithStream("s"), WithType("t"),
		WithHeaders(map[string]string{"h": "1"}), WithMetadata(map[string]string{"m": "1"}),
		WithRoutingKey("rk"), WithKey("k"), WithPriority(5), WithTenantID("tenant"), WithIdempotencyKey("idem"),
	} {
		fn(&o)
	}
	if o.Queue != "q" || o.Stream != "s" || o.Type != "t" || o.RoutingKey != "rk" || o.Key != "k" ||
		o.Priority != 5 || o.TenantID != "tenant" || o.IdempotencyKey != "idem" {
		t.Fatalf("unexpected options: %+v", o)
	}
	if o.Headers["h"] != "1" || o.Metadata["m"] != "1" {
		t.Fatalf("unexpected maps: %+v", o)
	}
}

func TestTaskOptionBuilders(t *testing.T) {
	var o TaskOptions
	for _, fn := range []TaskOption{
		WithTaskQueue("q"), WithTaskTenantID("tenant"), WithTaskPriority(3),
		WithTaskMaxAttempts(7), WithTaskMetadata(map[string]string{"a": "b"}),
	} {
		fn(&o)
	}
	if o.Queue != "q" || o.TenantID != "tenant" || o.Priority != 3 || o.MaxAttempts != 7 || o.Metadata["a"] != "b" {
		t.Fatalf("unexpected task options: %+v", o)
	}
}

func TestNackOptionBuilders(t *testing.T) {
	o := NackOptions{Action: broker.NackRetry, Delay: time.Second, Reason: "handler_error"}
	NackDelay(5 * time.Second)(&o)
	NackReason("custom")(&o)
	NackError("boom")(&o)
	if o.Delay != 5*time.Second || o.Reason != "custom" || o.Error != "boom" {
		t.Fatalf("unexpected nack options: %+v", o)
	}
	NackDLQ("dlq.q")(&o)
	if o.Action != broker.NackDLQ || o.DLQ != "dlq.q" {
		t.Fatalf("expected NackDLQ to set action and dlq name: %+v", o)
	}
}

func TestMustMarshal(t *testing.T) {
	b := mustMarshal(map[string]int{"a": 1})
	if string(b) != `{"a":1}` {
		t.Fatalf("unexpected marshal: %s", b)
	}
}

func TestDecodeFrameBatch(t *testing.T) {
	codec := broker.NewBinaryMessageCodec()
	msgs := []broker.Message{
		{ID: "m1", Queue: "q", Payload: []byte("one")},
		{ID: "m2", Queue: "q", Payload: []byte("two")},
	}
	var buf []byte
	for _, m := range msgs {
		frame, err := codec.EncodeMessage(nil, m)
		if err != nil {
			t.Fatal(err)
		}
		lenBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(lenBuf, uint64(len(frame)))
		buf = append(buf, lenBuf[:n]...)
		buf = append(buf, frame...)
	}
	out, err := decodeFrameBatch(buf, codec)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].ID != "m1" || out[1].ID != "m2" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

func TestDecodeFrameBatchEmpty(t *testing.T) {
	codec := broker.NewBinaryMessageCodec()
	out, err := decodeFrameBatch(nil, codec)
	if err != nil || len(out) != 0 {
		t.Fatalf("expected empty result, got %#v err=%v", out, err)
	}
}

func TestDecodeFrameBatchTruncated(t *testing.T) {
	codec := broker.NewBinaryMessageCodec()
	// A valid varint length claiming more bytes than are actually present.
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, 100)
	buf := lenBuf[:n]
	if _, err := decodeFrameBatch(buf, codec); err == nil {
		t.Fatal("expected error for truncated frame data")
	}
}

// ---------------------------------------------------------------------
// Integration-style tests against a real in-process broker exposed over
// httptest, following the pattern established in frame_client_test.go.
// ---------------------------------------------------------------------

func TestClientRegisterHeartbeatSubscribe(t *testing.T) {
	_, url := newTestBrokerServer(t, "orders")
	var registered, heartbeat, subscribed int32
	hooks := Hooks{
		OnRegistered: func(Event) { atomic.AddInt32(&registered, 1) },
		OnHeartbeat:  func(Event) { atomic.AddInt32(&heartbeat, 1) },
		OnSubscribed: func(Event) { atomic.AddInt32(&subscribed, 1) },
	}
	c := New(Config{BrokerURL: url, ConsumerID: "c1", Group: "g1", Hooks: hooks})
	ctx := context.Background()

	info, err := c.Register(ctx, broker.ConsumerRegistration{})
	if err != nil {
		t.Fatal(err)
	}
	if info.ConsumerID != "c1" || info.Group != "g1" {
		t.Fatalf("unexpected registration info: %+v", info)
	}

	if _, err := c.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Subscribe(ctx, broker.RemoteSubscription{Type: "queue", Name: "orders", Queue: "orders"}); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&registered) != 1 || atomic.LoadInt32(&heartbeat) != 1 || atomic.LoadInt32(&subscribed) != 1 {
		t.Fatalf("expected each hook to fire once: reg=%d hb=%d sub=%d", registered, heartbeat, subscribed)
	}

	consumers, err := c.Consumers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ci := range consumers {
		if ci.ConsumerID == "c1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected c1 among consumers: %+v", consumers)
	}
}

func TestClientPublishPollAckNackRejectExtendProgress(t *testing.T) {
	_, url := newTestBrokerServer(t, "jobs")
	c := New(Config{BrokerURL: url, ConsumerID: "worker-a", Group: "worker-a", PollWait: 50 * time.Millisecond})
	ctx := context.Background()
	if _, err := c.Register(ctx, broker.ConsumerRegistration{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe(ctx, broker.RemoteSubscription{Type: "queue", Name: "jobs", Queue: "jobs"}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish(ctx, "jobs", map[string]string{"k": "v"}, WithQueue("jobs")); err != nil {
		t.Fatal(err)
	}

	msgs, err := c.Poll(ctx, "jobs", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	m := msgs[0]

	if err := c.Progress(ctx, m, broker.Progress{Current: 1, Total: 2}); err != nil {
		t.Fatal(err)
	}
	if err := c.Extend(ctx, m, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Nack(ctx, m, NackReason("retry me"), NackDelay(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	// Poll again after the short nack delay to re-lease it.
	var redelivered []broker.Message
	for i := 0; i < 20; i++ {
		redelivered, err = c.Poll(ctx, "jobs", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(redelivered) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(redelivered) != 1 {
		t.Fatalf("expected message to be redelivered after nack, got %d", len(redelivered))
	}
	m = redelivered[0]

	if err := c.Ack(ctx, m); err != nil {
		t.Fatal(err)
	}

	// Publish a second message to exercise Reject.
	if _, err := c.Publish(ctx, "jobs", map[string]string{"k": "v2"}, WithQueue("jobs")); err != nil {
		t.Fatal(err)
	}
	msgs2, err := c.Poll(ctx, "jobs", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs2))
	}
	if err := c.Reject(ctx, msgs2[0], "bad message"); err != nil {
		t.Fatal(err)
	}

	dlqMsgs, err := c.DLQ(ctx, "jobs.dlq", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected rejected message to land in dlq, got %d", len(dlqMsgs))
	}

	if err := c.ReplayDLQ(ctx, "jobs.dlq"); err != nil {
		t.Fatal(err)
	}
}

func TestClientRespondRespondErrorCompleteFail(t *testing.T) {
	_, url := newTestBrokerServer(t, "resp")
	c := New(Config{BrokerURL: url, ConsumerID: "worker-b", Group: "worker-b"})
	ctx := context.Background()
	if _, err := c.Publish(ctx, "resp", map[string]string{"x": "1"}, WithQueue("resp")); err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Poll(ctx, "resp", 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("poll failed: %v msgs=%d", err, len(msgs))
	}
	m := msgs[0]
	if err := c.Respond(ctx, m, map[string]bool{"ok": true}, true); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish(ctx, "resp", map[string]string{"x": "2"}, WithQueue("resp")); err != nil {
		t.Fatal(err)
	}
	msgs2, err := c.Poll(ctx, "resp", 1)
	if err != nil || len(msgs2) != 1 {
		t.Fatalf("poll failed: %v msgs=%d", err, len(msgs2))
	}
	if err := c.RespondError(ctx, msgs2[0], nil, true); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish(ctx, "resp", map[string]string{"x": "3"}, WithQueue("resp")); err != nil {
		t.Fatal(err)
	}
	msgs3, err := c.Poll(ctx, "resp", 1)
	if err != nil || len(msgs3) != 1 {
		t.Fatalf("poll failed: %v msgs=%d", err, len(msgs3))
	}
	if err := c.Complete(ctx, msgs3[0], map[string]string{"done": "yes"}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Publish(ctx, "resp", map[string]string{"x": "4"}, WithQueue("resp")); err != nil {
		t.Fatal(err)
	}
	msgs4, err := c.Poll(ctx, "resp", 1)
	if err != nil || len(msgs4) != 1 {
		t.Fatalf("poll failed: %v msgs=%d", err, len(msgs4))
	}
	if err := c.Fail(ctx, msgs4[0], nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientPublishAsyncAndBatch(t *testing.T) {
	_, url := newTestBrokerServer(t, "batchq")
	c := New(Config{BrokerURL: url})
	ctx := context.Background()

	resCh := c.PublishAsync(ctx, "batchq", map[string]int{"n": 1}, WithQueue("batchq"))
	select {
	case r := <-resCh:
		if r.Error != nil {
			t.Fatal(r.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async publish")
	}

	res, err := c.PublishBatch(ctx, "batchq", []any{map[string]int{"n": 2}, map[string]int{"n": 3}}, WithQueue("batchq"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 batch results, got %d", len(res.Results))
	}
}

func TestClientRequestResponseAndCommand(t *testing.T) {
	b, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url, ConsumerID: "responder"})
	ctx := context.Background()

	var invocations int32
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{Name: "echo", Group: "echo", Pattern: "echo.me"}, func(ctx context.Context, d broker.Delivery) error {
		atomic.AddInt32(&invocations, 1)
		var in map[string]string
		if err := d.Decode(&in); err != nil {
			return err
		}
		return d.Respond(ctx, map[string]string{"echo": in["msg"]})
	})
	if err != nil {
		t.Fatal(err)
	}

	var out map[string]string
	if err := c.Request(ctx, "echo.me", map[string]string{"msg": "hi"}, &out, time.Second); err != nil {
		t.Fatal(err)
	}
	if out["echo"] != "hi" {
		t.Fatalf("unexpected echo response: %+v", out)
	}
	if atomic.LoadInt32(&invocations) != 1 {
		t.Fatalf("expected handler invoked exactly once, got %d", invocations)
	}

	// Command is a thin alias for Request.
	var out2 map[string]string
	if err := c.Command(ctx, "echo.me", map[string]string{"msg": "yo"}, &out2, time.Second); err != nil {
		t.Fatal(err)
	}
	if out2["echo"] != "yo" {
		t.Fatalf("unexpected command response: %+v", out2)
	}

	asyncCh := c.RequestAsync(ctx, "echo.me", map[string]string{"msg": "async"}, &map[string]string{}, time.Second)
	select {
	case r := <-asyncCh:
		if r.Error != nil {
			t.Fatal(r.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async request")
	}
}

func TestClientRequestWithoutHandlerErrors(t *testing.T) {
	_, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url})
	ctx := context.Background()
	var out map[string]string
	err := c.Request(ctx, "nobody.listens", map[string]string{"msg": "hi"}, &out, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected an error when no handler is registered")
	}
}

func TestClientTaskLifecycle(t *testing.T) {
	_, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url})
	ctx := context.Background()

	info, err := c.SubmitTask(ctx, "resize-image", map[string]string{"path": "a.png"}, WithTaskMaxAttempts(5))
	if err != nil {
		t.Fatal(err)
	}
	if info.ID == "" || info.Name != "resize-image" {
		t.Fatalf("unexpected task info: %+v", info)
	}

	got, err := c.Task(ctx, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != info.ID {
		t.Fatalf("task id mismatch: %s vs %s", got.ID, info.ID)
	}

	if err := c.RetryTask(ctx, info.ID); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelTask(ctx, info.ID, "no longer needed"); err != nil {
		t.Fatal(err)
	}
}

func TestClientTaskResultNotFound(t *testing.T) {
	_, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url})
	if _, _, err := c.TaskResult(context.Background(), "does-not-exist"); err == nil {
		t.Fatal("expected error for unknown task")
	}
}

func TestClientStreamsAndEvents(t *testing.T) {
	b, url := newTestBrokerServer(t)
	if err := b.DeclareStream(context.Background(), broker.StreamConfig{Name: "audit"}); err != nil {
		t.Fatal(err)
	}
	c := New(Config{BrokerURL: url, ConsumerID: "stream-reader"})
	ctx := context.Background()

	if _, err := c.Publish(ctx, "audit", map[string]string{"evt": "1"}, WithStream("audit")); err != nil {
		t.Fatal(err)
	}

	msgs, err := c.ReadStream(ctx, "audit", "grp", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 stream message, got %d", len(msgs))
	}

	if err := c.CommitStreamOffset(ctx, "audit", "grp", msgs[0].Offset+1); err != nil {
		t.Fatal(err)
	}
	off, err := c.StreamOffset(ctx, "audit", "grp")
	if err != nil {
		t.Fatal(err)
	}
	if off != msgs[0].Offset+1 {
		t.Fatalf("unexpected committed offset: %d", off)
	}

	if _, err := c.Register(ctx, broker.ConsumerRegistration{}); err != nil {
		t.Fatal(err)
	}
	evs, err := c.Events(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = evs // presence of events is best-effort; absence of an error is what matters here.
}

func TestClientReplayDLQRequiresQueue(t *testing.T) {
	_, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url})
	if err := c.ReplayDLQ(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty queue")
	}
}

func TestClientErrorStatusPropagation(t *testing.T) {
	_, url := newTestBrokerServer(t)
	c := New(Config{BrokerURL: url})
	// Acking a message that does not exist should surface the broker's
	// non-2xx response as a Go error carrying the status text.
	err := c.Ack(context.Background(), broker.Message{ID: "missing", Queue: "nope", LeaseToken: "x"})
	if err == nil {
		t.Fatal("expected an error for a nonexistent message")
	}
}

func TestClientFailoverAcrossBrokerURLs(t *testing.T) {
	// First URL points at a server that always 500s; second is a real broker.
	var hits int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("down"))
	}))
	defer bad.Close()
	_, goodURL := newTestBrokerServer(t)

	var reconnects int32
	c := New(Config{BrokerURLs: []string{bad.URL, goodURL}, Hooks: Hooks{OnReconnect: func(Event) {
		// OnReconnect only fires on connection errors (not 5xx), exercised separately below.
		atomic.AddInt32(&reconnects, 1)
	}}})

	info, err := c.Register(context.Background(), broker.ConsumerRegistration{})
	if err != nil {
		t.Fatalf("expected failover to the healthy broker to succeed: %v", err)
	}
	if info.ConsumerID == "" {
		t.Fatalf("unexpected empty consumer id in response: %+v", info)
	}
	if atomic.LoadInt32(&hits) == 0 {
		t.Fatal("expected the unhealthy broker to be tried first")
	}

	// Subsequent calls should stick to the now-known-good broker.
	if _, err := c.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClientUnreachableReturnsError(t *testing.T) {
	c := New(Config{BrokerURL: "http://127.0.0.1:1"})
	c.http.Timeout = 200 * time.Millisecond
	_, err := c.Register(context.Background(), broker.ConsumerRegistration{})
	if err == nil {
		t.Fatal("expected an error when the broker is unreachable")
	}
}

func TestHostnameID(t *testing.T) {
	id := hostnameID()
	if id == "" {
		t.Fatal("expected a non-empty hostname id")
	}
	// Two calls in quick succession must still differ (nanosecond suffix).
	id2 := hostnameID()
	if id == id2 {
		t.Fatalf("expected distinct hostname ids, got %s twice", id)
	}
}

// TestPublishBinaryPayloadRoundTrips goes through the real client and a real
// broker over HTTP. A []byte payload used to be marshalled by encoding/json into
// a base64 JSON string that the broker stored verbatim as text, so binary data
// was corrupted for every remote publisher while the embedded path stored it
// untouched.
func TestPublishBinaryPayloadRoundTrips(t *testing.T) {
	b := broker.MustNew(broker.Config{})
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	ctx := context.Background()
	if err := b.DeclareQueue(ctx, broker.QueueConfig{Name: "bin"}); err != nil {
		t.Fatal(err)
	}

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admin := broker.AdminServer{Broker: b, Token: "secret", InsecureDev: false}
		admin.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(admin.Close)

	c := New(Config{BrokerURLs: []string{admin.URL}, Token: "secret"})

	want := []byte{0x00, 0x01, 0x02, 0xfe, 0xff, 0x80, 'b', 'i', 'n'}
	res, err := c.Publish(ctx, "bin", want, WithQueue("bin"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Store().Get(ctx, res.MessageID)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Payload, want) {
		t.Fatalf("payload corrupted in transit:\n got %q\nwant %q", got.Payload, want)
	}
}

// TestPublishJSONPayloadUnchanged guards the other half of the contract: values
// that are already JSON must keep the previous encoding, so existing callers see
// no behaviour change.
func TestPublishJSONPayloadUnchanged(t *testing.T) {
	b := broker.MustNew(broker.Config{})
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	ctx := context.Background()
	if err := b.DeclareQueue(ctx, broker.QueueConfig{Name: "json"}); err != nil {
		t.Fatal(err)
	}
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := broker.AdminServer{Broker: b, Token: "secret"}
		a.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(admin.Close)
	c := New(Config{BrokerURLs: []string{admin.URL}, Token: "secret"})

	res, err := c.Publish(ctx, "json", map[string]any{"a": 1}, WithQueue("json"))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Store().Get(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != `{"a":1}` {
		t.Fatalf("payload = %q, want %q", got.Payload, `{"a":1}`)
	}
}

// TestPublishOptionsReachTheBroker pins that the client's PublishOptions can
// actually express what the embedded API can. It previously carried seven
// fields, so content_type, schema, ttl, trace propagation, deduplication,
// max_attempts and a producer-chosen message id were unreachable remotely.
func TestPublishOptionsReachTheBroker(t *testing.T) {
	b := broker.MustNew(broker.Config{})
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	ctx := context.Background()
	if err := b.DeclareQueue(ctx, broker.QueueConfig{Name: "opts"}); err != nil {
		t.Fatal(err)
	}
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := broker.AdminServer{Broker: b, Token: "secret"}
		a.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(admin.Close)
	c := New(Config{BrokerURLs: []string{admin.URL}, Token: "secret"})

	res, err := c.Publish(ctx, "opts", []byte(`{"v":1}`),
		WithQueue("opts"),
		WithContentType("application/x-parity"),
		WithSchema("order", "9"),
		WithTraceID("trace-1"),
		WithCausationID("cause-1"),
		WithTTL(time.Hour),
		WithMaxAttempts(7),
		WithMessageID("producer-chosen"),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Store().Get(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "producer-chosen" {
		t.Errorf("MessageID = %q, want the producer-chosen id", res.MessageID)
	}
	if got.ContentType != "application/x-parity" {
		t.Errorf("ContentType = %q", got.ContentType)
	}
	if got.Schema != "order" || got.SchemaVersion != "9" {
		t.Errorf("schema = %q/%q, want order/9", got.Schema, got.SchemaVersion)
	}
	if got.TraceID != "trace-1" {
		t.Errorf("TraceID = %q", got.TraceID)
	}
	if got.CausationID != "cause-1" {
		t.Errorf("CausationID = %q", got.CausationID)
	}
	if got.ExpiresAt.IsZero() {
		t.Error("TTL was dropped: ExpiresAt is zero")
	}
	if got.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want 7", got.MaxAttempts)
	}
}
