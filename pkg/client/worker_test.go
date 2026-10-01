package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

// ---------------------------------------------------------------------
// Pure helper functions.
// ---------------------------------------------------------------------

func TestSanitize(t *testing.T) {
	tests := map[string]string{
		"a.b":        "a.b",
		"tenant.*":   "tenant.star",
		"tenant.#":   "tenant.all",
		"a/b c:d":    "a_b_c_d",
		"plain":      "plain",
		"multi * # ": "multi_star_all_",
	}
	for in, want := range tests {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", "", "x", "y"); got != "x" {
		t.Fatalf("got %q", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
	if got := firstNonEmpty("first", "second"); got != "first" {
		t.Fatalf("got %q, want first", got)
	}
}

func TestMaxInt(t *testing.T) {
	if maxInt(3, 5) != 5 {
		t.Fatal("expected 5")
	}
	if maxInt(5, 3) != 5 {
		t.Fatal("expected 5")
	}
	if maxInt(4, 4) != 4 {
		t.Fatal("expected 4")
	}
}

func TestAsString(t *testing.T) {
	if asString("hello") != "hello" {
		t.Fatal("expected hello")
	}
	if asString(42) != "" {
		t.Fatal("expected empty string for non-string value")
	}
	if asString(nil) != "" {
		t.Fatal("expected empty string for nil")
	}
}

// ---------------------------------------------------------------------
// Worker registration / handler-lookup logic (no network).
// ---------------------------------------------------------------------

func newTestWorker(t *testing.T) *Worker {
	t.Helper()
	w, err := NewWorker(Config{BrokerURL: "http://127.0.0.1:0", ConsumerID: "w1", Group: "g1", Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.pool.Shutdown(context.Background()) })
	return w
}

func noopHandler(context.Context, Delivery) error               { return nil }
func noopRequestHandler(context.Context, Delivery) (any, error) { return nil, nil }
func noopTaskHandler(context.Context, Delivery) (any, error)    { return nil, nil }

func TestWorkerHandleRegistrationAndSnapshots(t *testing.T) {
	w := newTestWorker(t)
	w.HandleQueue("orders", noopHandler)
	w.HandleTopic("events.created", noopHandler)
	w.HandlePattern("events.*", noopHandler)
	w.HandleTask("resize", noopTaskHandler)
	w.HandleTaskQueue("custom-task", "tasks.custom", noopTaskHandler)
	w.HandleRequest("lookup", noopRequestHandler)
	w.HandleRequestQueue("custom-req", "request.custom", noopRequestHandler)
	w.HandleCommand("do-thing", noopRequestHandler)
	w.HandleCommandQueue("custom-cmd", "command.custom", noopRequestHandler)

	queues, topics, patterns, tasks, requests, commands, taskQueues, requestQueues, commandQueues := w.snapshotNames()
	assertContains(t, queues, "orders")
	assertContains(t, topics, "events.created")
	assertContains(t, patterns, "events.*")
	assertContains(t, tasks, "resize")
	assertContains(t, tasks, "custom-task")
	assertContains(t, requests, "lookup")
	assertContains(t, requests, "custom-req")
	assertContains(t, commands, "do-thing")
	assertContains(t, commands, "custom-cmd")

	if taskQueues["resize"] != "tasks.resize" {
		t.Fatalf("expected default task queue naming, got %q", taskQueues["resize"])
	}
	if taskQueues["custom-task"] != "tasks.custom" {
		t.Fatalf("expected custom task queue, got %q", taskQueues["custom-task"])
	}
	if requestQueues["lookup"] != "request.lookup" {
		t.Fatalf("expected default request queue naming, got %q", requestQueues["lookup"])
	}
	if requestQueues["custom-req"] != "request.custom" {
		t.Fatalf("expected custom request queue, got %q", requestQueues["custom-req"])
	}
	if commandQueues["do-thing"] != "command.do-thing" {
		t.Fatalf("expected default command queue naming, got %q", commandQueues["do-thing"])
	}
	if commandQueues["custom-cmd"] != "command.custom" {
		t.Fatalf("expected custom command queue, got %q", commandQueues["custom-cmd"])
	}

	pollQueues := w.snapshotPollQueues()
	for _, q := range []string{"orders", "sub.w1.events.created", "sub.w1.events.star", "tasks.resize", "tasks.custom", "request.lookup", "request.custom", "command.do-thing", "command.custom"} {
		if _, ok := pollQueues[q]; !ok {
			t.Errorf("expected poll queue %q to be registered, have %v", q, pollQueues)
		}
	}
}

func assertContains(t *testing.T, haystack []string, needle string) {
	t.Helper()
	for _, v := range haystack {
		if v == needle {
			return
		}
	}
	t.Errorf("expected %v to contain %q", haystack, needle)
}

func TestWorkerFindHandler(t *testing.T) {
	w := newTestWorker(t)
	w.HandleQueue("q1", noopHandler)
	w.HandleTopic("t1", noopHandler)
	w.HandlePattern("p.*", noopHandler)
	w.HandleTask("task1", noopTaskHandler)
	w.HandleRequest("req1", noopRequestHandler)
	w.HandleCommand("cmd1", noopRequestHandler)

	tests := []struct {
		name     string
		msg      broker.Message
		wantKind string
		wantNil  bool
	}{
		{"task by task name", broker.Message{TaskName: "task1"}, "task", false},
		{"task by type", broker.Message{Type: "task1"}, "task", false},
		{"command by topic with correlation", broker.Message{CorrelationID: "c1", Topic: "cmd1"}, "request", false},
		{"request by topic with correlation", broker.Message{CorrelationID: "c1", Topic: "req1"}, "request", false},
		{"request by type with correlation", broker.Message{CorrelationID: "c1", Type: "req1"}, "request", false},
		{"queue handler", broker.Message{Queue: "q1"}, "handler", false},
		{"topic handler", broker.Message{Topic: "t1"}, "handler", false},
		{"pattern handler", broker.Message{Topic: "p.anything"}, "handler", false},
		{"pattern handler via routing key", broker.Message{RoutingKey: "p.routed"}, "handler", false},
		{"no handler", broker.Message{Queue: "unknown", Topic: "unknown"}, "", true},
		{"correlation without matching command/request falls through to queue", broker.Message{CorrelationID: "c1", Queue: "q1"}, "handler", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, kind := w.findHandler(tt.msg)
			if tt.wantNil {
				if h != nil {
					t.Fatalf("expected nil handler, got kind=%s", kind)
				}
				return
			}
			if h == nil {
				t.Fatalf("expected a handler for %+v", tt.msg)
			}
			if kind != tt.wantKind {
				t.Fatalf("got kind %q, want %q", kind, tt.wantKind)
			}
		})
	}
}

func TestWorkerTrackAndCancelMessage(t *testing.T) {
	w := newTestWorker(t)
	var cancelled int32
	w.trackMessage("m1", func() { atomic.AddInt32(&cancelled, 1) })
	w.cancelMessage("m1")
	if atomic.LoadInt32(&cancelled) != 1 {
		t.Fatal("expected cancel func to be invoked")
	}
	// Cancelling an unknown id or empty id must be a no-op, not a panic.
	w.cancelMessage("unknown")
	w.cancelMessage("")
	w.trackMessage("", func() { t.Fatal("should never be tracked") })

	w.trackMessage("m2", func() {})
	w.untrackMessage("m2")
	w.cancelMessage("m2") // already untracked: no-op
}

// ---------------------------------------------------------------------
// End-to-end tests driving Worker.Run against a real broker.
// ---------------------------------------------------------------------

func TestWorkerEndToEndQueueHandlerAcks(t *testing.T) {
	_, url := newTestBrokerServer(t, "jobs")
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "worker-e2e", Group: "worker-e2e", Concurrency: 2, HeartbeatInterval: 20 * time.Millisecond, PollWait: 10 * time.Millisecond, VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	w.HandleQueue("jobs", func(ctx context.Context, d Delivery) error {
		var payload map[string]string
		if err := d.Decode(&payload); err != nil {
			return err
		}
		got <- payload["job"]
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// Give the worker a brief moment to register/subscribe before publishing.
	time.Sleep(30 * time.Millisecond)
	if _, err := w.Client().Publish(ctx, "jobs", map[string]string{"job": "hello"}, WithQueue("jobs")); err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-got:
		if v != "hello" {
			t.Fatalf("unexpected payload: %s", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler invocation")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected Run error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for worker to stop")
	}
}

func TestWorkerEndToEndTaskHandlerCompletes(t *testing.T) {
	_, url := newTestBrokerServer(t)
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "task-worker", Group: "task-worker", Concurrency: 2, HeartbeatInterval: 20 * time.Millisecond, PollWait: 10 * time.Millisecond, VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	w.HandleTask("resize", func(ctx context.Context, d Delivery) (any, error) {
		defer close(done)
		return map[string]string{"status": "resized"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)

	info, err := w.Client().SubmitTask(ctx, "resize", map[string]string{"path": "a.png"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for task handler")
	}

	var result broker.TaskInfo
	for i := 0; i < 50; i++ {
		result, _, err = w.Client().TaskResult(ctx, info.ID)
		if err == nil && result.Status == broker.TaskSucceeded {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if result.Status != broker.TaskSucceeded {
		t.Fatalf("expected task to succeed, got status=%s err=%v", result.Status, err)
	}

	cancel()
	<-runDone
}

func TestWorkerEndToEndRequestHandlerResponds(t *testing.T) {
	_, url := newTestBrokerServer(t)
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "req-worker", Group: "req-worker", Concurrency: 2, HeartbeatInterval: 20 * time.Millisecond, PollWait: 10 * time.Millisecond, VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleRequest("lookup.user", func(ctx context.Context, d Delivery) (any, error) {
		var in map[string]string
		if err := d.Decode(&in); err != nil {
			return nil, err
		}
		return map[string]string{"name": "user-" + in["id"]}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)

	caller := New(Config{BrokerURL: url, ConsumerID: "caller"})
	var out map[string]string
	if err := caller.Request(ctx, "lookup.user", map[string]string{"id": "7"}, &out, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if out["name"] != "user-7" {
		t.Fatalf("unexpected response: %+v", out)
	}

	cancel()
	<-runDone
}

func TestWorkerHandlerPanicIsRecoveredAndNacked(t *testing.T) {
	_, url := newTestBrokerServer(t, "panicq")
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "panic-worker", Group: "panic-worker", Concurrency: 1, HeartbeatInterval: 20 * time.Millisecond, PollWait: 10 * time.Millisecond, VisibilityTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var calls int32
	var mu sync.Mutex
	var handlerErrs []error
	w.client.cfg.Hooks.OnHandlerError = func(e MessageEvent) {
		mu.Lock()
		handlerErrs = append(handlerErrs, e.Error)
		mu.Unlock()
	}
	w.HandleQueue("panicq", func(ctx context.Context, d Delivery) error {
		atomic.AddInt32(&calls, 1)
		panic("boom")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)

	if _, err := w.Client().Publish(ctx, "panicq", map[string]string{"x": "1"}, WithQueue("panicq")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(handlerErrs)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	n := len(handlerErrs)
	mu.Unlock()
	if n == 0 {
		t.Fatal("expected panic to be recovered and reported via OnHandlerError")
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("expected handler to have been invoked")
	}

	cancel()
	<-runDone
}

func TestWorkerNackOrDLQRejectsAfterMaxAttempts(t *testing.T) {
	_, url := newTestBrokerServer(t, "dlqtest")
	c := New(Config{BrokerURL: url, ConsumerID: "dlq-worker"})
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "dlq-worker2"})
	if err != nil {
		t.Fatal(err)
	}
	w.client = c
	ctx := context.Background()

	if _, err := c.Publish(ctx, "dlqtest", map[string]string{"x": "1"}, WithQueue("dlqtest")); err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Poll(ctx, "dlqtest", 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("poll failed: %v (%d msgs)", err, len(msgs))
	}
	m := msgs[0]
	m.Attempts = 2
	m.MaxAttempts = 3
	d := Delivery{Message: m, client: c}

	if err := w.nackOrDLQ(ctx, d, errors.New("boom")); err != nil {
		t.Fatal(err)
	}

	dlqMsgs, err := c.DLQ(ctx, "dlqtest.dlq", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected message rejected to dlq at max attempts, got %d", len(dlqMsgs))
	}
}

func TestWorkerNackOrDLQRetriesBelowMaxAttempts(t *testing.T) {
	_, url := newTestBrokerServer(t, "retrytest")
	c := New(Config{BrokerURL: url, ConsumerID: "retry-worker"})
	w, err := NewWorker(Config{BrokerURL: url, ConsumerID: "retry-worker2"})
	if err != nil {
		t.Fatal(err)
	}
	w.client = c
	ctx := context.Background()

	if _, err := c.Publish(ctx, "retrytest", map[string]string{"x": "1"}, WithQueue("retrytest")); err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Poll(ctx, "retrytest", 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("poll failed: %v (%d msgs)", err, len(msgs))
	}
	m := msgs[0]
	m.Attempts = 0
	m.MaxAttempts = 3
	d := Delivery{Message: m, client: c}

	if err := w.nackOrDLQ(ctx, d, errors.New("transient")); err != nil {
		t.Fatal(err)
	}

	dlqMsgs, err := c.DLQ(ctx, "retrytest.dlq", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlqMsgs) != 0 {
		t.Fatalf("expected message not yet in dlq, got %d", len(dlqMsgs))
	}
}
