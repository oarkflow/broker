package broker_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/broker"
	bclient "github.com/oarkflow/broker/pkg/client"
)

func remoteTestBroker(t *testing.T) (*broker.Broker, *httptest.Server) {
	t.Helper()
	b, err := broker.New(broker.Config{Mode: broker.ModeStandalone, DefaultVisibility: 200 * time.Millisecond, DefaultRetry: broker.RetryPolicy{MaxAttempts: 1, Initial: 10 * time.Millisecond, Max: 20 * time.Millisecond, Factor: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(broker.AdminServer{Broker: b, InsecureDev: true, MaxBodyBytes: 1 << 20}.Handler())
	t.Cleanup(func() { ts.Close(); _ = b.Stop(context.Background()) })
	return b, ts
}

func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func runWorker(t *testing.T, w *bclient.Worker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("worker did not stop")
		}
	})
	return cancel
}

func TestRemoteConsumerQueueLifecycle(t *testing.T) {
	_, ts := remoteTestBroker(t)
	var handled atomic.Int64
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "queue-consumer-1", Group: "queue-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, VisibilityTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleQueue("email.send", func(ctx context.Context, d bclient.Delivery) error { handled.Add(1); return nil })
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].Subscriptions) > 0
	})
	if _, err := c.Publish(context.Background(), "email.created", map[string]any{"to": "a@example.com"}, bclient.WithQueue("email.send")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return handled.Load() == 1 })
	waitFor(t, time.Second, func() bool { cs, _ := c.Consumers(context.Background()); return len(cs) == 1 && cs[0].Acked >= 1 })
}

func TestRemoteWildcardConsumerWorker(t *testing.T) {
	_, ts := remoteTestBroker(t)
	var seen atomic.Bool
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "wild-1", Group: "wild-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandlePattern("tenant.*.invoice.created", func(ctx context.Context, d bclient.Delivery) error {
		if d.Message.Topic == "tenant.123.invoice.created" {
			seen.Store(true)
		}
		return nil
	})
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].Subscriptions) > 0
	})
	if _, err := c.Publish(context.Background(), "tenant.123.invoice.created", map[string]any{"id": "INV-1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return seen.Load() })
}

func TestRemoteTaskWorkerProgressAndResult(t *testing.T) {
	_, ts := remoteTestBroker(t)
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "task-1", Group: "pdf-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleTask("pdf.generate", func(ctx context.Context, d bclient.Delivery) (any, error) {
		if err := d.Progress(ctx, broker.Progress{Percent: 55, Message: "rendering", UpdatedAt: time.Now()}); err != nil {
			return nil, err
		}
		return map[string]any{"url": "file://ok.pdf"}, nil
	})
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].Subscriptions) > 0
	})
	task, err := c.SubmitTask(context.Background(), "pdf.generate", map[string]any{"invoice": "INV-1"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		info, _ := c.Task(context.Background(), task.ID)
		return info.Status == broker.TaskSucceeded
	})
	info, payload, err := c.TaskResult(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Progress.Percent != 55 {
		t.Fatalf("expected progress 55, got %+v", info.Progress)
	}
	var out map[string]string
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatal(err)
	}
	if out["url"] != "file://ok.pdf" {
		t.Fatalf("unexpected task result: %s", payload)
	}
}

func TestRemoteRequestResponse(t *testing.T) {
	_, ts := remoteTestBroker(t)
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "rpc-1", Group: "rpc-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleRequest("payment.authorize", func(ctx context.Context, d bclient.Delivery) (any, error) {
		return map[string]any{"approved": true}, nil
	})
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].Subscriptions) > 0
	})
	var resp struct {
		Approved bool `json:"approved"`
	}
	if err := c.Request(context.Background(), "payment.authorize", map[string]any{"amount": 100}, &resp, time.Second); err != nil {
		t.Fatal(err)
	}
	if !resp.Approved {
		t.Fatalf("expected approved response")
	}
}

func TestRemoteFailureMovesToDLQ(t *testing.T) {
	b, ts := remoteTestBroker(t)
	var attempts atomic.Int64
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "fail-1", Group: "fail-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleQueue("fail.q", func(ctx context.Context, d bclient.Delivery) error { attempts.Add(1); return assertErr{} })
	runWorker(t, w)
	waitFor(t, time.Second, func() bool { return len(b.Consumers().List()) == 1 })
	if _, err := b.Publisher().Publish(context.Background(), "fail.event", map[string]any{"x": 1}, broker.WithQueue("fail.q"), broker.WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		msgs, _ := b.Store().ListDLQ(context.Background(), "fail.q.dlq", 10)
		return attempts.Load() >= 1 && len(msgs) == 1 && msgs[0].Status == broker.MessageDeadLettered
	})
}

type assertErr struct{}

func (assertErr) Error() string { return "forced failure" }
