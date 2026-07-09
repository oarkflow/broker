package broker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fastworker/broker"
	bclient "github.com/oarkflow/fastworker/broker/client"
)

func TestRemoteCommandHandler(t *testing.T) {
	_, ts := remoteTestBroker(t)
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "cmd-1", Group: "cmd-workers", Concurrency: 1, PollWait: 20 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleCommand("payment.capture", func(ctx context.Context, d bclient.Delivery) (any, error) {
		return map[string]any{"captured": true}, nil
	})
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].CommandTypes) == 1
	})
	var resp struct {
		Captured bool `json:"captured"`
	}
	if err := c.Command(context.Background(), "payment.capture", map[string]any{"id": "p1"}, &resp, time.Second); err != nil {
		t.Fatal(err)
	}
	if !resp.Captured {
		t.Fatalf("expected captured response")
	}
}

func TestRemoteWorkerAutoExtendsLease(t *testing.T) {
	b, ts := remoteTestBroker(t)
	var handled atomic.Bool
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "lease-1", Group: "lease-workers", Concurrency: 1, MaxInFlight: 1, PollWait: 10 * time.Millisecond, HeartbeatInterval: 25 * time.Millisecond, VisibilityTimeout: 80 * time.Millisecond, LeaseExtensionInterval: 25 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleQueue("slow.q", func(ctx context.Context, d bclient.Delivery) error {
		time.Sleep(220 * time.Millisecond)
		handled.Store(true)
		return nil
	})
	runWorker(t, w)
	waitFor(t, time.Second, func() bool { return len(b.Consumers().List()) == 1 })
	if _, err := b.Publisher().Publish(context.Background(), "slow", map[string]any{"x": 1}, broker.WithQueue("slow.q")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return handled.Load() })
	waitFor(t, 2*time.Second, func() bool {
		msgs, _ := b.Store().ListQueue(context.Background(), "slow.q", 10)
		return len(msgs) == 0
	})
}

func TestRemoteConsumerHeartbeatTimeoutMarksDisconnected(t *testing.T) {
	b, err := broker.New(broker.Config{Mode: broker.ModeStandalone, RemoteConsumerTimeout: 80 * time.Millisecond, LeaseSweepInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	_, err = b.RegisterRemoteConsumer(context.Background(), broker.ConsumerRegistration{ConsumerID: "dead-1", Group: "dead-workers"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		info, ok := b.Consumers().Get("dead-1")
		return ok && info.State == broker.ConsumerDisconnected
	})
}

func TestTaskCancelSendsConsumerEventAndCancelsWorkerContext(t *testing.T) {
	_, ts := remoteTestBroker(t)
	started := make(chan broker.TaskInfo, 1)
	cancelled := make(chan struct{}, 1)
	w, err := bclient.NewWorker(bclient.Config{BrokerURL: ts.URL, ConsumerID: "cancel-1", Group: "cancel-workers", Concurrency: 1, PollWait: 10 * time.Millisecond, HeartbeatInterval: 25 * time.Millisecond, VisibilityTimeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.HandleTask("long.task", func(ctx context.Context, d bclient.Delivery) (any, error) {
		started <- broker.TaskInfo{ID: d.Message.TaskID}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	})
	runWorker(t, w)
	c := bclient.New(bclient.Config{BrokerURL: ts.URL, PollWait: 20 * time.Millisecond})
	waitFor(t, time.Second, func() bool {
		cs, _ := c.Consumers(context.Background())
		return len(cs) == 1 && len(cs[0].Subscriptions) > 0
	})
	task, err := c.SubmitTask(context.Background(), "long.task", map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}
	if err := c.CancelTask(context.Background(), task.ID, "test cancel"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("worker did not receive cancellation event")
	}
	info, err := c.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != broker.TaskCancelled {
		t.Fatalf("expected cancelled task, got %+v", info)
	}
}

func TestRemoteClientFailoverBrokerURLs(t *testing.T) {
	_, ts := remoteTestBroker(t)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad", http.StatusBadGateway) }))
	defer bad.Close()
	c := bclient.New(bclient.Config{BrokerURLs: []string{bad.URL, ts.URL}, PollWait: 20 * time.Millisecond})
	if _, err := c.Publish(context.Background(), "failover.event", map[string]any{"ok": true}, bclient.WithQueue("failover.q")); err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Poll(context.Background(), "failover.q", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected failover poll message, got %d", len(msgs))
	}
}
