package broker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type invoice struct {
	ID string `json:"id"`
}
type reply struct {
	OK bool `json:"ok"`
}

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b := MustNew(Config{DefaultVisibility: 20 * time.Millisecond, PollInterval: time.Millisecond, LeaseSweepInterval: 5 * time.Millisecond})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

func TestWildcardRoutingAndConsumer(t *testing.T) {
	b := newTestBroker(t)
	got := make(chan string, 1)
	_, err := b.Subscribe(context.Background(), ConsumerConfig{Name: "invoice", Group: "invoice", Pattern: "tenant.*.invoice.created", Concurrency: 2}, func(ctx context.Context, d Delivery) error {
		var inv invoice
		if err := d.Decode(&inv); err != nil {
			return err
		}
		got <- inv.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Publisher().Publish(context.Background(), "tenant.123.invoice.created", invoice{ID: "inv-1"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v != "inv-1" {
			t.Fatalf("got %s", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestRequestResponse(t *testing.T) {
	b := newTestBroker(t)
	_, err := b.Subscribe(context.Background(), ConsumerConfig{Name: "lookup", Group: "lookup", Pattern: "user.lookup"}, func(ctx context.Context, d Delivery) error {
		return d.Respond(ctx, reply{OK: true})
	})
	if err != nil {
		t.Fatal(err)
	}
	var res reply
	if err := b.Publisher().Request(context.Background(), "user.lookup", map[string]string{"id": "u1"}, &res, WithTimeout(time.Second), WithRequireHandler(true)); err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatal("expected ok response")
	}
}

func TestRetryToDLQ(t *testing.T) {
	b := newTestBroker(t)
	_, err := b.Subscribe(context.Background(), ConsumerConfig{Name: "failer", Group: "failer", Queue: "jobs", VisibilityTimeout: 10 * time.Millisecond, RetryPolicy: RetryPolicy{MaxAttempts: 2, Initial: time.Millisecond, Max: time.Millisecond}, DLQ: "jobs.dlq"}, func(ctx context.Context, d Delivery) error {
		return errors.New("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Publisher().Publish(context.Background(), "jobs", map[string]bool{"x": true}, WithQueue("jobs"), WithMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		msgs, _ := b.store.ListQueue(context.Background(), "jobs.dlq", 10)
		if len(msgs) == 1 && msgs[0].Status == MessageDeadLettered {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("dlq timeout: %+v", msgs)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestMoveBetweenQueues(t *testing.T) {
	b := newTestBroker(t)
	res, err := b.Publisher().Publish(context.Background(), "source", map[string]string{"x": "y"}, WithQueue("source.queue"))
	if err != nil {
		t.Fatal(err)
	}
	mv, err := b.store.Move(context.Background(), MoveRequest{SourceType: DestinationQueue, Source: "source.queue", DestinationType: DestinationQueue, Destination: "target.queue", MessageIDs: []string{res.MessageID}, Mode: MoveRemoveSource, PreserveID: true, ResetAttempts: true, ResetLease: true})
	if err != nil {
		t.Fatal(err)
	}
	if mv.Moved != 1 {
		t.Fatalf("moved %d", mv.Moved)
	}
	msgs, err := b.store.ListQueue(context.Background(), "target.queue", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != res.MessageID {
		t.Fatalf("unexpected target msgs: %+v", msgs)
	}
}

func TestTaskSubmitAndResult(t *testing.T) {
	b := newTestBroker(t)
	_, err := b.RegisterTask(context.Background(), "sum", func(ctx context.Context, d TaskDelivery) (any, error) {
		return map[string]int{"sum": 3}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := b.Publisher().SubmitTask(context.Background(), "sum", map[string]int{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Task.Status != TaskSucceeded {
		t.Fatalf("status %s", res.Task.Status)
	}
	var out map[string]int
	if err := h.Result(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if out["sum"] != 3 {
		t.Fatalf("unexpected result %+v", out)
	}
}

func TestStoreHealthFencingDegradedMode(t *testing.T) {
	dir := t.TempDir()
	st, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer st.Close()

	b, err := New(Config{
		Store:             st,
		DefaultQueue:      "events",
		DefaultVisibility: time.Second,
	})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer b.Stop(ctx)

	if b.IsStoreFenced() {
		t.Fatalf("broker store should not be fenced initially")
	}

	// Trigger a store fault
	fault := errors.New("simulated unrecoverable I/O disk failure")
	b.HealthMonitor().Fence(fault)

	// Verify broker detected fence
	if !b.IsStoreFenced() {
		t.Fatalf("expected broker to be fenced after fault")
	}
	if b.State() != BrokerDegraded {
		t.Fatalf("expected broker state BrokerDegraded, got %v", b.State())
	}

	// Publishing should fail fast with ErrStorageUnavailable
	_, err = b.Publisher().Publish(ctx, "events", []byte("data"))
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("expected ErrStorageUnavailable when publishing while fenced, got: %v", err)
	}

	// Check /readyz via AdminServer returns 503
	admin := AdminServer{Broker: b, InsecureDev: true}
	req := httptest.NewRequest("GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable from /readyz when fenced, got %d", rec.Code)
	}
}

