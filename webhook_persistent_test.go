package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPersistentWebhookRetryAndCircuitBreaker(t *testing.T) {
	var requestCount atomic.Int32
	var shouldFail atomic.Bool
	shouldFail.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dispatcher := NewPersistentWebhookDispatcher(PersistentWebhookConfig{
		MaxAttempts:      2,
		InitialBackoff:   5 * time.Millisecond,
		MaxBackoff:       10 * time.Millisecond,
		FailureThreshold: 2, // 2 failed dispatches open the circuit
		CooldownDuration: 50 * time.Millisecond,
	})

	ctx := context.Background()

	// 1. Dispatch 1: fails after 2 attempts
	n1 := Notification{ID: "notif-1", Type: "test.event"}
	err := dispatcher.Dispatch(ctx, srv.URL, n1)
	if err == nil {
		t.Fatal("expected dispatch error")
	}

	hist := dispatcher.History(srv.URL)
	if len(hist) != 2 {
		t.Fatalf("expected 2 attempts recorded in history, got %d", len(hist))
	}

	// 2. Dispatch 2: fails again, trips circuit breaker
	n2 := Notification{ID: "notif-2", Type: "test.event"}
	err = dispatcher.Dispatch(ctx, srv.URL, n2)
	if err == nil {
		t.Fatal("expected dispatch error")
	}

	if dispatcher.CircuitState(srv.URL) != CircuitOpen {
		t.Fatalf("expected circuit to be Open, got %s", dispatcher.CircuitState(srv.URL))
	}

	// 3. Dispatch 3: fast fail with ErrCircuitOpen (no HTTP request made)
	beforeCount := requestCount.Load()
	n3 := Notification{ID: "notif-3", Type: "test.event"}
	err = dispatcher.Dispatch(ctx, srv.URL, n3)
	if err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if requestCount.Load() != beforeCount {
		t.Fatal("expected no HTTP requests when circuit is open")
	}

	// 4. Server recovers and circuit is reset
	shouldFail.Store(false)
	dispatcher.ResetCircuit(srv.URL)

	err = dispatcher.Dispatch(ctx, srv.URL, n3)
	if err != nil {
		t.Fatalf("expected successful dispatch after reset, got %v", err)
	}
	if dispatcher.CircuitState(srv.URL) != CircuitClosed {
		t.Fatalf("expected circuit Closed, got %s", dispatcher.CircuitState(srv.URL))
	}
}

func TestPersistentWebhookReTrigger(t *testing.T) {
	var delivered atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dispatcher := NewPersistentWebhookDispatcher(PersistentWebhookConfig{
		MaxAttempts: 1,
	})

	ctx := context.Background()
	n := Notification{ID: "notif-replay", Type: "order.paid"}

	// Initial dispatch
	err := dispatcher.Dispatch(ctx, srv.URL, n)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("expected 1 delivery, got %d", delivered.Load())
	}

	// Re-trigger by notification ID
	err = dispatcher.ReTrigger(ctx, srv.URL, "notif-replay")
	if err != nil {
		t.Fatalf("ReTrigger failed: %v", err)
	}
	if delivered.Load() != 2 {
		t.Fatalf("expected 2 deliveries after re-trigger, got %d", delivered.Load())
	}
}
