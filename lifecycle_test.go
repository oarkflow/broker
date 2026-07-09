package fastworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLifecycleEventsAndCallbacks(t *testing.T) {
	var mu sync.Mutex
	seen := map[EventType]int{}
	cbSeen := map[string]int{}
	p := MustNew(DefaultConfig(), WithWorkers(1), WithQueueSize(16), WithAutoScale(false), WithLifecycleHook(func(ctx context.Context, e Event) {
		mu.Lock()
		seen[e.Type]++
		mu.Unlock()
	}))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	errFirst := errors.New("try again")
	attempts := 0
	err := p.SubmitFunc(func(ctx context.Context) error {
		attempts++
		ReportProgress(ctx, 50, "half")
		if attempts == 1 {
			return Retryable(errFirst)
		}
		return nil
	}, JobOptions{ID: "life-1", MaxAttempts: 2, Backoff: ConstantBackoff(1 * time.Millisecond), Callback: &Callback{
		OnQueued:   func(context.Context, JobInfo) { mu.Lock(); cbSeen["queued"]++; mu.Unlock() },
		OnStart:    func(context.Context, JobInfo) { mu.Lock(); cbSeen["start"]++; mu.Unlock() },
		OnRetry:    func(context.Context, JobInfo, int, error) { mu.Lock(); cbSeen["retry"]++; mu.Unlock() },
		OnProgress: func(context.Context, JobInfo, Progress) { mu.Lock(); cbSeen["progress"]++; mu.Unlock() },
		OnSuccess:  func(context.Context, JobInfo) { mu.Lock(); cbSeen["success"]++; mu.Unlock() },
		OnFinally:  func(context.Context, JobInfo, error) { mu.Lock(); cbSeen["finally"]++; mu.Unlock() },
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, typ := range []EventType{EventPoolStarted, EventJobSubmitted, EventJobAccepted, EventJobStarted, EventJobProgress, EventJobFailed, EventJobRetrying, EventJobSucceeded, EventPoolStopped} {
		if seen[typ] == 0 {
			t.Fatalf("missing lifecycle event %s; seen=%v", typ, seen)
		}
	}
	if cbSeen["queued"] != 1 || cbSeen["retry"] != 1 || cbSeen["success"] != 1 || cbSeen["finally"] != 1 || cbSeen["progress"] == 0 || cbSeen["start"] == 0 {
		t.Fatalf("callbacks not fired correctly: %#v", cbSeen)
	}
}

func TestLifecycleHandlerPanicRecovered(t *testing.T) {
	lc := NewLifecycle()
	lc.On(func(context.Context, Event) { panic("boom") })
	p := MustNewPool(WithLifecycle(lc), WithWorkers(1))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	_ = p.Terminate()
	if lc.RecoveredPanics() == 0 {
		t.Fatal("expected lifecycle to recover handler panic")
	}
}
