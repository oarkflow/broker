package fastworker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitQueueAcceptsAndSmooths(t *testing.T) {
	p := MustNew(Config{MinWorkers: 2, QueueSize: 32})
	p.SetRateLimit("api", RateLimit{Rate: 20, Burst: 1}) // 1 immediate, then ~50ms spacing.
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()

	var completed atomic.Int64
	for i := 0; i < 6; i++ {
		if err := p.SubmitFunc(func(context.Context) error {
			completed.Add(1)
			return nil
		}, JobOptions{RateLimitKey: "api"}); err != nil {
			t.Fatalf("submit %d should be accepted and queued, got %v", i, err)
		}
	}

	time.Sleep(25 * time.Millisecond)
	if n := completed.Load(); n >= 6 {
		t.Fatalf("rate limiter did not smooth execution; completed too quickly: %d", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for completed.Load() != 6 {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for smoothed jobs, completed=%d stats=%+v", completed.Load(), p.Stats())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if st := p.Stats(); st.Rejected != 0 {
		t.Fatalf("queued rate limiting should not reject accepted jobs, stats=%+v", st)
	}
}

func TestRateLimitRejectModePreserved(t *testing.T) {
	p := MustNew(Config{MinWorkers: 1, QueueSize: 8})
	p.SetRateLimit("api", RateLimit{Rate: 1, Burst: 1, Mode: RateLimitReject})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()

	if err := p.SubmitFunc(func(context.Context) error { return nil }, JobOptions{RateLimitKey: "api"}); err != nil {
		t.Fatalf("first submit should pass burst token: %v", err)
	}
	if err := p.SubmitFunc(func(context.Context) error { return nil }, JobOptions{RateLimitKey: "api"}); err != ErrRateLimited {
		t.Fatalf("second submit should be rejected in reject mode, got %v", err)
	}
}
