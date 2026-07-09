package fastworker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPauseResumeStop(t *testing.T) {
	p := MustNew(Config{MinWorkers: 2, QueueSize: 16})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	if err := p.Pause(); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	for i := 0; i < 4; i++ {
		if err := p.SubmitFunc(func(context.Context) error { n.Add(1); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := n.Load(); got != 0 {
		t.Fatalf("paused pool ran jobs: %d", got)
	}
	if err := p.Resume(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := n.Load(); got != 4 {
		t.Fatalf("expected 4 jobs, got %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRetryDLQ(t *testing.T) {
	p := MustNew(Config{MinWorkers: 1, QueueSize: 16})
	_ = p.Start()
	_ = p.SubmitFunc(func(context.Context) error { return context.DeadlineExceeded }, JobOptions{MaxAttempts: 2, Backoff: ConstantBackoff(time.Millisecond)})
	time.Sleep(30 * time.Millisecond)
	_ = p.Stop(context.Background())
	if p.Stats().Retried == 0 || p.Stats().DeadLettered == 0 {
		t.Fatalf("expected retry and dlq: %+v", p.Stats())
	}
}
