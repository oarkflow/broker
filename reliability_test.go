package fastworker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestFutureWaitsForRetrySuccess(t *testing.T) {
	p := MustNew(Config{MinWorkers: 1, QueueSize: 16})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())
	var attempts atomic.Int64
	f, err := SubmitResult[int](p, func(context.Context) (int, error) {
		if attempts.Add(1) == 1 {
			return 0, errors.New("temporary")
		}
		return 99, nil
	}, JobOptions{MaxAttempts: 2, Backoff: ConstantBackoff(time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.Get(context.Background())
	if err != nil {
		t.Fatalf("future returned early error: %v", err)
	}
	if v != 99 {
		t.Fatalf("value=%d", v)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

func TestDelayedQueueWakesForEarlierJob(t *testing.T) {
	p := MustNew(Config{MinWorkers: 1, QueueSize: 16})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())
	done := make(chan struct{})
	_ = p.ScheduleAfter("late", time.Hour, JobFunc(func(context.Context) error { return nil }))
	time.Sleep(10 * time.Millisecond)
	if err := p.SubmitFunc(func(context.Context) error { close(done); return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("immediate job was blocked behind delayed job")
	}
	_ = p.Terminate()
}

func TestWaitIdle(t *testing.T) {
	p := MustNew(Config{MinWorkers: 2, QueueSize: 32})
	_ = p.Start()
	for i := 0; i < 10; i++ {
		_ = p.SubmitFunc(func(context.Context) error { time.Sleep(time.Millisecond); return nil })
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if p.Stats().Completed != 10 {
		t.Fatalf("completed=%d", p.Stats().Completed)
	}
	_ = p.Stop(context.Background())
}
