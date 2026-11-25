package pool

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestQueuePolicyBuilderAndProgress(t *testing.T) {
	p := MustNewPool(WithWorkers(1), WithQueueSize(32), WithJobTracking(true), WithQueuePolicy("email", QueuePolicy{MaxAttempts: 1, RateLimitKey: "email"}), WithRateLimit("email", RateLimit{Rate: 1000, Burst: 1}))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()
	done := make(chan struct{})
	err := p.JobFunc(func(ctx context.Context) error { ReportProgress(ctx, 50, "half"); close(done); return nil }).Queue("email").ID("job-progress").Submit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-done
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if p.Stats().Completed != 1 {
		t.Fatalf("completed=%d", p.Stats().Completed)
	}
}

func TestPauseResumeNamedQueue(t *testing.T) {
	p := MustNewPool(WithWorkers(1), WithQueueSize(16), WithJobTracking(true))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()
	q := p.Queue("reports")
	q.Pause()
	var ran atomic.Int64
	if err := q.SubmitFunc(func(context.Context) error { ran.Add(1); return nil }, JobOptions{ID: "paused-job"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatal("job ran while queue was paused")
	}
	q.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 {
		t.Fatalf("ran=%d", ran.Load())
	}
}

func TestHTTPHandlerAcceptsAndProcesses(t *testing.T) {
	p := MustNewPool(WithWorkers(1), WithQueueSize(16), WithJobTracking(true))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()
	done := make(chan struct{})
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Post("/work", HTTPHandler(p, HTTPOptions{Queue: "api", RespondJobID: true, Handler: func(ctx context.Context, r HTTPRequest) error { close(done); return nil }}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	defer app.ShutdownWithTimeout(time.Second)
	client := fh.NewClient(fh.ClientConfig{Timeout: 5 * time.Second, Retry: fh.NoRetry(), Logger: fh.NewNoopLogger()})
	resp, err := client.R().Bytes([]byte(`{"ok":true}`), "application/json").Post(context.Background(), "http://"+ln.Addr().String()+"/work")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Close()
	body, _ := resp.Bytes()
	if resp.StatusCode() != fh.StatusAccepted || !strings.Contains(string(body), `"job_id":"job-`) {
		t.Fatalf("code=%d body=%s", resp.StatusCode(), body)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("job not processed")
	}
}

func TestSpilloverAcceptsWhenQueueFull(t *testing.T) {
	dir := t.TempDir()
	p := MustNewPool(WithWorkers(1), WithQueueSize(1), WithBackpressure(BackpressureReject), WithSpillover(dir, 10))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Terminate()
	block := make(chan struct{})
	_ = p.SubmitFunc(func(context.Context) error { <-block; return nil })
	accepted := 0
	for i := 0; i < 5; i++ {
		if err := p.SubmitFunc(func(context.Context) error { return nil }); err == nil {
			accepted++
		}
	}
	close(block)
	if accepted == 0 {
		t.Fatal("expected spillover to accept extra jobs")
	}
}
