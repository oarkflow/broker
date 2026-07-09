package fastworker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduleCronEvery(t *testing.T) {
	p := MustNew(Config{MinWorkers: 1, QueueSize: 16})
	_ = p.Start()
	defer p.Terminate()
	var n atomic.Int64
	h, err := p.ScheduleCron("tick", "@every 5ms", JobFunc(func(context.Context) error { n.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Cancel()
	time.Sleep(100 * time.Millisecond)
	if n.Load() == 0 {
		t.Fatal("cron did not run")
	}
}
