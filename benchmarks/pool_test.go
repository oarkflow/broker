package benchmarks

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/oarkflow/fastworker"
)

func addAtomic(_ context.Context, done *atomic.Int64) error {
	done.Add(1)
	return nil
}

type atomicTask struct {
	done *atomic.Int64
	fn   func(context.Context, *atomic.Int64) error
}

func (j atomicTask) Run(ctx context.Context) error { return j.fn(ctx, j.done) }

func BenchmarkSubmitRun(b *testing.B) {
	p := fastworker.MustNew(fastworker.Config{MinWorkers: 16, MaxWorkers: 16, QueueSize: b.N, Backpressure: fastworker.BackpressureBlock, DisableStats: true})
	_ = p.Start()
	var done atomic.Int64
	job := &atomicTask{done: &done, fn: addAtomic}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.SubmitFast(job)
	}
	for done.Load() < int64(b.N) {
	}
	b.StopTimer()
	_ = p.Stop(context.Background())
}

type atomicJob struct{ done *atomic.Int64 }

func (j atomicJob) Run(context.Context) error { j.done.Add(1); return nil }

func BenchmarkSubmitRunReusableJob(b *testing.B) {
	p := fastworker.MustNew(fastworker.Config{MinWorkers: 16, MaxWorkers: 16, QueueSize: b.N, Backpressure: fastworker.BackpressureBlock, DisableStats: true})
	_ = p.Start()
	var done atomic.Int64
	job := atomicJob{done: &done}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.SubmitFast(job)
	}
	for done.Load() < int64(b.N) {
	}
	b.StopTimer()
	_ = p.Stop(context.Background())
}
