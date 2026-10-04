package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These benchmarks cover the store lock and allocation behaviour that the
// embedded data plane depends on. Two alternatives were implemented, measured
// and rejected; the reasoning is recorded here so they are not retried blindly.
//
// Rejected: promoting the empty-queue Lease check and the no-op CommitOffset
// check to RWMutex read locks. Both are faster single-threaded (Lease 64ns to
// 53ns, CommitOffset 36ns to 26ns) but collapse publisher throughput when many
// goroutines take the read lock repeatedly: publisher Append ran roughly 10x
// slower with 32 spinning pollers and roughly 8x slower with 32 spinning
// no-op committers. The store has one global RWMutex, so read-lock traffic from
// pollers and committers lands on the same cache line publishers need.
// ProfileLatency ships a 1ms PollInterval and Config.PollInterval is not clamped,
// so the spin regime is reachable in production. Shrinking the critical section
// is the fix; relaxing the lock is not.

func benchQueue(tb testing.TB, store *MemoryStore, names ...string) {
	tb.Helper()
	for _, name := range names {
		if err := store.DeclareQueue(context.Background(), QueueConfig{Name: name}); err != nil {
			tb.Fatal(err)
		}
	}
}

var benchIDCounter atomic.Int64

func newBenchID() string { return "bench-msg-" + itoa(int(benchIDCounter.Add(1))) }

// BenchmarkIdleQueuePoll measures a poll against a queue holding nothing, which
// is what every idle consumer does between messages.
func BenchmarkIdleQueuePoll(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	benchQueue(b, store, "idle")

	req := LeaseRequest{Queue: "idle", ConsumerID: "c1", Limit: 32}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Lease(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// startPollers runs n consumers polling an empty queue. A zero interval spins with
// no pause, which is deliberately unrealistic but bounds the reader-contention
// worst case. PollInterval defaults to 5ms and ProfileLatency uses 1ms.
func startPollers(tb testing.TB, ctx context.Context, store *MemoryStore, n int, interval time.Duration) func() {
	tb.Helper()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := LeaseRequest{Queue: "idle", ConsumerID: "c", Limit: 32}
			if interval == 0 {
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, _ = store.Lease(ctx, req)
				}
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_, _ = store.Lease(ctx, req)
				}
			}
		}()
	}
	return func() {
		close(stop)
		wg.Wait()
	}
}

// BenchmarkAppendWithIdlePollers measures publish throughput with idle consumers
// attached at the real poll rate.
func BenchmarkAppendWithIdlePollers(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	benchQueue(b, store, "idle", "target")

	stop := startPollers(b, ctx, store, 32, 5*time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Append(ctx, Message{ID: newBenchID(), Queue: "target", Payload: []byte("payload")}, []Destination{{Kind: DestinationQueue, Name: "target"}})
	}
	b.StopTimer()
	stop()
}

// BenchmarkAppendWithSpinningPollers is the unthrottled worst case. It guards the
// rejected read-lock fast path: a regression here means Lease or CommitOffset has
// started taking the store read lock on a hot path again.
func BenchmarkAppendWithSpinningPollers(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	benchQueue(b, store, "idle", "target")

	stop := startPollers(b, ctx, store, 32, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Append(ctx, Message{ID: newBenchID(), Queue: "target", Payload: []byte("payload")}, []Destination{{Kind: DestinationQueue, Name: "target"}})
	}
	b.StopTimer()
	stop()
}

// BenchmarkSweepLeasesNoop measures the idle lease sweeper, which runs on a timer
// whether or not anything is in flight.
func BenchmarkSweepLeasesNoop(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	benchQueue(b, store, "sweep")

	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SweepLeases(ctx, now); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendWithSpinningSweepers covers the SweepLeases fast path, which
// takes only leasedMu. leasedMu is also taken on every append, so this checks
// that skipping the store lock did not simply move the contention.
func BenchmarkAppendWithSpinningSweepers(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	benchQueue(b, store, "target")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	now := time.Now()
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = store.SweepLeases(ctx, now)
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Append(ctx, Message{ID: newBenchID(), Queue: "target", Payload: []byte("payload")}, []Destination{{Kind: DestinationQueue, Name: "target"}})
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// BenchmarkBackpressureCheck measures the broker-level depth check with an empty
// queue. It used to call Store.Stats once per destination, and Stats scans every
// message, queue, topic and stream in the store.
func BenchmarkBackpressureCheck(b *testing.B) {
	ctx := context.Background()
	bk, err := New(Config{Backpressure: BackpressureReject})
	if err != nil {
		b.Fatal(err)
	}
	defer bk.Stop(ctx)
	for _, q := range []string{"bp-a", "bp-b", "bp-c", "bp-d"} {
		if err := bk.DeclareQueue(ctx, QueueConfig{Name: q, MaxDepth: 1000000}); err != nil {
			b.Fatal(err)
		}
	}
	dests := []Destination{
		{Kind: DestinationQueue, Name: "bp-a"},
		{Kind: DestinationQueue, Name: "bp-b"},
		{Kind: DestinationQueue, Name: "bp-c"},
		{Kind: DestinationQueue, Name: "bp-d"},
	}
	prep := preparedPublish{msg: Message{ID: "m"}, dests: dests}
	p := bk.Publisher()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.checkBackpressure(ctx, prep); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBackpressureCheckWithBacklog is the case that regressed hardest, since
// the old per-destination Stats call grew with the store: about 768us per call at
// a 5000 message backlog, against roughly 154ns now.
func BenchmarkBackpressureCheckWithBacklog(b *testing.B) {
	ctx := context.Background()
	bk, err := New(Config{Backpressure: BackpressureReject})
	if err != nil {
		b.Fatal(err)
	}
	defer bk.Stop(ctx)
	if err := bk.DeclareQueue(ctx, QueueConfig{Name: "bp-big", MaxDepth: 1000000}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		if _, err := bk.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("bp-big")); err != nil {
			b.Fatal(err)
		}
	}
	prep := preparedPublish{msg: Message{ID: "m"}, dests: []Destination{
		{Kind: DestinationQueue, Name: "bp-big"},
		{Kind: DestinationQueue, Name: "bp-big"},
		{Kind: DestinationQueue, Name: "bp-big"},
		{Kind: DestinationQueue, Name: "bp-big"},
	}}
	p := bk.Publisher()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.checkBackpressure(ctx, prep); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentPublishWithBackpressure is the end-to-end effect on the
// publish path. The old per-destination Stats call held the store read lock
// across a full-store scan, which serialised every publisher in the process.
func BenchmarkConcurrentPublishWithBackpressure(b *testing.B) {
	ctx := context.Background()
	bk, err := New(Config{Backpressure: BackpressureReject})
	if err != nil {
		b.Fatal(err)
	}
	defer bk.Stop(ctx)
	for _, q := range []string{"bp-a", "bp-b"} {
		if err := bk.DeclareQueue(ctx, QueueConfig{Name: q, MaxDepth: 1000000}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = bk.Publisher().Publish(ctx, "", "payload", WithQueue("bp-a"), WithQueue("bp-b"))
		}
	})
}
