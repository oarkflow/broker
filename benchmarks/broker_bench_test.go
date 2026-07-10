package benchmarks

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func BenchmarkPublishThroughput(b *testing.B) {
	br := broker.MustNew(broker.Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	br.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q"})
	pub := br.Publisher()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), broker.WithQueue("bench.q")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	br := broker.MustNew(broker.Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	br.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q"})
	pub := br.Publisher()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), broker.WithQueue("bench.q")); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPublishLatency(b *testing.B) {
	br := broker.MustNew(broker.Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	br.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q"})
	pub := br.Publisher()
	var durations []time.Duration

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), broker.WithQueue("bench.q")); err != nil {
			b.Fatal(err)
		}
		durations = append(durations, time.Since(start))
	}

	b.StopTimer()

	if len(durations) == 0 {
		return
	}

	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sortTimes(sorted)

	b.ReportMetric(float64(sorted[len(sorted)*50/100].Microseconds()), "p50_us")
	b.ReportMetric(float64(sorted[len(sorted)*95/100].Microseconds()), "p95_us")
	b.ReportMetric(float64(sorted[len(sorted)*99/100].Microseconds()), "p99_us")
}

func BenchmarkLeaseThroughput(b *testing.B) {
	br := broker.MustNew(broker.Config{
		PollInterval:       time.Millisecond,
		LeaseSweepInterval: 100 * time.Millisecond,
		DefaultVisibility:  30 * time.Second,
	})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	br.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q", VisibilityTimeout: 30 * time.Second})
	pub := br.Publisher()

	for i := 0; i < 10000; i++ {
		if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), broker.WithQueue("bench.q")); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leased, err := br.Store().Lease(ctx, broker.LeaseRequest{
			Queue:             "bench.q",
			ConsumerID:        "bench",
			Limit:             1,
			VisibilityTimeout: 30 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = leased
	}
}

func BenchmarkAckThroughput(b *testing.B) {
	br := broker.MustNew(broker.Config{
		PollInterval:       time.Millisecond,
		LeaseSweepInterval: 100 * time.Millisecond,
		DefaultVisibility:  30 * time.Second,
	})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	br.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q", VisibilityTimeout: 30 * time.Second})
	pub := br.Publisher()
	store := br.Store()

	for i := 0; i < 10000; i++ {
		if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), broker.WithQueue("bench.q")); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leased, err := store.Lease(ctx, broker.LeaseRequest{
			Queue:             "bench.q",
			ConsumerID:        "bench",
			Limit:             1,
			VisibilityTimeout: 30 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range leased {
			if err := store.Ack(ctx, broker.AckRequest{
				MessageID:  m.ID,
				Queue:      "bench.q",
				LeaseToken: m.LeaseToken,
				ConsumerID: "bench",
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkDeclareQueue(b *testing.B) {
	br := broker.MustNew(broker.Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		name := RandString(8)
		if err := br.DeclareQueue(ctx, broker.QueueConfig{Name: name}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryStoreAppend(b *testing.B) {
	store := broker.NewMemoryStore()
	ctx := context.Background()
	store.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q"})

	msg := broker.Message{
		ID:        "test-id",
		Topic:     "bench",
		Queue:     "bench.q",
		Payload:   []byte(`{"ok":true}`),
		CreatedAt: time.Now(),
	}
	dests := []broker.Destination{{Kind: broker.DestinationQueue, Name: "bench.q"}}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.ID = RandString(16)
		if _, err := store.Append(ctx, msg, dests); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryStoreLease(b *testing.B) {
	store := broker.NewMemoryStore()
	ctx := context.Background()
	store.DeclareQueue(ctx, broker.QueueConfig{Name: "bench.q", VisibilityTimeout: 30 * time.Second})

	msg := broker.Message{
		ID:        "test-id",
		Topic:     "bench",
		Queue:     "bench.q",
		Payload:   []byte(`{"ok":true}`),
		CreatedAt: time.Now(),
	}
	dests := []broker.Destination{{Kind: broker.DestinationQueue, Name: "bench.q"}}

	for i := 0; i < 10000; i++ {
		msg.ID = RandString(16)
		if _, err := store.Append(ctx, msg, dests); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leased, err := store.Lease(ctx, broker.LeaseRequest{
			Queue:             "bench.q",
			ConsumerID:        "bench",
			Limit:             1,
			VisibilityTimeout: 30 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = leased
	}
}

var (
	rngPool = sync.Pool{
		New: func() any {
			return rand.New(rand.NewSource(time.Now().UnixNano()))
		},
	}
)

func RandString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	rng := rngPool.Get().(*rand.Rand)
	defer rngPool.Put(rng)

	buf := make([]byte, n)
	for i := range buf {
		buf[i] = letters[rng.Intn(len(letters))]
	}
	return string(buf)
}

func sortTimes(d []time.Duration) {
	n := len(d)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if d[j] < d[i] {
				d[i], d[j] = d[j], d[i]
			}
		}
	}
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
