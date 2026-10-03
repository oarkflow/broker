package broker

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type atomicAppendStore interface {
	DeclareQueue(context.Context, QueueConfig) error
	AppendAtomic(context.Context, []AppendEntry) ([]Message, error)
}

func benchmarkAppendStore(b *testing.B, store atomicAppendStore, queueName string) {
	b.Helper()
	benchmarkAppendStoreP(b, store, queueName, 1)
}

// benchmarkAppendStoreP runs with parallelism*GOMAXPROCS concurrent callers,
// each waiting for its own durable (fsynced) acknowledgement.
func benchmarkAppendStoreP(b *testing.B, store atomicAppendStore, queueName string, parallelism int) {
	b.Helper()
	if err := store.DeclareQueue(context.Background(), QueueConfig{Name: queueName}); err != nil {
		b.Fatal(err)
	}
	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := strconv.FormatUint(seq.Add(1), 10)
			if _, err := store.AppendAtomic(context.Background(), []AppendEntry{{
				Message:      Message{ID: id, Topic: "bench", Payload: []byte("payload")},
				Destinations: []Destination{{Kind: DestinationQueue, Name: queueName}},
			}}); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "append/s")
}

func BenchmarkFileWALAppendParallel(b *testing.B) {
	store, err := NewBinaryFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkAppendStore(b, store, "bench")
}

func BenchmarkPartitionedBinaryWALAppendParallel4(b *testing.B) {
	store, err := NewPartitionedBinaryWALStore(b.TempDir(), 4, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkAppendStore(b, store, "bench")
}

func BenchmarkPartitionedBinaryWALAppendParallel8(b *testing.B) {
	store, err := NewPartitionedBinaryWALStore(b.TempDir(), 8, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkAppendStore(b, store, "bench")
}

// roundTripStore is the producer/consumer surface used by the end-to-end
// throughput test below.
type roundTripStore interface {
	atomicAppendStore
	Lease(context.Context, LeaseRequest) ([]Message, error)
	Ack(context.Context, AckRequest) error
	AckBatch(context.Context, []AckRequest) []error
}

// benchmarkRoundTrip drives the shape a real dispatcher produces: concurrent
// producers appending while concurrent workers lease and ack, so every message
// costs three durable records.
func benchmarkRoundTrip(b *testing.B, store roundTripStore, producers, workers, batch int) {
	b.Helper()
	ctx := context.Background()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "rt"}); err != nil {
		b.Fatal(err)
	}
	total := b.N
	var next, done atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				end := next.Add(int64(batch))
				start := end - int64(batch) + 1
				if start > int64(total) {
					return
				}
				entries := make([]AppendEntry, 0, batch)
				for i := start; i <= end && i <= int64(total); i++ {
					entries = append(entries, AppendEntry{
						Message:      Message{ID: strconv.FormatInt(i, 10), Topic: "rt", Payload: []byte("payload")},
						Destinations: []Destination{{Kind: DestinationQueue, Name: "rt"}},
					})
				}
				if _, err := store.AppendAtomic(ctx, entries); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer := "w" + strconv.Itoa(id)
			for done.Load() < int64(total) {
				msgs, err := store.Lease(ctx, LeaseRequest{Queue: "rt", ConsumerID: consumer, Limit: batch, VisibilityTimeout: time.Minute})
				if err != nil {
					b.Error(err)
					return
				}
				if len(msgs) == 0 {
					time.Sleep(50 * time.Microsecond)
					continue
				}
				acks := make([]AckRequest, 0, len(msgs))
				for _, m := range msgs {
					acks = append(acks, AckRequest{MessageID: m.ID, Queue: "rt", LeaseToken: m.LeaseToken, ConsumerID: consumer})
				}
				for _, err := range store.AckBatch(ctx, acks) {
					if err != nil {
						b.Error(err)
						return
					}
				}
				done.Add(int64(len(msgs)))
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkBinaryWALRoundTrip8x16(b *testing.B) {
	store, err := NewBinaryFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkRoundTrip(b, store, 8, 16, 1)
}

func BenchmarkFileWALRoundTrip8x16(b *testing.B) {
	store, err := NewFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkRoundTrip(b, store, 8, 16, 1)
}

func benchmarkBinaryAppendConcurrency(b *testing.B, parallelism int) {
	store, err := NewBinaryFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkAppendStoreP(b, store, "bench", parallelism)
}

func BenchmarkBinaryWALAppendConc10(b *testing.B)  { benchmarkBinaryAppendConcurrency(b, 1) }
func BenchmarkBinaryWALAppendConc60(b *testing.B)  { benchmarkBinaryAppendConcurrency(b, 6) }
func BenchmarkBinaryWALAppendConc250(b *testing.B) { benchmarkBinaryAppendConcurrency(b, 25) }

func benchmarkBinaryRoundTripBatch(b *testing.B, batch int) {
	store, err := NewBinaryFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	benchmarkRoundTrip(b, store, 8, 16, batch)
}

func BenchmarkBinaryWALRoundTripBatch10(b *testing.B)  { benchmarkBinaryRoundTripBatch(b, 10) }
func BenchmarkBinaryWALRoundTripBatch100(b *testing.B) { benchmarkBinaryRoundTripBatch(b, 100) }

// BenchmarkBinaryWALAppendBulk8 mirrors `redis-benchmark -c 8 -P <batch>`: eight
// callers, each submitting <batch> messages per durable call.
func benchmarkBinaryAppendBulk(b *testing.B, batch int) {
	store, err := NewBinaryFileWALStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	if err := store.DeclareQueue(context.Background(), QueueConfig{Name: "bench"}); err != nil {
		b.Fatal(err)
	}
	var seq atomic.Uint64
	var wg sync.WaitGroup
	calls := b.N / batch
	var issued atomic.Int64
	b.ResetTimer()
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for issued.Add(1) <= int64(calls) {
				entries := make([]AppendEntry, batch)
				for i := range entries {
					entries[i] = AppendEntry{
						Message:      Message{ID: strconv.FormatUint(seq.Add(1), 10), Topic: "bench", Payload: []byte("payload")},
						Destinations: []Destination{{Kind: DestinationQueue, Name: "bench"}},
					}
				}
				if _, err := store.AppendAtomic(context.Background(), entries); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(calls*batch)/b.Elapsed().Seconds(), "append/s")
}

func BenchmarkBinaryWALAppendBulk1(b *testing.B)   { benchmarkBinaryAppendBulk(b, 1) }
func BenchmarkBinaryWALAppendBulk10(b *testing.B)  { benchmarkBinaryAppendBulk(b, 10) }
func BenchmarkBinaryWALAppendBulk100(b *testing.B) { benchmarkBinaryAppendBulk(b, 100) }
