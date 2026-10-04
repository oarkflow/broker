package broker

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// The wire frame codecs had no benchmark in-tree, so the FWB1 versus FWB2
// tradeoff was unmeasured even though FWB2 exists specifically to remove the
// string-key TLV overhead that profiling shows dominating the consume path.
// These benchmarks cover encode, decode and the batch helpers.

func benchFrameCodecs(b *testing.B) map[string]FrameCodec {
	return map[string]FrameCodec{
		"FWB1": NewBinaryMessageCodec(),
		"FWB2": NewCompactMessageCodec(WithCompactMaxFrameSize(defaultMaxFrame)),
	}
}

// realisticFrameMessage mirrors what the store actually holds after a publish:
// populated identity, routing and timing fields, a payload, and small headers.
func realisticFrameMessage(i int) Message {
	now := time.Unix(1700000000, 0).UTC()
	return Message{
		ID:             "msg-6f1c2d3e4a5b6c7d8e9f0a1b2c3d4e5f",
		Topic:          "orders.created",
		Queue:          "orders.process",
		Stream:         "orders",
		Partition:      3,
		Offset:         918273,
		Key:            "customer-4471",
		RoutingKey:     "orders.eu",
		Type:           "order.v2",
		ContentType:    "application/json",
		CorrelationID:  "corr-1a2b3c4d5e6f",
		CausationID:    "cause-9f8e7d6c5b4a",
		IdempotencyKey: "idem-11223344",
		TenantID:       "tenant-acme",
		Priority:       5,
		CreatedAt:      now,
		AvailableAt:    now,
		ExpiresAt:      now.Add(time.Hour),
		Payload:        make([]byte, 256),
		Headers:        map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "x-region": "eu-west-1"},
		Metadata:       map[string]string{"schema_version": "2", "origin": "checkout"},
	}
}

func BenchmarkFrameCodecEncode(b *testing.B) {
	msg := realisticFrameMessage(1)
	for name, codec := range benchFrameCodecs(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.EncodeMessage(nil, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFrameCodecEncodeIntoBuffer(b *testing.B) {
	msg := realisticFrameMessage(1)
	for name, codec := range benchFrameCodecs(b) {
		b.Run(name, func(b *testing.B) {
			buf := make([]byte, 0, 4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := codec.EncodeMessage(buf[:0], msg)
				if err != nil {
					b.Fatal(err)
				}
				buf = out
			}
			b.SetBytes(int64(len(buf)))
		})
	}
}

func BenchmarkFrameCodecDecode(b *testing.B) {
	for name, codec := range benchFrameCodecs(b) {
		encoded, err := codec.EncodeMessage(nil, realisticFrameMessage(1))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var out Message
			for i := 0; i < b.N; i++ {
				if err := codec.DecodeMessage(encoded, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFrameCodecRoundTrip(b *testing.B) {
	msg := realisticFrameMessage(1)
	for name, codec := range benchFrameCodecs(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var out Message
			for i := 0; i < b.N; i++ {
				encoded, err := codec.EncodeMessage(nil, msg)
				if err != nil {
					b.Fatal(err)
				}
				if err := codec.DecodeMessage(encoded, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The batch helpers are what the poll and ack endpoints actually call, so a
// per-message regression in them cannot be absorbed by the single-message path.
func BenchmarkEncodeMessageFrameBatch100(b *testing.B) {
	msgs := make([]Message, 100)
	for i := range msgs {
		msgs[i] = realisticFrameMessage(i)
	}
	for name, codec := range benchFrameCodecs(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := EncodeMessageFrameBatch(nil, codec, msgs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*len(msgs))/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// A minimal message isolates the fixed per-frame cost from the field-count cost.
func BenchmarkFrameCodecEncodeMinimal(b *testing.B) {
	msg := Message{ID: "m1", Topic: "t", Payload: []byte("hi")}
	for name, codec := range benchFrameCodecs(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.EncodeMessage(nil, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Broker-side publish primitives, with no HTTP or codec in the way. This is the
// floor the embedded path can reach.
func BenchmarkMemoryStoreAppend(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	msg := realisticFrameMessage(1)
	msg.IdempotencyKey = ""
	dests := []Destination{{Kind: DestinationQueue, Name: "q"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.ID = newBenchID()
		if _, err := store.Append(ctx, msg, dests); err != nil {
			b.Fatal(err)
		}
	}
}

// The full embedded publish, which is the number an in-process user actually
// gets. It includes codec, hooks, metrics, notifications and history.
func BenchmarkEmbeddedPublish(b *testing.B) {
	ctx := context.Background()
	bk, err := New(Config{})
	if err != nil {
		b.Fatal(err)
	}
	defer bk.Stop(ctx)
	if err := bk.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bk.Publisher().Publish(ctx, "", payload, WithQueue("q"), WithMessageID(newBenchID())); err != nil {
			b.Fatal(err)
		}
	}
}

// Same publish, with the observability features that production enables. The
// delta against BenchmarkEmbeddedPublish is the per-message cost of hooks,
// metrics and history.
func BenchmarkEmbeddedPublishWithObservability(b *testing.B) {
	ctx := context.Background()
	bk, err := New(Config{
		EnableNotifications:         true,
		EnableConsumerEventTracking: true,
		EnablePublisherTracking:     true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer bk.Stop(ctx)
	if err := bk.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bk.Publisher().Publish(ctx, "", payload, WithQueue("q"), WithMessageID(newBenchID())); err != nil {
			b.Fatal(err)
		}
	}
}

// The embedded consume path: lease, then ack every message returned.
func BenchmarkEmbeddedLeaseAck(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	msg := realisticFrameMessage(1)
	// IdempotencyKey would trip the duplicate check across iterations, and the
	// fixture's historical ExpiresAt would make every seeded message expire on
	// the lease path instead of being delivered.
	msg.IdempotencyKey = ""
	msg.ExpiresAt = time.Time{}
	dests := []Destination{{Kind: DestinationQueue, Name: "q"}}
	refill := func(n int) {
		for i := 0; i < n; i++ {
			msg.ID = newBenchID()
			if _, err := store.Append(ctx, msg, dests); err != nil {
				b.Fatal(err)
			}
		}
	}
	const batch = 100
	refill(batch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		refill(batch)
		b.StartTimer()
		msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", ConsumerGroup: "g", Limit: batch})
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) != batch {
			b.Fatalf("leased %d, want %d", len(msgs), batch)
		}
		for _, m := range msgs {
			if err := store.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "q", ConsumerID: "c", LeaseToken: m.LeaseToken}); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
}

// Append deep copies each message into the store. AppendAtomicOwned transfers
// ownership of the payload, headers and metadata instead, which is what the
// "Prefer: return=minimal" publish path uses and what an embedded publisher
// cannot currently reach. These two make the difference explicit.
func BenchmarkAppendCloningVsOwned(b *testing.B) {
	ctx := context.Background()
	msg := realisticFrameMessage(1)
	msg.IdempotencyKey = ""
	msg.ExpiresAt = time.Time{}
	dests := []Destination{{Kind: DestinationQueue, Name: "q"}}

	b.Run("AppendCloning", func(b *testing.B) {
		store := NewMemoryStore()
		defer store.Close()
		if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg.ID = newBenchID()
			if _, err := store.Append(ctx, msg, dests); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AppendAtomicOwned", func(b *testing.B) {
		store := NewMemoryStore()
		defer store.Close()
		if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Ownership transfer forbids reuse, so each iteration needs its own
			// payload, headers and metadata. The benchmark measures the store
			// path, not the caller-side construction.
			entry := AppendEntry{
				Message: Message{
					ID: newBenchID(), Topic: msg.Topic, Queue: msg.Queue,
					Payload: make([]byte, len(msg.Payload)),
					Headers: map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "x-region": "eu-west-1"},
				},
				Destinations: dests,
			}
			if err := store.AppendAtomicOwned(ctx, []AppendEntry{entry}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkLeaseAckMapAblation isolates the cost of deep-copying Headers and
// Metadata at store boundaries. cloneMap is over half of all allocations on the
// consume path, so this bounds what an immutable or copy-on-write header map
// would buy. Compare against BenchmarkEmbeddedLeaseAck.
func BenchmarkLeaseAckMapAblation(b *testing.B) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		headers map[string]string
		meta    map[string]string
		history int
	}{
		{"WithHeadersAndMetadata", realisticFrameMessage(1).Headers, realisticFrameMessage(1).Metadata, 32},
		{"NoHeadersOrMetadata", nil, nil, 32},
		{"NoHistory", realisticFrameMessage(1).Headers, realisticFrameMessage(1).Metadata, 0},
		{"Neither", nil, nil, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			store := NewMemoryStore()
			defer store.Close()
			store.SetHistoryLimit(tc.history)
			if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
				b.Fatal(err)
			}
			msg := realisticFrameMessage(1)
			msg.IdempotencyKey = ""
			msg.ExpiresAt = time.Time{}
			msg.Headers = tc.headers
			msg.Metadata = tc.meta
			dests := []Destination{{Kind: DestinationQueue, Name: "q"}}
			refill := func(n int) {
				for i := 0; i < n; i++ {
					msg.ID = newBenchID()
					if _, err := store.Append(ctx, msg, dests); err != nil {
						b.Fatal(err)
					}
				}
			}
			const batch = 100
			refill(batch)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				refill(batch)
				b.StartTimer()
				msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", ConsumerGroup: "g", Limit: batch})
				if err != nil {
					b.Fatal(err)
				}
				for _, m := range msgs {
					if err := store.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "q", ConsumerID: "c", LeaseToken: m.LeaseToken}); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}

// BenchmarkEmbeddedPublishOwned compares the copying publish with the
// ownership-transferring one. Message construction is excluded from the timer for
// both variants: PublishOwned requires a fresh Message per call, so counting its
// allocation against the store path would compare setup cost rather than the
// publish path. The pool is refilled outside the timer and is large enough that
// it never runs dry within one iteration.
func BenchmarkEmbeddedPublishOwned(b *testing.B) {
	ctx := context.Background()
	newBroker := func() *Broker {
		bk, err := New(Config{})
		if err != nil {
			b.Fatal(err)
		}
		if err := bk.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			b.Fatal(err)
		}
		return bk
	}
	const poolSize = 4096
	newMsg := func(i int) Message {
		return Message{
			ID:       fmt.Sprintf("m-%d", i),
			Queue:    "q",
			Payload:  make([]byte, 256),
			Headers:  map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
			Metadata: map[string]string{"schema_version": "2"},
		}
	}

	b.Run("Publish", func(b *testing.B) {
		bk := newBroker()
		defer bk.Stop(ctx)
		payload := make([]byte, 256)
		headers := map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
		meta := map[string]string{"schema_version": "2"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := fmt.Sprintf("m-%d", i)
			if _, err := bk.Publisher().Publish(ctx, "", payload,
				WithQueue("q"), WithMessageID(id), WithHeaders(headers), WithMetadata(meta)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PublishOwned", func(b *testing.B) {
		bk := newBroker()
		defer bk.Stop(ctx)
		pool := make([]Message, poolSize)
		for i := range pool {
			pool[i] = newMsg(i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i == 0 {
				b.StopTimer()
				for j := range pool {
					pool[j] = newMsg(i*poolSize + j)
				}
				b.StartTimer()
			}
			if _, err := bk.Publisher().PublishOwned(ctx, pool[i%poolSize]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkReadStreamTail measures the read a stream consumer makes when it is
// caught up at the tail, which is the common case for a live consumer. It used to
// scan every id the stream had ever accumulated on every call.
func BenchmarkReadStreamTail(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareStream(ctx, StreamConfig{Name: "s"}); err != nil {
		b.Fatal(err)
	}
	const total = 50000
	for i := 0; i < total; i++ {
		if _, err := store.Append(ctx, Message{
			ID: fmt.Sprintf("m-%d", i), Stream: "s", Payload: make([]byte, 64),
		}, []Destination{{Kind: DestinationStream, Name: "s"}}); err != nil {
			b.Fatal(err)
		}
	}
	after := uint64(total - 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read the same window repeatedly, which is what a caught-up consumer does.
		if _, err := store.ReadStream(ctx, StreamReadRequest{Stream: "s", AfterOffset: after, Limit: 100}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadStreamHead is the unchanged worst case: reading from offset zero
// still has to walk from the start.
func BenchmarkReadStreamHead(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareStream(ctx, StreamConfig{Name: "s"}); err != nil {
		b.Fatal(err)
	}
	const total = 50000
	for i := 0; i < total; i++ {
		if _, err := store.Append(ctx, Message{
			ID: fmt.Sprintf("m-%d", i), Stream: "s", Payload: make([]byte, 64),
		}, []Destination{{Kind: DestinationStream, Name: "s"}}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.ReadStream(ctx, StreamReadRequest{Stream: "s", AfterOffset: 0, Limit: 100}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSegmentedWALConcurrentAppends measures concurrent publishers against
// the segmented WAL, the store the readiness tracker recommends. It used to hold
// writeMu across its own fsync, so every publish paid a fully serialized Sync and
// the group-commit writer had nothing to coalesce.
func BenchmarkSegmentedWALConcurrentAppends(b *testing.B) {
	runConcurrentAppends(b, func(tb testing.TB, dir string) Store {
		store, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir})
		if err != nil {
			tb.Fatal(err)
		}
		return store
	})
}

// BenchmarkBinaryWALConcurrentAppends is the reference: BinaryFileWALStore
// already released its write lock before awaiting the fsync, so it shows what
// group-commit sharing looks like when it works.
func BenchmarkBinaryWALConcurrentAppends(b *testing.B) {
	runConcurrentAppends(b, func(tb testing.TB, dir string) Store {
		store, err := NewBinaryFileWALStore(filepath.Join(dir, "wal"))
		if err != nil {
			tb.Fatal(err)
		}
		return store
	})
}

func runConcurrentAppends(b *testing.B, open func(testing.TB, string) Store) {
	ctx := context.Background()
	store := open(b, b.TempDir())
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	dests := []Destination{{Kind: DestinationQueue, Name: "q"}}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = store.Append(ctx, Message{
				ID: newBenchID(), Queue: "q", Payload: make([]byte, 128),
			}, dests)
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "append/s")
}
