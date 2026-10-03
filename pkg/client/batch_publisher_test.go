package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func newWALBrokerServer(tb testing.TB, queue string) (*broker.Broker, string) {
	tb.Helper()
	store, err := broker.NewBinaryFileWALStore(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	b := broker.MustNew(broker.Config{Store: store, DefaultQueue: queue, NotificationHistoryCapacity: 0})
	if err := b.Start(context.Background()); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = b.Stop(context.Background()) })
	if err := b.DeclareQueue(context.Background(), broker.QueueConfig{Name: queue}); err != nil {
		tb.Fatal(err)
	}
	srv := httptest.NewServer(broker.AdminServer{Broker: b, InsecureDev: true, MaxBodyBytes: 32 << 20}.Handler())
	tb.Cleanup(srv.Close)
	return b, srv.URL
}

// Every concurrent single-message Publish is acknowledged, none is lost or
// duplicated, and all of them are durably queued.
func TestBatchPublisherAcknowledgesEveryMessage(t *testing.T) {
	b, url := newWALBrokerServer(t, "q")
	c := New(Config{BrokerURL: url, FrameCodec: b.FrameCodec()})
	p := c.NewBatchPublisher(BatchPublisherConfig{})
	defer p.Close()

	const n = 2000
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := p.Publish(context.Background(), broker.Message{ID: "bp-" + strconv.Itoa(i), Topic: "t", Queue: "q", Payload: []byte("x")})
			if err != nil {
				failed.Add(1)
				t.Errorf("publish %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.FailNow()
	}
	seen := map[string]bool{}
	for len(seen) < n {
		msgs, err := c.PollFrames(context.Background(), "q", 500)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			if seen[m.ID] {
				t.Fatalf("duplicate %s", m.ID)
			}
			seen[m.ID] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("got %d of %d messages", len(seen), n)
	}
}

// A message the broker rejects reports the broker's error to its caller
// instead of pretending success, and does not poison later messages.
func TestBatchPublisherPropagatesErrors(t *testing.T) {
	b, url := newWALBrokerServer(t, "q")
	c := New(Config{BrokerURL: url, FrameCodec: b.FrameCodec()})
	p := c.NewBatchPublisher(BatchPublisherConfig{})
	defer p.Close()
	first := broker.Message{ID: "d1", Topic: "t", Queue: "q", Payload: []byte("x"), DeduplicationID: "same"}
	if err := p.Publish(context.Background(), first); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	dup := first
	dup.ID = "d2"
	if err := p.Publish(context.Background(), dup); err == nil {
		t.Fatal("duplicate deduplication id was acknowledged")
	}
	if err := p.Publish(context.Background(), broker.Message{ID: "d3", Topic: "t", Queue: "q", Payload: []byte("y")}); err != nil {
		t.Fatalf("publish after a rejection: %v", err)
	}
}

func TestBatchPublisherClose(t *testing.T) {
	b, url := newWALBrokerServer(t, "q")
	c := New(Config{BrokerURL: url, FrameCodec: b.FrameCodec()})
	p := c.NewBatchPublisher(BatchPublisherConfig{})
	if err := p.Publish(context.Background(), broker.Message{ID: "a", Topic: "t", Queue: "q", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if err := p.Publish(context.Background(), broker.Message{ID: "b", Topic: "t", Queue: "q"}); !errors.Is(err, ErrBatchPublisherClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p2 := c.NewBatchPublisher(BatchPublisherConfig{})
	defer p2.Close()
	if err := p2.Publish(ctx, broker.Message{ID: "c", Topic: "t", Queue: "q"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_ = time.Now
}

// BenchmarkSinglePublish compares N concurrent single-message publishers using
// one HTTP request per message (JSON) against the coalescing BatchPublisher,
// both against a durable binary WAL, each caller waiting for its own fsync.
func benchmarkSingle(b *testing.B, coalesce bool, conc int) {
	br, url := newWALBrokerServer(b, "q")
	c := New(Config{BrokerURL: url, FrameCodec: br.FrameCodec()})
	var pub *BatchPublisher
	if coalesce {
		pub = c.NewBatchPublisher(BatchPublisherConfig{})
		defer pub.Close()
	}
	var seq atomic.Int64
	var wg sync.WaitGroup
	per := b.N / conc
	if per == 0 {
		per = 1
	}
	b.ResetTimer()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := "m-" + strconv.FormatInt(seq.Add(1), 10)
				var err error
				if coalesce {
					err = pub.Publish(context.Background(), broker.Message{ID: id, Topic: "t", Queue: "q", Payload: []byte("payload")})
				} else {
					_, err = c.Publish(context.Background(), "t", "payload", WithQueue("q"))
				}
				if err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(per*conc)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkSinglePublishPerRequest64(b *testing.B)  { benchmarkSingle(b, false, 64) }
func BenchmarkSinglePublishCoalesced64(b *testing.B)   { benchmarkSingle(b, true, 64) }
func BenchmarkSinglePublishPerRequest256(b *testing.B) { benchmarkSingle(b, false, 256) }
func BenchmarkSinglePublishCoalesced256(b *testing.B)  { benchmarkSingle(b, true, 256) }

// One rejected message must not fail unrelated messages coalesced into the same
// request: exactly the duplicates are rejected, everything else is accepted.
func TestBatchPublisherIsolatesRejectedMessage(t *testing.T) {
	b, url := newWALBrokerServer(t, "q")
	c := New(Config{BrokerURL: url, FrameCodec: b.FrameCodec()})
	p := c.NewBatchPublisher(BatchPublisherConfig{MaxInFlight: 1})
	defer p.Close()
	if err := p.Publish(context.Background(), broker.Message{ID: "seed", Topic: "t", Queue: "q", Payload: []byte("x"), DeduplicationID: "taken"}); err != nil {
		t.Fatal(err)
	}
	const good = 200
	var wg sync.WaitGroup
	var goodFailed, badAccepted atomic.Int64
	for i := 0; i < good; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := p.Publish(context.Background(), broker.Message{ID: "g" + strconv.Itoa(i), Topic: "t", Queue: "q", Payload: []byte("x")}); err != nil {
				goodFailed.Add(1)
			}
		}(i)
		if i%20 == 0 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := p.Publish(context.Background(), broker.Message{ID: "bad" + strconv.Itoa(i), Topic: "t", Queue: "q", Payload: []byte("x"), DeduplicationID: "taken"}); err == nil {
					badAccepted.Add(1)
				}
			}(i)
		}
	}
	wg.Wait()
	if goodFailed.Load() != 0 {
		t.Fatalf("%d valid messages were failed by a neighbour's rejection", goodFailed.Load())
	}
	if badAccepted.Load() != 0 {
		t.Fatalf("%d duplicate messages were acknowledged", badAccepted.Load())
	}
}

func BenchmarkSinglePublishPerRequest1024(b *testing.B) { benchmarkSingle(b, false, 1024) }
func BenchmarkSinglePublishCoalesced1024(b *testing.B)  { benchmarkSingle(b, true, 1024) }
func BenchmarkSinglePublishPerRequest4096(b *testing.B) { benchmarkSingle(b, false, 4096) }
func BenchmarkSinglePublishCoalesced4096(b *testing.B)  { benchmarkSingle(b, true, 4096) }
