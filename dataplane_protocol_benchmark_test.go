package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// These benchmarks measure the server's per-message protocol cost in isolation:
// prebuilt request bodies, an in-memory store, and no network or client. They
// show what each wire format costs the broker, separate from fsync or load
// generator effects.
func protocolBenchServer(b *testing.B) (http.Handler, *Broker) {
	b.Helper()
	store := NewMemoryStore()
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	br := MustNew(Config{Store: store, DefaultQueue: "q", NotificationHistoryCapacity: 0})
	if err := br.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { br.Stop(context.Background()) })
	if err := br.DeclareQueue(context.Background(), QueueConfig{Name: "q"}); err != nil {
		b.Fatal(err)
	}
	return AdminServer{Broker: br, InsecureDev: true, MaxBodyBytes: 64 << 20}.Handler(), br
}

func benchmarkProtocolPublish(b *testing.B, frame bool, batch int) {
	h, br := protocolBenchServer(b)
	payload := bytes.Repeat([]byte("x"), 256)
	var body []byte
	var path, ctype string
	if frame {
		msgs := make([]Message, batch)
		for i := range msgs {
			msgs[i] = Message{Topic: "bench", Queue: "q", Payload: payload, ContentType: "application/octet-stream"}
		}
		var err error
		body, err = EncodeMessageFrameBatch(nil, br.FrameCodec(), msgs)
		if err != nil {
			b.Fatal(err)
		}
		path, ctype = "/v1/publish/frame/batch", "application/vnd.pool.broker.frames"
	} else {
		items := make([]map[string]any, batch)
		for i := range items {
			items[i] = map[string]any{"topic": "bench", "queue": "q", "payload": json.RawMessage(`"` + string(payload) + `"`)}
		}
		body, _ = json.Marshal(items)
		path, ctype = "/publish/batch", "application/json"
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", ctype)
		req.Header.Set("Prefer", "return=minimal")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code >= 300 {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		if i%64 == 63 { // keep the backlog (and GC cost) bounded
			b.StopTimer()
			drainQueue(b, br)
			b.StartTimer()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
}

func drainQueue(b *testing.B, br *Broker) {
	b.Helper()
	if _, err := br.store.PurgeQueue(context.Background(), "q"); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkProtocolPublishJSON1(b *testing.B)    { benchmarkProtocolPublish(b, false, 1) }
func BenchmarkProtocolPublishFrame1(b *testing.B)   { benchmarkProtocolPublish(b, true, 1) }
func BenchmarkProtocolPublishJSON100(b *testing.B)  { benchmarkProtocolPublish(b, false, 100) }
func BenchmarkProtocolPublishFrame100(b *testing.B) { benchmarkProtocolPublish(b, true, 100) }
func BenchmarkProtocolPublishJSON500(b *testing.B)  { benchmarkProtocolPublish(b, false, 500) }
func BenchmarkProtocolPublishFrame500(b *testing.B) { benchmarkProtocolPublish(b, true, 500) }

var _ = strconv.Itoa

// benchmarkProtocolConsume measures lease+ack through the HTTP handlers for one
// wire format. The queue is refilled outside the timer.
func benchmarkProtocolConsume(b *testing.B, frame bool, batch int) {
	h, br := protocolBenchServer(b)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 256)
	refill := func() {
		entries := make([]AppendEntry, batch)
		for i := range entries {
			entries[i] = AppendEntry{
				Message:      Message{ID: br.nextID("m"), Topic: "bench", Queue: "q", Payload: payload},
				Destinations: []Destination{{Kind: DestinationQueue, Name: "q"}},
			}
		}
		if _, err := br.store.(AtomicStore).AppendAtomic(ctx, entries); err != nil {
			b.Fatal(err)
		}
	}
	do := func(method, path, ctype string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}
		req.Header.Set("Prefer", "return=minimal")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		refill()
		b.StartTimer()
		if frame {
			rec := do(http.MethodGet, "/v1/poll/frame?queue=q&consumer_id=c&consumer_group=g&limit="+strconv.Itoa(batch), "", nil)
			msgs, err := DecodeMessageFrameBatch(rec.Body.Bytes(), br.FrameCodec(), batch)
			if err != nil || len(msgs) != batch {
				b.Fatalf("poll: n=%d err=%v code=%d", len(msgs), err, rec.Code)
			}
			acks := make([]AckRequest, len(msgs))
			for j := range msgs {
				acks[j] = AckRequest{MessageID: msgs[j].ID, Queue: "q", LeaseToken: msgs[j].LeaseToken, ConsumerID: "c"}
			}
			if rec := do(http.MethodPost, "/v1/ack/frame", "application/vnd.pool.broker.acks", EncodeAckFrameBatch(nil, acks)); rec.Code >= 300 {
				b.Fatalf("ack: %d %s", rec.Code, rec.Body.String())
			}
		} else {
			body, _ := json.Marshal(map[string]any{"consumer_id": "c", "consumer_group": "g", "limit": batch})
			rec := do(http.MethodPost, "/queues/q/pull", "application/json", body)
			var msgs []struct {
				ID         string `json:"id"`
				LeaseToken string `json:"lease_token"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &msgs); err != nil || len(msgs) != batch {
				b.Fatalf("pull: n=%d err=%v code=%d", len(msgs), err, rec.Code)
			}
			acks := make([]map[string]any, len(msgs))
			for j, m := range msgs {
				acks[j] = map[string]any{"message_id": m.ID, "queue": "q", "lease_token": m.LeaseToken, "consumer_id": "c"}
			}
			ackBody, _ := json.Marshal(acks)
			if rec := do(http.MethodPost, "/ack/batch", "application/json", ackBody); rec.Code >= 300 {
				b.Fatalf("ack: %d %s", rec.Code, rec.Body.String())
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkProtocolConsumeJSON100(b *testing.B)  { benchmarkProtocolConsume(b, false, 100) }
func BenchmarkProtocolConsumeFrame100(b *testing.B) { benchmarkProtocolConsume(b, true, 100) }
