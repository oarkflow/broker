package broker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func makeQueueEntries(queue string, count int, key string) []AppendEntry {
	entries := make([]AppendEntry, count)
	for i := range entries {
		entries[i] = AppendEntry{
			Message:      Message{ID: fmt.Sprintf("message-%06d", i), Topic: "bench", Key: key, Payload: []byte("payload"), CreatedAt: time.Now()},
			Destinations: []Destination{{Kind: DestinationQueue, Name: queue}},
		}
	}
	return entries
}

func TestBinaryWALMinimalBatchRecoveryAndBatchAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	codec := NewFrameCodec(FrameCodecConfig{Type: CodecFWB1, MaxFrameSize: 8 << 20})
	store, err := NewBinaryFileWALStoreWithCodec(dir, codec)
	if err != nil {
		t.Fatal(err)
	}
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAtomicMinimal(ctx, makeQueueEntries("q", 128, "")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewBinaryFileWALStoreWithCodec(dir, codec)
	if err != nil {
		t.Fatal(err)
	}
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	messages, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", ConsumerGroup: "g", Limit: 128, VisibilityTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 128 {
		t.Fatalf("expected 128 recovered messages, got %d", len(messages))
	}
	acks := make([]AckRequest, len(messages))
	for i := range messages {
		acks[i] = AckRequest{MessageID: messages[i].ID, Queue: "q", LeaseToken: messages[i].LeaseToken, ConsumerID: "c"}
	}
	for i, err := range store.AckBatch(ctx, acks) {
		if err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}
	if err := store.Ack(ctx, acks[0]); err != nil {
		t.Fatalf("duplicate ack should remain idempotent after payload eviction: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Queues["q"] != 0 || stats.Messages != 0 {
		t.Fatalf("expected drained and evicted queue, got %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionedWALOrderingRecoveryAndDrain(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	codec := NewFrameCodec(FrameCodecConfig{Type: CodecFWB1, MaxFrameSize: 8 << 20})
	store, err := NewPartitionedBinaryWALStore(dir, 4, codec)
	if err != nil {
		t.Fatal(err)
	}
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	entries := makeQueueEntries("q", 256, "same-ordering-key")
	if err := store.AppendAtomicMinimal(ctx, entries); err != nil {
		t.Fatal(err)
	}
	var shard int
	for i := range entries {
		value, ok := store.index.Load(entries[i].Message.ID)
		if !ok {
			t.Fatalf("missing shard index for %s", entries[i].Message.ID)
		}
		if i == 0 {
			shard = value.(int)
		} else if value.(int) != shard {
			t.Fatalf("same ordering key split across shards: %d and %d", shard, value.(int))
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewPartitionedBinaryWALStore(dir, 4, codec)
	if err != nil {
		t.Fatal(err)
	}
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	messages, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", ConsumerGroup: "g", Limit: 256, VisibilityTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 256 {
		t.Fatalf("expected 256 recovered messages, got %d", len(messages))
	}
	acks := make([]AckRequest, len(messages))
	for i := range messages {
		acks[i] = AckRequest{MessageID: messages[i].ID, Queue: "q", LeaseToken: messages[i].LeaseToken, ConsumerID: "c"}
	}
	for i, err := range store.AckBatch(ctx, acks) {
		if err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}
	if err := store.Ack(ctx, acks[0]); err != nil {
		t.Fatalf("duplicate ack should remain idempotent after payload eviction: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Queues["q"] != 0 || stats.Messages != 0 {
		t.Fatalf("expected drained partitioned store, got %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeFrameBatchPublishPollAndAck(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	b := MustNew(Config{Store: store, DefaultQueue: "q", NotificationHistoryCapacity: 0})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true, MaxBodyBytes: 8 << 20}.Handler())
	defer server.Close()

	messages := make([]Message, 64)
	for i := range messages {
		messages[i] = Message{Topic: "bench", Queue: "q", Payload: []byte(strconv.Itoa(i))}
	}
	body, err := EncodeMessageFrameBatch(nil, b.FrameCodec(), messages)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/publish/frame/batch", bytes.NewReader(body))
	req.Header.Set("Prefer", "return=minimal")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("publish status %d", resp.StatusCode)
	}

	query := url.Values{"queue": {"q"}, "consumer_id": {"c"}, "consumer_group": {"g"}, "limit": {"64"}}
	resp, err = http.Get(server.URL + "/v1/poll/frame?" + query.Encode())
	if err != nil {
		t.Fatal(err)
	}
	frameBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll status %d", resp.StatusCode)
	}
	leased, err := DecodeMessageFrameBatch(frameBody, b.FrameCodec(), 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 64 {
		t.Fatalf("expected 64 leased messages, got %d", len(leased))
	}
	acks := make([]AckRequest, len(leased))
	for i := range leased {
		acks[i] = AckRequest{MessageID: leased[i].ID, Queue: "q", LeaseToken: leased[i].LeaseToken, ConsumerID: "c"}
	}
	ackReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/ack/frame", bytes.NewReader(EncodeAckFrameBatch(nil, acks)))
	ackReq.Header.Set("Prefer", "return=minimal")
	ackResp, err := http.DefaultClient.Do(ackReq)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, ackResp.Body)
	ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack status %d", ackResp.StatusCode)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Queues["q"] != 0 || stats.Messages != 0 {
		t.Fatalf("expected queue fully drained and payloads evicted, got %+v", stats)
	}
}
