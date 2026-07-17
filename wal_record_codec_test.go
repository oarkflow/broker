package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func testWALMessage(id string) Message {
	now := time.Unix(1773748800, 123456789).UTC()
	return Message{
		ID: id, ParentID: "parent", Topic: "orders.created", Queue: "orders", Stream: "audit",
		Partition: 3, Offset: 99, Key: "customer-42", RoutingKey: "orders.eu.created",
		Type: "order", ContentType: "application/octet-stream", Schema: "order", SchemaVersion: "2",
		Payload: bytes.Repeat([]byte{1, 2, 3, 4}, 64), Headers: map[string]string{"x-tenant": "acme", "x-source": "api"},
		Metadata: map[string]string{"region": "eu"}, TraceID: "trace", SpanID: "span", CorrelationID: "corr",
		CausationID: "cause", RequestID: "request", ReplyTo: "reply", ReplyGroup: "reply-group",
		IdempotencyKey: "idempotent", DeduplicationID: "dedup", Checksum: "checksum", Priority: 7,
		CreatedAt: now, AvailableAt: now.Add(time.Second), ExpiresAt: now.Add(time.Hour), Attempts: 2,
		MaxAttempts: 9, Redeliveries: 1, TenantID: "tenant", ProducerID: "producer", ConsumerID: "consumer",
		ConsumerGroup: "group", LeaseToken: "lease", LeaseUntil: now.Add(time.Minute), OriginalTopic: "original.topic",
		OriginalQueue: "original.queue", OriginalStream: "original.stream", OriginalOffset: 10,
		Status: MessageLeased, LastError: "last error", DeadLetterReason: "reason", TaskID: "task", TaskName: "task-name",
	}
}

func normalizeWALRecordTimes(r *walRecord) {
	r.At = r.At.Round(0).UTC()
	normalizeMessage := func(m *Message) {
		m.CreatedAt = m.CreatedAt.Round(0).UTC()
		m.AvailableAt = m.AvailableAt.Round(0).UTC()
		m.ExpiresAt = m.ExpiresAt.Round(0).UTC()
		m.LeaseUntil = m.LeaseUntil.Round(0).UTC()
	}
	normalizeMessage(&r.Message)
	for i := range r.Messages {
		normalizeMessage(&r.Messages[i])
	}
	for i := range r.Entries {
		normalizeMessage(&r.Entries[i].Message)
	}
}

func TestWALRecordCodecRoundTripAllOperations(t *testing.T) {
	now := time.Unix(1773748800, 987654321).UTC()
	message := testWALMessage("message-1")
	entries := []AppendEntry{
		{Message: testWALMessage("message-1"), Destinations: []Destination{{Kind: DestinationQueue, Name: "orders"}}},
		{Message: testWALMessage("message-2"), Destinations: []Destination{{Kind: DestinationQueue, Name: "orders"}}},
	}
	messages := []Message{testWALMessage("message-1"), testWALMessage("message-2")}
	acks := []AckRequest{
		{MessageID: "message-1", Queue: "orders", LeaseToken: "lease-1", ConsumerID: "consumer"},
		{MessageID: "message-2", Queue: "orders", LeaseToken: "lease-2", ConsumerID: "consumer"},
	}
	cases := []walRecord{
		{Op: "declare_queue", At: now, Queue: QueueConfig{Name: "orders", Workers: 8, MaxInFlight: 1024, MaxDepth: 100000, VisibilityTimeout: time.Minute, AckMode: AckManual, RetryPolicy: DefaultRetryPolicy(), DLQ: "orders.dlq", Ordering: OrderKey, Backpressure: BackpressureBlock, Strategy: StrategyConsistentHash}},
		{Op: "declare_topic", At: now, Topic: TopicConfig{Name: "orders.created", Partitions: 16, Retention: 24 * time.Hour, Paused: true}},
		{Op: "declare_stream", At: now, Stream: StreamConfig{Name: "audit", Partitions: 8, Retention: 7 * 24 * time.Hour, Compact: true}},
		{Op: "append", At: now, Message: message, Dests: []Destination{{Kind: DestinationQueue, Name: "orders"}, {Kind: DestinationStream, Name: "audit"}}},
		{Op: "append_atomic", At: now, Entries: entries},
		{Op: "lease", At: now, Message: message, Lease: LeaseRequest{Queue: "orders", ConsumerID: "consumer", ConsumerGroup: "group", Limit: 64, VisibilityTimeout: time.Minute, LeaseToken: "lease", MessageID: "message-1"}},
		{Op: "lease_batch", At: now, Messages: messages, Lease: LeaseRequest{Queue: "orders", ConsumerID: "consumer", ConsumerGroup: "group", Limit: 64, VisibilityTimeout: time.Minute}},
		{Op: "update_batch", At: now, Messages: messages},
		{Op: "ack", At: now, Ack: acks[0]},
		{Op: "ack_batch", At: now, Acks: acks},
		{Op: "nack", At: now, Nack: NackRequest{MessageID: "message-1", Queue: "orders", LeaseToken: "lease", ConsumerID: "consumer", Action: NackRetry, Reason: "transient", Error: "timeout", Delay: time.Second, DLQ: "orders.dlq", ResetLease: true, IncrementAttempt: true}},
		{Op: "move", At: now, Move: MoveRequest{SourceType: DestinationQueue, Source: "orders", DestinationType: DestinationDLQ, Destination: "orders.dlq", MessageIDs: []string{"message-1", "message-2"}, Filter: MessageFilter{Status: MessageFailed, Type: "order", TenantID: "tenant", Headers: map[string]string{"x-source": "api"}}, Mode: MoveQuarantine, PreserveID: true, PreserveOffset: true, ResetAttempts: true, ResetLease: true, ResetError: true, Reason: "manual", Actor: "admin", DryRun: true}},
		{Op: "pause_queue", At: now, QueueName: "orders"},
		{Op: "resume_queue", At: now, QueueName: "orders"},
		{Op: "purge_queue", At: now, QueueName: "orders"},
		{Op: "update", At: now, Message: message},
		{Op: "commit_offset", At: now, Offset: OffsetCommit{Stream: "audit", ConsumerGroup: "analytics", Offset: 12345}},
	}
	codec, err := NewWALRecordCodec(WALCodecConfig{MaxRecordSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, original := range cases {
		t.Run(original.Op, func(t *testing.T) {
			frame, err := codec.encode(nil, original)
			if err != nil {
				t.Fatal(err)
			}
			if !isNativeBinaryWALRecord(frame) {
				t.Fatal("record is not BWL1")
			}
			var decoded walRecord
			if err := codec.decode(frame, &decoded); err != nil {
				t.Fatal(err)
			}
			normalizeWALRecordTimes(&original)
			normalizeWALRecordTimes(&decoded)
			if !reflect.DeepEqual(original, decoded) {
				t.Fatalf("roundtrip mismatch\noriginal: %#v\ndecoded:  %#v", original, decoded)
			}
		})
	}
}

func TestWALRecordCodecEncryptionSigningAndCorruption(t *testing.T) {
	codec, err := NewWALRecordCodec(WALCodecConfig{
		MaxRecordSize: 1 << 20,
		EncryptionKey: []byte("0123456789abcdef0123456789abcdef"),
		SigningKey:    []byte("signing-key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	original := walRecord{Op: "append_atomic", At: time.Now().UTC(), Entries: []AppendEntry{{Message: testWALMessage("m1"), Destinations: []Destination{{Kind: DestinationQueue, Name: "orders"}}}}}
	frame, err := codec.encode(nil, original)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(frame, original.Entries[0].Message.Payload) {
		t.Fatal("encrypted WAL record contains plaintext payload")
	}
	var decoded walRecord
	if err := codec.decode(frame, &decoded); err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), frame...)
	corrupted[len(corrupted)/2] ^= 0x40
	if err := codec.decode(corrupted, &decoded); err == nil {
		t.Fatal("expected corruption to be detected")
	}
}

func TestBinaryWALLegacyJSONFrameRecovery(t *testing.T) {
	dir := t.TempDir()
	codec := NewBinaryMessageCodec(WithFrameMaxSize(8 << 20))
	record := walRecord{Op: "append_atomic", At: time.Now().UTC(), Entries: []AppendEntry{{Message: Message{ID: "legacy-1", Topic: "legacy", Payload: []byte("legacy")}, Destinations: []Destination{{Kind: DestinationQueue, Name: "legacy-q"}}}}}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := codec.EncodeMessage(nil, Message{ID: "legacy-wal", Type: "wal.append_atomic", ContentType: "application/json", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], uint64(len(frame)))
	wal := append(prefix[:n:n], frame...)
	if err := os.WriteFile(filepath.Join(dir, "broker.wal.fwb"), wal, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewBinaryFileWALStoreWithCodec(dir, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	messages, err := store.ListQueue(context.Background(), "legacy-q", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ID != "legacy-1" {
		t.Fatalf("legacy recovery failed: %+v", messages)
	}
}

func BenchmarkWALRecordCodecAppendBatch50(b *testing.B) {
	codec, err := NewWALRecordCodec(WALCodecConfig{MaxRecordSize: 8 << 20})
	if err != nil {
		b.Fatal(err)
	}
	entries := makeQueueEntries("bench", 50, "")
	record := walRecord{Op: "append_atomic", At: time.Now().UTC(), Entries: entries}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame, err := codec.encode(nil, record)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkLegacyJSONWALRecordAppendBatch50(b *testing.B) {
	entries := makeQueueEntries("bench", 50, "")
	record := walRecord{Op: "append_atomic", At: time.Now().UTC(), Entries: entries}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkWALRecordCodecAckBatch256(b *testing.B) {
	codec, err := NewWALRecordCodec(WALCodecConfig{MaxRecordSize: 8 << 20})
	if err != nil {
		b.Fatal(err)
	}
	acks := make([]AckRequest, 256)
	for i := range acks {
		acks[i] = AckRequest{MessageID: "message-000000000000", Queue: "bench", LeaseToken: "lease-000000000000", ConsumerID: "consumer"}
	}
	record := walRecord{Op: "ack_batch", At: time.Now().UTC(), Acks: acks}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame, err := codec.encode(nil, record)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func TestWALRecordCodecBatchSizeReduction(t *testing.T) {
	codec, err := NewWALRecordCodec(WALCodecConfig{MaxRecordSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	record := walRecord{Op: "append_atomic", At: time.Now().UTC(), Entries: makeQueueEntries("bench", 50, "")}
	binaryRecord, err := codec.encode(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	jsonRecord, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("BWL1=%d bytes legacy JSON=%d bytes reduction=%.1f%%", len(binaryRecord), len(jsonRecord), 100*(1-float64(len(binaryRecord))/float64(len(jsonRecord))))
	if len(binaryRecord)*2 >= len(jsonRecord) {
		t.Fatalf("expected native record to be less than half the JSON size: binary=%d json=%d", len(binaryRecord), len(jsonRecord))
	}
}
