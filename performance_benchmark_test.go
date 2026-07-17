package broker

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkBinaryWALPublishBatch50(b *testing.B) {
	store, err := NewBinaryFileWALStoreWithCodec(b.TempDir(), NewFrameCodec(FrameCodecConfig{Type: CodecFWB1, MaxFrameSize: 128 << 20}))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	store.SetHistoryLimit(0)
	store.SetRetainCompleted(false)
	if err := store.DeclareQueue(context.Background(), QueueConfig{Name: "bench"}); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	entries := make([]AppendEntry, 50)
	for i := range entries {
		entries[i].Destinations = []Destination{{Kind: DestinationQueue, Name: "bench"}}
		entries[i].Message.Topic = "bench"
		entries[i].Message.Payload = payload
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		prefix := strconv.Itoa(n) + "-"
		for i := range entries {
			entries[i].Message.ID = prefix + strconv.Itoa(i)
		}
		if err := store.AppendAtomicOwned(context.Background(), entries); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(entries))/b.Elapsed().Seconds(), "msg/s")
}
