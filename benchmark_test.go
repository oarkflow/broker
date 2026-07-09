package broker

import (
	"context"
	"testing"
	"time"
)

func BenchmarkTopicMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !TopicMatch("tenant.*.invoice.#", "tenant.123.invoice.created") {
			b.Fatal("no match")
		}
	}
}

func BenchmarkPublishToQueueMemory(b *testing.B) {
	br := MustNew(Config{PollInterval: time.Millisecond})
	ctx := context.Background()
	if err := br.Start(ctx); err != nil {
		b.Fatal(err)
	}
	defer br.Stop(ctx)
	pub := br.Publisher()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pub.Publish(ctx, "bench", []byte(`{"ok":true}`), WithQueue("bench.q")); err != nil {
			b.Fatal(err)
		}
	}
}
