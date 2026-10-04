package broker

import "testing"

// MemoryStore.Stats reported 0 retained messages for every topic, because the
// topic branch reused the stream-lag counter, which requires Offset > after.
// Topic messages carry no stream offset, so with after=0 nothing ever counted,
// and the count disagreed with ListTopic on the same store. A topic holding five
// messages rendered as zero in the operator console, which reads this field.
func TestStatsTopicCountMatchesListTopic(t *testing.T) {
	cases := map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store { return NewMemoryStore() },
		"partitioned": func(t *testing.T) Store {
			st, err := NewPartitionedBinaryWALStore(t.TempDir(), 4, NewFrameCodec(FrameCodecConfig{Type: CodecFWB1}))
			if err != nil {
				t.Fatal(err)
			}
			return st
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			store := mk(t)
			ctx := t.Context()
			b := MustNew(Config{Store: store})
			if err := b.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer b.Stop(ctx)

			const published = 5
			for i := 0; i < published; i++ {
				if _, err := b.Publisher().Publish(ctx, "events", []byte("x"), nil); err != nil {
					t.Fatal(err)
				}
			}

			msgs, err := store.ListTopic(ctx, "events", 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != published {
				t.Fatalf("ListTopic returned %d, want %d", len(msgs), published)
			}

			st, err := store.Stats(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := st.Topics["events"]; got != published {
				t.Errorf("Stats().Topics[events] = %d, ListTopic returned %d", got, len(msgs))
			}
		})
	}
}
