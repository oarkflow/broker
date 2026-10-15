package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/broker"
)

func producerExample(ctx context.Context, dir string) error {
	store, err := broker.NewPartitionedBinaryWALStore(dir, 4, nil)
	if err != nil {
		return err
	}
	var lastID string
	if err := withBroker(ctx, store, func(b *broker.Broker) error {
		for seq := uint64(0); seq < 2; seq++ {
			result, err := b.Publisher().Publish(ctx, "orders", []byte("order"), broker.WithQueue("jobs"), broker.WithKey(fmt.Sprint(seq)), broker.WithProducer("tenant-a/checkout", 1, seq))
			if err != nil {
				return err
			}
			lastID = result.MessageID
		}
		for _, attempt := range []struct {
			epoch int32
			seq   uint64
			want  error
		}{{1, 0, broker.ErrOutOfOrderSequence}, {1, 3, broker.ErrOutOfOrderSequence}, {0, 2, broker.ErrProducerFenced}} {
			_, err := b.Publisher().Publish(ctx, "orders", "invalid", broker.WithQueue("jobs"), broker.WithProducer("tenant-a/checkout", attempt.epoch, attempt.seq))
			if err := expectError(err, attempt.want); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	store, err = broker.NewPartitionedBinaryWALStore(dir, 4, nil)
	if err != nil {
		return err
	}
	if err := withBroker(ctx, store, func(b *broker.Broker) error {
		retry, err := b.Publisher().Publish(ctx, "orders", "retry", broker.WithQueue("jobs"), broker.WithKey("changed-after-restart"), broker.WithProducer("tenant-a/checkout", 1, 1))
		if err != nil || !retry.Duplicate || retry.MessageID != lastID {
			return fmt.Errorf("restart retry=%+v, error=%v", retry, err)
		}
		if _, err := b.Publisher().Publish(ctx, "orders", "new epoch", broker.WithQueue("jobs"), broker.WithProducer("tenant-a/checkout", 2, 0)); err != nil {
			return err
		}
		fmt.Println("  Changing keys preserves sequences; restart retry returns original ID; newer epoch accepted")
		return producerBatchAPIs(ctx, store)
	}); err != nil {
		return err
	}

	// The helper registry is in memory; durable sequencing above belongs to the store.
	registry := broker.NewProducerRegistry()
	snapshot, err := registry.RegisterProducer("helper", 2)
	if err != nil {
		return err
	}
	snapshot.Epoch = 999 // Mutating the returned snapshot does not change the registry.
	if _, _, err := registry.CheckAndRecord("helper", 2, 0, "first"); err != nil {
		return err
	}
	duplicate, original, err := registry.CheckAndRecord("helper", 2, 0, "retry")
	if err != nil || !duplicate || original != "first" {
		return fmt.Errorf("registry retry: duplicate=%v, id=%q, error=%v", duplicate, original, err)
	}
	fmt.Println("  Registry registration returns a snapshot; latest-sequence retry identifies its original message")
	return nil
}

func producerBatchAPIs(ctx context.Context, store *broker.PartitionedBinaryWALStore) error {
	entries := func(start uint64) []broker.AppendEntry {
		batch := make([]broker.AppendEntry, 2)
		for i := range batch {
			seq := start + uint64(i)
			batch[i] = broker.AppendEntry{Message: broker.Message{ID: fmt.Sprintf("batch-%d", seq), Key: fmt.Sprint(seq), Payload: []byte("owned batch payload"), ProducerID: "tenant-a/batches", ProducerEpoch: 1, ProducerSequence: &seq}, Destinations: []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}}
		}
		return batch
	}
	if _, err := store.AppendAtomic(ctx, entries(0)); err != nil {
		return err
	}
	if err := store.AppendAtomicMinimal(ctx, entries(2)); err != nil {
		return err
	}
	// Fresh buffers are handed over; do not reuse or mutate them after this call.
	if err := store.AppendAtomicOwned(ctx, entries(4)); err != nil {
		return err
	}
	fmt.Println("  Atomic, minimal and owned batches share the producer's sequence fence")
	return nil
}
