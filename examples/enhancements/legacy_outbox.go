package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/broker"
)

// unavailableStore is only a failure-injection fixture for this example.
type unavailableStore struct{ broker.Store }

func (unavailableStore) Append(context.Context, broker.Message, []broker.Destination) ([]broker.Message, error) {
	return nil, broker.ErrStorageUnavailable
}

func legacyOutboxExample(ctx context.Context, _ string) error {
	return withBroker(ctx, broker.NewMemoryStore(), func(b *broker.Broker) error {
		// Despite its compatibility name, this worker keeps pending entries in memory.
		worker, err := broker.NewDurableOutbox(broker.DurableOutboxConfig{Broker: b})
		if err != nil {
			return err
		}
		message := broker.Message{ID: "owned", Payload: []byte("original")}
		dests := []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}
		if _, err := worker.Enqueue(ctx, message, dests); err != nil {
			return err
		}
		message.Payload[0], dests[0].Name = 'X', "changed"
		if _, err := worker.Enqueue(ctx, broker.Message{ID: "owned"}, dests); err == nil {
			return fmt.Errorf("duplicate outbox ID overwrote an entry")
		}
		if err := worker.Flush(ctx); err != nil {
			return err
		}
		stored, exists, err := b.Store().Get(ctx, "owned")
		if err != nil || !exists || string(stored.Payload) != "original" || stored.Queue != "jobs" {
			return fmt.Errorf("outbox ownership=%+v, error=%v", stored, err)
		}
		for i := 0; i < 2; i++ {
			if err := worker.Start(); err != nil {
				return err
			}
			if err := worker.Stop(); err != nil {
				return err
			}
		}
		fmt.Println("  Legacy worker copies inputs, rejects duplicate IDs, and can restart safely (in memory only)")
		return withBroker(ctx, unavailableStore{Store: broker.NewMemoryStore()}, func(failed *broker.Broker) error {
			worker, err := broker.NewDurableOutbox(broker.DurableOutboxConfig{Broker: failed})
			if err != nil {
				return err
			}
			if _, err := worker.Enqueue(ctx, broker.Message{ID: "failed"}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err != nil {
				return err
			}
			if err := expectError(worker.Flush(ctx), broker.ErrStorageUnavailable); err != nil {
				return err
			}
			fmt.Println("  Flush returns its append failure instead of reporting success")
			return nil
		})
	})
}
