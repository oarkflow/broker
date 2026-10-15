package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
)

func streamExample(ctx context.Context, dir string) error {
	stores := []struct {
		name string
		open func(string) (broker.Store, error)
	}{
		{"memory", func(string) (broker.Store, error) { return broker.NewMemoryStore(), nil }},
		{"json-wal", func(dir string) (broker.Store, error) { return broker.NewFileWALStore(dir) }},
		{"binary-wal", func(dir string) (broker.Store, error) { return broker.NewBinaryFileWALStore(dir) }},
		{"segmented-wal", func(dir string) (broker.Store, error) {
			return broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: dir, DisableCompactor: true})
		}},
		{"sharded-wal", func(dir string) (broker.Store, error) { return broker.NewPartitionedBinaryWALStore(dir, 4, nil) }},
		{"sqlite", func(dir string) (broker.Store, error) {
			return sqlitestore.OpenMessageStore("sqlite", filepath.Join(dir, "messages.db"))
		}},
	}
	for _, kind := range stores {
		path := filepath.Join(dir, kind.name)
		// SQLite needs its parent directory to exist before opening the database.
		if err := makeDirectory(path); err != nil {
			return err
		}
		store, err := kind.open(path)
		if err != nil {
			return err
		}
		if err := withBroker(ctx, store, func(b *broker.Broker) error {
			if err := b.DeclareStream(ctx, broker.StreamConfig{Name: "input"}); err != nil {
				return err
			}
			for i := 0; i < 2; i++ {
				if _, err := b.Publisher().Publish(ctx, "input", "order", broker.WithStream("input"), broker.WithTenantID("tenant-a")); err != nil {
					return err
				}
			}
			position := broker.OffsetCommit{Stream: "input", ConsumerGroup: "billing", Offset: 1, TenantID: "tenant-a"}
			// Mixing a standalone commit with a later transaction preserves tenant scope on restart.
			if err := b.CommitOffset(ctx, position); err != nil {
				return err
			}
			position.Offset = 2
			tx, err := b.Publisher().BeginStreamTx(ctx, position, 1)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
			// Unkeyed queue outputs co-locate with the input on the sharded store.
			if err := tx.Publish("billed", "output", broker.WithQueue("jobs"), broker.WithMessageID("out")); err != nil {
				return err
			}
			if _, err := tx.Commit(ctx); err != nil {
				return err
			}
			stale, err := b.Publisher().BeginStreamTx(ctx, position, 1)
			if err != nil {
				return err
			}
			_, err = stale.Commit(ctx)
			if err := expectError(err, broker.ErrOffsetConflict); err != nil {
				return err
			}
			return verifyStreamState(ctx, b.Store())
		}); err != nil {
			return fmt.Errorf("%s: %w", kind.name, err)
		}
		if kind.name != "memory" {
			reopened, err := kind.open(path)
			if err != nil {
				return err
			}
			if err := withBroker(ctx, reopened, func(b *broker.Broker) error { return verifyStreamState(ctx, b.Store()) }); err != nil {
				return err
			}
		}
		suffix := ""
		if kind.name != "memory" {
			suffix = ", restart verified"
		}
		fmt.Printf("  %s: output + tenant offset committed, stale commit rejected%s\n", kind.name, suffix)
	}
	return nil
}

func verifyStreamState(ctx context.Context, store broker.Store) error {
	for _, tenant := range []string{"tenant-a", "tenant-b", ""} {
		got, err := store.ConsumerOffset(ctx, broker.OffsetCommit{Stream: "input", ConsumerGroup: "billing", TenantID: tenant})
		want := uint64(0)
		if tenant == "tenant-a" {
			want = 2
		}
		if err != nil || got != want {
			return fmt.Errorf("tenant %q offset=%d, want=%d, error=%v", tenant, got, want, err)
		}
	}
	message, exists, err := store.Get(ctx, "out")
	if err != nil || !exists || message.TenantID != "tenant-a" {
		return fmt.Errorf("transaction output=%+v, exists=%v, error=%v", message, exists, err)
	}
	return nil
}
