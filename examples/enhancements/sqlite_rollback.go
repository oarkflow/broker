package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
)

func sqliteRollbackExample(ctx context.Context, dir string) error {
	if err := makeDirectory(dir); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "messages.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := sqlitestore.NewMessageStore(db)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := store.Append(ctx, broker.Message{ID: "input"}, []broker.Destination{{Kind: broker.DestinationStream, Name: "input"}}); err != nil {
		return err
	}
	if _, err := store.Append(ctx, broker.Message{ID: "leased"}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err != nil {
		return err
	}
	leased, err := store.Lease(ctx, broker.LeaseRequest{Queue: "jobs", ConsumerID: "worker", Limit: 1, VisibilityTimeout: time.Hour})
	if err != nil || len(leased) != 1 {
		return fmt.Errorf("lease=%v, error=%v", leased, err)
	}
	// Deliberately close this disposable database to demonstrate persistence failure.
	if err := db.Close(); err != nil {
		return err
	}
	offset := broker.OffsetCommit{Stream: "input", ConsumerGroup: "billing", Offset: 1}
	out, err := store.CommitStreamTransaction(ctx, broker.StreamTransaction{Offset: offset, Entries: []broker.AppendEntry{{Message: broker.Message{ID: "ghost"}, Destinations: []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}}}})
	if err == nil || len(out) != 0 {
		return fmt.Errorf("failed database unexpectedly committed: output=%v, error=%v", out, err)
	}
	if _, exists, err := store.Get(ctx, "ghost"); err != nil || exists {
		return fmt.Errorf("failed output exists=%v, error=%v", exists, err)
	}
	if got, err := store.ConsumerOffset(ctx, offset); err != nil || got != 0 {
		return fmt.Errorf("failed transaction offset=%d, error=%v", got, err)
	}
	message, exists, err := store.Get(ctx, "leased")
	if err != nil || !exists || message.Status != broker.MessageLeased || message.LeaseToken != leased[0].LeaseToken {
		return fmt.Errorf("unrelated lease changed: %+v, error=%v", message, err)
	}
	fmt.Println("  Closed-database write rejected: no output, unchanged offset, original lease preserved")
	return nil
}
