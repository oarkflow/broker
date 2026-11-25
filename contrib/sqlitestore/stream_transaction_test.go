package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func TestSQLiteStreamTransactionRestartAndRollback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMessageStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareStream(ctx, broker.StreamConfig{Name: "input"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"in-1", "in-2"} {
		if _, err := store.Append(ctx, broker.Message{ID: id}, []broker.Destination{{Kind: broker.DestinationStream, Name: "input"}}); err != nil {
			t.Fatal(err)
		}
	}
	tx := broker.StreamTransaction{
		Offset:  broker.OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1, TenantID: "tenant-a"},
		Entries: []broker.AppendEntry{{Message: broker.Message{ID: "out"}, Destinations: []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}}},
	}
	if _, err := store.CommitStreamTransaction(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMessageStore("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if offset, err := store.ConsumerOffset(ctx, tx.Offset); err != nil || offset != 1 {
		t.Fatalf("recovered offset=%d, error=%v", offset, err)
	}
	if _, ok, err := store.Get(ctx, "out"); err != nil || !ok {
		t.Fatalf("recovered output exists=%v, error=%v", ok, err)
	}
	if _, err := store.CommitStreamTransaction(ctx, tx); !errors.Is(err, broker.ErrOffsetConflict) {
		t.Fatalf("stale transaction accepted: %v", err)
	}
	leased, err := store.Lease(ctx, broker.LeaseRequest{Queue: "jobs", ConsumerID: "worker", Limit: 1, VisibilityTimeout: time.Hour})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease before failure: %v, %v", leased, err)
	}
	if err := store.db.Close(); err != nil {
		t.Fatal(err)
	}
	tx.ExpectedOffset = 1
	tx.Offset.Offset = 2
	tx.Entries[0].Message.ID = "ghost"
	if out, err := store.CommitStreamTransaction(ctx, tx); err == nil || len(out) != 0 {
		t.Fatalf("closed database commit output=%v, error=%v", out, err)
	}
	if offset, err := store.ConsumerOffset(ctx, tx.Offset); err != nil || offset != 1 {
		t.Fatalf("failed commit advanced offset=%d, error=%v", offset, err)
	}
	if _, ok, err := store.Get(ctx, "ghost"); err != nil || ok {
		t.Fatalf("failed commit exposed output=%v, error=%v", ok, err)
	}
	message, ok, err := store.Get(ctx, "out")
	if err != nil || !ok || message.Status != broker.MessageLeased || message.LeaseToken != leased[0].LeaseToken {
		t.Fatalf("failed transaction changed an unrelated lease: %+v, %v", message, err)
	}
}

var _ broker.StreamTransactionStore = (*MessageStore)(nil)
