package sqlitestore

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/oarkflow/broker"
)

func TestSQLiteStorePersistsTransactionally(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "messages.db")
	store, err := OpenMessageStore("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, broker.Message{ID: "m1", TenantID: "tenant-a", Payload: []byte("payload")}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMessageStore("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	message, ok, err := reopened.Get(ctx, "m1")
	if err != nil || !ok || message.TenantID != "tenant-a" {
		t.Fatalf("message=%+v ok=%t err=%v", message, ok, err)
	}
}

func TestSQLiteFailureRollsBackHotIndex(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMessageStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Append(ctx, broker.Message{ID: "ghost"}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err == nil {
		t.Fatal("append unexpectedly succeeded after database close")
	}
	if _, ok, err := store.Get(ctx, "ghost"); err != nil || ok {
		t.Fatalf("failed transaction remained visible: ok=%t err=%v", ok, err)
	}
}
