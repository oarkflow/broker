package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func openOutboxDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func enqueueOutbox(t *testing.T, db *sql.DB, outbox *Outbox, id string) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := outbox.EnqueueTx(context.Background(), tx, broker.Message{ID: id, Payload: []byte("payload")}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxApplicationTransactionAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")
	db := openOutboxDB(t, path)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE orders(id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		id := fmt.Sprint(commit)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO orders(id) VALUES(?)`, id); err != nil {
			t.Fatal(err)
		}
		if err := o.EnqueueTx(ctx, tx, broker.Message{ID: id, Payload: []byte("order created")}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "orders"}}); err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM orders WHERE id=?`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		_, exists, err := o.GetEntry(ctx, id)
		if err != nil || exists != commit || (count == 1) != commit {
			t.Fatalf("commit=%v: outbox exists=%v, orders=%d, error=%v", commit, exists, count, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openOutboxDB(t, path)
	o, err = NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	target, err := broker.NewBinaryFileWALStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	n, err := o.Dispatch(ctx, target, OutboxDispatchConfig{})
	if err != nil || n != 1 {
		t.Fatalf("dispatch after restart: count=%d, error=%v", n, err)
	}
	entry, exists, err := o.GetEntry(ctx, "true")
	if err != nil || !exists || entry.Status != broker.OutboxPublished || entry.Attempts != 1 || entry.PublishedAt == nil {
		t.Fatalf("dispatch state: %+v, exists=%v, error=%v", entry, exists, err)
	}
}

func TestOutboxRecoversAppendBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")
	db := openOutboxDB(t, path)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	enqueueOutbox(t, db, o, "event")
	claim, ok, err := o.claim(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim: %v, %v", ok, err)
	}
	entry, _, err := o.GetEntry(ctx, "event")
	if err != nil {
		t.Fatal(err)
	}
	walDir := t.TempDir()
	target, err := broker.NewBinaryFileWALStore(walDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Append(ctx, entry.Message, entry.Destinations); err != nil {
		t.Fatal(err)
	}
	// Model a process dying between append and finish, then lease expiration.
	if _, err := db.Exec(`UPDATE fw_transactional_outbox SET lease_until=0 WHERE id=? AND lease_token=?`, claim.id, claim.token); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(db.Close(), target.Close()); err != nil {
		t.Fatal(err)
	}
	db = openOutboxDB(t, path)
	o, err = NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	target, err = broker.NewBinaryFileWALStore(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if n, err := o.Dispatch(ctx, target, OutboxDispatchConfig{}); err != nil || n != 1 {
		t.Fatalf("recovered dispatch: count=%d, error=%v", n, err)
	}
	messages, err := target.ListQueue(ctx, "jobs", 10)
	if err != nil || len(messages) != 1 {
		t.Fatalf("retry duplicated message: count=%d, error=%v", len(messages), err)
	}
	entry, _, err = o.GetEntry(ctx, "event")
	if err != nil || entry.Attempts != 2 || entry.Status != broker.OutboxPublished {
		t.Fatalf("recovered entry: %+v, error=%v", entry, err)
	}
}

func TestOutboxConcurrentWorkersAndLeaseFencing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")
	db := openOutboxDB(t, path)
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		enqueueOutbox(t, db, o, fmt.Sprint(i))
	}
	target := broker.NewMemoryStore()
	defer target.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		// Independent DB handles model separate worker processes.
		workerDB := openOutboxDB(t, path)
		workerOutbox, err := NewOutbox(workerDB)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			if _, err := workerOutbox.Dispatch(ctx, target, OutboxDispatchConfig{AllowVolatileTarget: true}); err != nil {
				t.Errorf("dispatch: %v", err)
			}
		})
	}
	wg.Wait()
	messages, err := target.ListQueue(ctx, "jobs", 100)
	if err != nil || len(messages) != 30 {
		t.Fatalf("concurrent dispatch: count=%d, error=%v", len(messages), err)
	}
	enqueueOutbox(t, db, o, "fenced")
	old, ok, err := o.claim(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatal("missing old claim", err)
	}
	if _, err := db.Exec(`UPDATE fw_transactional_outbox SET lease_until=0 WHERE id=?`, old.id); err != nil {
		t.Fatal(err)
	}
	current, ok, err := o.claim(ctx, time.Minute)
	if err != nil || !ok || current.token == old.token {
		t.Fatal("missing replacement claim", err)
	}
	if err := o.finish(ctx, old, nil); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("stale worker completed replacement claim: %v", err)
	}
	if err := o.finish(ctx, current, nil); err != nil {
		t.Fatal(err)
	}
}

type unavailableOutboxTarget struct{ *broker.MemoryStore }

func (*unavailableOutboxTarget) Append(context.Context, broker.Message, []broker.Destination) ([]broker.Message, error) {
	return nil, broker.ErrStorageUnavailable
}

func TestOutboxRetainsFailedDispatchAndRejectsOverwrite(t *testing.T) {
	ctx := context.Background()
	db := openOutboxDB(t, filepath.Join(t.TempDir(), "app.db"))
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	enqueueOutbox(t, db, o, "event")
	target := &unavailableOutboxTarget{broker.NewMemoryStore()}
	defer target.Close()
	if n, err := o.Dispatch(ctx, target, OutboxDispatchConfig{AllowVolatileTarget: true}); n != 0 || !errors.Is(err, broker.ErrStorageUnavailable) {
		t.Fatalf("failed dispatch: count=%d, error=%v", n, err)
	}
	entry, _, err := o.GetEntry(ctx, "event")
	if err != nil || entry.Status != broker.OutboxFailed || entry.Attempts != 1 || entry.LastError == "" {
		t.Fatalf("failure was not retained: %+v, error=%v", entry, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = o.EnqueueTx(ctx, tx, broker.Message{ID: "event"}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}})
	tx.Rollback()
	if err == nil {
		t.Fatal("duplicate ID overwrote original entry")
	}
	if n, err := o.Dispatch(ctx, target.MemoryStore, OutboxDispatchConfig{AllowVolatileTarget: true}); err != nil || n != 1 {
		t.Fatalf("retry: count=%d, error=%v", n, err)
	}
}

func TestOutboxRejectsVolatileTargetBeforeClaim(t *testing.T) {
	db := openOutboxDB(t, filepath.Join(t.TempDir(), "app.db"))
	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	enqueueOutbox(t, db, o, "event")
	target := broker.NewMemoryStore()
	defer target.Close()
	if _, err := o.Dispatch(context.Background(), target, OutboxDispatchConfig{}); !errors.Is(err, broker.ErrUnsupportedMode) {
		t.Fatalf("volatile target accepted without opt-in: %v", err)
	}
	entry, _, err := o.GetEntry(context.Background(), "event")
	if err != nil || entry.Attempts != 0 || entry.Status != broker.OutboxPending {
		t.Fatalf("rejected dispatch changed entry: %+v, error=%v", entry, err)
	}
}
