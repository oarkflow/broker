package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
)

// appendThenLoseAck models an ambiguous failure after the target persisted a message.
// It is an example fixture, not a production store implementation.
type appendThenLoseAck struct{ broker.Store }

func (appendThenLoseAck) ConfirmationLevel() broker.ConfirmLevel { return broker.ConfirmPersisted }
func (s appendThenLoseAck) Append(ctx context.Context, message broker.Message, dests []broker.Destination) ([]broker.Message, error) {
	if _, err := s.Store.Append(ctx, message, dests); err != nil {
		return nil, err
	}
	return nil, broker.ErrStorageUnavailable
}

func openApplicationDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=synchronous(FULL)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err == nil {
		db.SetMaxOpenConns(1)
	}
	return db, err
}

func enqueueOrder(ctx context.Context, db *sql.DB, outbox *sqlitestore.Outbox, id string, commit bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders(id) VALUES(?)`, id); err != nil {
		return err
	}
	if err := outbox.EnqueueTx(ctx, tx, broker.Message{ID: id, Payload: []byte("order created")}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}); err != nil {
		return err
	}
	if !commit {
		return tx.Rollback()
	}
	return tx.Commit()
}

func outboxExample(ctx context.Context, dir string) error {
	if err := makeDirectory(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, "application.db")
	db, err := openApplicationDB(path)
	if err != nil {
		return err
	}
	defer func() { db.Close() }()
	o, err := sqlitestore.NewOutbox(db)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE orders(id TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	if err := enqueueOrder(ctx, db, o, "rolled-back", false); err != nil {
		return err
	}
	if _, exists, err := o.GetEntry(ctx, "rolled-back"); err != nil || exists {
		return fmt.Errorf("rolled-back outbox entry exists=%v, error=%v", exists, err)
	}
	if err := enqueueOrder(ctx, db, o, "committed", true); err != nil {
		return err
	}
	// Reopen before dispatch: pending intent belongs to the database, not the worker.
	if err := db.Close(); err != nil {
		return err
	}
	db, err = openApplicationDB(path)
	if err != nil {
		return err
	}
	o, err = sqlitestore.NewOutbox(db)
	if err != nil {
		return err
	}
	target, err := broker.NewBinaryFileWALStore(filepath.Join(dir, "broker-wal"))
	if err != nil {
		return err
	}
	defer target.Close()
	_, err = o.Dispatch(ctx, appendThenLoseAck{Store: target}, sqlitestore.OutboxDispatchConfig{})
	if err := expectError(err, broker.ErrStorageUnavailable); err != nil {
		return err
	}
	entry, exists, err := o.GetEntry(ctx, "committed")
	if err != nil || !exists || entry.Status != broker.OutboxFailed || entry.Attempts != 1 {
		return fmt.Errorf("failed dispatch state=%+v, error=%v", entry, err)
	}
	if n, err := o.Dispatch(ctx, target, sqlitestore.OutboxDispatchConfig{}); err != nil || n != 1 {
		return fmt.Errorf("retry completed=%d, error=%v", n, err)
	}
	entry, _, err = o.GetEntry(ctx, "committed")
	if err != nil || entry.Status != broker.OutboxPublished || entry.Attempts != 2 {
		return fmt.Errorf("retried entry=%+v, error=%v", entry, err)
	}
	volatile := broker.NewMemoryStore()
	defer volatile.Close()
	_, err = o.Dispatch(ctx, volatile, sqlitestore.OutboxDispatchConfig{})
	if err := expectError(err, broker.ErrUnsupportedMode); err != nil {
		return err
	}
	fmt.Println("  Application rollback removes intent; committed intent survives reopening")
	fmt.Println("  Lost append acknowledgement retried safely: published status, attempts=2; volatile target rejected")

	for i := 0; i < 8; i++ {
		if err := enqueueOrder(ctx, db, o, fmt.Sprintf("worker-%d", i), true); err != nil {
			return err
		}
	}
	type result struct {
		n   int
		err error
	}
	results := make(chan result, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		workerDB, err := openApplicationDB(path)
		if err != nil {
			return err
		}
		defer workerDB.Close()
		worker, err := sqlitestore.NewOutbox(workerDB)
		if err != nil {
			return err
		}
		wg.Go(func() {
			n, err := worker.Dispatch(ctx, target, sqlitestore.OutboxDispatchConfig{BatchSize: 8})
			results <- result{n, err}
		})
	}
	wg.Wait()
	close(results)
	completed := 0
	for result := range results {
		if result.err != nil {
			return result.err
		}
		completed += result.n
	}
	messages, err := target.ListQueue(ctx, "jobs", 100)
	if err != nil || completed != 8 || len(messages) != 9 {
		return fmt.Errorf("workers completed=%d, retained=%d, error=%v", completed, len(messages), err)
	}
	fmt.Println("  Three workers with independent database handles dispatch eight distinct claims")
	return nil
}
