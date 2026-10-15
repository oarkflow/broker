package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
)

func main() {
	dir := flag.String("dir", ".outbox-data", "application database and broker WAL directory")
	flag.Parse()
	if err := run(*dir); err != nil {
		log.Fatal(err)
	}
}

func run(dir string) error {
	ctx := context.Background()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "application.db")+"?_pragma=synchronous(FULL)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	outbox, err := sqlitestore.NewOutbox(db)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	id := rand.Text()
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders(id,status) VALUES(?,?)`, id, "created"); err != nil {
		return err
	}
	message := broker.Message{ID: "order-created-" + id, Topic: "orders.created", Payload: []byte(fmt.Sprintf(`{"order_id":%q}`, id)), ContentType: "application/json"}
	if err := outbox.EnqueueTx(ctx, tx, message, []broker.Destination{{Kind: broker.DestinationQueue, Name: "orders"}}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	target, err := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: filepath.Join(dir, "broker-wal")})
	if err != nil {
		return err
	}
	defer target.Close()
	n, err := outbox.Dispatch(ctx, target, sqlitestore.OutboxDispatchConfig{})
	if err != nil {
		return err
	}
	fmt.Printf("committed order %s; dispatched %d pending events to durable broker storage\n", id, n)
	return nil
}
