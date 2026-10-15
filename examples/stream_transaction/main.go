package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/broker"
)

func main() {
	dir := flag.String("dir", ".stream-transaction-data", "durable broker WAL directory")
	flag.Parse()
	if err := run(*dir); err != nil {
		log.Fatal(err)
	}
}

func run(dir string) error {
	ctx := context.Background()
	store, err := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: dir})
	if err != nil {
		return err
	}
	b, err := broker.New(broker.Config{Store: store})
	if err != nil {
		store.Close()
		return err
	}
	defer b.Stop(ctx)
	if err := b.Start(ctx); err != nil {
		return err
	}
	if err := b.DeclareStream(ctx, broker.StreamConfig{Name: "input"}); err != nil {
		return err
	}
	if _, err := b.Publisher().Publish(ctx, "input", []byte("process me"), broker.WithStream("input")); err != nil {
		return err
	}
	commit := broker.OffsetCommit{Stream: "input", ConsumerGroup: "processor"}
	previous, err := b.ConsumerOffset(ctx, commit)
	if err != nil {
		return err
	}
	messages, err := b.ReadStream(ctx, broker.StreamReadRequest{Stream: commit.Stream, ConsumerGroup: commit.ConsumerGroup, Limit: 100})
	if err != nil || len(messages) == 0 {
		return err
	}
	commit.Offset = messages[len(messages)-1].Offset
	tx, err := b.Publisher().BeginStreamTx(ctx, commit, previous)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, message := range messages {
		output := []byte(strings.ToUpper(string(message.Payload)))
		if err := tx.Publish("processed", output, broker.WithStream("output")); err != nil {
			return err
		}
	}
	results, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("atomically committed input offset %d -> %d and %d output records\n", previous, commit.Offset, len(results))
	return nil
}
