// Command enhancements demonstrates the APIs and behavior changes listed in
// docs/ENHANCEMENT_PLAN.md. All scenarios terminate without an external server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/broker"
)

type scenario struct {
	name string
	run  func(context.Context, string) error
}

func main() {
	name := flag.String("scenario", "all", "all, routing, producer, request-reply, stream, stream-http, outbox, sqlite-rollback, legacy-outbox, wal")
	base := flag.String("dir", "", "optional parent directory for a retained, unique run directory")
	flag.Parse()
	if err := run(*name, *base); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(name, base string) error {
	scenarios := []scenario{
		{"routing", routingExample}, {"producer", producerExample},
		{"request-reply", requestReplyExample}, {"stream", streamExample},
		{"stream-http", streamHTTPExample}, {"outbox", outboxExample},
		{"sqlite-rollback", sqliteRollbackExample}, {"legacy-outbox", legacyOutboxExample},
		{"wal", walExample},
	}
	if base != "" {
		if err := os.MkdirAll(base, 0o700); err != nil {
			return err
		}
	}
	dir, err := os.MkdirTemp(base, "broker-examples-")
	if err != nil {
		return err
	}
	if base == "" {
		defer os.RemoveAll(dir)
	} else {
		fmt.Println("Retaining run data in", dir)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	found := false
	for _, s := range scenarios {
		if name != "all" && name != s.name {
			continue
		}
		found = true
		fmt.Printf("[%s]\n", s.name)
		if err := s.run(ctx, filepath.Join(dir, s.name)); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}
	if !found {
		return fmt.Errorf("unknown scenario %q; use -help to list scenarios", name)
	}
	return nil
}

// withBroker makes ownership and shutdown explicit, including on an example error.
func withBroker(ctx context.Context, store broker.Store, fn func(*broker.Broker) error) (err error) {
	b, err := broker.New(broker.Config{Store: store})
	if err != nil {
		return errors.Join(err, store.Close())
	}
	defer func() { err = errors.Join(err, b.Stop(context.Background())) }()
	if err := b.Start(ctx); err != nil {
		return err
	}
	return fn(b)
}

func expectError(err, want error) error {
	if !errors.Is(err, want) {
		return fmt.Errorf("expected %v, got %v", want, err)
	}
	return nil
}

func makeDirectory(path string) error { return os.MkdirAll(path, 0o700) }
