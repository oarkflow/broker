package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18091", "HTTP listen address")
	data := flag.String("data", "./.broker-data/complete", "durable broker data directory")
	token := flag.String("token", "complete-example-token", "API bearer token")
	flag.Parse()
	if err := run(*addr, *data, *token); err != nil {
		fmt.Fprintln(os.Stderr, "broker failed:", err)
		os.Exit(1)
	}
}

func run(addr, data, token string) error {
	store, err := broker.NewFileWALStore(filepath.Join(data, "wal"))
	if err != nil {
		return err
	}
	b, err := broker.New(broker.Config{
		Name: "complete-server", Mode: broker.ModeStandalone, Store: store,
		PerformanceProfile: broker.ProfileDurable, PollInterval: 5 * time.Millisecond,
		EnableNotifications: true, EnableRequestResponse: true, EnableTaskTracking: true,
	})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := b.Start(ctx); err != nil {
		return err
	}
	for _, queue := range []string{"complete.jobs", "complete.jobs.dlq"} {
		if err := b.DeclareQueue(ctx, broker.QueueConfig{Name: queue, VisibilityTimeout: 30 * time.Second}); err != nil {
			return err
		}
	}
	if err := b.DeclareTopic(ctx, broker.TopicConfig{Name: "complete.events", Partitions: 1}); err != nil {
		return err
	}
	if err := b.DeclareStream(ctx, broker.StreamConfig{Name: "complete.stream", Partitions: 1}); err != nil {
		return err
	}
	admin := broker.AdminServer{Broker: b, Token: token, MaxBodyBytes: 1 << 20}
	server := admin.HTTPServer(addr)
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	fmt.Printf("complete broker listening addr=%s data=%s\n", addr, data)
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	return errors.Join(server.Shutdown(stopCtx), b.Stop(stopCtx))
}
