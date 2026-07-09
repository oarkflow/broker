package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/fastworker/broker"
	"github.com/oarkflow/fastworker/examples/internal/exconfig"
	"github.com/oarkflow/fastworker/examples/internal/exobserve"
)

func main() {
	configPath := flag.String("config", "examples/complete_broker/broker.bcl", "BCL broker configuration")
	flag.Parse()

	root, err := exconfig.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	bc := exconfig.FirstBlock(root, "broker")
	if bc == nil {
		log.Fatal("missing broker block")
	}

	frameCodec := exconfig.FrameCodec(bc)
	var store broker.Store
	if wal := bc.String("wal", ""); wal != "" {
		format := strings.ToLower(bc.String("wal_format", "binary"))
		if format == "json" || format == "jsonl" {
			st, err := broker.NewFileWALStore(wal)
			if err != nil {
				log.Fatalf("open JSON WAL store: %v", err)
			}
			store = st
		} else {
			st, err := broker.NewBinaryFileWALStore(wal, exconfig.FrameCodecOptions(bc)...)
			if err != nil {
				log.Fatalf("open binary WAL store: %v", err)
			}
			store = st
		}
	}

	obsSink, err := exobserve.NewJSONLSink(bc.String("observer_jsonl", "./.broker-data/observability/broker-events.jsonl"))
	if err != nil {
		log.Fatalf("open observability sink: %v", err)
	}
	defer obsSink.Close()
	observer := exobserve.BrokerObserver{Prefix: "complete-broker", Sink: obsSink}

	b, err := broker.New(broker.Config{
		Name:                  first(bc.Name, "complete"),
		Mode:                  broker.ModeStandalone,
		Store:                 store,
		FrameCodec:            frameCodec,
		DefaultQueue:          bc.String("default_queue", "default"),
		DefaultVisibility:     bc.Duration("default_visibility", 30*time.Second),
		RemoteConsumerTimeout: bc.Duration("remote_consumer_timeout", 45*time.Second),
		EnableNotifications:   true,
		EnableRequestResponse: true,
		EnableTaskTracking:    true,
		Hooks:                 observer.Hooks(),
	})
	if err != nil {
		log.Fatalf("create broker: %v", err)
	}
	if bc.Bool("log_events", true) {
		broker.LogNotifications(b.Notifications(), "complete-broker", bc.Bool("debug_events", false), log.Printf)
	}
	for _, cb := range bc.Blocks("callback") {
		if !cb.Bool("enabled", true) {
			continue
		}
		name := first(cb.Name, "observer")
		b.Notifications().Subscribe("example-"+name, observer.NotificationCallback(name))
		log.Printf("configured notification callback=%s sink=%s", name, bc.String("observer_jsonl", "./.broker-data/observability/broker-events.jsonl"))
	}
	if err := b.Start(context.Background()); err != nil {
		log.Fatalf("start broker: %v", err)
	}

	declareConfiguredResources(context.Background(), b, bc)

	tokens := buildTokens(bc)
	admin := broker.AdminServer{
		Broker:       b,
		ACL:          broker.NewACL(tokens...),
		InsecureDev:  bc.Bool("insecure_dev", false),
		MaxBodyBytes: exconfig.ParseBytes(bc.String("max_body", "1MB"), 1<<20),
	}
	addr := bc.String("addr", ":8090")
	srv := admin.HTTPServer(addr)
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen %s: %v", addr, err)
		}
	case <-time.After(200 * time.Millisecond):
	}
	fmt.Printf("complete broker listening on %s config=%s\n", addr, *configPath)
	fmt.Println("data-plane: /v1, admin: /queues /topics /streams /dlq /routes, consumer discovery: /v1/consumers, broker-sent events: /v1/consumers/{id}/events")
	if admin.InsecureDev {
		fmt.Println("WARNING: insecure_dev enabled by BCL; use tokens/TLS for production")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = b.Stop(ctx)
}

func declareConfiguredResources(ctx context.Context, b *broker.Broker, bc *exconfig.Node) {
	for _, q := range bc.Blocks("queue") {
		cfg := broker.QueueConfig{
			Name:              q.Name,
			VisibilityTimeout: q.Duration("visibility_timeout", 30*time.Second),
			MaxDepth:          q.Int("max_depth", 0),
			DLQ:               q.String("dlq", q.Name+".dlq"),
			Backpressure:      broker.BackpressurePolicy(q.String("backpressure", string(broker.BackpressureReject))),
			RetryPolicy:       broker.RetryPolicy{MaxAttempts: q.Int("max_attempts", 3), Initial: 100 * time.Millisecond, Max: 5 * time.Second, Factor: 2, Jitter: true},
		}
		if err := b.DeclareQueue(ctx, cfg); err != nil {
			log.Fatalf("declare queue %s: %v", q.Name, err)
		}
		log.Printf("configured queue=%s dlq=%s", cfg.Name, cfg.DLQ)
	}
	for _, t := range bc.Blocks("topic") {
		if err := b.DeclareTopic(ctx, broker.TopicConfig{Name: t.Name, Partitions: t.Int("partitions", 1), Retention: t.Duration("retention", 24*time.Hour)}); err != nil {
			log.Fatalf("declare topic %s: %v", t.Name, err)
		}
		log.Printf("configured topic=%s", t.Name)
	}
	for _, s := range bc.Blocks("stream") {
		if err := b.DeclareStream(ctx, broker.StreamConfig{Name: s.Name, Partitions: s.Int("partitions", 1), Retention: s.Duration("retention", 24*time.Hour)}); err != nil {
			log.Fatalf("declare stream %s: %v", s.Name, err)
		}
		log.Printf("configured stream=%s", s.Name)
	}
	for _, r := range bc.Blocks("route") {
		route := broker.Route{Name: r.Name, Mode: broker.RouteTopic, Match: r.String("match", ""), Enabled: true}
		if to := r.Strings("to"); len(to) >= 2 {
			route.To = append(route.To, destination(to[0], to[1]))
		}
		if cto := r.Strings("copy_to"); len(cto) >= 2 {
			route.CopyTo = append(route.CopyTo, destination(cto[0], cto[1]))
		}
		b.AddRoute(route)
		log.Printf("configured route=%s match=%s to=%v copy_to=%v", route.Name, route.Match, route.To, route.CopyTo)
	}
}

func destination(kind, name string) broker.Destination {
	switch strings.ToLower(kind) {
	case "queue":
		return broker.Destination{Kind: broker.DestinationQueue, Name: name}
	case "topic":
		return broker.Destination{Kind: broker.DestinationTopic, Name: name}
	case "stream":
		return broker.Destination{Kind: broker.DestinationStream, Name: name}
	case "dlq":
		return broker.Destination{Kind: broker.DestinationDLQ, Name: name}
	default:
		return broker.Destination{Kind: broker.DestinationQueue, Name: name}
	}
}

func buildTokens(bc *exconfig.Node) []broker.AdminToken {
	var out []broker.AdminToken
	for _, t := range bc.Blocks("token") {
		value := t.String("value", "")
		if value == "" {
			continue
		}
		out = append(out, broker.AdminToken{Name: first(t.Name, "token"), Token: value, Permissions: t.Strings("permissions")})
	}
	return out
}

func first(v, d string) string {
	if v != "" {
		return v
	}
	return d
}
