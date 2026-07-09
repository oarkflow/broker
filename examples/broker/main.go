package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
	"github.com/oarkflow/broker/examples/internal/exconfig"
	"github.com/oarkflow/broker/examples/internal/exobserve"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "broker/broker.bcl", "BCL broker configuration")
	flag.Parse()

	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	bc := exconfig.FirstBlock(root, "broker")
	if bc == nil {
		logger.Log.Fatal("missing broker block")
	}

	frameCodec := exconfig.FrameCodec(bc)
	var store broker.Store
	if ms := bc.Block("message_store", "sqlite"); ms != nil && ms.Bool("enabled", false) {
		driver := ms.String("driver", "sqlite")
		dsn := ms.String("dsn", "./.broker-data/broker-messages.sqlite")
		st, err := sqlitestore.OpenMessageStore(driver, dsn)
		if err != nil {
			logger.Log.Fatal("open sqlite message store", zlog.String("driver", driver), zlog.String("dsn", dsn), zlog.Err(err))
		}
		store = st
		logger.Log.Info("configured sqlite message store", zlog.String("driver", driver), zlog.String("dsn", dsn))
	} else if wal := bc.String("wal", ""); wal != "" {
		format := strings.ToLower(bc.String("wal_format", "binary"))
		if format == "json" || format == "jsonl" {
			st, err := broker.NewFileWALStore(wal)
			if err != nil {
				logger.Log.Fatal("open JSON WAL store", zlog.Err(err))
			}
			store = st
		} else {
			st, err := broker.NewBinaryFileWALStore(wal, exconfig.FrameCodecOptions(bc)...)
			if err != nil {
				logger.Log.Fatal("open binary WAL store", zlog.Err(err))
			}
			store = st
		}
	}

	var stateStore broker.StateStore
	if ss := bc.Block("state_store", "sqlite"); ss != nil && ss.Bool("enabled", false) {
		driver := ss.String("driver", "sqlite")
		dsn := ss.String("dsn", "./.broker-data/broker-state.sqlite")
		st, err := sqlitestore.Open(driver, dsn)
		if err != nil {
			logger.Log.Fatal("open sqlite state store", zlog.String("driver", driver), zlog.String("dsn", dsn), zlog.Err(err))
		}
		stateStore = st
		logger.Log.Info("configured sqlite state_store", zlog.String("driver", driver), zlog.String("dsn", dsn))
	}

	obsSink, err := exobserve.NewJSONLSink(bc.String("observer_jsonl", "./.broker-data/observability/broker-events.jsonl"))
	if err != nil {
		logger.Log.Fatal("open observability sink", zlog.Err(err))
	}
	defer obsSink.Close()
	observer := exobserve.BrokerObserver{Prefix: "complete-broker", Sink: obsSink}

	b, err := broker.New(broker.Config{
		Name:                  first(bc.Name, "complete"),
		Mode:                  broker.ModeStandalone,
		Store:                 store,
		StateStore:            stateStore,
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
		logger.Log.Fatal("create broker", zlog.Err(err))
	}
	if bc.Bool("log_events", true) {
		broker.LogNotifications(b.Notifications(), "complete-broker", bc.Bool("debug_events", false))
	}
	for _, cb := range bc.Blocks("callback") {
		if !cb.Bool("enabled", true) {
			continue
		}
		name := first(cb.Name, "observer")
		b.Notifications().Subscribe("example-"+name, observer.NotificationCallback(name))
		logger.Log.Info("configured notification callback", zlog.String("name", name), zlog.String("sink", bc.String("observer_jsonl", "./.broker-data/observability/broker-events.jsonl")))
	}
	if err := b.Start(context.Background()); err != nil {
		logger.Log.Fatal("start broker", zlog.Err(err))
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
			logger.Log.Fatal("listen", zlog.String("addr", addr), zlog.Err(err))
		}
	case <-time.After(200 * time.Millisecond):
	}
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
			logger.Log.Fatal("declare queue", zlog.String("name", q.Name), zlog.Err(err))
		}
		logger.Log.Info("configured queue", zlog.String("name", cfg.Name), zlog.String("dlq", cfg.DLQ))
	}
	for _, t := range bc.Blocks("topic") {
		if err := b.DeclareTopic(ctx, broker.TopicConfig{Name: t.Name, Partitions: t.Int("partitions", 1), Retention: t.Duration("retention", 24*time.Hour)}); err != nil {
			logger.Log.Fatal("declare topic", zlog.String("name", t.Name), zlog.Err(err))
		}
		logger.Log.Info("configured topic", zlog.String("name", t.Name))
	}
	for _, s := range bc.Blocks("stream") {
		if err := b.DeclareStream(ctx, broker.StreamConfig{Name: s.Name, Partitions: s.Int("partitions", 1), Retention: s.Duration("retention", 24*time.Hour)}); err != nil {
			logger.Log.Fatal("declare stream", zlog.String("name", s.Name), zlog.Err(err))
		}
		logger.Log.Info("configured stream", zlog.String("name", s.Name))
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
		logger.Log.Info("configured route", zlog.String("name", route.Name), zlog.String("match", route.Match), zlog.Any("to", route.To), zlog.Any("copy_to", route.CopyTo))
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
