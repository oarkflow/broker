package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/fastworker/broker"
	bclient "github.com/oarkflow/fastworker/broker/client"
	"github.com/oarkflow/fastworker/examples/internal/exconfig"
	"github.com/oarkflow/fastworker/examples/internal/exobserve"
	"github.com/oarkflow/fastworker/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "examples/complete_consumer/consumer.bcl", "BCL consumer configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	cc := exconfig.FirstBlock(root, "consumer")
	if cc == nil {
		logger.Log.Fatal("missing consumer block")
	}
	obsSink, err := exobserve.NewJSONLSink(cc.String("observer_jsonl", "./.broker-data/observability/consumer-events.jsonl"))
	if err != nil {
		logger.Log.Fatal("open observability sink", zlog.Err(err))
	}
	defer obsSink.Close()
	observer := exobserve.ClientObserver{Prefix: "complete-consumer", Sink: obsSink}

	cfg := bclient.Config{
		BrokerURLs:              cc.Strings("broker_urls"),
		Token:                   cc.String("token", ""),
		ConsumerID:              cc.String("consumer_id", "complete-consumer-1"),
		Group:                   cc.String("group", "complete-consumers"),
		Name:                    cc.Name,
		Concurrency:             cc.Int("concurrency", 4),
		MaxInFlight:             cc.Int("max_inflight", 16),
		PollWait:                cc.Duration("poll_wait", 5*time.Second),
		HeartbeatInterval:       cc.Duration("heartbeat", 5*time.Second),
		VisibilityTimeout:       cc.Duration("visibility_timeout", 30*time.Second),
		FrameCodec:              exconfig.FrameCodec(cc),
		DisableAutoExtendLeases: !cc.Bool("auto_extend", true),
	}
	if cc.Bool("log_hooks", true) {
		cfg.Hooks = observer.Hooks()
	}
	w, err := bclient.NewWorker(cfg)
	if err != nil {
		logger.Log.Fatal("create worker", zlog.Err(err))
	}

	for _, h := range cc.Blocks("handler") {
		registerConsumerHandler(w, h)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("complete consumer started config=%s consumer=%s group=%s\n", *configPath, cfg.ConsumerID, cfg.Group)
	fmt.Println("delivery=polling, broker-events=SSE/long-poll, service-discovery=/v1/consumers")
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Log.Fatal("consumer run failed", zlog.Err(err))
	}
}

func registerConsumerHandler(w *bclient.Worker, h *exconfig.Node) {
	typ := strings.ToLower(h.String("type", "queue"))
	name := h.String("name", h.Name)
	failText := h.String("fail_on_contains", "")
	handler := func(ctx context.Context, d bclient.Delivery) error {
		var payload map[string]any
		_ = json.Unmarshal(d.Message.Payload, &payload)
		logger.Log.Info("consumer received", zlog.String("kind", typ), zlog.String("name", name), zlog.String("message", d.Message.ID), zlog.String("topic", d.Message.Topic), zlog.String("queue", d.Message.Queue), zlog.String("payload", string(d.Message.Payload)))
		if failText != "" && strings.Contains(string(d.Message.Payload), failText) {
			return fmt.Errorf("configured failure matched %q", failText)
		}
		_ = d.Progress(ctx, broker.Progress{Percent: 100, Message: "consumer handled", UpdatedAt: time.Now()})
		return nil
	}
	switch typ {
	case "queue":
		w.HandleQueue(name, handler)
	case "topic":
		w.HandleTopic(name, handler)
	case "pattern", "wildcard":
		w.HandlePattern(name, handler)
	default:
		logger.Log.Fatal("unsupported consumer handler type", zlog.String("type", typ))
	}
	logger.Log.Info("configured consumer handler", zlog.String("type", typ), zlog.String("name", name))
}
