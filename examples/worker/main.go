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

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/examples/internal/exconfig"
	"github.com/oarkflow/broker/examples/internal/exobserve"
	bclient "github.com/oarkflow/broker/pkg/client"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "examples/worker/worker.bcl", "BCL worker configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	wc := exconfig.FirstBlock(root, "worker")
	if wc == nil {
		logger.Log.Fatal("missing worker block")
	}
	obsSink, err := exobserve.NewJSONLSink(wc.String("observer_jsonl", "./.broker-data/observability/worker-events.jsonl"))
	if err != nil {
		logger.Log.Fatal("open observability sink", zlog.Err(err))
	}
	defer obsSink.Close()
	observer := exobserve.ClientObserver{Prefix: "complete-worker", Sink: obsSink}

	cfg := bclient.Config{
		BrokerURLs:              wc.Strings("broker_urls"),
		Token:                   wc.String("token", ""),
		ConsumerID:              wc.String("consumer_id", "complete-worker-1"),
		Group:                   wc.String("group", "complete-workers"),
		Name:                    wc.Name,
		Concurrency:             wc.Int("concurrency", 8),
		MaxInFlight:             wc.Int("max_inflight", 32),
		PollWait:                wc.Duration("poll_wait", 5*time.Second),
		HeartbeatInterval:       wc.Duration("heartbeat", 5*time.Second),
		VisibilityTimeout:       wc.Duration("visibility_timeout", 45*time.Second),
		FrameCodec:              exconfig.FrameCodec(wc),
		LeaseExtensionInterval:  wc.Duration("lease_extension_interval", 10*time.Second),
		MaxLeaseExtensions:      wc.Int("max_lease_extensions", 12),
		DisableAutoExtendLeases: !wc.Bool("auto_extend", true),
	}
	if wc.Bool("log_hooks", true) {
		cfg.Hooks = observer.Hooks()
	}
	w, err := bclient.NewWorker(cfg)
	if err != nil {
		logger.Log.Fatal("create worker", zlog.Err(err))
	}

	for _, h := range wc.Blocks("handler") {
		registerWorkerHandler(w, h)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("complete worker started config=%s consumer=%s group=%s\n", *configPath, cfg.ConsumerID, cfg.Group)
	fmt.Println("task/request/command handlers registered; delivery=polling; broker-events=SSE/long-poll")
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Log.Fatal("worker run failed", zlog.Err(err))
	}
}

func registerWorkerHandler(w *bclient.Worker, h *exconfig.Node) {
	typ := strings.ToLower(h.String("type", "task"))
	name := h.String("name", h.Name)
	switch typ {
	case "task":
		steps := h.Strings("steps")
		if len(steps) == 0 {
			steps = []string{"received", "working", "complete"}
		}
		w.HandleTaskQueue(name, h.String("queue", ""), func(ctx context.Context, d bclient.Delivery) (any, error) {
			logger.Log.Info("task started", zlog.String("name", name), zlog.String("task_id", d.Message.TaskID), zlog.String("message", d.Message.ID), zlog.String("payload", string(d.Message.Payload)))
			for i, step := range steps {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(150 * time.Millisecond):
				}
				pct := float64((i + 1) * 100 / len(steps))
				_ = d.Progress(ctx, broker.Progress{Percent: pct, Message: step, UpdatedAt: time.Now()})
				logger.Log.Info("task progress", zlog.String("name", name), zlog.String("task_id", d.Message.TaskID), zlog.Float64("percent", pct), zlog.String("step", step))
			}
			return map[string]any{"ok": true, "task": name, "task_id": d.Message.TaskID, "url": "file://generated/example.pdf"}, nil
		})
	case "request":
		w.HandleRequestQueue(name, h.String("queue", ""), func(ctx context.Context, d bclient.Delivery) (any, error) {
			var req map[string]any
			_ = json.Unmarshal(d.Message.Payload, &req)
			logger.Log.Info("request handled", zlog.String("subject", name), zlog.String("correlation", d.Message.CorrelationID), zlog.Any("payload", req))
			return map[string]any{"authorized": true, "subject": name, "auth_id": "auth-demo-001", "request": req}, nil
		})
	case "command":
		w.HandleCommandQueue(name, h.String("queue", ""), func(ctx context.Context, d bclient.Delivery) (any, error) {
			var req map[string]any
			_ = json.Unmarshal(d.Message.Payload, &req)
			logger.Log.Info("command handled", zlog.String("subject", name), zlog.String("correlation", d.Message.CorrelationID), zlog.Any("payload", req))
			return map[string]any{"captured": true, "subject": name, "capture_id": "cap-demo-001", "request": req}, nil
		})
	default:
		logger.Log.Fatal("unsupported worker handler type", zlog.String("type", typ))
	}
	logger.Log.Info("configured worker handler", zlog.String("type", typ), zlog.String("name", name))
}
