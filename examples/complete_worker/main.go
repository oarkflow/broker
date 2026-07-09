package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/fastworker/broker"
	bclient "github.com/oarkflow/fastworker/broker/client"
	"github.com/oarkflow/fastworker/examples/internal/exconfig"
	"github.com/oarkflow/fastworker/examples/internal/exobserve"
)

func main() {
	configPath := flag.String("config", "examples/complete_worker/worker.bcl", "BCL worker configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	wc := exconfig.FirstBlock(root, "worker")
	if wc == nil {
		log.Fatal("missing worker block")
	}
	obsSink, err := exobserve.NewJSONLSink(wc.String("observer_jsonl", "./.broker-data/observability/worker-events.jsonl"))
	if err != nil {
		log.Fatalf("open observability sink: %v", err)
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
		log.Fatalf("create worker: %v", err)
	}

	for _, h := range wc.Blocks("handler") {
		registerWorkerHandler(w, h)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("complete worker started config=%s consumer=%s group=%s\n", *configPath, cfg.ConsumerID, cfg.Group)
	fmt.Println("task/request/command handlers registered; delivery=polling; broker-events=SSE/long-poll")
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
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
		w.HandleTask(name, func(ctx context.Context, d bclient.Delivery) (any, error) {
			log.Printf("task started name=%s task_id=%s message=%s payload=%s", name, d.Message.TaskID, d.Message.ID, string(d.Message.Payload))
			for i, step := range steps {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(150 * time.Millisecond):
				}
				pct := float64((i + 1) * 100 / len(steps))
				_ = d.Progress(ctx, broker.Progress{Percent: pct, Message: step, UpdatedAt: time.Now()})
				log.Printf("task progress name=%s task_id=%s percent=%.0f step=%s", name, d.Message.TaskID, pct, step)
			}
			return map[string]any{"ok": true, "task": name, "task_id": d.Message.TaskID, "url": "file://generated/example.pdf"}, nil
		})
	case "request":
		w.HandleRequest(name, func(ctx context.Context, d bclient.Delivery) (any, error) {
			var req map[string]any
			_ = json.Unmarshal(d.Message.Payload, &req)
			log.Printf("request handled subject=%s correlation=%s payload=%v", name, d.Message.CorrelationID, req)
			return map[string]any{"authorized": true, "subject": name, "auth_id": "auth-demo-001", "request": req}, nil
		})
	case "command":
		w.HandleCommand(name, func(ctx context.Context, d bclient.Delivery) (any, error) {
			var req map[string]any
			_ = json.Unmarshal(d.Message.Payload, &req)
			log.Printf("command handled subject=%s correlation=%s payload=%v", name, d.Message.CorrelationID, req)
			return map[string]any{"captured": true, "subject": name, "capture_id": "cap-demo-001", "request": req}, nil
		})
	default:
		log.Fatalf("unsupported worker handler type %q", typ)
	}
	log.Printf("configured worker handler type=%s name=%s", typ, name)
}
