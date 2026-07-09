// Command pipeline_consumer demonstrates a fastworker_broker consumer whose
// message handler runs each delivery through a dagflow DAG pipeline
// (validate -> enrich -> branch -> review|fulfill -> notify) instead of a
// single inline function. The pipeline is defined in bcl/workflow.bcl and
// executed synchronously per message via dagflow.Engine.RunSync; the
// resulting task status/result drives Ack/Respond/Nack on the broker side.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/broker/examples/internal/exconfig"
	"github.com/oarkflow/broker/examples/internal/exobserve"
	bclient "github.com/oarkflow/broker/pkg/client"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/dagflow"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "pipeline_consumer/pipeline_consumer.bcl", "BCL consumer configuration")
	workflowDir := flag.String("workflow-bcl", "pipeline_consumer/bcl", "dagflow workflow BCL directory")
	flag.Parse()

	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	cc := exconfig.FirstBlock(root, "consumer")
	if cc == nil {
		logger.Log.Fatal("missing consumer block")
	}
	obsSink, err := exobserve.NewJSONLSink(cc.String("observer_jsonl", "./.broker-data/observability/pipeline-consumer-events.jsonl"))
	if err != nil {
		logger.Log.Fatal("open observability sink", zlog.Err(err))
	}
	defer obsSink.Close()
	observer := exobserve.ClientObserver{Prefix: "pipeline-consumer", Sink: obsSink}

	cfg := bclient.Config{
		BrokerURLs:              cc.Strings("broker_urls"),
		Token:                   cc.String("token", ""),
		ConsumerID:              cc.String("consumer_id", "pipeline-consumer-1"),
		Group:                   cc.String("group", "pipeline-consumers"),
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

	engine := newPipelineEngine(*workflowDir)
	if err := engine.Start(context.Background()); err != nil {
		logger.Log.Fatal("start dagflow engine", zlog.Err(err))
	}

	for _, h := range cc.Blocks("handler") {
		registerPipelineHandler(w, engine, h)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("pipeline consumer started config=%s workflow_bcl=%s consumer=%s group=%s\n", *configPath, *workflowDir, cfg.ConsumerID, cfg.Group)
	fmt.Println("each delivery runs through a dagflow pipeline before ack/respond")
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Log.Fatal("consumer run failed", zlog.Err(err))
	}
}

// newPipelineEngine builds an in-process dagflow engine backed by in-memory
// task/chain/job stores (no HTTP server, no external database) and loads the
// order_pipeline workflow from workflowDir.
func newPipelineEngine(workflowDir string) *dagflow.Engine {
	engine := dagflow.NewEngine(dagflow.NewMemoryTaskStore(), dagflow.NewMemoryChainStore(), dagflow.NewMemoryBroker())
	engine.RegisterHandlers(map[string]dagflow.Handler{
		"order:validate": validateOrder,
		"order:enrich":   enrichOrder,
		"order:review":   reviewOrder,
		"order:fulfill":  fulfillOrder,
		"order:notify":   notifyOrder,
	})
	cfg, err := dagflow.LoadBCL(workflowDir)
	if err != nil {
		logger.Log.Fatal("load pipeline workflow bcl", zlog.Err(err))
	}
	if err := engine.LoadConfig(cfg); err != nil {
		logger.Log.Fatal("load pipeline workflow config", zlog.Err(err))
	}
	return engine
}

// registerPipelineHandler wires a consumer.bcl "handler" block to a broker
// queue/topic/pattern subscription whose Handler runs the delivery payload
// through the named dagflow workflow synchronously.
func registerPipelineHandler(w *bclient.Worker, engine *dagflow.Engine, h *exconfig.Node) {
	typ := strings.ToLower(h.String("type", "queue"))
	name := h.String("name", h.Name)
	workflowID := h.String("workflow", "order_pipeline")

	handler := func(ctx context.Context, d bclient.Delivery) error {
		var payload map[string]any
		if err := d.Decode(&payload); err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}
		task, err := engine.RunSync(withDelivery(ctx, d), workflowID, payload)
		if err != nil {
			return fmt.Errorf("pipeline %s: %w", workflowID, err)
		}
		if task.Status != dagflow.TaskCompleted {
			return fmt.Errorf("pipeline %s ended in status %s: %s", workflowID, task.Status, task.Error)
		}
		logger.Log.Info("pipeline completed",
			zlog.String("workflow", workflowID),
			zlog.String("task", task.ID),
			zlog.String("message", d.Message.ID),
		)
		return d.Respond(ctx, task.Result, true)
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
	logger.Log.Info("configured pipeline handler", zlog.String("type", typ), zlog.String("name", name), zlog.String("workflow", workflowID))
}
