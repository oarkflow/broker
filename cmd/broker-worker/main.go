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
	"github.com/oarkflow/fastworker/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	brokerURL := flag.String("broker", env("BROKER_URL", "http://127.0.0.1:8090"), "broker URL")
	brokerURLs := flag.String("brokers", env("BROKER_URLS", ""), "comma-separated broker failover URLs")
	token := flag.String("token", env("BROKER_WORKER_TOKEN", env("BROKER_TOKEN", "")), "worker bearer token")
	id := flag.String("id", env("BROKER_CONSUMER_ID", ""), "consumer id")
	group := flag.String("group", env("BROKER_CONSUMER_GROUP", "workers"), "consumer group")
	queues := flag.String("queue", env("BROKER_QUEUES", ""), "comma-separated queues to handle")
	patterns := flag.String("pattern", env("BROKER_PATTERNS", ""), "comma-separated wildcard topic patterns to handle")
	tasks := flag.String("task", env("BROKER_TASKS", ""), "comma-separated task names to handle")
	requests := flag.String("request", env("BROKER_REQUESTS", ""), "comma-separated request subjects to handle")
	commands := flag.String("command", env("BROKER_COMMANDS", ""), "comma-separated command subjects to handle")
	concurrency := flag.Int("concurrency", envInt("BROKER_WORKER_CONCURRENCY", 8), "local worker concurrency")
	fail := flag.Bool("fail", false, "return handler errors for testing DLQ/retry")
	disableAutoExtend := flag.Bool("disable-auto-extend", envBool("BROKER_DISABLE_AUTO_EXTEND"), "disable automatic lease extension while handlers run")
	flag.Parse()

	worker, err := bclient.NewWorker(bclient.Config{BrokerURL: *brokerURL, BrokerURLs: csv(*brokerURLs), Token: *token, ConsumerID: *id, Group: *group, Concurrency: *concurrency, MaxInFlight: *concurrency * 4, PollLimit: 1, PollWait: 30 * time.Second, VisibilityTimeout: 30 * time.Second, DisableAutoExtendLeases: *disableAutoExtend, 	Hooks: bclient.LoggingHooksWith("worker")})
	if err != nil {
		logger.Log.Fatal("create worker", zlog.Err(err))
	}
	for _, q := range csv(*queues) {
		queue := q
		worker.HandleQueue(queue, func(ctx context.Context, d bclient.Delivery) error {
			logger.Log.Info("queue message", zlog.String("queue", queue), zlog.String("message", d.Message.ID), zlog.String("type", d.Message.Type))
			if *fail {
				return fmt.Errorf("forced failure")
			}
			return nil
		})
	}
	for _, p := range csv(*patterns) {
		pattern := p
		worker.HandlePattern(pattern, func(ctx context.Context, d bclient.Delivery) error {
			logger.Log.Info("pattern match", zlog.String("pattern", pattern), zlog.String("topic", d.Message.Topic), zlog.String("message", d.Message.ID))
			return nil
		})
	}
	for _, t := range csv(*tasks) {
		task := t
		worker.HandleTask(task, func(ctx context.Context, d bclient.Delivery) (any, error) {
			logger.Log.Info("task received", zlog.String("task", task), zlog.String("message", d.Message.ID))
			_ = d.Progress(ctx, broker.Progress{Percent: 50, Message: "working", UpdatedAt: time.Now()})
			if *fail {
				return nil, fmt.Errorf("forced task failure")
			}
			return map[string]any{"ok": true, "task": task, "message_id": d.Message.ID}, nil
		})
	}
	for _, s := range csv(*requests) {
		subject := s
		worker.HandleRequest(subject, func(ctx context.Context, d bclient.Delivery) (any, error) {
			var payload any
			_ = json.Unmarshal(d.Message.Payload, &payload)
			logger.Log.Info("request received", zlog.String("subject", subject), zlog.String("message", d.Message.ID))
			if *fail {
				return nil, fmt.Errorf("forced request failure")
			}
			return map[string]any{"ok": true, "subject": subject, "echo": payload}, nil
		})
	}
	for _, s := range csv(*commands) {
		subject := s
		worker.HandleCommand(subject, func(ctx context.Context, d bclient.Delivery) (any, error) {
			var payload any
			_ = json.Unmarshal(d.Message.Payload, &payload)
			logger.Log.Info("command received", zlog.String("subject", subject), zlog.String("message", d.Message.ID))
			if *fail {
				return nil, fmt.Errorf("forced command failure")
			}
			return map[string]any{"ok": true, "command": subject, "echo": payload}, nil
		})
	}
	if *queues == "" && *patterns == "" && *tasks == "" && *requests == "" && *commands == "" {
		logger.Log.Fatal("no handlers configured; provide -queue, -pattern, -task, -request, or -command")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Log.Info("remote worker starting", zlog.String("broker", *brokerURL), zlog.String("group", *group))
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Log.Fatal("worker run failed", zlog.Err(err))
	}
}

func csv(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		var out int
		if _, err := fmt.Sscanf(v, "%d", &out); err == nil && out > 0 {
			return out
		}
	}
	return d
}

func envBool(k string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
