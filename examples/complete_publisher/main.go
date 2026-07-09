package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/fastworker/broker"
	bclient "github.com/oarkflow/fastworker/broker/client"
	"github.com/oarkflow/fastworker/examples/internal/exconfig"
	"github.com/oarkflow/fastworker/examples/internal/exobserve"
	"github.com/oarkflow/fastworker/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "complete_publisher/publisher.bcl", "BCL publisher configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	pc := exconfig.FirstBlock(root, "publisher")
	if pc == nil {
		logger.Log.Fatal("missing publisher block")
	}
	obsSink, err := exobserve.NewJSONLSink(pc.String("observer_jsonl", "./.broker-data/observability/publisher-events.jsonl"))
	if err != nil {
		logger.Log.Fatal("open observability sink", zlog.Err(err))
	}
	defer obsSink.Close()

	c := bclient.New(bclient.Config{BrokerURLs: pc.Strings("broker_urls"), Token: pc.String("token", ""), PollWait: 5 * time.Second, FrameCodec: exconfig.FrameCodec(pc)})
	ctx := context.Background()
	reqTimeout := pc.Duration("request_timeout", 10*time.Second)
	taskWait := pc.Duration("task_wait_timeout", 20*time.Second)
	streamGroup := pc.String("stream_group", "publisher-demo")
	requireConsumers := pc.Bool("require_consumers", true)
	discoveryWait := pc.Duration("discovery_wait", 15*time.Second)
	logger.Log.Info("publisher started", zlog.String("config", *configPath))

	for _, op := range pc.Blocks("operation") {
		obsSink.Write("publisher.operation.start", map[string]any{"name": op.Name, "mode": op.String("mode", "publish")})
		if requireConsumers {
			if err := ensureOperationReady(ctx, c, op, discoveryWait); err != nil {
				logger.Log.Warn("CALLBACK publisher.operation.failed", zlog.String("operation", op.Name), zlog.Err(err))
				obsSink.Write("publisher.operation.failed", map[string]any{"name": op.Name, "error": err.Error()})
				continue
			}
		}
		if err := runOperation(ctx, c, op, reqTimeout, taskWait, streamGroup); err != nil {
			logger.Log.Warn("CALLBACK publisher.operation.failed", zlog.String("operation", op.Name), zlog.Err(err))
			obsSink.Write("publisher.operation.failed", map[string]any{"name": op.Name, "error": err.Error()})
		} else {
			logger.Log.Info("CALLBACK publisher.operation.succeeded", zlog.String("operation", op.Name))
			obsSink.Write("publisher.operation.succeeded", map[string]any{"name": op.Name})
		}
	}
}

func ensureOperationReady(ctx context.Context, c *bclient.Client, op *exconfig.Node, timeout time.Duration) error {
	mode := op.String("mode", "publish")
	switch mode {
	case "task":
		queue := op.String("queue", "")
		if queue == "" {
			queue = "tasks." + op.String("name", op.Name)
		}
		return waitForConsumerQueue(ctx, c, queue, timeout, "task worker")
	case "request":
		queue := op.String("queue", "")
		if queue == "" {
			queue = "request." + sanitizeSubject(op.String("subject", op.Name))
		}
		return waitForConsumerQueue(ctx, c, queue, timeout, "request worker")
	case "command":
		queue := op.String("queue", "")
		if queue == "" {
			queue = "command." + sanitizeSubject(op.String("subject", op.Name))
		}
		return waitForConsumerQueue(ctx, c, queue, timeout, "command worker")
	}
	return nil
}

func waitForConsumerQueue(ctx context.Context, c *bclient.Client, queue string, timeout time.Duration, role string) error {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var last []broker.ConsumerInfo
	for {
		consumers, err := c.Consumers(ctx)
		if err == nil {
			last = consumers
			for _, info := range consumers {
				if consumerReadyForQueue(info, queue) {
					logger.Log.Info("discovery ok",
						zlog.String("role", role),
						zlog.String("queue", queue),
						zlog.String("consumer", info.ConsumerID),
						zlog.String("group", info.Group),
					)
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no live %s is subscribed to queue %q; start examples/complete_worker with matching worker.bcl before running this operation (registered_consumers=%s)", role, queue, consumerSummary(last))
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func consumerReadyForQueue(info broker.ConsumerInfo, queue string) bool {
	if info.Paused || info.Draining || info.State == broker.ConsumerPaused || info.State == broker.ConsumerDraining || info.State == broker.ConsumerStopped || info.State == broker.ConsumerFailed || info.State == broker.ConsumerDisconnected {
		return false
	}
	for _, q := range info.Queues {
		if q == queue {
			return true
		}
	}
	for _, sub := range info.Subscriptions {
		if sub.Queue == queue || sub.Name == queue {
			return true
		}
	}
	return false
}

func consumerSummary(consumers []broker.ConsumerInfo) string {
	if len(consumers) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(consumers))
	for _, c := range consumers {
		queues := append([]string{}, c.Queues...)
		for _, s := range c.Subscriptions {
			if s.Queue != "" {
				queues = append(queues, s.Queue)
			}
		}
		parts = append(parts, fmt.Sprintf("%s(state=%s,queues=%v)", c.ConsumerID, c.State, queues))
	}
	return strings.Join(parts, "; ")
}

func sanitizeSubject(s string) string {
	return strings.NewReplacer("#", "all", "*", "star", "/", "_", " ", "_", ":", "_").Replace(s)
}

func runOperation(ctx context.Context, c *bclient.Client, op *exconfig.Node, reqTimeout, taskWait time.Duration, streamGroup string) error {
	mode := op.String("mode", "publish")
	logger.Log.Info("operation", zlog.String("name", op.Name), zlog.String("mode", mode))
	switch mode {
	case "publish":
		res, err := c.Publish(ctx, op.String("topic", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), publishOpts(op)...)
		if err != nil {
			logger.Log.Warn("publish failed", zlog.String("message_id", res.MessageID), zlog.Int("destinations", len(res.Destinations)), zlog.Err(err))
		} else {
			logger.Log.Info("published", zlog.String("message_id", res.MessageID), zlog.Int("destinations", len(res.Destinations)))
		}
		return err
	case "async":
		ch := c.PublishAsync(ctx, op.String("topic", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), publishOpts(op)...)
		res := <-ch
		if res.Error != nil {
			logger.Log.Warn("async publish failed", zlog.String("message_id", res.Result.MessageID), zlog.Int("destinations", len(res.Result.Destinations)), zlog.Err(res.Error))
		} else {
			logger.Log.Info("async published", zlog.String("message_id", res.Result.MessageID), zlog.Int("destinations", len(res.Result.Destinations)))
		}
		return res.Error
	case "batch":
		var payloads []any
		for _, p := range exconfig.PayloadsJSON(op) {
			payloads = append(payloads, json.RawMessage(exconfig.MustJSONPayload(p)))
		}
		res, err := c.PublishBatch(ctx, op.String("topic", op.Name), payloads, publishOpts(op)...)
		if err != nil {
			logger.Log.Warn("batch publish failed", zlog.Int("count", len(res.Results)), zlog.Int("errors", len(res.Errors)), zlog.Err(err))
		} else {
			logger.Log.Info("batch published", zlog.Int("count", len(res.Results)), zlog.Int("errors", len(res.Errors)))
		}
		return err
	case "frame_publish":
		msg := broker.Message{ID: op.String("id", ""), Topic: op.String("topic", op.Name), Queue: op.String("queue", ""), Stream: op.String("stream", ""), Type: op.String("type", "frame"), ContentType: "application/json", Payload: []byte(exconfig.PayloadJSON(op, `{}`)), Headers: map[string]string{"x-codec": "fwb1"}, CreatedAt: time.Now(), Status: broker.MessageQueued}
		res, err := c.PublishFrame(ctx, msg)
		if err != nil {
			logger.Log.Warn("frame publish failed", zlog.String("message_id", res.MessageID), zlog.Int("destinations", len(res.Destinations)), zlog.Err(err))
		} else {
			logger.Log.Info("frame published", zlog.String("message_id", res.MessageID), zlog.Int("destinations", len(res.Destinations)))
		}
		return err
	case "frame_poll":
		msgs, err := c.PollFrames(ctx, op.String("queue", "default"), op.Int("limit", 1))
		if err != nil {
			logger.Log.Warn("frame poll failed", zlog.Err(err))
		} else {
			logger.Log.Info("frame poll", zlog.Int("count", len(msgs)))
			for _, m := range msgs {
				logger.Log.Info("frame message",
					zlog.String("message_id", m.ID),
					zlog.String("topic", m.Topic),
					zlog.String("queue", m.Queue),
					zlog.String("payload", string(m.Payload)),
				)
			}
		}
		return err
	case "task":
		task, err := c.SubmitTask(ctx, op.String("name", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), taskOpts(op)...)
		if err != nil {
			return err
		}
		logger.Log.Info("submitted task", zlog.String("task_id", task.ID), zlog.String("status", string(task.Status)))
		if op.Bool("wait", true) {
			return waitTask(ctx, c, task.ID, taskWait)
		}
		return nil
	case "request":
		var resp map[string]any
		err := c.Request(ctx, op.String("subject", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), &resp, reqTimeout)
		if err != nil {
			logger.Log.Warn("request failed", zlog.String("subject", op.String("subject", op.Name)), zlog.Err(err))
		} else {
			logger.Log.Info("request completed", zlog.String("subject", op.String("subject", op.Name)), zlog.Any("response", resp))
		}
		return err
	case "command":
		var resp map[string]any
		err := c.Command(ctx, op.String("subject", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), &resp, reqTimeout)
		if err != nil {
			logger.Log.Warn("command failed", zlog.String("subject", op.String("subject", op.Name)), zlog.Err(err))
		} else {
			logger.Log.Info("command completed", zlog.String("subject", op.String("subject", op.Name)), zlog.Any("response", resp))
		}
		return err
	case "stream":
		res, err := c.Publish(ctx, op.String("topic", op.Name), json.RawMessage(exconfig.PayloadJSON(op, `{}`)), publishOpts(op)...)
		if err != nil {
			return err
		}
		stream := op.String("stream", "audit.events")
		msgs, rerr := c.ReadStream(ctx, stream, streamGroup, 0, 10)
		if rerr != nil {
			logger.Log.Warn("stream read failed", zlog.String("message_id", res.MessageID), zlog.Int("read", len(msgs)), zlog.Err(rerr))
		} else {
			logger.Log.Info("stream publish", zlog.String("message_id", res.MessageID), zlog.Int("read", len(msgs)))
		}
		if rerr == nil && len(msgs) > 0 {
			_ = c.CommitStreamOffset(ctx, stream, streamGroup, msgs[len(msgs)-1].Offset)
			logger.Log.Info("stream committed", zlog.Int64("offset", int64(msgs[len(msgs)-1].Offset)), zlog.String("group", streamGroup))
		}
		return rerr
	default:
		return fmt.Errorf("unsupported mode %q", mode)
	}
}

func publishOpts(op *exconfig.Node) []bclient.PublishOption {
	var opts []bclient.PublishOption
	if q := op.String("queue", ""); q != "" {
		opts = append(opts, bclient.WithQueue(q))
	}
	if s := op.String("stream", ""); s != "" {
		opts = append(opts, bclient.WithStream(s))
	}
	if typ := op.String("type", ""); typ != "" {
		opts = append(opts, bclient.WithType(typ))
	}
	if key := op.String("key", ""); key != "" {
		opts = append(opts, bclient.WithKey(key))
	}
	if rk := op.String("routing_key", ""); rk != "" {
		opts = append(opts, bclient.WithRoutingKey(rk))
	}
	if p := op.Int("priority", 0); p != 0 {
		opts = append(opts, bclient.WithPriority(p))
	}
	return opts
}

func taskOpts(op *exconfig.Node) []bclient.TaskOption {
	var opts []bclient.TaskOption
	if q := op.String("queue", ""); q != "" {
		opts = append(opts, bclient.WithTaskQueue(q))
	}
	if p := op.Int("priority", 0); p != 0 {
		opts = append(opts, bclient.WithTaskPriority(p))
	}
	return opts
}

func waitTask(ctx context.Context, c *bclient.Client, id string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, raw, err := c.TaskResult(ctx, id)
		if err == nil && (info.Status == broker.TaskSucceeded || info.Status == broker.TaskFailed || info.Status == broker.TaskCancelled || info.Status == broker.TaskDeadLettered) {
			logger.Log.Info("task ended",
				zlog.String("task_id", id),
				zlog.String("status", string(info.Status)),
				zlog.Float64("progress", info.Progress.Percent),
				zlog.String("result", string(raw)),
				zlog.String("error", info.LastError),
			)
			if info.Status != broker.TaskSucceeded {
				return fmt.Errorf("task ended with status %s: %s", info.Status, info.LastError)
			}
			return nil
		}
		if err == nil {
			logger.Log.Info("task status",
				zlog.String("task_id", id),
				zlog.String("status", string(info.Status)),
				zlog.Float64("progress", info.Progress.Percent),
			)
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("task %s timed out", id)
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
