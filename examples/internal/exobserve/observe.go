package exobserve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oarkflow/broker"
	bclient "github.com/oarkflow/broker/pkg/client"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

type JSONLSink struct {
	mu   sync.Mutex
	file *os.File
}

func NewJSONLSink(path string) (*JSONLSink, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &JSONLSink{file: f}, nil
}

func (s *JSONLSink) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *JSONLSink) Write(kind string, value any) {
	if s == nil || s.file == nil {
		return
	}
	rec := map[string]any{"ts": time.Now().Format(time.RFC3339Nano), "kind": kind, "value": value}
	b, _ := json.Marshal(rec)
	s.mu.Lock()
	_, _ = s.file.Write(append(b, '\n'))
	s.mu.Unlock()
}

type BrokerObserver struct {
	Prefix string
	Sink   *JSONLSink
}

func (o BrokerObserver) prefix() string {
	if o.Prefix == "" {
		return "broker-observer"
	}
	return o.Prefix
}

func (o BrokerObserver) Hooks() broker.Hooks {
	prefix := o.prefix()
	return broker.Hooks{
		OnStarted: func(_ context.Context, e broker.BrokerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "broker.started", zlog.String("state", string(e.State)))...)
			o.Sink.Write("broker.hook.started", e)
		},
		OnReady: func(_ context.Context, e broker.BrokerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "broker.ready", zlog.String("state", string(e.State)))...)
			o.Sink.Write("broker.hook.ready", e)
		},
		OnStopped: func(_ context.Context, e broker.BrokerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "broker.stopped", zlog.String("state", string(e.State)))...)
			o.Sink.Write("broker.hook.stopped", e)
		},
		BeforePublish: func(_ context.Context, e broker.PublishEvent) error {
			logger.Log.Info("hook", hookAttrs(prefix, "before_publish",
				strAttr("message_id", e.Message.ID),
				strAttr("topic", e.Message.Topic),
				strAttr("queue", e.Message.Queue),
				strAttr("type", e.Message.Type),
			)...)
			o.Sink.Write("broker.hook.before_publish", e)
			return nil
		},
		AfterPublish: func(_ context.Context, e broker.PublishEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "after_publish",
				strAttr("message_id", e.Message.ID),
				zlog.Int("destinations", len(e.Destinations)),
			)...)
			o.Sink.Write("broker.hook.after_publish", e)
		},
		OnPublishConfirmed: func(_ context.Context, e broker.PublishEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "publish_confirmed", strAttr("message_id", e.Message.ID))...)
			o.Sink.Write("broker.hook.publish_confirmed", e)
		},
		OnConsumerRegistered: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.registered",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
				strAttr("state", string(e.State)),
			)...)
			o.Sink.Write("broker.hook.consumer_registered", e)
		},
		OnConsumerAssigned: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.assigned",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
				strAttr("queue", e.Queue),
			)...)
			o.Sink.Write("broker.hook.consumer_assigned", e)
		},
		OnConsumerPaused: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.paused",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
			)...)
			o.Sink.Write("broker.hook.consumer_paused", e)
		},
		OnConsumerResumed: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.resumed",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
			)...)
			o.Sink.Write("broker.hook.consumer_resumed", e)
		},
		OnConsumerDraining: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.draining",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
			)...)
			o.Sink.Write("broker.hook.consumer_draining", e)
		},
		OnConsumerStopped: func(_ context.Context, e broker.ConsumerEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "consumer.stopped",
				strAttr("consumer", e.Consumer),
				strAttr("group", e.Group),
				strAttr("state", string(e.State)),
			)...)
			o.Sink.Write("broker.hook.consumer_stopped", e)
		},
		OnTaskCreated: func(_ context.Context, e broker.TaskEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "task.created",
				strAttr("task_id", e.Task.ID),
				strAttr("task_name", e.Task.Name),
				strAttr("status", string(e.Task.Status)),
			)...)
			o.Sink.Write("broker.hook.task_created", e)
		},
		OnTaskQueued: func(_ context.Context, e broker.TaskEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "task.queued",
				strAttr("task_id", e.Task.ID),
				strAttr("queue", e.Task.Queue),
			)...)
			o.Sink.Write("broker.hook.task_queued", e)
		},
		OnTaskStarted: func(_ context.Context, e broker.TaskEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "task.started", strAttr("task_id", e.Task.ID))...)
			o.Sink.Write("broker.hook.task_started", e)
		},
		OnTaskProgress: func(_ context.Context, e broker.TaskEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "task.progress",
				strAttr("task_id", e.Task.ID),
				zlog.Float64("percent", e.Task.Progress.Percent),
				strAttr("message", e.Task.Progress.Message),
			)...)
			o.Sink.Write("broker.hook.task_progress", e)
		},
		OnTaskSucceeded: func(_ context.Context, e broker.TaskEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "task.succeeded", strAttr("task_id", e.Task.ID))...)
			o.Sink.Write("broker.hook.task_succeeded", e)
		},
		OnTaskFailed: func(_ context.Context, e broker.TaskEvent) {
			attrs := hookAttrs(prefix, "task.failed", strAttr("task_id", e.Task.ID))
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			logger.Log.Warn("hook", attrs...)
			o.Sink.Write("broker.hook.task_failed", e)
		},
		OnTaskCancelled: func(_ context.Context, e broker.TaskEvent) {
			attrs := hookAttrs(prefix, "task.cancelled", strAttr("task_id", e.Task.ID))
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			logger.Log.Warn("hook", attrs...)
			o.Sink.Write("broker.hook.task_cancelled", e)
		},
		OnRetryScheduled: func(_ context.Context, e broker.RetryEvent) {
			attrs := hookAttrs(prefix, "retry.scheduled",
				strAttr("message_id", e.Message.ID),
				zlog.String("delay", e.Delay.String()),
			)
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			logger.Log.Warn("hook", attrs...)
			o.Sink.Write("broker.hook.retry_scheduled", e)
		},
		OnDeadLettered: func(_ context.Context, e broker.DLQEvent) {
			attrs := hookAttrs(prefix, "dlq.dead_lettered",
				strAttr("message_id", e.Message.ID),
				strAttr("queue", e.Queue),
				strAttr("reason", e.Reason),
			)
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			logger.Log.Error("hook", attrs...)
			o.Sink.Write("broker.hook.dead_lettered", e)
		},
		OnDLQReplay: func(_ context.Context, e broker.DLQEvent) {
			logger.Log.Info("hook", hookAttrs(prefix, "dlq.replay",
				strAttr("queue", e.Queue),
				strAttr("message_id", e.Message.ID),
			)...)
			o.Sink.Write("broker.hook.dlq_replay", e)
		},
	}
}

func (o BrokerObserver) NotificationCallback(name string) broker.Callback {
	if name == "" {
		name = "notification-callback"
	}
	prefix := o.prefix()
	return broker.CallbackFunc(func(_ context.Context, n broker.Notification) error {
		attrs := make([]zlog.Attr, 0, 8)
		attrs = append(attrs,
			zlog.String("prefix", prefix),
			zlog.String("name", name),
			zlog.String("type", n.Type),
			zlog.String("severity", string(n.Severity)),
		)
		if n.MessageID != "" {
			attrs = append(attrs, zlog.String("message_id", n.MessageID))
		}
		if n.TaskID != "" {
			attrs = append(attrs, zlog.String("task_id", n.TaskID))
		}
		if n.Queue != "" {
			attrs = append(attrs, zlog.String("queue", n.Queue))
		}
		if n.Topic != "" {
			attrs = append(attrs, zlog.String("topic", n.Topic))
		}
		if n.Consumer != "" {
			attrs = append(attrs, zlog.String("consumer", n.Consumer))
		}
		logger.Log.Info("callback", attrs...)
		o.Sink.Write("broker.notification."+name, n)
		return nil
	})
}

type ClientObserver struct {
	Prefix string
	Sink   *JSONLSink
}

func (o ClientObserver) prefix() string {
	if o.Prefix == "" {
		return "client-observer"
	}
	return o.Prefix
}

func (o ClientObserver) Hooks() bclient.Hooks {
	base := bclient.LoggingHooksWith(o.Prefix)
	wrapEvent := func(kind string, next func(bclient.Event)) func(bclient.Event) {
		return func(e bclient.Event) {
			if next != nil {
				next(e)
			}
			o.Sink.Write(kind, e)
		}
	}
	wrapMsg := func(kind string, next func(bclient.MessageEvent)) func(bclient.MessageEvent) {
		return func(e bclient.MessageEvent) {
			if next != nil {
				next(e)
			}
			o.Sink.Write(kind, e)
		}
	}
	return bclient.Hooks{
		OnConnecting:    wrapEvent("client.hook.connecting", base.OnConnecting),
		OnConnected:     wrapEvent("client.hook.connected", base.OnConnected),
		OnAuthenticated: wrapEvent("client.hook.authenticated", base.OnAuthenticated),
		OnRegistered:    wrapEvent("client.hook.registered", base.OnRegistered),
		OnSubscribed:    wrapEvent("client.hook.subscribed", base.OnSubscribed),
		OnPollStart:     wrapEvent("client.hook.poll_start", base.OnPollStart),
		OnHeartbeat:     wrapEvent("client.hook.heartbeat", base.OnHeartbeat),
		OnReconnect:     wrapEvent("client.hook.reconnect", base.OnReconnect),
		OnDrain:         wrapEvent("client.hook.drain", base.OnDrain),
		OnStopped:       wrapEvent("client.hook.stopped", base.OnStopped),
		OnNotification: func(e bclient.NotificationEvent) {
			if base.OnNotification != nil {
				base.OnNotification(e)
			}
			prefix := o.prefix()
			attrs := make([]zlog.Attr, 0, 6)
			attrs = append(attrs,
				zlog.String("prefix", prefix),
				zlog.String("name", "broker_event"),
				zlog.String("type", e.Notification.Type),
				zlog.String("severity", string(e.Notification.Severity)),
			)
			if e.Notification.MessageID != "" {
				attrs = append(attrs, zlog.String("message_id", e.Notification.MessageID))
			}
			if e.Notification.TaskID != "" {
				attrs = append(attrs, zlog.String("task_id", e.Notification.TaskID))
			}
			if e.Notification.Queue != "" {
				attrs = append(attrs, zlog.String("queue", e.Notification.Queue))
			}
			logger.Log.Info("callback", attrs...)
			o.Sink.Write("client.notification.broker_event", e)
		},
		OnMessageReceived: wrapMsg("client.hook.message_received", base.OnMessageReceived),
		BeforeHandle: func(e bclient.MessageEvent) error {
			if base.BeforeHandle != nil {
				if err := base.BeforeHandle(e); err != nil {
					return err
				}
			}
			o.Sink.Write("client.hook.before_handle", e)
			return nil
		},
		AfterHandle:     wrapMsg("client.hook.after_handle", base.AfterHandle),
		OnHandlerError:  wrapMsg("client.hook.handler_error", base.OnHandlerError),
		OnAck:           wrapMsg("client.hook.ack", base.OnAck),
		OnNack:          wrapMsg("client.hook.nack", base.OnNack),
		OnReject:        wrapMsg("client.hook.reject", base.OnReject),
		OnProgress:      wrapMsg("client.hook.progress", base.OnProgress),
		OnLeaseExtended: wrapMsg("client.hook.lease_extended", base.OnLeaseExtended),
	}
}

func hookAttrs(prefix, name string, extra ...zlog.Attr) []zlog.Attr {
	attrs := make([]zlog.Attr, 0, 3+len(extra))
	attrs = append(attrs,
		zlog.String("prefix", prefix),
		zlog.String("name", name),
	)
	for _, a := range extra {
		if a.Kind != zlog.KindInvalid {
			attrs = append(attrs, a)
		}
	}
	return attrs
}

func strAttr(k, v string) zlog.Attr {
	if v == "" {
		return zlog.Attr{}
	}
	return zlog.String(k, v)
}

func PrintJSON(title string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("\n%s\n%s\n", title, b)
}
