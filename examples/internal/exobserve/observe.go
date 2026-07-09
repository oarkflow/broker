package exobserve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oarkflow/fastworker/broker"
	bclient "github.com/oarkflow/fastworker/broker/client"
	"github.com/oarkflow/fastworker/logger"
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

func (o BrokerObserver) logf(format string, args ...any) {
	prefix := o.Prefix
	if prefix == "" {
		prefix = "broker-observer"
	}
	logger.StdLog.Printf(prefix+" "+format, args...)
}

func (o BrokerObserver) Hooks() broker.Hooks {
	return broker.Hooks{
		OnStarted: func(_ context.Context, e broker.BrokerEvent) {
			o.logf("HOOK broker.started state=%s", e.State)
			o.Sink.Write("broker.hook.started", e)
		},
		OnReady: func(_ context.Context, e broker.BrokerEvent) {
			o.logf("HOOK broker.ready state=%s", e.State)
			o.Sink.Write("broker.hook.ready", e)
		},
		OnStopped: func(_ context.Context, e broker.BrokerEvent) {
			o.logf("HOOK broker.stopped state=%s", e.State)
			o.Sink.Write("broker.hook.stopped", e)
		},
		BeforePublish: func(_ context.Context, e broker.PublishEvent) error {
			o.logf("HOOK before_publish message=%s topic=%s queue=%s type=%s", e.Message.ID, e.Message.Topic, e.Message.Queue, e.Message.Type)
			o.Sink.Write("broker.hook.before_publish", e)
			return nil
		},
		AfterPublish: func(_ context.Context, e broker.PublishEvent) {
			o.logf("HOOK after_publish message=%s destinations=%d", e.Message.ID, len(e.Destinations))
			o.Sink.Write("broker.hook.after_publish", e)
		},
		OnPublishConfirmed: func(_ context.Context, e broker.PublishEvent) {
			o.logf("HOOK publish_confirmed message=%s", e.Message.ID)
			o.Sink.Write("broker.hook.publish_confirmed", e)
		},
		OnConsumerRegistered: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.registered consumer=%s group=%s state=%s", e.Consumer, e.Group, e.State)
			o.Sink.Write("broker.hook.consumer_registered", e)
		},
		OnConsumerAssigned: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.assigned consumer=%s group=%s queue=%s", e.Consumer, e.Group, e.Queue)
			o.Sink.Write("broker.hook.consumer_assigned", e)
		},
		OnConsumerPaused: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.paused consumer=%s group=%s", e.Consumer, e.Group)
			o.Sink.Write("broker.hook.consumer_paused", e)
		},
		OnConsumerResumed: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.resumed consumer=%s group=%s", e.Consumer, e.Group)
			o.Sink.Write("broker.hook.consumer_resumed", e)
		},
		OnConsumerDraining: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.draining consumer=%s group=%s", e.Consumer, e.Group)
			o.Sink.Write("broker.hook.consumer_draining", e)
		},
		OnConsumerStopped: func(_ context.Context, e broker.ConsumerEvent) {
			o.logf("HOOK consumer.stopped consumer=%s group=%s state=%s", e.Consumer, e.Group, e.State)
			o.Sink.Write("broker.hook.consumer_stopped", e)
		},
		OnTaskCreated: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.created task=%s name=%s status=%s", e.Task.ID, e.Task.Name, e.Task.Status)
			o.Sink.Write("broker.hook.task_created", e)
		},
		OnTaskQueued: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.queued task=%s queue=%s", e.Task.ID, e.Task.Queue)
			o.Sink.Write("broker.hook.task_queued", e)
		},
		OnTaskStarted: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.started task=%s", e.Task.ID)
			o.Sink.Write("broker.hook.task_started", e)
		},
		OnTaskProgress: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.progress task=%s percent=%.0f message=%s", e.Task.ID, e.Task.Progress.Percent, e.Task.Progress.Message)
			o.Sink.Write("broker.hook.task_progress", e)
		},
		OnTaskSucceeded: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.succeeded task=%s", e.Task.ID)
			o.Sink.Write("broker.hook.task_succeeded", e)
		},
		OnTaskFailed: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.failed task=%s error=%v", e.Task.ID, e.Error)
			o.Sink.Write("broker.hook.task_failed", e)
		},
		OnTaskCancelled: func(_ context.Context, e broker.TaskEvent) {
			o.logf("HOOK task.cancelled task=%s error=%v", e.Task.ID, e.Error)
			o.Sink.Write("broker.hook.task_cancelled", e)
		},
		OnRetryScheduled: func(_ context.Context, e broker.RetryEvent) {
			o.logf("HOOK retry.scheduled message=%s delay=%s error=%v", e.Message.ID, e.Delay, e.Error)
			o.Sink.Write("broker.hook.retry_scheduled", e)
		},
		OnDeadLettered: func(_ context.Context, e broker.DLQEvent) {
			o.logf("HOOK dlq.dead_lettered message=%s queue=%s reason=%s error=%v", e.Message.ID, e.Queue, e.Reason, e.Error)
			o.Sink.Write("broker.hook.dead_lettered", e)
		},
		OnDLQReplay: func(_ context.Context, e broker.DLQEvent) {
			o.logf("HOOK dlq.replay queue=%s message=%s", e.Queue, e.Message.ID)
			o.Sink.Write("broker.hook.dlq_replay", e)
		},
	}
}

func (o BrokerObserver) NotificationCallback(name string) broker.Callback {
	if name == "" {
		name = "notification-callback"
	}
	return broker.CallbackFunc(func(_ context.Context, n broker.Notification) error {
		o.logf("CALLBACK %s notification=%s severity=%s message=%s task=%s queue=%s topic=%s consumer=%s", name, n.Type, n.Severity, n.MessageID, n.TaskID, n.Queue, n.Topic, n.Consumer)
		o.Sink.Write("broker.notification."+name, n)
		return nil
	})
}

type ClientObserver struct {
	Prefix string
	Sink   *JSONLSink
}

func (o ClientObserver) logf(format string, args ...any) {
	prefix := o.Prefix
	if prefix == "" {
		prefix = "client-observer"
	}
	logger.StdLog.Printf(prefix+" "+format, args...)
}

func (o ClientObserver) Hooks() bclient.Hooks {
	base := bclient.LoggingHooksWith(o.Prefix, logger.StdLog.Printf)
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
			o.logf("CALLBACK broker_event type=%s severity=%s message=%s task=%s queue=%s", e.Notification.Type, e.Notification.Severity, e.Notification.MessageID, e.Notification.TaskID, e.Notification.Queue)
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

func PrintJSON(title string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("\n%s\n%s\n", title, b)
}
