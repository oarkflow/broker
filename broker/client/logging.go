package client

import (
	"time"

	fw "github.com/oarkflow/fastworker/logger"
	"github.com/oarkflow/zlog"
)

// LoggingHooks returns lifecycle hooks that log structured remote-consumer events.
func LoggingHooks(prefix string) Hooks {
	return LoggingHooksWith(prefix)
}

func LoggingHooksWith(prefix string) Hooks {
	if prefix == "" {
		prefix = "worker"
	}
	return Hooks{
		OnConnecting: func(e Event) {
			attrs := baseAttrs(prefix, "connecting", e.ConsumerID, e.Group)
			fw.Log.Info("lifecycle", attrs...)
		},
		OnConnected: func(e Event) {
			attrs := baseAttrs(prefix, "connected", e.ConsumerID, e.Group)
			fw.Log.Info("lifecycle", attrs...)
		},
		OnAuthenticated: func(e Event) {
			attrs := baseAttrs(prefix, "authenticated", e.ConsumerID, e.Group)
			fw.Log.Info("lifecycle", attrs...)
		},
		OnRegistered: func(e Event) {
			attrs := baseAttrs(prefix, "registered", e.ConsumerID, e.Group, strAttr("consumer_state", string(e.State)))
			fw.Log.Info("lifecycle", attrs...)
		},
		OnSubscribed: func(e Event) {
			attrs := baseAttrs(prefix, "subscribed", e.ConsumerID, e.Group, strAttr("consumer_state", string(e.State)))
			fw.Log.Info("lifecycle", attrs...)
		},
		OnPollStart: func(e Event) {
			attrs := baseAttrs(prefix, "polling", e.ConsumerID, e.Group)
			fw.Log.Info("lifecycle", attrs...)
		},
		OnHeartbeat: func(e Event) {
			attrs := baseAttrs(prefix, "heartbeat", e.ConsumerID, e.Group, strAttr("consumer_state", string(e.State)))
			fw.Log.Info("lifecycle", attrs...)
		},
		OnReconnect: func(e Event) {
			attrs := baseAttrs(prefix, "reconnect", e.ConsumerID, e.Group)
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			fw.Log.Warn("lifecycle", attrs...)
		},
		OnDrain: func(e Event) {
			attrs := baseAttrs(prefix, "drain", e.ConsumerID, e.Group, strAttr("consumer_state", string(e.State)))
			fw.Log.Info("lifecycle", attrs...)
		},
		OnStopped: func(e Event) {
			attrs := baseAttrs(prefix, "stopped", e.ConsumerID, e.Group, strAttr("consumer_state", string(e.State)))
			fw.Log.Info("lifecycle", attrs...)
		},
		OnNotification: func(e NotificationEvent) {
			attrs := make([]zlog.Attr, 0, 8)
			attrs = append(attrs,
				zlog.String("prefix", prefix),
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
			if e.Notification.Topic != "" {
				attrs = append(attrs, zlog.String("topic", e.Notification.Topic))
			}
			if len(e.Notification.Data) > 0 {
				attrs = append(attrs, zlog.Any("data", e.Notification.Data))
			}
			fw.Log.Info("notification", attrs...)
		},
		OnMessageReceived: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "received", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
			if e.Message.Attempts > 0 {
				attrs = append(attrs, zlog.Int("attempt", e.Message.Attempts))
			}
			if !e.Message.LeaseUntil.IsZero() {
				attrs = append(attrs, zlog.Time("lease_until", e.Message.LeaseUntil))
			}
			fw.Log.Info("message", attrs...)
		},
		BeforeHandle: func(e MessageEvent) error {
			attrs := msgAttrs(prefix, "handler.start", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
			fw.Log.Info("message", attrs...)
			return nil
		},
		AfterHandle: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "handler.done", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
			fw.Log.Info("message", attrs...)
		},
		OnHandlerError: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "handler.error", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			fw.Log.Error("message", attrs...)
		},
		OnAck: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "ack", e.Message.ID, e.Queue, "", "")
			fw.Log.Info("message", attrs...)
		},
		OnNack: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "nack", e.Message.ID, e.Queue, "", "")
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			fw.Log.Warn("message", attrs...)
		},
		OnReject: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "reject", e.Message.ID, e.Queue, "", "")
			if e.Error != nil {
				attrs = append(attrs, zlog.Err(e.Error))
			}
			fw.Log.Warn("message", attrs...)
		},
		OnProgress: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "progress", e.Message.ID, e.Queue, "", "")
			fw.Log.Info("message", attrs...)
		},
		OnLeaseExtended: func(e MessageEvent) {
			attrs := msgAttrs(prefix, "lease.extend", e.Message.ID, e.Queue, "", "")
			fw.Log.Debug("message", attrs...)
		},
	}
}

func baseAttrs(prefix, state, consumerID, group string, extra ...zlog.Attr) []zlog.Attr {
	attrs := make([]zlog.Attr, 0, 6+len(extra))
	attrs = append(attrs,
		zlog.String("prefix", prefix),
		zlog.String("state", state),
		zlog.String("consumer", consumerID),
		zlog.String("group", group),
	)
	attrs = append(attrs, extra...)
	return attrs
}

func msgAttrs(prefix, event, messageID, queue, topic, msgType string) []zlog.Attr {
	attrs := make([]zlog.Attr, 0, 6)
	attrs = append(attrs,
		zlog.String("prefix", prefix),
		zlog.String("event", event),
	)
	if messageID != "" {
		attrs = append(attrs, zlog.String("message_id", messageID))
	}
	if queue != "" {
		attrs = append(attrs, zlog.String("queue", queue))
	}
	if topic != "" {
		attrs = append(attrs, zlog.String("topic", topic))
	}
	if msgType != "" {
		attrs = append(attrs, zlog.String("type", msgType))
	}
	return attrs
}

func strAttr(k, v string) zlog.Attr {
	if v == "" {
		return zlog.Attr{}
	}
	return zlog.String(k, v)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
