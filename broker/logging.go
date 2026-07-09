package broker

import (
	"context"

	"github.com/oarkflow/fastworker/logger"
	"github.com/oarkflow/zlog"
)

// LogNotifications writes broker notifications as structured log entries.
// Debug heartbeat events are intentionally skipped unless includeDebug is true.
func LogNotifications(bus *NotificationBus, id string, includeDebug bool) {
	if bus == nil {
		return
	}
	if id == "" {
		id = "broker-live-log"
	}
	bus.Subscribe(id, CallbackFunc(func(_ context.Context, n Notification) error {
		if n.Severity == SeverityDebug && !includeDebug {
			return nil
		}
		attrs := make([]zlog.Attr, 0, 6)
		attrs = append(attrs,
			zlog.String("type", n.Type),
			zlog.String("severity", string(n.Severity)),
		)
		if n.MessageID != "" {
			attrs = append(attrs, zlog.String("message_id", n.MessageID))
		}
		if n.TaskID != "" {
			attrs = append(attrs, zlog.String("task_id", n.TaskID))
		}
		if n.Topic != "" {
			attrs = append(attrs, zlog.String("topic", n.Topic))
		}
		if n.Queue != "" {
			attrs = append(attrs, zlog.String("queue", n.Queue))
		}
		if n.Consumer != "" {
			attrs = append(attrs, zlog.String("consumer", n.Consumer))
		}
		if n.ConsumerGroup != "" {
			attrs = append(attrs, zlog.String("group", n.ConsumerGroup))
		}
		if len(n.Data) > 0 {
			attrs = append(attrs, zlog.Any("data", n.Data))
		}
		logger.Log.Info("broker_event", attrs...)
		return nil
	}))
}
