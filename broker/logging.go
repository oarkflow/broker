package broker

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// LogNotifications writes broker notifications as compact live operational logs.
// Debug heartbeat events are intentionally skipped unless includeDebug is true.
func LogNotifications(bus *NotificationBus, id string, includeDebug bool, logf func(string, ...any)) {
	if bus == nil {
		return
	}
	if id == "" {
		id = "broker-live-log"
	}
	if logf == nil {
		logf = log.Printf
	}
	bus.Subscribe(id, CallbackFunc(func(_ context.Context, n Notification) error {
		if n.Severity == SeverityDebug && !includeDebug {
			return nil
		}
		logf("broker event=%s severity=%s message=%s task=%s topic=%s queue=%s consumer=%s group=%s data=%s", n.Type, n.Severity, n.MessageID, n.TaskID, n.Topic, n.Queue, n.Consumer, n.ConsumerGroup, formatNotificationData(n.Data))
		return nil
	}))
}

func formatNotificationData(data map[string]any) string {
	if len(data) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, formatNotificationValue(data[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatNotificationValue(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x.Format(time.RFC3339Nano)
	default:
		return x
	}
}
