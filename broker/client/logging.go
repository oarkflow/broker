package client

import (
	"log"
	"time"
)

// LoggingHooks returns lifecycle hooks that print compact, useful remote-consumer logs.
func LoggingHooks(prefix string) Hooks {
	return LoggingHooksWith(prefix, log.Printf)
}

func LoggingHooksWith(prefix string, logf func(string, ...any)) Hooks {
	if prefix == "" {
		prefix = "worker"
	}
	if logf == nil {
		logf = log.Printf
	}
	p := func(format string, args ...any) {
		logf(prefix+" "+format, args...)
	}
	return Hooks{
		OnConnecting:    func(e Event) { p("lifecycle=connecting consumer=%s group=%s", e.ConsumerID, e.Group) },
		OnConnected:     func(e Event) { p("lifecycle=connected consumer=%s group=%s", e.ConsumerID, e.Group) },
		OnAuthenticated: func(e Event) { p("lifecycle=authenticated consumer=%s group=%s", e.ConsumerID, e.Group) },
		OnRegistered:    func(e Event) { p("lifecycle=registered consumer=%s group=%s state=%s", e.ConsumerID, e.Group, e.State) },
		OnSubscribed:    func(e Event) { p("lifecycle=subscribed consumer=%s group=%s state=%s", e.ConsumerID, e.Group, e.State) },
		OnPollStart:     func(e Event) { p("lifecycle=polling consumer=%s group=%s", e.ConsumerID, e.Group) },
		OnHeartbeat:     func(e Event) { p("lifecycle=heartbeat consumer=%s group=%s state=%s", e.ConsumerID, e.Group, e.State) },
		OnReconnect:     func(e Event) { p("lifecycle=reconnect consumer=%s group=%s error=%v", e.ConsumerID, e.Group, e.Error) },
		OnDrain:         func(e Event) { p("lifecycle=drain consumer=%s group=%s state=%s", e.ConsumerID, e.Group, e.State) },
		OnStopped:       func(e Event) { p("lifecycle=stopped consumer=%s group=%s state=%s", e.ConsumerID, e.Group, e.State) },
		OnNotification: func(e NotificationEvent) {
			p("notification type=%s severity=%s message=%s task=%s queue=%s topic=%s data=%v", e.Notification.Type, e.Notification.Severity, e.Notification.MessageID, e.Notification.TaskID, e.Notification.Queue, e.Notification.Topic, e.Notification.Data)
		},
		OnMessageReceived: func(e MessageEvent) {
			p("message=received id=%s queue=%s topic=%s type=%s attempt=%d lease_until=%s", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type, e.Message.Attempts, formatTime(e.Message.LeaseUntil))
		},
		BeforeHandle: func(e MessageEvent) error {
			p("message=handler.start id=%s queue=%s topic=%s type=%s", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
			return nil
		},
		AfterHandle: func(e MessageEvent) {
			p("message=handler.done id=%s queue=%s topic=%s type=%s", e.Message.ID, e.Queue, e.Message.Topic, e.Message.Type)
		},
		OnHandlerError: func(e MessageEvent) {
			p("message=handler.error id=%s queue=%s topic=%s error=%v", e.Message.ID, e.Queue, e.Message.Topic, e.Error)
		},
		OnAck:           func(e MessageEvent) { p("message=ack id=%s queue=%s", e.Message.ID, e.Queue) },
		OnNack:          func(e MessageEvent) { p("message=nack id=%s queue=%s error=%v", e.Message.ID, e.Queue, e.Error) },
		OnReject:        func(e MessageEvent) { p("message=reject id=%s queue=%s error=%v", e.Message.ID, e.Queue, e.Error) },
		OnProgress:      func(e MessageEvent) { p("message=progress id=%s queue=%s", e.Message.ID, e.Queue) },
		OnLeaseExtended: func(e MessageEvent) { p("message=lease.extend id=%s queue=%s", e.Message.ID, e.Queue) },
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
