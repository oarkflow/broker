package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AuditEvent represents a structured audit log entry.
type AuditEvent struct {
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Principal string         `json:"principal"`
	TenantID  string         `json:"tenant_id,omitempty"`
	Action    string         `json:"action"`
	Resource  string         `json:"resource"`
	Status    string         `json:"status"`
	Detail    string         `json:"detail,omitempty"`
	Error     string         `json:"error,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// AuditSeverity represents the severity of an audit event.
type AuditSeverity string

const (
	AuditInfo     AuditSeverity = "info"
	AuditWarn     AuditSeverity = "warn"
	AuditCritical AuditSeverity = "critical"
)

// AuditSink is the interface for writing audit events.
type AuditSink interface {
	Write(ctx context.Context, event AuditEvent) error
}

// AuditLogger provides structured audit logging for the broker.
type AuditLogger struct {
	mu     sync.RWMutex
	sinks  []AuditSink
	events []AuditEvent
	cap    int
}

// NewAuditLogger creates a new audit logger with the given capacity.
func NewAuditLogger(capacity int) *AuditLogger {
	if capacity <= 0 {
		capacity = 10000
	}
	return &AuditLogger{
		events: make([]AuditEvent, 0, capacity),
		cap:    capacity,
	}
}

// AddSink adds an audit sink for writing events.
func (a *AuditLogger) AddSink(sink AuditSink) {
	a.mu.Lock()
	a.sinks = append(a.sinks, sink)
	a.mu.Unlock()
}

// Log records an audit event and writes it to all sinks.
func (a *AuditLogger) Log(ctx context.Context, event AuditEvent) {
	if event.ID == "" {
		event.ID = fmt.Sprintf("audit-%s", newIDFromTime())
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.TenantID == "" {
		if p, ok := scopedPrincipalFromContext(ctx); ok && len(p.Tenants) == 1 && p.Tenants[0] != "*" {
			event.TenantID = p.Tenants[0]
		}
	}
	a.mu.Lock()
	a.events = append(a.events, event)
	if len(a.events) > a.cap {
		copy(a.events, a.events[len(a.events)-a.cap:])
		a.events = a.events[:a.cap]
	}
	sinks := make([]AuditSink, len(a.sinks))
	copy(sinks, a.sinks)
	a.mu.Unlock()

	for _, sink := range sinks {
		_ = sink.Write(ctx, event)
	}
}

// LogPublish records a publish audit event.
func (a *AuditLogger) LogPublish(ctx context.Context, principal, messageID, topic, queue string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    "message.publish",
		Resource:  fmt.Sprintf("topic=%s queue=%s", topic, queue),
		Status:    status,
		Metadata:  map[string]any{"message_id": messageID},
	})
}

// LogAck records an ack audit event.
func (a *AuditLogger) LogAck(ctx context.Context, principal, messageID, queue string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    "message.ack",
		Resource:  fmt.Sprintf("queue=%s id=%s", queue, messageID),
		Status:    status,
	})
}

// LogNack records a nack audit event.
func (a *AuditLogger) LogNack(ctx context.Context, principal, messageID, queue, action string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    "message.nack",
		Resource:  fmt.Sprintf("queue=%s id=%s", queue, messageID),
		Status:    status,
		Metadata:  map[string]any{"nack_action": action},
	})
}

// LogMove records a message movement audit event.
func (a *AuditLogger) LogMove(ctx context.Context, principal string, req MoveRequest, result MoveResult, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    "message.move",
		Resource:  fmt.Sprintf("%s/%s -> %s/%s", req.SourceType, req.Source, req.DestinationType, req.Destination),
		Status:    status,
		Metadata: map[string]any{
			"mode":    string(req.Mode),
			"moved":   result.Moved,
			"copied":  result.Copied,
			"skipped": result.Skipped,
		},
	})
}

// LogQueue records a queue operation audit event.
func (a *AuditLogger) LogQueue(ctx context.Context, principal, action, queue string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    fmt.Sprintf("queue.%s", action),
		Resource:  queue,
		Status:    status,
	})
}

// LogStream records a stream operation audit event.
func (a *AuditLogger) LogStream(ctx context.Context, principal, action, stream string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    fmt.Sprintf("stream.%s", action),
		Resource:  stream,
		Status:    status,
	})
}

// LogAuth records an authentication/authorization audit event.
func (a *AuditLogger) LogAuth(ctx context.Context, principal, action, resource string, allowed bool, detail string) {
	status := "allowed"
	if !allowed {
		status = "denied"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    action,
		Resource:  resource,
		Status:    status,
		Detail:    detail,
	})
}

// LogDLQ records a DLQ operation audit event.
func (a *AuditLogger) LogDLQ(ctx context.Context, principal, action, queue string, count int, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    fmt.Sprintf("dlq.%s", action),
		Resource:  queue,
		Status:    status,
		Metadata:  map[string]any{"count": count},
	})
}

// LogSchema records a schema validation audit event.
func (a *AuditLogger) LogSchema(ctx context.Context, principal, schema, version string, valid bool, detail string) {
	status := "valid"
	if !valid {
		status = "invalid"
	}
	a.Log(ctx, AuditEvent{
		Principal: principal,
		Action:    "schema.validate",
		Resource:  fmt.Sprintf("%s@%s", schema, version),
		Status:    status,
		Detail:    detail,
	})
}

// History returns recent audit events.
func (a *AuditLogger) History(limit int) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if limit <= 0 || limit > len(a.events) {
		limit = len(a.events)
	}
	out := make([]AuditEvent, limit)
	copy(out, a.events[len(a.events)-limit:])
	return out
}

// ConsoleAuditSink writes audit events to stdout.
type ConsoleAuditSink struct{}

func (s ConsoleAuditSink) Write(_ context.Context, event AuditEvent) error {
	fmt.Printf("[AUDIT] %s principal=%s action=%s resource=%s status=%s detail=%s error=%s\n",
		event.Timestamp.Format(time.RFC3339),
		event.Principal, event.Action, event.Resource, event.Status, event.Detail, event.Error)
	return nil
}

// NotificationAuditSink writes audit events to the notification bus.
type NotificationAuditSink struct {
	Bus *NotificationBus
}

func (s NotificationAuditSink) Write(ctx context.Context, event AuditEvent) error {
	if s.Bus == nil {
		return nil
	}
	severity := SeverityInfo
	if event.Status == "denied" || event.Status == "failure" || event.Status == "invalid" {
		severity = SeverityWarn
	}
	s.Bus.Publish(Notification{
		Type:      "audit." + event.Action,
		Source:    "audit",
		Severity:  severity,
		Timestamp: event.Timestamp,
		Data: map[string]any{
			"principal": event.Principal,
			"resource":  event.Resource,
			"status":    event.Status,
			"detail":    event.Detail,
			"error":     event.Error,
		},
	})
	return nil
}
