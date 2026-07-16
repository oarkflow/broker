package broker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// TraceStatus represents the status of a trace span.
type TraceStatus string

const (
	TraceOK    TraceStatus = "ok"
	TraceError TraceStatus = "error"
)

// TraceSpan represents a single trace span.
type TraceSpan struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id,omitempty"`
	Operation  string            `json:"operation"`
	Service    string            `json:"service"`
	StartTime  time.Time         `json:"start_time"`
	EndTime    time.Time         `json:"end_time,omitempty"`
	Duration   time.Duration     `json:"duration,omitempty"`
	Status     TraceStatus       `json:"status"`
	Error      string            `json:"error,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// TraceExporter is the interface for exporting trace spans.
type TraceExporter interface {
	ExportSpan(ctx context.Context, span TraceSpan) error
	Flush(ctx context.Context) error
}

// TracerConfig configures the broker's tracing system.
type TracerConfig struct {
	// Enabled activates tracing. Default: false.
	Enabled bool

	// ServiceName is the service name for traces. Default: "fastworker-broker".
	ServiceName string

	// SampleRate is the probability of sampling a trace (0.0-1.0). Default: 1.0.
	SampleRate float64

	// Exporter is the trace exporter. Default: no-op.
	Exporter TraceExporter

	// MaxSpans limits in-memory span buffer. Default: 10000.
	MaxSpans int
}

// Tracer provides distributed tracing capabilities for the broker.
type Tracer struct {
	cfg    TracerConfig
	spans  []TraceSpan
	mu     sync.RWMutex
	nextID uint64
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewTracer creates a new tracer with the given configuration.
func NewTracer(cfg TracerConfig) *Tracer {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "fastworker-broker"
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}
	if cfg.MaxSpans <= 0 {
		cfg.MaxSpans = 10000
	}
	return &Tracer{
		cfg:    cfg,
		spans:  make([]TraceSpan, 0, cfg.MaxSpans),
		stopCh: make(chan struct{}),
	}
}

// Start begins background span flushing.
func (t *Tracer) Start() {
	if t.cfg.Exporter == nil {
		return
	}
	t.wg.Add(1)
	go t.flushLoop()
}

// Stop halts tracing and flushes remaining spans.
func (t *Tracer) Stop() {
	close(t.stopCh)
	t.wg.Wait()
	if t.cfg.Exporter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = t.cfg.Exporter.Flush(ctx)
	}
}

func (t *Tracer) flushLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.flush()
		case <-t.stopCh:
			t.flush()
			return
		}
	}
}

func (t *Tracer) flush() {
	t.mu.Lock()
	if len(t.spans) == 0 {
		t.mu.Unlock()
		return
	}
	batch := t.spans
	t.spans = make([]TraceSpan, 0, t.cfg.MaxSpans)
	t.mu.Unlock()

	if t.cfg.Exporter == nil {
		return
	}
	ctx := context.Background()
	for _, span := range batch {
		_ = t.cfg.Exporter.ExportSpan(ctx, span)
	}
}

// StartSpan begins a new trace span.
func (t *Tracer) StartSpan(ctx context.Context, operation string, attrs map[string]string) (context.Context, *TraceSpan) {
	if !t.cfg.Enabled || t.cfg.SampleRate < 1.0 {
		return ctx, nil
	}
	traceID := TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = generateTraceID()
	}
	spanID := generateSpanID()
	parentSpanID := SpanIDFromContext(ctx)
	span := &TraceSpan{
		TraceID:    traceID,
		SpanID:     spanID,
		ParentID:   parentSpanID,
		Operation:  operation,
		Service:    t.cfg.ServiceName,
		StartTime:  time.Now(),
		Status:     TraceOK,
		Attributes: attrs,
	}
	ctx = context.WithValue(ctx, traceIDKey{}, traceID)
	ctx = context.WithValue(ctx, spanIDKey{}, spanID)
	return ctx, span
}

// EndSpan completes a trace span and records it.
func (t *Tracer) EndSpan(span *TraceSpan, err error) {
	if span == nil {
		return
	}
	span.EndTime = time.Now()
	span.Duration = span.EndTime.Sub(span.StartTime)
	if err != nil {
		span.Status = TraceError
		span.Error = err.Error()
	}
	t.mu.Lock()
	if len(t.spans) < t.cfg.MaxSpans {
		t.spans = append(t.spans, *span)
	}
	t.mu.Unlock()
}

// FinishWithErr completes a span, optionally recording an error.
func (t *Tracer) FinishWithErr(span *TraceSpan, err error) {
	t.EndSpan(span, err)
}

// Finish completes a span successfully.
func (t *Tracer) Finish(span *TraceSpan) {
	t.EndSpan(span, nil)
}

// Spans returns a copy of the current in-memory spans.
func (t *Tracer) Spans() []TraceSpan {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TraceSpan, len(t.spans))
	copy(out, t.spans)
	return out
}

// ClearSpans clears the in-memory span buffer.
func (t *Tracer) ClearSpans() {
	t.mu.Lock()
	t.spans = t.spans[:0]
	t.mu.Unlock()
}

type traceIDKey struct{}
type spanIDKey struct{}

// TraceIDFromContext extracts the trace ID from context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey{}).(string); ok {
		return v
	}
	return ""
}

// SpanIDFromContext extracts the span ID from context.
func SpanIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(spanIDKey{}).(string); ok {
		return v
	}
	return ""
}

func generateTraceID() string {
	return fmt.Sprintf("trace-%d-%s", time.Now().UnixNano(), randomHex(8))
}

func generateSpanID() string {
	return fmt.Sprintf("span-%d-%s", time.Now().UnixNano(), randomHex(6))
}

// InMemoryExporter stores trace spans in memory for debugging/testing.
type InMemoryExporter struct {
	mu    sync.RWMutex
	spans []TraceSpan
}

func NewInMemoryExporter() *InMemoryExporter {
	return &InMemoryExporter{spans: make([]TraceSpan, 0)}
}

func (e *InMemoryExporter) ExportSpan(_ context.Context, span TraceSpan) error {
	e.mu.Lock()
	e.spans = append(e.spans, span)
	e.mu.Unlock()
	return nil
}

func (e *InMemoryExporter) Flush(_ context.Context) error { return nil }

func (e *InMemoryExporter) Spans() []TraceSpan {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]TraceSpan, len(e.spans))
	copy(out, e.spans)
	return out
}

// MultiExporter exports to multiple exporters.
type MultiExporter struct {
	exporters []TraceExporter
}

func NewMultiExporter(exporters ...TraceExporter) *MultiExporter {
	return &MultiExporter{exporters: exporters}
}

func (m *MultiExporter) ExportSpan(ctx context.Context, span TraceSpan) error {
	var lastErr error
	for _, e := range m.exporters {
		if err := e.ExportSpan(ctx, span); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *MultiExporter) Flush(ctx context.Context) error {
	var lastErr error
	for _, e := range m.exporters {
		if err := e.Flush(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// TracingHooks returns a Hooks struct that instruments broker operations with tracing.
func (t *Tracer) TracingHooks() Hooks {
	return Hooks{
		BeforePublish: func(ctx context.Context, e PublishEvent) error {
			ctx, span := t.StartSpan(ctx, "publish", map[string]string{
				"topic":    e.Message.Topic,
				"queue":    e.Message.Queue,
				"type":     e.Message.Type,
				"tenant":   e.Message.TenantID,
				"trace_id": e.Message.TraceID,
			})
			e.Message.TraceID = TraceIDFromContext(ctx)
			e.Message.SpanID = SpanIDFromContext(ctx)
			_ = span
			return nil
		},
		AfterPublish: func(ctx context.Context, e PublishEvent) {
			t.Finish(nil)
		},
		BeforeHandle: func(ctx context.Context, e MessageEvent) error {
			ctx, span := t.StartSpan(ctx, "handle", map[string]string{
				"message_id": e.Message.ID,
				"queue":      e.Queue,
				"consumer":   e.Consumer,
			})
			_ = ctx
			_ = span
			return nil
		},
		AfterHandle: func(ctx context.Context, e MessageEvent) {
			t.Finish(nil)
		},
		OnHandlerError: func(ctx context.Context, e MessageEvent) {
			t.FinishWithErr(nil, e.Error)
		},
	}
}

// ConsoleExporter writes trace spans to an io.Writer for debugging.
type ConsoleExporter struct {
	// Prefix is prepended to each line. Default: "[TRACE] ".
	Prefix string
}

func NewConsoleExporter() *ConsoleExporter {
	return &ConsoleExporter{Prefix: "[TRACE] "}
}

func (e *ConsoleExporter) ExportSpan(_ context.Context, span TraceSpan) error {
	attrs := ""
	if len(span.Attributes) > 0 {
		parts := make([]string, 0, len(span.Attributes))
		for k, v := range span.Attributes {
			parts = append(parts, k+"="+v)
		}
		attrs = " " + strings.Join(parts, " ")
	}
	errStr := ""
	if span.Error != "" {
		errStr = " error=" + span.Error
	}
	fmt.Printf("%s trace=%s span=%s parent=%s op=%s service=%s status=%s duration=%s%s%s\n",
		e.Prefix, span.TraceID, span.SpanID, span.ParentID, span.Operation, span.Service,
		span.Status, span.Duration.Round(time.Microsecond), errStr, attrs)
	return nil
}

func (e *ConsoleExporter) Flush(_ context.Context) error { return nil }
