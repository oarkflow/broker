package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// OTLPConfig configures the OpenTelemetry Protocol exporter.
type OTLPConfig struct {
	Endpoint           string            `json:"endpoint"`
	Interval           time.Duration     `json:"interval"`
	ServiceName        string            `json:"service_name"`
	ResourceAttributes map[string]string `json:"resource_attributes"`
	Headers            map[string]string `json:"headers"`
	Client             *http.Client      `json:"-"`
}

// OTLPOption configures an OTLPConfig.
type OTLPOption func(*OTLPConfig)

// WithOTLPServiceName sets the service.name resource attribute.
func WithOTLPServiceName(name string) OTLPOption {
	return func(c *OTLPConfig) {
		c.ServiceName = name
	}
}

// WithOTLPResourceAttributes sets custom resource attributes.
func WithOTLPResourceAttributes(attrs map[string]string) OTLPOption {
	return func(c *OTLPConfig) {
		if c.ResourceAttributes == nil {
			c.ResourceAttributes = make(map[string]string)
		}
		for k, v := range attrs {
			c.ResourceAttributes[k] = v
		}
	}
}

// WithOTLPHeaders sets HTTP request headers for export.
func WithOTLPHeaders(headers map[string]string) OTLPOption {
	return func(c *OTLPConfig) {
		if c.Headers == nil {
			c.Headers = make(map[string]string)
		}
		for k, v := range headers {
			c.Headers[k] = v
		}
	}
}

// WithOTLPHTTPClient sets a custom HTTP client.
func WithOTLPHTTPClient(client *http.Client) OTLPOption {
	return func(c *OTLPConfig) {
		c.Client = client
	}
}

// OTLPExporter pushes broker metrics to an OTEL collector via OTLP/HTTP.
type OTLPExporter struct {
	cfg       OTLPConfig
	metrics   *PrometheusMetrics
	startTime time.Time
	stopCh    chan struct{}
}

// NewOTLPExporter creates a new OTLP metrics exporter.
func NewOTLPExporter(endpoint string, interval time.Duration, metrics *PrometheusMetrics, opts ...OTLPOption) (*OTLPExporter, error) {
	if endpoint == "" {
		endpoint = os.Getenv("BROKER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("otlp endpoint cannot be empty")
	}

	// Normalize endpoint URL
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	if !strings.HasSuffix(endpoint, "/v1/metrics") {
		endpoint = strings.TrimSuffix(endpoint, "/") + "/v1/metrics"
	}

	if interval <= 0 {
		interval = 15 * time.Second
	}
	if metrics == nil {
		metrics = GlobalPrometheusMetrics()
	}

	cfg := OTLPConfig{
		Endpoint:           endpoint,
		Interval:           interval,
		ServiceName:        "broker",
		ResourceAttributes: make(map[string]string),
		Headers:            make(map[string]string),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}

	return &OTLPExporter{
		cfg:       cfg,
		metrics:   metrics,
		startTime: time.Now(),
		stopCh:    make(chan struct{}),
	}, nil
}

// Stop terminates the exporter background loop.
func (e *OTLPExporter) Stop() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
}

// Run executes periodic exports until context cancellation or Stop().
func (e *OTLPExporter) Run(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			exportCtx, cancel := context.WithTimeout(ctx, e.cfg.Interval)
			if err := e.Export(exportCtx); err != nil && ctx.Err() == nil {
				log.Printf("[OTLP] export error: %v", err)
			}
			cancel()
		}
	}
}

// Export collects and transmits metrics to the OTLP receiver.
func (e *OTLPExporter) Export(ctx context.Context) error {
	payload := e.Collect()
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal otlp payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create otlp request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("post otlp metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("otlp endpoint returned status %d: %s", resp.StatusCode, string(body))
}

// OTLP JSON data structures according to OpenTelemetry Protocol specification.

type otlpResourceAttribute struct {
	Key   string          `json:"key"`
	Value otlpStringValue `json:"value"`
}

type otlpStringValue struct {
	StringValue string `json:"stringValue"`
}

type otlpResource struct {
	Attributes []otlpResourceAttribute `json:"attributes"`
}

type otlpInstrumentationScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type otlpDataPoint struct {
	StartTimeUnixNano string                  `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string                  `json:"timeUnixNano"`
	AsInt             string                  `json:"asInt,omitempty"`
	AsDouble          float64                 `json:"asDouble,omitempty"`
	Attributes        []otlpResourceAttribute `json:"attributes,omitempty"`
}

type otlpHistogramDataPoint struct {
	StartTimeUnixNano string                  `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string                  `json:"timeUnixNano"`
	Count             string                  `json:"count"`
	Sum               float64                 `json:"sum"`
	BucketCounts      []string                `json:"bucketCounts"`
	ExplicitBounds    []float64               `json:"explicitBounds"`
	Attributes        []otlpResourceAttribute `json:"attributes,omitempty"`
}

type otlpSum struct {
	AggregationTemporality int             `json:"aggregationTemporality"` // 2 = CUMULATIVE
	IsMonotonic            bool            `json:"isMonotonic"`
	DataPoints             []otlpDataPoint `json:"dataPoints"`
}

type otlpGauge struct {
	DataPoints []otlpDataPoint `json:"dataPoints"`
}

type otlpHistogram struct {
	AggregationTemporality int                      `json:"aggregationTemporality"`
	DataPoints             []otlpHistogramDataPoint `json:"dataPoints"`
}

type otlpMetric struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Unit        string         `json:"unit,omitempty"`
	Gauge       *otlpGauge     `json:"gauge,omitempty"`
	Sum         *otlpSum       `json:"sum,omitempty"`
	Histogram   *otlpHistogram `json:"histogram,omitempty"`
}

type otlpScopeMetrics struct {
	Scope   otlpInstrumentationScope `json:"scope"`
	Metrics []otlpMetric             `json:"metrics"`
}

type otlpResourceMetrics struct {
	Resource     otlpResource       `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

// OTLPExportRequest is the top-level body for /v1/metrics
type OTLPExportRequest struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

// Collect reads the current PrometheusMetrics and converts them to OTLP JSON structure.
func (e *OTLPExporter) Collect() *OTLPExportRequest {
	m := e.metrics
	m.UpdateGauges()

	nowNano := strconv.FormatInt(time.Now().UnixNano(), 10)
	startNano := strconv.FormatInt(e.startTime.UnixNano(), 10)

	// Build resource attributes
	attrs := []otlpResourceAttribute{
		{Key: "service.name", Value: otlpStringValue{StringValue: e.cfg.ServiceName}},
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		attrs = append(attrs, otlpResourceAttribute{Key: "host.name", Value: otlpStringValue{StringValue: host}})
	}
	for k, v := range e.cfg.ResourceAttributes {
		attrs = append(attrs, otlpResourceAttribute{Key: k, Value: otlpStringValue{StringValue: v}})
	}

	var metrics []otlpMetric

	// Helper for cumulative sum counter
	addSum := func(name, desc, unit string, val uint64, extraAttrs ...otlpResourceAttribute) {
		metrics = append(metrics, otlpMetric{
			Name:        name,
			Description: desc,
			Unit:        unit,
			Sum: &otlpSum{
				AggregationTemporality: 2, // CUMULATIVE
				IsMonotonic:            true,
				DataPoints: []otlpDataPoint{
					{
						StartTimeUnixNano: startNano,
						TimeUnixNano:      nowNano,
						AsInt:             strconv.FormatUint(val, 10),
						Attributes:        extraAttrs,
					},
				},
			},
		})
	}

	// Helper for gauge
	addGauge := func(name, desc, unit string, val int64, extraAttrs ...otlpResourceAttribute) {
		metrics = append(metrics, otlpMetric{
			Name:        name,
			Description: desc,
			Unit:        unit,
			Gauge: &otlpGauge{
				DataPoints: []otlpDataPoint{
					{
						TimeUnixNano: nowNano,
						AsInt:        strconv.FormatInt(val, 10),
						Attributes:   extraAttrs,
					},
				},
			},
		})
	}

	// Helper for histogram
	addHistogram := func(name, desc, unit string, buckets *[10]atomic.Uint64) {
		explicitBounds := []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000, 5.000}
		bucketCounts := make([]string, 10)
		var totalCount uint64
		var sumSeconds float64

		// Approximation of sum using bucket medians
		medians := []float64{0.0005, 0.003, 0.0075, 0.0175, 0.0375, 0.075, 0.175, 0.375, 0.75, 3.0}
		for i := 0; i < 10; i++ {
			c := buckets[i].Load()
			bucketCounts[i] = strconv.FormatUint(c, 10)
			totalCount += c
			sumSeconds += float64(c) * medians[i]
		}

		metrics = append(metrics, otlpMetric{
			Name:        name,
			Description: desc,
			Unit:        unit,
			Histogram: &otlpHistogram{
				AggregationTemporality: 2,
				DataPoints: []otlpHistogramDataPoint{
					{
						StartTimeUnixNano: startNano,
						TimeUnixNano:      nowNano,
						Count:             strconv.FormatUint(totalCount, 10),
						Sum:               sumSeconds,
						BucketCounts:      bucketCounts,
						ExplicitBounds:    explicitBounds,
					},
				},
			},
		})
	}

	// Counters
	addSum("broker.messages.published", "Total messages published", "1", m.published.Load())
	addSum("broker.messages.delivered", "Total messages delivered", "1", m.delivered.Load())
	addSum("broker.messages.acked", "Total messages acknowledged", "1", m.acked.Load())
	addSum("broker.messages.nacked", "Total messages rejected/nacked", "1", m.nacked.Load())
	addSum("broker.messages.dead_lettered", "Total messages routed to dead-letter", "1", m.deadLettered.Load())
	addSum("broker.messages.retried", "Total message delivery retries", "1", m.retried.Load())
	addSum("broker.messages.moved", "Total messages moved across destinations", "1", m.moved.Load())
	addSum("broker.store.fenced", "Total storage fencing occurrences", "1", m.storeFenced.Load())
	addSum("broker.bytes.published", "Total payload bytes published", "By", m.publishedBytes.Load())
	addSum("broker.bytes.delivered", "Total payload bytes delivered", "By", m.deliveredBytes.Load())

	// Gauges
	addGauge("broker.consumers.active", "Active registered consumers", "1", m.activeConsumers.Load())
	addGauge("broker.subscriptions.active", "Active subscriptions", "1", m.activeSubscriptions.Load())
	addGauge("broker.store.messages", "Total messages stored", "1", m.storeMessages.Load())
	addGauge("broker.store.leased", "Total leased messages", "1", m.storeLeased.Load())
	addGauge("broker.store.dead_letter", "Total dead-letter messages stored", "1", m.storeDeadLetter.Load())
	addGauge("broker.runtime.goroutines", "Number of Go goroutines", "1", m.goroutines.Load())
	addGauge("broker.runtime.memory_alloc_bytes", "Memory allocated in bytes", "By", m.memoryAllocBytes.Load())
	addGauge("broker.rate.published", "Instantaneous message publish rate", "1/s", int64(m.publishRate.Load()))
	addGauge("broker.rate.consumed", "Instantaneous message consume rate", "1/s", int64(m.consumeRate.Load()))

	// Per-queue metrics
	m.queueDepth.Range(func(k, v any) bool {
		qName := k.(string)
		val := v.(*atomic.Int64).Load()
		addGauge("broker.queue.depth", "Current queue depth", "1", val,
			otlpResourceAttribute{Key: "queue", Value: otlpStringValue{StringValue: qName}})
		return true
	})
	m.queueAcked.Range(func(k, v any) bool {
		qName := k.(string)
		val := v.(*atomic.Uint64).Load()
		addSum("broker.queue.acked", "Messages acknowledged per queue", "1", val,
			otlpResourceAttribute{Key: "queue", Value: otlpStringValue{StringValue: qName}})
		return true
	})
	m.queueNacked.Range(func(k, v any) bool {
		qName := k.(string)
		val := v.(*atomic.Uint64).Load()
		addSum("broker.queue.nacked", "Messages rejected per queue", "1", val,
			otlpResourceAttribute{Key: "queue", Value: otlpStringValue{StringValue: qName}})
		return true
	})
	m.queueDLQ.Range(func(k, v any) bool {
		qName := k.(string)
		val := v.(*atomic.Uint64).Load()
		addSum("broker.queue.dlq", "Messages dead-lettered per queue", "1", val,
			otlpResourceAttribute{Key: "queue", Value: otlpStringValue{StringValue: qName}})
		return true
	})

	// Per-topic metrics
	m.topicPublished.Range(func(k, v any) bool {
		tName := k.(string)
		val := v.(*atomic.Uint64).Load()
		addSum("broker.topic.published", "Messages published per topic", "1", val,
			otlpResourceAttribute{Key: "topic", Value: otlpStringValue{StringValue: tName}})
		return true
	})

	// Latency histograms
	addHistogram("broker.publish.latency", "Message publish duration", "s", &m.publishLatencyBuckets)
	addHistogram("broker.consume.latency", "Message delivery duration", "s", &m.consumeLatencyBuckets)
	addHistogram("broker.ack.latency", "Message ack processing duration", "s", &m.ackLatencyBuckets)

	return &OTLPExportRequest{
		ResourceMetrics: []otlpResourceMetrics{
			{
				Resource: otlpResource{
					Attributes: attrs,
				},
				ScopeMetrics: []otlpScopeMetrics{
					{
						Scope: otlpInstrumentationScope{
							Name:    "github.com/oarkflow/broker",
							Version: "1.0.0",
						},
						Metrics: metrics,
					},
				},
			},
		},
	}
}
