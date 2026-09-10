package broker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestOTLPExporterCollect(t *testing.T) {
	metrics := NewPrometheusMetrics()

	// Record some sample data
	metrics.RecordPublished("orders.topic", 128, 2*time.Millisecond)
	metrics.RecordDelivered("orders.queue", 128, 4*time.Millisecond)
	metrics.RecordAcked("orders.queue", 12*time.Millisecond)
	metrics.RecordNacked("orders.queue")
	metrics.RecordDeadLettered("orders.queue")
	metrics.RecordRetried()
	metrics.RecordMoved()
	metrics.RecordStoreFenced()
	metrics.SetActiveConsumers(5)
	metrics.SetActiveSubscriptions(3)
	metrics.UpdateStoreStats(StoreStats{
		Messages:   42,
		Leased:     2,
		DeadLetter: 1,
	})

	exp, err := NewOTLPExporter("http://localhost:4318", 10*time.Second, metrics,
		WithOTLPServiceName("test-broker"),
		WithOTLPResourceAttributes(map[string]string{
			"deployment.environment": "staging",
			"region":                 "us-east-1",
		}),
	)
	if err != nil {
		t.Fatalf("failed to create OTLPExporter: %v", err)
	}

	payload := exp.Collect()
	if len(payload.ResourceMetrics) != 1 {
		t.Fatalf("expected 1 ResourceMetrics, got %d", len(payload.ResourceMetrics))
	}

	rm := payload.ResourceMetrics[0]
	// Check resource attributes
	attrMap := make(map[string]string)
	for _, a := range rm.Resource.Attributes {
		attrMap[a.Key] = a.Value.StringValue
	}
	if attrMap["service.name"] != "test-broker" {
		t.Fatalf("expected service.name test-broker, got %s", attrMap["service.name"])
	}
	if attrMap["deployment.environment"] != "staging" {
		t.Fatalf("expected staging environment, got %s", attrMap["deployment.environment"])
	}

	// Check metrics
	if len(rm.ScopeMetrics) != 1 {
		t.Fatalf("expected 1 ScopeMetrics, got %d", len(rm.ScopeMetrics))
	}

	sm := rm.ScopeMetrics[0]
	metricMap := make(map[string]otlpMetric)
	for _, m := range sm.Metrics {
		metricMap[m.Name] = m
	}

	// Verify published sum counter
	pubMetric, ok := metricMap["broker.messages.published"]
	if !ok || pubMetric.Sum == nil || len(pubMetric.Sum.DataPoints) == 0 {
		t.Fatalf("missing or invalid broker.messages.published metric: %+v", pubMetric)
	}
	if pubMetric.Sum.DataPoints[0].AsInt != "1" {
		t.Fatalf("expected published count 1, got %s", pubMetric.Sum.DataPoints[0].AsInt)
	}

	// Verify active consumers gauge
	consumerMetric, ok := metricMap["broker.consumers.active"]
	if !ok || consumerMetric.Gauge == nil || len(consumerMetric.Gauge.DataPoints) == 0 {
		t.Fatalf("missing or invalid broker.consumers.active metric: %+v", consumerMetric)
	}
	if consumerMetric.Gauge.DataPoints[0].AsInt != "5" {
		t.Fatalf("expected active consumers 5, got %s", consumerMetric.Gauge.DataPoints[0].AsInt)
	}

	// Verify latency histogram
	histMetric, ok := metricMap["broker.publish.latency"]
	if !ok || histMetric.Histogram == nil || len(histMetric.Histogram.DataPoints) == 0 {
		t.Fatalf("missing or invalid broker.publish.latency histogram: %+v", histMetric)
	}
	hdp := histMetric.Histogram.DataPoints[0]
	if hdp.Count != "1" {
		t.Fatalf("expected 1 count in publish latency histogram, got %s", hdp.Count)
	}
	if len(hdp.ExplicitBounds) != 10 {
		t.Fatalf("expected 10 explicit bounds, got %d", len(hdp.ExplicitBounds))
	}
}

func TestOTLPExporterHTTPTransmission(t *testing.T) {
	var receivedPayload atomic.Pointer[OTLPExportRequest]
	var receivedAuthHeader atomic.Pointer[string]

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/v1/metrics" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		auth := r.Header.Get("Authorization")
		receivedAuthHeader.Store(&auth)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var req OTLPExportRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		receivedPayload.Store(&req)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	metrics := NewPrometheusMetrics()
	metrics.RecordPublished("test.topic", 64, 1*time.Millisecond)

	exp, err := NewOTLPExporter(server.URL, 50*time.Millisecond, metrics,
		WithOTLPServiceName("trans-test"),
		WithOTLPHeaders(map[string]string{
			"Authorization": "Bearer otlp-token-999",
		}),
	)
	if err != nil {
		t.Fatalf("failed to create exporter: %v", err)
	}

	// Test Export single shot
	ctx := context.Background()
	if err := exp.Export(ctx); err != nil {
		t.Fatalf("export failed: %v", err)
	}

	auth := receivedAuthHeader.Load()
	if auth == nil || *auth != "Bearer otlp-token-999" {
		t.Fatalf("expected auth header Bearer otlp-token-999, got %v", auth)
	}

	payload := receivedPayload.Load()
	if payload == nil || len(payload.ResourceMetrics) == 0 {
		t.Fatalf("expected valid payload transmitted to server")
	}

	// Test background Run loop
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		exp.Run(runCtx)
		close(done)
	}()

	time.Sleep(120 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("OTLPExporter Run did not exit within 1s after cancel")
	}
}

func TestBrokerOTLPIntegration(t *testing.T) {
	var exportCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exportCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := MustNew(Config{
		Name:         "broker-otlp-test",
		Store:        NewMemoryStore(),
		OTLPEndpoint: server.URL,
		OTLPInterval: 30 * time.Millisecond,
	})

	if b.OTLPExporter() == nil {
		t.Fatal("expected OTLPExporter to be initialized on broker")
	}

	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Publish message to trigger stats
	_, _ = b.Store().Append(ctx, Message{ID: "m1", Queue: "default"}, []Destination{{Kind: DestinationQueue, Name: "default"}})
	time.Sleep(100 * time.Millisecond)

	_ = b.Stop(ctx)

	if exportCount.Load() == 0 {
		t.Fatalf("expected at least 1 OTLP export from broker background runner, got %d", exportCount.Load())
	}
}
