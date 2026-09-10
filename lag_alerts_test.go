package broker

import (
	"context"
	"sync"
	"testing"
)

func TestLagMonitorStreamAndQueueAlerts(t *testing.T) {
	b := MustNew(Config{
		Store: NewMemoryStore(),
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()

	var receivedAlerts []LagAlert
	var mu sync.Mutex

	monitor := NewLagMonitor(b, LagAlertConfig{
		StreamThresholds: map[string]LagThreshold{
			"orders-stream": {Warning: 10, Critical: 25},
		},
		QueueThresholds: map[string]LagThreshold{
			"urgent-queue": {Warning: 5, Critical: 15},
		},
		OnAlert: func(alert LagAlert) {
			mu.Lock()
			receivedAlerts = append(receivedAlerts, alert)
			mu.Unlock()
		},
	})

	// 1. Declare and append to stream
	_ = b.Store().DeclareStream(ctx, StreamConfig{Name: "orders-stream"})
	for i := 0; i < 30; i++ {
		_, err := b.Publisher().Publish(ctx, "", "order", WithStream("orders-stream"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Commit consumer group offset at 2 (lag = 28)
	err := b.Store().CommitOffset(ctx, OffsetCommit{
		Stream:        "orders-stream",
		ConsumerGroup: "inventory-worker",
		Offset:        2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Append to queue (18 messages -> lag = 18, critical = 15)
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "urgent-queue"})
	for i := 0; i < 18; i++ {
		_, err := b.Publisher().Publish(ctx, "", "urgent-task", WithQueue("urgent-queue"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// 3. Run CheckLag
	alerts, err := monitor.CheckLag(ctx)
	if err != nil {
		t.Fatalf("CheckLag error: %v", err)
	}

	if len(alerts) < 2 {
		t.Fatalf("expected at least 2 alerts, got %d: %+v", len(alerts), alerts)
	}

	foundStreamAlert := false
	foundQueueAlert := false

	for _, a := range alerts {
		if a.Target == "orders-stream" && a.ConsumerGroup == "inventory-worker" {
			foundStreamAlert = true
			if a.Level != LagAlertCritical {
				t.Fatalf("expected critical stream alert, got %s", a.Level)
			}
			if a.CurrentLag != 28 {
				t.Fatalf("expected stream lag 28, got %d", a.CurrentLag)
			}
		}
		if a.Target == "urgent-queue" {
			foundQueueAlert = true
			if a.Level != LagAlertCritical {
				t.Fatalf("expected critical queue alert, got %s", a.Level)
			}
			if a.CurrentLag != 18 {
				t.Fatalf("expected queue lag 18, got %d", a.CurrentLag)
			}
		}
	}

	if !foundStreamAlert {
		t.Fatal("missing expected stream alert")
	}
	if !foundQueueAlert {
		t.Fatal("missing expected queue alert")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedAlerts) < 2 {
		t.Fatalf("expected OnAlert callbacks, got %d", len(receivedAlerts))
	}
}
