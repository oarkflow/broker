package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func TestMemoryStateStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	st := broker.NewMemoryStateStore()
	if err := st.PutBrokerState(ctx, broker.BrokerStateRecord{Name: "b1", Mode: broker.ModeStandalone, State: broker.BrokerRunning}); err != nil {
		t.Fatal(err)
	}
	if rec, ok, err := st.GetBrokerState(ctx, "b1"); err != nil || !ok || rec.State != broker.BrokerRunning {
		t.Fatalf("broker state rec=%+v ok=%v err=%v", rec, ok, err)
	}
	task := broker.TaskInfo{ID: "task-1", Name: "pdf.generate", Queue: "tasks.pdf.generate", Status: broker.TaskQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := st.PutTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := st.GetTask(ctx, task.ID); err != nil || !ok || got.Status != broker.TaskQueued {
		t.Fatalf("task got=%+v ok=%v err=%v", got, ok, err)
	}
	consumer := broker.ConsumerInfo{ConsumerRegistration: broker.ConsumerRegistration{ConsumerID: "c1", Group: "g1"}, State: broker.ConsumerReady, LastHeartbeat: time.Now()}
	if err := st.PutConsumer(ctx, consumer); err != nil {
		t.Fatal(err)
	}
	if err := st.AddConsumerEvent(ctx, "c1", broker.Notification{ID: "evt-1", Type: "message.leased", Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	events, err := st.ConsumerEvents(ctx, "c1", "", 10)
	if err != nil || len(events) != 1 || events[0].ID != "evt-1" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	pub := broker.PublisherRecord{ID: "pub-1", Operation: "publish", Status: "succeeded", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := st.PutPublisherRecord(ctx, pub); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := st.GetPublisherRecord(ctx, pub.ID); err != nil || !ok || got.Status != "succeeded" {
		t.Fatalf("pub got=%+v ok=%v err=%v", got, ok, err)
	}
}
