package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oarkflow/broker/cluster"
)

type queueControlLookupStore struct {
	*MemoryStore
	denyQueueLookup bool
}

func (s *queueControlLookupStore) LookupQueueConfig(ctx context.Context, name string) (QueueConfig, bool, error) {
	if s.denyQueueLookup && name == "jobs" {
		return QueueConfig{}, false, nil
	}
	return s.MemoryStore.LookupQueueConfig(ctx, name)
}

func TestAssignedQueuePauseResumeUseDurableAllPartitionTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stores := make(map[int]*MemoryStore)
	partitionStores := make(map[int]*queueControlLookupStore)
	db, err := NewDistributed(Config{NodeID: "queue-control-node", Store: NewMemoryStore()}, DistributedConfig{
		UseEtcdRaft: true, WALDir: t.TempDir(), RaftNodeID: 1,
		ReplicationFactor: 1, MinInSyncReplicas: 1, SubmitTimeout: 2 * time.Second,
		PartitionStoreFactory: func(_ context.Context, partition *cluster.PartitionMetadata) (Store, error) {
			store := NewMemoryStore()
			stores[partition.ID.Partition] = store
			wrapped := &queueControlLookupStore{MemoryStore: store}
			partitionStores[partition.ID.Partition] = wrapped
			return wrapped, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer db.Stop(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for !db.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !db.IsLeader() {
		t.Fatal("single-node controller did not elect a leader")
	}
	if err := db.ControllerQuorum().RegisterNode(ctx, &cluster.NodeInfo{
		ID: "queue-control-node", Address: "queue-control-node", ClientAddress: "https://queue-control-node.test", State: cluster.NodeActive,
	}); err != nil {
		t.Fatal(err)
	}
	for partition := 0; partition < 3; partition++ {
		if err := db.AssignResourcePartition(ctx, cluster.PartitionResourceQueue, "jobs", partition, []string{"queue-control-node"}, "queue-control-node"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.ReconcilePartitionGroups(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for partition := 0; partition < 3; partition++ {
		groupID := fmt.Sprintf("partition/queue/jobs/%d", partition)
		for {
			group, ok := db.RaftGroup(groupID)
			if ok && group.IsLeader() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("partition group %q did not elect a leader", groupID)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := stores[partition].Append(ctx, Message{ID: fmt.Sprintf("job-%d", partition), Queue: "jobs", Partition: partition}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); err != nil {
			t.Fatal(err)
		}
	}

	partitionStores[2].denyQueueLookup = true
	aborted, err := db.ApplyAssignedQueueControl(ctx, "jobs", "pause")
	if err == nil || aborted == nil || aborted.TransactionID == "" {
		t.Fatalf("pause with an undeclared partition queue = result=%+v err=%v, want failed transaction", aborted, err)
	}
	abortRecord, readErr := db.ControllerQuorum().GetTransactionLinearizable(ctx, aborted.TransactionID)
	if readErr != nil || abortRecord.Decision != cluster.TransactionAbort || len(abortRecord.Resolved) != 3 {
		t.Fatalf("failed prepare transaction = %+v err=%v, want fully resolved abort", abortRecord, readErr)
	}
	for partition := 0; partition < 2; partition++ {
		stores[partition].mu.RLock()
		queue := stores[partition].queues["jobs"]
		unchanged := queue != nil && queue.state == QueueActive && queue.depth == 1
		stores[partition].mu.RUnlock()
		if !unchanged {
			t.Fatalf("partition %d changed after aborted prepare", partition)
		}
	}
	stores[2].mu.RLock()
	queue := stores[2].queues["jobs"]
	unchanged := queue != nil && queue.state == QueueActive && queue.depth == 1
	stores[2].mu.RUnlock()
	if !unchanged {
		t.Fatal("partition 2 changed after its prepare was rejected")
	}
	partitionStores[2].denyQueueLookup = false

	paused, err := db.ApplyAssignedQueueControl(ctx, "jobs", "pause")
	if err != nil {
		t.Fatalf("atomic pause: result=%+v err=%v", paused, err)
	}
	if paused.TransactionID == "" || len(paused.Completed) != 3 {
		t.Fatalf("pause result = %+v, want transaction ID and all partitions complete", paused)
	}
	commit, err := db.ControllerQuorum().GetTransactionLinearizable(ctx, paused.TransactionID)
	if err != nil || commit.Decision != cluster.TransactionCommit || len(commit.Resolved) != 3 {
		t.Fatalf("pause transaction = %+v err=%v, want committed and fully resolved", commit, err)
	}
	for partition, store := range stores {
		leased, err := store.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "worker", Limit: 1, VisibilityTimeout: time.Second})
		if !errors.Is(err, ErrPaused) || len(leased) != 0 {
			t.Fatalf("partition %d after pause: leased=%v err=%v", partition, leased, err)
		}
	}

	resumed, err := db.ApplyAssignedQueueControl(ctx, "jobs", "resume")
	if err != nil {
		t.Fatalf("atomic resume: result=%+v err=%v", resumed, err)
	}
	if resumed.TransactionID == "" || len(resumed.Completed) != 3 {
		t.Fatalf("resume result = %+v, want transaction ID and all partitions complete", resumed)
	}
	for partition, store := range stores {
		leased, err := store.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "worker", Limit: 1, VisibilityTimeout: time.Second})
		if err != nil || len(leased) != 1 {
			t.Fatalf("partition %d after resume: leased=%v err=%v", partition, leased, err)
		}
	}

	purged, err := db.ApplyAssignedQueueControl(ctx, "jobs", "purge")
	if err != nil {
		t.Fatalf("atomic purge: result=%+v err=%v", purged, err)
	}
	if purged.TransactionID == "" || len(purged.Completed) != 3 || len(purged.Purged) != 3 {
		t.Fatalf("purge result = %+v, want transaction ID, all partitions complete, and exact counts", purged)
	}
	commit, err = db.ControllerQuorum().GetTransactionLinearizable(ctx, purged.TransactionID)
	if err != nil || commit.Decision != cluster.TransactionCommit || len(commit.Resolved) != 3 {
		t.Fatalf("purge transaction = %+v err=%v, want committed and fully resolved", commit, err)
	}
	for partition, count := range purged.Purged {
		if count != 1 {
			t.Fatalf("purge count for partition %d = %d, want 1", partition, count)
		}
	}
}
