package broker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/broker/cluster"
)

type failOnceTransactionResolver struct {
	delegate cluster.TransactionParticipantResolver
	failedID string
	failed   bool
}

func (r *failOnceTransactionResolver) CommitTransaction(ctx context.Context, txID, participantID string) error {
	if participantID == r.failedID && !r.failed {
		r.failed = true
		return errors.New("injected source resolution interruption")
	}
	return r.delegate.CommitTransaction(ctx, txID, participantID)
}

func (r *failOnceTransactionResolver) AbortTransaction(ctx context.Context, txID, participantID string) error {
	return r.delegate.AbortTransaction(ctx, txID, participantID)
}

func (r *failOnceTransactionResolver) ForgetTransaction(ctx context.Context, txID, participantID string) error {
	if forgetter, ok := r.delegate.(cluster.TransactionParticipantForgetter); ok {
		return forgetter.ForgetTransaction(ctx, txID, participantID)
	}
	return nil
}

func TestDistributedOffsetAppendRecoversSourceAfterOutputRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stores := make(map[string]*MemoryStore)
	db, err := NewDistributed(Config{NodeID: "tx-recovery-node", Store: NewMemoryStore()}, DistributedConfig{
		UseEtcdRaft: true, WALDir: t.TempDir(), RaftNodeID: 1,
		ReplicationFactor: 1, MinInSyncReplicas: 1, SubmitTimeout: 2 * time.Second,
		PartitionStoreFactory: func(_ context.Context, partition *cluster.PartitionMetadata) (Store, error) {
			store := NewMemoryStore()
			stores["partition/"+partition.ID.String()] = store
			return store, nil
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
		ID: "tx-recovery-node", Address: "tx-recovery-node", ClientAddress: "https://tx-recovery-node.test", State: cluster.NodeActive,
	}); err != nil {
		t.Fatal(err)
	}
	for _, partition := range []int{0, 1} {
		if err := db.AssignResourcePartition(ctx, cluster.PartitionResourceStream, "events", partition, []string{"tx-recovery-node"}, "tx-recovery-node"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.ReconcilePartitionGroups(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, groupID := range []string{"partition/stream/events/0", "partition/stream/events/1"} {
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
	}

	commit := OffsetCommit{Stream: "events", ConsumerGroup: "cg", Offset: 12, Partitioned: true, Partition: 0, TransactionID: "client-tx-retention"}
	entries := []AppendEntry{{
		Message:      Message{ID: "output-retained", Stream: "events", Partition: 1},
		Destinations: []Destination{{Kind: DestinationStream, Name: "events"}},
	}}
	commit, entries, fingerprint, err := prepareOffsetAppendRequest(ctx, commit, entries)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorID := distributedOffsetAppendCoordinatorID(commit)
	deadline = time.Now().Add(10 * time.Second)
	sourceID := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: "events", Partition: 0}.Canonical()
	outputID := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: "events", Partition: 1}.Canonical()
	outputParticipantID := PartitionTransactionParticipantID(outputID)
	sourceParticipantID := PartitionOffsetTransactionParticipantID(sourceID)
	participantIDs := []string{outputParticipantID, sourceParticipantID}
	if err := db.ControllerQuorum().BeginTransactionWithFingerprint(ctx, coordinatorID, participantIDs, deadline, fingerprint); err != nil {
		t.Fatal(err)
	}
	tx, err := db.ControllerQuorum().GetTransactionLinearizable(ctx, coordinatorID)
	if err != nil {
		t.Fatal(err)
	}
	outputParticipant, err := db.NewPartitionAppendTransactionParticipant(ctx, outputID, entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := outputParticipant.PrepareTransaction(ctx, coordinatorID, tx.Deadline); err != nil {
		t.Fatal(err)
	}
	if err := db.ControllerQuorum().PrepareTransactionParticipant(ctx, coordinatorID, outputParticipantID); err != nil {
		t.Fatal(err)
	}
	resultParticipant, ok := outputParticipant.(PartitionTransactionResult)
	if !ok {
		t.Fatal("output participant does not expose prepared output")
	}
	prepared := resultParticipant.PreparedMessages()
	if len(prepared) != 1 {
		t.Fatalf("prepared output count = %d, want 1", len(prepared))
	}
	outputs := []OffsetAppendOutputReference{{Kind: DestinationStream, Name: "events", Partition: 1, MessageID: prepared[0].ID}}
	sourceParticipant, err := db.newPartitionOffsetOutcomeTransactionParticipant(ctx, sourceID, coordinatorID, commit, fingerprint, tx.CreatedAt, prepared, outputs)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceParticipant.PrepareTransaction(ctx, coordinatorID, tx.Deadline); err != nil {
		t.Fatal(err)
	}
	if err := db.ControllerQuorum().PrepareTransactionParticipant(ctx, coordinatorID, sourceParticipantID); err != nil {
		t.Fatal(err)
	}
	if err := db.ControllerQuorum().DecideTransaction(ctx, coordinatorID, true); err != nil {
		t.Fatal(err)
	}

	resolver := &failOnceTransactionResolver{delegate: db.TransactionParticipantResolver(), failedID: sourceParticipantID}
	if err := db.ControllerQuorum().RecoverTransaction(ctx, coordinatorID, resolver); err == nil || !resolver.failed {
		t.Fatalf("first recovery error = %v, injected failure observed = %t; want source resolution interruption", err, resolver.failed)
	}
	interrupted, err := db.ControllerQuorum().GetTransactionLinearizable(ctx, coordinatorID)
	if err != nil {
		t.Fatal(err)
	}
	if !sortedStringContains(interrupted.Resolved, outputParticipantID) || sortedStringContains(interrupted.Resolved, sourceParticipantID) {
		t.Fatalf("resolved participants after interruption = %v, want output only (source %s)", interrupted.Resolved, sourceParticipantID)
	}

	outputStore := stores["partition/stream/events/1"]
	if outputStore == nil {
		t.Fatal("output partition store was not created")
	}
	outputStore.mu.Lock()
	delete(outputStore.messages, prepared[0].ID)
	outputStore.mu.Unlock()
	if _, ok, err := outputStore.Get(ctx, prepared[0].ID); err != nil || ok {
		t.Fatalf("retained message lookup: exists=%t err=%v, want message removed", ok, err)
	}

	if err := db.ControllerQuorum().RecoverTransaction(ctx, coordinatorID, db.TransactionParticipantResolver()); err != nil {
		t.Fatal(err)
	}
	completed, err := db.ControllerQuorum().GetTransactionLinearizable(ctx, coordinatorID)
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Resolved) != len(participantIDs) {
		t.Fatalf("resolved participants = %v, want %v", completed.Resolved, participantIDs)
	}
	if _, found, err := db.lookupPartitionOffsetAppendOutcome(ctx, commit, fingerprint); err != nil || !found {
		t.Fatalf("recovered source outcome found=%t err=%v", found, err)
	}
	if got, err := stores["partition/stream/events/0"].ConsumerOffset(ctx, commit); err != nil || got != 12 {
		t.Fatalf("source offset = %d err=%v, want 12", got, err)
	}
}

func TestPartitionStreamTransactionResultSurvivesInterleavingAndSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.DeclareStream(ctx, StreamConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	adapter := &brokerStoreAdapter{store: store}
	groupID := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: "events", Partition: 3}
	participantID := PartitionTransactionParticipantID(groupID)
	transactionID := "tx-offset-result"

	stagedPayload, err := json.Marshal([]AppendEntry{{
		Message:      Message{ID: "transaction-output", Stream: "events", Partition: 3},
		Destinations: []Destination{{Kind: DestinationStream, Name: "events"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	preparePayload, err := json.Marshal(brokerTransactionCommand{
		TransactionID: transactionID,
		ParticipantID: participantID,
		Operation:     cluster.BrokerCmdAppendAtomic,
		Payload:       stagedPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.ReplicatePrepareTransaction(ctx, preparePayload); err != nil {
		t.Fatal(err)
	}

	interleaved, err := store.Append(ctx,
		Message{ID: "interleaved-output", Stream: "events", Partition: 3},
		[]Destination{{Kind: DestinationStream, Name: "events"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(interleaved) != 1 || interleaved[0].Offset != 1 {
		t.Fatalf("interleaved append = %#v, want one message at offset 1", interleaved)
	}

	resolvePayload, err := json.Marshal(brokerTransactionCommand{
		TransactionID: transactionID,
		ParticipantID: participantID,
		Decision:      cluster.TransactionCommit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.ReplicateResolveTransaction(ctx, resolvePayload); err != nil {
		t.Fatal(err)
	}

	snapshot, err := adapter.ReplicateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	restoredStore := NewMemoryStore()
	restoredAdapter := &brokerStoreAdapter{store: restoredStore}
	if err := restoredAdapter.ReplicateRestore(snapshot); err != nil {
		t.Fatal(err)
	}

	// Simulate output-stream retention deleting the original message after the
	// output participant committed. Recovery still needs the replicated result.
	restoredStore.mu.Lock()
	delete(restoredStore.messages, "transaction-output")
	restoredStore.mu.Unlock()
	results, found := restoredAdapter.lookupTransactionResult(transactionID, participantID)
	if !found || len(results) != 1 {
		t.Fatalf("retained transaction result = %#v, found %t", results, found)
	}
	if results[0].Offset != 2 || results[0].Partition != 3 {
		t.Fatalf("retained output = %#v, want offset 2 in partition 3", results[0])
	}
}
