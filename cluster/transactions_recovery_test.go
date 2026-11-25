package cluster

import (
	"context"
	"testing"
	"time"
)

type transactionCleanupRecorder struct {
	commits   []string
	forgotten []string
	t         *testing.T
}

func (r *transactionCleanupRecorder) CommitTransaction(_ context.Context, _, participant string) error {
	if len(r.forgotten) != 0 {
		r.t.Fatalf("participant %q committed after cleanup began: %v", participant, r.forgotten)
	}
	r.commits = append(r.commits, participant)
	return nil
}

func (*transactionCleanupRecorder) AbortTransaction(context.Context, string, string) error {
	return nil
}

func (r *transactionCleanupRecorder) ForgetTransaction(_ context.Context, _, participant string) error {
	if len(r.commits) != 2 {
		r.t.Fatalf("participant %q forgotten before both commits: %v", participant, r.commits)
	}
	r.forgotten = append(r.forgotten, participant)
	return nil
}

func TestRecoveryForgetsParticipantsOnlyAfterAllCommitsResolve(t *testing.T) {
	metadata := NewClusterMetadata()
	controller, err := NewRaftController(RaftControllerConfig{
		ID: 1, NodeID: "transaction-recovery", Peers: map[uint64]string{1: "transaction-recovery"},
		ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		StateMachine: NewMetadataStateMachine(metadata),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(); err != nil {
		t.Fatal(err)
	}
	defer controller.Stop()
	if err := controller.Campaign(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !controller.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !controller.IsLeader() {
		t.Fatal("controller did not become leader")
	}

	quorum, err := NewControllerQuorum(ControllerQuorumConfig{Controller: controller, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	participants := []string{"output-partition", "source-partition"}
	if err := quorum.BeginTransaction(ctx, "offset-append", participants, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, participant := range participants {
		if err := quorum.PrepareTransactionParticipant(ctx, "offset-append", participant); err != nil {
			t.Fatal(err)
		}
	}
	if err := quorum.DecideTransaction(ctx, "offset-append", true); err != nil {
		t.Fatal(err)
	}
	resolver := &transactionCleanupRecorder{t: t}
	if err := quorum.RecoverTransaction(ctx, "offset-append", resolver); err != nil {
		t.Fatal(err)
	}
	if len(resolver.commits) != 2 || len(resolver.forgotten) != 2 {
		t.Fatalf("recovery commits=%v forgets=%v, want both participants", resolver.commits, resolver.forgotten)
	}
}
