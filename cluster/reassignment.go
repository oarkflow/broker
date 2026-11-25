package cluster

import (
	"context"
	"fmt"
	"sort"
)

func validateBeginPartitionReassignment(pm *PartitionMetadata, cmd MetadataCommand) error {
	if pm == nil {
		return ErrPartitionNotFound
	}
	if cmd.UpdatedAt.IsZero() || cmd.Epoch == 0 || cmd.ExpectedLeader == "" {
		return fmt.Errorf("%w: reassignment requires a timestamp, expected leader, and epoch", ErrInvalidPartitionAssignment)
	}
	if err := validatePartitionAssignment(pm.ID, cmd.ExpectedReplicas, cmd.ExpectedLeader); err != nil {
		return err
	}
	if err := validatePartitionAssignment(pm.ID, cmd.Replicas, cmd.Leader); err != nil {
		return err
	}
	if pm.Reassignment != nil {
		pending := pm.Reassignment
		if pending.Phase == PartitionReassignmentCopying && pending.ExpectedEpoch == cmd.Epoch &&
			pending.ExpectedLeader == cmd.ExpectedLeader && sameReplicaSet(pending.ExpectedReplicas, cmd.ExpectedReplicas) &&
			pending.DesiredLeader == cmd.Leader && sameReplicaSet(pending.DesiredReplicas, cmd.Replicas) {
			return nil
		}
		return fmt.Errorf("%w: another reassignment is already active", ErrPartitionReassignmentRequired)
	}
	if pm.LeaderEpoch != cmd.Epoch || pm.Leader != cmd.ExpectedLeader || !sameReplicaSet(pm.Replicas, cmd.ExpectedReplicas) {
		return ErrStaleEpoch
	}
	if pm.Leader == cmd.Leader && sameReplicaSet(pm.Replicas, cmd.Replicas) {
		return fmt.Errorf("%w: desired assignment is unchanged", ErrInvalidPartitionAssignment)
	}
	return nil
}

func validateCommitPartitionReassignment(pm *PartitionMetadata, cmd MetadataCommand) error {
	if pm == nil {
		return ErrPartitionNotFound
	}
	pending := pm.Reassignment
	if pending == nil {
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase == PartitionReassignmentCleanup {
		if pm.Leader == cmd.Leader && sameReplicaSet(pm.Replicas, cmd.Replicas) && pm.LeaderEpoch == cmd.ResultEpoch {
			return nil
		}
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase != PartitionReassignmentCopying || cmd.UpdatedAt.IsZero() ||
		cmd.Epoch != pending.ExpectedEpoch || cmd.ExpectedLeader != pending.ExpectedLeader ||
		!sameReplicaSet(cmd.ExpectedReplicas, pending.ExpectedReplicas) ||
		cmd.Leader != pending.DesiredLeader || !sameReplicaSet(cmd.Replicas, pending.DesiredReplicas) {
		return ErrPartitionReassignmentRequired
	}
	if pm.LeaderEpoch != pending.ExpectedEpoch || pm.Leader != pending.ExpectedLeader || !sameReplicaSet(pm.Replicas, pending.ExpectedReplicas) {
		return ErrStaleEpoch
	}
	if cmd.ResultEpoch < pending.ExpectedEpoch || cmd.Leader != pending.ExpectedLeader && cmd.ResultEpoch == pending.ExpectedEpoch {
		return ErrStaleEpoch
	}
	return validateISR(cmd.ISR, pending.DesiredReplicas)
}

func validateFinishPartitionReassignment(pm *PartitionMetadata, cmd MetadataCommand) error {
	if pm == nil {
		return ErrPartitionNotFound
	}
	pending := pm.Reassignment
	if pending == nil {
		if pm.Leader == cmd.Leader && pm.LeaderEpoch == cmd.ResultEpoch && sameReplicaSet(pm.Replicas, cmd.Replicas) {
			return nil
		}
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase != PartitionReassignmentCleanup || pm.Leader != pending.DesiredLeader ||
		!sameReplicaSet(pm.Replicas, pending.DesiredReplicas) || cmd.Leader != pending.DesiredLeader ||
		!sameReplicaSet(cmd.Replicas, pending.DesiredReplicas) || cmd.ResultEpoch != pm.LeaderEpoch {
		return ErrPartitionReassignmentRequired
	}
	return nil
}

func validateBeginPartitionReassignmentAbort(pm *PartitionMetadata, cmd MetadataCommand) error {
	if pm == nil {
		return ErrPartitionNotFound
	}
	pending := pm.Reassignment
	if pending == nil {
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase == PartitionReassignmentAborting {
		if cmd.ExpectedLeader == pending.ExpectedLeader && cmd.Epoch == pending.ExpectedEpoch &&
			sameReplicaSet(cmd.ExpectedReplicas, pending.ExpectedReplicas) {
			return nil
		}
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase != PartitionReassignmentCopying || cmd.UpdatedAt.IsZero() ||
		cmd.ExpectedLeader != pending.ExpectedLeader || cmd.Epoch != pending.ExpectedEpoch ||
		!sameReplicaSet(cmd.ExpectedReplicas, pending.ExpectedReplicas) ||
		pm.Leader != pending.ExpectedLeader || pm.LeaderEpoch != pending.ExpectedEpoch ||
		!sameReplicaSet(pm.Replicas, pending.ExpectedReplicas) {
		return ErrPartitionReassignmentRequired
	}
	return nil
}

func validateFinishPartitionReassignmentAbort(pm *PartitionMetadata, cmd MetadataCommand) error {
	if pm == nil {
		return ErrPartitionNotFound
	}
	pending := pm.Reassignment
	if pending == nil {
		if pm.Leader == cmd.ExpectedLeader && pm.LeaderEpoch == cmd.Epoch && sameReplicaSet(pm.Replicas, cmd.ExpectedReplicas) {
			return nil
		}
		return ErrPartitionReassignmentRequired
	}
	if pending.Phase != PartitionReassignmentAborting || cmd.UpdatedAt.IsZero() ||
		cmd.ExpectedLeader != pending.ExpectedLeader || cmd.Epoch != pending.ExpectedEpoch ||
		!sameReplicaSet(cmd.ExpectedReplicas, pending.ExpectedReplicas) ||
		pm.Leader != pending.ExpectedLeader || pm.LeaderEpoch != pending.ExpectedEpoch ||
		!sameReplicaSet(pm.Replicas, pending.ExpectedReplicas) {
		return ErrPartitionReassignmentRequired
	}
	return nil
}

func validateReassignmentReplicas(cq *ControllerQuorum, replicas []string, leader string, current *PartitionMetadata) error {
	if err := validatePartitionNodeStates(cq.meta.Nodes, current, replicas, leader); err != nil {
		return err
	}
	if cq.requireClientAddress {
		if err := validateRoutableReplicas(cq.meta.Nodes, replicas); err != nil {
			return err
		}
	}
	if cq.peerIDForAddress == nil {
		return nil
	}
	peerOwners := make(map[uint64]string, len(replicas))
	for _, replica := range replicas {
		node := cq.meta.Nodes[replica]
		if node == nil {
			return fmt.Errorf("%w: partition replica %q is not registered", ErrNodeNotFound, replica)
		}
		peerID, ok := cq.peerIDForAddress(node.Address)
		if !ok || cq.peerRoleForAddress != nil && cq.peerRoleForAddress(node.Address) != "voter" {
			return fmt.Errorf("%w: partition replica %q is not an authenticated controller voter", ErrNodeUnavailable, replica)
		}
		if previous, exists := peerOwners[peerID]; exists {
			return fmt.Errorf("%w: logical replicas %q and %q map to controller peer %d", ErrInvalidPartitionAssignment, previous, replica, peerID)
		}
		peerOwners[peerID] = replica
	}
	return nil
}

func clonePartitionMetadata(pm *PartitionMetadata) *PartitionMetadata {
	if pm == nil {
		return nil
	}
	copyPM := *pm
	copyPM.Replicas = append([]string(nil), pm.Replicas...)
	copyPM.ISR = append([]string(nil), pm.ISR...)
	if pm.Reassignment != nil {
		copyReassignment := *pm.Reassignment
		copyReassignment.ExpectedReplicas = append([]string(nil), pm.Reassignment.ExpectedReplicas...)
		copyReassignment.DesiredReplicas = append([]string(nil), pm.Reassignment.DesiredReplicas...)
		copyPM.Reassignment = &copyReassignment
	}
	return &copyPM
}

func partitionReferencesNode(pm *PartitionMetadata, nodeID string) bool {
	if pm == nil {
		return false
	}
	if containsReplica(pm.Replicas, nodeID) {
		return true
	}
	return pm.Reassignment != nil && (containsReplica(pm.Reassignment.ExpectedReplicas, nodeID) || containsReplica(pm.Reassignment.DesiredReplicas, nodeID))
}

func validatePendingReassignmentNodeState(partitions map[PartitionGroupID]*PartitionMetadata, node *NodeInfo) error {
	if node == nil || node.State == NodeActive {
		return nil
	}
	for _, pm := range partitions {
		if pm == nil || pm.Reassignment == nil || pm.Reassignment.Phase != PartitionReassignmentCopying {
			continue
		}
		pending := pm.Reassignment
		if containsReplica(pending.DesiredReplicas, node.ID) && !containsReplica(pending.ExpectedReplicas, node.ID) {
			return fmt.Errorf("%w: target replica %q cannot leave active state during reassignment of %s", ErrNodeUnavailable, node.ID, pm.ID)
		}
	}
	return nil
}

// BeginResourcePartitionReassignment records the desired set before any Raft
// membership changes. The expected assignment fences concurrent operators.
func (cq *ControllerQuorum) BeginResourcePartitionReassignment(ctx context.Context, kind PartitionResourceKind, name string, partition int, expectedReplicas []string, expectedLeader string, expectedEpoch uint64, desiredReplicas []string, desiredLeader string) error {
	expectedReplicas = append([]string(nil), expectedReplicas...)
	desiredReplicas = append([]string(nil), desiredReplicas...)
	sort.Strings(expectedReplicas)
	sort.Strings(desiredReplicas)
	pid := (PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpBeginPartitionReassignment, Partition: &pid,
		ExpectedReplicas: expectedReplicas, ExpectedLeader: expectedLeader, Epoch: expectedEpoch,
		Replicas: desiredReplicas, Leader: desiredLeader, RequireClientAddress: cq.requireClientAddress,
	})
}

// CommitResourcePartitionReassignment switches the active assignment after
// the caller has caught up the new replicas, transferred leadership, and
// verified the resulting live ISR. Old members remain in Raft until the
// separate finish step so recovery can discover and complete cleanup.
func (cq *ControllerQuorum) CommitResourcePartitionReassignment(ctx context.Context, kind PartitionResourceKind, name string, partition int, expectedReplicas []string, expectedLeader string, expectedEpoch uint64, desiredReplicas []string, desiredLeader string, resultEpoch uint64, isr []string) error {
	expectedReplicas = append([]string(nil), expectedReplicas...)
	desiredReplicas = append([]string(nil), desiredReplicas...)
	isr = append([]string(nil), isr...)
	sort.Strings(expectedReplicas)
	sort.Strings(desiredReplicas)
	sort.Strings(isr)
	pid := (PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpCommitPartitionReassignment, Partition: &pid,
		ExpectedReplicas: expectedReplicas, ExpectedLeader: expectedLeader, Epoch: expectedEpoch,
		Replicas: desiredReplicas, Leader: desiredLeader, ResultEpoch: resultEpoch, ISR: isr,
	})
}

// FinishResourcePartitionReassignment clears the durable transition only
// after the old Raft members have been removed and the new assignment is live.
func (cq *ControllerQuorum) FinishResourcePartitionReassignment(ctx context.Context, kind PartitionResourceKind, name string, partition int, desiredReplicas []string, desiredLeader string, resultEpoch uint64) error {
	desiredReplicas = append([]string(nil), desiredReplicas...)
	sort.Strings(desiredReplicas)
	pid := (PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpFinishPartitionReassignment, Partition: &pid,
		Replicas: desiredReplicas, Leader: desiredLeader, ResultEpoch: resultEpoch,
	})
}

// BeginResourcePartitionReassignmentAbort durably stops provisioning new
// targets. The caller must remove every target-only Raft member before finish.
func (cq *ControllerQuorum) BeginResourcePartitionReassignmentAbort(ctx context.Context, kind PartitionResourceKind, name string, partition int, expectedReplicas []string, expectedLeader string, expectedEpoch uint64) error {
	expectedReplicas = append([]string(nil), expectedReplicas...)
	sort.Strings(expectedReplicas)
	pid := (PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpBeginPartitionReassignmentAbort, Partition: &pid,
		ExpectedReplicas: expectedReplicas, ExpectedLeader: expectedLeader, Epoch: expectedEpoch,
	})
}

// FinishResourcePartitionReassignmentAbort clears a copying transition only
// after every target-only learner or voter has left the partition group.
func (cq *ControllerQuorum) FinishResourcePartitionReassignmentAbort(ctx context.Context, kind PartitionResourceKind, name string, partition int, expectedReplicas []string, expectedLeader string, expectedEpoch uint64) error {
	expectedReplicas = append([]string(nil), expectedReplicas...)
	sort.Strings(expectedReplicas)
	pid := (PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpFinishPartitionReassignmentAbort, Partition: &pid,
		ExpectedReplicas: expectedReplicas, ExpectedLeader: expectedLeader, Epoch: expectedEpoch,
	})
}
