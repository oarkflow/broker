package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/oarkflow/broker/cluster"
)

// ReassignDrainingPartitionsOnce advances pending safe reassignments and starts
// replacements for assigned nodes that the controller has explicitly marked
// draining, failed, or left. It also moves non-leader replicas when their
// assignment load exceeds an eligible target by at least two partitions, or
// when fresh workload signals predict a material improvement. It only advances
// partitions led by this broker; other brokers can run the same sweep for
// their local partition leaders.
func (db *DistributedBroker) ReassignDrainingPartitionsOnce(ctx context.Context) (int, error) {
	if db == nil || db.controllerQuorum == nil || db.raftCtrl == nil {
		return 0, ErrUnsupportedMode
	}
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return 0, err
	}
	nodes := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	replicaLoad := make(map[string]int, len(snapshot.Nodes))
	leaderLoad := make(map[string]int, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node != nil {
			nodes[node.ID] = node
		}
	}
	for _, partition := range snapshot.Partitions {
		if partition == nil {
			continue
		}
		for _, replica := range partition.Replicas {
			replicaLoad[replica]++
		}
		if partition.Leader != "" {
			leaderLoad[partition.Leader]++
		}
		if pending := partition.Reassignment; pending != nil {
			for _, replica := range pending.DesiredReplicas {
				if !containsReplicaID(partition.Replicas, replica) {
					replicaLoad[replica]++
				}
			}
		}
	}
	activeNodes := make([]*cluster.NodeInfo, 0, len(nodes))
	for _, node := range snapshot.Nodes {
		if node != nil && node.State == cluster.NodeActive {
			activeNodes = append(activeNodes, node)
		}
	}
	var liveLoads map[string]PlacementNodeLoadSignals
	workloadComplete, progressComplete, aggregateDiskComplete, partitionDiskComplete := false, false, false, false
	if len(activeNodes) > 0 {
		liveLoads, workloadComplete, progressComplete, aggregateDiskComplete, partitionDiskComplete = db.gatherPlacementSignals(ctx, activeNodes, snapshot.Partitions)
		diskComplete := aggregateDiskComplete && partitionDiskComplete
		if workloadComplete || diskComplete {
			current, readErr := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
			topologyUnchanged := readErr == nil && samePlacementTopology(snapshot, current)
			workloadComplete = workloadComplete && topologyUnchanged
			aggregateDiskComplete = aggregateDiskComplete && topologyUnchanged
			partitionDiskComplete = partitionDiskComplete && topologyUnchanged
			if !topologyUnchanged {
				progressComplete = false
				liveLoads = nil
			}
		}
	}
	diskComplete := aggregateDiskComplete && partitionDiskComplete
	partitions := make([]*cluster.PartitionMetadata, 0, len(snapshot.Partitions))
	for _, partition := range snapshot.Partitions {
		if partition != nil {
			partitions = append(partitions, partition)
		}
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID.String() < partitions[j].ID.String()
	})
	advanced := 0
	var sweepErr error
	reassignmentStartedThisSweep := false
	for _, partition := range partitions {
		if err := ctx.Err(); err != nil {
			return advanced, errors.Join(sweepErr, err)
		}
		if partition == nil {
			continue
		}
		groupID := "partition/" + partition.ID.String()
		controller, exists := db.RaftGroup(groupID)
		if !exists || !controller.IsLeader() {
			continue
		}
		desired := []string(nil)
		leader := ""
		if pending := partition.Reassignment; pending != nil {
			desired = append(desired, pending.DesiredReplicas...)
			leader = pending.DesiredLeader
		} else {
			needsReplacement := false
			for _, replica := range partition.Replicas {
				node := nodes[replica]
				if node == nil || node.State != cluster.NodeActive {
					needsReplacement = true
					continue
				}
				desired = append(desired, replica)
			}
			if !needsReplacement {
				desired, leader = db.balancedReplicaMove(partition, nodes, replicaLoad, liveLoads, diskComplete)
				if len(desired) == 0 && workloadComplete && !reassignmentStartedThisSweep {
					desired, leader = db.workloadBalancedReplicaMove(partition, nodes, liveLoads, progressComplete, diskComplete)
				}
				if len(desired) == 0 && diskComplete && !reassignmentStartedThisSweep {
					desired, leader = db.diskBalancedReplicaMove(partition, nodes, liveLoads, progressComplete)
				}
				if len(desired) == 0 {
					continue
				}
			}
			for len(desired) < db.distCfg.ReplicationFactor {
				candidate := ""
				if diskComplete {
					candidate = db.leastLoadedActiveReplicaWithDisk(nodes, desired, replicaLoad, partition, liveLoads)
				} else {
					candidate = db.leastLoadedActiveReplica(nodes, desired, replicaLoad)
				}
				if candidate == "" {
					if db.Broker != nil && db.Broker.promMetrics != nil {
						db.Broker.promMetrics.RecordPartitionReassignmentError()
					}
					sweepErr = errors.Join(sweepErr, fmt.Errorf("partition %s has no eligible active voter with sufficient placement capacity", partition.ID))
					break
				}
				desired = append(desired, candidate)
			}
			if len(desired) != db.distCfg.ReplicationFactor {
				continue
			}
			sort.Strings(desired)
			leader = partition.Leader
			if !containsReplicaID(desired, leader) || nodes[leader] == nil || nodes[leader].State != cluster.NodeActive {
				leader = leastLoadedLeader(desired, leaderLoad)
			}
		}
		progress, err := db.ReassignResourcePartition(ctx, partition.ID.Kind, partition.ID.Topic, partition.ID.Partition, desired, leader)
		if err != nil {
			if db.Broker != nil && db.Broker.promMetrics != nil {
				db.Broker.promMetrics.RecordPartitionReassignmentError()
			}
			sweepErr = errors.Join(sweepErr, fmt.Errorf("reassign partition %s: %w", partition.ID, err))
			continue
		}
		if progress != nil && db.Broker != nil && db.Broker.promMetrics != nil {
			metrics := db.Broker.promMetrics
			if partition.Reassignment == nil && !sameStringSet(desired, partition.Replicas) &&
				progress.Partition != nil && progress.Partition.Reassignment != nil {
				metrics.RecordPartitionReassignmentStarted()
			}
			if partition.Reassignment != nil && progress.Complete {
				metrics.RecordPartitionReassignmentCompleted()
			} else if progress.Partition != nil && progress.Partition.Reassignment != nil {
				metrics.RecordPartitionReassignmentPending()
			}
		}
		if partition.Reassignment == nil && progress != nil {
			for _, replica := range partition.Replicas {
				if !containsReplicaID(desired, replica) {
					replicaLoad[replica]--
				}
			}
			for _, replica := range desired {
				if !containsReplicaID(partition.Replicas, replica) {
					replicaLoad[replica]++
				}
			}
			if partition.Leader != leader {
				leaderLoad[partition.Leader]--
				leaderLoad[leader]++
			}
			if workloadComplete {
				adjustPlacementWorkloads(liveLoads, partition, desired)
			}
			if diskComplete {
				adjustPlacementDiskUsage(liveLoads, partition, desired, nodes)
			}
			if (workloadComplete || diskComplete) && !sameStringSet(desired, partition.Replicas) && len(partition.Replicas) == db.distCfg.ReplicationFactor {
				reassignmentStartedThisSweep = true
			}
		}
		if progress != nil && !progress.Complete {
			advanced++
		}
	}
	return advanced, sweepErr
}

func (db *DistributedBroker) workloadBalancedReplicaMove(
	partition *cluster.PartitionMetadata,
	nodes map[string]*cluster.NodeInfo,
	loads map[string]PlacementNodeLoadSignals,
	progressComplete bool,
	diskComplete bool,
) ([]string, string) {
	if partition == nil || partition.Reassignment != nil || partition.Leader == "" ||
		len(partition.Replicas) != db.distCfg.ReplicationFactor {
		return nil, ""
	}
	partitionKey := partition.ID.Canonical().String()
	sources := append([]string(nil), partition.Replicas...)
	sort.Strings(sources)
	candidateIDs := make([]string, 0, len(nodes))
	for id := range nodes {
		candidateIDs = append(candidateIDs, id)
	}
	sort.Strings(candidateIDs)
	var bestReplicas []string
	bestImprovement := uint64(0)
	bestLag := uint64(^uint64(0))
	bestDomainDiversity := false
	for _, source := range sources {
		if source == partition.Leader {
			continue
		}
		sourceNode := nodes[source]
		sourceLoad, sourceOK := loads[source]
		partitionLoad, partitionOK := sourceLoad.PartitionWorkloads[partitionKey]
		if sourceNode == nil || sourceNode.State != cluster.NodeActive || !sourceOK || !sourceLoad.WorkloadKnown || !partitionOK {
			continue
		}
		if progressComplete && (!sourceLoad.PartitionProgressKnown || !sourceLoad.PartitionRecentActive || !sourceLoad.PartitionInSync) {
			continue
		}
		sourceTotal := placementWorkloadTotal(sourceLoad.WorkloadMessages, sourceLoad.WorkloadLeased)
		moving := placementWorkloadTotal(partitionLoad.Messages, partitionLoad.Leased)
		if moving == 0 || moving > sourceTotal {
			continue
		}
		partitionBytes, hasPartitionBytes := sourceLoad.PartitionDiskUsage[partitionKey]
		if diskComplete && (!hasPartitionBytes || partitionBytes == 0) {
			continue
		}
		retained := make([]string, 0, len(partition.Replicas)-1)
		for _, replica := range partition.Replicas {
			if replica != source {
				retained = append(retained, replica)
			}
		}
		for _, candidate := range candidateIDs {
			candidateNode := nodes[candidate]
			if candidateNode == nil || candidateNode.State != cluster.NodeActive || candidateNode.Address == "" || candidateNode.ClientAddress == "" ||
				containsReplicaID(partition.Replicas, candidate) {
				continue
			}
			peerID, registered := db.raftCtrl.PeerID(candidateNode.Address)
			if !registered || db.raftCtrl.PeerRole(peerID) != "voter" {
				continue
			}
			candidateLoad, ok := loads[candidate]
			if !ok || !candidateLoad.WorkloadKnown {
				continue
			}
			if progressComplete && (!candidateLoad.PartitionProgressKnown || !candidateLoad.PartitionRecentActive || !candidateLoad.PartitionInSync) {
				continue
			}
			if diskComplete && !hasDiskHeadroom(candidateLoad, partitionBytes) {
				continue
			}
			targetTotal := placementWorkloadTotal(candidateLoad.WorkloadMessages, candidateLoad.WorkloadLeased)
			before := absoluteDifference(sourceTotal, targetTotal)
			sourceAfter := sourceTotal - moving
			targetAfter := saturatingAddUint64(targetTotal, moving)
			after := absoluteDifference(sourceAfter, targetAfter)
			if after >= before {
				continue
			}
			improvement := before - after
			if improvement < 100 || improvement < before/5 {
				continue
			}
			lag := candidateLoad.PartitionLag
			if !progressComplete {
				lag = ^uint64(0)
			}
			diverse := replicaAddsFailureDomain(candidate, retained, nodes)
			if len(bestReplicas) > 0 {
				if improvement < bestImprovement || improvement == bestImprovement &&
					(bestDomainDiversity && !diverse || diverse == bestDomainDiversity && lag >= bestLag) {
					continue
				}
			}
			desired := append(append([]string(nil), retained...), candidate)
			sort.Strings(desired)
			bestReplicas, bestImprovement, bestLag = desired, improvement, lag
			bestDomainDiversity = diverse
		}
	}
	if len(bestReplicas) == 0 {
		return nil, ""
	}
	return bestReplicas, partition.Leader
}

func placementWorkloadTotal(messages, leased int) uint64 {
	if messages < 0 || leased < 0 {
		return 0
	}
	return uint64(messages) + uint64(leased)
}

func absoluteDifference(left, right uint64) uint64 {
	if left >= right {
		return left - right
	}
	return right - left
}

func adjustPlacementWorkloads(loads map[string]PlacementNodeLoadSignals, partition *cluster.PartitionMetadata, desired []string) {
	if partition == nil || len(desired) == 0 {
		return
	}
	key := partition.ID.Canonical().String()
	var moved StoreStats
	movedKnown := false
	for _, source := range partition.Replicas {
		if containsReplicaID(desired, source) {
			continue
		}
		load, ok := loads[source]
		if !ok {
			continue
		}
		part, ok := load.PartitionWorkloads[key]
		if !ok {
			continue
		}
		if !movedKnown {
			moved, movedKnown = part, true
		}
		load.WorkloadMessages -= part.Messages
		load.WorkloadLeased -= part.Leased
		delete(load.PartitionWorkloads, key)
		loads[source] = load
	}
	for _, target := range desired {
		if containsReplicaID(partition.Replicas, target) {
			continue
		}
		load, ok := loads[target]
		if !ok || !movedKnown {
			continue
		}
		load.PartitionWorkloads[key] = moved
		load.WorkloadMessages += moved.Messages
		load.WorkloadLeased += moved.Leased
		loads[target] = load
	}
}

func replicaAddsFailureDomain(candidate string, retained []string, nodes map[string]*cluster.NodeInfo) bool {
	node := nodes[candidate]
	if node == nil || node.Metadata == nil || node.Metadata["failure_domain"] == "" {
		return false
	}
	domain := node.Metadata["failure_domain"]
	for _, replica := range retained {
		if existing := nodes[replica]; existing != nil && existing.Metadata != nil && existing.Metadata["failure_domain"] == domain {
			return false
		}
	}
	return true
}

func (db *DistributedBroker) diskBalancedReplicaMove(
	partition *cluster.PartitionMetadata,
	nodes map[string]*cluster.NodeInfo,
	loads map[string]PlacementNodeLoadSignals,
	progressComplete bool,
) ([]string, string) {
	if partition == nil || partition.Reassignment != nil || partition.Leader == "" ||
		len(partition.Replicas) != db.distCfg.ReplicationFactor {
		return nil, ""
	}
	partitionKey := partition.ID.Canonical().String()
	sources := append([]string(nil), partition.Replicas...)
	sort.Strings(sources)
	candidateIDs := make([]string, 0, len(nodes))
	for id := range nodes {
		candidateIDs = append(candidateIDs, id)
	}
	sort.Strings(candidateIDs)
	var bestReplicas []string
	bestImprovement := 0.0
	bestLag := uint64(^uint64(0))
	bestDomainDiversity := false
	for _, source := range sources {
		if source == partition.Leader || nodes[source] == nil || nodes[source].State != cluster.NodeActive {
			continue
		}
		sourceLoad, ok := loads[source]
		moving, hasPartitionUsage := sourceLoad.PartitionDiskUsage[partitionKey]
		if !ok || !sourceLoad.DiskUsageKnown || !hasPartitionUsage || moving == 0 ||
			sourceLoad.DiskCapacityBytes == 0 || moving > sourceLoad.DiskUsedBytes {
			continue
		}
		if progressComplete && (!sourceLoad.PartitionProgressKnown || !sourceLoad.PartitionRecentActive || !sourceLoad.PartitionInSync) {
			continue
		}
		retained := make([]string, 0, len(partition.Replicas)-1)
		for _, replica := range partition.Replicas {
			if replica != source {
				retained = append(retained, replica)
			}
		}
		sourceBefore := float64(sourceLoad.DiskUsedBytes) / float64(sourceLoad.DiskCapacityBytes)
		sourceAfter := float64(sourceLoad.DiskUsedBytes-moving) / float64(sourceLoad.DiskCapacityBytes)
		for _, candidate := range candidateIDs {
			candidateNode := nodes[candidate]
			if candidateNode == nil || candidateNode.State != cluster.NodeActive || candidateNode.Address == "" || candidateNode.ClientAddress == "" ||
				containsReplicaID(partition.Replicas, candidate) {
				continue
			}
			peerID, registered := db.raftCtrl.PeerID(candidateNode.Address)
			if !registered || db.raftCtrl.PeerRole(peerID) != "voter" {
				continue
			}
			candidateLoad, ok := loads[candidate]
			if !ok || !candidateLoad.DiskUsageKnown || candidateLoad.DiskCapacityBytes == 0 || moving > candidateLoad.DiskAvailableBytes {
				continue
			}
			if progressComplete && (!candidateLoad.PartitionProgressKnown || !candidateLoad.PartitionRecentActive || !candidateLoad.PartitionInSync) {
				continue
			}
			// Preserve at least 20% of the incoming partition's size as free space
			// after catch-up, accounting for logical-size estimation error.
			reserve := moving / 5
			if candidateLoad.DiskAvailableBytes-moving < reserve {
				continue
			}
			targetBefore := float64(candidateLoad.DiskUsedBytes) / float64(candidateLoad.DiskCapacityBytes)
			if sourceBefore-targetBefore < 0.10 {
				continue
			}
			targetAfter := float64(saturatingAddUint64(candidateLoad.DiskUsedBytes, moving)) / float64(candidateLoad.DiskCapacityBytes)
			beforeGap := sourceBefore - targetBefore
			afterGap := absoluteFloatDifference(sourceAfter, targetAfter)
			if afterGap >= beforeGap {
				continue
			}
			improvement := beforeGap - afterGap
			lag := candidateLoad.PartitionLag
			if !progressComplete {
				lag = ^uint64(0)
			}
			diverse := replicaAddsFailureDomain(candidate, retained, nodes)
			if len(bestReplicas) > 0 {
				if improvement < bestImprovement || improvement == bestImprovement &&
					(bestDomainDiversity && !diverse || diverse == bestDomainDiversity && lag >= bestLag) {
					continue
				}
			}
			desired := append(append([]string(nil), retained...), candidate)
			sort.Strings(desired)
			bestReplicas, bestImprovement, bestLag = desired, improvement, lag
			bestDomainDiversity = diverse
		}
	}
	if len(bestReplicas) == 0 {
		return nil, ""
	}
	return bestReplicas, partition.Leader
}

func absoluteFloatDifference(left, right float64) float64 {
	if left >= right {
		return left - right
	}
	return right - left
}

func adjustPlacementDiskUsage(loads map[string]PlacementNodeLoadSignals, partition *cluster.PartitionMetadata, desired []string, nodes map[string]*cluster.NodeInfo) {
	if partition == nil || len(desired) == 0 {
		return
	}
	key := partition.ID.Canonical().String()
	moved, movedKnown := estimatedPartitionDiskBytes(partition, nodes, loads)
	if !movedKnown {
		return
	}
	for _, source := range partition.Replicas {
		if containsReplicaID(desired, source) {
			continue
		}
		load, ok := loads[source]
		if !ok {
			continue
		}
		used, ok := load.PartitionDiskUsage[key]
		if !ok || used > load.DiskUsedBytes {
			continue
		}
		load.DiskUsedBytes -= used
		load.DiskAvailableBytes = saturatingAddUint64(load.DiskAvailableBytes, used)
		delete(load.PartitionDiskUsage, key)
		loads[source] = load
	}
	for _, target := range desired {
		if containsReplicaID(partition.Replicas, target) {
			continue
		}
		load, ok := loads[target]
		if !ok || moved > load.DiskAvailableBytes {
			continue
		}
		load.PartitionDiskUsage[key] = moved
		load.DiskUsedBytes = saturatingAddUint64(load.DiskUsedBytes, moved)
		load.DiskAvailableBytes -= moved
		loads[target] = load
	}
}

func (db *DistributedBroker) balancedReplicaMove(partition *cluster.PartitionMetadata, nodes map[string]*cluster.NodeInfo, load map[string]int, liveLoads map[string]PlacementNodeLoadSignals, diskComplete bool) ([]string, string) {
	if partition == nil || partition.Leader == "" || len(partition.Replicas) != db.distCfg.ReplicationFactor {
		return nil, ""
	}
	sources := make([]string, 0, len(partition.Replicas))
	for _, replica := range partition.Replicas {
		node := nodes[replica]
		if replica != partition.Leader && node != nil && node.State == cluster.NodeActive {
			sources = append(sources, replica)
		}
	}
	if len(sources) == 0 {
		return nil, ""
	}
	sort.Slice(sources, func(i, j int) bool {
		if load[sources[i]] != load[sources[j]] {
			return load[sources[i]] > load[sources[j]]
		}
		return sources[i] < sources[j]
	})
	for _, source := range sources {
		retained := make([]string, 0, len(partition.Replicas)-1)
		for _, replica := range partition.Replicas {
			if replica != source {
				retained = append(retained, replica)
			}
		}
		candidate := ""
		if diskComplete {
			candidate = db.leastLoadedActiveReplicaWithDisk(nodes, retained, load, partition, liveLoads)
		} else {
			candidate = db.leastLoadedActiveReplica(nodes, retained, load)
		}
		if candidate == "" || load[source] < load[candidate]+2 {
			continue
		}
		desired := append(retained, candidate)
		sort.Strings(desired)
		return desired, partition.Leader
	}
	return nil, ""
}

func containsReplicaID(replicas []string, id string) bool {
	for _, replica := range replicas {
		if replica == id {
			return true
		}
	}
	return false
}

func (db *DistributedBroker) leastLoadedActiveReplica(nodes map[string]*cluster.NodeInfo, selected []string, load map[string]int) string {
	candidates := db.eligibleActiveReplicaCandidates(nodes, selected, load)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func (db *DistributedBroker) leastLoadedActiveReplicaWithDisk(
	nodes map[string]*cluster.NodeInfo,
	selected []string,
	load map[string]int,
	partition *cluster.PartitionMetadata,
	liveLoads map[string]PlacementNodeLoadSignals,
) string {
	incoming, known := estimatedPartitionDiskBytes(partition, nodes, liveLoads)
	if !known {
		return ""
	}
	for _, candidate := range db.eligibleActiveReplicaCandidates(nodes, selected, load) {
		candidateLoad, ok := liveLoads[candidate]
		if ok && hasDiskHeadroom(candidateLoad, incoming) {
			return candidate
		}
	}
	return ""
}

func (db *DistributedBroker) eligibleActiveReplicaCandidates(nodes map[string]*cluster.NodeInfo, selected []string, load map[string]int) []string {
	selectedSet := make(map[string]struct{}, len(selected))
	selectedDomains := make(map[string]struct{}, len(selected))
	for _, id := range selected {
		selectedSet[id] = struct{}{}
		if node := nodes[id]; node != nil && node.Metadata != nil && node.Metadata["failure_domain"] != "" {
			selectedDomains[node.Metadata["failure_domain"]] = struct{}{}
		}
	}
	candidates := make([]string, 0, len(nodes))
	for id, node := range nodes {
		if node == nil || node.State != cluster.NodeActive || node.Address == "" || node.ClientAddress == "" {
			continue
		}
		if _, used := selectedSet[id]; used {
			continue
		}
		peerID, registered := db.raftCtrl.PeerID(node.Address)
		if !registered || db.raftCtrl.PeerRole(peerID) != "voter" {
			continue
		}
		candidates = append(candidates, id)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := nodes[candidates[i]], nodes[candidates[j]]
		leftDomain, rightDomain := "", ""
		if left.Metadata != nil {
			leftDomain = left.Metadata["failure_domain"]
		}
		if right.Metadata != nil {
			rightDomain = right.Metadata["failure_domain"]
		}
		_, leftDuplicate := selectedDomains[leftDomain]
		_, rightDuplicate := selectedDomains[rightDomain]
		if leftDomain == "" {
			leftDuplicate = true
		}
		if rightDomain == "" {
			rightDuplicate = true
		}
		if leftDuplicate != rightDuplicate {
			return !leftDuplicate
		}
		if load[candidates[i]] != load[candidates[j]] {
			return load[candidates[i]] < load[candidates[j]]
		}
		return candidates[i] < candidates[j]
	})
	return candidates
}

func estimatedPartitionDiskBytes(partition *cluster.PartitionMetadata, nodes map[string]*cluster.NodeInfo, loads map[string]PlacementNodeLoadSignals) (uint64, bool) {
	if partition == nil {
		return 0, false
	}
	key := partition.ID.Canonical().String()
	var maximum uint64
	known := false
	for _, replica := range partition.Replicas {
		node := nodes[replica]
		if node == nil || node.State != cluster.NodeActive {
			continue
		}
		load, ok := loads[replica]
		if !ok || !load.DiskUsageKnown {
			return 0, false
		}
		used, ok := load.PartitionDiskUsage[key]
		if !ok {
			return 0, false
		}
		if used > maximum {
			maximum = used
		}
		known = true
	}
	return maximum, known && maximum > 0
}

func hasDiskHeadroom(load PlacementNodeLoadSignals, incoming uint64) bool {
	if !load.DiskUsageKnown || incoming > load.DiskAvailableBytes {
		return false
	}
	reserve := incoming / 5
	return load.DiskAvailableBytes-incoming >= reserve
}

func leastLoadedLeader(replicas []string, load map[string]int) string {
	if len(replicas) == 0 {
		return ""
	}
	candidates := append([]string(nil), replicas...)
	sort.Slice(candidates, func(i, j int) bool {
		if load[candidates[i]] != load[candidates[j]] {
			return load[candidates[i]] < load[candidates[j]]
		}
		return candidates[i] < candidates[j]
	})
	return candidates[0]
}

// RunAutomaticReassignment periodically replaces replicas on draining, failed,
// or departed nodes and balances healthy replica assignments. Set interval to
// zero to disable it.
func (db *DistributedBroker) RunAutomaticReassignment(ctx context.Context, interval time.Duration, onError func(error)) error {
	if db == nil || db.controllerQuorum == nil || db.raftCtrl == nil {
		return ErrUnsupportedMode
	}
	if interval <= 0 {
		return fmt.Errorf("automatic reassignment interval must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if onError == nil {
		onError = func(error) {}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := db.ReassignDrainingPartitionsOnce(ctx); err != nil && ctx.Err() == nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
