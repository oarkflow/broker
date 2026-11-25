package broker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/oarkflow/broker/cluster"
)

// ClusterUpgradeNode is one controller peer in a read-only rolling-upgrade
// preflight. Order is zero for nodes already at the target version or when the
// plan has blockers; otherwise it is the recommended one-based sequence.
type ClusterUpgradeNode struct {
	PeerID               uint64            `json:"peer_id"`
	Address              string            `json:"address"`
	LogicalNodeID        string            `json:"logical_node_id,omitempty"`
	State                cluster.NodeState `json:"state,omitempty"`
	Role                 string            `json:"role"`
	BrokerVersion        string            `json:"broker_version,omitempty"`
	ReplicaProgressKnown bool              `json:"replica_progress_known"`
	ReplicasRecentActive bool              `json:"replicas_recent_active"`
	ReplicasInSync       bool              `json:"replicas_in_sync"`
	MaxPartitionLag      uint64            `json:"max_partition_lag"`
	ControllerLead       bool              `json:"controller_leader"`
	UpgradeNeeded        bool              `json:"upgrade_needed"`
	Order                int               `json:"order,omitempty"`
	Blockers             []string          `json:"blockers,omitempty"`
}

// ClusterUpgradePlan reports a deterministic peer order and control-plane
// blockers for a rolling upgrade. CompatibilityVerified is always false:
// this plan cannot prove storage or mixed-version protocol compatibility.
type ClusterUpgradePlan struct {
	TargetVersion                 string               `json:"target_version"`
	ControllerLeader              uint64               `json:"controller_leader"`
	ControllerTerm                uint64               `json:"controller_term"`
	JointChangePending            bool                 `json:"joint_change_pending"`
	PendingReassignments          int                  `json:"pending_reassignments"`
	ReplicationFactor             int                  `json:"replication_factor"`
	MinimumISR                    int                  `json:"minimum_isr"`
	MaxReplicationLag             uint64               `json:"max_replication_lag"`
	VersionOrderingKnown          bool                 `json:"version_ordering_known"`
	LivePartitionProgressComplete bool                 `json:"live_partition_progress_complete"`
	StatePreflightReady           bool                 `json:"state_preflight_ready"`
	CompatibilityVerified         bool                 `json:"compatibility_verified"`
	RecommendedPeerIDs            []uint64             `json:"recommended_peer_ids,omitempty"`
	Nodes                         []ClusterUpgradeNode `json:"nodes"`
	Blockers                      []string             `json:"blockers,omitempty"`
	ManualChecks                  []string             `json:"manual_checks"`
}

// PlanClusterUpgrade provides a quorum-confirmed view of broker versions and
// a conservative controller-peer upgrade order. It is advisory and read-only.
func (db *DistributedBroker) PlanClusterUpgrade(ctx context.Context, targetVersion string) (*ClusterUpgradePlan, error) {
	if db == nil || db.raftCtrl == nil || db.controllerQuorum == nil {
		return nil, fmt.Errorf("%w: etcd/raft controller quorum is not enabled", ErrUnsupportedMode)
	}
	if targetVersion == "" || len(targetVersion) > cluster.MaxNodeBrokerVersionBytes ||
		!utf8.ValidString(targetVersion) || strings.TrimSpace(targetVersion) != targetVersion ||
		strings.IndexFunc(targetVersion, unicode.IsControl) >= 0 {
		return nil, fmt.Errorf("%w: invalid target broker version", ErrInvalidMessage)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	controllerTerm := db.raftCtrl.Term()
	leaderID := db.raftCtrl.LeaderID()
	jointChangePending := db.raftCtrl.JointConsensusPending()
	peers := db.raftCtrl.PeerAddresses()
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	plan := &ClusterUpgradePlan{
		TargetVersion:         targetVersion,
		ControllerLeader:      leaderID,
		ControllerTerm:        controllerTerm,
		JointChangePending:    jointChangePending,
		ReplicationFactor:     db.distCfg.ReplicationFactor,
		MinimumISR:            db.distCfg.MinInSyncReplicas,
		MaxReplicationLag:     db.distCfg.MaxReplicationLag,
		CompatibilityVerified: false,
		Nodes:                 make([]ClusterUpgradeNode, 0, len(peers)),
		ManualChecks: []string{
			"verify the target build supports every active storage and snapshot format",
			"verify mixed-version Raft commands and native protocol compatibility for this release",
			"confirm every broker listener and readiness probe is healthy",
			"take and verify backups before changing binaries",
		},
	}
	targetParsedVersion, targetVersionOrdered := parseStableBrokerVersion(targetVersion)
	plan.VersionOrderingKnown = targetVersionOrdered
	if leaderID == 0 {
		plan.Blockers = append(plan.Blockers, "controller leader is unknown")
	}
	if plan.JointChangePending {
		plan.Blockers = append(plan.Blockers, "a joint controller membership change is in progress")
	}
	nodeByAddress := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node == nil {
			continue
		}
		if prior := nodeByAddress[node.Address]; prior != nil && prior.ID != node.ID {
			return nil, fmt.Errorf("%w: controller address %q maps to multiple logical nodes", cluster.ErrInvalidPartitionAssignment, node.Address)
		}
		nodeByAddress[node.Address] = node
	}
	type orderedPeer struct {
		index int
		id    uint64
	}
	order := make([]orderedPeer, 0, len(peers))
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		address := peers[id]
		entry := ClusterUpgradeNode{
			PeerID: id, Address: address, Role: db.raftCtrl.PeerRole(id),
			ControllerLead: id == leaderID,
		}
		node := nodeByAddress[address]
		if node == nil {
			plan.VersionOrderingKnown = false
			entry.Blockers = append(entry.Blockers, "controller peer has no logical node registration")
		} else {
			entry.LogicalNodeID = node.ID
			entry.State = node.State
			entry.BrokerVersion = node.Metadata[cluster.NodeMetadataBrokerVersion]
			if _, ordered := parseStableBrokerVersion(entry.BrokerVersion); !ordered {
				plan.VersionOrderingKnown = false
			}
			if node.State != cluster.NodeActive {
				entry.Blockers = append(entry.Blockers, fmt.Sprintf("logical node state is %q, expected active", node.State))
			}
			if entry.BrokerVersion == "" {
				entry.Blockers = append(entry.Blockers, "broker version is unknown; inspect the node with brokerd -version")
			}
		}
		if entry.Role != "voter" && entry.Role != "learner" {
			entry.Blockers = append(entry.Blockers, fmt.Sprintf("controller peer role %q is not upgradeable", entry.Role))
		}
		entry.UpgradeNeeded = entry.BrokerVersion != targetVersion
		if currentVersion, currentOrdered := parseStableBrokerVersion(entry.BrokerVersion); targetVersionOrdered && currentOrdered {
			comparison := compareStableBrokerVersions(currentVersion, targetParsedVersion)
			if comparison > 0 {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer %d: target version %q is older than current version %q", id, targetVersion, entry.BrokerVersion))
			} else if comparison == 0 {
				entry.UpgradeNeeded = false
			}
		}
		if entry.UpgradeNeeded {
			order = append(order, orderedPeer{index: len(plan.Nodes), id: id})
		}
		if len(entry.Blockers) > 0 {
			for _, blocker := range entry.Blockers {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer %d: %s", id, blocker))
			}
		}
		plan.Nodes = append(plan.Nodes, entry)
	}
	if !plan.VersionOrderingKnown {
		plan.ManualChecks = append(plan.ManualChecks, "verify the target tag is a forward upgrade from any broker version that is not a stable vMAJOR.MINOR.PATCH release")
	}
	for _, partition := range snapshot.Partitions {
		if partition != nil && partition.Reassignment != nil {
			plan.PendingReassignments++
		}
	}
	if plan.PendingReassignments > 0 {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("%d partition reassignments are in progress", plan.PendingReassignments))
	}
	if len(snapshot.Partitions) == 0 {
		plan.LivePartitionProgressComplete = true
	} else {
		activeNodes := make([]*cluster.NodeInfo, 0, len(snapshot.Nodes))
		for _, node := range snapshot.Nodes {
			if node != nil && node.State == cluster.NodeActive {
				activeNodes = append(activeNodes, node)
			}
		}
		loads, _, progressComplete, _, _ := db.gatherPlacementSignals(ctx, activeNodes, snapshot.Partitions)
		plan.LivePartitionProgressComplete = progressComplete
		if !progressComplete {
			plan.Blockers = append(plan.Blockers, "fresh live partition progress could not be collected from every active broker")
		} else {
			for i := range plan.Nodes {
				entry := &plan.Nodes[i]
				if entry.LogicalNodeID == "" || entry.State != cluster.NodeActive {
					continue
				}
				load, ok := loads[entry.LogicalNodeID]
				if !ok || !load.PartitionProgressKnown {
					plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer %d: live replica progress is unavailable", entry.PeerID))
					continue
				}
				entry.ReplicaProgressKnown = true
				entry.ReplicasRecentActive = load.PartitionRecentActive
				entry.ReplicasInSync = load.PartitionInSync
				entry.MaxPartitionLag = load.PartitionLag
				if !load.PartitionRecentActive {
					plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer %d: one or more assigned replicas are inactive", entry.PeerID))
				}
				if !load.PartitionInSync {
					plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer %d: one or more assigned replicas are outside the in-sync replica set", entry.PeerID))
				}
			}
		}
	}
	if db.raftCtrl.Term() != controllerTerm || db.raftCtrl.LeaderID() != leaderID ||
		db.raftCtrl.JointConsensusPending() != jointChangePending ||
		!sameControllerPeerAddresses(peers, db.raftCtrl.PeerAddresses()) {
		plan.Blockers = append(plan.Blockers, "controller leadership or membership changed while the plan was being built")
	}
	// Learners are upgraded first, then non-leader voters, with the controller
	// leader last. IDs break ties so every broker returns the same order.
	sort.SliceStable(order, func(i, j int) bool {
		left, right := plan.Nodes[order[i].index], plan.Nodes[order[j].index]
		class := func(node ClusterUpgradeNode) int {
			if node.ControllerLead {
				return 3
			}
			if node.Role == "learner" {
				return 1
			}
			return 2
		}
		if class(left) != class(right) {
			return class(left) < class(right)
		}
		return left.PeerID < right.PeerID
	})
	if len(plan.Blockers) == 0 {
		for i, peer := range order {
			plan.Nodes[peer.index].Order = i + 1
			plan.RecommendedPeerIDs = append(plan.RecommendedPeerIDs, peer.id)
		}
		plan.StatePreflightReady = true
	}
	sort.Strings(plan.Blockers)
	return plan, nil
}

func sameControllerPeerAddresses(left, right map[uint64]string) bool {
	if len(left) != len(right) {
		return false
	}
	for id, address := range left {
		if right[id] != address {
			return false
		}
	}
	return true
}

type stableBrokerVersion [3]uint64

func parseStableBrokerVersion(value string) (stableBrokerVersion, bool) {
	var parsed stableBrokerVersion
	if len(value) < 6 || value[0] != 'v' {
		return parsed, false
	}
	parts := strings.Split(value[1:], ".")
	if len(parts) != len(parsed) {
		return parsed, false
	}
	for i, part := range parts {
		if part == "" || len(part) > 1 && part[0] == '0' {
			return stableBrokerVersion{}, false
		}
		for j := range part {
			if part[j] < '0' || part[j] > '9' {
				return stableBrokerVersion{}, false
			}
		}
		component, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return stableBrokerVersion{}, false
		}
		parsed[i] = component
	}
	return parsed, true
}

func compareStableBrokerVersions(left, right stableBrokerVersion) int {
	for i := range left {
		if left[i] < right[i] {
			return -1
		}
		if left[i] > right[i] {
			return 1
		}
	}
	return 0
}
