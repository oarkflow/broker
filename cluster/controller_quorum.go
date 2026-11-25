package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	ErrPartitionNotFound             = errors.New("cluster: partition metadata not found")
	ErrInvalidPartitionAssignment    = errors.New("cluster: invalid partition assignment")
	ErrInvalidISR                    = errors.New("cluster: invalid in-sync replica set")
	ErrNodeInUse                     = errors.New("cluster: node is still assigned to a partition")
	ErrNodeUnavailable               = errors.New("cluster: node state does not allow this placement")
	ErrInvalidNodeState              = errors.New("cluster: invalid node lifecycle state")
	ErrInvalidNodeClientAddress      = errors.New("cluster: invalid node client address")
	ErrInvalidNodeFailureDomain      = errors.New("cluster: invalid node failure domain")
	ErrInvalidNodeCapacityWeight     = errors.New("cluster: invalid node capacity weight")
	ErrInvalidNodeBrokerVersion      = errors.New("cluster: invalid node broker version")
	ErrPartitionReassignmentRequired = errors.New("cluster: partition reassignment requires the safe reassignment protocol")
	ErrPartitionTransferRequired     = errors.New("cluster: partition leader changes require a Raft leadership transfer")
	ErrSchemaNotFound                = errors.New("cluster: schema metadata not found")
	ErrInvalidSchema                 = errors.New("cluster: invalid schema metadata")
	ErrSchemaDeprecated              = errors.New("cluster: schema version is deprecated")
	ErrInvalidACLPrincipal           = errors.New("cluster: invalid ACL principal")
	ErrACLPrincipalNotFound          = errors.New("cluster: ACL principal not found")
)

// MetadataOpType identifies a cluster metadata mutation.
type MetadataOpType byte

const (
	OpRegisterNode                     MetadataOpType = 1
	OpDeregisterNode                   MetadataOpType = 2
	OpAssignPartition                  MetadataOpType = 3
	OpUpdateISR                        MetadataOpType = 4
	OpAdvanceEpoch                     MetadataOpType = 5
	OpBeginTransaction                 MetadataOpType = 6
	OpPrepareTransactionParticipant    MetadataOpType = 7
	OpDecideTransaction                MetadataOpType = 8
	OpResolveTransactionParticipant    MetadataOpType = 9
	OpRegisterSchema                   MetadataOpType = 10
	OpDeprecateSchema                  MetadataOpType = 11
	OpPutACLPrincipal                  MetadataOpType = 12
	OpRevokeACLPrincipal               MetadataOpType = 13
	OpPruneResolvedTransactions        MetadataOpType = 14
	OpBeginPartitionReassignment       MetadataOpType = 15
	OpCommitPartitionReassignment      MetadataOpType = 16
	OpFinishPartitionReassignment      MetadataOpType = 17
	OpBeginPartitionReassignmentAbort  MetadataOpType = 18
	OpFinishPartitionReassignmentAbort MetadataOpType = 19
)

const MaxTransactionPruneBatch = 1000

// MetadataCommand represents a serialized mutation submitted to the controller quorum.
type MetadataCommand struct {
	Op                     MetadataOpType    `json:"op"`
	Node                   *NodeInfo         `json:"node,omitempty"`
	BrokerVersion          string            `json:"broker_version,omitempty"`
	Partition              *PartitionGroupID `json:"partition,omitempty"`
	Replicas               []string          `json:"replicas,omitempty"`
	ExpectedReplicas       []string          `json:"expected_replicas,omitempty"`
	Leader                 string            `json:"leader,omitempty"`
	RequireClientAddress   bool              `json:"require_client_address,omitempty"`
	ISR                    []string          `json:"isr,omitempty"`
	Epoch                  uint64            `json:"epoch,omitempty"`
	ResultEpoch            uint64            `json:"result_epoch,omitempty"`
	ExpectedLeader         string            `json:"expected_leader,omitempty"`
	UpdatedAt              time.Time         `json:"updated_at,omitempty"`
	TransactionID          string            `json:"transaction_id,omitempty"`
	TransactionFingerprint string            `json:"transaction_fingerprint,omitempty"`
	Participants           []string          `json:"participants,omitempty"`
	Participant            string            `json:"participant,omitempty"`
	Decision               string            `json:"decision,omitempty"`
	Deadline               time.Time         `json:"deadline,omitempty"`
	Schema                 *SchemaDefinition `json:"schema,omitempty"`
	SchemaName             string            `json:"schema_name,omitempty"`
	SchemaVersion          string            `json:"schema_version,omitempty"`
	ACLPrincipal           *ACLPrincipal     `json:"acl_principal,omitempty"`
	ACLPrincipalName       string            `json:"acl_principal_name,omitempty"`
	PruneBefore            time.Time         `json:"prune_before,omitempty"`
	Limit                  int               `json:"limit,omitempty"`
}

// SchemaDefinition stores the deterministic validation metadata replicated by
// the controller quorum. It intentionally has no dependency on broker types.
type SchemaDefinition struct {
	Name           string    `json:"name"`
	Version        string    `json:"version"`
	ContentType    string    `json:"content_type"`
	Required       []string  `json:"required,omitempty"`
	AllowUnknown   bool      `json:"allow_unknown"`
	CompatibleWith string    `json:"compatible_with,omitempty"`
	Deprecated     bool      `json:"deprecated,omitempty"`
	Description    string    `json:"description,omitempty"`
	RegisteredAt   time.Time `json:"registered_at"`
}

// ACLPrincipal is replicated credential policy. TokenHash contains only a
// one-way digest; bearer token values are never written to controller logs.
type ACLPrincipal struct {
	Name                  string    `json:"name"`
	TokenHash             string    `json:"token_hash,omitempty"`
	CertificateIdentities []string  `json:"certificate_identities,omitempty"`
	Permissions           []string  `json:"permissions,omitempty"`
	Tenants               []string  `json:"tenants,omitempty"`
	RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
	Burst                 int       `json:"burst,omitempty"`
	MaxConcurrent         int       `json:"max_concurrent,omitempty"`
	ExpiresAt             time.Time `json:"expires_at,omitempty"`
	Revoked               bool      `json:"revoked,omitempty"`
	UpdatedAt             time.Time `json:"updated_at"`
}

// PartitionMetadata describes the current topology of a single partition.
type PartitionMetadata struct {
	ID           PartitionGroupID       `json:"id"`
	Leader       string                 `json:"leader"`
	LeaderEpoch  uint64                 `json:"leader_epoch"`
	Replicas     []string               `json:"replicas"`
	ISR          []string               `json:"isr"`
	Routable     bool                   `json:"routable,omitempty"`
	Reassignment *PartitionReassignment `json:"reassignment,omitempty"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// PartitionReassignment is durable controller state for a staged replica-set
// change. The current assignment remains authoritative while Phase is copying;
// after commit, the desired assignment is active while old replicas are removed.
type PartitionReassignment struct {
	ExpectedReplicas []string  `json:"expected_replicas"`
	ExpectedLeader   string    `json:"expected_leader"`
	ExpectedEpoch    uint64    `json:"expected_epoch"`
	DesiredReplicas  []string  `json:"desired_replicas"`
	DesiredLeader    string    `json:"desired_leader"`
	StartedAt        time.Time `json:"started_at"`
	Phase            string    `json:"phase"`
}

const (
	PartitionReassignmentCopying  = "copying"
	PartitionReassignmentCleanup  = "cleanup"
	PartitionReassignmentAborting = "aborting"
)

// PartitionPlacementCandidate carries a voter eligible for initial placement,
// controller Raft progress, and optional complete disk, workload, and
// partition replication signals observed while planning.
type PartitionPlacementCandidate struct {
	NodeID                 string `json:"node_id"`
	ReplicationLag         uint64 `json:"replication_lag"`
	ProgressKnown          bool   `json:"progress_known"`
	RecentActive           bool   `json:"recent_active"`
	PartitionLag           uint64 `json:"partition_lag,omitempty"`
	PartitionProgressKnown bool   `json:"partition_progress_known"`
	PartitionRecentActive  bool   `json:"partition_recent_active"`
	PartitionInSync        bool   `json:"partition_in_sync"`
	DiskUsageKnown         bool   `json:"disk_usage_known"`
	DiskAvailableBytes     uint64 `json:"disk_available_bytes,omitempty"`
	WorkloadMessages       int    `json:"workload_messages,omitempty"`
	WorkloadLeased         int    `json:"workload_leased,omitempty"`
	WorkloadKnown          bool   `json:"workload_known"`
}

// ControllerQuorumStats reports bounded counts for cluster metadata and
// transaction recovery without returning every replicated record.
type ControllerQuorumStats struct {
	Nodes                         int `json:"nodes"`
	Partitions                    int `json:"partitions"`
	Transactions                  int `json:"transactions"`
	Schemas                       int `json:"schemas"`
	ACLPrincipals                 int `json:"acl_principals"`
	UndecidedTransactions         int `json:"undecided_transactions"`
	TransactionsPendingResolution int `json:"transactions_pending_resolution"`
}

// ClusterMetadata holds the in-memory replicated topology.
type ClusterMetadata struct {
	mu                 sync.RWMutex
	proposalMu         sync.Mutex
	Nodes              map[string]*NodeInfo                    `json:"nodes"`
	Partitions         map[PartitionGroupID]*PartitionMetadata `json:"partitions"`
	Transactions       map[string]*CoordinatorTransaction      `json:"transactions"`
	transactionIDRoot  *transactionIDNode                      `json:"-"`
	transactionIDCount int                                     `json:"-"`
	Schemas            map[string]*SchemaDefinition            `json:"schemas,omitempty"`
	ACLPrincipals      map[string]*ACLPrincipal                `json:"acl_principals,omitempty"`
	ACLRevision        uint64                                  `json:"acl_revision,omitempty"`
}

func NewClusterMetadata() *ClusterMetadata {
	return &ClusterMetadata{
		Nodes:         make(map[string]*NodeInfo),
		Partitions:    make(map[PartitionGroupID]*PartitionMetadata),
		Transactions:  make(map[string]*CoordinatorTransaction),
		Schemas:       make(map[string]*SchemaDefinition),
		ACLPrincipals: make(map[string]*ACLPrincipal),
	}
}

// MetadataStateMachine applies metadata commands to ClusterMetadata.
type MetadataStateMachine struct {
	meta *ClusterMetadata
}

func NewMetadataStateMachine(meta *ClusterMetadata) *MetadataStateMachine {
	if meta == nil {
		meta = NewClusterMetadata()
	}
	ensureClusterMetadataMaps(meta)
	return &MetadataStateMachine{meta: meta}
}

func ensureClusterMetadataMaps(meta *ClusterMetadata) {
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.Nodes == nil {
		meta.Nodes = make(map[string]*NodeInfo)
	}
	if meta.Partitions == nil {
		meta.Partitions = make(map[PartitionGroupID]*PartitionMetadata)
	}
	if meta.Transactions == nil {
		meta.Transactions = make(map[string]*CoordinatorTransaction)
	}
	if meta.transactionIDCount != len(meta.Transactions) {
		meta.transactionIDRoot = nil
		meta.transactionIDCount = 0
		for id := range meta.Transactions {
			meta.transactionIDRoot = insertTransactionID(meta.transactionIDRoot, id)
			meta.transactionIDCount++
		}
	}
	if meta.Schemas == nil {
		meta.Schemas = make(map[string]*SchemaDefinition)
	}
	if meta.ACLPrincipals == nil {
		meta.ACLPrincipals = make(map[string]*ACLPrincipal)
	}
}

func (sm *MetadataStateMachine) Apply(entry *LogEntry) error {
	if len(entry.Data) == 0 {
		return nil
	}

	var cmd MetadataCommand
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("decode metadata command: %w", err)
	}

	sm.meta.mu.Lock()
	defer sm.meta.mu.Unlock()

	switch cmd.Op {
	case OpRegisterNode:
		if cmd.Node == nil || cmd.Node.ID == "" {
			return errors.New("invalid node info")
		}
		if cmd.BrokerVersion != "" {
			if err := validateBrokerVersion(cmd.BrokerVersion); err != nil {
				return err
			}
			if cmd.UpdatedAt.IsZero() {
				return errors.New("node broker version update is missing its timestamp")
			}
			current := sm.meta.Nodes[cmd.Node.ID]
			if current == nil {
				return ErrNodeNotFound
			}
			if current.State == NodeLeft {
				return ErrInvalidNodeState
			}
			updated := cloneNodeInfo(current)
			if updated.Metadata == nil {
				updated.Metadata = make(map[string]string, 1)
			}
			updated.Metadata[NodeMetadataBrokerVersion] = cmd.BrokerVersion
			updated.UpdatedAt = cmd.UpdatedAt
			sm.meta.Nodes[cmd.Node.ID] = updated
			return nil
		}
		if err := validateNodePlacementMetadata(cmd.Node); err != nil {
			return err
		}
		if err := validateClientAddress(cmd.Node.ClientAddress); err != nil {
			return err
		}
		if !validNodeState(cmd.Node.State) {
			return ErrInvalidNodeState
		}
		if current := sm.meta.Nodes[cmd.Node.ID]; current != nil && current.State == NodeLeft && cmd.Node.State != NodeLeft {
			return fmt.Errorf("%w: departed logical node %q cannot be reactivated", ErrInvalidNodeState, cmd.Node.ID)
		}
		if err := validateUniqueNodeAddress(sm.meta.Nodes, cmd.Node); err != nil {
			return err
		}
		if err := validateNodeClientAddressChange(sm.meta.Nodes, sm.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if err := validateNodeFailureDomainChange(sm.meta.Nodes, sm.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if err := validatePendingReassignmentNodeState(sm.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if cmd.Node.State == NodeLeft {
			for _, pm := range sm.meta.Partitions {
				if partitionReferencesNode(pm, cmd.Node.ID) {
					return fmt.Errorf("%w: %s", ErrNodeInUse, cmd.Node.ID)
				}
			}
		}
		sm.meta.Nodes[cmd.Node.ID] = cloneNodeInfo(cmd.Node)

	case OpDeregisterNode:
		if cmd.Node == nil || cmd.Node.ID == "" {
			return errors.New("invalid node ID")
		}
		for _, pm := range sm.meta.Partitions {
			if partitionReferencesNode(pm, cmd.Node.ID) {
				return fmt.Errorf("%w: %s", ErrNodeInUse, cmd.Node.ID)
			}
		}
		delete(sm.meta.Nodes, cmd.Node.ID)

	case OpAssignPartition:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidPartitionAssignment)
		}
		if err := validatePartitionAssignment(*cmd.Partition, cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		pid := cmd.Partition.Canonical()
		if err := validatePartitionNodeStates(sm.meta.Nodes, sm.meta.Partitions[pid], cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		if cmd.RequireClientAddress {
			if err := validateRoutableReplicas(sm.meta.Nodes, cmd.Replicas); err != nil {
				return err
			}
		}
		if existing := sm.meta.Partitions[pid]; existing != nil {
			if existing.Leader == cmd.Leader && sameReplicaSet(existing.Replicas, cmd.Replicas) {
				if cmd.RequireClientAddress {
					existing.Routable = true
				}
				return nil
			}
			return fmt.Errorf("%w: %s", ErrPartitionReassignmentRequired, pid)
		}
		sm.meta.Partitions[pid] = &PartitionMetadata{
			ID:          pid,
			Leader:      cmd.Leader,
			LeaderEpoch: 1,
			Replicas:    append([]string(nil), cmd.Replicas...),
			ISR:         append([]string(nil), cmd.ISR...),
			Routable:    cmd.RequireClientAddress,
			UpdatedAt:   cmd.UpdatedAt,
		}

	case OpBeginPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pid := cmd.Partition.Canonical()
		pm := sm.meta.Partitions[pid]
		if err := validateBeginPartitionReassignment(pm, cmd); err != nil {
			return err
		}
		if pm.Reassignment != nil {
			return nil
		}
		if err := validatePartitionNodeStates(sm.meta.Nodes, pm, cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		if cmd.RequireClientAddress {
			if err := validateRoutableReplicas(sm.meta.Nodes, cmd.Replicas); err != nil {
				return err
			}
		}
		pm.Reassignment = &PartitionReassignment{
			ExpectedReplicas: append([]string(nil), cmd.ExpectedReplicas...),
			ExpectedLeader:   cmd.ExpectedLeader,
			ExpectedEpoch:    cmd.Epoch,
			DesiredReplicas:  append([]string(nil), cmd.Replicas...),
			DesiredLeader:    cmd.Leader,
			StartedAt:        cmd.UpdatedAt,
			Phase:            PartitionReassignmentCopying,
		}
		pm.UpdatedAt = cmd.UpdatedAt

	case OpCommitPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pm := sm.meta.Partitions[cmd.Partition.Canonical()]
		if err := validateCommitPartitionReassignment(pm, cmd); err != nil {
			return err
		}
		if pm.Reassignment.Phase == PartitionReassignmentCleanup {
			return nil
		}
		if err := validatePartitionNodeStates(sm.meta.Nodes, pm, cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		if pm.Routable {
			if err := validateRoutableReplicas(sm.meta.Nodes, cmd.Replicas); err != nil {
				return err
			}
		}
		pm.Replicas = append([]string(nil), pm.Reassignment.DesiredReplicas...)
		pm.Leader = pm.Reassignment.DesiredLeader
		pm.LeaderEpoch = cmd.ResultEpoch
		pm.ISR = append([]string(nil), cmd.ISR...)
		pm.Reassignment.Phase = PartitionReassignmentCleanup
		pm.UpdatedAt = cmd.UpdatedAt

	case OpFinishPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pm := sm.meta.Partitions[cmd.Partition.Canonical()]
		if err := validateFinishPartitionReassignment(pm, cmd); err != nil {
			return err
		}
		pm.Reassignment = nil
		pm.UpdatedAt = cmd.UpdatedAt

	case OpBeginPartitionReassignmentAbort:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pm := sm.meta.Partitions[cmd.Partition.Canonical()]
		if err := validateBeginPartitionReassignmentAbort(pm, cmd); err != nil {
			return err
		}
		pm.Reassignment.Phase = PartitionReassignmentAborting
		pm.UpdatedAt = cmd.UpdatedAt

	case OpFinishPartitionReassignmentAbort:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pm := sm.meta.Partitions[cmd.Partition.Canonical()]
		if err := validateFinishPartitionReassignmentAbort(pm, cmd); err != nil {
			return err
		}
		pm.Reassignment = nil
		pm.UpdatedAt = cmd.UpdatedAt

	case OpUpdateISR:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidISR)
		} else {
			pid := cmd.Partition.Canonical()
			pm, ok := sm.meta.Partitions[pid]
			if !ok {
				return ErrPartitionNotFound
			}
			if err := validateISR(cmd.ISR, pm.Replicas); err != nil {
				return err
			}
			pm.ISR = append([]string(nil), cmd.ISR...)
			pm.UpdatedAt = cmd.UpdatedAt
		}

	case OpAdvanceEpoch:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidPartitionAssignment)
		} else {
			pid := cmd.Partition.Canonical()
			pm, ok := sm.meta.Partitions[pid]
			if !ok {
				return ErrPartitionNotFound
			}
			if pm.Reassignment != nil {
				return ErrPartitionReassignmentRequired
			}
			if cmd.Leader != "" && !containsReplica(pm.Replicas, cmd.Leader) {
				return fmt.Errorf("%w: leader %q is not assigned to %s", ErrInvalidPartitionAssignment, cmd.Leader, pid)
			}
			if cmd.RequireClientAddress {
				if err := validateRoutableReplicas(sm.meta.Nodes, pm.Replicas); err != nil {
					return err
				}
				pm.Routable = true
			}
			if cmd.Leader != "" {
				// Commands from logs written before expected-leader fencing was
				// introduced have no ExpectedLeader. Replay those committed entries
				// as recorded; all new proposals are fenced in validateCommand.
				if cmd.ExpectedLeader != "" && (pm.Leader != cmd.ExpectedLeader || pm.LeaderEpoch != cmd.Epoch) {
					return ErrStaleEpoch
				}
				if err := validatePartitionLeaderState(sm.meta.Nodes, pm.Leader, cmd.Leader); err != nil {
					return err
				}
			}
			pm.LeaderEpoch++
			if cmd.Leader != "" {
				pm.Leader = cmd.Leader
			}
			pm.UpdatedAt = cmd.UpdatedAt
		}

	case OpBeginTransaction:
		if cmd.UpdatedAt.IsZero() {
			return ErrInvalidTransaction
		}
		if err := validateTransactionFingerprint(cmd.TransactionFingerprint); err != nil {
			return err
		}
		if err := validateTransactionBegin(cmd.TransactionID, cmd.Participants, cmd.Deadline); err != nil {
			return err
		}
		if current, ok := sm.meta.Transactions[cmd.TransactionID]; ok {
			if sameStrings(current.Participants, cmd.Participants) && current.Deadline.Equal(cmd.Deadline) && current.Fingerprint == cmd.TransactionFingerprint {
				return nil
			}
			return ErrTransactionConflict
		}
		if !cmd.UpdatedAt.Before(cmd.Deadline) {
			return ErrTransactionExpired
		}
		sm.meta.Transactions[cmd.TransactionID] = &CoordinatorTransaction{
			ID: cmd.TransactionID, Participants: append([]string(nil), cmd.Participants...),
			Fingerprint: cmd.TransactionFingerprint, Deadline: cmd.Deadline, CreatedAt: cmd.UpdatedAt, UpdatedAt: cmd.UpdatedAt,
		}
		sm.meta.transactionIDRoot = insertTransactionID(sm.meta.transactionIDRoot, cmd.TransactionID)
		sm.meta.transactionIDCount++

	case OpPrepareTransactionParticipant:
		if cmd.UpdatedAt.IsZero() {
			return ErrInvalidTransaction
		}
		tx, ok := sm.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if tx.Decision != "" {
			return ErrTransactionDecided
		}
		if !cmd.UpdatedAt.Before(tx.Deadline) {
			return ErrTransactionExpired
		}
		if !containsString(tx.Participants, cmd.Participant) {
			return ErrUnknownTransactionParticipant
		}
		if !containsString(tx.Prepared, cmd.Participant) {
			tx.Prepared = appendUniqueSorted(tx.Prepared, cmd.Participant)
			tx.UpdatedAt = cmd.UpdatedAt
		}

	case OpDecideTransaction:
		if cmd.UpdatedAt.IsZero() {
			return ErrInvalidTransaction
		}
		tx, ok := sm.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if cmd.Decision != TransactionCommit && cmd.Decision != TransactionAbort {
			return ErrInvalidTransactionDecision
		}
		if tx.Decision != "" {
			if tx.Decision == cmd.Decision {
				return nil
			}
			return ErrTransactionConflict
		}
		if cmd.Decision == TransactionCommit && !sameStrings(tx.Prepared, tx.Participants) {
			return ErrTransactionNotPrepared
		}
		if cmd.Decision == TransactionCommit && !cmd.UpdatedAt.Before(tx.Deadline) {
			return ErrTransactionExpired
		}
		tx.Decision = cmd.Decision
		tx.UpdatedAt = cmd.UpdatedAt

	case OpResolveTransactionParticipant:
		if cmd.UpdatedAt.IsZero() {
			return ErrInvalidTransaction
		}
		tx, ok := sm.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if tx.Decision == "" {
			return ErrTransactionUndecided
		}
		if !containsString(tx.Participants, cmd.Participant) {
			return ErrUnknownTransactionParticipant
		}
		if !containsString(tx.Resolved, cmd.Participant) {
			tx.Resolved = appendUniqueSorted(tx.Resolved, cmd.Participant)
			tx.UpdatedAt = cmd.UpdatedAt
		}

	case OpPruneResolvedTransactions:
		if cmd.PruneBefore.IsZero() || cmd.Limit <= 0 || cmd.Limit > MaxTransactionPruneBatch {
			return ErrInvalidTransaction
		}
		ids := make([]string, 0, len(sm.meta.Transactions))
		for id, tx := range sm.meta.Transactions {
			if tx.Decision != "" && len(tx.Resolved) == len(tx.Participants) && tx.UpdatedAt.Before(cmd.PruneBefore) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		if len(ids) > cmd.Limit {
			ids = ids[:cmd.Limit]
		}
		for _, id := range ids {
			delete(sm.meta.Transactions, id)
			sm.meta.transactionIDRoot = deleteTransactionID(sm.meta.transactionIDRoot, id)
			sm.meta.transactionIDCount--
		}

	case OpRegisterSchema:
		if cmd.Schema == nil || cmd.UpdatedAt.IsZero() {
			return ErrInvalidSchema
		}
		schema, err := normalizeSchemaDefinition(*cmd.Schema)
		if err != nil || schema.Deprecated {
			return ErrInvalidSchema
		}
		key := schemaMetadataKey(schema.Name, schema.Version)
		if existing := sm.meta.Schemas[key]; existing != nil {
			if existing.Deprecated {
				return ErrSchemaDeprecated
			}
			schema.RegisteredAt = existing.RegisteredAt
		} else {
			schema.RegisteredAt = cmd.UpdatedAt
		}
		sm.meta.Schemas[key] = &schema

	case OpDeprecateSchema:
		if cmd.SchemaName == "" || cmd.SchemaVersion == "" {
			return ErrInvalidSchema
		}
		locator, err := normalizeSchemaDefinition(SchemaDefinition{Name: cmd.SchemaName, Version: cmd.SchemaVersion})
		if err != nil || cmd.UpdatedAt.IsZero() {
			return ErrInvalidSchema
		}
		key := schemaMetadataKey(locator.Name, locator.Version)
		schema, ok := sm.meta.Schemas[key]
		if !ok {
			return ErrSchemaNotFound
		}
		schema.Deprecated = true

	case OpPutACLPrincipal:
		if cmd.ACLPrincipal == nil || cmd.UpdatedAt.IsZero() {
			return ErrInvalidACLPrincipal
		}
		principal, err := normalizeACLPrincipal(*cmd.ACLPrincipal)
		if err != nil || principal.Revoked {
			return ErrInvalidACLPrincipal
		}
		principal.UpdatedAt = cmd.UpdatedAt
		sm.meta.ACLPrincipals[principal.Name] = &principal
		sm.meta.ACLRevision++

	case OpRevokeACLPrincipal:
		if cmd.ACLPrincipalName == "" || cmd.UpdatedAt.IsZero() {
			return ErrInvalidACLPrincipal
		}
		principal := sm.meta.ACLPrincipals[cmd.ACLPrincipalName]
		if principal == nil {
			return ErrACLPrincipalNotFound
		}
		principal.Revoked = true
		principal.UpdatedAt = cmd.UpdatedAt
		sm.meta.ACLRevision++

	default:
		return fmt.Errorf("cluster metadata: unknown operation %d", cmd.Op)
	}

	return nil
}

func (sm *MetadataStateMachine) Snapshot() ([]byte, error) {
	sm.meta.mu.RLock()
	defer sm.meta.mu.RUnlock()
	return json.Marshal(sm.meta)
}

func (sm *MetadataStateMachine) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var newMeta ClusterMetadata
	if err := json.Unmarshal(data, &newMeta); err != nil {
		return err
	}
	if newMeta.Nodes == nil {
		newMeta.Nodes = make(map[string]*NodeInfo)
	}
	if newMeta.Partitions == nil {
		newMeta.Partitions = make(map[PartitionGroupID]*PartitionMetadata)
	}
	if newMeta.Transactions == nil {
		newMeta.Transactions = make(map[string]*CoordinatorTransaction)
	}
	newMeta.transactionIDRoot = nil
	newMeta.transactionIDCount = 0
	for id := range newMeta.Transactions {
		newMeta.transactionIDRoot = insertTransactionID(newMeta.transactionIDRoot, id)
		newMeta.transactionIDCount++
	}
	if newMeta.Schemas == nil {
		newMeta.Schemas = make(map[string]*SchemaDefinition)
	}
	if newMeta.ACLPrincipals == nil {
		newMeta.ACLPrincipals = make(map[string]*ACLPrincipal)
	}
	if err := validateClusterMetadataSnapshot(&newMeta); err != nil {
		return fmt.Errorf("validate cluster metadata snapshot: %w", err)
	}
	canonicalPartitions := make(map[PartitionGroupID]*PartitionMetadata, len(newMeta.Partitions))
	for key, partition := range newMeta.Partitions {
		canonicalKey := key.Canonical()
		if _, exists := canonicalPartitions[canonicalKey]; exists {
			return fmt.Errorf("validate cluster metadata snapshot: duplicate canonical partition %s", canonicalKey)
		}
		copyPartition := clonePartitionMetadata(partition)
		copyPartition.ID = partition.ID.Canonical()
		canonicalPartitions[canonicalKey] = copyPartition
	}
	newMeta.Partitions = canonicalPartitions
	for key, schema := range newMeta.Schemas {
		if schema == nil {
			return fmt.Errorf("validate cluster metadata snapshot: invalid schema %q", key)
		}
		normalized, err := normalizeSchemaDefinition(*schema)
		if err != nil || normalized.Name != schema.Name || normalized.Version != schema.Version ||
			key != schemaMetadataKey(schema.Name, schema.Version) || schema.RegisteredAt.IsZero() {
			return fmt.Errorf("validate cluster metadata snapshot: invalid schema %q", key)
		}
		normalized.RegisteredAt = schema.RegisteredAt
		newMeta.Schemas[key] = &normalized
	}
	for name, principal := range newMeta.ACLPrincipals {
		if principal == nil {
			return fmt.Errorf("validate cluster metadata snapshot: invalid ACL principal %q", name)
		}
		normalized, err := normalizeACLPrincipal(*principal)
		if err != nil || normalized.Name != name || principal.UpdatedAt.IsZero() {
			return fmt.Errorf("validate cluster metadata snapshot: invalid ACL principal %q", name)
		}
		normalized.UpdatedAt = principal.UpdatedAt
		newMeta.ACLPrincipals[name] = &normalized
	}
	sm.meta.mu.Lock()
	defer sm.meta.mu.Unlock()
	sm.meta.Nodes = newMeta.Nodes
	sm.meta.Partitions = newMeta.Partitions
	sm.meta.Transactions = newMeta.Transactions
	sm.meta.transactionIDRoot = newMeta.transactionIDRoot
	sm.meta.transactionIDCount = newMeta.transactionIDCount
	sm.meta.Schemas = newMeta.Schemas
	sm.meta.ACLPrincipals = newMeta.ACLPrincipals
	sm.meta.ACLRevision = newMeta.ACLRevision
	return nil
}

// ControllerQuorum coordinates cluster topology and partition assignments.
type ControllerQuorum struct {
	controller           *RaftController
	meta                 *ClusterMetadata
	timeout              time.Duration
	peerIDForAddress     func(string) (uint64, bool)
	peerRoleForAddress   func(string) string
	requireClientAddress bool
}

// ControllerQuorumConfig configures the metadata consensus quorum.
type ControllerQuorumConfig struct {
	Controller *RaftController
	Metadata   *ClusterMetadata
	Timeout    time.Duration
	// PeerIDForAddress, when set, verifies that placements reference the
	// authenticated controller Raft peer set and resolves duplicate addresses.
	PeerIDForAddress func(string) (uint64, bool)
	// PeerRoleForAddress, when set, ensures assigned replicas are current voters.
	PeerRoleForAddress func(string) string
	// RequireClientAddress requires every assigned replica to advertise its
	// HTTP(S) endpoint so any replica can become a routable leader.
	RequireClientAddress bool
}

// NewControllerQuorum creates a metadata consensus controller.
func NewControllerQuorum(cfg ControllerQuorumConfig) (*ControllerQuorum, error) {
	if cfg.Controller == nil {
		return nil, errors.New("controller quorum: RaftController is required")
	}
	if cfg.Metadata == nil {
		cfg.Metadata = NewClusterMetadata()
	}
	ensureClusterMetadataMaps(cfg.Metadata)
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &ControllerQuorum{
		controller:           cfg.Controller,
		meta:                 cfg.Metadata,
		timeout:              cfg.Timeout,
		peerIDForAddress:     cfg.PeerIDForAddress,
		peerRoleForAddress:   cfg.PeerRoleForAddress,
		requireClientAddress: cfg.RequireClientAddress,
	}, nil
}

// RegisterNode submits a node registration command through consensus.
func (cq *ControllerQuorum) RegisterNode(ctx context.Context, node *NodeInfo) error {
	if node == nil || node.ID == "" {
		return errors.New("invalid node info")
	}
	if err := validateNodePlacementMetadata(node); err != nil {
		return err
	}
	cmd := MetadataCommand{
		Op:   OpRegisterNode,
		Node: cloneNodeInfo(node),
	}
	return cq.propose(ctx, cmd)
}

// ReportNodeBrokerVersion updates only the broker_version metadata key for an
// existing node. The replicated command remains an OpRegisterNode envelope so
// older binaries can apply its full Node snapshot; newer state machines merge
// the version key into the current record without replacing concurrent metadata.
func (cq *ControllerQuorum) ReportNodeBrokerVersion(ctx context.Context, nodeID, version string) error {
	if nodeID == "" {
		return errors.New("invalid node ID")
	}
	if err := validateBrokerVersion(version); err != nil {
		return err
	}
	nodes, err := cq.GetNodesLinearizable(ctx)
	if err != nil {
		return err
	}
	var node *NodeInfo
	for _, candidate := range nodes {
		if candidate != nil && candidate.ID == nodeID {
			node = candidate
			break
		}
	}
	if node == nil {
		return ErrNodeNotFound
	}
	if node.State == NodeLeft {
		return ErrInvalidNodeState
	}
	if node.Metadata[NodeMetadataBrokerVersion] == version {
		return nil
	}
	if node.Metadata == nil {
		node.Metadata = make(map[string]string, 1)
	}
	node.Metadata[NodeMetadataBrokerVersion] = version
	return cq.propose(ctx, MetadataCommand{
		Op: OpRegisterNode, Node: node, BrokerVersion: version,
	})
}

// DeregisterNode removes a node registration through consensus.
func (cq *ControllerQuorum) DeregisterNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return errors.New("invalid node ID")
	}
	cmd := MetadataCommand{
		Op:   OpDeregisterNode,
		Node: &NodeInfo{ID: nodeID},
	}
	return cq.propose(ctx, cmd)
}

// AssignPartition creates or updates a partition assignment across the cluster.
func (cq *ControllerQuorum) AssignPartition(ctx context.Context, topic string, partition int, replicas []string, leader string) error {
	return cq.AssignResourcePartition(ctx, PartitionResourceTopic, topic, partition, replicas, leader)
}

// AssignResourcePartition creates an initial assignment for a typed resource.
// Topic assignments remain the default for callers using AssignPartition.
func (cq *ControllerQuorum) AssignResourcePartition(ctx context.Context, kind PartitionResourceKind, name string, partition int, replicas []string, leader string) error {
	replicas = append([]string(nil), replicas...)
	sort.Strings(replicas)
	pid := (PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	cmd := MetadataCommand{
		Op:                   OpAssignPartition,
		Partition:            &pid,
		Replicas:             append([]string(nil), replicas...),
		Leader:               leader,
		ISR:                  append([]string(nil), replicas...),
		RequireClientAddress: cq.requireClientAddress,
	}
	return cq.propose(ctx, cmd)
}

// AssignBalancedResourcePartition selects replicas from the supplied active
// controller voters and commits that selection while holding the same proposal
// lock used by all metadata mutations. Concurrent assignments therefore score
// against the latest committed partition counts.
func (cq *ControllerQuorum) AssignBalancedResourcePartition(ctx context.Context, kind PartitionResourceKind, name string, partition, replicationFactor int, candidates []PartitionPlacementCandidate) error {
	pid := (PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	if name == "" || partition < 0 || replicationFactor <= 0 || len(candidates) < replicationFactor {
		return fmt.Errorf("%w: invalid resource, replication factor, or candidate count", ErrInvalidPartitionAssignment)
	}
	candidates = append([]PartitionPlacementCandidate(nil), candidates...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].NodeID < candidates[j].NodeID })
	for i, candidate := range candidates {
		if candidate.NodeID == "" || candidate.WorkloadMessages < 0 || candidate.WorkloadLeased < 0 ||
			(i > 0 && candidates[i-1].NodeID == candidate.NodeID) {
			return fmt.Errorf("%w: placement candidates must be unique with non-negative workload metrics", ErrInvalidPartitionAssignment)
		}
	}
	cq.meta.proposalMu.Lock()
	defer cq.meta.proposalMu.Unlock()
	if !cq.controller.IsLeader() {
		return ErrNotLeader
	}
	cq.meta.mu.RLock()
	replicaCounts := make(map[string]int, len(candidates))
	leaderCounts := make(map[string]int, len(candidates))
	capacityWeights := make(map[string]uint32, len(candidates))
	failureDomains := make(map[string]string, len(candidates))
	peerOwners := make(map[uint64]string, len(candidates))
	for _, candidate := range candidates {
		id := candidate.NodeID
		node := cq.meta.Nodes[id]
		if node == nil || node.State != NodeActive || node.Address == "" {
			cq.meta.mu.RUnlock()
			return fmt.Errorf("%w: placement candidate %q is not registered and active", ErrNodeUnavailable, id)
		}
		if cq.peerIDForAddress != nil {
			peerID, ok := cq.peerIDForAddress(node.Address)
			if !ok {
				cq.meta.mu.RUnlock()
				return fmt.Errorf("%w: placement candidate %q is not an authenticated controller voter", ErrNodeUnavailable, id)
			}
			if previous, exists := peerOwners[peerID]; exists {
				cq.meta.mu.RUnlock()
				return fmt.Errorf("%w: placement candidates %q and %q map to the same controller peer", ErrInvalidPartitionAssignment, previous, id)
			}
			peerOwners[peerID] = id
		}
		if cq.peerRoleForAddress != nil && cq.peerRoleForAddress(node.Address) != "voter" {
			cq.meta.mu.RUnlock()
			return fmt.Errorf("%w: placement candidate %q is not a controller voter", ErrNodeUnavailable, id)
		}
		replicaCounts[id] = 0
		leaderCounts[id] = 0
		capacityWeights[id] = NodeCapacityWeight(node)
		failureDomains[id] = strings.TrimSpace(node.Metadata[NodeMetadataFailureDomain])
	}
	for _, assigned := range cq.meta.Partitions {
		for _, replica := range assigned.Replicas {
			if _, exists := replicaCounts[replica]; exists {
				replicaCounts[replica]++
			}
		}
		if _, exists := leaderCounts[assigned.Leader]; exists {
			leaderCounts[assigned.Leader]++
		}
	}
	cq.meta.mu.RUnlock()
	key := fmt.Sprintf("%s/%s/%d", pid.Kind, pid.Topic, pid.Partition)
	score := func(id string) uint64 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(id))
		return h.Sum64()
	}
	placementLess := func(a, b PartitionPlacementCandidate) bool {
		if placementLoadLess(replicaCounts[a.NodeID], capacityWeights[a.NodeID], replicaCounts[b.NodeID], capacityWeights[b.NodeID]) {
			return true
		}
		if placementLoadLess(replicaCounts[b.NodeID], capacityWeights[b.NodeID], replicaCounts[a.NodeID], capacityWeights[a.NodeID]) {
			return false
		}
		if a.PartitionProgressKnown != b.PartitionProgressKnown {
			return a.PartitionProgressKnown
		}
		if a.PartitionRecentActive != b.PartitionRecentActive {
			return a.PartitionRecentActive
		}
		if a.PartitionInSync != b.PartitionInSync {
			return a.PartitionInSync
		}
		if a.PartitionLag != b.PartitionLag {
			return a.PartitionLag < b.PartitionLag
		}
		if a.DiskUsageKnown != b.DiskUsageKnown {
			return a.DiskUsageKnown
		}
		if a.DiskUsageKnown && a.DiskAvailableBytes != b.DiskAvailableBytes {
			return a.DiskAvailableBytes > b.DiskAvailableBytes
		}
		if a.WorkloadKnown != b.WorkloadKnown {
			return a.WorkloadKnown
		}
		if a.WorkloadKnown {
			if placementLoadLess(a.WorkloadMessages, capacityWeights[a.NodeID], b.WorkloadMessages, capacityWeights[b.NodeID]) {
				return true
			}
			if placementLoadLess(b.WorkloadMessages, capacityWeights[b.NodeID], a.WorkloadMessages, capacityWeights[a.NodeID]) {
				return false
			}
			if placementLoadLess(a.WorkloadLeased, capacityWeights[a.NodeID], b.WorkloadLeased, capacityWeights[b.NodeID]) {
				return true
			}
			if placementLoadLess(b.WorkloadLeased, capacityWeights[b.NodeID], a.WorkloadLeased, capacityWeights[a.NodeID]) {
				return false
			}
		}
		if a.ProgressKnown != b.ProgressKnown {
			return a.ProgressKnown
		}
		if a.RecentActive != b.RecentActive {
			return a.RecentActive
		}
		if a.ReplicationLag != b.ReplicationLag {
			return a.ReplicationLag < b.ReplicationLag
		}
		if placementLoadLess(leaderCounts[a.NodeID], capacityWeights[a.NodeID], leaderCounts[b.NodeID], capacityWeights[b.NodeID]) {
			return true
		}
		if placementLoadLess(leaderCounts[b.NodeID], capacityWeights[b.NodeID], leaderCounts[a.NodeID], capacityWeights[a.NodeID]) {
			return false
		}
		si, sj := score(a.NodeID), score(b.NodeID)
		if si != sj {
			return si < sj
		}
		return a.NodeID < b.NodeID
	}
	selectedCandidates := make([]PartitionPlacementCandidate, 0, replicationFactor)
	selectedDomains := make(map[string]struct{}, replicationFactor)
	for len(selectedCandidates) < replicationFactor {
		best := -1
		bestDomainPenalty := 3
		for i, candidate := range candidates {
			alreadySelected := false
			for _, selected := range selectedCandidates {
				if selected.NodeID == candidate.NodeID {
					alreadySelected = true
					break
				}
			}
			if alreadySelected {
				continue
			}
			penalty := 1 // unknown failure domain
			if domain := failureDomains[candidate.NodeID]; domain != "" {
				penalty = 0
				if _, exists := selectedDomains[domain]; exists {
					penalty = 2
				}
			}
			if best < 0 || penalty < bestDomainPenalty || (penalty == bestDomainPenalty && placementLess(candidate, candidates[best])) {
				best, bestDomainPenalty = i, penalty
			}
		}
		if best < 0 {
			break
		}
		selected := candidates[best]
		selectedCandidates = append(selectedCandidates, selected)
		if domain := failureDomains[selected.NodeID]; domain != "" {
			selectedDomains[domain] = struct{}{}
		}
	}
	replicas := make([]string, 0, replicationFactor)
	selected := make(map[string]PartitionPlacementCandidate, replicationFactor)
	for _, candidate := range selectedCandidates {
		replicas = append(replicas, candidate.NodeID)
		selected[candidate.NodeID] = candidate
	}
	leader := replicas[0]
	for _, id := range replicas[1:] {
		a, b := selected[id], selected[leader]
		betterLeader := false
		switch {
		case placementLoadLess(leaderCounts[id], capacityWeights[id], leaderCounts[leader], capacityWeights[leader]):
			betterLeader = true
		case placementLoadLess(leaderCounts[leader], capacityWeights[leader], leaderCounts[id], capacityWeights[id]):
			betterLeader = false
		case a.PartitionProgressKnown != b.PartitionProgressKnown:
			betterLeader = a.PartitionProgressKnown
		case a.PartitionRecentActive != b.PartitionRecentActive:
			betterLeader = a.PartitionRecentActive
		case a.PartitionInSync != b.PartitionInSync:
			betterLeader = a.PartitionInSync
		case a.PartitionLag != b.PartitionLag:
			betterLeader = a.PartitionLag < b.PartitionLag
		case a.DiskUsageKnown != b.DiskUsageKnown:
			betterLeader = a.DiskUsageKnown
		case a.DiskUsageKnown && a.DiskAvailableBytes != b.DiskAvailableBytes:
			betterLeader = a.DiskAvailableBytes > b.DiskAvailableBytes
		case a.WorkloadKnown && b.WorkloadKnown && a.WorkloadMessages != b.WorkloadMessages:
			betterLeader = placementLoadLess(a.WorkloadMessages, capacityWeights[id], b.WorkloadMessages, capacityWeights[leader])
		case a.WorkloadKnown && b.WorkloadKnown && a.WorkloadLeased != b.WorkloadLeased:
			betterLeader = placementLoadLess(a.WorkloadLeased, capacityWeights[id], b.WorkloadLeased, capacityWeights[leader])
		case a.ProgressKnown != b.ProgressKnown:
			betterLeader = a.ProgressKnown
		case a.RecentActive != b.RecentActive:
			betterLeader = a.RecentActive
		case a.ReplicationLag != b.ReplicationLag:
			betterLeader = a.ReplicationLag < b.ReplicationLag
		default:
			betterLeader = score(id) < score(leader)
		}
		if betterLeader {
			leader = id
		}
	}
	cmd := MetadataCommand{
		Op: OpAssignPartition, Partition: &pid, Replicas: replicas, Leader: leader,
		RequireClientAddress: cq.requireClientAddress,
	}
	return cq.proposeLocked(ctx, cmd)
}

// UpdateISR submits an updated in-sync replica set for a partition.
func (cq *ControllerQuorum) UpdateISR(ctx context.Context, topic string, partition int, isr []string) error {
	return cq.UpdateResourceISR(ctx, PartitionResourceTopic, topic, partition, isr)
}

// UpdateResourceISR submits an updated ISR for a typed resource partition.
func (cq *ControllerQuorum) UpdateResourceISR(ctx context.Context, kind PartitionResourceKind, name string, partition int, isr []string) error {
	pid := (PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	cmd := MetadataCommand{
		Op:        OpUpdateISR,
		Partition: &pid,
		ISR:       append([]string(nil), isr...),
	}
	return cq.propose(ctx, cmd)
}

// AdvanceEpoch is retained for source compatibility, but metadata epochs can
// no longer be changed without transferring the partition Raft group first.
func (cq *ControllerQuorum) AdvanceEpoch(ctx context.Context, topic string, partition int, newLeader string) error {
	return ErrPartitionTransferRequired
}

// CommitPartitionLeaderTransfer advances controller metadata after the
// partition Raft group has transferred leadership. expectedLeader and epoch
// fence races between the transfer and this metadata commit.
func (cq *ControllerQuorum) CommitPartitionLeaderTransfer(ctx context.Context, topic string, partition int, expectedLeader string, epoch uint64, newLeader string) error {
	return cq.CommitResourcePartitionLeaderTransfer(ctx, PartitionResourceTopic, topic, partition, expectedLeader, epoch, newLeader)
}

// CommitResourcePartitionLeaderTransfer advances typed resource metadata after
// the corresponding partition Raft group has transferred leadership.
func (cq *ControllerQuorum) CommitResourcePartitionLeaderTransfer(ctx context.Context, kind PartitionResourceKind, name string, partition int, expectedLeader string, epoch uint64, newLeader string) error {
	if expectedLeader == "" || newLeader == "" || expectedLeader == newLeader {
		return fmt.Errorf("%w: expected and target leaders must be distinct", ErrInvalidPartitionAssignment)
	}
	pid := (PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	return cq.propose(ctx, MetadataCommand{
		Op: OpAdvanceEpoch, Partition: &pid, Leader: newLeader,
		ExpectedLeader: expectedLeader, Epoch: epoch,
		RequireClientAddress: cq.requireClientAddress,
	})
}

// GetPartition retrieves current partition metadata from the replicated state.
func (cq *ControllerQuorum) GetPartition(topic string, partition int) (*PartitionMetadata, error) {
	return cq.GetResourcePartition(PartitionResourceTopic, topic, partition)
}

// GetResourcePartition returns metadata for one typed resource partition.
func (cq *ControllerQuorum) GetResourcePartition(kind PartitionResourceKind, name string, partition int) (*PartitionMetadata, error) {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	pid := (PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	pm, ok := cq.meta.Partitions[pid]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return clonePartitionMetadata(pm), nil
}

// GetPartitionLinearizable returns partition metadata after a quorum-confirmed
// ReadIndex has been applied by this controller.
func (cq *ControllerQuorum) GetPartitionLinearizable(ctx context.Context, topic string, partition int) (*PartitionMetadata, error) {
	return cq.GetResourcePartitionLinearizable(ctx, PartitionResourceTopic, topic, partition)
}

// GetResourcePartitionLinearizable returns typed resource metadata after a
// quorum-confirmed ReadIndex has been applied by this controller.
func (cq *ControllerQuorum) GetResourcePartitionLinearizable(ctx context.Context, kind PartitionResourceKind, name string, partition int) (*PartitionMetadata, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.GetResourcePartition(kind, name, partition)
}

// ListPartitions returns all active partition metadata entries.
func (cq *ControllerQuorum) ListPartitions() []*PartitionMetadata {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	out := make([]*PartitionMetadata, 0, len(cq.meta.Partitions))
	for _, pm := range cq.meta.Partitions {
		out = append(out, clonePartitionMetadata(pm))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID.Kind.normalized() != out[j].ID.Kind.normalized() {
			return out[i].ID.Kind.normalized() < out[j].ID.Kind.normalized()
		}
		if out[i].ID.Topic == out[j].ID.Topic {
			return out[i].ID.Partition < out[j].ID.Partition
		}
		return out[i].ID.Topic < out[j].ID.Topic
	})
	return out
}

// ListPartitionsLinearizable returns a quorum-confirmed view of partition metadata.
func (cq *ControllerQuorum) ListPartitionsLinearizable(ctx context.Context) ([]*PartitionMetadata, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.ListPartitions(), nil
}

// HasResourcePartitionsLinearizable checks whether any partition for a typed
// resource is assigned after a quorum-confirmed ReadIndex. It avoids cloning
// and sorting the full assignment table on write-routing paths.
func (cq *ControllerQuorum) HasResourcePartitionsLinearizable(ctx context.Context, kind PartitionResourceKind, name string) (bool, error) {
	if cq == nil || cq.controller == nil || name == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return false, err
	}
	kind = (PartitionGroupID{Kind: kind}).Canonical().Kind
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	for id := range cq.meta.Partitions {
		id = id.Canonical()
		if id.Kind == kind && id.Topic == name {
			return true, nil
		}
	}
	return false, nil
}

// ControllerTopologySnapshot is a defensive, stable-order view of the node
// registry and partition assignments taken under one metadata read lock.
type ControllerTopologySnapshot struct {
	Nodes      []*NodeInfo          `json:"nodes"`
	Partitions []*PartitionMetadata `json:"partitions"`
}

// TopologySnapshotLinearizable returns nodes and partition assignments from
// one quorum-confirmed read point and one consistent local metadata snapshot.
func (cq *ControllerQuorum) TopologySnapshotLinearizable(ctx context.Context) (*ControllerTopologySnapshot, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	snapshot := &ControllerTopologySnapshot{
		Nodes:      make([]*NodeInfo, 0, len(cq.meta.Nodes)),
		Partitions: make([]*PartitionMetadata, 0, len(cq.meta.Partitions)),
	}
	for _, node := range cq.meta.Nodes {
		snapshot.Nodes = append(snapshot.Nodes, cloneNodeInfo(node))
	}
	sort.Slice(snapshot.Nodes, func(i, j int) bool { return snapshot.Nodes[i].ID < snapshot.Nodes[j].ID })
	for _, partition := range cq.meta.Partitions {
		snapshot.Partitions = append(snapshot.Partitions, clonePartitionMetadata(partition))
	}
	sort.Slice(snapshot.Partitions, func(i, j int) bool {
		if snapshot.Partitions[i].ID.Kind.normalized() != snapshot.Partitions[j].ID.Kind.normalized() {
			return snapshot.Partitions[i].ID.Kind.normalized() < snapshot.Partitions[j].ID.Kind.normalized()
		}
		if snapshot.Partitions[i].ID.Topic == snapshot.Partitions[j].ID.Topic {
			return snapshot.Partitions[i].ID.Partition < snapshot.Partitions[j].ID.Partition
		}
		return snapshot.Partitions[i].ID.Topic < snapshot.Partitions[j].ID.Topic
	})
	return snapshot, nil
}

// GetNodes returns all currently registered nodes.
func (cq *ControllerQuorum) GetNodes() []*NodeInfo {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	out := make([]*NodeInfo, 0, len(cq.meta.Nodes))
	for _, n := range cq.meta.Nodes {
		copyNode := *n
		if n.Metadata != nil {
			copyNode.Metadata = make(map[string]string, len(n.Metadata))
			for key, value := range n.Metadata {
				copyNode.Metadata[key] = value
			}
		}
		out = append(out, &copyNode)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// GetNodesLinearizable returns node metadata after a quorum-confirmed read
// point has been applied locally.
func (cq *ControllerQuorum) GetNodesLinearizable(ctx context.Context) ([]*NodeInfo, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.GetNodes(), nil
}

// IsLeader checks if this controller is the active leader of the metadata quorum.
func (cq *ControllerQuorum) IsLeader() bool {
	return cq.controller.IsLeader()
}

func (cq *ControllerQuorum) propose(ctx context.Context, cmd MetadataCommand) error {
	cq.meta.proposalMu.Lock()
	defer cq.meta.proposalMu.Unlock()
	return cq.proposeLocked(ctx, cmd)
}

func (cq *ControllerQuorum) proposeLocked(ctx context.Context, cmd MetadataCommand) error {
	term := cq.controller.Term()
	localLeader := cq.controller.IsLeader()
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), cq.timeout)
		defer cancel()
	}
	if !localLeader {
		// Follower metadata may lag. Establish a quorum read point before
		// validating locally; the subsequent Raft proposal is forwarded to the
		// elected controller leader and still waits for local application.
		if err := cq.controller.ReadIndex(ctx); err != nil {
			return err
		}
	}
	if cmd.UpdatedAt.IsZero() {
		cmd.UpdatedAt = time.Now().UTC()
	}
	if cmd.Op == OpRegisterNode && cmd.Node != nil {
		cmd.Node = cloneNodeInfo(cmd.Node)
		cq.meta.mu.RLock()
		if existing := cq.meta.Nodes[cmd.Node.ID]; existing != nil {
			if cmd.Node.JoinedAt.IsZero() {
				cmd.Node.JoinedAt = existing.JoinedAt
			}
			if cmd.Node.State == "" {
				cmd.Node.State = existing.State
			}
			if cmd.Node.Metadata == nil {
				cmd.Node.Metadata = cloneNodeInfo(existing).Metadata
			}
		}
		cq.meta.mu.RUnlock()
		if cmd.Node.JoinedAt.IsZero() {
			cmd.Node.JoinedAt = cmd.UpdatedAt
		}
		if cmd.Node.State == "" {
			cmd.Node.State = NodeJoining
		}
		cmd.Node.UpdatedAt = cmd.UpdatedAt
	}
	if err := cq.validateCommand(cmd); err != nil {
		return err
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	if localLeader {
		return cq.controller.ProposeAtTerm(ctx, term, data)
	}
	return cq.controller.Propose(ctx, data)
}

func (cq *ControllerQuorum) validateCommand(cmd MetadataCommand) error {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	switch cmd.Op {
	case OpRegisterNode:
		if cmd.Node == nil || cmd.Node.ID == "" {
			return errors.New("invalid node info")
		}
		if cmd.BrokerVersion != "" {
			if err := validateBrokerVersion(cmd.BrokerVersion); err != nil {
				return err
			}
			if cmd.UpdatedAt.IsZero() {
				return errors.New("node broker version update is missing its timestamp")
			}
			current := cq.meta.Nodes[cmd.Node.ID]
			if current == nil {
				return ErrNodeNotFound
			}
			if current.State == NodeLeft {
				return ErrInvalidNodeState
			}
			return nil
		}
		if err := validateNodePlacementMetadata(cmd.Node); err != nil {
			return err
		}
		if err := validateClientAddress(cmd.Node.ClientAddress); err != nil {
			return err
		}
		if !validNodeState(cmd.Node.State) {
			return ErrInvalidNodeState
		}
		if current := cq.meta.Nodes[cmd.Node.ID]; current != nil && current.State == NodeLeft && cmd.Node.State != NodeLeft {
			return fmt.Errorf("%w: departed logical node %q cannot be reactivated", ErrInvalidNodeState, cmd.Node.ID)
		}
		if err := validateUniqueNodeAddress(cq.meta.Nodes, cmd.Node); err != nil {
			return err
		}
		if err := validateNodeClientAddressChange(cq.meta.Nodes, cq.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if err := validateNodeFailureDomainChange(cq.meta.Nodes, cq.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if err := validatePendingReassignmentNodeState(cq.meta.Partitions, cmd.Node); err != nil {
			return err
		}
		if existing := cq.meta.Nodes[cmd.Node.ID]; existing != nil && existing.Address != cmd.Node.Address && cq.peerRoleForAddress != nil && cq.peerRoleForAddress(existing.Address) != "" {
			return fmt.Errorf("%w: logical node %q still uses controller peer address %q", ErrNodeInUse, cmd.Node.ID, existing.Address)
		}
		if cmd.Node.State == NodeLeft {
			for _, pm := range cq.meta.Partitions {
				if partitionReferencesNode(pm, cmd.Node.ID) {
					return fmt.Errorf("%w: %s", ErrNodeInUse, cmd.Node.ID)
				}
			}
		}
	case OpDeregisterNode:
		if cmd.Node == nil || cmd.Node.ID == "" {
			return errors.New("invalid node ID")
		}
		if node := cq.meta.Nodes[cmd.Node.ID]; node != nil && cq.peerRoleForAddress != nil && cq.peerRoleForAddress(node.Address) != "" {
			return fmt.Errorf("%w: logical node %q still has controller peer membership", ErrNodeInUse, cmd.Node.ID)
		}
		for _, pm := range cq.meta.Partitions {
			if partitionReferencesNode(pm, cmd.Node.ID) {
				return fmt.Errorf("%w: %s", ErrNodeInUse, cmd.Node.ID)
			}
		}
	case OpAssignPartition:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidPartitionAssignment)
		}
		if err := validatePartitionAssignment(*cmd.Partition, cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		pid := cmd.Partition.Canonical()
		if err := validatePartitionNodeStates(cq.meta.Nodes, cq.meta.Partitions[pid], cmd.Replicas, cmd.Leader); err != nil {
			return err
		}
		if cmd.RequireClientAddress {
			if err := validateRoutableReplicas(cq.meta.Nodes, cmd.Replicas); err != nil {
				return err
			}
		}
		if cq.peerIDForAddress != nil {
			peerOwners := make(map[uint64]string, len(cmd.Replicas))
			for _, replica := range cmd.Replicas {
				node := cq.meta.Nodes[replica]
				if node == nil {
					return fmt.Errorf("%w: partition replica %q is not registered", ErrNodeNotFound, replica)
				}
				peerID, ok := cq.peerIDForAddress(node.Address)
				if !ok {
					return fmt.Errorf("%w: partition replica %q address %q is not an authenticated controller peer", ErrNodeUnavailable, replica, node.Address)
				}
				if cq.peerRoleForAddress != nil && cq.peerRoleForAddress(node.Address) != "voter" {
					return fmt.Errorf("%w: partition replica %q is not a controller voter", ErrNodeUnavailable, replica)
				}
				if previous, exists := peerOwners[peerID]; exists {
					return fmt.Errorf("%w: logical replicas %q and %q map to controller peer %d", ErrInvalidPartitionAssignment, previous, replica, peerID)
				}
				peerOwners[peerID] = replica
			}
		}
		if existing := cq.meta.Partitions[pid]; existing != nil &&
			(existing.Leader != cmd.Leader || !sameReplicaSet(existing.Replicas, cmd.Replicas)) {
			return fmt.Errorf("%w: %s", ErrPartitionReassignmentRequired, pid)
		}
		if err := validateISR(cmd.ISR, cmd.Replicas); err != nil {
			return err
		}
	case OpBeginPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pid := cmd.Partition.Canonical()
		pm := cq.meta.Partitions[pid]
		if err := validateBeginPartitionReassignment(pm, cmd); err != nil {
			return err
		}
		if err := validateReassignmentReplicas(cq, cmd.Replicas, cmd.Leader, pm); err != nil {
			return err
		}
	case OpCommitPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		pid := cmd.Partition.Canonical()
		pm := cq.meta.Partitions[pid]
		if err := validateCommitPartitionReassignment(pm, cmd); err != nil {
			return err
		}
		if err := validateReassignmentReplicas(cq, cmd.Replicas, cmd.Leader, pm); err != nil {
			return err
		}
		if err := validateISR(cmd.ISR, cmd.Replicas); err != nil {
			return err
		}
	case OpFinishPartitionReassignment:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		if err := validateFinishPartitionReassignment(cq.meta.Partitions[cmd.Partition.Canonical()], cmd); err != nil {
			return err
		}
	case OpBeginPartitionReassignmentAbort:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		if err := validateBeginPartitionReassignmentAbort(cq.meta.Partitions[cmd.Partition.Canonical()], cmd); err != nil {
			return err
		}
	case OpFinishPartitionReassignmentAbort:
		if cmd.Partition == nil {
			return ErrInvalidPartitionAssignment
		}
		if err := validateFinishPartitionReassignmentAbort(cq.meta.Partitions[cmd.Partition.Canonical()], cmd); err != nil {
			return err
		}
	case OpUpdateISR:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidISR)
		}
		pm, ok := cq.meta.Partitions[cmd.Partition.Canonical()]
		if !ok {
			return ErrPartitionNotFound
		}
		return validateISR(cmd.ISR, pm.Replicas)
	case OpAdvanceEpoch:
		if cmd.Partition == nil {
			return fmt.Errorf("%w: missing partition ID", ErrInvalidPartitionAssignment)
		}
		pm, ok := cq.meta.Partitions[cmd.Partition.Canonical()]
		if !ok {
			return ErrPartitionNotFound
		}
		if pm.Reassignment != nil {
			return ErrPartitionReassignmentRequired
		}
		if cmd.Leader != "" && !containsReplica(pm.Replicas, cmd.Leader) {
			return fmt.Errorf("%w: leader %q is not assigned to %s", ErrInvalidPartitionAssignment, cmd.Leader, *cmd.Partition)
		}
		if cmd.Leader != "" {
			if cmd.ExpectedLeader == "" {
				return ErrPartitionTransferRequired
			}
			if pm.Leader != cmd.ExpectedLeader || pm.LeaderEpoch != cmd.Epoch {
				return ErrStaleEpoch
			}
			if err := validatePartitionLeaderState(cq.meta.Nodes, pm.Leader, cmd.Leader); err != nil {
				return err
			}
			if cmd.RequireClientAddress {
				if err := validateRoutableReplicas(cq.meta.Nodes, pm.Replicas); err != nil {
					return err
				}
			}
		}
	case OpBeginTransaction:
		if err := validateTransactionFingerprint(cmd.TransactionFingerprint); err != nil {
			return err
		}
		if err := validateTransactionBegin(cmd.TransactionID, cmd.Participants, cmd.Deadline); err != nil {
			return err
		}
		if tx, ok := cq.meta.Transactions[cmd.TransactionID]; ok {
			if sameStrings(tx.Participants, cmd.Participants) && tx.Deadline.Equal(cmd.Deadline) && tx.Fingerprint == cmd.TransactionFingerprint {
				return nil
			}
			return ErrTransactionConflict
		}
		if !cmd.UpdatedAt.Before(cmd.Deadline) {
			return ErrTransactionExpired
		}
	case OpPrepareTransactionParticipant:
		tx, ok := cq.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if tx.Decision != "" {
			return ErrTransactionDecided
		}
		if !cmd.UpdatedAt.Before(tx.Deadline) {
			return ErrTransactionExpired
		}
		if !containsString(tx.Participants, cmd.Participant) {
			return ErrUnknownTransactionParticipant
		}
	case OpDecideTransaction:
		tx, ok := cq.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if cmd.Decision != TransactionCommit && cmd.Decision != TransactionAbort {
			return ErrInvalidTransactionDecision
		}
		if tx.Decision != "" {
			if tx.Decision != cmd.Decision {
				return ErrTransactionConflict
			}
			return nil
		}
		if cmd.Decision == TransactionCommit && !sameStrings(tx.Prepared, tx.Participants) {
			return ErrTransactionNotPrepared
		}
		if cmd.Decision == TransactionCommit && !cmd.UpdatedAt.Before(tx.Deadline) {
			return ErrTransactionExpired
		}
	case OpResolveTransactionParticipant:
		tx, ok := cq.meta.Transactions[cmd.TransactionID]
		if !ok {
			return ErrTransactionNotFound
		}
		if tx.Decision == "" {
			return ErrTransactionUndecided
		}
		if !containsString(tx.Participants, cmd.Participant) {
			return ErrUnknownTransactionParticipant
		}
	case OpPruneResolvedTransactions:
		if cmd.PruneBefore.IsZero() || cmd.Limit <= 0 || cmd.Limit > MaxTransactionPruneBatch {
			return ErrInvalidTransaction
		}
	case OpRegisterSchema:
		if cmd.Schema == nil {
			return ErrInvalidSchema
		}
		normalized, err := normalizeSchemaDefinition(*cmd.Schema)
		if err != nil || normalized.Deprecated {
			return ErrInvalidSchema
		}
		if existing := cq.meta.Schemas[schemaMetadataKey(normalized.Name, normalized.Version)]; existing != nil && existing.Deprecated {
			return ErrSchemaDeprecated
		}
	case OpDeprecateSchema:
		if cmd.SchemaName == "" || cmd.SchemaVersion == "" {
			return ErrInvalidSchema
		}
		locator, err := normalizeSchemaDefinition(SchemaDefinition{Name: cmd.SchemaName, Version: cmd.SchemaVersion})
		if err != nil {
			return ErrInvalidSchema
		}
		if cq.meta.Schemas[schemaMetadataKey(locator.Name, locator.Version)] == nil {
			return ErrSchemaNotFound
		}
	default:
		return fmt.Errorf("cluster metadata: unknown operation %d", cmd.Op)
	}
	return nil
}

func validatePartitionAssignment(pid PartitionGroupID, replicas []string, leader string) error {
	kind := pid.Kind.normalized()
	if pid.Topic == "" || pid.Partition < 0 ||
		(kind != PartitionResourceTopic && kind != PartitionResourceStream && kind != PartitionResourceQueue) {
		return fmt.Errorf("%w: resource name must be non-empty, kind supported, and partition non-negative", ErrInvalidPartitionAssignment)
	}
	if len(replicas) == 0 || leader == "" {
		return fmt.Errorf("%w: at least one replica and a leader are required", ErrInvalidPartitionAssignment)
	}
	seen := make(map[string]struct{}, len(replicas))
	for _, replica := range replicas {
		if replica == "" {
			return fmt.Errorf("%w: empty replica ID", ErrInvalidPartitionAssignment)
		}
		if _, duplicate := seen[replica]; duplicate {
			return fmt.Errorf("%w: duplicate replica %q", ErrInvalidPartitionAssignment, replica)
		}
		seen[replica] = struct{}{}
	}
	if _, ok := seen[leader]; !ok {
		return fmt.Errorf("%w: leader %q is not in the replica set", ErrInvalidPartitionAssignment, leader)
	}
	return nil
}

func validateUniqueNodeAddress(nodes map[string]*NodeInfo, candidate *NodeInfo) error {
	if candidate == nil || candidate.Address == "" {
		return nil
	}
	for id, node := range nodes {
		if id != candidate.ID && node != nil && node.Address == candidate.Address {
			return fmt.Errorf("%w: controller address %q is already registered to logical node %q", ErrNodeInUse, candidate.Address, id)
		}
	}
	return nil
}

func validateRoutableReplicas(nodes map[string]*NodeInfo, replicas []string) error {
	for _, replica := range replicas {
		node := nodes[replica]
		if node == nil {
			return fmt.Errorf("%w: partition replica %q is not registered", ErrNodeNotFound, replica)
		}
		if node.ClientAddress == "" {
			return fmt.Errorf("%w: partition replica %q has no advertised client address", ErrInvalidNodeClientAddress, replica)
		}
	}
	return nil
}

func validateNodeClientAddressChange(nodes map[string]*NodeInfo, partitions map[PartitionGroupID]*PartitionMetadata, candidate *NodeInfo) error {
	if candidate == nil || candidate.ClientAddress != "" {
		return nil
	}
	current := nodes[candidate.ID]
	if current == nil || current.ClientAddress == "" {
		return nil
	}
	for _, partition := range partitions {
		if partitionReferencesNode(partition, candidate.ID) {
			return fmt.Errorf("%w: cannot clear client address for assigned replica %q", ErrNodeInUse, candidate.ID)
		}
	}
	return nil
}

func validateNodePlacementMetadata(node *NodeInfo) error {
	if node == nil || node.Metadata == nil {
		return nil
	}
	version := node.Metadata[NodeMetadataBrokerVersion]
	if version != "" {
		if err := validateBrokerVersion(version); err != nil {
			return err
		}
	}
	domain := node.Metadata[NodeMetadataFailureDomain]
	if domain != "" && (len(domain) > MaxNodeFailureDomainBytes || !utf8.ValidString(domain) || strings.IndexFunc(domain, unicode.IsControl) >= 0) {
		return fmt.Errorf("%w: value must be valid UTF-8, contain no control characters, and be at most %d bytes", ErrInvalidNodeFailureDomain, MaxNodeFailureDomainBytes)
	}
	if rawWeight := node.Metadata[NodeMetadataCapacityWeight]; rawWeight != "" {
		weight, err := strconv.ParseUint(rawWeight, 10, 32)
		if err != nil || weight < 1 || weight > MaxNodeCapacityWeight {
			return fmt.Errorf("%w: %s must be a decimal integer from 1 through %d", ErrInvalidNodeCapacityWeight, NodeMetadataCapacityWeight, MaxNodeCapacityWeight)
		}
	}
	return nil
}

func validateBrokerVersion(version string) error {
	if version == "" || len(version) > MaxNodeBrokerVersionBytes || !utf8.ValidString(version) || strings.TrimSpace(version) != version || strings.IndexFunc(version, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: value must be valid UTF-8, contain no control characters, have no surrounding whitespace, and be at most %d bytes", ErrInvalidNodeBrokerVersion, MaxNodeBrokerVersionBytes)
	}
	return nil
}

// NodeCapacityWeight returns the validated relative placement weight for a
// node, using one when the optional metadata key is absent.
func NodeCapacityWeight(node *NodeInfo) uint32 {
	if node == nil || node.Metadata == nil || node.Metadata[NodeMetadataCapacityWeight] == "" {
		return DefaultNodeCapacityWeight
	}
	weight, err := strconv.ParseUint(node.Metadata[NodeMetadataCapacityWeight], 10, 32)
	if err != nil || weight < 1 || weight > MaxNodeCapacityWeight {
		return DefaultNodeCapacityWeight
	}
	return uint32(weight)
}

func placementLoadLess(countA int, weightA uint32, countB int, weightB uint32) bool {
	wA, wB := int(weightA), int(weightB)
	qA, rA := countA/wA, countA%wA
	qB, rB := countB/wB, countB%wB
	if qA != qB {
		return qA < qB
	}
	return rA*wB < rB*wA
}

func validateNodeFailureDomainChange(nodes map[string]*NodeInfo, partitions map[PartitionGroupID]*PartitionMetadata, candidate *NodeInfo) error {
	if candidate == nil {
		return nil
	}
	current := nodes[candidate.ID]
	if current == nil || strings.TrimSpace(current.Metadata[NodeMetadataFailureDomain]) == strings.TrimSpace(candidate.Metadata[NodeMetadataFailureDomain]) {
		return nil
	}
	for _, partition := range partitions {
		if partitionReferencesNode(partition, candidate.ID) {
			return fmt.Errorf("%w: cannot change failure domain for assigned replica %q; reassign it first", ErrNodeInUse, candidate.ID)
		}
	}
	return nil
}

func validatePartitionNodeStates(nodes map[string]*NodeInfo, current *PartitionMetadata, replicas []string, leader string) error {
	currentReplicas := make(map[string]struct{})
	currentLeader := ""
	if current != nil {
		for _, replica := range current.Replicas {
			currentReplicas[replica] = struct{}{}
		}
		currentLeader = current.Leader
	}
	for _, replica := range replicas {
		node := nodes[replica]
		if node == nil {
			continue
		}
		if node.State == NodeLeft {
			return fmt.Errorf("%w: departed node %s", ErrNodeInUse, replica)
		}
		if node.State == NodeDraining || node.State == NodeFailed {
			if _, alreadyAssigned := currentReplicas[replica]; !alreadyAssigned {
				return fmt.Errorf("%w: cannot add %s node %s as a replica", ErrNodeUnavailable, node.State, replica)
			}
		}
	}
	return validatePartitionLeaderState(nodes, currentLeader, leader)
}

func validatePartitionLeaderState(nodes map[string]*NodeInfo, currentLeader, proposedLeader string) error {
	if proposedLeader == "" || proposedLeader == currentLeader {
		return nil
	}
	if node := nodes[proposedLeader]; node != nil && node.State != NodeActive {
		return fmt.Errorf("%w: node %s in state %s cannot become leader", ErrNodeUnavailable, proposedLeader, node.State)
	}
	return nil
}

func validateISR(isr, replicas []string) error {
	replicaSet := make(map[string]struct{}, len(replicas))
	for _, replica := range replicas {
		replicaSet[replica] = struct{}{}
	}
	seen := make(map[string]struct{}, len(isr))
	for _, node := range isr {
		if node == "" {
			return fmt.Errorf("%w: empty node ID", ErrInvalidISR)
		}
		if _, ok := replicaSet[node]; !ok {
			return fmt.Errorf("%w: %q is not assigned to the partition", ErrInvalidISR, node)
		}
		if _, duplicate := seen[node]; duplicate {
			return fmt.Errorf("%w: duplicate node %q", ErrInvalidISR, node)
		}
		seen[node] = struct{}{}
	}
	return nil
}

func containsReplica(replicas []string, id string) bool {
	for _, replica := range replicas {
		if replica == id {
			return true
		}
	}
	return false
}

func sameReplicaSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, replica := range a {
		set[replica] = struct{}{}
	}
	for _, replica := range b {
		if _, ok := set[replica]; !ok {
			return false
		}
	}
	return true
}

func cloneNodeInfo(node *NodeInfo) *NodeInfo {
	if node == nil {
		return nil
	}
	copyNode := *node
	if node.Metadata != nil {
		copyNode.Metadata = make(map[string]string, len(node.Metadata))
		for key, value := range node.Metadata {
			copyNode.Metadata[key] = value
		}
	}
	return &copyNode
}

func validateClientAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return nil
	}
	if len(address) > 2048 {
		return fmt.Errorf("%w: URL exceeds 2048 bytes", ErrInvalidNodeClientAddress)
	}
	parsed, err := url.ParseRequestURI(address)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w %q: expected an http(s) URL without credentials, query, or fragment", ErrInvalidNodeClientAddress, address)
	}
	return nil
}
