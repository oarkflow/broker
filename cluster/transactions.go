package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"
)

const (
	TransactionCommit                = "commit"
	TransactionAbort                 = "abort"
	MaxTransactionParticipants       = 1024
	MaxTransactionIDBytes            = 256
	MaxTransactionParticipantIDBytes = 512
	DefaultTransactionPageSize       = 100
	MaxTransactionPageSize           = 1000
)

type transactionIDNode struct {
	id          string
	height      int
	left, right *transactionIDNode
}

func transactionIDHeight(node *transactionIDNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func updateTransactionIDHeight(node *transactionIDNode) {
	left, right := transactionIDHeight(node.left), transactionIDHeight(node.right)
	if left > right {
		node.height = left + 1
	} else {
		node.height = right + 1
	}
}

func rotateTransactionIDRight(root *transactionIDNode) *transactionIDNode {
	pivot := root.left
	root.left = pivot.right
	pivot.right = root
	updateTransactionIDHeight(root)
	updateTransactionIDHeight(pivot)
	return pivot
}

func rotateTransactionIDLeft(root *transactionIDNode) *transactionIDNode {
	pivot := root.right
	root.right = pivot.left
	pivot.left = root
	updateTransactionIDHeight(root)
	updateTransactionIDHeight(pivot)
	return pivot
}

func balanceTransactionIDNode(root *transactionIDNode) *transactionIDNode {
	if root == nil {
		return nil
	}
	updateTransactionIDHeight(root)
	balance := transactionIDHeight(root.left) - transactionIDHeight(root.right)
	if balance > 1 {
		if transactionIDHeight(root.left.left) < transactionIDHeight(root.left.right) {
			root.left = rotateTransactionIDLeft(root.left)
		}
		return rotateTransactionIDRight(root)
	}
	if balance < -1 {
		if transactionIDHeight(root.right.right) < transactionIDHeight(root.right.left) {
			root.right = rotateTransactionIDRight(root.right)
		}
		return rotateTransactionIDLeft(root)
	}
	return root
}

func insertTransactionID(root *transactionIDNode, id string) *transactionIDNode {
	if root == nil {
		return &transactionIDNode{id: id, height: 1}
	}
	if id < root.id {
		root.left = insertTransactionID(root.left, id)
	} else if id > root.id {
		root.right = insertTransactionID(root.right, id)
	} else {
		return root
	}
	return balanceTransactionIDNode(root)
}

func deleteTransactionID(root *transactionIDNode, id string) *transactionIDNode {
	if root == nil {
		return nil
	}
	if id < root.id {
		root.left = deleteTransactionID(root.left, id)
	} else if id > root.id {
		root.right = deleteTransactionID(root.right, id)
	} else {
		if root.left == nil {
			return root.right
		}
		if root.right == nil {
			return root.left
		}
		successor := root.right
		for successor.left != nil {
			successor = successor.left
		}
		root.id = successor.id
		root.right = deleteTransactionID(root.right, successor.id)
	}
	return balanceTransactionIDNode(root)
}

func transactionIDsAfter(root *transactionIDNode, after string, limit int) []string {
	ids := make([]string, 0, limit)
	var walk func(*transactionIDNode)
	walk = func(node *transactionIDNode) {
		if node == nil || len(ids) == limit {
			return
		}
		if node.id > after {
			walk(node.left)
			if len(ids) < limit {
				ids = append(ids, node.id)
			}
			walk(node.right)
			return
		}
		walk(node.right)
	}
	walk(root)
	return ids
}

var (
	ErrTransactionNotFound           = errors.New("cluster: transaction not found")
	ErrTransactionConflict           = errors.New("cluster: transaction ID conflicts with existing transaction")
	ErrTransactionDecided            = errors.New("cluster: transaction already has a decision")
	ErrTransactionNotPrepared        = errors.New("cluster: transaction participants are not all prepared")
	ErrTransactionUndecided          = errors.New("cluster: transaction has no durable decision")
	ErrUnknownTransactionParticipant = errors.New("cluster: unknown transaction participant")
	ErrInvalidTransactionDecision    = errors.New("cluster: invalid transaction decision")
	ErrInvalidTransaction            = errors.New("cluster: invalid transaction")
	ErrTransactionExpired            = errors.New("cluster: transaction deadline expired")
	ErrTransactionAborted            = errors.New("cluster: transaction aborted")
)

// CoordinatorTransaction is the durable coordinator record replicated through
// the controller quorum. Participants must make prepare, commit, and abort
// operations idempotent because recovery can repeat any operation.
type CoordinatorTransaction struct {
	ID           string    `json:"id"`
	Fingerprint  string    `json:"fingerprint,omitempty"`
	Participants []string  `json:"participants"`
	Prepared     []string  `json:"prepared,omitempty"`
	Decision     string    `json:"decision,omitempty"`
	Resolved     []string  `json:"resolved,omitempty"`
	Deadline     time.Time `json:"deadline"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// TransactionParticipantResolver applies a durable coordinator decision to a
// participant. Implementations must be idempotent across retries and restarts.
type TransactionParticipantResolver interface {
	CommitTransaction(context.Context, string, string) error
	AbortTransaction(context.Context, string, string) error
}

// TransactionParticipant durably stages participant work during Prepare. The
// implementation must identify staged work by transactionID and make all
// three operations safe to retry after an uncertain result or process restart.
type TransactionParticipant interface {
	TransactionParticipantResolver
	PrepareTransaction(context.Context, string, time.Time) error
}

// TransactionParticipantForgetter releases durable participant outcome markers
// after the coordinator has recorded every participant in a transaction as
// resolved. Implementors should make ForgetTransaction idempotent; recovery
// retries cleanup for transactions whose participant resolutions were already
// committed by the coordinator.
type TransactionParticipantForgetter interface {
	ForgetTransaction(context.Context, string, string) error
}

func validateTransactionBegin(id string, participants []string, deadline time.Time) error {
	if id == "" || len(id) > MaxTransactionIDBytes || !utf8.ValidString(id) || deadline.IsZero() ||
		len(participants) == 0 || len(participants) > MaxTransactionParticipants {
		return ErrInvalidTransaction
	}
	previous := ""
	for _, participant := range participants {
		if participant == "" || len(participant) > MaxTransactionParticipantIDBytes ||
			!utf8.ValidString(participant) || participant <= previous {
			return ErrInvalidTransaction
		}
		previous = participant
	}
	return nil
}

func validateTransactionFingerprint(fingerprint string) error {
	if len(fingerprint) > 256 || !utf8.ValidString(fingerprint) {
		return ErrInvalidTransaction
	}
	return nil
}

func canonicalParticipants(participants []string) ([]string, error) {
	if len(participants) == 0 || len(participants) > MaxTransactionParticipants {
		return nil, ErrInvalidTransaction
	}
	out := append([]string(nil), participants...)
	sort.Strings(out)
	for i, participant := range out {
		if participant == "" || len(participant) > MaxTransactionParticipantIDBytes ||
			!utf8.ValidString(participant) || (i > 0 && participant == out[i-1]) {
			return nil, ErrInvalidTransaction
		}
	}
	return out, nil
}

func containsString(values []string, value string) bool {
	i := sort.SearchStrings(values, value)
	return i < len(values) && values[i] == value
}

func appendUniqueSorted(values []string, value string) []string {
	if containsString(values, value) {
		return values
	}
	out := append(append([]string(nil), values...), value)
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func validateClusterMetadataSnapshot(meta *ClusterMetadata) error {
	addresses := make(map[string]string, len(meta.Nodes))
	for id, node := range meta.Nodes {
		if node == nil || id == "" || node.ID != id || !validNodeState(node.State) {
			return fmt.Errorf("invalid node record %q", id)
		}
		if err := validateNodePlacementMetadata(node); err != nil {
			return fmt.Errorf("invalid node record %q: %w", id, err)
		}
		if node.Address != "" {
			if previous, exists := addresses[node.Address]; exists {
				return fmt.Errorf("controller address %q is registered to both logical nodes %q and %q", node.Address, previous, id)
			}
			addresses[node.Address] = id
		}
		if err := validateClientAddress(node.ClientAddress); err != nil {
			return fmt.Errorf("invalid node record %q: %w", id, err)
		}
	}
	for id, partition := range meta.Partitions {
		if partition == nil || partition.ID.Canonical() != id.Canonical() || partition.LeaderEpoch == 0 {
			return fmt.Errorf("invalid partition record %s", id)
		}
		if err := validatePartitionAssignment(partition.ID, partition.Replicas, partition.Leader); err != nil {
			return err
		}
		if err := validateISR(partition.ISR, partition.Replicas); err != nil {
			return err
		}
		if err := validatePartitionNodeStates(meta.Nodes, partition, partition.Replicas, partition.Leader); err != nil {
			return fmt.Errorf("partition %s: %w", id, err)
		}
		if partition.Routable {
			if err := validateRoutableReplicas(meta.Nodes, partition.Replicas); err != nil {
				return fmt.Errorf("partition %s: %w", id, err)
			}
		}
		if pending := partition.Reassignment; pending != nil {
			if pending.StartedAt.IsZero() || pending.ExpectedEpoch == 0 || pending.ExpectedLeader == "" || pending.DesiredLeader == "" {
				return fmt.Errorf("partition %s has incomplete reassignment metadata", id)
			}
			if err := validatePartitionAssignment(partition.ID, pending.ExpectedReplicas, pending.ExpectedLeader); err != nil {
				return fmt.Errorf("partition %s has invalid reassignment source: %w", id, err)
			}
			if err := validatePartitionAssignment(partition.ID, pending.DesiredReplicas, pending.DesiredLeader); err != nil {
				return fmt.Errorf("partition %s has invalid reassignment target: %w", id, err)
			}
			switch pending.Phase {
			case PartitionReassignmentCopying, PartitionReassignmentAborting:
				if partition.Leader != pending.ExpectedLeader || partition.LeaderEpoch != pending.ExpectedEpoch || !sameReplicaSet(partition.Replicas, pending.ExpectedReplicas) {
					return fmt.Errorf("partition %s reassignment copy phase does not match the active assignment", id)
				}
			case PartitionReassignmentCleanup:
				epochInvalid := partition.LeaderEpoch < pending.ExpectedEpoch || pending.DesiredLeader != pending.ExpectedLeader && partition.LeaderEpoch == pending.ExpectedEpoch
				if partition.Leader != pending.DesiredLeader || epochInvalid || !sameReplicaSet(partition.Replicas, pending.DesiredReplicas) {
					return fmt.Errorf("partition %s reassignment cleanup phase does not match the active assignment", id)
				}
			default:
				return fmt.Errorf("partition %s has invalid reassignment phase %q", id, pending.Phase)
			}
			if pending.Phase == PartitionReassignmentCopying {
				for _, replica := range pending.DesiredReplicas {
					if !containsReplica(pending.ExpectedReplicas, replica) {
						node := meta.Nodes[replica]
						if node == nil || node.State != NodeActive {
							return fmt.Errorf("partition %s reassignment target %q is not active", id, replica)
						}
					}
				}
			}
			if partition.Routable {
				if err := validateRoutableReplicas(meta.Nodes, pending.DesiredReplicas); err != nil {
					return fmt.Errorf("partition %s reassignment target: %w", id, err)
				}
			}
		}
	}
	for id, tx := range meta.Transactions {
		if tx == nil || id == "" || tx.ID != id {
			return fmt.Errorf("invalid transaction record %q", id)
		}
		if err := validateTransactionBegin(tx.ID, tx.Participants, tx.Deadline); err != nil {
			return fmt.Errorf("transaction %s: %w", id, err)
		}
		if err := validateTransactionFingerprint(tx.Fingerprint); err != nil {
			return fmt.Errorf("transaction %s fingerprint: %w", id, err)
		}
		if tx.CreatedAt.IsZero() || tx.UpdatedAt.IsZero() || !tx.CreatedAt.Before(tx.Deadline) {
			return fmt.Errorf("transaction %s has invalid timestamps", id)
		}
		if err := validateSortedSubset(tx.Prepared, tx.Participants); err != nil {
			return fmt.Errorf("transaction %s prepared set: %w", id, err)
		}
		if err := validateSortedSubset(tx.Resolved, tx.Participants); err != nil {
			return fmt.Errorf("transaction %s resolved set: %w", id, err)
		}
		switch tx.Decision {
		case "":
			if len(tx.Resolved) != 0 {
				return fmt.Errorf("transaction %s has resolutions without a decision", id)
			}
		case TransactionAbort:
		case TransactionCommit:
			if !sameStrings(tx.Prepared, tx.Participants) {
				return fmt.Errorf("transaction %s committed before every participant prepared", id)
			}
		default:
			return fmt.Errorf("transaction %s has invalid decision %q", id, tx.Decision)
		}
	}
	for key, schema := range meta.Schemas {
		if schema == nil {
			return fmt.Errorf("invalid schema record %q", key)
		}
		normalized, err := normalizeSchemaDefinition(*schema)
		if err != nil || normalized.Name != schema.Name || normalized.Version != schema.Version ||
			key != schemaMetadataKey(schema.Name, schema.Version) || schema.RegisteredAt.IsZero() {
			return fmt.Errorf("invalid schema record %q", key)
		}
	}
	return nil
}

func validateSortedSubset(values, allowed []string) error {
	previous := ""
	for _, value := range values {
		if value == "" || value <= previous || !containsString(allowed, value) {
			return ErrInvalidTransaction
		}
		previous = value
	}
	return nil
}

// BeginTransaction records a stable participant set and expiry in the quorum.
// Repeating the same begin request is idempotent; reusing the ID for different
// participants or a different deadline returns ErrTransactionConflict.
func (cq *ControllerQuorum) BeginTransaction(ctx context.Context, id string, participants []string, deadline time.Time) error {
	return cq.BeginTransactionWithFingerprint(ctx, id, participants, deadline, "")
}

// BeginTransactionWithFingerprint binds an optional caller-computed request
// identity to the durable participant set. Reusing an ID for different input
// is rejected before participants are prepared.
func (cq *ControllerQuorum) BeginTransactionWithFingerprint(ctx context.Context, id string, participants []string, deadline time.Time, fingerprint string) error {
	canonical, err := canonicalParticipants(participants)
	if err != nil || id == "" || deadline.IsZero() || validateTransactionFingerprint(fingerprint) != nil {
		return ErrInvalidTransaction
	}
	return cq.propose(ctx, MetadataCommand{
		Op: OpBeginTransaction, TransactionID: id, Participants: canonical, Deadline: deadline.UTC(), TransactionFingerprint: fingerprint,
	})
}

// PrepareTransactionParticipant records one participant's durable vote.
func (cq *ControllerQuorum) PrepareTransactionParticipant(ctx context.Context, id, participant string) error {
	return cq.propose(ctx, MetadataCommand{Op: OpPrepareTransactionParticipant, TransactionID: id, Participant: participant})
}

// DecideTransaction commits only after every participant is prepared. Abort
// can be recorded before all votes arrive. A durable decision cannot change.
func (cq *ControllerQuorum) DecideTransaction(ctx context.Context, id string, commit bool) error {
	decision := TransactionAbort
	if commit {
		decision = TransactionCommit
	}
	return cq.propose(ctx, MetadataCommand{Op: OpDecideTransaction, TransactionID: id, Decision: decision})
}

// ResolveTransactionParticipant marks a participant complete after its
// idempotent commit or abort handler has succeeded.
func (cq *ControllerQuorum) ResolveTransactionParticipant(ctx context.Context, id, participant string) error {
	return cq.propose(ctx, MetadataCommand{Op: OpResolveTransactionParticipant, TransactionID: id, Participant: participant})
}

// PruneResolvedTransactions removes at most limit completed records whose
// final update is older than before. The deletion is replicated, deterministic,
// and never removes undecided or unresolved work.
func (cq *ControllerQuorum) PruneResolvedTransactions(ctx context.Context, before time.Time, limit int) (int, error) {
	if before.IsZero() {
		return 0, ErrInvalidTransaction
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > MaxTransactionPruneBatch {
		limit = MaxTransactionPruneBatch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cq.meta.proposalMu.Lock()
	defer cq.meta.proposalMu.Unlock()
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return 0, err
	}
	cq.meta.mu.RLock()
	priorCount := len(cq.meta.Transactions)
	cq.meta.mu.RUnlock()
	if err := cq.proposeLocked(ctx, MetadataCommand{
		Op: OpPruneResolvedTransactions, PruneBefore: before.UTC(), Limit: limit,
	}); err != nil {
		return 0, err
	}
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	return priorCount - len(cq.meta.Transactions), nil
}

// GetTransaction returns a defensive copy of the replicated transaction.
func (cq *ControllerQuorum) GetTransaction(id string) (*CoordinatorTransaction, error) {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	tx, ok := cq.meta.Transactions[id]
	if !ok {
		return nil, ErrTransactionNotFound
	}
	return cloneCoordinatorTransaction(tx), nil
}

// GetTransactionLinearizable reads a transaction after a quorum-confirmed
// controller read point has been applied locally.
func (cq *ControllerQuorum) GetTransactionLinearizable(ctx context.Context, id string) (*CoordinatorTransaction, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.GetTransaction(id)
}

// ListTransactions returns coordinator records in stable ID order.
func (cq *ControllerQuorum) ListTransactions() []*CoordinatorTransaction {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	out := make([]*CoordinatorTransaction, 0, len(cq.meta.Transactions))
	for _, tx := range cq.meta.Transactions {
		out = append(out, cloneCoordinatorTransaction(tx))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListTransactionsPage returns a stable page ordered by transaction ID. The
// cursor is exclusive and can be passed back as after on the next request.
func (cq *ControllerQuorum) ListTransactionsPage(after string, limit int) ([]*CoordinatorTransaction, string, error) {
	if len(after) > MaxTransactionIDBytes || !utf8.ValidString(after) {
		return nil, "", ErrInvalidTransaction
	}
	if limit <= 0 {
		limit = DefaultTransactionPageSize
	}
	if limit > MaxTransactionPageSize {
		limit = MaxTransactionPageSize
	}
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	pageIDs := transactionIDsAfter(cq.meta.transactionIDRoot, after, limit+1)
	next := ""
	if len(pageIDs) > limit {
		pageIDs = pageIDs[:limit]
		next = pageIDs[len(pageIDs)-1]
	}
	out := make([]*CoordinatorTransaction, 0, len(pageIDs))
	for _, id := range pageIDs {
		out = append(out, cloneCoordinatorTransaction(cq.meta.Transactions[id]))
	}
	return out, next, nil
}

// ListTransactionsPageLinearizable returns one bounded transaction page after
// a quorum-confirmed read point has been applied locally.
func (cq *ControllerQuorum) ListTransactionsPageLinearizable(ctx context.Context, after string, limit int) ([]*CoordinatorTransaction, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, "", err
	}
	return cq.ListTransactionsPage(after, limit)
}

// ListTransactionsLinearizable returns a quorum-confirmed transaction view.
func (cq *ControllerQuorum) ListTransactionsLinearizable(ctx context.Context) ([]*CoordinatorTransaction, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.ListTransactions(), nil
}

// Stats reports counts for controller metadata and transaction recovery.
func (cq *ControllerQuorum) Stats() ControllerQuorumStats {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	stats := ControllerQuorumStats{
		Nodes:         len(cq.meta.Nodes),
		Partitions:    len(cq.meta.Partitions),
		Transactions:  len(cq.meta.Transactions),
		Schemas:       len(cq.meta.Schemas),
		ACLPrincipals: len(cq.meta.ACLPrincipals),
	}
	for _, tx := range cq.meta.Transactions {
		if tx.Decision == "" {
			stats.UndecidedTransactions++
		} else if len(tx.Resolved) < len(tx.Participants) {
			stats.TransactionsPendingResolution++
		}
	}
	return stats
}

// StatsLinearizable returns coordinator counts after a quorum-confirmed read
// point has been applied locally.
func (cq *ControllerQuorum) StatsLinearizable(ctx context.Context) (ControllerQuorumStats, error) {
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return ControllerQuorumStats{}, err
	}
	return cq.Stats(), nil
}

func cloneCoordinatorTransaction(tx *CoordinatorTransaction) *CoordinatorTransaction {
	copyTx := *tx
	copyTx.Participants = append([]string(nil), tx.Participants...)
	copyTx.Prepared = append([]string(nil), tx.Prepared...)
	copyTx.Resolved = append([]string(nil), tx.Resolved...)
	return &copyTx
}

type transactionParticipantSet map[string]TransactionParticipant

func (p transactionParticipantSet) CommitTransaction(ctx context.Context, txID, participantID string) error {
	participant, ok := p[participantID]
	if !ok {
		return fmt.Errorf("transaction participant %s is unavailable", participantID)
	}
	return participant.CommitTransaction(ctx, txID, participantID)
}

func (p transactionParticipantSet) AbortTransaction(ctx context.Context, txID, participantID string) error {
	participant, ok := p[participantID]
	if !ok {
		return fmt.Errorf("transaction participant %s is unavailable", participantID)
	}
	return participant.AbortTransaction(ctx, txID, participantID)
}

func (p transactionParticipantSet) ForgetTransaction(ctx context.Context, txID, participantID string) error {
	participant, ok := p[participantID]
	if !ok {
		return fmt.Errorf("transaction participant %s is unavailable", participantID)
	}
	forgetter, ok := participant.(TransactionParticipantForgetter)
	if !ok {
		return nil
	}
	return forgetter.ForgetTransaction(ctx, txID, participantID)
}

// ExecuteTransaction coordinates a fixed participant set through prepare,
// durable decision, and recovery. Participant implementations own durable
// staging; this method records their votes and drives idempotent resolution.
// Participants prepare in sorted ID order and commits resolve in reverse order,
// allowing IDs to encode dependencies between effects.
func (cq *ControllerQuorum) ExecuteTransaction(ctx context.Context, id string, participants map[string]TransactionParticipant, deadline time.Time) error {
	return cq.ExecuteTransactionWithFingerprint(ctx, id, participants, deadline, "")
}

// ExecuteTransactionWithFingerprint coordinates a fixed participant set and
// binds retries to the same request payload identity.
func (cq *ControllerQuorum) ExecuteTransactionWithFingerprint(ctx context.Context, id string, participants map[string]TransactionParticipant, deadline time.Time, fingerprint string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateTransactionFingerprint(fingerprint); err != nil {
		return err
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return err
	}
	participantIDs := make([]string, 0, len(participants))
	for id, participant := range participants {
		if participant == nil {
			return ErrInvalidTransaction
		}
		participantIDs = append(participantIDs, id)
	}
	participantIDs, err := canonicalParticipants(participantIDs)
	if err != nil {
		return err
	}
	tx, err := cq.GetTransaction(id)
	if errors.Is(err, ErrTransactionNotFound) {
		if err := cq.BeginTransactionWithFingerprint(ctx, id, participantIDs, deadline, fingerprint); err != nil {
			return err
		}
		tx, err = cq.GetTransaction(id)
	}
	if err != nil {
		return err
	}
	if !sameStrings(tx.Participants, participantIDs) || !tx.Deadline.Equal(deadline.UTC()) || tx.Fingerprint != fingerprint {
		return ErrTransactionConflict
	}
	resolver := transactionParticipantSet(participants)
	if tx.Decision == TransactionCommit {
		return cq.RecoverTransaction(ctx, id, resolver)
	}
	if tx.Decision == TransactionAbort {
		return errors.Join(ErrTransactionAborted, cq.RecoverTransaction(ctx, id, resolver))
	}
	if !time.Now().Before(tx.Deadline) {
		return errors.Join(ErrTransactionExpired, cq.abortAndRecoverTransaction(ctx, id, resolver))
	}
	for _, participantID := range participantIDs {
		alreadyPrepared := containsString(tx.Prepared, participantID)
		if err := ctx.Err(); err != nil {
			return errors.Join(err, cq.abortAndRecoverTransaction(ctx, id, resolver))
		}
		participant := participants[participantID]
		prepareCtx, cancel := context.WithDeadline(ctx, tx.Deadline)
		prepareErr := participant.PrepareTransaction(prepareCtx, id, tx.Deadline)
		cancel()
		if prepareErr != nil {
			prepareErr := fmt.Errorf("prepare transaction %s participant %s: %w", id, participantID, prepareErr)
			return errors.Join(prepareErr, cq.abortAndRecoverTransaction(ctx, id, resolver))
		}
		if alreadyPrepared {
			// Replaying prepare lets the participant verify that its reconstructed
			// payload still matches the durable staged work behind this vote.
			continue
		}
		if err := cq.PrepareTransactionParticipant(ctx, id, participantID); err != nil {
			return errors.Join(err, cq.abortAndRecoverTransaction(ctx, id, resolver))
		}
	}
	if err := cq.DecideTransaction(ctx, id, true); err != nil {
		if ctx.Err() != nil {
			return errors.Join(err, cq.abortAndRecoverTransaction(ctx, id, resolver))
		}
		// A competing executor may have durably chosen abort while this caller
		// was preparing. Resolve the actual replicated decision instead of
		// leaving staged participant work for the deadline recovery sweep.
		current, readErr := cq.GetTransactionLinearizable(ctx, id)
		if readErr == nil {
			switch current.Decision {
			case TransactionCommit:
				return cq.RecoverTransaction(ctx, id, resolver)
			case TransactionAbort:
				return errors.Join(ErrTransactionAborted, cq.RecoverTransaction(ctx, id, resolver))
			}
		}
		if errors.Is(err, ErrTransactionExpired) {
			return errors.Join(err, cq.abortAndRecoverTransaction(ctx, id, resolver))
		}
		return errors.Join(err, readErr)
	}
	return cq.RecoverTransaction(ctx, id, resolver)
}

// abortAndRecoverTransaction gives cleanup its own bounded context. Caller
// cancellation must not strand participants that have already prepared work.
func (cq *ControllerQuorum) abortAndRecoverTransaction(ctx context.Context, id string, resolver TransactionParticipantResolver) error {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	timeout := cq.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cleanupCtx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	decideErr := cq.DecideTransaction(cleanupCtx, id, false)
	recoverErr := cq.RecoverTransaction(cleanupCtx, id, resolver)
	return errors.Join(decideErr, recoverErr)
}

// RecoverTransaction replays the durable decision for one transaction. It is
// useful to ExecuteTransaction, whose resolver may only know that transaction's
// participants.
func (cq *ControllerQuorum) RecoverTransaction(ctx context.Context, id string, resolver TransactionParticipantResolver) error {
	if resolver == nil {
		return errors.New("transaction recovery requires a participant resolver")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !cq.IsLeader() {
		return ErrNotLeader
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return err
	}
	tx, err := cq.GetTransaction(id)
	if err != nil {
		return err
	}
	return cq.recoverTransaction(ctx, tx, resolver)
}

// RecoverTransactions aborts expired undecided transactions and replays
// committed/aborted participant decisions. Resolver calls are repeated after
// crashes until their success is recorded in the controller quorum.
func (cq *ControllerQuorum) RecoverTransactions(ctx context.Context, resolver TransactionParticipantResolver) error {
	if resolver == nil {
		return errors.New("transaction recovery requires a participant resolver")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !cq.IsLeader() {
		return ErrNotLeader
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return err
	}
	var recoveryErr error
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(recoveryErr, err)
		}
		page, next, err := cq.ListTransactionsPage(after, DefaultTransactionPageSize)
		if err != nil {
			return errors.Join(recoveryErr, err)
		}
		for _, tx := range page {
			if err := ctx.Err(); err != nil {
				return errors.Join(recoveryErr, err)
			}
			if err := cq.recoverTransaction(ctx, tx, resolver); err != nil {
				recoveryErr = errors.Join(recoveryErr, err)
			}
		}
		if next == "" {
			break
		}
		after = next
	}
	return recoveryErr
}

// RunTransactionRecovery repeatedly sweeps the durable coordinator backlog
// while this controller is leader. It runs a sweep immediately, then retries
// at interval so leadership changes and transient participant failures are
// recovered without an application-owned scheduler. Errors from a sweep are
// reported to onError and do not stop later retries. Context cancellation
// stops the runner and returns ctx.Err().
func (cq *ControllerQuorum) RunTransactionRecovery(ctx context.Context, interval time.Duration, resolver TransactionParticipantResolver, onError func(error)) error {
	if cq == nil || resolver == nil || onError == nil {
		return errors.New("transaction recovery requires a controller quorum, participant resolver, and error callback")
	}
	if interval <= 0 {
		return errors.New("transaction recovery interval must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cq.IsLeader() {
			if err := cq.RecoverTransactions(ctx, resolver); err != nil && ctx.Err() == nil {
				onError(err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (cq *ControllerQuorum) recoverTransaction(ctx context.Context, tx *CoordinatorTransaction, resolver TransactionParticipantResolver) error {
	if tx.Decision == "" {
		if time.Now().Before(tx.Deadline) {
			return nil
		}
		if err := cq.DecideTransaction(ctx, tx.ID, false); err != nil {
			return fmt.Errorf("abort expired transaction %s: %w", tx.ID, err)
		}
		tx.Decision = TransactionAbort
	}
	var recoveryErr error
	participants := append([]string(nil), tx.Participants...)
	if tx.Decision == TransactionCommit {
		// Commit in reverse prepare order. Participants can assign lexical order
		// to establish dependencies, such as applying a source checkpoint only
		// after every output partition has committed.
		for left, right := 0, len(participants)-1; left < right; left, right = left+1, right-1 {
			participants[left], participants[right] = participants[right], participants[left]
		}
	}
	resolved := make(map[string]bool, len(tx.Resolved)+len(participants))
	for _, participant := range tx.Resolved {
		resolved[participant] = true
	}
	for _, participant := range participants {
		if err := ctx.Err(); err != nil {
			return errors.Join(recoveryErr, err)
		}
		if resolved[participant] {
			continue
		}
		var err error
		if tx.Decision == TransactionCommit {
			err = resolver.CommitTransaction(ctx, tx.ID, participant)
		} else {
			err = resolver.AbortTransaction(ctx, tx.ID, participant)
		}
		if err != nil {
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("recover transaction %s participant %s: %w", tx.ID, participant, err))
			if tx.Decision == TransactionCommit {
				// Do not apply dependent commit effects after an earlier participant
				// failed. Recovery retries from the first unresolved participant.
				break
			}
			continue
		}
		if err := cq.ResolveTransactionParticipant(ctx, tx.ID, participant); err != nil {
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("record transaction %s participant %s resolution: %w", tx.ID, participant, err))
			if tx.Decision == TransactionCommit {
				break
			}
			continue
		}
		resolved[participant] = true
	}
	allResolved := len(resolved) >= len(participants)
	if allResolved {
		for _, participant := range participants {
			if !resolved[participant] {
				allResolved = false
				break
			}
		}
	}
	if allResolved {
		if forgetter, ok := resolver.(TransactionParticipantForgetter); ok {
			for _, participant := range participants {
				if err := ctx.Err(); err != nil {
					return errors.Join(recoveryErr, err)
				}
				if err := forgetter.ForgetTransaction(ctx, tx.ID, participant); err != nil {
					recoveryErr = errors.Join(recoveryErr, fmt.Errorf("forget transaction %s participant %s: %w", tx.ID, participant, err))
				}
			}
		}
	}
	return recoveryErr
}
