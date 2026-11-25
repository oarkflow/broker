package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/oarkflow/broker/cluster"
)

type partitionAppendRef struct {
	index int
	entry AppendEntry
}

type distributedOffsetAppendPlan struct {
	multiPartition bool
	groups         map[cluster.PartitionGroupID][]partitionAppendRef
	groupIDs       []cluster.PartitionGroupID
}

func (db *DistributedBroker) planOffsetAppendTransaction(ctx context.Context, commit OffsetCommit, entries []AppendEntry) (distributedOffsetAppendPlan, error) {
	plan := distributedOffsetAppendPlan{groups: make(map[cluster.PartitionGroupID][]partitionAppendRef)}
	if !commit.Partitioned {
		return plan, nil
	}
	sourceID := (cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: commit.Stream, Partition: commit.Partition}).Canonical()
	type target struct {
		id       cluster.PartitionGroupID
		assigned bool
		source   bool
	}
	allTargets := make([][]target, len(entries))
	for i, entry := range entries {
		destinations := entry.Destinations
		if len(destinations) == 0 {
			switch {
			case entry.Message.Queue != "":
				destinations = []Destination{{Kind: DestinationQueue, Name: entry.Message.Queue}}
			case entry.Message.Stream != "":
				destinations = []Destination{{Kind: DestinationStream, Name: entry.Message.Stream}}
			case entry.Message.Topic != "":
				destinations = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
			}
		}
		for _, destination := range destinations {
			var kind cluster.PartitionResourceKind
			switch destination.Kind {
			case DestinationTopic:
				kind = cluster.PartitionResourceTopic
			case DestinationStream:
				kind = cluster.PartitionResourceStream
			case DestinationQueue:
				kind = cluster.PartitionResourceQueue
			case DestinationArchive, DestinationDLQ:
				return distributedOffsetAppendPlan{}, fmt.Errorf("%w: offset-plus-append transactions do not support archive or dead-letter destinations", ErrUnsupportedMode)
			default:
				continue
			}
			if destination.Name == "" {
				return distributedOffsetAppendPlan{}, errf(ErrInvalidMessage, "transaction destination name required")
			}
			assigned, err := db.hasResourcePartitions(ctx, kind, destination.Name)
			if err != nil {
				return distributedOffsetAppendPlan{}, err
			}
			if assigned && kind == cluster.PartitionResourceQueue {
				configLookup, ok := db.Broker.store.(QueueConfigLookup)
				if !ok {
					return distributedOffsetAppendPlan{}, fmt.Errorf("%w: distributed queue transactions require queue configuration lookup", ErrUnsupportedMode)
				}
				queueConfig, found, err := configLookup.LookupQueueConfig(ctx, destination.Name)
				if err != nil {
					return distributedOffsetAppendPlan{}, err
				}
				if found && queueConfig.MaxDepth > 0 {
					return distributedOffsetAppendPlan{}, fmt.Errorf("%w: multi-partition transactions cannot append to bounded queue %q", ErrUnsupportedMode, destination.Name)
				}
			}
			targetID := (cluster.PartitionGroupID{Kind: kind, Topic: destination.Name, Partition: entry.Message.Partition}).Canonical()
			allTargets[i] = append(allTargets[i], target{id: targetID, assigned: assigned, source: targetID == sourceID})
			if assigned && targetID != sourceID {
				plan.multiPartition = true
			}
		}
	}
	if !plan.multiPartition {
		return plan, nil
	}
	for i, targets := range allTargets {
		if len(targets) != 1 || !targets[0].assigned || targets[0].source {
			return distributedOffsetAppendPlan{}, fmt.Errorf("%w: multi-partition offset-plus-append requires one assigned output partition per entry, distinct from the source checkpoint partition", ErrUnsupportedMode)
		}
		if targets[0].id.Partition < 0 {
			return distributedOffsetAppendPlan{}, errf(ErrInvalidMessage, "output partition must be non-negative")
		}
		plan.groups[targets[0].id] = append(plan.groups[targets[0].id], partitionAppendRef{index: i, entry: entries[i]})
	}
	plan.groupIDs = make([]cluster.PartitionGroupID, 0, len(plan.groups))
	for id := range plan.groups {
		plan.groupIDs = append(plan.groupIDs, id)
	}
	sort.Slice(plan.groupIDs, func(i, j int) bool {
		return PartitionTransactionParticipantID(plan.groupIDs[i]) < PartitionTransactionParticipantID(plan.groupIDs[j])
	})
	return plan, nil
}

func distributedOffsetAppendCoordinatorID(commit OffsetCommit) string {
	identity := commit.TenantID + "\x00" + commit.Stream + "\x00" + commit.ConsumerGroup + "\x00" + commit.TransactionID
	digest := sha256.Sum256([]byte(identity))
	return "broker-offset-append:" + hex.EncodeToString(digest[:])
}

func deterministicTransactionMessageID(coordinatorID, fingerprint string, createdAt time.Time, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", coordinatorID, fingerprint, createdAt.UTC().Format(time.RFC3339Nano), index)))
	return "tx-" + hex.EncodeToString(digest[:])
}

func (b *Broker) commitDistributedOffsetAppend(ctx context.Context, commit OffsetCommit, entries []AppendEntry, plan distributedOffsetAppendPlan) ([]Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db := b.distributedPartitions
	if db == nil || db.controllerQuorum == nil || !commit.Partitioned || commit.TransactionID == "" || len(entries) == 0 {
		return nil, fmt.Errorf("%w: distributed offset-plus-append requires an assigned source partition and transaction ID", ErrUnsupportedMode)
	}
	commit, entries, fingerprint, err := prepareOffsetAppendRequest(ctx, commit, entries)
	if err != nil {
		return nil, err
	}
	if cached, found, err := db.lookupPartitionOffsetAppendOutcome(ctx, commit, fingerprint); err != nil {
		return nil, err
	} else if found {
		return cached.Messages, nil
	}
	coordinatorID := distributedOffsetAppendCoordinatorID(commit)
	transactionTimeout := db.distCfg.TransactionTimeout
	if transactionTimeout <= 0 {
		transactionTimeout = 30 * time.Second
	}
	deadline := time.Now().Add(transactionTimeout)
	tx, err := db.controllerQuorum.GetTransactionLinearizable(ctx, coordinatorID)
	txFound := err == nil
	if err != nil && !errors.Is(err, cluster.ErrTransactionNotFound) {
		return nil, err
	}
	resolver := db.TransactionParticipantResolver()
	if txFound {
		if tx.Fingerprint != fingerprint {
			return nil, cluster.ErrTransactionConflict
		}
		if tx.Decision != "" {
			if err := db.controllerQuorum.RecoverTransaction(ctx, coordinatorID, resolver); err != nil {
				return nil, err
			}
			if tx.Decision == cluster.TransactionAbort {
				return nil, cluster.ErrTransactionAborted
			}
			cached, found, err := db.lookupPartitionOffsetAppendOutcome(ctx, commit, fingerprint)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("committed transaction %s has no retained offset outcome", coordinatorID)
			}
			return cached.Messages, nil
		}
	}
	for _, id := range plan.groupIDs {
		if _, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition); err != nil {
			if txFound && tx.Decision == "" {
				return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
			}
			return nil, err
		}
	}
	participantIDs := make([]string, 0, len(plan.groupIDs)+1)
	for _, id := range plan.groupIDs {
		participantIDs = append(participantIDs, PartitionTransactionParticipantID(id))
	}
	sourceID := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: commit.Stream, Partition: commit.Partition}.Canonical()
	offsetParticipantID := PartitionOffsetTransactionParticipantID(sourceID)
	participantIDs = append(participantIDs, offsetParticipantID)
	sort.Strings(participantIDs)
	if !txFound {
		beginErr := db.controllerQuorum.BeginTransactionWithFingerprint(ctx, coordinatorID, participantIDs, deadline, fingerprint)
		if beginErr != nil && !errors.Is(beginErr, cluster.ErrTransactionConflict) {
			return nil, beginErr
		}
		tx, err = db.controllerQuorum.GetTransactionLinearizable(ctx, coordinatorID)
		if err != nil {
			return nil, errors.Join(beginErr, err)
		}
		txFound = true
	}
	if !sameTransactionParticipantIDs(tx.Participants, participantIDs) || tx.Fingerprint != fingerprint {
		return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, cluster.ErrTransactionConflict)
	}
	deadline = tx.Deadline
	if tx.Decision != "" {
		if err := db.controllerQuorum.RecoverTransaction(ctx, coordinatorID, resolver); err != nil {
			return nil, err
		}
		if tx.Decision == cluster.TransactionAbort {
			return nil, cluster.ErrTransactionAborted
		}
		cached, found, err := db.lookupPartitionOffsetAppendOutcome(ctx, commit, fingerprint)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("committed transaction %s has no retained offset outcome", coordinatorID)
		}
		return cached.Messages, nil
	}
	if !time.Now().Before(deadline) {
		return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, cluster.ErrTransactionExpired)
	}
	participants := make(map[string]cluster.TransactionParticipant, len(participantIDs))
	outputParticipants := make(map[cluster.PartitionGroupID]cluster.TransactionParticipant, len(plan.groupIDs))
	for _, id := range plan.groupIDs {
		refs := plan.groups[id]
		groupEntries := make([]AppendEntry, len(refs))
		for j, ref := range refs {
			entry := entries[ref.index]
			if entry.Message.ID == "" {
				entry.Message.ID = deterministicTransactionMessageID(coordinatorID, fingerprint, tx.CreatedAt, ref.index)
			}
			if entry.Message.CreatedAt.IsZero() {
				entry.Message.CreatedAt = tx.CreatedAt
			}
			groupEntries[j] = entry
		}
		participant, err := db.NewPartitionAppendTransactionParticipant(ctx, id, groupEntries)
		if err != nil {
			return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
		}
		participantID := PartitionTransactionParticipantID(id)
		participants[participantID] = participant
		outputParticipants[id] = participant
	}
	// Prepare output partitions first and persist each vote before constructing
	// the source participant's result cache.
	for _, id := range plan.groupIDs {
		participantID := PartitionTransactionParticipantID(id)
		participant := outputParticipants[id]
		if err := participant.PrepareTransaction(ctx, coordinatorID, deadline); err != nil {
			return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
		}
		if !sortedStringContains(tx.Prepared, participantID) {
			if err := db.controllerQuorum.PrepareTransactionParticipant(ctx, coordinatorID, participantID); err != nil {
				return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
			}
		}
	}
	results := make([]Message, len(entries))
	for _, id := range plan.groupIDs {
		participant, ok := outputParticipants[id].(PartitionTransactionResult)
		if !ok {
			return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, fmt.Errorf("partition append participant does not expose prepared results"))
		}
		messages := participant.PreparedMessages()
		refs := plan.groups[id]
		if len(messages) != len(refs) {
			return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, fmt.Errorf("partition %s returned %d prepared messages for %d entries", id, len(messages), len(refs)))
		}
		for j, ref := range refs {
			results[ref.index] = messages[j]
		}
	}
	outputRefs := make([]OffsetAppendOutputReference, len(entries))
	for id, refs := range plan.groups {
		var destinationKind DestinationKind
		switch id.Kind {
		case cluster.PartitionResourceTopic:
			destinationKind = DestinationTopic
		case cluster.PartitionResourceStream:
			destinationKind = DestinationStream
		case cluster.PartitionResourceQueue:
			destinationKind = DestinationQueue
		default:
			return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, cluster.ErrInvalidTransaction)
		}
		for _, ref := range refs {
			outputRefs[ref.index] = OffsetAppendOutputReference{
				Kind: destinationKind, Name: id.Topic, Partition: id.Partition, MessageID: results[ref.index].ID,
			}
		}
	}
	offsetParticipant, err := db.newPartitionOffsetOutcomeTransactionParticipant(ctx, sourceID, coordinatorID, commit, fingerprint, tx.CreatedAt, results, outputRefs)
	if err != nil {
		return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
	}
	participants[offsetParticipantID] = offsetParticipant
	if err := ctx.Err(); err != nil {
		return nil, db.abortPartitionTransaction(ctx, coordinatorID, resolver, err)
	}
	if err := db.controllerQuorum.ExecuteTransactionWithFingerprint(ctx, coordinatorID, participants, deadline, fingerprint); err != nil {
		return nil, err
	}
	committed, found, err := db.lookupPartitionOffsetAppendOutcome(ctx, commit, fingerprint)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("committed transaction %s has no retained offset outcome", coordinatorID)
	}
	return committed.Messages, nil
}

func (db *DistributedBroker) abortPartitionTransaction(ctx context.Context, id string, resolver cluster.TransactionParticipantResolver, cause error) error {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	cleanupCtx, cancel := context.WithTimeout(base, 5*time.Second)
	defer cancel()
	decideErr := db.controllerQuorum.DecideTransaction(cleanupCtx, id, false)
	recoverErr := db.controllerQuorum.RecoverTransaction(cleanupCtx, id, resolver)
	return errors.Join(cause, decideErr, recoverErr)
}

func sameTransactionParticipantIDs(a, b []string) bool {
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

func sortedStringContains(values []string, value string) bool {
	i := sort.SearchStrings(values, value)
	return i < len(values) && values[i] == value
}
