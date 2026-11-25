package broker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker/cluster"
)

const partitionTransactionParticipantPrefix = "partition-tx:"
const partitionOffsetTransactionParticipantPrefix = "partition-tx:!offset:"

// PartitionTransactionParticipantID returns the stable coordinator key for a
// partition transaction participant.
func PartitionTransactionParticipantID(id cluster.PartitionGroupID) string {
	id = id.Canonical()
	data, _ := json.Marshal(id)
	return partitionTransactionParticipantPrefix + base64.RawURLEncoding.EncodeToString(data)
}

// PartitionOffsetTransactionParticipantID returns the stable coordinator key
// for a stream checkpoint and its cached transaction outcome. Its ordering
// makes the checkpoint resolve after output participants on commit.
func PartitionOffsetTransactionParticipantID(id cluster.PartitionGroupID) string {
	id = id.Canonical()
	data, _ := json.Marshal(id)
	return partitionOffsetTransactionParticipantPrefix + base64.RawURLEncoding.EncodeToString(data)
}

// NewPartitionAppendTransactionParticipant creates a durable participant that
// stages a deterministic append in one assigned resource partition. The
// caller must perform broker-level authorization, schema, rate, and tenant
// quota admission before constructing the participant.
func (db *DistributedBroker) NewPartitionAppendTransactionParticipant(ctx context.Context, id cluster.PartitionGroupID, entries []AppendEntry) (cluster.TransactionParticipant, error) {
	id = id.Canonical()
	_, route, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil && (!errors.Is(err, ErrBrokerUnavailable) || route == nil || route.LocalLeader) {
		return nil, err
	}
	if !routed || route.Partition.ID.Canonical() != id {
		return nil, cluster.ErrPartitionNotFound
	}
	if len(entries) == 0 {
		return nil, errf(ErrInvalidMessage, "partition transaction append requires entries")
	}
	for _, entry := range entries {
		kind, name, single := singlePartitionDestination(entry.Destinations)
		if !single {
			if entry.Message.Queue != "" {
				kind, name, single = cluster.PartitionResourceQueue, entry.Message.Queue, true
			} else if entry.Message.Stream != "" {
				kind, name, single = cluster.PartitionResourceStream, entry.Message.Stream, true
			} else if entry.Message.Topic != "" {
				kind, name, single = cluster.PartitionResourceTopic, entry.Message.Topic, true
			}
		}
		if !single || kind != id.Kind || name != id.Topic || entry.Message.Partition != id.Partition {
			return nil, fmt.Errorf("%w: participant append must target exactly partition %s", ErrUnsupportedMode, id)
		}
	}
	payload, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	return &partitionTransactionParticipant{
		db: db, id: id, operation: cluster.BrokerCmdAppendAtomic, payload: payload,
		requestFingerprint: transactionPayloadFingerprint(payload),
	}, nil
}

// NewPartitionOffsetTransactionParticipant creates a participant that stages
// a consumer checkpoint in one assigned stream partition.
func (db *DistributedBroker) NewPartitionOffsetTransactionParticipant(ctx context.Context, id cluster.PartitionGroupID, commit OffsetCommit) (cluster.TransactionParticipant, error) {
	id = id.Canonical()
	if id.Kind != cluster.PartitionResourceStream || commit.Stream != id.Topic || !commit.Partitioned || commit.Partition != id.Partition {
		return nil, fmt.Errorf("%w: offset participant must match an assigned stream partition", ErrInvalidMessage)
	}
	_, route, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil && (!errors.Is(err, ErrBrokerUnavailable) || route == nil || route.LocalLeader) {
		return nil, err
	}
	if !routed || route.Partition.ID.Canonical() != id {
		return nil, cluster.ErrPartitionNotFound
	}
	commit, err = durableOffsetCommit(ctx, commit)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(replicatedOffsetCommit{Version: 2, Commit: commit})
	if err != nil {
		return nil, err
	}
	return &partitionTransactionParticipant{
		db: db, id: id, operation: cluster.BrokerCmdCommitOffset, payload: payload,
		requestFingerprint: transactionPayloadFingerprint(payload),
	}, nil
}

func (db *DistributedBroker) newPartitionQueueControlTransactionParticipant(ctx context.Context, id cluster.PartitionGroupID, action string) (cluster.TransactionParticipant, error) {
	id = id.Canonical()
	if id.Kind != cluster.PartitionResourceQueue || (action != "pause" && action != "resume" && action != "purge") {
		return nil, cluster.ErrInvalidTransaction
	}
	_, route, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil && (!errors.Is(err, ErrBrokerUnavailable) || route == nil || route.LocalLeader) {
		return nil, err
	}
	if !routed || route.Partition.ID.Canonical() != id {
		return nil, cluster.ErrPartitionNotFound
	}
	operation := cluster.BrokerCmdPauseQueue
	if action == "resume" {
		operation = cluster.BrokerCmdResumeQueue
	} else if action == "purge" {
		operation = cluster.BrokerCmdPurgeQueue
	}
	payload, err := json.Marshal(id.Topic)
	if err != nil {
		return nil, err
	}
	return &partitionTransactionParticipant{
		db: db, id: id, operation: operation, payload: payload,
		requestFingerprint: transactionPayloadFingerprint(payload),
		participantID:      PartitionTransactionParticipantID(id),
	}, nil
}

func (db *DistributedBroker) newPartitionOffsetOutcomeTransactionParticipant(ctx context.Context, id cluster.PartitionGroupID, coordinatorID string, commit OffsetCommit, fingerprint string, createdAt time.Time, messages []Message, outputs []OffsetAppendOutputReference) (cluster.TransactionParticipant, error) {
	id = id.Canonical()
	if id.Kind != cluster.PartitionResourceStream || commit.Stream != id.Topic || !commit.Partitioned || commit.Partition != id.Partition ||
		coordinatorID == "" || commit.TransactionID == "" || fingerprint == "" {
		return nil, fmt.Errorf("%w: offset outcome participant requires a transaction ID matching an assigned stream partition", ErrInvalidMessage)
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil && (!errors.Is(err, ErrBrokerUnavailable) || route == nil || route.LocalLeader) {
		return nil, err
	}
	if !routed || route.Partition.ID.Canonical() != id {
		return nil, cluster.ErrPartitionNotFound
	}
	if route.LocalLeader {
		for _, output := range outputs {
			if output.Kind == DestinationStream {
				if _, ok := store.(interface {
					updateOffsetAppendTransactionResult(OffsetCommit, string, []Message) error
				}); !ok {
					return nil, fmt.Errorf("%w: source partition store cannot repair transaction result offsets", ErrUnsupportedMode)
				}
			}
		}
	}
	commit, err = durableOffsetCommit(ctx, commit)
	if err != nil {
		return nil, err
	}
	if createdAt.IsZero() {
		return nil, cluster.ErrInvalidTransaction
	}
	if len(outputs) != len(messages) {
		return nil, cluster.ErrInvalidTransaction
	}
	for i, output := range outputs {
		if output.MessageID == "" || output.MessageID != messages[i].ID || output.Name == "" || output.Partition < 0 {
			return nil, cluster.ErrInvalidTransaction
		}
		switch output.Kind {
		case DestinationTopic, DestinationStream, DestinationQueue:
		default:
			return nil, cluster.ErrInvalidTransaction
		}
	}
	transaction := OffsetAppendTransaction{Fingerprint: fingerprint, Messages: compactOffsetAppendMessages(messages), Outputs: outputs, CreatedAt: createdAt.UTC()}
	payload, err := json.Marshal(replicatedOffsetAppend{Offset: commit, Transaction: transaction, CoordinatorID: coordinatorID})
	if err != nil {
		return nil, err
	}
	return &partitionTransactionParticipant{
		db: db, id: id, operation: cluster.BrokerCmdCommitOffsetAppend, payload: payload,
		requestFingerprint: transactionPayloadFingerprint(payload),
		participantID:      PartitionOffsetTransactionParticipantID(id),
	}, nil
}

type partitionTransactionParticipant struct {
	db                 *DistributedBroker
	id                 cluster.PartitionGroupID
	operation          cluster.BrokerCommandType
	payload            json.RawMessage
	requestFingerprint string
	participantID      string
	preparedMessages   []Message
	purgeMu            sync.Mutex
	purgedCount        int
	purgedCountReady   bool
}

// PartitionTransactionResult exposes the deterministic messages prepared by an
// append participant. Read it after successful transaction execution when the
// participant was created by NewPartitionAppendTransactionParticipant.
type PartitionTransactionResult interface {
	PreparedMessages() []Message
}

func (p *partitionTransactionParticipant) PreparedMessages() []Message {
	return cloneTransactionMessages(p.preparedMessages)
}

func (p *partitionTransactionParticipant) PurgedCount(ctx context.Context, txID string) (int, bool, error) {
	p.purgeMu.Lock()
	defer p.purgeMu.Unlock()
	return p.purgedCount, p.purgedCountReady, nil
}

func (p *partitionTransactionParticipant) PrepareTransaction(ctx context.Context, txID string, deadline time.Time) error {
	participantID := p.participantID
	if participantID == "" {
		participantID = PartitionTransactionParticipantID(p.id)
	}
	messages, err := p.db.forwardPartitionTransactionPrepare(ctx, p.id, txID, deadline, p.operation, p.payload, p.requestFingerprint, participantID)
	if err != nil {
		return err
	}
	p.preparedMessages = cloneTransactionMessages(messages)
	return nil
}

func (db *DistributedBroker) preparePartitionTransactionFromPeer(ctx context.Context, id cluster.PartitionGroupID, txID string, deadline time.Time, operation cluster.BrokerCommandType, payload []byte, requestFingerprint string) ([]Message, error) {
	if deadline.IsZero() || !time.Now().Before(deadline) {
		return nil, cluster.ErrTransactionExpired
	}
	if len(payload) == 0 || len(payload) > 64<<20 {
		return nil, cluster.ErrInvalidTransaction
	}
	if requestFingerprint != transactionPayloadFingerprint(payload) {
		return nil, cluster.ErrInvalidTransaction
	}
	if db.controllerQuorum == nil {
		return nil, fmt.Errorf("%w: partition transaction requires the controller quorum", ErrUnsupportedMode)
	}
	participantID := PartitionTransactionParticipantID(id)
	if operation == cluster.BrokerCmdCommitOffsetAppend {
		if id.Kind != cluster.PartitionResourceStream {
			return nil, cluster.ErrInvalidTransaction
		}
		participantID = PartitionOffsetTransactionParticipantID(id)
	}
	coordinatorTx, err := db.controllerQuorum.GetTransactionLinearizable(ctx, txID)
	if err != nil {
		return nil, err
	}
	participantIndex := sort.SearchStrings(coordinatorTx.Participants, participantID)
	participantPresent := participantIndex < len(coordinatorTx.Participants) && coordinatorTx.Participants[participantIndex] == participantID
	if coordinatorTx.Decision != "" || !coordinatorTx.Deadline.Equal(deadline.UTC()) || !participantPresent {
		return nil, cluster.ErrTransactionConflict
	}
	if !time.Now().Before(coordinatorTx.Deadline) {
		return nil, cluster.ErrTransactionExpired
	}
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return nil, err
	}
	if !route.LocalLeader {
		return nil, NotLeaderError(route.LeaderClientAddress, route.RaftTerm)
	}
	staged, found, err := db.lookupPartitionStagedTransaction(ctx, id, txID, participantID)
	if err != nil {
		return nil, err
	}
	if found {
		if staged.Operation != operation || staged.RequestFingerprint != requestFingerprint {
			return nil, cluster.ErrTransactionConflict
		}
		if staged.Rejection != "" {
			return nil, transactionPrepareRejectionError(staged.Rejection)
		}
		return stagedTransactionMessages(staged)
	}
	store, currentRoute, routed, err := db.partitionStoreForResourceWrite(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return nil, err
	}
	if !routed || !currentRoute.LocalLeader || store == nil {
		return nil, fmt.Errorf("%w: partition leader has no local transaction store", ErrBrokerUnavailable)
	}
	var stagedPayload json.RawMessage
	switch operation {
	case cluster.BrokerCmdAppendAtomic:
		var entries []AppendEntry
		if err := json.Unmarshal(payload, &entries); err != nil {
			return nil, err
		}
		for i := range entries {
			entry := &entries[i]
			if len(entry.Destinations) == 0 {
				switch {
				case entry.Message.Queue != "":
					entry.Destinations = []Destination{{Kind: DestinationQueue, Name: entry.Message.Queue}}
				case entry.Message.Stream != "":
					entry.Destinations = []Destination{{Kind: DestinationStream, Name: entry.Message.Stream}}
				case entry.Message.Topic != "":
					entry.Destinations = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
				}
			}
			kind, name, single := singlePartitionDestination(entry.Destinations)
			if !single || kind != id.Kind || name != id.Topic || entry.Message.Partition != id.Partition {
				return nil, fmt.Errorf("%w: participant append must target exactly partition %s", ErrInvalidMessage, id)
			}
		}
		if id.Kind == cluster.PartitionResourceQueue {
			if err := db.syncQueueConfigOnPartition(ctx, store, id.Topic, id.Partition, currentRoute.FencingEpoch); err != nil {
				return nil, err
			}
		}
		preparer, ok := store.(interface {
			prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
		})
		if !ok {
			return nil, fmt.Errorf("%w: partition store cannot prepare atomic appends", ErrUnsupportedMode)
		}
		prepared, err := preparer.prepareAppendAtomic(ctx, entries)
		if err != nil {
			return nil, err
		}
		stagedPayload, err = json.Marshal(storedAppendEntries(prepared))
		if err != nil {
			return nil, err
		}
	case cluster.BrokerCmdCommitOffset:
		var versioned replicatedOffsetCommit
		if err := json.Unmarshal(payload, &versioned); err != nil {
			return nil, err
		}
		if _, err := store.ConsumerOffset(ctx, versioned.Commit); err != nil {
			return nil, err
		}
		versioned.Commit.TransactionID = txID
		versioned.Commit, err = durableOffsetCommit(ctx, versioned.Commit)
		if err != nil {
			return nil, err
		}
		stagedPayload, err = json.Marshal(versioned)
		if err != nil {
			return nil, err
		}
	case cluster.BrokerCmdCommitOffsetAppend:
		var combined replicatedOffsetAppend
		if err := json.Unmarshal(payload, &combined); err != nil {
			return nil, err
		}
		coordinatorID := combined.CoordinatorID
		if coordinatorID == "" {
			coordinatorID = combined.Offset.TransactionID
		}
		if len(combined.Entries) != 0 || combined.Transaction.Fingerprint == "" || coordinatorID != txID ||
			!combined.Offset.Partitioned || combined.Offset.Stream != id.Topic || combined.Offset.Partition != id.Partition {
			return nil, cluster.ErrInvalidTransaction
		}
		if len(combined.Transaction.Outputs) != 0 {
			if len(combined.Transaction.Outputs) != len(combined.Transaction.Messages) {
				return nil, cluster.ErrInvalidTransaction
			}
			for i, output := range combined.Transaction.Outputs {
				if output.MessageID != combined.Transaction.Messages[i].ID || output.Name == "" || output.Partition < 0 ||
					(output.Kind != DestinationTopic && output.Kind != DestinationStream && output.Kind != DestinationQueue) {
					return nil, cluster.ErrInvalidTransaction
				}
			}
		}
		if _, err := store.ConsumerOffset(ctx, combined.Offset); err != nil {
			return nil, err
		}
		combined.Offset, err = durableOffsetCommit(ctx, combined.Offset)
		if err != nil {
			return nil, err
		}
		stagedPayload, err = json.Marshal(combined)
		if err != nil {
			return nil, err
		}
	case cluster.BrokerCmdPauseQueue, cluster.BrokerCmdResumeQueue:
		if id.Kind != cluster.PartitionResourceQueue {
			return nil, cluster.ErrInvalidTransaction
		}
		var queue string
		if err := json.Unmarshal(payload, &queue); err != nil || queue != id.Topic {
			return nil, cluster.ErrInvalidTransaction
		}
		lookup, ok := store.(QueueConfigLookup)
		if !ok {
			return nil, fmt.Errorf("%w: partition store cannot validate queue control", ErrUnsupportedMode)
		}
		_, found, err := lookup.LookupQueueConfig(ctx, queue)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrQueueNotFound
		}
		stagedPayload = append(json.RawMessage(nil), payload...)
	case cluster.BrokerCmdPurgeQueue:
		if id.Kind != cluster.PartitionResourceQueue {
			return nil, cluster.ErrInvalidTransaction
		}
		var queue string
		if err := json.Unmarshal(payload, &queue); err != nil || queue != id.Topic {
			return nil, cluster.ErrInvalidTransaction
		}
		lookup, ok := store.(QueueConfigLookup)
		if !ok {
			return nil, fmt.Errorf("%w: partition store cannot validate queue purge", ErrUnsupportedMode)
		}
		if _, found, err := lookup.LookupQueueConfig(ctx, queue); err != nil {
			return nil, err
		} else if !found {
			return nil, ErrQueueNotFound
		}
		stagedPayload = append(json.RawMessage(nil), payload...)
	default:
		return nil, fmt.Errorf("unsupported partition transaction operation %q", operation)
	}
	if operation == cluster.BrokerCmdCommitOffsetAppend {
		var combined replicatedOffsetAppend
		if err := json.Unmarshal(stagedPayload, &combined); err != nil {
			return nil, err
		}
		for _, output := range combined.Transaction.Outputs {
			if output.Kind == DestinationStream {
				if _, ok := store.(interface {
					updateOffsetAppendTransactionResult(OffsetCommit, string, []Message) error
				}); !ok {
					return nil, fmt.Errorf("%w: source partition store cannot repair transaction result offsets", ErrUnsupportedMode)
				}
			}
		}
	}
	command, err := json.Marshal(brokerTransactionCommand{
		TransactionID: txID, ParticipantID: participantID, Operation: operation,
		RequestFingerprint: requestFingerprint, Payload: stagedPayload,
	})
	if err != nil {
		return nil, err
	}
	if err := db.proposePartitionTransaction(ctx, id, cluster.BrokerCmdPrepareTransaction, command); err != nil {
		return nil, err
	}
	staged, found, err = db.lookupPartitionStagedTransaction(ctx, id, txID, participantID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("partition prepare committed without a durable staging record")
	}
	if staged.Rejection != "" {
		return nil, transactionPrepareRejectionError(staged.Rejection)
	}
	return stagedTransactionMessages(staged)
}

func transactionPrepareRejectionError(reason string) error {
	switch reason {
	case ErrBackpressure.Error():
		return ErrBackpressure
	case ErrQueueNotFound.Error():
		return ErrQueueNotFound
	default:
		return fmt.Errorf("transaction prepare rejected: %s", reason)
	}
}

func stagedTransactionMessages(staged stagedBrokerTransaction) ([]Message, error) {
	if staged.Operation == cluster.BrokerCmdCommitOffsetAppend {
		var combined replicatedOffsetAppend
		if err := json.Unmarshal(staged.Payload, &combined); err != nil {
			return nil, err
		}
		return cloneTransactionMessages(combined.Transaction.Messages), nil
	}
	if staged.Operation != cluster.BrokerCmdAppendAtomic {
		return nil, nil
	}
	var entries []AppendEntry
	if err := json.Unmarshal(staged.Payload, &entries); err != nil {
		return nil, err
	}
	messages := make([]Message, len(entries))
	for i := range entries {
		messages[i] = cloneMessage(entries[i].Message)
	}
	return messages, nil
}

func cloneTransactionMessages(messages []Message) []Message {
	out := make([]Message, len(messages))
	for i := range messages {
		out[i] = cloneMessage(messages[i])
	}
	return out
}

func (p *partitionTransactionParticipant) CommitTransaction(ctx context.Context, txID, participantID string) error {
	if err := p.db.resolvePartitionTransaction(ctx, txID, participantID, cluster.TransactionCommit); err != nil {
		return err
	}
	if p.operation != cluster.BrokerCmdPurgeQueue {
		return nil
	}
	count, found, err := p.db.lookupPartitionTransactionPurgeResult(ctx, p.id, txID, participantID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("committed purge result for participant %s is unavailable", participantID)
	}
	p.purgeMu.Lock()
	p.purgedCount = count
	p.purgedCountReady = true
	p.purgeMu.Unlock()
	return nil
}

func (p *partitionTransactionParticipant) AbortTransaction(ctx context.Context, txID, participantID string) error {
	return p.db.resolvePartitionTransaction(ctx, txID, participantID, cluster.TransactionAbort)
}

func (p *partitionTransactionParticipant) ForgetTransaction(ctx context.Context, txID, participantID string) error {
	return p.db.forgetPartitionTransaction(ctx, txID, participantID)
}

func (db *DistributedBroker) proposePartitionTransaction(ctx context.Context, id cluster.PartitionGroupID, commandType cluster.BrokerCommandType, payload []byte) error {
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return err
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: commandType, Payload: payload})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, id.Kind, id.Topic, id.Partition, route.FencingEpoch, data)
}

func (db *DistributedBroker) resolvePartitionTransaction(ctx context.Context, txID, participantID, decision string) error {
	id, err := decodePartitionTransactionParticipantID(participantID)
	if err != nil {
		return err
	}
	command := brokerTransactionCommand{TransactionID: txID, ParticipantID: participantID, Decision: decision}
	if decision == cluster.TransactionCommit && strings.HasPrefix(participantID, partitionOffsetTransactionParticipantPrefix) {
		staged, found, err := db.lookupPartitionStagedTransaction(ctx, id, txID, participantID)
		if err != nil {
			return err
		}
		if found && staged.Operation == cluster.BrokerCmdCommitOffsetAppend {
			var combined replicatedOffsetAppend
			if err := json.Unmarshal(staged.Payload, &combined); err != nil {
				return err
			}
			coordinatorID := combined.CoordinatorID
			if coordinatorID == "" {
				coordinatorID = combined.Offset.TransactionID
			}
			if coordinatorID != txID || !combined.Offset.Partitioned ||
				combined.Offset.Stream != id.Topic || combined.Offset.Partition != id.Partition {
				return cluster.ErrInvalidTransaction
			}
			messages := cloneTransactionMessages(combined.Transaction.Messages)
			outputs := combined.Transaction.Outputs
			if len(outputs) != 0 && len(outputs) != len(messages) {
				return cluster.ErrInvalidTransaction
			}
			if len(outputs) == len(messages) {
				hasStreamOutput := false
				resultsByPartition := make(map[cluster.PartitionGroupID]map[string]Message)
				for i, prepared := range messages {
					output := outputs[i]
					if output.MessageID != prepared.ID || output.Partition < 0 || output.Name == "" {
						return cluster.ErrInvalidTransaction
					}
					if output.Kind != DestinationTopic && output.Kind != DestinationStream && output.Kind != DestinationQueue {
						return cluster.ErrInvalidTransaction
					}
					if output.Kind != DestinationStream {
						continue
					}
					hasStreamOutput = true
					outputID := cluster.PartitionGroupID{Kind: cluster.PartitionResourceStream, Topic: output.Name, Partition: output.Partition}
					committedByID, loaded := resultsByPartition[outputID]
					if !loaded {
						participantID := PartitionTransactionParticipantID(outputID)
						committed, found, err := db.lookupPartitionTransactionResult(ctx, outputID, txID, participantID)
						if err != nil {
							return fmt.Errorf("resolve committed output participant %s: %w", participantID, err)
						}
						if !found {
							return fmt.Errorf("committed output participant %s has no retained result", participantID)
						}
						committedByID = make(map[string]Message, len(committed))
						for _, actual := range committed {
							if actual.ID == "" || actual.Stream != output.Name || actual.Partition != output.Partition {
								return cluster.ErrInvalidTransaction
							}
							if _, duplicate := committedByID[actual.ID]; duplicate {
								return cluster.ErrInvalidTransaction
							}
							committedByID[actual.ID] = actual
						}
						resultsByPartition[outputID] = committedByID
					}
					actual, exists := committedByID[output.MessageID]
					if !exists || actual.ID != prepared.ID || actual.Stream != output.Name || actual.Partition != output.Partition {
						return fmt.Errorf("committed stream output %q is unavailable for transaction result resolution", prepared.ID)
					}
					if actual.Offset < prepared.Offset {
						return cluster.ErrInvalidTransaction
					}
					messages[i].Offset = actual.Offset
				}
				if hasStreamOutput {
					command.ResultMessages = messages
				}
			}
		}
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}
	return db.proposePartitionTransaction(ctx, id, cluster.BrokerCmdResolveTransaction, payload)
}

func (db *DistributedBroker) forgetPartitionTransaction(ctx context.Context, txID, participantID string) error {
	id, err := decodePartitionTransactionParticipantID(participantID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(brokerTransactionCommand{TransactionID: txID, ParticipantID: participantID})
	if err != nil {
		return err
	}
	return db.proposePartitionTransaction(ctx, id, cluster.BrokerCmdForgetTransaction, payload)
}

func decodePartitionTransactionParticipantID(participantID string) (cluster.PartitionGroupID, error) {
	prefix := partitionTransactionParticipantPrefix
	offsetParticipant := false
	if strings.HasPrefix(participantID, partitionOffsetTransactionParticipantPrefix) {
		prefix = partitionOffsetTransactionParticipantPrefix
		offsetParticipant = true
	} else if !strings.HasPrefix(participantID, partitionTransactionParticipantPrefix) {
		return cluster.PartitionGroupID{}, fmt.Errorf("unsupported transaction participant %q", participantID)
	}
	encoded := strings.TrimPrefix(participantID, prefix)
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return cluster.PartitionGroupID{}, fmt.Errorf("invalid partition transaction participant: %w", err)
	}
	var id cluster.PartitionGroupID
	if err := json.Unmarshal(data, &id); err != nil {
		return cluster.PartitionGroupID{}, fmt.Errorf("invalid partition transaction participant: %w", err)
	}
	if id.Topic == "" || id.Partition < 0 || id.Canonical().Kind != id.Kind {
		return cluster.PartitionGroupID{}, fmt.Errorf("invalid partition transaction participant %q", participantID)
	}
	if offsetParticipant && id.Kind != cluster.PartitionResourceStream {
		return cluster.PartitionGroupID{}, fmt.Errorf("offset transaction participant %q does not identify a stream partition", participantID)
	}
	if PartitionTransactionParticipantID(id) != participantID && PartitionOffsetTransactionParticipantID(id) != participantID {
		return cluster.PartitionGroupID{}, fmt.Errorf("non-canonical partition transaction participant %q", participantID)
	}
	return id, nil
}

type distributedTransactionResolver struct {
	broker   *DistributedBroker
	fallback cluster.TransactionParticipantResolver
}

func (r distributedTransactionResolver) CommitTransaction(ctx context.Context, txID, participantID string) error {
	if strings.HasPrefix(participantID, partitionTransactionParticipantPrefix) {
		return r.broker.resolvePartitionTransaction(ctx, txID, participantID, cluster.TransactionCommit)
	}
	if r.fallback == nil {
		return fmt.Errorf("transaction participant %q requires a configured resolver", participantID)
	}
	return r.fallback.CommitTransaction(ctx, txID, participantID)
}

func (r distributedTransactionResolver) AbortTransaction(ctx context.Context, txID, participantID string) error {
	if strings.HasPrefix(participantID, partitionTransactionParticipantPrefix) {
		return r.broker.resolvePartitionTransaction(ctx, txID, participantID, cluster.TransactionAbort)
	}
	if r.fallback == nil {
		return fmt.Errorf("transaction participant %q requires a configured resolver", participantID)
	}
	return r.fallback.AbortTransaction(ctx, txID, participantID)
}

func (r distributedTransactionResolver) ForgetTransaction(ctx context.Context, txID, participantID string) error {
	if strings.HasPrefix(participantID, partitionTransactionParticipantPrefix) {
		return r.broker.forgetPartitionTransaction(ctx, txID, participantID)
	}
	if forgetter, ok := r.fallback.(cluster.TransactionParticipantForgetter); ok {
		return forgetter.ForgetTransaction(ctx, txID, participantID)
	}
	return nil
}

// TransactionParticipantResolver returns the built-in partition resolver,
// delegating non-partition participants to the configured fallback resolver.
func (db *DistributedBroker) TransactionParticipantResolver() cluster.TransactionParticipantResolver {
	return distributedTransactionResolver{broker: db, fallback: db.distCfg.TransactionResolver}
}
