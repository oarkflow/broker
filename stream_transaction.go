package broker

import (
	"context"
	"fmt"
)

// StreamTransaction atomically advances one consumer group's input offset and
// appends its outputs. ExpectedOffset must equal the currently committed offset;
// Offset.Offset must advance it and cannot exceed the input stream's end.
// All outputs belong to the same tenant as the offset commit.
type StreamTransaction struct {
	Offset         OffsetCommit  `json:"offset"`
	ExpectedOffset uint64        `json:"expected_offset"`
	Entries        []AppendEntry `json:"entries,omitempty"`
}

// StreamTransactionStore is optional. Stores must either commit the entire
// operation or reject it; callers must never emulate it with separate appends
// and offset commits. External application side effects are outside this atomicity.
type StreamTransactionStore interface {
	CommitStreamTransaction(context.Context, StreamTransaction) ([]Message, error)
}

func normalizeStreamTransaction(ctx context.Context, tx StreamTransaction) (StreamTransaction, error) {
	if err := ctx.Err(); err != nil {
		return tx, err
	}
	if tx.Offset.Stream == "" || tx.Offset.ConsumerGroup == "" || tx.Offset.Offset <= tx.ExpectedOffset {
		return tx, errf(ErrInvalidMessage, "stream, consumer group and an advancing offset are required")
	}
	tenant, err := resolveTenant(ctx, tx.Offset.TenantID)
	if err != nil {
		return tx, err
	}
	tx.Offset.TenantID = tenant
	tx.Entries = append([]AppendEntry(nil), tx.Entries...)
	for i := range tx.Entries {
		msg := &tx.Entries[i].Message
		if msg.TenantID == "" {
			msg.TenantID = tenant
		}
		msg.TenantID, err = resolveTenant(ctx, msg.TenantID)
		if err != nil {
			return tx, err
		}
		if msg.TenantID != tenant {
			return tx, errf(ErrUnauthorized, "transaction outputs must belong to the input tenant")
		}
	}
	return tx, nil
}

func (s *MemoryStore) validateStreamTransactionLocked(tx StreamTransaction) error {
	if s.closed {
		return ErrClosed
	}
	stream := s.streams[tx.Offset.Stream]
	if stream == nil {
		return ErrStreamNotFound
	}
	current := s.offsets[tx.Offset.Stream][offsetGroupKey(tx.Offset.TenantID, tx.Offset.ConsumerGroup)]
	if current != tx.ExpectedOffset {
		return errf(ErrOffsetConflict, "committed offset is %d, expected %d", current, tx.ExpectedOffset)
	}
	if tx.Offset.Offset > stream.offset {
		return errf(ErrInvalidMessage, "commit offset %d exceeds stream end %d", tx.Offset.Offset, stream.offset)
	}
	if err := s.validateAtomicLocked(tx.Entries); err != nil {
		return err
	}
	// Reject ID collisions and output-dropping backpressure policies before any
	// mutation. A transaction must not overwrite retained data or discard output.
	ids := make(map[string]struct{}, len(tx.Entries))
	depths := make(map[string]int)
	for _, entry := range tx.Entries {
		dests := entry.Destinations
		if len(dests) == 0 {
			dests = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
		}
		for i, dest := range dests {
			id := entry.Message.ID
			if i > 0 {
				id += ":copy:" + itoa(i)
			}
			_, seen := ids[id]
			_, completed := s.completed[id]
			if seen || completed || s.hasMsgLocked(id) {
				return errf(ErrDuplicateMessage, "transaction output id %q already exists", id)
			}
			ids[id] = struct{}{}
			if dest.Kind == DestinationQueue || dest.Kind == DestinationDLQ {
				depths[dest.Name]++
				if q := s.queues[dest.Name]; q != nil && q.cfg.MaxDepth > 0 && q.depth+depths[dest.Name] > q.cfg.MaxDepth {
					return ErrBackpressure
				}
			}
		}
	}
	return nil
}

func (s *MemoryStore) CommitStreamTransaction(ctx context.Context, tx StreamTransaction) ([]Message, error) {
	tx, err := normalizeStreamTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateStreamTransactionLocked(tx); err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(tx.Entries))
	for _, entry := range tx.Entries {
		messages, err := s.appendLocked(entry.Message, entry.Destinations)
		if err != nil {
			return nil, err // all error-producing conditions were prevalidated
		}
		out = append(out, messages...)
	}
	s.setTransactionOffsetLocked(tx.Offset)
	return out, nil
}

func (s *MemoryStore) setTransactionOffsetLocked(offset OffsetCommit) {
	if s.offsets[offset.Stream] == nil {
		s.offsets[offset.Stream] = make(map[string]uint64)
	}
	s.offsets[offset.Stream][offsetGroupKey(offset.TenantID, offset.ConsumerGroup)] = offset.Offset
}

// The owning WAL store holds writeMu through preparation, fsync and application.
// The memory lock is released during fsync, so reads continue to see the old
// offset and no output. The operation becomes visible under one memory lock.
func durableStreamTransaction(ctx context.Context, mem *MemoryStore, tx StreamTransaction, persist func(walRecord) error) ([]Message, error) {
	tx, err := normalizeStreamTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	mem.mu.RLock()
	err = mem.validateStreamTransactionLocked(tx)
	mem.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	out, err := mem.prepareAppendAtomic(ctx, tx.Entries)
	if err != nil {
		return nil, err
	}
	offset, err := durableOffsetCommit(ctx, tx.Offset)
	if err != nil {
		return nil, err
	}
	record := walRecord{Op: "stream_transaction", Offset: offset, ExpectedOffset: tx.ExpectedOffset, Entries: storedAppendEntries(out)}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := persist(record); err != nil {
		return nil, err
	}
	if err := mem.applyStreamTransaction(record); err != nil {
		return nil, fmt.Errorf("apply durable stream transaction: %w", err)
	}
	return out, nil
}

// applyStreamTransaction installs a prepared record; Offset already contains
// the persisted tenant-scoped group key. It also serves WAL recovery.
func (s *MemoryStore) applyStreamTransaction(record walRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.Offset.Stream == "" || record.Offset.ConsumerGroup == "" || record.Offset.Offset <= record.ExpectedOffset {
		return errf(ErrInvalidMessage, "invalid recovered stream transaction offset")
	}
	stream := s.streams[record.Offset.Stream]
	if stream == nil {
		return ErrStreamNotFound
	}
	if record.Offset.Offset > stream.offset {
		return errf(ErrInvalidMessage, "recovered transaction offset exceeds stream end")
	}
	current := s.offsets[record.Offset.Stream][record.Offset.ConsumerGroup]
	if current == record.Offset.Offset {
		return nil // replay of the same committed operation is idempotent
	}
	if current != record.ExpectedOffset {
		return errf(ErrOffsetConflict, "recovered offset is %d, expected %d", current, record.ExpectedOffset)
	}
	for _, entry := range record.Entries {
		if _, err := s.appendLockedCollect(entry.Message, entry.Destinations, false, false, true); err != nil {
			return err
		}
	}
	s.setTransactionOffsetLocked(record.Offset)
	return nil
}

func (s *FileWALStore) CommitStreamTransaction(ctx context.Context, tx StreamTransaction) ([]Message, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return durableStreamTransaction(ctx, s.MemoryStore, tx, s.write)
}

func (s *BinaryFileWALStore) CommitStreamTransaction(ctx context.Context, tx StreamTransaction) ([]Message, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return durableStreamTransaction(ctx, s.MemoryStore, tx, s.write)
}

func (s *SegmentedWALStore) CommitStreamTransaction(ctx context.Context, tx StreamTransaction) ([]Message, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return durableStreamTransaction(ctx, s.MemoryStore, tx, s.writeRecordLocked)
}

func (s *PartitionedBinaryWALStore) CommitStreamTransaction(ctx context.Context, tx StreamTransaction) ([]Message, error) {
	tx, err := normalizeStreamTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	locked, err := s.prepareProducerAppend(tx.Entries)
	if err != nil {
		return nil, err
	}
	defer s.unlockProducers(locked)
	idx := s.shardForStable("stream:" + tx.Offset.Stream)
	for _, entry := range tx.Entries {
		msg := entry.Message
		fixedPlacement := msg.IdempotencyKey != "" || msg.DeduplicationID != "" || msg.Key != "" || msg.RoutingKey != "" || (msg.ProducerID != "" && msg.ProducerSequence != nil)
		// Unkeyed queue/topic outputs have no ordering or deduplication shard
		// constraint. Co-locate them with the input instead of hashing their
		// newly generated IDs; remember() and recovery retain their placement.
		if fixedPlacement && s.shardForMessage(msg, entry.Destinations) != idx {
			return nil, errf(ErrUnsupportedMode, "stream transaction spans WAL shards")
		}
		for _, dest := range entry.Destinations {
			if (dest.Kind == DestinationStream || dest.Kind == DestinationArchive) && s.shardForStable("stream:"+dest.Name) != idx {
				return nil, errf(ErrUnsupportedMode, "stream transaction spans WAL shards")
			}
		}
	}
	out, err := s.shards[idx].CommitStreamTransaction(ctx, tx)
	if err == nil {
		s.remember(idx, out)
	}
	return out, err
}

var _ StreamTransactionStore = (*MemoryStore)(nil)
var _ StreamTransactionStore = (*FileWALStore)(nil)
var _ StreamTransactionStore = (*BinaryFileWALStore)(nil)
var _ StreamTransactionStore = (*SegmentedWALStore)(nil)
var _ StreamTransactionStore = (*PartitionedBinaryWALStore)(nil)
