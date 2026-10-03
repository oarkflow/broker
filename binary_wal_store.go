package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BinaryFileWALStore is a durable single-node store backed by the native BWL1
// operation codec. BWL1 writes packed mutation records directly—without JSON
// or a generic Message wrapper—and protects them with CRC32C plus optional
// HMAC-SHA256 and AES-GCM.
//
// WAL-first ordering guarantee: every durable mutation (Lease, Ack, Nack,
// Move, Update, PauseQueue, ResumeQueue, PurgeQueue, CommitOffset) encodes and
// group-commits the WAL record BEFORE applying the change to the in-memory
// index. A crash between WAL write and memory application is safe: on recovery
// the WAL record is replayed and the in-memory state is rebuilt correctly.
type BinaryFileWALStore struct {
	*MemoryStore
	path     string
	file     *os.File
	codec    FrameCodec // retained for legacy FWB1/FWB2 WAL recovery
	walCodec *WALRecordCodec
	keyName  string
	gc       *groupCommitWriter
	writeMu  sync.Mutex
}

func (*BinaryFileWALStore) ConfirmationLevel() ConfirmLevel { return ConfirmPersisted }

func (s *BinaryFileWALStore) StoreHealth() error {
	if s == nil || s.gc == nil {
		return ErrClosed
	}
	return s.gc.healthError()
}

func (s *BinaryFileWALStore) SetHealthCallback(fn func(error)) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.gc != nil {
		s.gc.setFaultCallback(fn)
	}
}

func NewBinaryFileWALStore(dir string, opts ...BinaryMessageCodecOption) (*BinaryFileWALStore, error) {
	return NewBinaryFileWALStoreWithCodec(dir, NewBinaryMessageCodec(opts...))
}

// NewBinaryFileWALStoreWithCodec creates a BinaryFileWALStore with a specific codec (FWB1 or FWB2).
func NewBinaryFileWALStoreWithCodec(dir string, codec FrameCodec) (*BinaryFileWALStore, error) {
	if codec == nil {
		codec = NewBinaryMessageCodec()
	}
	walCodec, err := newWALRecordCodecFromFrameCodec(codec)
	if err != nil {
		return nil, err
	}
	return NewBinaryFileWALStoreWithWALCodec(dir, codec, walCodec)
}

// NewBinaryFileWALStoreWithWALCodec allows applications using a custom network
// FrameCodec to configure the native BWL1 durability codec explicitly. The
// frame codec is retained only to recover pre-BWL1 FWB1/FWB2 JSON records.
func NewBinaryFileWALStoreWithWALCodec(dir string, legacyCodec FrameCodec, walCodec *WALRecordCodec) (*BinaryFileWALStore, error) {
	if legacyCodec == nil {
		legacyCodec = NewBinaryMessageCodec()
	}
	if walCodec == nil {
		var err error
		walCodec, err = newWALRecordCodecFromFrameCodec(legacyCodec)
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "broker.wal.fwb")
	st := &BinaryFileWALStore{MemoryStore: NewMemoryStore(), path: path, codec: legacyCodec, walCodec: walCodec}
	if err := st.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st.file = f
	st.gc = newGroupCommitWriter(f)
	return st, nil
}

func (s *BinaryFileWALStore) recover() error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var pos int64
	for {
		l, err := binary.ReadUvarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.normalizeRecoveredInflight()
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				_ = os.Truncate(s.path, pos)
				s.normalizeRecoveredInflight()
				return nil
			}
			return err
		}
		pos += int64(uvarintLen(l))
		maxFrame := uint64(s.walCodec.maxRecordSize)
		if maxFrame < 1<<20 {
			maxFrame = 1 << 20 // retain compatibility with legacy default frames
		}
		if l == 0 || l > maxFrame {
			_ = os.Truncate(s.path, pos-int64(uvarintLen(l)))
			s.normalizeRecoveredInflight()
			return nil
		}
		buf := make([]byte, int(l))
		if _, err := io.ReadFull(r, buf); err != nil {
			_ = os.Truncate(s.path, pos-int64(uvarintLen(l)))
			s.normalizeRecoveredInflight()
			return nil
		}
		pos += int64(l)
		var rec walRecord
		if isNativeBinaryWALRecord(buf) {
			if err := s.walCodec.decode(buf, &rec); err != nil {
				return fmt.Errorf("decode BWL record at offset %d: %w", pos-int64(l), err)
			}
		} else {
			// Backward-compatible recovery for WAL files produced before BWL1.
			// New writes never use this JSON path.
			var msg Message
			if err := s.codec.DecodeMessage(buf, &msg); err != nil {
				return fmt.Errorf("decode legacy WAL frame at offset %d: %w", pos-int64(l), err)
			}
			if err := json.Unmarshal(msg.Payload, &rec); err != nil {
				return fmt.Errorf("decode legacy JSON WAL record at offset %d: %w", pos-int64(l), err)
			}
		}
		s.applyRecovered(rec)
	}
}

func (s *BinaryFileWALStore) normalizeRecoveredInflight() {
	s.MemoryStore.mu.Lock()
	defer s.MemoryStore.mu.Unlock()
	for _, m := range s.MemoryStore.messages {
		switch m.Status {
		case MessageLeased, MessageDelivered, MessageProcessing, MessageProgressing:
			m.Status = MessageQueued
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			m.Redeliveries++
			s.MemoryStore.addHistoryLocked(m.ID, MessageQueued, "recovered_redelivery", m.Queue, "", "broker restart", m.ConsumerID, "")
		}
	}
}

func (s *BinaryFileWALStore) applyRecovered(r walRecord) {
	ctx := context.Background()
	switch r.Op {
	case "declare_queue":
		_ = s.MemoryStore.DeclareQueue(ctx, r.Queue)
	case "declare_topic":
		_ = s.MemoryStore.DeclareTopic(ctx, r.Topic)
	case "declare_stream":
		_ = s.MemoryStore.DeclareStream(ctx, r.Stream)
	case "append":
		_, _ = s.MemoryStore.Append(ctx, r.Message, r.Dests)
	case "append_atomic":
		_ = s.MemoryStore.appendRecoveredAtomic(r.Entries)
	case "lease":
		_ = s.MemoryStore.Update(ctx, r.Message)
	case "lease_batch", "update_batch":
		_ = errors.Join(s.MemoryStore.UpdateBatch(ctx, r.Messages)...)
	case "ack":
		_ = s.MemoryStore.Ack(ctx, r.Ack)
	case "ack_batch":
		_ = errors.Join(s.MemoryStore.AckBatch(ctx, r.Acks)...)
	case "nack":
		_ = s.MemoryStore.Nack(ctx, r.Nack)
	case "move":
		_, _ = s.MemoryStore.Move(ctx, r.Move)
	case "pause_queue":
		_ = s.MemoryStore.PauseQueue(ctx, r.QueueName)
	case "resume_queue":
		_ = s.MemoryStore.ResumeQueue(ctx, r.QueueName)
	case "purge_queue":
		_, _ = s.MemoryStore.PurgeQueue(ctx, r.QueueName)
	case "update":
		_ = s.MemoryStore.Update(ctx, r.Message)
	case "commit_offset":
		_ = s.MemoryStore.CommitOffset(ctx, r.Offset)
	case "sweep_leases":
		_ = s.MemoryStore.applySweepLeases(r.Messages)
	}
}
func (s *BinaryFileWALStore) write(r walRecord) error {
	req, err := s.writeAsync(r)
	if err != nil {
		return err
	}
	return s.gc.await(req)
}

// writeAsync queues the record in WAL order and returns before the fsync. The
// caller must hold writeMu while calling it and call s.gc.await(req) after
// releasing the lock.
func (s *BinaryFileWALStore) writeAsync(r walRecord) (*groupCommitReq, error) {
	if s.file == nil || s.gc == nil {
		return nil, ErrClosed
	}
	r.At = time.Now()
	r.Checksum = "" // BWL1 CRC32C/HMAC protect the complete native record.
	record, err := s.walCodec.encode(nil, r)
	if err != nil {
		return nil, err
	}
	return s.gc.enqueue(record, true)
}

func uvarintLen(v uint64) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutUvarint(b[:], v)
}

func (s *BinaryFileWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "queue"); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.write(walRecord{Op: "declare_queue", Queue: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareQueue(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "topic"); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.write(walRecord{Op: "declare_topic", Topic: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareTopic(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "stream"); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.write(walRecord{Op: "declare_stream", Stream: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareStream(ctx, cfg)
}
func (s *BinaryFileWALStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	return s.durableAppend(ctx, []AppendEntry{{Message: msg, Destinations: dests}})
}
func (s *BinaryFileWALStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	return s.durableAppend(ctx, entries)
}
func (s *BinaryFileWALStore) AppendAtomicMinimal(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}
func (s *BinaryFileWALStore) AppendAtomicOwned(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}
func (s *BinaryFileWALStore) durableAppend(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	s.writeMu.Lock()
	out, err := s.MemoryStore.prepareAppendAtomic(ctx, entries)
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	persisted := storedAppendEntries(out)
	req, err := s.writeAsync(walRecord{Op: "append_atomic", Entries: persisted})
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	// Apply immediately, hidden from readers, so the next append validates
	// against it; the fsync wait happens outside writeMu so concurrent appends
	// share one commit.
	applyErr := s.MemoryStore.appendRecoveredAtomicPending(persisted)
	s.writeMu.Unlock()
	if err := s.gc.await(req); err != nil {
		return nil, err
	}
	s.MemoryStore.commitPending(persisted)
	if applyErr != nil {
		return nil, fmt.Errorf("apply durable append: %w", applyErr)
	}
	return out, nil
}

// Lease implements WAL-first ordering: prepare the lease state (no mutation),
// fsync the WAL record, then apply the mutation to the in-memory index.
func (s *BinaryFileWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	s.writeMu.Lock()
	msgs, err := s.MemoryStore.prepareLease(ctx, req)
	if err != nil || len(msgs) == 0 {
		s.writeMu.Unlock()
		return msgs, err
	}
	pending, err := s.writeAsync(walRecord{Op: "lease_batch", Messages: msgs, Lease: req})
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	applyErr := s.MemoryStore.applyLease(ctx, msgs, req)
	s.writeMu.Unlock()
	if err := s.gc.await(pending); err != nil {
		return nil, err
	}
	if applyErr != nil {
		return nil, applyErr
	}
	return msgs, nil
}

// Ack implements WAL-first ordering: validate (read-only), WAL, then apply.
func (s *BinaryFileWALStore) Ack(ctx context.Context, req AckRequest) error {
	s.writeMu.Lock()
	if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
		s.writeMu.Unlock()
		return err
	}
	pending, err := s.writeAsync(walRecord{Op: "ack", Ack: req})
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	applyErr := s.MemoryStore.Ack(ctx, req)
	s.writeMu.Unlock()
	if err := s.gc.await(pending); err != nil {
		return err
	}
	return applyErr
}

// AckBatch implements WAL-first ordering: validate all, WAL the valid subset, then apply.
func (s *BinaryFileWALStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	s.writeMu.Lock()
	errs := make([]error, len(reqs))
	valid := make([]int, 0, len(reqs))
	validReqs := make([]AckRequest, 0, len(reqs))
	for i, req := range reqs {
		if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
			errs[i] = err
		} else {
			valid = append(valid, i)
			validReqs = append(validReqs, req)
		}
	}
	if len(valid) == 0 {
		s.writeMu.Unlock()
		return errs
	}
	pending, walErr := s.writeAsync(walRecord{Op: "ack_batch", Acks: validReqs})
	if walErr != nil {
		s.writeMu.Unlock()
		for _, i := range valid {
			errs[i] = walErr
		}
		return errs
	}
	applyErrs := s.MemoryStore.AckBatch(ctx, validReqs)
	s.writeMu.Unlock()
	if walErr := s.gc.await(pending); walErr != nil {
		for _, i := range valid {
			errs[i] = walErr
		}
		return errs
	}
	for j, i := range valid {
		errs[i] = applyErrs[j]
	}
	return errs
}

// Nack implements WAL-first ordering: validate (read-only), WAL, then apply.
func (s *BinaryFileWALStore) Nack(ctx context.Context, req NackRequest) error {
	s.writeMu.Lock()
	if err := s.MemoryStore.validateNackLease(ctx, req); err != nil {
		s.writeMu.Unlock()
		return err
	}
	pending, err := s.writeAsync(walRecord{Op: "nack", Nack: req})
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	applyErr := s.MemoryStore.Nack(ctx, req)
	s.writeMu.Unlock()
	if err := s.gc.await(pending); err != nil {
		return err
	}
	return applyErr
}

// ExtendLease implements WAL-first ordering: validate, snapshot the updated
// lease-until timestamp, WAL, then apply.
func (s *BinaryFileWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Validate (read-only) and snapshot the current message state.
	m, err := s.MemoryStore.validateExtendLease(ctx, req)
	if err != nil {
		return err
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	m.LeaseUntil = time.Now().Add(req.VisibilityTimeout)
	// WAL-first: persist the post-extension state.
	if err := s.write(walRecord{Op: "lease", Message: m, Lease: req}); err != nil {
		return err
	}
	return s.MemoryStore.ExtendLease(ctx, req)
}

// Update implements WAL-first ordering. The target must already exist;
// validating first avoids appending a WAL record that the apply step would
// reject, which would leave a spurious record for replay to trip over.
func (s *BinaryFileWALStore) Update(ctx context.Context, msg Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	existing, ok, err := s.MemoryStore.Get(ctx, msg.ID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, existing.TenantID) {
		return ErrUnauthorized
	}
	if err := s.write(walRecord{Op: "update", Message: msg}); err != nil {
		return err
	}
	return s.MemoryStore.Update(ctx, msg)
}

// UpdateBatch implements WAL-first ordering: validate the batch, WAL the valid
// subset, then apply. WALing entries whose targets are missing would append
// records the apply step rejects.
func (s *BinaryFileWALStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	errs := make([]error, len(messages))
	successful := make([]Message, 0, len(messages))
	indices := make([]int, 0, len(messages))
	for i, msg := range messages {
		existing, ok, err := s.MemoryStore.Get(ctx, msg.ID)
		if err != nil {
			errs[i] = err
		} else if !ok {
			errs[i] = ErrMessageNotFound
		} else if !tenantAllowed(ctx, existing.TenantID) {
			errs[i] = ErrUnauthorized
		} else {
			successful = append(successful, messages[i])
			indices = append(indices, i)
		}
	}
	if len(successful) == 0 {
		return errs
	}
	if walErr := s.write(walRecord{Op: "update_batch", Messages: successful}); walErr != nil {
		for _, i := range indices {
			errs[i] = walErr
		}
		return errs
	}
	applied := s.MemoryStore.UpdateBatch(ctx, successful)
	for j, i := range indices {
		errs[i] = applied[j]
	}
	return errs
}

// Move implements WAL-first ordering.
func (s *BinaryFileWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.write(walRecord{Op: "move", Move: req}); err != nil {
		return MoveResult{}, err
	}
	return s.MemoryStore.Move(ctx, req)
}

// PauseQueue implements WAL-first ordering.
// requireQueue validates a control-plane target before its WAL record is
// appended, so a typo'd name cannot leave a record that fails to replay.
func (s *BinaryFileWALStore) requireQueue(name string) error {
	if !s.MemoryStore.queueExists(name) {
		return ErrQueueNotFound
	}
	return nil
}

func (s *BinaryFileWALStore) PauseQueue(ctx context.Context, name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.requireQueue(name); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "pause_queue", QueueName: name}); err != nil {
		return err
	}
	return s.MemoryStore.PauseQueue(ctx, name)
}

// ResumeQueue implements WAL-first ordering.
func (s *BinaryFileWALStore) ResumeQueue(ctx context.Context, name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.requireQueue(name); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "resume_queue", QueueName: name}); err != nil {
		return err
	}
	return s.MemoryStore.ResumeQueue(ctx, name)
}

// PurgeQueue implements WAL-first ordering.
func (s *BinaryFileWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.requireQueue(name); err != nil {
		return 0, err
	}
	if err := s.write(walRecord{Op: "purge_queue", QueueName: name}); err != nil {
		return 0, err
	}
	return s.MemoryStore.PurgeQueue(ctx, name)
}

// CommitOffset implements WAL-first ordering.
func (s *BinaryFileWALStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	// WAL-first: persist before updating the in-memory offset map.
	if err := s.write(walRecord{Op: "commit_offset", Offset: persisted}); err != nil {
		return err
	}
	return s.MemoryStore.CommitOffset(context.Background(), persisted)
}

// SweepLeases implements WAL-first ordering: prepare the deterministic expiry
// set, persist it, then apply it. The promoted MemoryStore.SweepLeases mutates
// memory only, so without this override lease expirations are not durable.
func (s *BinaryFileWALStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	expired, err := s.MemoryStore.prepareSweepLeases(ctx, now)
	if err != nil || len(expired) == 0 {
		return 0, err
	}
	if err := s.write(walRecord{Op: "sweep_leases", Messages: expired}); err != nil {
		return 0, err
	}
	return s.MemoryStore.applySweepLeases(expired), nil
}

func (s *BinaryFileWALStore) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var joined error
	if s.gc != nil {
		joined = errors.Join(joined, s.gc.close())
	}
	joined = errors.Join(joined, s.MemoryStore.Close())
	if s.file != nil {
		joined = errors.Join(joined, s.file.Close())
	}
	return joined
}
