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
	}
}
func (s *BinaryFileWALStore) write(r walRecord) error {
	if s.file == nil || s.gc == nil {
		return ErrClosed
	}
	r.At = time.Now()
	r.Checksum = "" // BWL1 CRC32C/HMAC protect the complete native record.
	record, err := s.walCodec.encode(nil, r)
	if err != nil {
		return err
	}
	return s.gc.writeUvarintPrefixed(record)
}

func uvarintLen(v uint64) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutUvarint(b[:], v)
}

func (s *BinaryFileWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "queue"); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "declare_queue", Queue: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareQueue(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "topic"); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "declare_topic", Topic: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareTopic(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "stream"); err != nil {
		return err
	}
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
	defer s.writeMu.Unlock()
	out, err := s.MemoryStore.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	persisted := storedAppendEntries(out)
	if err := s.write(walRecord{Op: "append_atomic", Entries: persisted}); err != nil {
		return nil, err
	}
	if err := s.MemoryStore.appendRecoveredAtomic(persisted); err != nil {
		return nil, fmt.Errorf("apply durable append: %w", err)
	}
	return out, nil
}
func (s *BinaryFileWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	msgs, err := s.MemoryStore.Lease(ctx, req)
	if err != nil || len(msgs) == 0 {
		return msgs, err
	}
	if err := s.write(walRecord{Op: "lease_batch", Messages: msgs, Lease: req}); err != nil {
		return msgs, err
	}
	return msgs, nil
}
func (s *BinaryFileWALStore) Ack(ctx context.Context, req AckRequest) error {
	if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
		return err
	}
	if err := s.MemoryStore.Ack(ctx, req); err != nil {
		return err
	}
	return s.write(walRecord{Op: "ack", Ack: req})
}
func (s *BinaryFileWALStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	errs := s.MemoryStore.AckBatch(ctx, reqs)
	successful := make([]AckRequest, 0, len(reqs))
	indices := make([]int, 0, len(reqs))
	for i, err := range errs {
		if err == nil {
			successful = append(successful, reqs[i])
			indices = append(indices, i)
		}
	}
	if len(successful) == 0 {
		return errs
	}
	if err := s.write(walRecord{Op: "ack_batch", Acks: successful}); err != nil {
		for _, i := range indices {
			errs[i] = err
		}
	}
	return errs
}
func (s *BinaryFileWALStore) Nack(ctx context.Context, req NackRequest) error {
	if err := s.MemoryStore.validateNackLease(ctx, req); err != nil {
		return err
	}
	if err := s.MemoryStore.Nack(ctx, req); err != nil {
		return err
	}
	return s.write(walRecord{Op: "nack", Nack: req})
}
func (s *BinaryFileWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	if err := s.MemoryStore.ExtendLease(ctx, req); err != nil {
		return err
	}
	m, ok, err := s.MemoryStore.Get(ctx, req.MessageID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrMessageNotFound
	}
	return s.write(walRecord{Op: "lease", Message: m, Lease: req})
}
func (s *BinaryFileWALStore) Update(ctx context.Context, msg Message) error {
	if err := s.MemoryStore.Update(ctx, msg); err != nil {
		return err
	}
	return s.write(walRecord{Op: "update", Message: msg})
}
func (s *BinaryFileWALStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	errs := s.MemoryStore.UpdateBatch(ctx, messages)
	successful := make([]Message, 0, len(messages))
	indices := make([]int, 0, len(messages))
	for i, err := range errs {
		if err == nil {
			successful = append(successful, messages[i])
			indices = append(indices, i)
		}
	}
	if len(successful) == 0 {
		return errs
	}
	if err := s.write(walRecord{Op: "update_batch", Messages: successful}); err != nil {
		for _, i := range indices {
			errs[i] = err
		}
	}
	return errs
}
func (s *BinaryFileWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	res, err := s.MemoryStore.Move(ctx, req)
	if err != nil {
		return MoveResult{}, err
	}
	if err := s.write(walRecord{Op: "move", Move: req}); err != nil {
		return res, err
	}
	return res, nil
}
func (s *BinaryFileWALStore) PauseQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.PauseQueue(ctx, name); err != nil {
		return err
	}
	return s.write(walRecord{Op: "pause_queue", QueueName: name})
}
func (s *BinaryFileWALStore) ResumeQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.ResumeQueue(ctx, name); err != nil {
		return err
	}
	return s.write(walRecord{Op: "resume_queue", QueueName: name})
}
func (s *BinaryFileWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	n, err := s.MemoryStore.PurgeQueue(ctx, name)
	if err != nil {
		return n, err
	}
	if err := s.write(walRecord{Op: "purge_queue", QueueName: name}); err != nil {
		return n, err
	}
	return n, nil
}
func (s *BinaryFileWALStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	if err := s.MemoryStore.CommitOffset(context.Background(), persisted); err != nil {
		return err
	}
	return s.write(walRecord{Op: "commit_offset", Offset: persisted})
}

func (s *BinaryFileWALStore) Close() error {
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
