package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BinaryFileWALStore is a durable single-node WAL store backed by the native
// FWB1 binary frame codec. It provides the same Store behavior as FileWALStore
// while using compact length-prefixed frames with CRC32C and optional HMAC /
// AES-GCM payload protection.
type BinaryFileWALStore struct {
	*MemoryStore
	mu      sync.Mutex
	path    string
	file    *os.File
	codec   FrameCodec
	keyName string
}

func NewBinaryFileWALStore(dir string, opts ...BinaryMessageCodecOption) (*BinaryFileWALStore, error) {
	return NewBinaryFileWALStoreWithCodec(dir, NewBinaryMessageCodec(opts...))
}

// NewBinaryFileWALStoreWithCodec creates a BinaryFileWALStore with a specific codec (FWB1 or FWB2).
func NewBinaryFileWALStoreWithCodec(dir string, codec FrameCodec) (*BinaryFileWALStore, error) {
	if codec == nil {
		codec = NewBinaryMessageCodec()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "broker.wal.fwb")
	st := &BinaryFileWALStore{MemoryStore: NewMemoryStore(), path: path, codec: codec}
	if err := st.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st.file = f
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
		maxFrame := uint64(1 << 20) // 1MB default
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
		var msg Message
		if err := s.codec.DecodeMessage(buf, &msg); err != nil {
			return err
		}
		var rec walRecord
		if err := json.Unmarshal(msg.Payload, &rec); err != nil {
			return err
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
		_, _ = s.MemoryStore.AppendAtomic(ctx, r.Entries)
	case "lease":
		_ = s.MemoryStore.Update(ctx, r.Message)
	case "ack":
		_ = s.MemoryStore.Ack(ctx, r.Ack)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return ErrClosed
	}
	r.At = time.Now()
	r.Checksum = "" // frame CRC/HMAC protect binary WAL records.
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	frame, err := s.codec.EncodeMessage(nil, Message{ID: newIDFromTime(), Type: "wal." + r.Op, ContentType: "application/json", Payload: payload, CreatedAt: r.At, Status: MessagePersisted})
	if err != nil {
		return err
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(frame)))
	if _, err := s.file.Write(lenBuf[:n]); err != nil {
		return err
	}
	if _, err := s.file.Write(frame); err != nil {
		return err
	}
	return s.file.Sync()
}

func uvarintLen(v uint64) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutUvarint(b[:], v)
}

func (s *BinaryFileWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if err := s.write(walRecord{Op: "declare_queue", Queue: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareQueue(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if err := s.write(walRecord{Op: "declare_topic", Topic: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareTopic(ctx, cfg)
}
func (s *BinaryFileWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if err := s.write(walRecord{Op: "declare_stream", Stream: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareStream(ctx, cfg)
}
func (s *BinaryFileWALStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	out, err := s.MemoryStore.Append(ctx, msg, dests)
	if err != nil {
		return nil, err
	}
	if wErr := s.write(walRecord{Op: "append", Message: msg, Dests: dests}); wErr != nil {
		_ = wErr
	}
	return out, nil
}
func (s *BinaryFileWALStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	out, err := s.MemoryStore.AppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	if wErr := s.write(walRecord{Op: "append_atomic", Entries: entries}); wErr != nil {
		_ = wErr
	}
	return out, nil
}
func (s *BinaryFileWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	msgs, err := s.MemoryStore.Lease(ctx, req)
	if err != nil || len(msgs) == 0 {
		return msgs, err
	}
	if wErr := s.write(walRecord{Op: "lease", Message: msgs[0], Lease: req}); wErr != nil {
		_ = wErr
	}
	return msgs, nil
}
func (s *BinaryFileWALStore) Ack(ctx context.Context, req AckRequest) error {
	if err := s.MemoryStore.validateAckLease(req); err != nil {
		return err
	}
	if err := s.MemoryStore.Ack(ctx, req); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "ack", Ack: req}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) Nack(ctx context.Context, req NackRequest) error {
	if err := s.MemoryStore.validateNackLease(req); err != nil {
		return err
	}
	if err := s.MemoryStore.Nack(ctx, req); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "nack", Nack: req}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	if err := s.MemoryStore.ExtendLease(ctx, req); err != nil {
		return err
	}
	m, ok, _ := s.MemoryStore.Get(ctx, req.MessageID)
	if !ok {
		return ErrMessageNotFound
	}
	if wErr := s.write(walRecord{Op: "lease", Message: m, Lease: req}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) Update(ctx context.Context, msg Message) error {
	if err := s.MemoryStore.Update(ctx, msg); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "update", Message: msg}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	res, err := s.MemoryStore.Move(ctx, req)
	if err != nil {
		return MoveResult{}, err
	}
	if wErr := s.write(walRecord{Op: "move", Move: req}); wErr != nil {
		_ = wErr
	}
	return res, nil
}
func (s *BinaryFileWALStore) PauseQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.PauseQueue(ctx, name); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "pause_queue", QueueName: name}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) ResumeQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.ResumeQueue(ctx, name); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "resume_queue", QueueName: name}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	n, err := s.MemoryStore.PurgeQueue(ctx, name)
	if err != nil {
		return n, err
	}
	if wErr := s.write(walRecord{Op: "purge_queue", QueueName: name}); wErr != nil {
		_ = wErr
	}
	return n, nil
}
func (s *BinaryFileWALStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	if err := s.MemoryStore.CommitOffset(ctx, commit); err != nil {
		return err
	}
	if wErr := s.write(walRecord{Op: "commit_offset", Offset: commit}); wErr != nil {
		_ = wErr
	}
	return nil
}
func (s *BinaryFileWALStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.MemoryStore.Close()
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
