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
	"strconv"
	"strings"
	"time"
)

// BinaryFileWALStore is a durable single-node WAL store backed by the native
// FWB1 binary frame codec. It provides the same Store behavior as FileWALStore
// while using compact length-prefixed frames with CRC32C and optional HMAC /
// AES-GCM payload protection.
//
// The WAL is split across a fixed number of shard files (like Kafka
// partitions on disk), each with its own independent group-commit writer
// goroutine. A record is routed to a shard by hashing the queue/topic/stream
// it belongs to, so all operations on one queue always land in the same
// shard file (preserving per-queue order) while operations on different
// queues fsync in parallel instead of funneling through one writer. The
// shard count is pinned to the WAL directory the first time it's created
// (recorded in broker.wal.shards) so it can never silently change across
// restarts in a way that would split one queue's history across files out
// of order.
type BinaryFileWALStore struct {
	*MemoryStore
	dir     string
	codec   FrameCodec
	keyName string
	shards  []*binaryWALShard
}

type binaryWALShard struct {
	file *os.File
	gc   *groupCommitWriter
}

// DefaultWALShards is the shard count used when a caller doesn't specify
// one (0 or negative). It's a conservative default that still gets most of
// the cross-queue parallelism benefit without opening excessive file
// descriptors on small deployments.
const DefaultWALShards = 4

func NewBinaryFileWALStore(dir string, opts ...BinaryMessageCodecOption) (*BinaryFileWALStore, error) {
	return NewBinaryFileWALStoreWithCodec(dir, NewBinaryMessageCodec(opts...))
}

// NewBinaryFileWALStoreWithCodec creates a BinaryFileWALStore with a specific codec (FWB1 or FWB2).
func NewBinaryFileWALStoreWithCodec(dir string, codec FrameCodec) (*BinaryFileWALStore, error) {
	return NewBinaryFileWALStoreSharded(dir, DefaultWALShards, codec)
}

// NewBinaryFileWALStoreSharded creates a BinaryFileWALStore whose WAL is
// partitioned across n shard files. n is only honored the first time the
// directory is initialized; subsequent opens reuse whatever shard count was
// pinned on creation, regardless of what's requested here.
func NewBinaryFileWALStoreSharded(dir string, n int, codec FrameCodec) (*BinaryFileWALStore, error) {
	if codec == nil {
		codec = NewBinaryMessageCodec()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	st := &BinaryFileWALStore{MemoryStore: NewMemoryStore(), dir: dir, codec: codec}
	resolved, err := st.resolveShardCount(n)
	if err != nil {
		return nil, err
	}
	if err := st.recoverAll(resolved); err != nil {
		return nil, err
	}
	st.shards = make([]*binaryWALShard, resolved)
	for i := 0; i < resolved; i++ {
		f, err := os.OpenFile(st.shardPath(i, resolved), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		st.shards[i] = &binaryWALShard{file: f, gc: newGroupCommitWriter(f)}
	}
	return st, nil
}

// resolveShardCount pins the shard count for a WAL directory on first use
// and returns the pinned value forever after, ignoring the requested count
// on later opens so per-queue ordering across restarts is never at risk.
func (s *BinaryFileWALStore) resolveShardCount(requested int) (int, error) {
	marker := filepath.Join(s.dir, "broker.wal.shards")
	if data, err := os.ReadFile(marker); err == nil {
		if n, perr := strconv.Atoi(strings.TrimSpace(string(data))); perr == nil && n > 0 {
			return n, nil
		}
	}
	if requested <= 0 {
		requested = 1
	}
	if err := os.WriteFile(marker, []byte(strconv.Itoa(requested)), 0o600); err != nil {
		return 0, err
	}
	return requested, nil
}

func (s *BinaryFileWALStore) shardPath(i, n int) string {
	return filepath.Join(s.dir, fmt.Sprintf("broker.wal.%d-of-%d.fwb", i, n))
}

// recoverAll replays the legacy pre-sharding single WAL file (upgrade path)
// followed by every current shard file.
func (s *BinaryFileWALStore) recoverAll(n int) error {
	if err := s.recoverFile(filepath.Join(s.dir, "broker.wal.fwb")); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if err := s.recoverFile(s.shardPath(i, n)); err != nil {
			return err
		}
	}
	s.normalizeRecoveredInflight()
	return nil
}

func (s *BinaryFileWALStore) recoverFile(path string) error {
	f, err := os.Open(path)
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
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				_ = os.Truncate(path, pos)
				return nil
			}
			return err
		}
		pos += int64(uvarintLen(l))
		maxFrame := uint64(1 << 20) // 1MB default
		if l == 0 || l > maxFrame {
			_ = os.Truncate(path, pos-int64(uvarintLen(l)))
			return nil
		}
		buf := make([]byte, int(l))
		if _, err := io.ReadFull(r, buf); err != nil {
			_ = os.Truncate(path, pos-int64(uvarintLen(l)))
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

func (s *BinaryFileWALStore) encodeRecord(r walRecord) ([]byte, error) {
	r.At = time.Now()
	r.Checksum = "" // frame CRC/HMAC protect binary WAL records.
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	frame, err := s.codec.EncodeMessage(nil, Message{ID: newIDFromTime(), Type: "wal." + r.Op, ContentType: "application/json", Payload: payload, CreatedAt: r.At, Status: MessagePersisted})
	if err != nil {
		return nil, err
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(frame)))
	buf := make([]byte, 0, n+len(frame))
	buf = append(buf, lenBuf[:n]...)
	buf = append(buf, frame...)
	return buf, nil
}

func (s *BinaryFileWALStore) shardIndex(r walRecord) int {
	return walShardIndex(r, len(s.shards))
}

func (s *BinaryFileWALStore) write(r walRecord) error {
	if len(s.shards) == 0 {
		return ErrClosed
	}
	buf, err := s.encodeRecord(r)
	if err != nil {
		return err
	}
	return s.shards[s.shardIndex(r)].gc.write(buf)
}

// writeBatch groups records by shard and fsyncs each shard's group once, in
// parallel across shards, instead of one fsync per record.
func (s *BinaryFileWALStore) writeBatch(records []walRecord) error {
	if len(s.shards) == 0 {
		return ErrClosed
	}
	if len(records) == 0 {
		return nil
	}
	byShard := make(map[int][]byte, len(s.shards))
	for _, r := range records {
		buf, err := s.encodeRecord(r)
		if err != nil {
			return err
		}
		idx := s.shardIndex(r)
		byShard[idx] = append(byShard[idx], buf...)
	}
	errCh := make(chan error, len(byShard))
	for idx, buf := range byShard {
		sh := s.shards[idx]
		buf := buf
		go func() { errCh <- sh.gc.write(buf) }()
	}
	var firstErr error
	for range byShard {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

// AppendBatch appends many independent entries via a single in-memory lock
// and, per destination shard, a single WAL fsync — while preserving
// per-entry error isolation (one bad entry doesn't fail the others).
func (s *BinaryFileWALStore) AppendBatch(ctx context.Context, entries []AppendEntry) []BatchAppendResult {
	results := s.MemoryStore.AppendBatch(ctx, entries)
	records := make([]walRecord, 0, len(entries))
	for i, e := range entries {
		if results[i].Err != nil {
			continue
		}
		records = append(records, walRecord{Op: "append", Message: e.Message, Dests: e.Destinations})
	}
	if len(records) > 0 {
		if wErr := s.writeBatch(records); wErr != nil {
			_ = wErr
		}
	}
	return results
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
	for _, sh := range s.shards {
		if sh.gc != nil {
			_ = sh.gc.close()
		}
	}
	_ = s.MemoryStore.Close()
	var firstErr error
	for _, sh := range s.shards {
		if sh.file != nil {
			if err := sh.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
