package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	SegmentMagic          uint32 = 0x42574C32 // "BWL2"
	SegmentVersion        byte   = 2
	SegmentTrailerMagic   uint32 = 0x434C4F53 // "CLOS"
	DefaultMaxSegmentSize        = 64 << 20   // 64 MiB
	DefaultMaxSegments           = 10
	DefaultCompactionInt         = 5 * time.Minute
)

// SegmentMeta tracks metadata about a single WAL segment.
type SegmentMeta struct {
	ID         uint64    `json:"id"`
	Filename   string    `json:"filename"`
	IndexFile  string    `json:"index_file"`
	SizeBytes  int64     `json:"size_bytes"`
	Records    uint64    `json:"records"`
	FirstSeq   uint64    `json:"first_seq"`
	LastSeq    uint64    `json:"last_seq"`
	CreatedAt  time.Time `json:"created_at"`
	ClosedAt   time.Time `json:"closed_at,omitempty"`
	CleanClose bool      `json:"clean_close"`
}

// SegmentedWALManifest tracks all active and archived segments in the log.
type SegmentedWALManifest struct {
	Version          int           `json:"version"`
	ActiveSegment    uint64        `json:"active_segment"`
	NextSeq          uint64        `json:"next_seq"`
	Segments         []SegmentMeta `json:"segments"`
	LastCompactionAt time.Time     `json:"last_compaction_at,omitempty"`
	CheckpointFile   string        `json:"checkpoint_file,omitempty"`
}

// SegmentedWALConfig configures the BWL2 segmented storage engine.
type SegmentedWALConfig struct {
	Dir                string
	MaxSegmentSize     int64
	MaxSegments        int
	CompactionInterval time.Duration
	Retention          time.Duration
	Codec              FrameCodec
	WALCodec           *WALRecordCodec
	DisableCompactor   bool
}

// SegmentedWALStore is a multi-segment, durable WAL store with manifest tracking,
// sparse indexing, active log compaction, and automatic migration from BWL1.
type SegmentedWALStore struct {
	*MemoryStore
	cfg          SegmentedWALConfig
	dir          string
	manifestPath string
	manifest     SegmentedWALManifest

	activeFile *os.File
	activeMeta *SegmentMeta
	activeIdx  *SparseIndex
	gc         *groupCommitWriter

	walCodec    *WALRecordCodec
	legacyCodec FrameCodec

	compactor *SegmentCompactor

	writeMu   sync.Mutex
	onFaultFn func(error)
}

func (*SegmentedWALStore) ConfirmationLevel() ConfirmLevel { return ConfirmPersisted }

func (s *SegmentedWALStore) StoreHealth() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.gc == nil {
		return ErrClosed
	}
	return s.gc.healthError()
}

func (s *SegmentedWALStore) SetHealthCallback(fn func(error)) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.onFaultFn = fn
	if s.gc != nil {
		s.gc.setFaultCallback(fn)
	}
}

// NewSegmentedWALStore opens or initializes a segmented BWL2 store in the given directory.
func NewSegmentedWALStore(cfg SegmentedWALConfig) (*SegmentedWALStore, error) {
	if cfg.Dir == "" {
		return nil, errors.New("segmented WAL directory is required")
	}
	if cfg.MaxSegmentSize <= 0 {
		cfg.MaxSegmentSize = DefaultMaxSegmentSize
	}
	if cfg.MaxSegments <= 0 {
		cfg.MaxSegments = DefaultMaxSegments
	}
	if cfg.CompactionInterval <= 0 {
		cfg.CompactionInterval = DefaultCompactionInt
	}
	if cfg.LegacyCodec() == nil {
		cfg.Codec = NewBinaryMessageCodec()
	}
	if cfg.WALCodec == nil {
		wc, err := newWALRecordCodecFromFrameCodec(cfg.Codec)
		if err != nil {
			return nil, err
		}
		cfg.WALCodec = wc
	}

	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}

	st := &SegmentedWALStore{
		MemoryStore:  NewMemoryStore(),
		cfg:          cfg,
		dir:          cfg.Dir,
		manifestPath: filepath.Join(cfg.Dir, "manifest.json"),
		walCodec:     cfg.WALCodec,
		legacyCodec:  cfg.Codec,
	}

	if err := st.recoverOrMigrate(); err != nil {
		return nil, fmt.Errorf("recover segmented WAL: %w", err)
	}

	if !cfg.DisableCompactor {
		st.compactor = newSegmentCompactor(st)
		st.compactor.Start()
	}

	return st, nil
}

func (cfg SegmentedWALConfig) LegacyCodec() FrameCodec { return cfg.Codec }

func (s *SegmentedWALStore) recoverOrMigrate() error {
	// Check if manifest.json exists
	if _, err := os.Stat(s.manifestPath); err == nil {
		// Load manifest
		if err := s.loadManifest(); err != nil {
			return err
		}
		if err := s.replaySegments(); err != nil {
			return err
		}
		s.cleanupOrphanFilesLocked()
		return nil
	}

	// Manifest does not exist. Check for legacy BWL1 or JSONL files to migrate
	legacyFWB := filepath.Join(s.dir, "broker.wal.fwb")
	legacyJSON := filepath.Join(s.dir, "broker.wal.jsonl")

	if _, err := os.Stat(legacyFWB); err == nil {
		return s.migrateLegacyFile(legacyFWB, true)
	}
	if _, err := os.Stat(legacyJSON); err == nil {
		return s.migrateLegacyFile(legacyJSON, false)
	}

	// Brand new store: initialize manifest and first segment
	s.manifest = SegmentedWALManifest{
		Version:       2,
		ActiveSegment: 1,
		NextSeq:       1,
		Segments:      nil,
	}
	return s.initActiveSegment(1, 1)
}

func (s *SegmentedWALStore) loadManifest() error {
	data, err := os.ReadFile(s.manifestPath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.manifest)
}

// syncSegmentMetaLocked writes meta into the manifest's Segments slice,
// replacing any existing entry with the same ID or appending a new one.
// Without this, in-memory mutations to activeMeta (size/record counts on
// every write, ClosedAt/CleanClose on rotation) would never reach the
// persisted manifest, which silently breaks anything that reads
// manifest.Segments: retention-based pruning (it keys off ClosedAt), and
// external tooling inspecting manifest.json.
func (s *SegmentedWALStore) syncSegmentMetaLocked(meta SegmentMeta) {
	for i := range s.manifest.Segments {
		if s.manifest.Segments[i].ID == meta.ID {
			s.manifest.Segments[i] = meta
			return
		}
	}
	s.manifest.Segments = append(s.manifest.Segments, meta)
}

func (s *SegmentedWALStore) saveManifestLocked() error {
	if s.activeMeta != nil {
		s.syncSegmentMetaLocked(*s.activeMeta)
	}
	data, err := json.MarshalIndent(s.manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.manifestPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.manifestPath)
}

func (s *SegmentedWALStore) initActiveSegment(segID uint64, startSeq uint64) error {
	filename := fmt.Sprintf("%020d.bwl2", segID)
	indexName := fmt.Sprintf("%020d.idx", segID)
	segPath := filepath.Join(s.dir, filename)

	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	meta := SegmentMeta{
		ID:        segID,
		Filename:  filename,
		IndexFile: indexName,
		SizeBytes: info.Size(),
		FirstSeq:  startSeq,
		LastSeq:   startSeq,
		CreatedAt: time.Now().UTC(),
	}

	// If file is newly created (size 0), write BWL2 segment header:
	// [Magic:4][Version:1][Flags:1][SegmentID:8][CreatedAtNano:8] = 22 bytes
	if info.Size() == 0 {
		var h [22]byte
		binary.BigEndian.PutUint32(h[0:4], SegmentMagic)
		h[4] = SegmentVersion
		h[5] = 0 // Flags
		binary.BigEndian.PutUint64(h[6:14], segID)
		binary.BigEndian.PutUint64(h[14:22], uint64(meta.CreatedAt.UnixNano()))
		if _, err := f.Write(h[:]); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		meta.SizeBytes = 22
	}

	s.activeFile = f
	s.activeMeta = &meta
	s.activeIdx = NewSparseIndex(defaultIndexStep)
	s.gc = newGroupCommitWriter(f)
	if s.onFaultFn != nil {
		s.gc.setFaultCallback(s.onFaultFn)
	}

	// Update manifest. saveManifestLocked syncs s.activeMeta (the new
	// segment, just assigned above) into manifest.Segments automatically.
	s.manifest.ActiveSegment = segID
	return s.saveManifestLocked()
}

func (s *SegmentedWALStore) replaySegments() error {
	ctx := context.Background()

	// If checkpoint file exists, restore baseline memory state first
	if s.manifest.CheckpointFile != "" {
		chkPath := filepath.Join(s.dir, s.manifest.CheckpointFile)
		if data, err := os.ReadFile(chkPath); err == nil {
			var snap MemoryStoreSnapshot
			if err := json.Unmarshal(data, &snap); err == nil {
				s.MemoryStore.Restore(snap)
			}
		}
	}

	// Replay all segments recorded in manifest in ID order
	sort.Slice(s.manifest.Segments, func(i, j int) bool {
		return s.manifest.Segments[i].ID < s.manifest.Segments[j].ID
	})

	for i := range s.manifest.Segments {
		meta := &s.manifest.Segments[i]
		segPath := filepath.Join(s.dir, meta.Filename)
		isActive := (meta.ID == s.manifest.ActiveSegment)

		records, validBytes, err := s.replaySegmentFile(ctx, segPath, meta, isActive)
		if err != nil {
			return fmt.Errorf("replay segment %s: %w", meta.Filename, err)
		}
		meta.Records = records
		meta.SizeBytes = validBytes
	}

	s.normalizeRecoveredInflight()

	// Open the active segment for appending
	activeID := s.manifest.ActiveSegment
	if activeID == 0 {
		activeID = 1
	}
	return s.initActiveSegment(activeID, s.manifest.NextSeq)
}

func (s *SegmentedWALStore) replaySegmentFile(ctx context.Context, path string, meta *SegmentMeta, isActive bool) (uint64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// A segment listed in the manifest with no file on disk is data
			// loss, not an empty segment: the manifest is only ever updated
			// to drop a segment reference *after* that segment's file is
			// safely superseded by a checkpoint (see SegmentCompactor.Compact).
			// Silently treating a missing file as "0 records" would mask
			// whatever it actually contained.
			return 0, 0, fmt.Errorf("%w: segment file %s is missing but referenced by manifest.json; run broker-wal -dir=%s to diagnose", ErrStorageUnavailable, path, s.dir)
		}
		return 0, 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var header [22]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if isActive {
				return 0, 0, nil
			}
			return 0, 0, fmt.Errorf("%w: segment file %s is truncated below its header but is not the active segment", ErrStorageUnavailable, path)
		}
		return 0, 0, err
	}

	if binary.BigEndian.Uint32(header[0:4]) != SegmentMagic {
		return 0, 0, errors.New("invalid BWL2 segment magic")
	}

	var pos int64 = 22
	var records uint64 = 0

	for {
		recPos := pos
		l, err := binary.ReadUvarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return records, pos, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Torn write at tail
				if isActive {
					_ = os.Truncate(path, pos)
				}
				return records, pos, nil
			}
			return records, pos, err
		}

		ulen := int64(uvarintLen(l))
		pos += ulen

		maxFrame := uint64(s.walCodec.maxRecordSize)
		if maxFrame < 1<<20 {
			maxFrame = 1 << 20
		}
		if l == 0 || l > maxFrame {
			if isActive {
				_ = os.Truncate(path, recPos)
			}
			return records, recPos, nil
		}

		buf := make([]byte, int(l))
		if _, err := io.ReadFull(r, buf); err != nil {
			if isActive {
				_ = os.Truncate(path, recPos)
			}
			return records, recPos, nil
		}
		pos += int64(l)

		var rec walRecord
		if isNativeBinaryWALRecord(buf) {
			if err := s.walCodec.decode(buf, &rec); err != nil {
				if isActive {
					_ = os.Truncate(path, recPos)
					return records, recPos, nil
				}
				return records, recPos, err
			}
		} else {
			// Legacy fallback within segment
			var msg Message
			if err := s.legacyCodec.DecodeMessage(buf, &msg); err == nil {
				_ = json.Unmarshal(msg.Payload, &rec)
			}
		}

		s.applyRecovered(rec)
		records++
		if s.manifest.NextSeq <= records {
			s.manifest.NextSeq = records + 1
		}
	}
}

// cleanupOrphanFilesLocked removes *.bwl2/*.idx files that are not referenced
// by the manifest and are not the active segment. These only appear if the
// process crashed between saving a pruned manifest and deleting the segment
// files it just pruned (see SegmentCompactor.Compact) — they hold no data
// reachable through the manifest, so leaving them around only wastes disk.
func (s *SegmentedWALStore) cleanupOrphanFilesLocked() {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	known := make(map[string]bool, len(s.manifest.Segments)*2)
	for _, seg := range s.manifest.Segments {
		known[seg.Filename] = true
		if seg.IndexFile != "" {
			known[seg.IndexFile] = true
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || known[name] {
			continue
		}
		ext := filepath.Ext(name)
		if ext != ".bwl2" && ext != ".idx" {
			continue
		}
		_ = os.Remove(filepath.Join(s.dir, name))
	}
}

func (s *SegmentedWALStore) migrateLegacyFile(legacyPath string, isFWB bool) error {
	s.manifest = SegmentedWALManifest{
		Version:       2,
		ActiveSegment: 1,
		NextSeq:       1,
		Segments:      nil,
	}

	// Create segment 1
	if err := s.initActiveSegment(1, 1); err != nil {
		return err
	}

	// Read and migrate records. Migration is deliberately fail-closed: the
	// legacy file is not renamed unless every record has been decoded, verified,
	// applied, and synced into BWL2.
	f, err := os.Open(legacyPath)
	if err != nil {
		s.abortLegacyMigration()
		return err
	}
	defer f.Close()

	var migrateErr error
	if isFWB {
		r := bufio.NewReaderSize(f, 1<<20)
		for {
			l, err := binary.ReadUvarint(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				migrateErr = fmt.Errorf("read legacy BWL1 record length: %w", err)
				break
			}
			if l == 0 || l > uint64(s.walCodec.maxRecordSize) {
				migrateErr = fmt.Errorf("legacy BWL1 record length %d is invalid", l)
				break
			}
			buf := make([]byte, int(l))
			if _, err := io.ReadFull(r, buf); err != nil {
				migrateErr = fmt.Errorf("read legacy BWL1 record: %w", err)
				break
			}
			var rec walRecord
			if isNativeBinaryWALRecord(buf) {
				if err := s.walCodec.decode(buf, &rec); err != nil {
					migrateErr = fmt.Errorf("decode legacy BWL1 record: %w", err)
					break
				}
			} else {
				var msg Message
				if err := s.legacyCodec.DecodeMessage(buf, &msg); err != nil {
					migrateErr = fmt.Errorf("decode legacy framed WAL record: %w", err)
					break
				}
				if err := json.Unmarshal(msg.Payload, &rec); err != nil {
					migrateErr = fmt.Errorf("decode legacy framed WAL payload: %w", err)
					break
				}
			}
			s.applyRecovered(rec)
			if err := s.writeRecordLocked(rec); err != nil {
				migrateErr = fmt.Errorf("write migrated BWL1 record: %w", err)
				break
			}
		}
	} else {
		r := bufio.NewReaderSize(f, 1<<20)
		lineNo := 0
		for {
			line, readErr := r.ReadBytes('\n')
			lineNo++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				if bytes.HasPrefix(trimmed, []byte("ENC:")) {
					migrateErr = fmt.Errorf("legacy JSON WAL line %d is encrypted; migrate it with the key-aware broker-wal tool", lineNo)
					break
				}
				var rec walRecord
				if err := json.Unmarshal(trimmed, &rec); err != nil {
					migrateErr = fmt.Errorf("decode legacy JSON WAL line %d: %w", lineNo, err)
					break
				}
				if err := verifyWALRecord(rec); err != nil {
					migrateErr = fmt.Errorf("verify legacy JSON WAL line %d: %w", lineNo, err)
					break
				}
				s.applyRecovered(rec)
				if err := s.writeRecordLocked(rec); err != nil {
					migrateErr = fmt.Errorf("write migrated JSON WAL line %d: %w", lineNo, err)
					break
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				migrateErr = fmt.Errorf("read legacy JSON WAL line %d: %w", lineNo, readErr)
				break
			}
		}
	}

	if migrateErr != nil {
		s.abortLegacyMigration()
		return migrateErr
	}

	backupPath := legacyPath + ".migrated"
	if _, err := os.Stat(backupPath); err == nil {
		s.abortLegacyMigration()
		return fmt.Errorf("legacy WAL backup already exists: %s", backupPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		s.abortLegacyMigration()
		return fmt.Errorf("check legacy WAL backup: %w", err)
	}
	if err := os.Rename(legacyPath, backupPath); err != nil {
		s.abortLegacyMigration()
		return fmt.Errorf("preserve migrated legacy WAL: %w", err)
	}
	return nil
}

// abortLegacyMigration removes only the newly-created BWL2 files. The source
// legacy WAL remains untouched so a failed migration is always retryable.
func (s *SegmentedWALStore) abortLegacyMigration() {
	if s.gc != nil {
		_ = s.gc.close()
		s.gc = nil
	}
	if s.activeFile != nil {
		_ = s.activeFile.Close()
		s.activeFile = nil
	}
	for _, segment := range s.manifest.Segments {
		_ = os.Remove(filepath.Join(s.dir, segment.Filename))
		if segment.IndexFile != "" {
			_ = os.Remove(filepath.Join(s.dir, segment.IndexFile))
		}
	}
	_ = os.Remove(s.manifestPath)
}

func (s *SegmentedWALStore) normalizeRecoveredInflight() {
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

func (s *SegmentedWALStore) applyRecovered(r walRecord) {
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

func (s *SegmentedWALStore) rotateSegmentLocked() error {
	if s.gc != nil {
		_ = s.gc.close()
	}

	if s.activeMeta != nil {
		s.activeMeta.ClosedAt = time.Now().UTC()
		s.activeMeta.CleanClose = true
		if s.activeIdx != nil {
			idxPath := filepath.Join(s.dir, s.activeMeta.IndexFile)
			_ = s.activeIdx.SaveToFile(idxPath)
		}
		// Persist the closed segment's final metadata now, while s.activeMeta
		// still refers to it — initActiveSegment below reassigns s.activeMeta
		// to the new segment, after which this one would no longer be reachable.
		s.syncSegmentMetaLocked(*s.activeMeta)
	}
	if s.activeFile != nil {
		_ = s.activeFile.Close()
	}

	nextSegID := s.manifest.ActiveSegment + 1
	startSeq := s.manifest.NextSeq
	return s.initActiveSegment(nextSegID, startSeq)
}

// writeRecordAsync enqueues a record for group commit and returns without waiting
// for the fsync. The caller must hold writeMu, so records reach the log in enqueue
// order, and must call gc.await on the result only after releasing it.
//
// Every operation in this store used to await its own Sync while still holding
// writeMu, which meant one fsync per operation, fully serialized: the group-commit
// writer could not coalesce anything, because no second operation could reach it
// until the first Sync returned. Releasing the lock before the wait is what lets
// concurrent appends, leases and acks share a single Sync.
func (s *SegmentedWALStore) writeRecordAsync(r walRecord) (*groupCommitReq, error) {
	if s.activeFile == nil || s.gc == nil {
		return nil, ErrClosed
	}

	// Check if active segment exceeded maxSegmentSize
	if s.activeMeta != nil && s.activeMeta.SizeBytes >= s.cfg.MaxSegmentSize {
		if err := s.rotateSegmentLocked(); err != nil {
			return nil, fmt.Errorf("rotate segment: %w", err)
		}
	}

	r.At = time.Now()
	record, err := s.walCodec.encode(nil, r)
	if err != nil {
		return nil, err
	}

	recLen := int64(len(record))
	ulen := int64(uvarintLen(uint64(recLen)))
	totalBytes := ulen + recLen

	req, err := s.gc.enqueue(record, true)
	if err != nil {
		return nil, err
	}

	seq := s.manifest.NextSeq
	s.manifest.NextSeq++

	if s.activeMeta != nil {
		offset := s.activeMeta.SizeBytes
		s.activeMeta.SizeBytes += totalBytes
		s.activeMeta.Records++
		s.activeMeta.LastSeq = seq
		if s.activeIdx != nil {
			s.activeIdx.Add(seq, offset, r.At.UnixNano())
		}
	}

	return req, nil
}

// writeRecordLocked is the synchronous form for callers that already hold
// writeMu, which is what the name means and what the control-plane callers rely
// on. It must not take the lock itself: sync.Mutex is not reentrant, so doing so
// deadlocks every Declare/Pause/Resume/Purge caller.
func (s *SegmentedWALStore) writeRecordLocked(r walRecord) error {
	req, err := s.writeRecordAsync(r)
	if err != nil {
		return err
	}
	return s.gc.await(req)
}

func (s *SegmentedWALStore) write(r walRecord) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeRecordLocked(r)
}

// ---------------- Store Interface Implementation (WAL-First) ----------------

func (s *SegmentedWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "declare_queue", Queue: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareQueue(ctx, cfg)
}

func (s *SegmentedWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "declare_topic", Topic: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareTopic(ctx, cfg)
}

func (s *SegmentedWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "declare_stream", Stream: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareStream(ctx, cfg)
}

func (s *SegmentedWALStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	return s.durableAppend(ctx, []AppendEntry{{Message: msg, Destinations: dests}})
}

func (s *SegmentedWALStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	return s.durableAppend(ctx, entries)
}

func (s *SegmentedWALStore) AppendAtomicMinimal(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}

func (s *SegmentedWALStore) AppendAtomicOwned(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}

func (s *SegmentedWALStore) durableAppend(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	s.writeMu.Lock()
	out, err := s.MemoryStore.prepareAppendAtomic(ctx, entries)
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	persisted := storedAppendEntries(out)
	req, err := s.writeRecordAsync(walRecord{Op: "append_atomic", Entries: persisted})
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	// Apply while still holding writeMu, but hidden from readers, so the next
	// append validates against it. The fsync wait happens after the unlock.
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

func (s *SegmentedWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	s.writeMu.Lock()
	msgs, err := s.MemoryStore.prepareLease(ctx, req)
	if err != nil || len(msgs) == 0 {
		s.writeMu.Unlock()
		return msgs, err
	}
	wreq, err := s.writeRecordAsync(walRecord{Op: "lease_batch", Messages: msgs, Lease: req})
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	applyErr := s.MemoryStore.applyLease(ctx, msgs, req)
	s.writeMu.Unlock()

	if err := s.gc.await(wreq); err != nil {
		return nil, err
	}
	if applyErr != nil {
		return nil, applyErr
	}
	return msgs, nil
}

func (s *SegmentedWALStore) Ack(ctx context.Context, req AckRequest) error {
	s.writeMu.Lock()
	if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
		s.writeMu.Unlock()
		return err
	}
	wreq, err := s.writeRecordAsync(walRecord{Op: "ack", Ack: req})
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	applyErr := s.MemoryStore.Ack(ctx, req)
	s.writeMu.Unlock()

	if err := s.gc.await(wreq); err != nil {
		return err
	}
	return applyErr
}

func (s *SegmentedWALStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	errs := make([]error, len(reqs))
	valid := make([]AckRequest, 0, len(reqs))
	for i, req := range reqs {
		if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
			errs[i] = err
		} else {
			valid = append(valid, req)
		}
	}
	if len(valid) > 0 {
		if err := s.writeRecordLocked(walRecord{Op: "ack_batch", Acks: valid}); err != nil {
			for i := range errs {
				if errs[i] == nil {
					errs[i] = err
				}
			}
			return errs
		}
		appErrs := s.MemoryStore.AckBatch(ctx, valid)
		vIdx := 0
		for i := range errs {
			if errs[i] == nil {
				errs[i] = appErrs[vIdx]
				vIdx++
			}
		}
	}
	return errs
}

func (s *SegmentedWALStore) Nack(ctx context.Context, req NackRequest) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.MemoryStore.validateNackLease(ctx, req); err != nil {
		return err
	}
	if err := s.writeRecordLocked(walRecord{Op: "nack", Nack: req}); err != nil {
		return err
	}
	return s.MemoryStore.Nack(ctx, req)
}

func (s *SegmentedWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	m, err := s.MemoryStore.validateExtendLease(ctx, req)
	if err != nil {
		return err
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	m.LeaseUntil = time.Now().Add(req.VisibilityTimeout)
	if err := s.writeRecordLocked(walRecord{Op: "lease", Message: m, Lease: req}); err != nil {
		return err
	}
	return s.MemoryStore.ExtendLease(ctx, req)
}

func (s *SegmentedWALStore) Update(ctx context.Context, msg Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "update", Message: msg}); err != nil {
		return err
	}
	return s.MemoryStore.Update(ctx, msg)
}

func (s *SegmentedWALStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	errs := make([]error, len(messages))
	if err := s.writeRecordLocked(walRecord{Op: "update_batch", Messages: messages}); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	return s.MemoryStore.UpdateBatch(ctx, messages)
}

func (s *SegmentedWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "move", Move: req}); err != nil {
		return MoveResult{}, err
	}
	return s.MemoryStore.Move(ctx, req)
}

func (s *SegmentedWALStore) PauseQueue(ctx context.Context, name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "pause_queue", QueueName: name}); err != nil {
		return err
	}
	return s.MemoryStore.PauseQueue(ctx, name)
}

func (s *SegmentedWALStore) ResumeQueue(ctx context.Context, name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "resume_queue", QueueName: name}); err != nil {
		return err
	}
	return s.MemoryStore.ResumeQueue(ctx, name)
}

func (s *SegmentedWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "purge_queue", QueueName: name}); err != nil {
		return 0, err
	}
	return s.MemoryStore.PurgeQueue(ctx, name)
}

func (s *SegmentedWALStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeRecordLocked(walRecord{Op: "commit_offset", Offset: commit}); err != nil {
		return err
	}
	return s.MemoryStore.CommitOffset(ctx, commit)
}

func (s *SegmentedWALStore) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var joined error
	if s.compactor != nil {
		s.compactor.Stop()
	}
	if s.gc != nil {
		joined = errors.Join(joined, s.gc.close())
	}
	if s.activeMeta != nil && s.activeIdx != nil {
		idxPath := filepath.Join(s.dir, s.activeMeta.IndexFile)
		joined = errors.Join(joined, s.activeIdx.SaveToFile(idxPath))
	}
	if s.activeFile != nil {
		joined = errors.Join(joined, s.activeFile.Close())
	}
	joined = errors.Join(joined, s.saveManifestLocked())
	return joined
}
