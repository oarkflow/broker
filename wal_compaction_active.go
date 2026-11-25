package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SegmentCompactor runs active log compaction and segment retention in the background.
type SegmentCompactor struct {
	store    *SegmentedWALStore
	stopCh   chan struct{}
	doneCh   chan struct{}
	mu       sync.Mutex
	running  bool
	interval time.Duration
}

func newSegmentCompactor(store *SegmentedWALStore) *SegmentCompactor {
	interval := store.cfg.CompactionInterval
	if interval <= 0 {
		interval = DefaultCompactionInt
	}
	return &SegmentCompactor{
		store:    store,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		interval: interval,
	}
}

// Start launches the compaction background goroutine.
func (c *SegmentCompactor) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Stop terminates the background compactor.
func (c *SegmentCompactor) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	close(c.stopCh)
	c.mu.Unlock()
	<-c.doneCh
}

func (c *SegmentCompactor) run() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			_ = c.Compact(context.Background())
		}
	}
}

// Compact performs an active compaction pass: creates a snapshot checkpoint of live state,
// and deletes immutable segments that are older than retention or exceed maxSegments.
func (c *SegmentCompactor) Compact(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.store.writeMu.Lock()
	defer c.store.writeMu.Unlock()

	manifest := &c.store.manifest
	if len(manifest.Segments) <= 1 {
		return nil
	}

	activeID := manifest.ActiveSegment
	maxSegs := c.store.cfg.MaxSegments
	if maxSegs <= 1 {
		maxSegs = 2
	}

	// Determine segments eligible for pruning
	var toDelete []SegmentMeta
	var remaining []SegmentMeta

	for _, seg := range manifest.Segments {
		// Active segment is never pruned
		if seg.ID == activeID {
			remaining = append(remaining, seg)
			continue
		}

		// Prune if we exceed max segments
		if len(manifest.Segments)-len(toDelete) > maxSegs {
			toDelete = append(toDelete, seg)
			continue
		}

		// Prune if segment is older than retention period
		if c.store.cfg.Retention > 0 && !seg.ClosedAt.IsZero() {
			if time.Since(seg.ClosedAt) > c.store.cfg.Retention {
				toDelete = append(toDelete, seg)
				continue
			}
		}

		remaining = append(remaining, seg)
	}

	if len(toDelete) == 0 {
		return nil
	}

	// Write checkpoint of live store before pruning old segments.
	checkpointPath := filepath.Join(c.store.dir, segmentedWALCheckpointFile)
	if err := c.writeCheckpointLocked(checkpointPath); err != nil {
		return fmt.Errorf("write compaction checkpoint: %w", err)
	}
	if archiver := c.store.cfg.SegmentArchiver; archiver != nil {
		for _, seg := range toDelete {
			if err := ctx.Err(); err != nil {
				return err
			}
			path := filepath.Join(c.store.dir, seg.Filename)
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open segment %s for archival: %w", seg.Filename, err)
			}
			archiveErr := archiver.ArchiveSegment(ctx, seg, file)
			closeErr := file.Close()
			if archiveErr != nil || closeErr != nil {
				return fmt.Errorf("archive segment %s: %w", seg.Filename, errors.Join(archiveErr, closeErr))
			}
		}
	}

	manifest.Segments = remaining
	manifest.LastCompactionAt = time.Now().UTC()
	manifest.CheckpointFile = segmentedWALCheckpointFile

	// Persist the manifest BEFORE deleting any segment file. If the process
	// crashes between these two steps, the pruned segment files become
	// harmless orphans on disk (no longer referenced by the manifest); if we
	// deleted first and crashed before saving, the manifest would still
	// reference now-missing files and recovery would treat that as data loss.
	if err := c.store.saveManifestLocked(); err != nil {
		return fmt.Errorf("persist manifest before segment deletion: %w", err)
	}

	for _, seg := range toDelete {
		segPath := filepath.Join(c.store.dir, seg.Filename)
		idxPath := filepath.Join(c.store.dir, seg.IndexFile)
		_ = os.Remove(segPath)
		_ = os.Remove(idxPath)
	}

	return nil
}

// RotateEncryptionKey writes a checkpoint and starts a new active segment using
// the codec's primary encryption key, then removes every older segment. Open
// the store with EncryptionKey set to the new key and DecryptionKeys containing
// the old keys before calling this method. Once it succeeds, the old keys are
// no longer needed to recover this store.
func (s *SegmentedWALStore) RotateEncryptionKey() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.gc == nil || s.activeFile == nil {
		return ErrClosed
	}
	if err := s.gc.healthError(); err != nil {
		return err
	}
	if s.walCodec == nil || !s.walCodec.encryptionEnabled() {
		return errors.New("WAL encryption must be enabled to rotate its key")
	}

	// The new segment must be created before the manifest drops old segments.
	// Its records and the checkpoint will use the codec's primary key.
	if err := s.rotateSegmentLocked(); err != nil {
		return fmt.Errorf("start segment with primary key: %w", err)
	}

	checkpointPath := filepath.Join(s.dir, segmentedWALCheckpointFile)
	if err := s.writeCheckpointSnapshot(checkpointPath, s.MemoryStore.Snapshot()); err != nil {
		return fmt.Errorf("write key-rotation checkpoint: %w", err)
	}

	previousSegments := append([]SegmentMeta(nil), s.manifest.Segments...)
	previousCheckpoint := s.manifest.CheckpointFile
	previousCompactionAt := s.manifest.LastCompactionAt
	var oldSegments []SegmentMeta
	for _, seg := range previousSegments {
		if seg.ID != s.manifest.ActiveSegment {
			oldSegments = append(oldSegments, seg)
		}
	}
	s.manifest.Segments = []SegmentMeta{*s.activeMeta}
	s.manifest.CheckpointFile = segmentedWALCheckpointFile
	s.manifest.LastCompactionAt = time.Now().UTC()
	if err := s.saveManifestLocked(); err != nil {
		s.manifest.Segments = previousSegments
		s.manifest.CheckpointFile = previousCheckpoint
		s.manifest.LastCompactionAt = previousCompactionAt
		return fmt.Errorf("persist key-rotation checkpoint manifest: %w", err)
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open WAL directory after manifest update: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync WAL directory before deleting old segments: %w", errors.Join(syncErr, closeErr))
	}

	// The manifest now points only at the checkpoint and new active segment.
	// Failed removals leave harmless unreferenced files and are reported so an
	// operator can clean them up after confirming the manifest.
	var cleanupErr error
	for _, seg := range oldSegments {
		if err := os.Remove(filepath.Join(s.dir, seg.Filename)); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove old segment %s: %w", seg.Filename, err))
		}
		if err := os.Remove(filepath.Join(s.dir, seg.IndexFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove old segment index %s: %w", seg.IndexFile, err))
		}
	}
	return cleanupErr
}

func (c *SegmentCompactor) writeCheckpointLocked(path string) error {
	snap := c.store.MemoryStore.Snapshot()
	return c.store.writeCheckpointSnapshot(path, snap)
}

func (s *SegmentedWALStore) writeCheckpointSnapshot(path string, snap MemoryStoreSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	data, err = s.walCodec.encryptCheckpoint(data)
	if err != nil {
		return err
	}

	// Use an exclusive, unpredictable same-directory temporary file. A fixed
	// path opened with O_TRUNC could follow a stale symlink and overwrite a
	// different file before the checkpoint rename.
	f, err := os.CreateTemp(filepath.Dir(path), ".broker-checkpoint-*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}

	written, writeErr := f.Write(data)
	if writeErr != nil || written != len(data) {
		closeErr := f.Close()
		if writeErr == nil {
			writeErr = fmt.Errorf("short checkpoint write: wrote %d of %d bytes", written, len(data))
		}
		return errors.Join(writeErr, closeErr)
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync checkpoint directory: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}
