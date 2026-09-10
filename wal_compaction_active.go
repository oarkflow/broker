package broker

import (
	"context"
	"encoding/json"
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

	// Write checkpoint of live store before pruning old segments
	checkpointPath := filepath.Join(c.store.dir, "checkpoint.bwl2")
	if err := c.writeCheckpointLocked(checkpointPath); err != nil {
		return fmt.Errorf("write compaction checkpoint: %w", err)
	}

	// Delete pruned segment files and index files
	for _, seg := range toDelete {
		segPath := filepath.Join(c.store.dir, seg.Filename)
		idxPath := filepath.Join(c.store.dir, seg.IndexFile)
		_ = os.Remove(segPath)
		_ = os.Remove(idxPath)
	}

	manifest.Segments = remaining
	manifest.LastCompactionAt = time.Now().UTC()
	manifest.CheckpointFile = "checkpoint.bwl2"

	return c.store.saveManifestLocked()
}

func (c *SegmentCompactor) writeCheckpointLocked(path string) error {
	snap := c.store.MemoryStore.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(path+".tmp", path)
}
