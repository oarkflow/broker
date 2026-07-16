package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// WALCompactionConfig controls WAL compaction and segment rotation behavior.
type WALCompactionConfig struct {
	// MaxSegmentSize is the maximum size in bytes before rotating to a new segment.
	// Default: 128 MiB.
	MaxSegmentSize int64

	// MaxSegments is the maximum number of active segments before oldest are archived.
	// Default: 10.
	MaxSegments int

	// CompactionInterval is how often to run compaction. Default: 5 minutes.
	CompactionInterval time.Duration

	// ArchiveDir is where archived (old) segments are moved. Empty = segments are deleted.
	ArchiveDir string

	// MaxAge is the maximum age of messages to keep during compaction.
	// Messages older than this are removed from active segments. Default: 0 (keep all).
	MaxAge time.Duration

	// RetainAcked controls whether acknowledged messages are kept during compaction.
	// Default: false (remove acked messages).
	RetainAcked bool

	// MinSegmentFiles is the minimum number of segment files to keep.
	// Compaction will not reduce below this count. Default: 1.
	MinSegmentFiles int
}

func defaultWALCompactionConfig() WALCompactionConfig {
	return WALCompactionConfig{
		MaxSegmentSize:    128 << 20,
		MaxSegments:       10,
		CompactionInterval: 5 * time.Minute,
		MinSegmentFiles:   1,
	}
}

// CompactableStore is a store that supports compaction operations.
type CompactableStore interface {
	Store
	// CompactableMessages returns all message IDs that can be safely removed.
	// Typically acked/completed messages that won't be needed.
	CompactableMessages(ctx context.Context) ([]string, error)
	// RemoveMessages removes the specified messages from the store.
	RemoveMessages(ctx context.Context, ids []string) error
}

// WALCompactor handles WAL segment rotation and compaction.
type WALCompactor struct {
	mu       sync.Mutex
	dir      string
	cfg      WALCompactionConfig
	store    *MemoryStore
	segments []walSegment
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

type walSegment struct {
	name    string
	path    string
	size    int64
	records int
	modTime time.Time
}

// NewWALCompactor creates a new WAL compactor for the given directory.
func NewWALCompactor(dir string, cfg WALCompactionConfig, store *MemoryStore) *WALCompactor {
	if cfg.MaxSegmentSize <= 0 {
		cfg.MaxSegmentSize = 128 << 20
	}
	if cfg.MaxSegments <= 0 {
		cfg.MaxSegments = 10
	}
	if cfg.CompactionInterval <= 0 {
		cfg.CompactionInterval = 5 * time.Minute
	}
	if cfg.MinSegmentFiles <= 0 {
		cfg.MinSegmentFiles = 1}
	return &WALCompactor{
		dir:   dir,
		cfg:   cfg,
		store: store,
		stopCh: make(chan struct{}),
	}
}

// Start begins periodic compaction.
func (wc *WALCompactor) Start() {
	wc.wg.Add(1)
	go wc.compactionLoop()
}

// Stop halts compaction and waits for the goroutine to finish.
func (wc *WALCompactor) Stop() {
	close(wc.stopCh)
	wc.wg.Wait()
}

func (wc *WALCompactor) compactionLoop() {
	defer wc.wg.Done()
	t := time.NewTicker(wc.cfg.CompactionInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := wc.Compact(); err != nil {
				// Log but don't crash on compaction errors.
				_ = err
			}
		case <-wc.stopCh:
			return
		}
	}
}

// Compact performs a single compaction pass: rotates the active segment, archives
// old segments, and optionally prunes acknowledged messages.
func (wc *WALCompactor) Compact() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	if err := wc.rotateSegment(); err != nil {
		return fmt.Errorf("wal compaction rotate: %w", err)
	}
	if err := wc.archiveOldSegments(); err != nil {
		return fmt.Errorf("wal compaction archive: %w", err)
	}
	if wc.store != nil && !wc.cfg.RetainAcked {
		wc.pruneAcknowledged()
	}
	return nil
}

func (wc *WALCompactor) rotateSegment() error {
	activePath := filepath.Join(wc.dir, "broker.wal.jsonl")
	info, err := os.Stat(activePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Size() < wc.cfg.MaxSegmentSize {
		return nil
	}
	ts := time.Now().Format("20060102-150405")
	segmentName := fmt.Sprintf("broker.wal.%s.jsonl", ts)
	segmentPath := filepath.Join(wc.dir, segmentName)
	if err := os.Rename(activePath, segmentPath); err != nil {
		return fmt.Errorf("rotate segment: %w", err)
	}
	wc.segments = append(wc.segments, walSegment{
		name:    segmentName,
		path:    segmentPath,
		size:    info.Size(),
		modTime: info.ModTime(),
	})
	return nil
}

func (wc *WALCompactor) archiveOldSegments() error {
	if len(wc.segments) <= wc.cfg.MinSegmentFiles {
		return nil
	}
	excess := len(wc.segments) - wc.cfg.MinSegmentFiles
	archived := 0
	for i := 0; i < excess && archived < excess; i++ {
		seg := wc.segments[i]
		if wc.cfg.ArchiveDir != "" {
			if err := os.MkdirAll(wc.cfg.ArchiveDir, 0o700); err != nil {
				return fmt.Errorf("create archive dir: %w", err)
			}
			dst := filepath.Join(wc.cfg.ArchiveDir, seg.name)
			if err := os.Rename(seg.path, dst); err != nil {
				return fmt.Errorf("archive segment %s: %w", seg.name, err)
			}
		} else {
			if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove segment %s: %w", seg.name, err)
			}
		}
		archived++
	}
	if archived > 0 {
		wc.segments = wc.segments[archived:]
	}
	return nil
}

func (wc *WALCompactor) pruneAcknowledged() {
	if wc.store == nil {
		return
	}
	wc.store.mu.Lock()
	defer wc.store.mu.Unlock()
	now := time.Now()
	for id, m := range wc.store.messages {
		if m == nil {
			continue
		}
		switch m.Status {
		case MessageCompleted, MessageExpired, MessagePurged, MessageArchived:
			// Remove old completed messages if MaxAge is set and they are old enough.
			if wc.cfg.MaxAge > 0 && !m.CreatedAt.IsZero() && now.Sub(m.CreatedAt) > wc.cfg.MaxAge {
				delete(wc.store.messages, id)
				if m.Queue != "" {
					if q := wc.store.queues[m.Queue]; q != nil {
						q.ids = removeID(q.ids, id)
					}
				}
			}
		}
	}
	// Clean up orphaned history entries for removed messages.
	for id := range wc.store.history {
		if _, exists := wc.store.messages[id]; !exists {
			delete(wc.store.history, id)
		}
	}
}

// ListSegments returns the list of WAL segments in the directory.
func (wc *WALCompactor) ListSegments() ([]walSegment, error) {
	entries, err := os.ReadDir(wc.dir)
	if err != nil {
		return nil, err
	}
	var segments []walSegment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(e.Name(), "broker.wal.") || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		if e.Name() == "broker.wal.jsonl" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		segments = append(segments, walSegment{
			name:    e.Name(),
			path:    filepath.Join(wc.dir, e.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].modTime.Before(segments[j].modTime)
	})
	return segments, nil
}

// ReplaySegment replays a specific WAL segment into a MemoryStore.
func ReplaySegment(path string, store *MemoryStore) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		line, readErr := r.ReadBytes('\n')
		if len(line) == 0 && errors.Is(readErr, io.EOF) {
			return nil
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			var rec walRecord
			if err := json.Unmarshal(trimmed, &rec); err != nil {
				if errors.Is(readErr, io.EOF) {
					return nil
				}
				offset, _ := f.Seek(0, io.SeekCurrent)
				return fmt.Errorf("corrupted WAL record at offset %d: %w", offset-int64(len(line)), err)
			}
			if err := verifyWALRecord(rec); err != nil {
				continue
			}
			// Apply recovered record using the FileWALStore method via a wrapper.
			wrapper := &FileWALStore{MemoryStore: store}
			if err := wrapper.applyRecovered(rec); err != nil {
				continue
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// WALSegmentStats returns compaction statistics.
type WALSegmentStats struct {
	ActiveSegmentSize int64  `json:"active_segment_size"`
	ActiveSegmentPath string `json:"active_segment_path"`
	ArchivedSegments  int    `json:"archived_segments"`
	ArchivedTotalSize int64  `json:"archived_total_size"`
	TotalMessages     int    `json:"total_messages"`
}

// Stats returns current compaction statistics.
func (wc *WALCompactor) Stats() WALSegmentStats {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	stats := WALSegmentStats{
		ActiveSegmentPath: filepath.Join(wc.dir, "broker.wal.jsonl"),
	}
	if info, err := os.Stat(stats.ActiveSegmentPath); err == nil {
		stats.ActiveSegmentSize = info.Size()
	}
	for _, seg := range wc.segments {
		stats.ArchivedSegments++
		stats.ArchivedTotalSize += seg.size
	}
	if wc.store != nil {
		wc.store.mu.RLock()
		stats.TotalMessages = len(wc.store.messages)
		wc.store.mu.RUnlock()
	}
	return stats
}

// RecoverFromAllSegments recovers from all WAL segments in a directory.
func RecoverFromAllSegments(dir string, store *MemoryStore) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), "broker.wal.") && strings.HasSuffix(e.Name(), ".jsonl") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		if err := ReplaySegment(p, store); err != nil {
			return fmt.Errorf("replay %s: %w", p, err)
		}
	}
	return nil
}

// verifyWALRecordSegment verifies the checksum of a WAL record from a segment file.
func verifyWALRecordSegment(r walRecord) error {
	if r.Checksum == "" {
		return nil
	}
	want := r.Checksum
	r.Checksum = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	got := hex.EncodeToString(sum[:])
	if got != want {
		return fmt.Errorf("wal checksum mismatch: got %s want %s", got, want)
	}
	return nil
}
