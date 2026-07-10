package cluster

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// SyncMode controls how often the WAL fsyncs to disk.
type SyncMode int

const (
	SyncAlways  SyncMode = iota // fsync on every Append
	SyncBatched                 // fsync at regular intervals (default)
	SyncNone                    // no fsync (testing/benchmarking only)
)

// RaftWAL is a write-ahead log for Raft entries.
// Format per record: [4-byte CRC32][4-byte length][payload]
type RaftWAL struct {
	mu   sync.RWMutex
	path string
	file *os.File
	bw   *bufio.Writer

	syncMode     SyncMode
	syncInterval time.Duration
	preallocSize int64

	stopCh chan struct{}
	wg     sync.WaitGroup

	// Index tracking
	firstIndex uint64
	lastIndex  uint64
	entries    map[uint64]*walRecord // index -> record offset/length for reads

	// Metrics
	bytesWritten atomic.Uint64
	syncCount    atomic.Uint64
}

type walRecord struct {
	offset int64
	length uint32
	term   uint64
}

// WALConfig holds configuration for the Raft WAL.
type WALConfig struct {
	Directory    string
	SyncMode     SyncMode
	SyncInterval time.Duration
	PreallocSize int64
}

// NewRaftWAL creates or opens a Raft WAL at the given directory.
func NewRaftWAL(cfg WALConfig) (*RaftWAL, error) {
	if err := os.MkdirAll(cfg.Directory, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	path := filepath.Join(cfg.Directory, "raft.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: stat: %w", err)
	}
	wasEmpty := fi.Size() == 0

	if cfg.SyncMode == 0 {
		cfg.SyncMode = SyncBatched
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 5 * time.Millisecond
	}
	if cfg.PreallocSize == 0 {
		cfg.PreallocSize = 64 * 1024 * 1024 // 64MB
	}

	w := &RaftWAL{
		path:         path,
		file:         file,
		entries:      make(map[uint64]*walRecord),
		syncMode:     cfg.SyncMode,
		syncInterval: cfg.SyncInterval,
		preallocSize: cfg.PreallocSize,
	}

	if err := w.replay(); err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: replay: %w", err)
	}

	// Pre-allocate only if the file was empty before replay
	if wasEmpty && w.preallocSize > 0 {
		if err := file.Truncate(w.preallocSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("wal: prealloc: %w", err)
		}
		// After replay on empty file, position is at 0 (Seek(0, io.SeekEnd) returned 0).
		// Truncate does not change the position, so we're still at 0.
	}

	w.bw = bufio.NewWriterSize(file, 64*1024)

	if w.syncMode == SyncBatched {
		w.stopCh = make(chan struct{})
		w.wg.Add(1)
		go w.syncLoop()
	}

	return w, nil
}

// syncLoop periodically fsyncs the WAL file.
func (w *RaftWAL) syncLoop() {
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()
	defer w.wg.Done()
	for {
		select {
		case <-ticker.C:
			if err := w.file.Sync(); err != nil {
				log.Printf("[wal] background sync error: %v", err)
			} else {
				w.syncCount.Add(1)
			}
		case <-w.stopCh:
			return
		}
	}
}

// replay reads the WAL file and rebuilds the in-memory index.
func (w *RaftWAL) replay() error {
	offset, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		// Read CRC
		var crc uint32
		if err := binary.Read(w.file, binary.BigEndian, &crc); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("read crc at offset %d: %w", offset, err)
		}

		// Read length
		var length uint32
		if err := binary.Read(w.file, binary.BigEndian, &length); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("read length at offset %d: %w", offset, err)
		}

		if length > 100*1024*1024 { // 100MB sanity check
			log.Printf("[wal] record too large (%d bytes) at offset %d, attempting recovery", length, offset)
			newOffset, ok := w.scanForward(offset + 1)
			if !ok {
				break
			}
			offset = newOffset
			if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
				return fmt.Errorf("wal: seek: %w", err)
			}
			continue
		}

		// Read payload
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("read payload at offset %d: %w", offset, err)
		}

		// Verify CRC
		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != crc {
			log.Printf("[wal] crc mismatch at offset %d, attempting recovery", offset)
			newOffset, ok := w.scanForward(offset + 1)
			if !ok {
				log.Printf("[wal] no valid records after offset %d, truncating", offset)
				break
			}
			log.Printf("[wal] recovered at offset %d", newOffset)
			offset = newOffset
			if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
				return fmt.Errorf("wal: seek: %w", err)
			}
			continue
		}

		// Decode entry
		entry, err := DecodeLogEntry(payload)
		if err != nil {
			log.Printf("[wal] decode error at offset %d: %v, truncating", offset, err)
			break
		}

		// Track record position
		recordLen := 4 + 4 + length // crc + length + payload
		w.entries[entry.Index] = &walRecord{
			offset: offset,
			length: recordLen,
			term:   entry.Term,
		}

		if w.firstIndex == 0 || entry.Index < w.firstIndex {
			w.firstIndex = entry.Index
		}
		if entry.Index > w.lastIndex {
			w.lastIndex = entry.Index
		}

		offset += int64(recordLen)
	}

	// Truncate file to clean position
	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek end: %w", err)
	}

	if len(w.entries) > 0 {
		log.Printf("[wal] replayed %d entries, index range [%d, %d]", len(w.entries), w.firstIndex, w.lastIndex)
	}

	return nil
}

// scanForward scans forward from the given offset looking for the next valid
// CRC-protected record. Returns the offset of the found record, or false if
// no valid record is found within 1MB.
func (w *RaftWAL) scanForward(from int64) (int64, bool) {
	maxScan := int64(1024 * 1024) // scan up to 1MB forward

	fi, err := w.file.Stat()
	if err != nil {
		return 0, false
	}
	fileSize := fi.Size()
	if from >= fileSize {
		return 0, false
	}

	end := from + maxScan
	if end > fileSize {
		end = fileSize
	}

	scanLen := end - from
	buf := make([]byte, scanLen)
	if _, err := w.file.ReadAt(buf, from); err != nil {
		return 0, false
	}

	for i := int64(0); i < scanLen; i++ {
		remaining := scanLen - i
		if remaining < 8 {
			break
		}
		candidateCRC := binary.BigEndian.Uint32(buf[i:])
		candidateLen := binary.BigEndian.Uint32(buf[i+4:])

		if candidateLen == 0 || candidateLen > 100*1024*1024 {
			continue
		}
		if remaining < int64(8+candidateLen) {
			continue
		}
		payload := buf[i+8 : i+8+int64(candidateLen)]
		if crc32.ChecksumIEEE(payload) == candidateCRC {
			return from + i, true
		}
	}
	return 0, false
}

// Append appends a log entry to the WAL.
func (w *RaftWAL) Append(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := EncodeLogEntry(entry)
	if err != nil {
		return fmt.Errorf("wal: encode: %w", err)
	}

	// Write CRC + length + payload
	crc := crc32.ChecksumIEEE(data)

	if err := binary.Write(w.bw, binary.BigEndian, crc); err != nil {
		return fmt.Errorf("wal: write crc: %w", err)
	}
	if err := binary.Write(w.bw, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("wal: write length: %w", err)
	}
	if _, err := w.bw.Write(data); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}

	// Flush buffered writer so data is in the OS page cache
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}

	// Sync based on mode
	if w.syncMode == SyncAlways {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
		w.syncCount.Add(1)
	}

	// Track bytes written
	w.bytesWritten.Add(uint64(4 + 4 + len(data)))

	// Update index
	offset, _ := w.file.Seek(0, io.SeekCurrent)
	recordLen := uint32(4 + 4 + len(data))
	w.entries[entry.Index] = &walRecord{
		offset: offset - int64(recordLen),
		length: recordLen,
		term:   entry.Term,
	}

	if w.firstIndex == 0 || entry.Index < w.firstIndex {
		w.firstIndex = entry.Index
	}
	if entry.Index > w.lastIndex {
		w.lastIndex = entry.Index
	}

	return nil
}

// Sync explicitly fsyncs the WAL file. Useful before taking a snapshot or
// when the caller needs durability guarantees in SyncBatched mode.
func (w *RaftWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	w.syncCount.Add(1)
	return nil
}

// Read reads a log entry by index.
func (w *RaftWAL) Read(index uint64) (*LogEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	record, ok := w.entries[index]
	if !ok {
		return nil, fmt.Errorf("wal: index %d not found", index)
	}

	// Seek to record position
	if _, err := w.file.Seek(record.offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	// Skip CRC + length
	if _, err := w.file.Seek(8, io.SeekCurrent); err != nil {
		return nil, fmt.Errorf("wal: seek past header: %w", err)
	}

	// Read payload
	payload := make([]byte, record.length-8)
	if _, err := io.ReadFull(w.file, payload); err != nil {
		return nil, fmt.Errorf("wal: read payload: %w", err)
	}

	return DecodeLogEntry(payload)
}

// ReadRange reads a range of log entries [from, to].
func (w *RaftWAL) ReadRange(from, to uint64) ([]*LogEntry, error) {
	entries := make([]*LogEntry, 0, to-from+1)
	for i := from; i <= to; i++ {
		entry, err := w.Read(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Truncate removes all entries with index > lastIndex.
func (w *RaftWAL) Truncate(lastIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for idx := range w.entries {
		if idx > lastIndex {
			delete(w.entries, idx)
		}
	}

	if lastIndex < w.lastIndex {
		w.lastIndex = lastIndex
	}

	return nil
}

// Compact removes entries before the given index.
func (w *RaftWAL) Compact(beforeIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for idx := range w.entries {
		if idx < beforeIndex {
			delete(w.entries, idx)
		}
	}

	if beforeIndex > w.firstIndex {
		w.firstIndex = beforeIndex
	}

	return nil
}

// FirstIndex returns the first index in the WAL.
func (w *RaftWAL) FirstIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.firstIndex
}

// LastIndex returns the last index in the WAL.
func (w *RaftWAL) LastIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastIndex
}

// Contains returns true if the WAL contains the given index.
func (w *RaftWAL) Contains(index uint64) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.entries[index]
	return ok
}

// Snapshot writes a snapshot marker to the WAL and truncates entries up to lastIncludedIndex.
func (w *RaftWAL) Snapshot(snapshot *Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write snapshot entry
	entry := &LogEntry{
		Term:    snapshot.Term,
		Index:   snapshot.LastIncludedIndex,
		Type:    EntrySnapshot,
		Data:    snapshot.Data,
		Command: EncodeUint64(snapshot.LastIncludedTerm),
	}

	data, err := EncodeLogEntry(entry)
	if err != nil {
		return fmt.Errorf("wal: encode snapshot: %w", err)
	}

	crc := crc32.ChecksumIEEE(data)
	if err := binary.Write(w.bw, binary.BigEndian, crc); err != nil {
		return fmt.Errorf("wal: write snapshot crc: %w", err)
	}
	if err := binary.Write(w.bw, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("wal: write snapshot length: %w", err)
	}
	if _, err := w.bw.Write(data); err != nil {
		return fmt.Errorf("wal: write snapshot payload: %w", err)
	}
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush snapshot: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync snapshot: %w", err)
	}
	w.syncCount.Add(1)
	w.bytesWritten.Add(uint64(4 + 4 + len(data)))

	// Compact entries before snapshot
	for idx := range w.entries {
		if idx <= snapshot.LastIncludedIndex {
			delete(w.entries, idx)
		}
	}
	w.firstIndex = snapshot.LastIncludedIndex + 1

	return nil
}

// Stats returns WAL statistics.
func (w *RaftWAL) Stats() map[string]any {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return map[string]any{
		"path":           w.path,
		"entries":        len(w.entries),
		"first_index":    w.firstIndex,
		"last_index":     w.lastIndex,
		"bytes_written":  w.bytesWritten.Load(),
		"sync_count":     w.syncCount.Load(),
	}
}

// Close closes the WAL.
func (w *RaftWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopCh != nil {
		close(w.stopCh)
	}
	w.mu.Unlock()
	w.wg.Wait()
	w.mu.Lock()

	if w.bw != nil {
		if err := w.bw.Flush(); err != nil {
			log.Printf("[wal] flush error on close: %v", err)
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			log.Printf("[wal] sync error on close: %v", err)
		}
		return w.file.Close()
	}
	return nil
}

// EncodeSnapshot serializes a Snapshot to JSON bytes.
func EncodeSnapshot(s *Snapshot) ([]byte, error) {
	return json.Marshal(s)
}

// DecodeSnapshot deserializes a Snapshot from JSON bytes.
func DecodeSnapshot(data []byte) (*Snapshot, error) {
	s := &Snapshot{}
	return s, json.Unmarshal(data, s)
}
