package broker

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	sparseIndexMagic   = 0x42574C58 // 'BWLX'
	sparseIndexVersion = 1
	defaultIndexStep   = 64 // Index every 64 records
)

// IndexEntry represents one sparse index checkpoint.
type IndexEntry struct {
	Seq    uint64 `json:"seq"`
	Offset int64  `json:"offset"`
	Time   int64  `json:"time"`
}

// SparseIndex maintains an ordered list of index entries for fast seeks.
type SparseIndex struct {
	mu      sync.RWMutex
	entries []IndexEntry
	step    int
	counter int
}

// NewSparseIndex creates an empty sparse index.
func NewSparseIndex(step int) *SparseIndex {
	if step <= 0 {
		step = defaultIndexStep
	}
	return &SparseIndex{
		entries: make([]IndexEntry, 0, 1024),
		step:    step,
	}
}

// Add appends an entry if the step count is reached or if it's the very first entry.
func (idx *SparseIndex) Add(seq uint64, offset int64, t int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.counter++
	if len(idx.entries) == 0 || idx.counter >= idx.step {
		idx.entries = append(idx.entries, IndexEntry{
			Seq:    seq,
			Offset: offset,
			Time:   t,
		})
		idx.counter = 0
		return true
	}
	return false
}

// ForceAdd unconditionally records an index entry (e.g. for segment start or key checkpoints).
func (idx *SparseIndex) ForceAdd(seq uint64, offset int64, t int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries = append(idx.entries, IndexEntry{
		Seq:    seq,
		Offset: offset,
		Time:   t,
	})
	idx.counter = 0
}

// FindOffset returns the closest file offset <= target sequence.
// If target sequence is before the first entry, returns 0.
func (idx *SparseIndex) FindOffset(seq uint64) int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return 0
	}
	n := len(idx.entries)
	i := sort.Search(n, func(i int) bool {
		return idx.entries[i].Seq > seq
	})
	if i == 0 {
		return idx.entries[0].Offset
	}
	return idx.entries[i-1].Offset
}

// Entries returns a copy of all index entries.
func (idx *SparseIndex) Entries() []IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]IndexEntry, len(idx.entries))
	copy(out, idx.entries)
	return out
}

// SaveToFile serializes the sparse index to disk in binary format:
// [Magic:4][Version:1][Reserved:3][Count:8][Entry1:24]...[EntryN:24]
func (idx *SparseIndex) SaveToFile(path string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64<<10)
	var header [16]byte
	binary.BigEndian.PutUint32(header[0:4], sparseIndexMagic)
	header[4] = sparseIndexVersion
	binary.BigEndian.PutUint64(header[8:16], uint64(len(idx.entries)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	var entryBuf [24]byte
	for _, e := range idx.entries {
		binary.BigEndian.PutUint64(entryBuf[0:8], e.Seq)
		binary.BigEndian.PutUint64(entryBuf[8:16], uint64(e.Offset))
		binary.BigEndian.PutUint64(entryBuf[16:24], uint64(e.Time))
		if _, err := w.Write(entryBuf[:]); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// LoadSparseIndex reads a sparse index binary file into memory.
func LoadSparseIndex(path string, step int) (*SparseIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64<<10)
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	if binary.BigEndian.Uint32(header[0:4]) != sparseIndexMagic {
		return nil, errors.New("invalid sparse index magic")
	}
	if header[4] != sparseIndexVersion {
		return nil, errors.New("unsupported sparse index version")
	}

	count := binary.BigEndian.Uint64(header[8:16])
	entries := make([]IndexEntry, count)
	var entryBuf [24]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(r, entryBuf[:]); err != nil {
			return nil, err
		}
		entries[i] = IndexEntry{
			Seq:    binary.BigEndian.Uint64(entryBuf[0:8]),
			Offset: int64(binary.BigEndian.Uint64(entryBuf[8:16])),
			Time:   int64(binary.BigEndian.Uint64(entryBuf[16:24])),
		}
	}

	idx := NewSparseIndex(step)
	idx.entries = entries
	return idx, nil
}
