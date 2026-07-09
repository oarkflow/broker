package fastworker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// FileStore appends JSONL job state transitions and DLQ records. It is a simple,
// crash-safe audit store for production diagnostics and replay tooling. It does
// not serialize arbitrary Go function jobs for restart execution.
type FileStore struct {
	mu   sync.Mutex
	jobs *os.File
	dlq  *os.File
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	jf, err := os.OpenFile(filepath.Join(dir, "jobs.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	df, err := os.OpenFile(filepath.Join(dir, "dlq.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = jf.Close()
		return nil, err
	}
	return &FileStore{jobs: jf, dlq: df}, nil
}
func (f *FileStore) SaveJob(ctx context.Context, info JobInfo) error {
	return f.write(ctx, f.jobs, info)
}
func (f *FileStore) SaveDeadLetter(ctx context.Context, info JobInfo) error {
	return f.write(ctx, f.dlq, info)
}
func (f *FileStore) write(ctx context.Context, file *os.File, info JobInfo) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := json.NewEncoder(file).Encode(info); err != nil {
		return err
	}
	return file.Sync()
}
func (f *FileStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	if f.jobs != nil {
		if e := f.jobs.Close(); e != nil {
			err = e
		}
	}
	if f.dlq != nil {
		if e := f.dlq.Close(); err == nil && e != nil {
			err = e
		}
	}
	return err
}
