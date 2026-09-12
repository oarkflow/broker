package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type raftDiskState struct {
	HardState []byte   `json:"hard_state,omitempty"`
	Snapshot  []byte   `json:"snapshot,omitempty"`
	Entries   [][]byte `json:"entries,omitempty"`
	Checksum  string   `json:"checksum,omitempty"`
}

type persistentRaftStorage struct {
	memory *etcdraft.MemoryStorage
	dir    string
	path   string
	loaded bool
}

func openPersistentRaftStorage(dir string) (*persistentRaftStorage, error) {
	if dir == "" {
		return &persistentRaftStorage{memory: etcdraft.NewMemoryStorage()}, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("etcd raft: create storage directory: %w", err)
	}
	p := &persistentRaftStorage{memory: etcdraft.NewMemoryStorage(), dir: dir, path: filepath.Join(dir, "etcd-raft-state.json")}
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return nil, fmt.Errorf("etcd raft: read durable state: %w", err)
	}
	var disk raftDiskState
	if err := json.Unmarshal(data, &disk); err != nil {
		return nil, fmt.Errorf("etcd raft: decode durable state: %w", err)
	}
	if disk.Checksum != "" {
		checksum := disk.Checksum
		disk.Checksum = ""
		actual, err := raftDiskChecksum(disk)
		if err != nil {
			return nil, fmt.Errorf("etcd raft: verify durable state: %w", err)
		}
		if actual != checksum {
			return nil, fmt.Errorf("etcd raft: durable state checksum mismatch")
		}
		disk.Checksum = checksum
	}
	if len(disk.Snapshot) > 0 {
		var snapshot raftpb.Snapshot
		if err := proto.Unmarshal(disk.Snapshot, &snapshot); err != nil {
			return nil, fmt.Errorf("etcd raft: decode snapshot: %w", err)
		}
		if !etcdraft.IsEmptySnap(&snapshot) {
			if err := p.memory.ApplySnapshot(&snapshot); err != nil {
				return nil, fmt.Errorf("etcd raft: restore snapshot: %w", err)
			}
		}
	}
	if len(disk.HardState) > 0 {
		var hardState raftpb.HardState
		if err := proto.Unmarshal(disk.HardState, &hardState); err != nil {
			return nil, fmt.Errorf("etcd raft: decode hard state: %w", err)
		}
		if err := p.memory.SetHardState(&hardState); err != nil {
			return nil, fmt.Errorf("etcd raft: restore hard state: %w", err)
		}
	}
	entries := make([]*raftpb.Entry, 0, len(disk.Entries))
	for i, encoded := range disk.Entries {
		var entry raftpb.Entry
		if err := proto.Unmarshal(encoded, &entry); err != nil {
			return nil, fmt.Errorf("etcd raft: decode entry %d: %w", i, err)
		}
		entries = append(entries, &entry)
	}
	if len(entries) > 0 {
		if err := p.memory.Append(entries); err != nil {
			return nil, fmt.Errorf("etcd raft: restore entries: %w", err)
		}
	}
	p.loaded = true
	return p, nil
}

func (p *persistentRaftStorage) persistReady(ready etcdraft.Ready) error {
	if !etcdraft.IsEmptySnap(ready.Snapshot) {
		if err := p.memory.ApplySnapshot(ready.Snapshot); err != nil && !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			return err
		}
	}
	if !etcdraft.IsEmptyHardState(ready.HardState) {
		if err := p.memory.SetHardState(ready.HardState); err != nil {
			return err
		}
	}
	if len(ready.Entries) > 0 {
		if err := p.memory.Append(ready.Entries); err != nil {
			return err
		}
	}
	return p.sync()
}

func (p *persistentRaftStorage) sync() error {
	if p.path == "" {
		return nil
	}
	hardState, _, err := p.memory.InitialState()
	if err != nil {
		return err
	}
	snapshot, err := p.memory.Snapshot()
	if err != nil {
		return err
	}
	first, err := p.memory.FirstIndex()
	if err != nil {
		return err
	}
	last, err := p.memory.LastIndex()
	if err != nil {
		return err
	}
	var entries []*raftpb.Entry
	if last >= first {
		entries, err = p.memory.Entries(first, last+1, math.MaxUint64)
		if err != nil {
			return err
		}
	}
	disk := raftDiskState{}
	if !etcdraft.IsEmptyHardState(hardState) {
		disk.HardState, err = proto.Marshal(hardState)
		if err != nil {
			return err
		}
	}
	if !etcdraft.IsEmptySnap(snapshot) {
		disk.Snapshot, err = proto.Marshal(snapshot)
		if err != nil {
			return err
		}
	}
	for i := range entries {
		encoded, marshalErr := proto.Marshal(entries[i])
		if marshalErr != nil {
			return marshalErr
		}
		disk.Entries = append(disk.Entries, encoded)
	}
	disk.Checksum, err = raftDiskChecksum(disk)
	if err != nil {
		return err
	}
	data, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := os.Rename(tmp, p.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	dir, err := os.Open(p.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	return errors.Join(err, closeErr)
}

func raftDiskChecksum(disk raftDiskState) (string, error) {
	disk.Checksum = ""
	data, err := json.Marshal(disk)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
