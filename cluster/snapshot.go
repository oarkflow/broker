package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

var (
	SnapshotMagic = [4]byte{'B', 'S', 'N', 'P'}
	SnapshotVer   = uint8(1)

	ErrInvalidSnapshotMagic     = errors.New("snapshot: invalid magic bytes")
	ErrUnsupportedSnapshotVer   = errors.New("snapshot: unsupported format version")
	ErrSnapshotChecksumMismatch = errors.New("snapshot: CRC32 checksum mismatch")
)

// SnapshotHeader contains metadata describing a state machine snapshot.
type SnapshotHeader struct {
	Magic     [4]byte
	Version   uint8
	Reserved  [3]byte
	Term      uint64
	Index     uint64
	Timestamp int64
	DataLen   uint64
	Checksum  uint32
}

// SnapshotData represents an in-memory or persisted snapshot.
type SnapshotData struct {
	Term      uint64
	Index     uint64
	Timestamp time.Time
	Payload   []byte
}

// Encode encodes snapshot data into the binary format:
// [Magic:4][Version:1][Reserved:3][Term:8][Index:8][Timestamp:8][DataLen:8][Checksum:4][Payload:N]
func (s *SnapshotData) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	checksum := crc32.ChecksumIEEE(s.Payload)
	header := SnapshotHeader{
		Magic:     SnapshotMagic,
		Version:   SnapshotVer,
		Term:      s.Term,
		Index:     s.Index,
		Timestamp: s.Timestamp.UnixNano(),
		DataLen:   uint64(len(s.Payload)),
		Checksum:  checksum,
	}

	if err := binary.Write(buf, binary.BigEndian, header); err != nil {
		return nil, err
	}
	if _, err := buf.Write(s.Payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeBinarySnapshot parses binary snapshot bytes into SnapshotData, verifying checksum.
func DecodeBinarySnapshot(data []byte) (*SnapshotData, error) {
	r := bytes.NewReader(data)
	var header SnapshotHeader
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("read snapshot header: %w", err)
	}

	if header.Magic != SnapshotMagic {
		return nil, ErrInvalidSnapshotMagic
	}
	if header.Version != SnapshotVer {
		return nil, fmt.Errorf("%w: got %d", ErrUnsupportedSnapshotVer, header.Version)
	}

	payload := make([]byte, header.DataLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read snapshot payload: %w", err)
	}

	calculatedChecksum := crc32.ChecksumIEEE(payload)
	if calculatedChecksum != header.Checksum {
		return nil, fmt.Errorf("%w: expected %08x, got %08x", ErrSnapshotChecksumMismatch, header.Checksum, calculatedChecksum)
	}

	return &SnapshotData{
		Term:      header.Term,
		Index:     header.Index,
		Timestamp: time.Unix(0, header.Timestamp),
		Payload:   payload,
	}, nil
}

// SnapshotManager manages snapshot creation, validation, and restoration.
type SnapshotManager struct{}

func NewSnapshotManager() *SnapshotManager {
	return &SnapshotManager{}
}

// CreateSnapshot captures a snapshot from the state machine and returns encoded bytes.
func (sm *SnapshotManager) CreateSnapshot(term, index uint64, stateMachine StateMachine) ([]byte, error) {
	if stateMachine == nil {
		return nil, errors.New("cannot snapshot nil state machine")
	}

	payload, err := stateMachine.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("state machine snapshot failed: %w", err)
	}

	snap := &SnapshotData{
		Term:      term,
		Index:     index,
		Timestamp: time.Now(),
		Payload:   payload,
	}

	return snap.Encode()
}

// RestoreSnapshot restores snapshot bytes into the state machine.
func (sm *SnapshotManager) RestoreSnapshot(data []byte, stateMachine StateMachine) error {
	if stateMachine == nil {
		return errors.New("cannot restore into nil state machine")
	}

	snap, err := DecodeBinarySnapshot(data)
	if err != nil {
		return err
	}

	return stateMachine.Restore(snap.Payload)
}
