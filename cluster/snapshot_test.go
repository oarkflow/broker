package cluster

import (
	"bytes"
	"testing"
)

type dummySM struct {
	data []byte
}

func (d *dummySM) Apply(e *LogEntry) error {
	d.data = append(d.data, e.Data...)
	return nil
}

func (d *dummySM) Snapshot() ([]byte, error) {
	return d.data, nil
}

func (d *dummySM) Restore(b []byte) error {
	d.data = append([]byte(nil), b...)
	return nil
}

func TestSnapshotEncodeDecodeAndRestore(t *testing.T) {
	mgr := NewSnapshotManager()

	origSM := &dummySM{data: []byte("test-cluster-snapshot-payload-12345")}
	encoded, err := mgr.CreateSnapshot(5, 120, origSM)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	snap, err := DecodeBinarySnapshot(encoded)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}

	if snap.Term != 5 || snap.Index != 120 {
		t.Fatalf("unexpected term/index: term=%d index=%d", snap.Term, snap.Index)
	}
	if !bytes.Equal(snap.Payload, origSM.data) {
		t.Fatalf("payload mismatch: %s != %s", string(snap.Payload), string(origSM.data))
	}

	newSM := &dummySM{}
	if err := mgr.RestoreSnapshot(encoded, newSM); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	if !bytes.Equal(newSM.data, origSM.data) {
		t.Fatalf("restored state mismatch: %s != %s", string(newSM.data), string(origSM.data))
	}

	// Corrupt checksum test
	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)-1] ^= 0xFF
	if _, err := DecodeBinarySnapshot(corrupted); err == nil {
		t.Fatalf("expected error decoding corrupted snapshot")
	}
}
