package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBinaryWALKeyRotationPreservesBackupAndRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	oldKey := []byte("old-bwl1-test-key-material-0000001")
	newKey := []byte("new-bwl1-test-key-material-0000002")
	oldCodec, err := NewWALRecordCodec(WALCodecConfig{EncryptionKey: oldKey})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewBinaryFileWALStoreWithWALCodec(dir, NewBinaryMessageCodec(), oldCodec)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "key-rotation-q"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("bwl1-key-rotation-%d", i)
		if _, err := store.Append(ctx, Message{ID: id, Payload: []byte(id)}, []Destination{{Kind: DestinationQueue, Name: "key-rotation-q"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	newCodec, err := NewWALRecordCodec(WALCodecConfig{EncryptionKey: newKey, DecryptionKeys: [][]byte{oldKey}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "broker.wal.fwb")
	report, backup, err := RotateBinaryWALEncryptionKey(path, oldCodec, newCodec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Valid() || report.Records < 6 {
		t.Fatalf("rotated BWL1 report = %+v", report)
	}
	if info, err := os.Stat(backup); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("rotation backup %q: info=%v err=%v", backup, info, err)
	}
	verified, err := VerifyBinaryWAL(path, newCodec, nil)
	if err != nil || !verified.Valid() || verified.Records != report.Records {
		t.Fatalf("verify rotated BWL1 WAL: report=%+v err=%v", verified, err)
	}
	recovered, err := NewBinaryFileWALStoreWithWALCodec(dir, NewBinaryMessageCodec(), newCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("bwl1-key-rotation-%d", i)
		message, found, err := recovered.Get(ctx, id)
		if err != nil || !found || string(message.Payload) != id {
			t.Fatalf("recovered %q: message=%+v found=%t err=%v", id, message, found, err)
		}
	}
}

func TestVerifyAndRepairBinaryWALTailPreservesOriginal(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), Message{ID: "m1"}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "broker.wal.fwb")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0x81}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	report, err := VerifyBinaryWAL(path, nil, nil)
	if err != nil || report.Valid() || len(report.Problems) != 1 || !report.Problems[0].Tail {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	_, backup, err := RepairBinaryWALTail(path, nil, nil)
	if err != nil || backup == "" {
		t.Fatalf("backup=%q err=%v", backup, err)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	repaired, err := VerifyBinaryWAL(path, nil, nil)
	if err != nil || !repaired.Valid() || repaired.Records == 0 {
		t.Fatalf("repaired report=%+v err=%v", repaired, err)
	}
	reopened, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok, err := reopened.Get(context.Background(), "m1"); err != nil || !ok {
		t.Fatalf("recovered message ok=%t err=%v", ok, err)
	}
}

func TestExportImportBinaryWALRoundTripAndRefuseOverwrite(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	store, err := NewBinaryFileWALStore(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "portable", TenantID: "a"}, []Destination{{Kind: DestinationStream, Name: "events"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	sourcePath := filepath.Join(sourceDir, "broker.wal.fwb")
	if report, err := ExportBinaryWAL(sourcePath, &exported, nil, nil); err != nil || report.Records == 0 {
		t.Fatalf("export report=%+v err=%v", report, err)
	}
	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "broker.wal.fwb")
	if report, err := ImportBinaryWAL(bytes.NewReader(exported.Bytes()), targetPath, nil); err != nil || !report.Valid() {
		t.Fatalf("import report=%+v err=%v", report, err)
	}
	if _, err := ImportBinaryWAL(bytes.NewReader(exported.Bytes()), targetPath, nil); !errors.Is(err, os.ErrExist) {
		t.Fatalf("overwrite should be refused, got %v", err)
	}
	recovered, err := NewBinaryFileWALStore(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	message, ok, err := recovered.Get(ctx, "portable")
	if err != nil || !ok || message.TenantID != "a" || message.Offset != 1 {
		t.Fatalf("round-trip message=%+v ok=%t err=%v", message, ok, err)
	}
}
