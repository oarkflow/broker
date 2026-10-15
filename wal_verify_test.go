package broker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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
	if _, err := store.CommitStreamTransaction(ctx, StreamTransaction{
		Offset:  OffsetCommit{Stream: "events", ConsumerGroup: "workers", Offset: 1, TenantID: "a"},
		Entries: []AppendEntry{{Message: Message{ID: "portable-output"}, Destinations: []Destination{{Kind: DestinationQueue, Name: "processed"}}}},
	}); err != nil {
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
	if offset, err := recovered.ConsumerOffset(ctx, OffsetCommit{Stream: "events", ConsumerGroup: "workers", TenantID: "a"}); err != nil || offset != 1 {
		t.Fatalf("round-trip transaction offset=%d, error=%v", offset, err)
	}
	if _, ok, err := recovered.Get(ctx, "portable-output"); err != nil || !ok {
		t.Fatalf("round-trip transaction output exists=%v, error=%v", ok, err)
	}
}
