package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/oarkflow/broker"
)

func walExample(ctx context.Context, dir string) error {
	sourceDir := filepath.Join(dir, "source")
	store, err := broker.NewBinaryFileWALStore(sourceDir)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := store.Append(ctx, broker.Message{ID: "input", TenantID: "tenant-a"}, []broker.Destination{{Kind: broker.DestinationStream, Name: "input"}}); err != nil {
		return err
	}
	offset := broker.OffsetCommit{Stream: "input", ConsumerGroup: "workers", TenantID: "tenant-a", Offset: 1}
	if _, err := store.CommitStreamTransaction(ctx, broker.StreamTransaction{Offset: offset, Entries: []broker.AppendEntry{{Message: broker.Message{ID: "portable-output"}, Destinations: []broker.Destination{{Kind: broker.DestinationQueue, Name: "jobs"}}}}}); err != nil {
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}
	path := filepath.Join(sourceDir, "broker.wal.fwb")
	report, err := broker.VerifyBinaryWAL(path, nil, nil)
	if err != nil || !report.Valid() {
		return fmt.Errorf("verification=%+v, error=%v", report, err)
	}
	var exported bytes.Buffer
	if _, err := broker.ExportBinaryWAL(path, &exported, nil, nil); err != nil {
		return err
	}
	targetDir := filepath.Join(dir, "imported")
	targetPath := filepath.Join(targetDir, "broker.wal.fwb")
	if _, err := broker.ImportBinaryWAL(bytes.NewReader(exported.Bytes()), targetPath, nil); err != nil {
		return err
	}
	if _, err := broker.ImportBinaryWAL(bytes.NewReader(exported.Bytes()), targetPath, nil); expectError(err, os.ErrExist) != nil {
		return fmt.Errorf("import overwrote an existing WAL: %v", err)
	}
	reopened, err := broker.NewBinaryFileWALStore(targetDir)
	if err != nil {
		return err
	}
	if err := verifyPortableTransaction(ctx, reopened, offset); err != nil {
		reopened.Close()
		return err
	}
	if err := reopened.Close(); err != nil {
		return err
	}
	fmt.Println("  Read-only verify, JSON export and exclusive import preserve transaction output and tenant offset")
	// Opening this copied BWL1 directory with the segmented store performs migration.
	segmented, err := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: targetDir, DisableCompactor: true})
	if err != nil {
		return err
	}
	if err := verifyPortableTransaction(ctx, segmented, offset); err != nil {
		segmented.Close()
		return err
	}
	if err := segmented.Close(); err != nil {
		return err
	}
	if report, err := broker.VerifySegmentedWAL(targetDir, nil, nil); err != nil || !report.Valid() {
		return fmt.Errorf("segmented verification=%+v, error=%v", report, err)
	}
	fmt.Println("  BWL1 -> segmented WAL migration preserves the same committed transaction")

	// Inject an incomplete length prefix into this disposable source, never a live WAL.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write([]byte{0x80})
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	// Repair returns the original diagnostic report; verify the installed file again.
	_, backup, err := broker.RepairBinaryWALTail(path, nil, nil)
	if err != nil || backup == "" {
		return fmt.Errorf("tail repair backup=%q, error=%v", backup, err)
	}
	if repaired, err := broker.VerifyBinaryWAL(path, nil, nil); err != nil || !repaired.Valid() {
		return fmt.Errorf("repaired WAL=%+v, error=%v", repaired, err)
	}
	fmt.Println("  Torn-tail repair restores a valid WAL and keeps a backup")
	return withBroker(ctx, broker.NewMemoryStore(), func(b *broker.Broker) error {
		_, err := b.Publisher().Publish(ctx, "memory", "payload", broker.WithConfirmLevel(broker.ConfirmPersisted))
		if err := expectError(err, broker.ErrUnsupportedMode); err != nil {
			return err
		}
		result, err := b.Publisher().Publish(ctx, "memory", "payload")
		if err != nil || result.Confirmed != broker.ConfirmEnqueued {
			return fmt.Errorf("memory confirmation=%q, error=%v", result.Confirmed, err)
		}
		fmt.Println("  Memory reports enqueued and rejects an unachievable persisted confirmation")
		return nil
	})
}

func verifyPortableTransaction(ctx context.Context, store broker.Store, offset broker.OffsetCommit) error {
	got, err := store.ConsumerOffset(ctx, offset)
	if err != nil || got != 1 {
		return fmt.Errorf("portable offset=%d, error=%v", got, err)
	}
	message, exists, err := store.Get(ctx, "portable-output")
	if err != nil || !exists || message.TenantID != "tenant-a" {
		return fmt.Errorf("portable output=%+v, error=%v", message, err)
	}
	return nil
}
