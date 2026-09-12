package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentedWALBasicAndRotation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Use very small segment size (1024 bytes) to force rotations
	cfg := SegmentedWALConfig{
		Dir:              dir,
		MaxSegmentSize:   1024,
		MaxSegments:      10,
		DisableCompactor: true,
	}

	st, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("new segmented wal store: %v", err)
	}
	defer st.Close()

	if err := st.DeclareQueue(ctx, QueueConfig{Name: "events"}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	// Write enough messages to trigger multiple rotations
	for i := 1; i <= 50; i++ {
		msg := Message{
			ID:        fmt.Sprintf("rot-msg-%d", i),
			Queue:     "events",
			Payload:   []byte(fmt.Sprintf("payload-large-data-block-%d-abcdefghijklmnopqrstuvwxyz", i)),
			CreatedAt: time.Now(),
		}
		if _, err := st.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "events"}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify multiple segments were created
	if st.manifest.ActiveSegment <= 1 {
		t.Fatalf("expected rotation to have occurred, active segment is %d", st.manifest.ActiveSegment)
	}

	if len(st.manifest.Segments) <= 1 {
		t.Fatalf("expected multiple segments in manifest, got %d", len(st.manifest.Segments))
	}

	// Verify with VerifySegmentedWAL
	report, err := VerifySegmentedWAL(dir, nil, nil)
	if err != nil {
		t.Fatalf("verify segmented wal: %v", err)
	}
	if !report.Valid() {
		t.Fatalf("verification failed with problems: %+v", report.Problems)
	}
	if report.TotalRecords < 50 {
		t.Fatalf("expected at least 50 records verified, got %d", report.TotalRecords)
	}
}

func TestSegmentedWALRecoveryConsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cfg := SegmentedWALConfig{
		Dir:              dir,
		MaxSegmentSize:   2048,
		MaxSegments:      10,
		DisableCompactor: true,
	}

	st1, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	_ = st1.DeclareQueue(ctx, QueueConfig{Name: "tasks"})
	_ = st1.DeclareQueue(ctx, QueueConfig{Name: "dlq"})

	for i := 1; i <= 10; i++ {
		msg := Message{
			ID:        fmt.Sprintf("task-%d", i),
			Queue:     "tasks",
			Payload:   []byte(fmt.Sprintf("data-%d", i)),
			CreatedAt: time.Now(),
		}
		_, err := st1.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "tasks"}})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Lease task-1 and task-2
	leased, err := st1.Lease(ctx, LeaseRequest{Queue: "tasks", ConsumerID: "worker-1", Limit: 2, VisibilityTimeout: 5 * time.Second})
	if err != nil || len(leased) != 2 {
		t.Fatalf("lease: %v, count=%d", err, len(leased))
	}

	// Ack task-1
	if err := st1.Ack(ctx, AckRequest{Queue: "tasks", MessageID: leased[0].ID, LeaseToken: leased[0].LeaseToken}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Nack task-2
	if err := st1.Nack(ctx, NackRequest{Queue: "tasks", MessageID: leased[1].ID, LeaseToken: leased[1].LeaseToken, Action: NackRequeue}); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Move task-3 to dlq
	if _, err := st1.Move(ctx, MoveRequest{SourceType: DestinationQueue, Source: "tasks", DestinationType: DestinationQueue, Destination: "dlq", MessageIDs: []string{"task-3"}, Mode: MoveRemoveSource, PreserveID: true}); err != nil {
		t.Fatalf("move: %v", err)
	}

	// Pause queue tasks
	if err := st1.PauseQueue(ctx, "tasks"); err != nil {
		t.Fatalf("pause queue: %v", err)
	}

	// Close st1
	if err := st1.Close(); err != nil {
		t.Fatalf("close st1: %v", err)
	}

	// Reopen st2 from the same directory
	st2, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()

	// task-1 should be completed
	m1, ok, err := st2.Get(ctx, leased[0].ID)
	if !ok || m1.Status != MessageCompleted {
		t.Fatalf("task-1 expected MessageCompleted, got ok=%v, status=%v", ok, m1.Status)
	}

	// task-2 should be requeued
	m2, ok, err := st2.Get(ctx, leased[1].ID)
	if !ok || m2.Status != MessageQueued {
		t.Fatalf("task-2 expected MessageQueued, got ok=%v, status=%v", ok, m2.Status)
	}

	// task-3 should be in dlq
	m3, ok, err := st2.Get(ctx, "task-3")
	if !ok || m3.Queue != "dlq" {
		t.Fatalf("task-3 expected in dlq, got ok=%v, queue=%q", ok, m3.Queue)
	}

	// Queue tasks should be paused
	_, err = st2.Lease(ctx, LeaseRequest{Queue: "tasks", ConsumerID: "w-check", Limit: 1, VisibilityTimeout: time.Second})
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("expected ErrPaused leasing from paused queue tasks, got: %v", err)
	}
}

func TestSegmentedWALCompactionAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cfg := SegmentedWALConfig{
		Dir:              dir,
		MaxSegmentSize:   512, // small segment size
		MaxSegments:      3,   // keep at most 3 segments
		DisableCompactor: true,
	}

	st, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	_ = st.DeclareQueue(ctx, QueueConfig{Name: "stream-q"})

	// Write enough messages to create 5+ segments
	for i := 1; i <= 30; i++ {
		msg := Message{
			ID:        fmt.Sprintf("c-msg-%d", i),
			Queue:     "stream-q",
			Payload:   []byte(fmt.Sprintf("large-message-payload-for-compaction-test-%d", i)),
			CreatedAt: time.Now(),
		}
		if _, err := st.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "stream-q"}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if len(st.manifest.Segments) < 4 {
		t.Fatalf("expected at least 4 segments before compaction, got %d", len(st.manifest.Segments))
	}

	// Create compactor and run Compact
	compactor := newSegmentCompactor(st)
	if err := compactor.Compact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Checkpoint file must exist
	chkPath := filepath.Join(dir, "checkpoint.bwl2")
	if _, err := os.Stat(chkPath); err != nil {
		t.Fatalf("checkpoint file missing after compaction: %v", err)
	}

	// Segments should now be pruned to MaxSegments
	if len(st.manifest.Segments) > cfg.MaxSegments {
		t.Fatalf("expected <= %d segments after compaction, got %d", cfg.MaxSegments, len(st.manifest.Segments))
	}

	_ = st.Close()

	// Reopen store from checkpoint + remaining segments
	st2, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	defer st2.Close()

	// Verify all 30 messages are still accessible
	for i := 1; i <= 30; i++ {
		id := fmt.Sprintf("c-msg-%d", i)
		m, ok, err := st2.Get(ctx, id)
		if !ok || err != nil || m.Status != MessageQueued {
			t.Fatalf("message %s not recovered properly after compaction: ok=%v status=%v", id, ok, m.Status)
		}
	}
}

func TestSegmentedWALTornWriteRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cfg := SegmentedWALConfig{
		Dir:              dir,
		MaxSegmentSize:   64 << 20,
		DisableCompactor: true,
	}

	st, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	_ = st.DeclareQueue(ctx, QueueConfig{Name: "torn-queue"})

	for i := 1; i <= 5; i++ {
		msg := Message{
			ID:        fmt.Sprintf("torn-msg-%d", i),
			Queue:     "torn-queue",
			Payload:   []byte("valid payload"),
			CreatedAt: time.Now(),
		}
		if _, err := st.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "torn-queue"}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	activeFilename := st.activeMeta.Filename
	activePath := filepath.Join(dir, activeFilename)

	// Close cleanly
	_ = st.Close()

	// Now append a torn/corrupted tail (e.g. 5 garbage bytes representing a half-written record)
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	// Write length 100 but only provide 3 bytes
	_, _ = f.Write([]byte{0x80, 0x01, 0xAA, 0xBB})
	_ = f.Sync()
	_ = f.Close()

	// Reopen store — recovery should detect the torn tail, truncate it, and successfully recover
	st2, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatalf("recovery failed on torn write: %v", err)
	}
	defer st2.Close()

	// Verify all 5 valid messages were preserved
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("torn-msg-%d", i)
		m, ok, err := st2.Get(ctx, id)
		if !ok || err != nil || m.Status != MessageQueued {
			t.Fatalf("expected message %s to survive torn write: ok=%v status=%v", id, ok, m.Status)
		}
	}

	// Verify appending new messages succeeds after torn write recovery
	newMsg := Message{
		ID:        "after-torn-msg",
		Queue:     "torn-queue",
		Payload:   []byte("new payload"),
		CreatedAt: time.Now(),
	}
	if _, err := st2.Append(ctx, newMsg, []Destination{{Kind: DestinationQueue, Name: "torn-queue"}}); err != nil {
		t.Fatalf("append after torn write recovery failed: %v", err)
	}
}

func TestSegmentedWALBWL1Migration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create legacy BWL1 store
	legacyStore, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("create legacy store: %v", err)
	}

	_ = legacyStore.DeclareQueue(ctx, QueueConfig{Name: "migrated-queue"})
	for i := 1; i <= 10; i++ {
		msg := Message{
			ID:        fmt.Sprintf("mig-msg-%d", i),
			Queue:     "migrated-queue",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			CreatedAt: time.Now(),
		}
		if _, err := legacyStore.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "migrated-queue"}}); err != nil {
			t.Fatalf("legacy append: %v", err)
		}
	}
	_ = legacyStore.Close()

	// Now open SegmentedWALStore on the same directory
	segStore, err := NewSegmentedWALStore(SegmentedWALConfig{
		Dir:              dir,
		DisableCompactor: true,
	})
	if err != nil {
		t.Fatalf("migrate to segmented store: %v", err)
	}
	defer segStore.Close()

	// Manifest should be created
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing after migration: %v", err)
	}

	// Legacy file should be renamed to .migrated
	if _, err := os.Stat(filepath.Join(dir, "broker.wal.fwb.migrated")); err != nil {
		t.Fatalf("expected broker.wal.fwb.migrated to exist: %v", err)
	}

	// All 10 migrated messages must exist and be queued
	for i := 1; i <= 10; i++ {
		id := fmt.Sprintf("mig-msg-%d", i)
		m, ok, err := segStore.Get(ctx, id)
		if !ok || err != nil || m.Status != MessageQueued {
			t.Fatalf("migrated message %s not found: ok=%v status=%v", id, ok, m.Status)
		}
	}
}

func TestSegmentedWALJSONMigration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	legacyStore, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.DeclareQueue(ctx, QueueConfig{Name: "json-migrated"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		msg := Message{ID: fmt.Sprintf("json-mig-%d", i), Payload: []byte("payload")}
		if _, err := legacyStore.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "json-migrated"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}

	segStore, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		t.Fatalf("migrate JSON WAL: %v", err)
	}
	defer segStore.Close()
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("json-mig-%d", i)
		msg, ok, err := segStore.Get(ctx, id)
		if err != nil || !ok || msg.Queue != "json-migrated" {
			t.Fatalf("migrated message %q missing: ok=%v message=%+v err=%v", id, ok, msg, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "broker.wal.jsonl.migrated")); err != nil {
		t.Fatalf("legacy JSON WAL was not preserved: %v", err)
	}
}

func TestSegmentedWALJSONMigrationFailurePreservesSource(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "broker.wal.jsonl")
	if err := os.WriteFile(legacyPath, []byte("{not-json}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true}); err == nil {
		t.Fatal("expected corrupt legacy WAL migration to fail")
	}
	if got, err := os.ReadFile(legacyPath); err != nil || string(got) != "{not-json}\n" {
		t.Fatalf("legacy source was not preserved: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial BWL2 manifest survived failed migration: %v", err)
	}
}

func TestSegmentedWALMoveIDsAreStableAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{"source", "copies"} {
		if err := store.DeclareQueue(ctx, QueueConfig{Name: queue}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, Message{ID: "move-me"}, []Destination{{Kind: DestinationQueue, Name: "source"}}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Move(ctx, MoveRequest{SourceType: DestinationQueue, Source: "source", DestinationType: DestinationQueue, Destination: "copies", MessageIDs: []string{"move-me"}, Mode: MoveCopy})
	if err != nil || result.Copied != 1 {
		t.Fatalf("move result=%+v err=%v", result, err)
	}
	before, err := store.ListQueue(ctx, "copies", 10)
	if err != nil || len(before) != 1 {
		t.Fatalf("before restart: messages=%+v err=%v", before, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.ListQueue(ctx, "copies", 10)
	if err != nil || len(after) != 1 || after[0].ID != before[0].ID {
		t.Fatalf("after restart: messages=%+v want ID=%q err=%v", after, before[0].ID, err)
	}
}
