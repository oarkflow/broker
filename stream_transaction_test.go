package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
)

var streamTransactionStores = []struct {
	name string
	open func(string) (Store, error)
}{
	{"memory", func(string) (Store, error) { return NewMemoryStore(), nil }},
	{"json", func(dir string) (Store, error) { return NewFileWALStore(dir) }},
	{"binary", func(dir string) (Store, error) { return NewBinaryFileWALStore(dir) }},
	{"segmented", func(dir string) (Store, error) {
		return NewSegmentedWALStore(SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	}},
	{"sharded", func(dir string) (Store, error) { return NewPartitionedBinaryWALStore(dir, 4, nil) }},
}

func seedTransactionInput(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.DeclareStream(ctx, StreamConfig{Name: "input"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := store.Append(ctx, Message{ID: fmt.Sprintf("input-%d", i)}, []Destination{{Kind: DestinationStream, Name: "input"}}); err != nil {
			t.Fatal(err)
		}
	}
}

func transactionOutput(store Store, id string) AppendEntry {
	entry := AppendEntry{Message: Message{ID: id, Payload: []byte("output")}, Destinations: []Destination{{Kind: DestinationQueue, Name: "jobs"}}}
	if sharded, ok := store.(*PartitionedBinaryWALStore); ok {
		for i := 0; ; i++ {
			entry.Message.Key = fmt.Sprint(i)
			if sharded.shardForMessage(entry.Message, entry.Destinations) == sharded.shardForStable("stream:input") {
				break
			}
		}
	}
	return entry
}

func TestStreamTransactionCommitConflictAndRestart(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			seedTransactionInput(t, s)
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
			out, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx)
			if err != nil || len(out) != 1 || out[0].ID != "out" {
				t.Fatalf("commit output=%v, error=%v", out, err)
			}
			tx.Entries = []AppendEntry{transactionOutput(s, "stale")}
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx); !errors.Is(err, ErrOffsetConflict) {
				t.Fatalf("stale transaction accepted: %v", err)
			}
			assertState := func() {
				t.Helper()
				offset, err := s.ConsumerOffset(context.Background(), tx.Offset)
				if err != nil || offset != 1 {
					t.Fatalf("offset=%d, error=%v", offset, err)
				}
				messages, err := s.ListQueue(context.Background(), "jobs", 10)
				if err != nil || len(messages) != 1 || messages[0].ID != "out" {
					t.Fatalf("outputs=%v, error=%v", messages, err)
				}
			}
			assertState()
			if tc.name != "memory" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = tc.open(dir)
				if err != nil {
					t.Fatal(err)
				}
				assertState()
			}
		})
	}
}

func TestStreamTransactionRejectsInvalidOutputsWithoutMutation(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seedTransactionInput(t, s)
			first := transactionOutput(s, "out")
			invalid := transactionOutput(s, "bad")
			invalid.Destinations[0].Kind = "invalid"
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{first, invalid}}
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("invalid output accepted: %v", err)
			}
			if _, ok, err := s.Get(context.Background(), "out"); err != nil || ok {
				t.Fatalf("partial output exists=%v, error=%v", ok, err)
			}
			offset, err := s.ConsumerOffset(context.Background(), tx.Offset)
			if err != nil || offset != 0 {
				t.Fatalf("rejected transaction advanced offset=%d, error=%v", offset, err)
			}
			tx.Entries = []AppendEntry{first}
			tx.Offset.Offset = 3
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("offset beyond stream end accepted: %v", err)
			}
			tx.Offset.Offset = 1
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, tx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled transaction accepted: %v", err)
			}
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx); err != nil {
				t.Fatalf("rejected attempts consumed state: %v", err)
			}
		})
	}
}

func TestStreamTransactionConcurrentCompareAndCommit(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seedTransactionInput(t, s)
			var successes atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Go(func() {
					tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, fmt.Sprintf("out-%d", i))}}
					_, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx)
					if err == nil {
						successes.Add(1)
					} else if !errors.Is(err, ErrOffsetConflict) {
						t.Errorf("competing transaction: %v", err)
					}
				})
			}
			wg.Wait()
			if successes.Load() != 1 {
				t.Fatalf("%d competing transactions committed", successes.Load())
			}
			messages, err := s.ListQueue(context.Background(), "jobs", 100)
			if err != nil || len(messages) != 1 {
				t.Fatalf("outputs=%d, error=%v", len(messages), err)
			}
		})
	}
}

func TestStreamTransactionVisibilityAndPersistenceFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s := NewMemoryStore()
			seedTransactionInput(t, s)
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := durableStreamTransaction(context.Background(), s, tx, func(walRecord) error {
					close(entered)
					<-release
					if fail {
						return ErrStorageUnavailable
					}
					return nil
				})
				done <- err
			}()
			<-entered
			snapshot := s.Snapshot()
			if snapshot.Offsets["input"]["workers"] != 0 || snapshot.Messages["out"].ID != "" {
				t.Error("transaction visible before persistence completed")
			}
			close(release)
			err := <-done
			if fail && !errors.Is(err, ErrStorageUnavailable) || !fail && err != nil {
				t.Fatalf("commit error=%v", err)
			}
			snapshot = s.Snapshot()
			if (snapshot.Offsets["input"]["workers"] == 1) != !fail || (snapshot.Messages["out"].ID == "out") != !fail {
				t.Fatal("output and offset did not commit together")
			}
		})
	}
}

// The child exits immediately after fsync without applying the record or closing
// the store. Reopening must install the output and offset from that one record.
func TestStreamTransactionCrashRecovery(t *testing.T) {
	if kind := os.Getenv("BROKER_STREAM_TX_CRASH_CHILD"); kind != "" {
		var open func(string) (Store, error)
		for _, tc := range streamTransactionStores {
			if tc.name == kind {
				open = tc.open
			}
		}
		s, err := open(os.Getenv("BROKER_STREAM_TX_CRASH_DIR"))
		if err != nil {
			t.Fatal(err)
		}
		seedTransactionInput(t, s)
		var mem *MemoryStore
		var persist func(walRecord) error
		switch st := s.(type) {
		case *FileWALStore:
			mem, persist = st.MemoryStore, st.write
		case *BinaryFileWALStore:
			mem, persist = st.MemoryStore, st.write
		case *SegmentedWALStore:
			mem, persist = st.MemoryStore, st.write
		}
		_, err = durableStreamTransaction(context.Background(), mem, StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}, func(r walRecord) error {
			if err := persist(r); err != nil {
				return err
			}
			os.Exit(0)
			return nil
		})
		t.Fatalf("child did not exit at persistence boundary: %v", err)
	}
	for _, tc := range streamTransactionStores {
		if tc.name == "memory" || tc.name == "sharded" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestStreamTransactionCrashRecovery$")
			cmd.Env = append(os.Environ(), "BROKER_STREAM_TX_CRASH_CHILD="+tc.name, "BROKER_STREAM_TX_CRASH_DIR="+dir)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("crash child: %v\n%s", err, out)
			}
			s, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			offset, err := s.ConsumerOffset(context.Background(), OffsetCommit{Stream: "input", ConsumerGroup: "workers"})
			if err != nil || offset != 1 {
				t.Fatalf("recovered offset=%d, error=%v", offset, err)
			}
			if _, exists, err := s.Get(context.Background(), "out"); err != nil || !exists {
				t.Fatalf("recovered output exists=%v, error=%v", exists, err)
			}
		})
	}
}

func TestStreamTransactionTenantIsolationAndRestart(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			seedTransactionInput(t, s)
			ctx := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Tenants: []string{"tenant-a"}})
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, tx); err != nil {
				t.Fatal(err)
			}
			bad := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "other-group", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "bad")}}
			bad.Entries[0].Message.TenantID = "tenant-b"
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, bad); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("cross-tenant output accepted: %v", err)
			}
			if tc.name != "memory" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = tc.open(dir)
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, tenant := range []string{"tenant-a", "tenant-b", ""} {
				offset, err := s.ConsumerOffset(context.Background(), OffsetCommit{Stream: "input", ConsumerGroup: "workers", TenantID: tenant})
				want := uint64(0)
				if tenant == "tenant-a" {
					want = 1
				}
				if err != nil || offset != want {
					t.Fatalf("tenant %q offset=%d, want=%d, error=%v", tenant, offset, want, err)
				}
			}
			if message, ok, err := s.Get(ctx, "out"); err != nil || !ok || message.TenantID != "tenant-a" {
				t.Fatalf("tenant output=%+v, exists=%v, error=%v", message, ok, err)
			}
		})
	}
}

func TestStreamTransactionRejectsOutputLossAndIDCollisions(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seedTransactionInput(t, s)
			ctx := context.Background()
			if err := s.DeclareQueue(ctx, QueueConfig{Name: "jobs", MaxDepth: 1, Backpressure: BackpressureDropOldest}); err != nil {
				t.Fatal(err)
			}
			old := transactionOutput(s, "old")
			if _, err := s.Append(ctx, old.Message, old.Destinations); err != nil {
				t.Fatal(err)
			}
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, tx); !errors.Is(err, ErrBackpressure) {
				t.Fatalf("transaction dropped queued output: %v", err)
			}
			messages, err := s.ListQueue(ctx, "jobs", 10)
			if err != nil || len(messages) != 1 || messages[0].ID != "old" {
				t.Fatalf("rejected transaction changed queue: %v, %v", messages, err)
			}
			if _, err := s.PurgeQueue(ctx, "jobs"); err != nil {
				t.Fatal(err)
			}
			tx.Entries[0] = transactionOutput(s, "input-0")
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, tx); !errors.Is(err, ErrDuplicateMessage) {
				t.Fatalf("transaction overwrote input message: %v", err)
			}
			tx.Entries = nil
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, tx); err != nil {
				t.Fatalf("offset-only transaction failed: %v", err)
			}
		})
	}
}

func TestStreamTransactionWALFailureDoesNotAdvanceOffset(t *testing.T) {
	for _, tc := range streamTransactionStores {
		if tc.name == "memory" || tc.name == "sharded" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seedTransactionInput(t, s)
			var file *os.File
			switch st := s.(type) {
			case *FileWALStore:
				file = st.file
			case *BinaryFileWALStore:
				file = st.file
			case *SegmentedWALStore:
				file = st.activeFile
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
			if out, err := s.(StreamTransactionStore).CommitStreamTransaction(context.Background(), tx); !errors.Is(err, ErrStorageUnavailable) || len(out) != 0 {
				t.Fatalf("failed WAL commit output=%v, error=%v", out, err)
			}
			offset, err := s.ConsumerOffset(context.Background(), tx.Offset)
			if err != nil || offset != 0 {
				t.Fatalf("failed WAL advanced offset=%d, error=%v", offset, err)
			}
			if _, ok, err := s.Get(context.Background(), "out"); err != nil || ok {
				t.Fatalf("failed WAL exposed output=%v, error=%v", ok, err)
			}
		})
	}
}

func TestStreamTransactionRejectsCrossShardOutputsBeforeWriting(t *testing.T) {
	s, err := NewPartitionedBinaryWALStore(t.TempDir(), 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedTransactionInput(t, s)
	local := transactionOutput(s, "local")
	other := transactionOutput(s, "other")
	for i := 0; ; i++ {
		other.Message.Key = fmt.Sprintf("other-%d", i)
		if s.shardForMessage(other.Message, other.Destinations) != s.shardForStable("stream:input") {
			break
		}
	}
	tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{local, other}}
	if _, err := s.CommitStreamTransaction(context.Background(), tx); !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("cross-shard transaction accepted: %v", err)
	}
	for _, id := range []string{"local", "other"} {
		if _, ok, err := s.Get(context.Background(), id); err != nil || ok {
			t.Fatalf("rejected transaction exposed %q: exists=%v, error=%v", id, ok, err)
		}
	}
	if offset, err := s.ConsumerOffset(context.Background(), tx.Offset); err != nil || offset != 0 {
		t.Fatalf("rejected transaction advanced offset=%d, error=%v", offset, err)
	}
}

func TestStreamTransactionSurvivesSegmentCompaction(t *testing.T) {
	dir := t.TempDir()
	cfg := SegmentedWALConfig{Dir: dir, MaxSegmentSize: 128, MaxSegments: 2, DisableCompactor: true}
	s, err := NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seedTransactionInput(t, s)
	tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{transactionOutput(s, "out")}}
	if _, err := s.CommitStreamTransaction(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		entry := transactionOutput(s, fmt.Sprintf("filler-%d", i))
		if _, err := s.Append(context.Background(), entry.Message, entry.Destinations); err != nil {
			t.Fatal(err)
		}
	}
	if err := newSegmentCompactor(s).Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.manifest.CheckpointFile == "" {
		t.Fatal("test did not create a compaction checkpoint")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSegmentedWALStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if offset, err := s.ConsumerOffset(context.Background(), tx.Offset); err != nil || offset != 1 {
		t.Fatalf("checkpoint lost offset=%d, error=%v", offset, err)
	}
	if _, exists, err := s.Get(context.Background(), "out"); err != nil || !exists {
		t.Fatalf("checkpoint lost output: exists=%v, error=%v", exists, err)
	}
	tx.ExpectedOffset, tx.Offset.Offset = 1, 2
	tx.Entries[0].Message.ID = "next"
	if _, err := s.CommitStreamTransaction(context.Background(), tx); err != nil {
		t.Fatalf("commit after checkpoint recovery: %v", err)
	}
}

func TestStreamTransactionCoLocatesUnkeyedQueueOutputs(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPartitionedBinaryWALStore(dir, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	seedTransactionInput(t, s)
	entry := AppendEntry{Destinations: []Destination{{Kind: DestinationQueue, Name: "jobs"}}}
	idx := s.shardForStable("stream:input")
	for i := 0; ; i++ {
		entry.Message.ID = fmt.Sprintf("unkeyed-%d", i)
		if s.shardForMessage(entry.Message, entry.Destinations) != idx {
			break
		}
	}
	tx := StreamTransaction{Offset: OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, Entries: []AppendEntry{entry}}
	if _, err := s.CommitStreamTransaction(context.Background(), tx); err != nil {
		t.Fatalf("unkeyed output was not co-located: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewPartitionedBinaryWALStore(dir, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if shard, err := s.lookupShard(context.Background(), entry.Message.ID); err != nil || shard != idx {
		t.Fatalf("recovered output shard=%d, want=%d, error=%v", shard, idx, err)
	}
}

func TestStreamTransactionAfterTenantOffsetCommitRecovers(t *testing.T) {
	for _, tc := range streamTransactionStores {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := tc.open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			seedTransactionInput(t, s)
			ctx := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Tenants: []string{"tenant-a"}})
			offset := OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}
			if err := s.CommitOffset(ctx, offset); err != nil {
				t.Fatal(err)
			}
			offset.Offset = 2
			if _, err := s.(StreamTransactionStore).CommitStreamTransaction(ctx, StreamTransaction{Offset: offset, ExpectedOffset: 1, Entries: []AppendEntry{transactionOutput(s, "out")}}); err != nil {
				t.Fatal(err)
			}
			if tc.name != "memory" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = tc.open(dir)
				if err != nil {
					t.Fatal(err)
				}
			}
			if got, err := s.ConsumerOffset(ctx, offset); err != nil || got != 2 {
				t.Fatalf("mixed commit recovery offset=%d, error=%v", got, err)
			}
			if got, err := s.ConsumerOffset(context.Background(), offset); err != nil || got != 0 {
				t.Fatalf("tenant commit leaked to global group: offset=%d, error=%v", got, err)
			}
		})
	}
}
