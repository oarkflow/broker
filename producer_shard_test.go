package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func sequencedEntry(id string, seq uint64) AppendEntry {
	return AppendEntry{Message: Message{ID: id, Key: id, ProducerID: "producer", ProducerEpoch: 1, ProducerSequence: &seq}, Destinations: []Destination{{Kind: DestinationQueue, Name: "jobs"}}}
}

func TestPartitionedProducerSequenceAcrossKeysAndRestart(t *testing.T) {
	for _, mode := range []string{"single", "atomic", "minimal", "owned"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewPartitionedBinaryWALStore(dir, 4, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			appendEntry := func(entry AppendEntry) error {
				switch mode {
				case "single":
					_, err := s.Append(context.Background(), entry.Message, entry.Destinations)
					return err
				case "atomic":
					_, err := s.AppendAtomic(context.Background(), []AppendEntry{entry})
					return err
				case "minimal":
					return s.AppendAtomicMinimal(context.Background(), []AppendEntry{entry})
				default:
					return s.AppendAtomicOwned(context.Background(), []AppendEntry{entry})
				}
			}
			for seq := uint64(0); seq < 8; seq++ {
				if err := appendEntry(sequencedEntry(fmt.Sprint(seq), seq)); err != nil {
					t.Fatalf("sequence %d: %v", seq, err)
				}
			}
			checkRetry := func() {
				t.Helper()
				var duplicate *DuplicateProducerSequenceError
				if err := appendEntry(sequencedEntry("retry-different-key", 7)); !errors.As(err, &duplicate) || duplicate.MessageID != "7" {
					t.Fatalf("retry did not find original: %v", err)
				}
				if err := appendEntry(sequencedEntry("gap", 10)); !errors.Is(err, ErrOutOfOrderSequence) {
					t.Fatalf("gap accepted: %v", err)
				}
				entry := sequencedEntry("fenced", 8)
				entry.Message.ProducerEpoch = 0
				if err := appendEntry(entry); !errors.Is(err, ErrProducerFenced) {
					t.Fatalf("old epoch accepted: %v", err)
				}
			}
			checkRetry()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = NewPartitionedBinaryWALStore(dir, 4, nil)
			if err != nil {
				t.Fatal(err)
			}
			checkRetry()
			if err := appendEntry(sequencedEntry("next", 8)); err != nil {
				t.Fatalf("next sequence after restart: %v", err)
			}
		})
	}
}

func TestPartitionedProducerConcurrentRetry(t *testing.T) {
	s, err := NewPartitionedBinaryWALStore(t.TempDir(), 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Go(func() {
			entry := sequencedEntry(fmt.Sprint(i), 0)
			_, err := s.Append(context.Background(), entry.Message, entry.Destinations)
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrDuplicateMessage) {
				t.Errorf("append: %v", err)
			}
		})
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("%d copies accepted", successes.Load())
	}
}

func TestPartitionedProducerStreamPlacementFailsBeforeMutation(t *testing.T) {
	s, err := NewPartitionedBinaryWALStore(t.TempDir(), 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := sequencedEntry("first", 0)
	first.Destinations = []Destination{{Kind: DestinationStream, Name: "events"}}
	if _, err := s.Append(context.Background(), first.Message, first.Destinations); err != nil {
		t.Fatal(err)
	}
	other := ""
	for i := 0; ; i++ {
		other = fmt.Sprintf("other-%d", i)
		if s.shardForStable("stream:"+other) != s.shardForStable("stream:events") {
			break
		}
	}
	second := sequencedEntry("second", 1)
	second.Destinations = []Destination{{Kind: DestinationStream, Name: other}}
	if _, err := s.AppendAtomic(context.Background(), []AppendEntry{second}); !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("cross-shard producer accepted: %v", err)
	}
	second.Destinations = first.Destinations
	if _, err := s.Append(context.Background(), second.Message, second.Destinations); err != nil {
		t.Fatalf("rejected attempt consumed sequence: %v", err)
	}
}

func TestPartitionedProducerRecoversLegacyPlacement(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			dir := t.TempDir()
			count := 1
			if split {
				count = 2
			}
			for i := 0; i < count; i++ {
				shard, err := NewBinaryFileWALStore(filepath.Join(dir, fmt.Sprintf("shard-%04d", i)))
				if err != nil {
					t.Fatal(err)
				}
				entry := sequencedEntry(fmt.Sprint(i), uint64(i))
				_, appendErr := shard.Append(context.Background(), entry.Message, entry.Destinations)
				closeErr := shard.Close()
				if err := errors.Join(appendErr, closeErr); err != nil {
					t.Fatal(err)
				}
			}
			s, err := NewPartitionedBinaryWALStore(dir, 4, nil)
			if split {
				if !errors.Is(err, ErrStorageUnavailable) {
					t.Fatalf("split producer history should fail closed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			entry := sequencedEntry("next", 1)
			if _, err := s.Append(context.Background(), entry.Message, entry.Destinations); err != nil {
				t.Fatalf("legacy placement lost: %v", err)
			}
		})
	}
}

func TestProducerRegistryOwnsStateAndRejectsStaleSequences(t *testing.T) {
	r := NewProducerRegistry()
	state, err := r.RegisterProducer("producer", 2)
	if err != nil {
		t.Fatal(err)
	}
	state.Epoch = 99
	if _, _, err := r.CheckAndRecord("producer", 1, 0, "old"); !errors.Is(err, ErrProducerFenced) {
		t.Fatalf("uninitialized producer accepted old epoch: %v", err)
	}
	for seq := uint64(0); seq < 2; seq++ {
		if _, _, err := r.CheckAndRecord("producer", 2, seq, fmt.Sprint(seq)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.CheckAndRecord("producer", 2, 0, "retry-old"); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("stale sequence incorrectly acknowledged as latest: %v", err)
	}
}
