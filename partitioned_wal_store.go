package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// PartitionedBinaryWALStore stripes messages across independent memory locks,
// WAL files, group-commit goroutines and fsync pipelines. Messages with the same
// idempotency, deduplication, key, or routing key are pinned to one shard.
// Atomic batches are atomic per shard; cross-shard atomicity requires an outbox
// or external transaction coordinator.
type PartitionedBinaryWALStore struct {
	shards          []*BinaryFileWALStore
	index           sync.Map // message ID -> shard index
	retainCompleted atomic.Bool
}

func (*PartitionedBinaryWALStore) ConfirmationLevel() ConfirmLevel { return ConfirmPersisted }

func (s *PartitionedBinaryWALStore) StoreHealth() error {
	if s == nil || len(s.shards) == 0 {
		return ErrClosed
	}
	var health error
	for i, shard := range s.shards {
		if err := shard.StoreHealth(); err != nil {
			health = errors.Join(health, fmt.Errorf("WAL shard %d: %w", i, err))
		}
	}
	return health
}

func (s *PartitionedBinaryWALStore) SetHealthCallback(fn func(error)) {
	for _, shard := range s.shards {
		shard.SetHealthCallback(fn)
	}
}

func NewPartitionedBinaryWALStore(dir string, shardCount int, codec FrameCodec) (*PartitionedBinaryWALStore, error) {
	if shardCount <= 0 {
		return nil, errf(ErrInvalidMessage, "WAL shard count must be positive")
	}
	if codec == nil {
		codec = NewBinaryMessageCodec()
	}
	s := &PartitionedBinaryWALStore{shards: make([]*BinaryFileWALStore, 0, shardCount)}
	s.retainCompleted.Store(true)
	for i := 0; i < shardCount; i++ {
		shard, err := NewBinaryFileWALStoreWithCodec(filepath.Join(dir, fmt.Sprintf("shard-%04d", i)), codec)
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("open WAL shard %d: %w", i, err)
		}
		s.shards = append(s.shards, shard)
	}
	s.rebuildIndex()
	return s, nil
}

func (s *PartitionedBinaryWALStore) ShardCount() int {
	if s == nil {
		return 0
	}
	return len(s.shards)
}
func (s *PartitionedBinaryWALStore) SetRetainCompleted(retain bool) {
	s.retainCompleted.Store(retain)
	for _, shard := range s.shards {
		shard.SetRetainCompleted(retain)
	}
	s.index.Range(func(key, value any) bool {
		s.index.Delete(key)
		return true
	})
	s.rebuildIndex()
}
func (s *PartitionedBinaryWALStore) SetHistoryLimit(limit int) {
	for _, shard := range s.shards {
		shard.SetHistoryLimit(limit)
	}
}
func (s *PartitionedBinaryWALStore) rebuildIndex() {
	for i, shard := range s.shards {
		shard.mu.RLock()
		for id := range shard.messages {
			s.index.Store(id, i)
		}
		shard.mu.RUnlock()
	}
}
func stableShardHash(value string) uint64 {
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	h := offset64
	for i := 0; i < len(value); i++ {
		h ^= uint64(value[i])
		h *= prime64
	}
	return h
}
func (s *PartitionedBinaryWALStore) shardForStable(value string) int {
	if len(s.shards) == 1 {
		return 0
	}
	return int(stableShardHash(value) % uint64(len(s.shards)))
}
func (s *PartitionedBinaryWALStore) shardForMessage(msg Message, dests []Destination) int {
	for _, dest := range dests {
		if dest.Kind == DestinationStream || dest.Kind == DestinationArchive {
			return s.shardForStable("stream:" + dest.Name)
		}
	}
	key := firstNonEmpty(msg.IdempotencyKey, firstNonEmpty(msg.DeduplicationID, firstNonEmpty(msg.Key, firstNonEmpty(msg.RoutingKey, msg.ID))))
	return s.shardForStable(key)
}
func (s *PartitionedBinaryWALStore) remember(shard int, messages []Message) {
	for i := range messages {
		s.index.Store(messages[i].ID, shard)
	}
}
func (s *PartitionedBinaryWALStore) rememberEntryIDs(shard int, entry AppendEntry) {
	dests := entry.Destinations
	if len(dests) == 0 {
		dests = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
	}
	copyNo := 0
	for _, dest := range dests {
		if dest.Name == "" {
			continue
		}
		id := entry.Message.ID
		if copyNo > 0 {
			id += ":copy:" + itoa(copyNo)
		}
		s.index.Store(id, shard)
		copyNo++
	}
}
func (s *PartitionedBinaryWALStore) lookupShard(ctx context.Context, id string) (int, error) {
	if value, ok := s.index.Load(id); ok {
		return value.(int), nil
	}
	for i, shard := range s.shards {
		if _, ok, err := shard.Get(ctx, id); err != nil {
			return 0, err
		} else if ok || shard.wasCompleted(id) {
			s.index.Store(id, i)
			return i, nil
		}
	}
	return 0, ErrMessageNotFound
}
func (s *PartitionedBinaryWALStore) forEachShard(fn func(*BinaryFileWALStore) error) error {
	if s == nil || len(s.shards) == 0 {
		return ErrClosed
	}
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, shard := range s.shards {
		wg.Add(1)
		go func(i int, shard *BinaryFileWALStore) { defer wg.Done(); errs[i] = fn(shard) }(i, shard)
	}
	wg.Wait()
	var joined error
	for i, err := range errs {
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("WAL shard %d: %w", i, err))
		}
	}
	return joined
}

func (s *PartitionedBinaryWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	return s.forEachShard(func(sh *BinaryFileWALStore) error { return sh.DeclareQueue(ctx, cfg) })
}
func (s *PartitionedBinaryWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	return s.forEachShard(func(sh *BinaryFileWALStore) error { return sh.DeclareTopic(ctx, cfg) })
}
func (s *PartitionedBinaryWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	return s.shards[s.shardForStable("stream:"+cfg.Name)].DeclareStream(ctx, cfg)
}
func (s *PartitionedBinaryWALStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	idx := s.shardForMessage(msg, dests)
	out, err := s.shards[idx].Append(ctx, msg, dests)
	if len(out) > 0 {
		s.remember(idx, out)
	}
	return out, err
}

func (s *PartitionedBinaryWALStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	type group struct {
		entries []AppendEntry
		input   []int
		results map[int][]Message
		err     error
	}
	groups := make([]group, len(s.shards))
	for i := range groups {
		groups[i].results = make(map[int][]Message)
	}
	for input, entry := range entries {
		idx := s.shardForMessage(entry.Message, entry.Destinations)
		groups[idx].entries = append(groups[idx].entries, entry)
		groups[idx].input = append(groups[idx].input, input)
	}
	var wg sync.WaitGroup
	for idx := range groups {
		g := &groups[idx]
		if len(g.entries) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, g *group) {
			defer wg.Done()
			stored, err := s.shards[idx].AppendAtomic(ctx, g.entries)
			g.err = err
			pos := 0
			for local, entry := range g.entries {
				count := nonEmptyDestinationCount(entry.Message, entry.Destinations)
				end := pos + count
				if end > len(stored) {
					end = len(stored)
				}
				if pos < end {
					result := stored[pos:end]
					g.results[g.input[local]] = result
					s.remember(idx, result)
				}
				pos = end
			}
		}(idx, g)
	}
	wg.Wait()
	var joined error
	for i := range groups {
		if groups[i].err != nil {
			joined = errors.Join(joined, fmt.Errorf("WAL shard %d: %w", i, groups[i].err))
		}
	}
	if joined != nil {
		return nil, joined
	}
	out := make([]Message, 0, len(entries))
	for i, entry := range entries {
		out = append(out, groups[s.shardForMessage(entry.Message, entry.Destinations)].results[i]...)
	}
	return out, nil
}
func (s *PartitionedBinaryWALStore) AppendAtomicMinimal(ctx context.Context, entries []AppendEntry) error {
	groups := make([][]AppendEntry, len(s.shards))
	for _, entry := range entries {
		idx := s.shardForMessage(entry.Message, entry.Destinations)
		groups[idx] = append(groups[idx], entry)
	}
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, entries []AppendEntry) {
			defer wg.Done()
			if err := s.shards[idx].AppendAtomicMinimal(ctx, entries); err != nil {
				errs[idx] = err
				return
			}
			for _, entry := range entries {
				s.rememberEntryIDs(idx, entry)
			}
		}(idx, groups[idx])
	}
	wg.Wait()
	var joined error
	for i, err := range errs {
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("WAL shard %d: %w", i, err))
		}
	}
	return joined
}
func (s *PartitionedBinaryWALStore) AppendAtomicOwned(ctx context.Context, entries []AppendEntry) error {
	groups := make([][]AppendEntry, len(s.shards))
	for _, entry := range entries {
		idx := s.shardForMessage(entry.Message, entry.Destinations)
		groups[idx] = append(groups[idx], entry)
	}
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, entries []AppendEntry) {
			defer wg.Done()
			if err := s.shards[idx].AppendAtomicOwned(ctx, entries); err != nil {
				errs[idx] = err
				return
			}
			for _, entry := range entries {
				s.rememberEntryIDs(idx, entry)
			}
		}(idx, groups[idx])
	}
	wg.Wait()
	var joined error
	for i, err := range errs {
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("WAL shard %d: %w", i, err))
		}
	}
	return joined
}
func (s *PartitionedBinaryWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	if req.Limit <= 0 {
		req.Limit = 1
	}
	start := s.shardForStable(req.ConsumerGroup + "\x00" + req.ConsumerID)
	out := make([]Message, 0, req.Limit)
	var joined error
	for offset := 0; offset < len(s.shards) && len(out) < req.Limit; offset++ {
		idx := (start + offset) % len(s.shards)
		shardReq := req
		shardReq.Limit = req.Limit - len(out)
		msgs, err := s.shards[idx].Lease(ctx, shardReq)
		if err != nil {
			if errors.Is(err, ErrQueueNotFound) {
				continue
			}
			joined = errors.Join(joined, fmt.Errorf("WAL shard %d: %w", idx, err))
			continue
		}
		if len(msgs) > 0 {
			s.remember(idx, msgs)
			out = append(out, msgs...)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	return nil, joined
}
func (s *PartitionedBinaryWALStore) Ack(ctx context.Context, req AckRequest) error {
	idx, err := s.lookupShard(ctx, req.MessageID)
	if err != nil {
		return err
	}
	err = s.shards[idx].Ack(ctx, req)
	if err == nil && !s.retainCompleted.Load() {
		s.index.Delete(req.MessageID)
	}
	return err
}
func (s *PartitionedBinaryWALStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	errs := make([]error, len(reqs))
	type item struct {
		req   AckRequest
		input int
	}
	groups := make([][]item, len(s.shards))
	for i, req := range reqs {
		idx, err := s.lookupShard(ctx, req.MessageID)
		if err != nil {
			errs[i] = err
			continue
		}
		groups[idx] = append(groups[idx], item{req, i})
	}
	var wg sync.WaitGroup
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, items []item) {
			defer wg.Done()
			batch := make([]AckRequest, len(items))
			for i := range items {
				batch[i] = items[i].req
			}
			shardErrs := s.shards[idx].AckBatch(ctx, batch)
			for i := range items {
				errs[items[i].input] = shardErrs[i]
				if shardErrs[i] == nil && !s.retainCompleted.Load() {
					s.index.Delete(items[i].req.MessageID)
				}
			}
		}(idx, groups[idx])
	}
	wg.Wait()
	return errs
}
func (s *PartitionedBinaryWALStore) Nack(ctx context.Context, req NackRequest) error {
	idx, err := s.lookupShard(ctx, req.MessageID)
	if err != nil {
		return err
	}
	return s.shards[idx].Nack(ctx, req)
}
func (s *PartitionedBinaryWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	idx, err := s.lookupShard(ctx, req.MessageID)
	if err != nil {
		return err
	}
	return s.shards[idx].ExtendLease(ctx, req)
}
func (s *PartitionedBinaryWALStore) Update(ctx context.Context, msg Message) error {
	idx, err := s.lookupShard(ctx, msg.ID)
	if err != nil {
		return err
	}
	return s.shards[idx].Update(ctx, msg)
}
func (s *PartitionedBinaryWALStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	errs := make([]error, len(messages))
	type item struct {
		msg   Message
		input int
	}
	groups := make([][]item, len(s.shards))
	for i, msg := range messages {
		idx, err := s.lookupShard(ctx, msg.ID)
		if err != nil {
			errs[i] = err
			continue
		}
		groups[idx] = append(groups[idx], item{msg, i})
	}
	var wg sync.WaitGroup
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, items []item) {
			defer wg.Done()
			batch := make([]Message, len(items))
			for i := range items {
				batch[i] = items[i].msg
			}
			shardErrs := s.shards[idx].UpdateBatch(ctx, batch)
			for i := range items {
				errs[items[i].input] = shardErrs[i]
			}
		}(idx, groups[idx])
	}
	wg.Wait()
	return errs
}
func (s *PartitionedBinaryWALStore) Get(ctx context.Context, id string) (Message, bool, error) {
	idx, err := s.lookupShard(ctx, id)
	if errors.Is(err, ErrMessageNotFound) {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, err
	}
	return s.shards[idx].Get(ctx, id)
}
func (s *PartitionedBinaryWALStore) History(ctx context.Context, id string) ([]MessageHistory, error) {
	idx, err := s.lookupShard(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.shards[idx].History(ctx, id)
}
func (s *PartitionedBinaryWALStore) ListQueue(ctx context.Context, name string, limit int) ([]Message, error) {
	return s.listAcross(ctx, limit, func(sh *BinaryFileWALStore) ([]Message, error) { return sh.ListQueue(ctx, name, limit) })
}
func (s *PartitionedBinaryWALStore) ListDLQ(ctx context.Context, name string, limit int) ([]Message, error) {
	return s.listAcross(ctx, limit, func(sh *BinaryFileWALStore) ([]Message, error) { return sh.ListDLQ(ctx, name, limit) })
}
func (s *PartitionedBinaryWALStore) listAcross(ctx context.Context, limit int, fn func(*BinaryFileWALStore) ([]Message, error)) ([]Message, error) {
	out := []Message{}
	var mu sync.Mutex
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		msgs, e := fn(sh)
		if e != nil && !errors.Is(e, ErrQueueNotFound) {
			return e
		}
		mu.Lock()
		out = append(out, msgs...)
		mu.Unlock()
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}
func (s *PartitionedBinaryWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	result := MoveResult{Destination: req.Destination, DryRun: req.DryRun}
	var mu sync.Mutex
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		r, e := sh.Move(ctx, req)
		mu.Lock()
		result.Moved += r.Moved
		result.Copied += r.Copied
		result.Skipped += r.Skipped
		result.MessageIDs = append(result.MessageIDs, r.MessageIDs...)
		mu.Unlock()
		return e
	})
	return result, err
}
func (s *PartitionedBinaryWALStore) PauseQueue(ctx context.Context, name string) error {
	return s.forEachShard(func(sh *BinaryFileWALStore) error { return sh.PauseQueue(ctx, name) })
}
func (s *PartitionedBinaryWALStore) ResumeQueue(ctx context.Context, name string) error {
	return s.forEachShard(func(sh *BinaryFileWALStore) error { return sh.ResumeQueue(ctx, name) })
}
func (s *PartitionedBinaryWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	total := 0
	var mu sync.Mutex
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		n, e := sh.PurgeQueue(ctx, name)
		mu.Lock()
		total += n
		mu.Unlock()
		return e
	})
	return total, err
}
func (s *PartitionedBinaryWALStore) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	return s.shards[s.shardForStable("stream:"+req.Stream)].ReadStream(ctx, req)
}
func (s *PartitionedBinaryWALStore) CommitOffset(ctx context.Context, c OffsetCommit) error {
	return s.shards[s.shardForStable("stream:"+c.Stream)].CommitOffset(ctx, c)
}
func (s *PartitionedBinaryWALStore) ConsumerOffset(ctx context.Context, c OffsetCommit) (uint64, error) {
	return s.shards[s.shardForStable("stream:"+c.Stream)].ConsumerOffset(ctx, c)
}
func (s *PartitionedBinaryWALStore) StreamLagContext(ctx context.Context, stream, group, tenant string) (uint64, error) {
	return s.shards[s.shardForStable("stream:"+stream)].StreamLagContext(ctx, stream, group, tenant)
}
func (s *PartitionedBinaryWALStore) StreamLagAllContext(ctx context.Context, stream, tenant string) (map[string]uint64, error) {
	return s.shards[s.shardForStable("stream:"+stream)].StreamLagAllContext(ctx, stream, tenant)
}
func (s *PartitionedBinaryWALStore) StreamHeadOffsetContext(ctx context.Context, stream, tenant string) (uint64, error) {
	return s.shards[s.shardForStable("stream:"+stream)].StreamHeadOffsetContext(ctx, stream, tenant)
}
func (s *PartitionedBinaryWALStore) Stats(ctx context.Context) (StoreStats, error) {
	result := StoreStats{Queues: map[string]int{}, Topics: map[string]int{}, Streams: map[string]int{}, Offsets: map[string]map[string]uint64{}, DLQ: map[string]int{}}
	var mu sync.Mutex
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		st, e := sh.Stats(ctx)
		if e != nil {
			return e
		}
		mu.Lock()
		defer mu.Unlock()
		for k, v := range st.Queues {
			result.Queues[k] += v
		}
		for k, v := range st.Topics {
			result.Topics[k] += v
		}
		for k, v := range st.Streams {
			result.Streams[k] += v
		}
		for k, v := range st.DLQ {
			result.DLQ[k] += v
		}
		for stream, groups := range st.Offsets {
			if result.Offsets[stream] == nil {
				result.Offsets[stream] = map[string]uint64{}
			}
			for group, off := range groups {
				if off > result.Offsets[stream][group] {
					result.Offsets[stream][group] = off
				}
			}
		}
		result.Messages += st.Messages
		result.Leased += st.Leased
		result.DeadLetter += st.DeadLetter
		return nil
	})
	return result, err
}
func (s *PartitionedBinaryWALStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	total := 0
	var mu sync.Mutex
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		n, e := sh.SweepLeases(ctx, now)
		mu.Lock()
		total += n
		mu.Unlock()
		return e
	})
	return total, err
}

// ListTopic gathers a topic's retained messages across shards. A topic is not
// partitioned in this store, so every shard is asked and the results are merged in
// publish order.
func (s *PartitionedBinaryWALStore) ListTopic(ctx context.Context, name string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 100
	}
	// forEachShard invokes the callback concurrently, one goroutine per shard, so
	// the accumulator needs a lock. Appending unguarded raced and silently dropped
	// messages.
	var mu sync.Mutex
	var all []Message
	err := s.forEachShard(func(sh *BinaryFileWALStore) error {
		msgs, err := sh.ListTopic(ctx, name, limit)
		if err != nil {
			if errors.Is(err, ErrTopicNotFound) {
				return nil
			}
			return err
		}
		mu.Lock()
		all = append(all, msgs...)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// ReadTopic resumes a topic read across shards from the message id last seen.
func (s *PartitionedBinaryWALStore) ReadTopic(ctx context.Context, name, afterID string, limit int) ([]Message, error) {
	msgs, err := s.ListTopic(ctx, name, 0)
	if err != nil {
		return nil, err
	}
	if afterID == "" {
		if limit > 0 && len(msgs) > limit {
			return msgs[:limit], nil
		}
		return msgs, nil
	}
	out := make([]Message, 0, len(msgs))
	found := false
	for _, m := range msgs {
		if !found {
			if m.ID == afterID {
				found = true
			}
			continue
		}
		out = append(out, m)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *PartitionedBinaryWALStore) Close() error {
	if s == nil {
		return nil
	}
	var joined error
	for i, sh := range s.shards {
		if err := sh.Close(); err != nil {
			joined = errors.Join(joined, fmt.Errorf("close WAL shard %d: %w", i, err))
		}
	}
	return joined
}

var _ Store = (*PartitionedBinaryWALStore)(nil)
var _ AtomicStore = (*PartitionedBinaryWALStore)(nil)
var _ MinimalAtomicStore = (*PartitionedBinaryWALStore)(nil)
var _ OwnedAtomicStore = (*PartitionedBinaryWALStore)(nil)
var _ BatchAckStore = (*PartitionedBinaryWALStore)(nil)
var _ BatchUpdateStore = (*PartitionedBinaryWALStore)(nil)
