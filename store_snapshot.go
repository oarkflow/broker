package broker

import "time"

// MemoryQueueSnapshot captures queue configuration, state, and ordered IDs for
// durable snapshot stores. It is intentionally exported so contrib stores can
// persist/recover the default in-memory data-plane without reaching into
// MemoryStore internals.
type MemoryQueueSnapshot struct {
	Config      QueueConfig `json:"config"`
	IDs         []string    `json:"ids"`
	State       QueueState  `json:"state"`
	Head        int         `json:"head,omitempty"`
	Depth       int         `json:"depth,omitempty"`
	HasPriority bool        `json:"has_priority,omitempty"`
}

type MemoryTopicSnapshot struct {
	Config TopicConfig `json:"config"`
	IDs    []string    `json:"ids"`
	State  QueueState  `json:"state"`
}

type MemoryStreamSnapshot struct {
	Config StreamConfig `json:"config"`
	IDs    []string     `json:"ids"`
	Offset uint64       `json:"offset"`
}

// MemoryStoreSnapshot is a complete durable image of a MemoryStore. It is used
// by lightweight SQL stores and backup/restore tooling. File-WAL remains the
// preferred high-throughput append path; snapshot stores are intended for
// embedded/single-node persistent deployments that prefer a SQL database.
type MemoryStoreSnapshot struct {
	Version   int                             `json:"version"`
	CreatedAt time.Time                       `json:"created_at"`
	Queues    map[string]MemoryQueueSnapshot  `json:"queues"`
	Topics    map[string]MemoryTopicSnapshot  `json:"topics"`
	Streams   map[string]MemoryStreamSnapshot `json:"streams"`
	Messages  map[string]Message              `json:"messages"`
	History   map[string][]MessageHistory     `json:"history"`
	Offsets   map[string]map[string]uint64    `json:"offsets"`
	Dedup     map[string]time.Time            `json:"dedup"`
	Idemp     map[string]string               `json:"idemp"`
	Producers map[string]ProducerState        `json:"producers,omitempty"`
	MoveOps   map[string]MoveResult           `json:"move_ops,omitempty"`
}

// Snapshot returns a consistent deep-copy image of the store.
func (s *MemoryStore) Snapshot() MemoryStoreSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := MemoryStoreSnapshot{
		Version:   1,
		CreatedAt: time.Now(),
		Queues:    make(map[string]MemoryQueueSnapshot, len(s.queues)),
		Topics:    make(map[string]MemoryTopicSnapshot, len(s.topics)),
		Streams:   make(map[string]MemoryStreamSnapshot, len(s.streams)),
		Messages:  make(map[string]Message, len(s.messages)+len(s.cold)),
		History:   make(map[string][]MessageHistory, len(s.history)),
		Offsets:   make(map[string]map[string]uint64, len(s.offsets)),
		Dedup:     make(map[string]time.Time, len(s.dedup)),
		Idemp:     make(map[string]string, len(s.idemp)),
		Producers: make(map[string]ProducerState, len(s.producerSeq)),
		MoveOps:   make(map[string]MoveResult, len(s.moveOps)),
	}
	for name, q := range s.queues {
		snap.Queues[name] = MemoryQueueSnapshot{Config: q.cfg, IDs: append([]string(nil), q.ids...), State: q.state, Head: q.head, Depth: q.depth, HasPriority: q.hasPriority}
	}
	for name, t := range s.topics {
		snap.Topics[name] = MemoryTopicSnapshot{Config: t.cfg, IDs: append([]string(nil), t.ids...), State: t.state}
	}
	for name, st := range s.streams {
		snap.Streams[name] = MemoryStreamSnapshot{Config: st.cfg, IDs: append([]string(nil), st.ids...), Offset: st.offset}
	}
	for id, m := range s.messages {
		snap.Messages[id] = cloneMessage(*m)
	}
	for id, slot := range s.cold {
		snap.Messages[id] = decodeCold(id, slot)
	}
	for id, h := range s.history {
		snap.History[id] = append([]MessageHistory(nil), h...)
	}
	for stream, groups := range s.offsets {
		snap.Offsets[stream] = make(map[string]uint64, len(groups))
		for group, off := range groups {
			snap.Offsets[stream][group] = off
		}
	}
	for k, v := range s.dedup {
		snap.Dedup[k] = v
	}
	for k, v := range s.idemp {
		snap.Idemp[k] = v
	}
	for k, v := range s.producerSeq {
		snap.Producers[k] = v
	}
	for k, v := range s.moveOps {
		snap.MoveOps[k] = v
	}
	return snap
}

// Restore replaces the current store with a snapshot. Leased/running messages
// are converted back to queued during restore so a crashed consumer cannot keep
// work invisible forever after process recovery.
func (s *MemoryStore) Restore(snap MemoryStoreSnapshot) {
	s.restoreSnapshot(snap, true)
}

// RestoreExact replaces the store with a consensus snapshot without changing
// lease state. Lease expiry is itself a replicated mutation and must not be
// decided independently by a recovering follower.
func (s *MemoryStore) RestoreExact(snap MemoryStoreSnapshot) {
	s.restoreSnapshot(snap, false)
}

func (s *MemoryStore) restoreSnapshot(snap MemoryStoreSnapshot, recoverInflight bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = map[string]*memoryQueue{}
	s.topics = map[string]*memoryTopic{}
	s.streams = map[string]*memoryStream{}
	s.messages = map[string]*Message{}
	s.cold = nil
	s.history = map[string][]MessageHistory{}
	s.offsets = map[string]map[string]uint64{}
	s.dedup = map[string]time.Time{}
	s.idemp = map[string]string{}
	s.producerSeq = map[string]ProducerState{}
	s.moveOps = map[string]MoveResult{}
	for name, q := range snap.Queues {
		state := q.State
		if state == "" {
			state = QueueActive
		}
		head := q.Head
		if head < 0 || head > len(q.IDs) {
			head = 0
		}
		s.queues[name] = &memoryQueue{cfg: q.Config, ids: append([]string(nil), q.IDs...), state: state, head: head, depth: q.Depth, hasPriority: q.HasPriority}
	}
	for name, t := range snap.Topics {
		state := t.State
		if state == "" {
			state = QueueActive
		}
		s.topics[name] = &memoryTopic{cfg: t.Config, ids: append([]string(nil), t.IDs...), state: state}
	}
	for name, st := range snap.Streams {
		s.streams[name] = &memoryStream{cfg: st.Config, ids: append([]string(nil), st.IDs...), offset: st.Offset}
	}
	for id, msg := range snap.Messages {
		m := cloneMessage(msg)
		if recoverInflight && (m.Status == MessageLeased || m.Status == MessageProcessing || m.Status == MessageProgressing || m.Status == MessageDelivered) {
			m.Status = MessageQueued
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			m.ConsumerID = ""
			m.Redeliveries++
		}
		s.messages[id] = &m
		s.noteLeasedLocked(&m)
	}
	// Version-1 snapshots did not persist queue counters. Reconstruct them when
	// absent so old backups remain readable and messages do not become invisible.
	for _, queue := range s.queues {
		if queue.depth != 0 || len(queue.ids) == 0 {
			continue
		}
		for _, id := range queue.ids[queue.head:] {
			v := s.scanViewLocked(id)
			if !v.exists || v.queue != queue.cfg.Name || isTerminalQueueStatus(v.status) {
				continue
			}
			queue.depth++
			if v.priority != 0 {
				queue.hasPriority = true
			}
		}
	}
	for id, h := range snap.History {
		s.history[id] = append([]MessageHistory(nil), h...)
	}
	for stream, groups := range snap.Offsets {
		s.offsets[stream] = make(map[string]uint64, len(groups))
		for group, off := range groups {
			s.offsets[stream][group] = off
		}
	}
	now := time.Now()
	for k, v := range snap.Dedup {
		if v.After(now) {
			s.dedup[k] = v
		}
	}
	for k, v := range snap.Idemp {
		s.idemp[k] = v
	}
	for k, v := range snap.Producers {
		s.producerSeq[k] = v
	}
	for k, v := range snap.MoveOps {
		s.moveOps[k] = v
	}
	if s.compact {
		s.demoteEligibleLocked()
	}
	s.closed = false
}
