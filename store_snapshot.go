package broker

import "time"

// MemoryQueueSnapshot captures queue configuration, state, and ordered IDs for
// durable snapshot stores. It is intentionally exported so contrib stores can
// persist/recover the default in-memory data-plane without reaching into
// MemoryStore internals.
type MemoryQueueSnapshot struct {
	Config QueueConfig `json:"config"`
	IDs    []string    `json:"ids"`
	State  QueueState  `json:"state"`
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
		Messages:  make(map[string]Message, len(s.messages)),
		History:   make(map[string][]MessageHistory, len(s.history)),
		Offsets:   make(map[string]map[string]uint64, len(s.offsets)),
		Dedup:     make(map[string]time.Time, len(s.dedup)),
		Idemp:     make(map[string]string, len(s.idemp)),
	}
	for name, q := range s.queues {
		snap.Queues[name] = MemoryQueueSnapshot{Config: q.cfg, IDs: append([]string(nil), q.ids...), State: q.state}
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
	return snap
}

// Restore replaces the current store with a snapshot. Leased/running messages
// are converted back to queued during restore so a crashed consumer cannot keep
// work invisible forever after process recovery.
func (s *MemoryStore) Restore(snap MemoryStoreSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = map[string]*memoryQueue{}
	s.topics = map[string]*memoryTopic{}
	s.streams = map[string]*memoryStream{}
	s.messages = map[string]*Message{}
	s.history = map[string][]MessageHistory{}
	s.offsets = map[string]map[string]uint64{}
	s.dedup = map[string]time.Time{}
	s.idemp = map[string]string{}
	for name, q := range snap.Queues {
		state := q.State
		if state == "" {
			state = QueueActive
		}
		s.queues[name] = &memoryQueue{cfg: q.Config, ids: append([]string(nil), q.IDs...), state: state}
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
		if m.Status == MessageLeased || m.Status == MessageProcessing || m.Status == MessageProgressing || m.Status == MessageDelivered {
			m.Status = MessageQueued
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			m.ConsumerID = ""
			m.Redeliveries++
		}
		s.messages[id] = &m
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
	s.closed = false
}
