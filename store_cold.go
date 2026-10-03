package broker

import "time"

// Compact backlog
//
// A queue that holds millions of undelivered messages pays for each one with a
// 752-byte Message struct, its strings, maps and payload: roughly 1.1 KB and
// about eight pointer-bearing heap objects per message, all of which the Go GC
// must scan on every cycle. Messages that have never been leased do not need
// that form. With SetCompactBacklog(true), a freshly queued message is kept as a
// coldSlot instead: its lossless BWL1 encoding (one noscan []byte) plus a small
// summary holding just what queue scans need. A cold message is decoded back
// into a normal *Message ("promoted") the first time anything needs to change
// it, and from then on behaves exactly as before.
//
// Invariants (all under MemoryStore.mu):
//   - an id is in at most one of s.messages (hot) and s.cold;
//   - cold messages are queue messages in status Queued or Scheduled that have
//     never been leased, so they are never terminal, leased, or dead-lettered;
//   - cold messages are never in topics or streams.
type coldSlot struct {
	raw         []byte // BWL1 message encoding without the id
	queue       string // canonical queue name (shared, not per-message)
	tenant      string // interned tenant id
	availableAt int64  // unix nanos; 0 means available immediately
	expiresAt   int64  // unix nanos; 0 means never
	priority    int32
}

// compactBacklogDefault is the initial setting for new stores. Production code
// leaves it false; tests flip it to run the whole suite against compact storage.
var compactBacklogDefault bool

// SetCompactBacklog switches compact backlog storage on or off. Turning it off
// promotes every cold message so the store returns to its normal form.
func (s *MemoryStore) SetCompactBacklog(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compact = on
	if !on {
		for id := range s.cold {
			s.promoteLocked(id)
		}
		s.cold = nil
	}
}

// CompactBacklogEnabled reports whether new queue messages are stored compactly.
func (s *MemoryStore) CompactBacklogEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compact
}

// ColdMessages returns how many messages are currently held in compact form.
func (s *MemoryStore) ColdMessages() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cold)
}

func nanosOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// internTenantLocked returns a shared copy of a tenant id so cold slots do not
// each retain their own string.
func (s *MemoryStore) internTenantLocked(tenant string) string {
	if tenant == "" {
		return ""
	}
	if v, ok := s.tenants[tenant]; ok {
		return v
	}
	if s.tenants == nil {
		s.tenants = make(map[string]string)
	}
	s.tenants[tenant] = tenant
	return tenant
}

// demoteLocked stores a freshly queued message in compact form. The caller has
// verified the invariants above and holds s.mu for writing.
func (s *MemoryStore) demoteLocked(m *Message, q *memoryQueue) {
	w := walBinaryWriter{buf: s.coldScratch[:0]}
	w.message(*m, msgFieldID)
	s.coldScratch = w.buf
	if s.cold == nil {
		s.cold = make(map[string]coldSlot)
	}
	s.cold[m.ID] = coldSlot{
		raw:         append([]byte(nil), w.buf...),
		queue:       q.cfg.Name,
		tenant:      s.internTenantLocked(m.TenantID),
		availableAt: nanosOrZero(m.AvailableAt),
		expiresAt:   nanosOrZero(m.ExpiresAt),
		priority:    int32(m.Priority),
	}
}

// compactableLocked reports whether m, just prepared for queue q, may be stored
// compactly.
func (s *MemoryStore) compactableLocked(m *Message) bool {
	return s.compact && m.Queue != "" && m.LeaseToken == "" &&
		(m.Status == MessageQueued || m.Status == MessageScheduled)
}

func decodeCold(id string, slot coldSlot) Message {
	r := walBinaryReader{buf: slot.raw, max: len(slot.raw) + 1}
	m := r.message(Message{ID: id})
	if r.err != nil {
		// The encoding is produced and consumed only by this process, so this is
		// unreachable; fail soft rather than take the broker down.
		return Message{ID: id, Queue: slot.queue, Status: MessageFailed, LastError: "compact backlog decode: " + r.err.Error()}
	}
	return m
}

// promoteLocked turns a cold message into a hot one and returns it. It returns
// nil if id is not cold. Callers hold s.mu for writing.
func (s *MemoryStore) promoteLocked(id string) *Message {
	slot, ok := s.cold[id]
	if !ok {
		return nil
	}
	m := decodeCold(id, slot)
	delete(s.cold, id)
	s.messages[id] = &m
	return &m
}

// msgLockedOK returns the live message for id, promoting it if it is cold. Use
// it wherever the result may be mutated. Callers hold s.mu for writing.
func (s *MemoryStore) msgLockedOK(id string) (*Message, bool) {
	if m, ok := s.messages[id]; ok {
		return m, true
	}
	if m := s.promoteLocked(id); m != nil {
		return m, true
	}
	return nil, false
}

func (s *MemoryStore) msgLocked(id string) *Message {
	m, _ := s.msgLockedOK(id)
	return m
}

// peekMsgOK returns a message for read-only use. A cold message is decoded into
// a temporary copy and is NOT promoted, so it is safe under a read lock; callers
// must not mutate the result.
func (s *MemoryStore) peekMsgOK(id string) (*Message, bool) {
	if m, ok := s.messages[id]; ok {
		return m, true
	}
	if slot, ok := s.cold[id]; ok {
		m := decodeCold(id, slot)
		return &m, true
	}
	return nil, false
}

func (s *MemoryStore) peekMsg(id string) *Message {
	m, _ := s.peekMsgOK(id)
	return m
}

func (s *MemoryStore) hasMsgLocked(id string) bool {
	if _, ok := s.messages[id]; ok {
		return true
	}
	_, ok := s.cold[id]
	return ok
}

// scanView is the subset of a message that queue scans filter on. For a cold
// message it comes from the slot summary without decoding anything.
type scanView struct {
	exists      bool
	cold        bool
	queue       string
	tenant      string
	priority    int
	availableAt time.Time
	expiresAt   time.Time
	status      MessageStatus
}

func (s *MemoryStore) scanViewLocked(id string) scanView {
	if m, ok := s.messages[id]; ok && m != nil {
		return scanView{exists: true, queue: m.Queue, tenant: m.TenantID, priority: m.Priority, availableAt: m.AvailableAt, expiresAt: m.ExpiresAt, status: m.Status}
	}
	if slot, ok := s.cold[id]; ok {
		v := scanView{exists: true, cold: true, queue: slot.queue, tenant: slot.tenant, priority: int(slot.priority), status: MessageQueued}
		if slot.availableAt != 0 {
			v.availableAt = time.Unix(0, slot.availableAt)
		}
		if slot.expiresAt != 0 {
			v.expiresAt = time.Unix(0, slot.expiresAt)
		}
		return v
	}
	return scanView{}
}

// demoteEligibleLocked compacts every hot message that qualifies, e.g. after a
// snapshot restore. Callers hold s.mu for writing.
func (s *MemoryStore) demoteEligibleLocked() {
	for id, m := range s.messages {
		if m == nil {
			continue
		}
		q := s.queues[m.Queue]
		if q == nil || !s.compactableLocked(m) {
			continue
		}
		s.demoteLocked(m, q)
		delete(s.messages, id)
	}
}
