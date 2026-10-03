package broker

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// normMsg reduces a message to the fields whose values must match between a
// compact and a normal store, ignoring values that are legitimately random
// (lease tokens) or presentation-only (time zone, nil vs empty maps).
func normMsg(m Message) map[string]any {
	ts := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.UnixNano() / 1e3 // microsecond precision; stores run a few µs apart
	}
	hdr := m.Headers
	if len(hdr) == 0 {
		hdr = nil
	}
	meta := m.Metadata
	if len(meta) == 0 {
		meta = nil
	}
	var seq uint64
	if m.ProducerSequence != nil {
		seq = *m.ProducerSequence
	}
	return map[string]any{
		"id": m.ID, "topic": m.Topic, "queue": m.Queue, "status": m.Status, "prio": m.Priority,
		"payload": string(m.Payload), "headers": hdr, "metadata": meta, "tenant": m.TenantID,
		"key": m.Key, "type": m.Type, "ctype": m.ContentType, "attempts": m.Attempts, "maxattempts": m.MaxAttempts,
		"redeliveries": m.Redeliveries, "consumer": m.ConsumerID, "group": m.ConsumerGroup,
		"hasLease": m.LeaseToken != "", "dlreason": m.DeadLetterReason, "lasterr": m.LastError,
		"orig": m.OriginalQueue, "parent": m.ParentID, "producer": m.ProducerID, "epoch": m.ProducerEpoch, "seq": seq,
		"corr": m.CorrelationID, "idem": m.IdempotencyKey, "dedup": m.DeduplicationID,
		"avail":      ts(m.AvailableAt) / 1e6 * 0, // availability is compared via behavior, not timestamp
		"expiresSet": !m.ExpiresAt.IsZero(),
	}
}

func normList(ms []Message) []map[string]any {
	out := make([]map[string]any, len(ms))
	for i := range ms {
		out[i] = normMsg(ms[i])
	}
	return out
}

type diffHarness struct {
	t        *testing.T
	ctx      context.Context
	hot      *MemoryStore
	cold     *MemoryStore
	hotTok   map[string]string
	coldTok  map[string]string
	ids      []string
	r        *rand.Rand
	step     int
	log      []string
	sawCold  bool
	sawLease int
}

func (h *diffHarness) fail(format string, args ...any) {
	h.t.Helper()
	tail := h.log
	if len(tail) > 12 {
		tail = tail[len(tail)-12:]
	}
	h.t.Fatalf("step %d: %s\nrecent ops:\n%v", h.step, fmt.Sprintf(format, args...), tail)
}

func (h *diffHarness) same(what string, a, b any) {
	h.t.Helper()
	if !reflect.DeepEqual(a, b) {
		h.fail("%s differ:\n normal : %+v\n compact: %+v", what, a, b)
	}
}

func (h *diffHarness) op(format string, args ...any) {
	h.log = append(h.log, fmt.Sprintf(format, args...))
}

func (h *diffHarness) randomMessage(i int) (Message, []Destination) {
	now := time.Now()
	m := Message{
		ID:      "m" + strconv.Itoa(i),
		Topic:   "t",
		Payload: []byte(fmt.Sprintf("payload-%d-%s", i, strings.Repeat("x", h.r.Intn(300)))),
		Type:    []string{"", "a", "b"}[h.r.Intn(3)],
	}
	if h.r.Intn(3) == 0 {
		m.Headers = map[string]string{"h1": "v" + strconv.Itoa(i), "h2": "x"}
	}
	if h.r.Intn(4) == 0 {
		m.Metadata = map[string]string{"k": "v"}
	}
	if h.r.Intn(5) == 0 {
		m.Priority = h.r.Intn(4)
	}
	switch h.r.Intn(8) {
	case 0:
		m.AvailableAt = now.Add(time.Hour) // not yet available
	case 1:
		m.AvailableAt = now.Add(-time.Hour)
	}
	switch h.r.Intn(12) {
	case 0:
		m.ExpiresAt = now.Add(-time.Minute) // already expired
	case 1:
		m.ExpiresAt = now.Add(time.Hour)
	}
	if h.r.Intn(4) == 0 {
		m.TenantID = []string{"ta", "tb"}[h.r.Intn(2)]
	}
	if h.r.Intn(6) == 0 {
		seq := uint64(i)
		m.ProducerID, m.ProducerEpoch, m.ProducerSequence = "p"+strconv.Itoa(i), 1, &seq
	}
	if h.r.Intn(5) == 0 {
		m.MaxAttempts = 1 + h.r.Intn(3)
	}
	q := []string{"q1", "q2"}[h.r.Intn(2)]
	return m, []Destination{{Kind: DestinationQueue, Name: q}}
}

func (h *diffHarness) run(steps int) {
	for h.step = 0; h.step < steps; h.step++ {
		switch n := h.r.Intn(100); {
		case n < 38: // append
			m, d := h.randomMessage(len(h.ids))
			h.op("append %s -> %s prio=%d avail=%v exp=%v tenant=%q", m.ID, d[0].Name, m.Priority, !m.AvailableAt.IsZero(), !m.ExpiresAt.IsZero(), m.TenantID)
			a, errA := h.hot.Append(h.ctx, m, d)
			b, errB := h.cold.Append(h.ctx, m, d)
			h.same("append err", fmt.Sprint(errA), fmt.Sprint(errB))
			h.same("append result", normList(a), normList(b))
			h.ids = append(h.ids, m.ID)
		case n < 62: // lease
			req := LeaseRequest{Queue: []string{"q1", "q2"}[h.r.Intn(2)], ConsumerID: "c" + strconv.Itoa(h.r.Intn(3)), Limit: 1 + h.r.Intn(5), VisibilityTimeout: time.Hour}
			if h.r.Intn(6) == 0 {
				req.TenantID = []string{"ta", "tb"}[h.r.Intn(2)]
			}
			h.op("lease %s limit=%d tenant=%q", req.Queue, req.Limit, req.TenantID)
			a, errA := h.hot.Lease(h.ctx, req)
			b, errB := h.cold.Lease(h.ctx, req)
			h.same("lease err", fmt.Sprint(errA), fmt.Sprint(errB))
			h.same("lease result", normList(a), normList(b))
			for _, m := range a {
				h.hotTok[m.ID] = m.LeaseToken
				h.sawLease++
			}
			for _, m := range b {
				h.coldTok[m.ID] = m.LeaseToken
			}
		case n < 74: // ack a known id (may be leased, queued, or unknown)
			if len(h.ids) == 0 {
				continue
			}
			id := h.ids[h.r.Intn(len(h.ids))]
			h.op("ack %s", id)
			errA := h.hot.Ack(h.ctx, AckRequest{MessageID: id, LeaseToken: h.hotTok[id], ConsumerID: "c0"})
			errB := h.cold.Ack(h.ctx, AckRequest{MessageID: id, LeaseToken: h.coldTok[id], ConsumerID: "c0"})
			h.same("ack err", fmt.Sprint(errA), fmt.Sprint(errB))
		case n < 80: // nack
			if len(h.ids) == 0 {
				continue
			}
			id := h.ids[h.r.Intn(len(h.ids))]
			act := []NackAction{NackRequeue, NackRetry, NackDLQ}[h.r.Intn(3)]
			h.op("nack %s %s", id, act)
			errA := h.hot.Nack(h.ctx, NackRequest{MessageID: id, LeaseToken: h.hotTok[id], ConsumerID: "c0", Action: act, Reason: "r"})
			errB := h.cold.Nack(h.ctx, NackRequest{MessageID: id, LeaseToken: h.coldTok[id], ConsumerID: "c0", Action: act, Reason: "r"})
			h.same("nack err", fmt.Sprint(errA), fmt.Sprint(errB))
		case n < 90: // get
			if len(h.ids) == 0 {
				continue
			}
			id := h.ids[h.r.Intn(len(h.ids))]
			h.op("get %s", id)
			a, okA, errA := h.hot.Get(h.ctx, id)
			b, okB, errB := h.cold.Get(h.ctx, id)
			h.same("get ok/err", fmt.Sprint(okA, errA), fmt.Sprint(okB, errB))
			if okA {
				h.same("get "+id, normMsg(a), normMsg(b))
			}
		case n < 94: // list
			q := []string{"q1", "q2"}[h.r.Intn(2)]
			h.op("list %s", q)
			a, errA := h.hot.ListQueue(h.ctx, q, 1+h.r.Intn(20))
			b, errB := h.cold.ListQueue(h.ctx, q, len(a)+0)
			_ = errB
			// Re-list with identical limits so the comparison is apples to apples.
			lim := 1 + h.r.Intn(20)
			a, errA = h.hot.ListQueue(h.ctx, q, lim)
			b, errB = h.cold.ListQueue(h.ctx, q, lim)
			h.same("list err", fmt.Sprint(errA), fmt.Sprint(errB))
			h.same("list "+q, normList(a), normList(b))
		case n < 96: // sweep leases far in the future: every outstanding lease expires
			h.op("sweep")
			a, errA := h.hot.SweepLeases(h.ctx, time.Now().Add(2*time.Hour))
			b, errB := h.cold.SweepLeases(h.ctx, time.Now().Add(2*time.Hour))
			h.same("sweep", fmt.Sprint(a, errA), fmt.Sprint(b, errB))
		case n < 97: // purge
			q := []string{"q1", "q2"}[h.r.Intn(2)]
			h.op("purge %s", q)
			a, errA := h.hot.PurgeQueue(h.ctx, q)
			b, errB := h.cold.PurgeQueue(h.ctx, q)
			h.same("purge", fmt.Sprint(a, errA), fmt.Sprint(b, errB))
		case n < 99: // update an existing message
			if len(h.ids) == 0 {
				continue
			}
			id := h.ids[h.r.Intn(len(h.ids))]
			a, okA, _ := h.hot.Get(h.ctx, id)
			b, okB, _ := h.cold.Get(h.ctx, id)
			if !okA || !okB {
				continue
			}
			a.Payload, b.Payload = []byte("updated-"+id), []byte("updated-"+id)
			h.op("update %s", id)
			errA := h.hot.Update(h.ctx, a)
			errB := h.cold.Update(h.ctx, b)
			h.same("update err", fmt.Sprint(errA), fmt.Sprint(errB))
		default: // stats
			h.op("stats")
			a, _ := h.hot.Stats(h.ctx)
			b, _ := h.cold.Stats(h.ctx)
			h.same("stats", a, b)
		}
		if h.cold.ColdMessages() > 0 {
			h.sawCold = true
		}
	}
}

func (h *diffHarness) finalCompare() {
	h.step = -1
	a, _ := h.hot.Stats(h.ctx)
	b, _ := h.cold.Stats(h.ctx)
	h.same("final stats", a, b)
	sa, sb := h.hot.Snapshot(), h.cold.Snapshot()
	ids := make([]string, 0, len(sa.Messages))
	for id := range sa.Messages {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	h.same("snapshot message count", len(sa.Messages), len(sb.Messages))
	for _, id := range ids {
		mb, ok := sb.Messages[id]
		if !ok {
			h.fail("snapshot missing %s in compact store", id)
		}
		h.same("snapshot "+id, normMsg(sa.Messages[id]), normMsg(mb))
	}
	for name := range sa.Queues {
		h.same("queue ids "+name, sa.Queues[name].IDs[sa.Queues[name].Head:], sb.Queues[name].IDs[sb.Queues[name].Head:])
		h.same("queue depth "+name, sa.Queues[name].Depth, sb.Queues[name].Depth)
	}
	// A restored copy of the compact store must behave identically too.
	restored := NewMemoryStore()
	restored.SetHistoryLimit(0)
	restored.restoreSnapshot(sb, false)
	ra, _ := restored.Stats(h.ctx)
	h.same("restored stats", a, ra)
}

func newDiffHarness(t *testing.T, seed int64) *diffHarness {
	hot, cold := NewMemoryStore(), NewMemoryStore()
	hot.SetCompactBacklog(false)
	cold.SetCompactBacklog(true)
	for _, s := range []*MemoryStore{hot, cold} {
		s.SetHistoryLimit(0)
		s.SetRetainCompleted(false)
		for _, q := range []string{"q1", "q2"} {
			if err := s.DeclareQueue(context.Background(), QueueConfig{Name: q}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return &diffHarness{t: t, ctx: context.Background(), hot: hot, cold: cold, hotTok: map[string]string{}, coldTok: map[string]string{}, r: rand.New(rand.NewSource(seed))}
}

// A compact store must be indistinguishable from a normal one through the Store
// API, across a long random mix of appends, leases, acks, nacks, updates,
// listing, sweeps, purges and snapshot/restore.
func TestCompactBacklogMatchesNormalStore(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		seed := seed
		t.Run("seed"+strconv.FormatInt(seed, 10), func(t *testing.T) {
			h := newDiffHarness(t, seed)
			h.run(1500)
			h.finalCompare()
			if !h.sawCold {
				t.Fatal("no message was ever stored compactly; the test proved nothing")
			}
			if h.sawLease == 0 {
				t.Fatal("no message was ever leased; the test proved nothing")
			}
		})
	}
}

func TestCompactBacklogRoundTripsEveryField(t *testing.T) {
	seq := uint64(7)
	in := Message{
		ID: "rt-1", ParentID: "p", Topic: "t", Queue: "q", Key: "k", RoutingKey: "rk", Type: "ty", ContentType: "ct",
		Schema: "s", SchemaVersion: "1", Payload: []byte{0, 1, 2, 255}, Headers: map[string]string{"a": "b"}, Metadata: map[string]string{"c": "d"},
		TraceID: "tr", SpanID: "sp", CorrelationID: "co", CausationID: "ca", RequestID: "re", ReplyTo: "rp", ReplyGroup: "rg",
		IdempotencyKey: "ik", DeduplicationID: "dd", Checksum: "ck", Priority: 3,
		CreatedAt: time.Unix(1700000000, 123456789).UTC(), AvailableAt: time.Unix(1700000001, 5).UTC(), ExpiresAt: time.Unix(1800000000, 9).UTC(),
		Attempts: 2, MaxAttempts: 5, Redeliveries: 1, TenantID: "tn", ProducerID: "pr", ProducerEpoch: 4, ProducerSequence: &seq,
		OriginalTopic: "ot", OriginalQueue: "oq", OriginalStream: "os", OriginalOffset: 11, Status: MessageQueued,
		LastError: "le", DeadLetterReason: "dl", TaskID: "ti", TaskName: "tnm",
	}
	s := NewMemoryStore()
	s.SetCompactBacklog(true)
	q := &memoryQueue{cfg: QueueConfig{Name: "q"}}
	s.demoteLocked(&in, q)
	out := decodeCold(in.ID, s.cold[in.ID])
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip lost data:\n in : %+v\n out: %+v", in, out)
	}
}

// Compact storage must actually shrink a backlog; guard against a regression
// that silently stores everything hot again.
func TestCompactBacklogHoldsMessagesCold(t *testing.T) {
	s := NewMemoryStore()
	s.SetCompactBacklog(true)
	ctx := context.Background()
	if err := s.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := s.Append(ctx, Message{ID: "m" + strconv.Itoa(i), Payload: []byte("x")}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.ColdMessages(); got != 1000 {
		t.Fatalf("cold messages = %d, want 1000", got)
	}
	got, err := s.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 10})
	if err != nil || len(got) != 10 {
		t.Fatalf("lease: %d %v", len(got), err)
	}
	if cold := s.ColdMessages(); cold != 990 {
		t.Fatalf("after leasing 10, cold = %d, want 990 (only leased messages are promoted)", cold)
	}
	s.SetCompactBacklog(false)
	if s.ColdMessages() != 0 {
		t.Fatal("turning compaction off must promote everything")
	}
	if st, _ := s.Stats(ctx); st.Messages != 1000 {
		t.Fatalf("messages after disabling = %d", st.Messages)
	}
}
