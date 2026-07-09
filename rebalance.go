package broker

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"
)

type GroupAssignment struct {
	ConsumerID string `json:"consumer_id"`
	Group      string `json:"group"`
	Partitions []int  `json:"partitions,omitempty"`
	HashSlot   uint32 `json:"hash_slot,omitempty"`
	TotalSlots int    `json:"total_slots,omitempty"`
}

type GroupState struct {
	Name        string            `json:"name"`
	Members     []string          `json:"members"`
	Assignments []GroupAssignment `json:"assignments"`
	Version     int               `json:"version"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type GroupRebalancer struct {
	mu       sync.RWMutex
	groups   map[string]*GroupState
	capacity int
}

func NewGroupRebalancer() *GroupRebalancer {
	return &GroupRebalancer{groups: map[string]*GroupState{}}
}

func (r *GroupRebalancer) JoinGroup(consumerID, group string) *GroupState {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[group]
	if !ok {
		g = &GroupState{Name: group, UpdatedAt: time.Now()}
		r.groups[group] = g
	}
	for _, m := range g.Members {
		if m == consumerID {
			g.UpdatedAt = time.Now()
			return cloneGroupState(g)
		}
	}
	g.Members = append(g.Members, consumerID)
	g.Version++
	g.UpdatedAt = time.Now()
	r.rebalanceLocked(g)
	return cloneGroupState(g)
}

func (r *GroupRebalancer) LeaveGroup(consumerID, group string) *GroupState {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[group]
	if !ok {
		return nil
	}
	filtered := make([]string, 0, len(g.Members))
	for _, m := range g.Members {
		if m != consumerID {
			filtered = append(filtered, m)
		}
	}
	g.Members = filtered
	g.Version++
	g.UpdatedAt = time.Now()
	if len(g.Members) == 0 {
		delete(r.groups, group)
		return nil
	}
	r.rebalanceLocked(g)
	return cloneGroupState(g)
}

func (r *GroupRebalancer) GetGroup(group string) *GroupState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[group]
	if !ok {
		return nil
	}
	return cloneGroupState(g)
}

func (r *GroupRebalancer) ListGroups() []*GroupState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*GroupState, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, cloneGroupState(g))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *GroupRebalancer) ConsumerAssignment(consumerID, group string) *GroupAssignment {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[group]
	if !ok {
		return nil
	}
	for _, a := range g.Assignments {
		if a.ConsumerID == consumerID {
			ca := a
			return &ca
		}
	}
	return nil
}

func (r *GroupRebalancer) rebalanceLocked(g *GroupState) {
	if len(g.Members) == 0 {
		g.Assignments = nil
		return
	}
	sort.Strings(g.Members)
	totalSlots := 256
	slotsPerMember := totalSlots / len(g.Members)
	remainder := totalSlots % len(g.Members)
	g.Assignments = make([]GroupAssignment, 0, len(g.Members))
	start := 0
	for i, member := range g.Members {
		slots := slotsPerMember
		if i < remainder {
			slots++
		}
		assigned := GroupAssignment{
			ConsumerID: member,
			Group:      g.Name,
			HashSlot:   uint32(start),
			TotalSlots: slots,
		}
		start += slots
		g.Assignments = append(g.Assignments, assigned)
	}
}

func (r *GroupRebalancer) ShouldLeaseForConsumer(consumerID, group string, messageKey string) bool {
	if group == "" {
		return true
	}
	r.mu.RLock()
	g, ok := r.groups[group]
	r.mu.RUnlock()
	if !ok || len(g.Members) <= 1 {
		return true
	}
	slot := hashKey(messageKey) % 256
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, a := range g.Assignments {
		if a.ConsumerID == consumerID {
			return slot >= a.HashSlot && slot < a.HashSlot+uint32(a.TotalSlots)
		}
	}
	return false
}

func hashKey(key string) uint32 {
	if key == "" {
		return 0
	}
	var h uint32
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return h
}

func (b *Broker) GroupRebalancer() *GroupRebalancer {
	if b == nil {
		return nil
	}
	if b.groupRebalancer == nil {
		b.groupRebalancer = NewGroupRebalancer()
	}
	return b.groupRebalancer
}

func (b *Broker) ConsumerJoinGroup(ctx context.Context, consumerID, group string) *GroupState {
	st := b.GroupRebalancer().JoinGroup(consumerID, group)
	b.notify.Publish(Notification{
		Type: "consumer.group_joined", Source: b.cfg.Name, Severity: SeverityInfo,
		Consumer: consumerID, ConsumerGroup: group, Timestamp: time.Now(),
		Data: map[string]any{"members": st.Members, "version": st.Version},
	})
	return st
}

func (b *Broker) ConsumerLeaveGroup(ctx context.Context, consumerID, group string) {
	g := b.GroupRebalancer().LeaveGroup(consumerID, group)
	b.notify.Publish(Notification{
		Type: "consumer.group_left", Source: b.cfg.Name, Severity: SeverityInfo,
		Consumer: consumerID, ConsumerGroup: group, Timestamp: time.Now(),
		Data: map[string]any{"members": func() []string {
			if g == nil {
				return nil
			}
			return g.Members
		}(), "version": func() int {
			if g == nil {
				return 0
			}
			return g.Version
		}()},
	})
}

func (b *Broker) startGroupRebalancer() {
	b.wg.Add(1)
	go b.groupRebalanceLoop()
}

func (b *Broker) groupRebalanceLoop() {
	defer b.wg.Done()
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.reconcileGroups(time.Now())
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) reconcileGroups(now time.Time) {
	if b.consumers == nil {
		return
	}
	groups := map[string]map[string]bool{}
	for _, c := range b.Consumers().List() {
		if c.State == ConsumerStopped || c.State == ConsumerDisconnected || c.State == ConsumerBanned {
			continue
		}
		if c.Group != "" {
			if groups[c.Group] == nil {
				groups[c.Group] = map[string]bool{}
			}
			groups[c.Group][c.ConsumerID] = true
		}
	}
	for group, active := range groups {
		g := b.GroupRebalancer().GetGroup(group)
		if g == nil {
			for cid := range active {
				b.GroupRebalancer().JoinGroup(cid, group)
			}
			continue
		}
		for _, member := range g.Members {
			if !active[member] {
				b.GroupRebalancer().LeaveGroup(member, group)
			}
		}
		for cid := range active {
			found := false
			for _, m := range g.Members {
				if m == cid {
					found = true
					break
				}
			}
			if !found {
				b.GroupRebalancer().JoinGroup(cid, group)
			}
		}
	}
}

func cloneGroupState(g *GroupState) *GroupState {
	if g == nil {
		return nil
	}
	members := make([]string, len(g.Members))
	copy(members, g.Members)
	assignments := make([]GroupAssignment, len(g.Assignments))
	copy(assignments, g.Assignments)
	return &GroupState{
		Name: g.Name, Members: members, Assignments: assignments,
		Version: g.Version, UpdatedAt: g.UpdatedAt,
	}
}

func init() {
	// ensure math import is used (for uint32)
	_ = math.MaxUint32
}
