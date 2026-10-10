package broker

import (
	"context"
	"hash/fnv"
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
	Weight     int    `json:"weight,omitempty"`
}

type GroupState struct {
	Name        string            `json:"name"`
	Members     []string          `json:"members"`
	Weights     map[string]int    `json:"weights,omitempty"`
	Assignments []GroupAssignment `json:"assignments"`
	Version     int               `json:"version"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type GroupRebalancer struct {
	mu       sync.RWMutex
	groups   map[string]*GroupState
	capacity int
	hooks    *RebalanceHooks
}

type RebalanceHooks struct {
	OnBeforeRebalance func(group string, oldMembers, newMembers []string)
	OnAfterRebalance  func(group string, state *GroupState)
	OnDrainStarted    func(group, consumerID string)
	OnDrainCompleted  func(group, consumerID string)
}

func NewGroupRebalancer() *GroupRebalancer {
	return &GroupRebalancer{groups: map[string]*GroupState{}}
}

func NewGroupRebalancerWithHooks(hooks *RebalanceHooks) *GroupRebalancer {
	return &GroupRebalancer{groups: map[string]*GroupState{}, hooks: hooks}
}

func (r *GroupRebalancer) JoinGroup(consumerID, group string) *GroupState {
	return r.JoinGroupWithWeight(consumerID, group, 1)
}

func (r *GroupRebalancer) JoinGroupWithWeight(consumerID, group string, weight int) *GroupState {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[group]
	if !ok {
		g = &GroupState{Name: group, Weights: map[string]int{}, UpdatedAt: time.Now()}
		r.groups[group] = g
	}
	if g.Weights == nil {
		g.Weights = map[string]int{}
	}
	oldMembers := append([]string(nil), g.Members...)
	for _, m := range g.Members {
		if m == consumerID {
			g.Weights[consumerID] = weight
			g.UpdatedAt = time.Now()
			return cloneGroupState(g)
		}
	}
	g.Members = append(g.Members, consumerID)
	g.Weights[consumerID] = weight
	g.Version++
	g.UpdatedAt = time.Now()
	if r.hooks != nil && r.hooks.OnBeforeRebalance != nil {
		r.hooks.OnBeforeRebalance(group, oldMembers, g.Members)
	}
	r.rebalanceLocked(g)
	if r.hooks != nil && r.hooks.OnAfterRebalance != nil {
		r.hooks.OnAfterRebalance(group, cloneGroupState(g))
	}
	return cloneGroupState(g)
}

func (r *GroupRebalancer) LeaveGroup(consumerID, group string) *GroupState {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[group]
	if !ok {
		return nil
	}
	oldMembers := append([]string(nil), g.Members...)
	filtered := make([]string, 0, len(g.Members))
	for _, m := range g.Members {
		if m != consumerID {
			filtered = append(filtered, m)
		}
	}
	g.Members = filtered
	delete(g.Weights, consumerID)
	g.Version++
	g.UpdatedAt = time.Now()
	if len(g.Members) == 0 {
		delete(r.groups, group)
		return nil
	}
	if r.hooks != nil && r.hooks.OnBeforeRebalance != nil {
		r.hooks.OnBeforeRebalance(group, oldMembers, g.Members)
	}
	r.rebalanceLocked(g)
	if r.hooks != nil && r.hooks.OnAfterRebalance != nil {
		r.hooks.OnAfterRebalance(group, cloneGroupState(g))
	}
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
	totalSlots := 256
	if g.Weights == nil {
		g.Weights = map[string]int{}
	}
	totalWeight := 0
	for _, m := range g.Members {
		w := g.Weights[m]
		if w <= 0 {
			w = 1
		}
		totalWeight += w
	}
	g.Assignments = make([]GroupAssignment, 0, len(g.Members))
	start := 0
	for _, member := range g.Members {
		w := g.Weights[member]
		if w <= 0 {
			w = 1
		}
		slots := (totalSlots * w) / totalWeight
		if slots <= 0 {
			slots = 1
		}
		assigned := GroupAssignment{
			ConsumerID: member,
			Group:      g.Name,
			Weight:     w,
			HashSlot:   uint32(start),
			TotalSlots: slots,
		}
		start += slots
		g.Assignments = append(g.Assignments, assigned)
	}
	// Fill remaining slots to reach totalSlots.
	remaining := totalSlots - start
	if remaining > 0 && len(g.Assignments) > 0 {
		last := &g.Assignments[len(g.Assignments)-1]
		last.TotalSlots += remaining
	}
}

func (r *GroupRebalancer) ShouldLeaseForConsumer(consumerID, group string, messageKey string) bool {
	if group == "" {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[group]
	if !ok || len(g.Members) <= 1 {
		return true
	}
	slot := hashKey(messageKey) % 256
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
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (b *Broker) GroupRebalancer() *GroupRebalancer {
	if b == nil {
		return nil
	}
	b.groupRebalancerOnce.Do(func() {
		if b.groupRebalancer == nil {
			b.groupRebalancer = NewGroupRebalancer()
		}
	})
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
		// Graceful drain: remove inactive members.
		for _, member := range g.Members {
			if !active[member] {
				if b.GroupRebalancer().hooks != nil && b.GroupRebalancer().hooks.OnDrainStarted != nil {
					b.GroupRebalancer().hooks.OnDrainStarted(group, member)
				}
				b.GroupRebalancer().LeaveGroup(member, group)
				if b.GroupRebalancer().hooks != nil && b.GroupRebalancer().hooks.OnDrainCompleted != nil {
					b.GroupRebalancer().hooks.OnDrainCompleted(group, member)
				}
			}
		}
		// Join new members.
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
