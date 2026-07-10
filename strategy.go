package broker

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StrategyType enumerates all built-in consumer routing strategies.
type StrategyType string

const (
	StrategyRoundRobin          StrategyType = "round_robin"
	StrategyWeightedRoundRobin  StrategyType = "weighted_round_robin"
	StrategyLeastLoaded         StrategyType = "least_loaded"
	StrategyRandom              StrategyType = "random"
	StrategyConsistentHash      StrategyType = "consistent_hash"
	StrategyCapabilityBased     StrategyType = "capability_based"
	StrategyPriorityAware       StrategyType = "priority_aware"
	StrategyAdaptiveLoadBalance StrategyType = "adaptive_load_balance"
	StrategyStickySession       StrategyType = "sticky_session"
	StrategyFanOut              StrategyType = "fan_out"
	StrategyRuleBased           StrategyType = "rule_based"
	StrategyPowerOfTwoChoices   StrategyType = "power_of_two_choices"
	StrategySharded             StrategyType = "sharded"
)

// ConsumerCandidate represents a consumer that can receive a routed message.
type ConsumerCandidate struct {
	ConsumerID   string            `json:"consumer_id"`
	Group        string            `json:"group"`
	Weight       int               `json:"weight"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	InFlight     int               `json:"in_flight"`
	MaxInFlight  int               `json:"max_in_flight"`
	Acked        int64             `json:"acked"`
	Failed       int64             `json:"failed"`
	Leased       int64             `json:"leased"`
	Healthy      bool              `json:"healthy"`
	LastLatency  time.Duration     `json:"last_latency"`
	AvgLatency   time.Duration     `json:"avg_latency"`
}

// LoadScore returns a 0.0 (fully loaded) to 1.0 (idle) score for a consumer.
func (c *ConsumerCandidate) LoadScore() float64 {
	if c.MaxInFlight <= 0 {
		return 1.0
	}
	available := c.MaxInFlight - c.InFlight
	if available <= 0 {
		return 0.0
	}
	return float64(available) / float64(c.MaxInFlight)
}

// SuccessRate returns 0.0 to 1.0 success ratio.
func (c *ConsumerCandidate) SuccessRate() float64 {
	total := c.Acked + c.Failed
	if total == 0 {
		return 1.0
	}
	return float64(c.Acked) / float64(total)
}

// ConsumerStrategy defines how messages are routed to consumers.
type ConsumerStrategy interface {
	// Select returns the best consumer for the given message from the candidates.
	Select(ctx context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error)
	// Name returns the strategy type identifier.
	Name() StrategyType
}

// StrategyConfig holds configuration for a consumer strategy.
type StrategyConfig struct {
	Type              StrategyType    `json:"type"`
	Queue             string          `json:"queue,omitempty"`
	Topic             string          `json:"topic,omitempty"`
	Weight            int             `json:"weight,omitempty"`
	HashKey           string          `json:"hash_key,omitempty"`
	StickyKey         string          `json:"sticky_key,omitempty"`
	PriorityThreshold int             `json:"priority_threshold,omitempty"`
	Capabilities      []string        `json:"capabilities,omitempty"`
	FallbackStrategy  StrategyType    `json:"fallback_strategy,omitempty"`
	Rules             []RoutingRule   `json:"rules,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

// NewStrategy creates a ConsumerStrategy from a StrategyConfig.
func NewStrategy(cfg StrategyConfig) ConsumerStrategy {
	switch cfg.Type {
	case StrategyRoundRobin:
		return NewRoundRobinStrategy()
	case StrategyWeightedRoundRobin:
		return NewWeightedRoundRobinStrategy()
	case StrategyLeastLoaded:
		return NewLeastLoadedStrategy()
	case StrategyRandom:
		return NewRandomStrategy()
	case StrategyConsistentHash:
		return NewConsistentHashStrategy(cfg.HashKey)
	case StrategyCapabilityBased:
		return NewCapabilityBasedStrategy(cfg.Capabilities)
	case StrategyPriorityAware:
		return NewPriorityAwareStrategy(cfg.PriorityThreshold)
	case StrategyAdaptiveLoadBalance:
		return NewAdaptiveLoadBalanceStrategy()
	case StrategyStickySession:
		return NewStickySessionStrategy(cfg.StickyKey)
	case StrategyFanOut:
		return NewFanOutStrategy()
	case StrategyRuleBased:
		return NewRuleBasedStrategy(cfg.Rules)
	case StrategyPowerOfTwoChoices:
		return NewPowerOfTwoChoicesStrategy()
	case StrategySharded:
		return NewShardedStrategy(cfg.HashKey)
	default:
		return NewRoundRobinStrategy()
	}
}

// ---------------------------------------------------------------------------
// Round Robin
// ---------------------------------------------------------------------------

type roundRobinStrategy struct {
	counter atomic.Uint64
}

func NewRoundRobinStrategy() ConsumerStrategy {
	return &roundRobinStrategy{}
}

func (s *roundRobinStrategy) Name() StrategyType { return StrategyRoundRobin }

func (s *roundRobinStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	idx := s.counter.Add(1) - 1
	return []ConsumerCandidate{candidates[idx%uint64(len(candidates))]}, nil
}

// ---------------------------------------------------------------------------
// Weighted Round Robin
// ---------------------------------------------------------------------------

type weightedRoundRobinStrategy struct {
	mu           sync.Mutex
	current      int
	currentWeight int
	candidates   []ConsumerCandidate
}

func NewWeightedRoundRobinStrategy() ConsumerStrategy {
	return &weightedRoundRobinStrategy{}
}

func (s *weightedRoundRobinStrategy) Name() StrategyType { return StrategyWeightedRoundRobin }

func (s *weightedRoundRobinStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.candidates) != len(candidates) {
		s.current = 0
		s.currentWeight = 0
		s.candidates = append([]ConsumerCandidate(nil), candidates...)
	}

	totalWeight := 0
	for _, c := range candidates {
		w := c.Weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w
	}

	for {
		if s.current >= len(candidates) {
			s.current = 0
			s.currentWeight = 0
		}
		w := candidates[s.current].Weight
		if w <= 0 {
			w = 1
		}
		if s.currentWeight < w {
			s.currentWeight++
			idx := s.current
			s.current++
			return []ConsumerCandidate{candidates[idx]}, nil
		}
		s.currentWeight = 0
		s.current++
		if s.current >= len(candidates) {
			s.current = 0
			return []ConsumerCandidate{candidates[0]}, nil
		}
	}
}

// ---------------------------------------------------------------------------
// Least Loaded
// ---------------------------------------------------------------------------

type leastLoadedStrategy struct{}

func NewLeastLoadedStrategy() ConsumerStrategy {
	return &leastLoadedStrategy{}
}

func (s *leastLoadedStrategy) Name() StrategyType { return StrategyLeastLoaded }

func (s *leastLoadedStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	best := candidates[0]
	bestScore := best.LoadScore()
	for i := 1; i < len(candidates); i++ {
		score := candidates[i].LoadScore()
		if score > bestScore {
			best = candidates[i]
			bestScore = score
		}
	}
	return []ConsumerCandidate{best}, nil
}

// ---------------------------------------------------------------------------
// Random
// ---------------------------------------------------------------------------

type randomStrategy struct{}

func NewRandomStrategy() ConsumerStrategy {
	return &randomStrategy{}
}

func (s *randomStrategy) Name() StrategyType { return StrategyRandom }

func (s *randomStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	idx := rand.Intn(len(candidates))
	return []ConsumerCandidate{candidates[idx]}, nil
}

// ---------------------------------------------------------------------------
// Consistent Hash
// ---------------------------------------------------------------------------

type consistentHashStrategy struct {
	hashKey string
}

func NewConsistentHashStrategy(hashKey string) ConsumerStrategy {
	if hashKey == "" {
		hashKey = "key"
	}
	return &consistentHashStrategy{hashKey: hashKey}
}

func (s *consistentHashStrategy) Name() StrategyType { return StrategyConsistentHash }

func (s *consistentHashStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	subject := s.extractHashKey(msg)
	h := uint64(hashKey(subject)) % uint64(len(candidates))
	return []ConsumerCandidate{candidates[h]}, nil
}

func (s *consistentHashStrategy) extractHashKey(msg Message) string {
	if s.hashKey == "key" {
		if msg.Key != "" {
			return msg.Key
		}
		return msg.RoutingKey
	}
	if val, ok := msg.Headers[s.hashKey]; ok {
		return val
	}
	if val, ok := msg.Metadata[s.hashKey]; ok {
		return val
	}
	return msg.Key
}

// ---------------------------------------------------------------------------
// Capability Based
// ---------------------------------------------------------------------------

type capabilityBasedStrategy struct {
	requiredCaps []string
}

func NewCapabilityBasedStrategy(requiredCaps []string) ConsumerStrategy {
	return &capabilityBasedStrategy{requiredCaps: requiredCaps}
}

func (s *capabilityBasedStrategy) Name() StrategyType { return StrategyCapabilityBased }

func (s *capabilityBasedStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	taskType := msg.TaskName
	if taskType == "" {
		taskType = msg.Type
	}
	caps := append(s.requiredCaps, taskType)

	filtered := make([]ConsumerCandidate, 0, len(candidates))
	for _, c := range candidates {
		if s.hasCapabilities(c, caps) {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return candidates, nil
	}
	best := filtered[0]
	bestScore := best.LoadScore()
	for i := 1; i < len(filtered); i++ {
		score := filtered[i].LoadScore()
		if score > bestScore {
			best = filtered[i]
			bestScore = score
		}
	}
	return []ConsumerCandidate{best}, nil
}

func (s *capabilityBasedStrategy) hasCapabilities(c ConsumerCandidate, required []string) bool {
	if len(required) == 0 {
		return true
	}
	if len(c.Capabilities) == 0 {
		return len(required) == 0
	}
	capSet := make(map[string]struct{}, len(c.Capabilities))
	for _, cap := range c.Capabilities {
		capSet[cap] = struct{}{}
	}
	for _, req := range required {
		if req == "" {
			continue
		}
		if _, ok := capSet[req]; !ok {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Priority Aware
// ---------------------------------------------------------------------------

type priorityAwareStrategy struct {
	threshold int
}

func NewPriorityAwareStrategy(threshold int) ConsumerStrategy {
	if threshold <= 0 {
		threshold = 5
	}
	return &priorityAwareStrategy{threshold: threshold}
}

func (s *priorityAwareStrategy) Name() StrategyType { return StrategyPriorityAware }

func (s *priorityAwareStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if msg.Priority >= s.threshold {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].LoadScore() > candidates[j].LoadScore()
		})
		return []ConsumerCandidate{candidates[0]}, nil
	}
	best := candidates[0]
	bestSuccess := best.SuccessRate()
	for i := 1; i < len(candidates); i++ {
		sr := candidates[i].SuccessRate()
		if sr > bestSuccess {
			best = candidates[i]
			bestSuccess = sr
		}
	}
	return []ConsumerCandidate{best}, nil
}

// ---------------------------------------------------------------------------
// Adaptive Load Balance
// ---------------------------------------------------------------------------

type adaptiveLoadBalanceStrategy struct {
	mu      sync.Mutex
	weights map[string]float64
	alpha   float64
}

func NewAdaptiveLoadBalanceStrategy() ConsumerStrategy {
	return &adaptiveLoadBalanceStrategy{
		weights: make(map[string]float64),
		alpha:   0.3,
	}
}

func (s *adaptiveLoadBalanceStrategy) Name() StrategyType { return StrategyAdaptiveLoadBalance }

func (s *adaptiveLoadBalanceStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	best := candidates[0]
	bestScore := s.computeScore(candidates[0])
	for i := 1; i < len(candidates); i++ {
		score := s.computeScore(candidates[i])
		if score > bestScore {
			best = candidates[i]
			bestScore = score
		}
	}
	if best.Acked > 0 || best.Failed > 0 {
		current := s.weights[best.ConsumerID]
		latencyScore := 1.0
		if best.AvgLatency > 0 {
			latencyScore = 1.0 / (1.0 + best.AvgLatency.Seconds())
		}
		s.weights[best.ConsumerID] = s.alpha*latencyScore + (1-s.alpha)*current
	}
	return []ConsumerCandidate{best}, nil
}

func (s *adaptiveLoadBalanceStrategy) computeScore(c ConsumerCandidate) float64 {
	loadScore := c.LoadScore()
	successRate := c.SuccessRate()
	adaptiveWeight := s.weights[c.ConsumerID]
	if adaptiveWeight == 0 {
		adaptiveWeight = 1.0
	}
	return 0.4*loadScore + 0.3*successRate + 0.3*adaptiveWeight
}

// ---------------------------------------------------------------------------
// Sticky Session
// ---------------------------------------------------------------------------

type stickySessionStrategy struct {
	stickyKey string
	mu        sync.RWMutex
	binding   map[string]string
}

func NewStickySessionStrategy(stickyKey string) ConsumerStrategy {
	if stickyKey == "" {
		stickyKey = "key"
	}
	return &stickySessionStrategy{
		stickyKey: stickyKey,
		binding:  make(map[string]string),
	}
}

func (s *stickySessionStrategy) Name() StrategyType { return StrategyStickySession }

func (s *stickySessionStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	sessionKey := s.extractSessionKey(msg)
	if sessionKey != "" {
		s.mu.RLock()
		bound := s.binding[sessionKey]
		s.mu.RUnlock()
		if bound != "" {
			for _, c := range candidates {
				if c.ConsumerID == bound && c.LoadScore() > 0 {
					return []ConsumerCandidate{c}, nil
				}
			}
		}
	}
	best := candidates[0]
	bestScore := best.LoadScore()
	for i := 1; i < len(candidates); i++ {
		score := candidates[i].LoadScore()
		if score > bestScore {
			best = candidates[i]
			bestScore = score
		}
	}
	if sessionKey != "" {
		s.mu.Lock()
		s.binding[sessionKey] = best.ConsumerID
		s.mu.Unlock()
	}
	return []ConsumerCandidate{best}, nil
}

func (s *stickySessionStrategy) extractSessionKey(msg Message) string {
	if val, ok := msg.Headers[s.stickyKey]; ok {
		return val
	}
	if val, ok := msg.Metadata[s.stickyKey]; ok {
		return val
	}
	return msg.Key
}

// ---------------------------------------------------------------------------
// Fan Out
// ---------------------------------------------------------------------------

type fanOutStrategy struct{}

func NewFanOutStrategy() ConsumerStrategy {
	return &fanOutStrategy{}
}

func (s *fanOutStrategy) Name() StrategyType { return StrategyFanOut }

func (s *fanOutStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	healthy := make([]ConsumerCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.Healthy || c.LoadScore() > 0 {
			healthy = append(healthy, c)
		}
	}
	if len(healthy) == 0 && len(candidates) > 0 {
		return candidates, nil
	}
	return healthy, nil
}

// ---------------------------------------------------------------------------
// Rule Based
// ---------------------------------------------------------------------------

type ruleBasedStrategy struct {
	rules []RoutingRule
}

func NewRuleBasedStrategy(rules []RoutingRule) ConsumerStrategy {
	return &ruleBasedStrategy{rules: rules}
}

func (s *ruleBasedStrategy) Name() StrategyType { return StrategyRuleBased }

func (s *ruleBasedStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	for _, rule := range s.rules {
		if !rule.Enabled {
			continue
		}
		if rule.Matches(msg) {
			if len(rule.TargetConsumers) > 0 {
				filtered := make([]ConsumerCandidate, 0)
				targetSet := make(map[string]struct{}, len(rule.TargetConsumers))
				for _, t := range rule.TargetConsumers {
					targetSet[t] = struct{}{}
				}
				for _, c := range candidates {
					if _, ok := targetSet[c.ConsumerID]; ok {
						filtered = append(filtered, c)
					}
				}
				if len(filtered) > 0 {
					return filtered, nil
				}
			}
			if rule.Strategy != nil {
				return rule.Strategy.Select(context.Background(), msg, candidates)
			}
			return []ConsumerCandidate{candidates[0]}, nil
		}
	}
	return candidates, nil
}

// ---------------------------------------------------------------------------
// Power of Two Choices
// ---------------------------------------------------------------------------

type powerOfTwoChoicesStrategy struct{}

func NewPowerOfTwoChoicesStrategy() ConsumerStrategy {
	return &powerOfTwoChoicesStrategy{}
}

func (s *powerOfTwoChoicesStrategy) Name() StrategyType { return StrategyPowerOfTwoChoices }

func (s *powerOfTwoChoicesStrategy) Select(_ context.Context, _ Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) == 1 {
		return []ConsumerCandidate{candidates[0]}, nil
	}
	i := rand.Intn(len(candidates))
	j := rand.Intn(len(candidates))
	for j == i {
		j = rand.Intn(len(candidates))
	}
	if candidates[i].LoadScore() >= candidates[j].LoadScore() {
		return []ConsumerCandidate{candidates[i]}, nil
	}
	return []ConsumerCandidate{candidates[j]}, nil
}

// ---------------------------------------------------------------------------
// Sharded
// ---------------------------------------------------------------------------

type shardedStrategy struct {
	hashKey string
}

func NewShardedStrategy(hashKey string) ConsumerStrategy {
	if hashKey == "" {
		hashKey = "key"
	}
	return &shardedStrategy{hashKey: hashKey}
}

func (s *shardedStrategy) Name() StrategyType { return StrategySharded }

func (s *shardedStrategy) Select(_ context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	subject := s.extractShardKey(msg)
	h := uint64(hashKey(subject))
	shard := h % uint64(len(candidates))
	return []ConsumerCandidate{candidates[shard]}, nil
}

func (s *shardedStrategy) extractShardKey(msg Message) string {
	if val, ok := msg.Headers[s.hashKey]; ok {
		return val
	}
	if val, ok := msg.Metadata[s.hashKey]; ok {
		return val
	}
	if msg.Key != "" {
		return msg.Key
	}
	return msg.RoutingKey
}

// ---------------------------------------------------------------------------
// StrategyRegistry provides named strategy lookup
// ---------------------------------------------------------------------------

type StrategyRegistry struct {
	mu         sync.RWMutex
	strategies map[StrategyType]ConsumerStrategy
}

func NewStrategyRegistry() *StrategyRegistry {
	return &StrategyRegistry{
		strategies: make(map[StrategyType]ConsumerStrategy),
	}
}

func (r *StrategyRegistry) Register(strategy ConsumerStrategy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.strategies[strategy.Name()] = strategy
}

func (r *StrategyRegistry) Get(name StrategyType) (ConsumerStrategy, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.strategies[name]
	return s, ok
}

func (r *StrategyRegistry) List() []StrategyType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]StrategyType, 0, len(r.strategies))
	for t := range r.strategies {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })
	return types
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseStrategyType(s string) StrategyType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "round_robin", "roundrobin", "rr":
		return StrategyRoundRobin
	case "weighted_round_robin", "weightedroundrobin", "wrr":
		return StrategyWeightedRoundRobin
	case "least_loaded", "leastloaded", "ll":
		return StrategyLeastLoaded
	case "random":
		return StrategyRandom
	case "consistent_hash", "consistenthash", "ch":
		return StrategyConsistentHash
	case "capability_based", "capabilitybased", "cap":
		return StrategyCapabilityBased
	case "priority_aware", "priorityaware", "pa":
		return StrategyPriorityAware
	case "adaptive_load_balance", "adaptiveloadbalance", "alb":
		return StrategyAdaptiveLoadBalance
	case "sticky_session", "stickysession", "ss":
		return StrategyStickySession
	case "fan_out", "fanout", "fo":
		return StrategyFanOut
	case "rule_based", "rulebased", "rb":
		return StrategyRuleBased
	case "power_of_two_choices", "poweroftwochoices", "p2c":
		return StrategyPowerOfTwoChoices
	case "sharded", "sh":
		return StrategySharded
	default:
		return StrategyRoundRobin
	}
}
