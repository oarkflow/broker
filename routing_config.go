package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// RoutingRule defines a single routing rule for rule-based strategy.
type RoutingRule struct {
	Name            string            `json:"name"`
	Priority        int               `json:"priority"`
	Enabled         bool              `json:"enabled"`
	MatchTopic      string            `json:"match_topic,omitempty"`
	MatchType       string            `json:"match_type,omitempty"`
	MatchHeaders    map[string]string `json:"match_headers,omitempty"`
	MatchTenantID   string            `json:"match_tenant_id,omitempty"`
	MatchPriority   int               `json:"match_priority_min,omitempty"`
	MatchKeyPattern string            `json:"match_key_pattern,omitempty"`
	TargetConsumers []string          `json:"target_consumers,omitempty"`
	Strategy        ConsumerStrategy  `json:"-"`
	StrategyType    StrategyType      `json:"strategy_type,omitempty"`
	Weight          int               `json:"weight,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// Matches returns true if the given message satisfies all conditions in this rule.
func (r *RoutingRule) Matches(msg Message) bool {
	if r.MatchTopic != "" && !TopicMatch(r.MatchTopic, msg.Topic) && !TopicMatch(r.MatchTopic, msg.RoutingKey) {
		return false
	}
	if r.MatchType != "" && !strings.EqualFold(r.MatchType, msg.Type) {
		return false
	}
	if r.MatchTenantID != "" && r.MatchTenantID != msg.TenantID {
		return false
	}
	if r.MatchPriority != 0 && msg.Priority < r.MatchPriority {
		return false
	}
	if r.MatchKeyPattern != "" && msg.Key != "" && !TopicMatch(r.MatchKeyPattern, msg.Key) {
		return false
	}
	for k, v := range r.MatchHeaders {
		if msg.Headers[k] != v {
			return false
		}
	}
	return true
}

// RoutingPlan defines a complete routing configuration for a queue or topic.
type RoutingPlan struct {
	Name          string          `json:"name"`
	Queue         string          `json:"queue,omitempty"`
	Topic         string          `json:"topic,omitempty"`
	Strategy      StrategyType    `json:"strategy"`
	Fallback      StrategyType    `json:"fallback,omitempty"`
	Weight        int             `json:"weight,omitempty"`
	HashKey       string          `json:"hash_key,omitempty"`
	StickyKey     string          `json:"sticky_key,omitempty"`
	Capabilities  []string        `json:"capabilities,omitempty"`
	PriorityThreshold int         `json:"priority_threshold,omitempty"`
	Rules         []RoutingRule   `json:"rules,omitempty"`
	Enabled       bool            `json:"enabled"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// RoutingConfig is the top-level configuration for the task routing system.
type RoutingConfig struct {
	Plans          []RoutingPlan     `json:"plans,omitempty"`
	DefaultStrategy StrategyType     `json:"default_strategy"`
	GlobalHashKey  string            `json:"global_hash_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// DefaultRoutingConfig returns a RoutingConfig with sensible defaults.
func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		DefaultStrategy: StrategyRoundRobin,
		Plans:           []RoutingPlan{},
	}
}

// RoutingConfigLoader handles loading and parsing routing configurations.
type RoutingConfigLoader struct {
	mu      sync.RWMutex
	config  RoutingConfig
	registry *StrategyRegistry
}

// NewRoutingConfigLoader creates a new config loader with default strategies registered.
func NewRoutingConfigLoader() *RoutingConfigLoader {
	loader := &RoutingConfigLoader{
		config:  DefaultRoutingConfig(),
		registry: NewStrategyRegistry(),
	}
	loader.registerDefaults()
	return loader
}

func (l *RoutingConfigLoader) registerDefaults() {
	l.registry.Register(NewRoundRobinStrategy())
	l.registry.Register(NewWeightedRoundRobinStrategy())
	l.registry.Register(NewLeastLoadedStrategy())
	l.registry.Register(NewRandomStrategy())
	l.registry.Register(NewConsistentHashStrategy(""))
	l.registry.Register(NewCapabilityBasedStrategy(nil))
	l.registry.Register(NewPriorityAwareStrategy(0))
	l.registry.Register(NewAdaptiveLoadBalanceStrategy())
	l.registry.Register(NewStickySessionStrategy(""))
	l.registry.Register(NewFanOutStrategy())
	l.registry.Register(NewRuleBasedStrategy(nil))
	l.registry.Register(NewPowerOfTwoChoicesStrategy())
	l.registry.Register(NewShardedStrategy(""))
}

// LoadFromFile loads routing configuration from a JSON file.
func (l *RoutingConfigLoader) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read routing config: %w", err)
	}
	return l.LoadFromBytes(data)
}

// LoadFromBytes parses routing configuration from JSON bytes.
func (l *RoutingConfigLoader) LoadFromBytes(data []byte) error {
	var cfg RoutingConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse routing config: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.config = cfg
	return nil
}

// LoadFromMap loads routing configuration from a map (useful for programmatic config).
func (l *RoutingConfigLoader) LoadFromMap(m map[string]any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return l.LoadFromBytes(data)
}

// GetConfig returns a copy of the current routing configuration.
func (l *RoutingConfigLoader) GetConfig() RoutingConfig {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.cloneConfig()
}

// AddPlan adds or replaces a routing plan.
func (l *RoutingConfigLoader) AddPlan(plan RoutingPlan) {
	l.mu.Lock()
	defer l.mu.Unlock()
	plan.UpdatedAt = time.Now()
	for i, p := range l.config.Plans {
		if p.Name == plan.Name {
			l.config.Plans[i] = plan
			return
		}
	}
	l.config.Plans = append(l.config.Plans, plan)
}

// RemovePlan removes a routing plan by name.
func (l *RoutingConfigLoader) RemovePlan(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, p := range l.config.Plans {
		if p.Name == name {
			l.config.Plans = append(l.config.Plans[:i], l.config.Plans[i+1:]...)
			return true
		}
	}
	return false
}

// GetPlan returns a routing plan by name.
func (l *RoutingConfigLoader) GetPlan(name string) (RoutingPlan, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, p := range l.config.Plans {
		if p.Name == name {
			return p, true
		}
	}
	return RoutingPlan{}, false
}

// GetPlanForQueue returns the first matching routing plan for a queue.
func (l *RoutingConfigLoader) GetPlanForQueue(queue string) (RoutingPlan, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, p := range l.config.Plans {
		if p.Enabled && p.Queue == queue {
			return p, true
		}
	}
	return RoutingPlan{}, false
}

// GetPlanForTopic returns the first matching routing plan for a topic.
func (l *RoutingConfigLoader) GetPlanForTopic(topic string) (RoutingPlan, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, p := range l.config.Plans {
		if p.Enabled && (p.Topic == topic || TopicMatch(p.Topic, topic)) {
			return p, true
		}
	}
	return RoutingPlan{}, false
}

// BuildStrategy builds a ConsumerStrategy from a RoutingPlan.
func (l *RoutingConfigLoader) BuildStrategy(plan RoutingPlan) ConsumerStrategy {
	cfg := StrategyConfig{
		Type:              plan.Strategy,
		HashKey:           plan.HashKey,
		StickyKey:         plan.StickyKey,
		PriorityThreshold: plan.PriorityThreshold,
		Capabilities:      plan.Capabilities,
		FallbackStrategy:  plan.Fallback,
		Rules:             plan.Rules,
	}
	return NewStrategy(cfg)
}

// Registry returns the strategy registry.
func (l *RoutingConfigLoader) Registry() *StrategyRegistry {
	return l.registry
}

func (l *RoutingConfigLoader) cloneConfig() RoutingConfig {
	out := l.config
	out.Plans = make([]RoutingPlan, len(l.config.Plans))
	copy(out.Plans, l.config.Plans)
	if l.config.Metadata != nil {
		out.Metadata = make(map[string]string, len(l.config.Metadata))
		for k, v := range l.config.Metadata {
			out.Metadata[k] = v
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// BCL Integration Layer
// ---------------------------------------------------------------------------

// BCLRoutingConfig holds BCL-compatible routing configuration.
// This struct can be populated from BCL decision tables or from standard
// JSON/YAML config files.
type BCLRoutingConfig struct {
	Version     string                   `json:"version"`
	Description string                   `json:"description,omitempty"`
	Strategies  []BCLStrategyDefinition `json:"strategies"`
	Rules       []BCLRuleDefinition     `json:"rules"`
	Plans       []BCLPlanDefinition     `json:"plans"`
	Policies    []BCLPolicyDefinition    `json:"policies,omitempty"`
}

type BCLStrategyDefinition struct {
	Name       string         `json:"name"`
	Type       StrategyType   `json:"type"`
	Params     map[string]any `json:"params,omitempty"`
	Conditions []BCLCondition `json:"conditions,omitempty"`
}

type BCLRuleDefinition struct {
	Name       string         `json:"name"`
	Priority   int            `json:"priority"`
	When       []BCLCondition `json:"when"`
	Then       BCLAction      `json:"then"`
	Enabled    bool           `json:"enabled"`
}

type BCLCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type BCLAction struct {
	Strategy     string   `json:"strategy,omitempty"`
	Consumers    []string `json:"consumers,omitempty"`
	Queue        string   `json:"queue,omitempty"`
	Weight       int      `json:"weight,omitempty"`
}

type BCLPlanDefinition struct {
	Name     string `json:"name"`
	Queue    string `json:"queue,omitempty"`
	Topic    string `json:"topic,omitempty"`
	Strategy string `json:"strategy"`
	Fallback string `json:"fallback,omitempty"`
	Rules    []string `json:"rules,omitempty"`
}

type BCLPolicyDefinition struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Config map[string]any `json:"config,omitempty"`
}

// ParseBCLConfig parses a BCL-format routing config and converts to internal RoutingConfig.
func ParseBCLConfig(bcl BCLRoutingConfig) (RoutingConfig, error) {
	loader := NewRoutingConfigLoader()
	for _, sd := range bcl.Strategies {
		st := parseStrategyType(string(sd.Type))
		_ = st
	}
	ruleMap := make(map[string]RoutingRule)
	for _, rd := range bcl.Rules {
		rule := RoutingRule{
			Name:         rd.Name,
			Priority:     rd.Priority,
			Enabled:      rd.Enabled,
			MatchHeaders: make(map[string]string),
		}
		for _, cond := range rd.When {
			applyCondition(&rule, cond)
		}
		if rd.Then.Strategy != "" {
			st := parseStrategyType(rd.Then.Strategy)
			rule.Strategy = NewStrategy(StrategyConfig{Type: st})
			rule.StrategyType = st
		}
		rule.TargetConsumers = rd.Then.Consumers
		ruleMap[rd.Name] = rule
	}
	for _, pd := range bcl.Plans {
		plan := RoutingPlan{
			Name:     pd.Name,
			Queue:    pd.Queue,
			Topic:    pd.Topic,
			Strategy: parseStrategyType(pd.Strategy),
			Fallback: parseStrategyType(pd.Fallback),
			Enabled:  true,
		}
		for _, rn := range pd.Rules {
			if rule, ok := ruleMap[rn]; ok {
				plan.Rules = append(plan.Rules, rule)
			}
		}
		loader.AddPlan(plan)
	}
	return loader.GetConfig(), nil
}

func applyCondition(rule *RoutingRule, cond BCLCondition) {
	switch strings.ToLower(cond.Field) {
	case "topic", "msg.topic":
		rule.MatchTopic = cond.Value
	case "type", "msg.type", "task_type":
		rule.MatchType = cond.Value
	case "tenant_id", "msg.tenant_id":
		rule.MatchTenantID = cond.Value
	case "priority", "msg.priority":
		fmt.Sscanf(cond.Value, "%d", &rule.MatchPriority)
	case "key", "msg.key", "routing_key":
		rule.MatchKeyPattern = cond.Value
	default:
		if strings.HasPrefix(cond.Field, "header.") {
			headerKey := strings.TrimPrefix(cond.Field, "header.")
			if rule.MatchHeaders == nil {
				rule.MatchHeaders = make(map[string]string)
			}
			rule.MatchHeaders[headerKey] = cond.Value
		}
	}
}

// ---------------------------------------------------------------------------
// TaskRouter: High-level routing engine
// ---------------------------------------------------------------------------

// TaskRouter provides strategy-based message routing with fallback support.
type TaskRouter struct {
	mu       sync.RWMutex
	loader   *RoutingConfigLoader
	plans    map[string]*compiledPlan
	fallback ConsumerStrategy
}

type compiledPlan struct {
	strategy ConsumerStrategy
	fallback ConsumerStrategy
	plan     RoutingPlan
}

// NewTaskRouter creates a TaskRouter from a RoutingConfigLoader.
func NewTaskRouter(loader *RoutingConfigLoader) *TaskRouter {
	cfg := loader.GetConfig()
	tr := &TaskRouter{
		loader:   loader,
		plans:    make(map[string]*compiledPlan),
		fallback: NewStrategy(StrategyConfig{Type: cfg.DefaultStrategy}),
	}
	for _, p := range cfg.Plans {
		tr.compilePlan(p)
	}
	return tr
}

// Reload re-reads configuration and recompiles all plans.
func (tr *TaskRouter) Reload() error {
	cfg := tr.loader.GetConfig()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.plans = make(map[string]*compiledPlan)
	tr.fallback = NewStrategy(StrategyConfig{Type: cfg.DefaultStrategy})
	for _, p := range cfg.Plans {
		tr.compilePlan(p)
	}
	return nil
}

func (tr *TaskRouter) compilePlan(p RoutingPlan) {
	strategy := tr.loader.BuildStrategy(p)
	var fallback ConsumerStrategy
	if p.Fallback != "" {
		fallback = NewStrategy(StrategyConfig{Type: p.Fallback})
	}
	tr.plans[p.Name] = &compiledPlan{
		strategy: strategy,
		fallback: fallback,
		plan:     p,
	}
}

// SelectConsumers selects one or more consumers for the given message.
func (tr *TaskRouter) SelectConsumers(ctx context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	plan := tr.findPlan(msg)
	if plan == nil {
		return tr.fallback.Select(ctx, msg, candidates)
	}
	result, err := plan.strategy.Select(ctx, msg, candidates)
	if err != nil && plan.fallback != nil {
		return plan.fallback.Select(ctx, msg, candidates)
	}
	if err != nil {
		return tr.fallback.Select(ctx, msg, candidates)
	}
	return result, nil
}

func (tr *TaskRouter) findPlan(msg Message) *compiledPlan {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if msg.Queue != "" {
		for _, p := range tr.plans {
			if p.plan.Queue == msg.Queue {
				return p
			}
		}
	}
	if msg.Topic != "" {
		for _, p := range tr.plans {
			if p.plan.Topic != "" && TopicMatch(p.plan.Topic, msg.Topic) {
				return p
			}
		}
	}
	if msg.TaskName != "" {
		for _, p := range tr.plans {
			if p.plan.Queue == "tasks."+msg.TaskName || p.plan.Name == msg.TaskName {
				return p
			}
		}
	}
	return nil
}

// RegisterPlan adds a plan at runtime.
func (tr *TaskRouter) RegisterPlan(plan RoutingPlan) {
	plan.UpdatedAt = time.Now()
	tr.loader.AddPlan(plan)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.compilePlan(plan)
}

// UnregisterPlan removes a plan at runtime.
func (tr *TaskRouter) UnregisterPlan(name string) {
	tr.loader.RemovePlan(name)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	delete(tr.plans, name)
}

// ListPlans returns all active plan names.
func (tr *TaskRouter) ListPlans() []string {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	names := make([]string, 0, len(tr.plans))
	for n := range tr.plans {
		names = append(names, n)
	}
	return names
}
