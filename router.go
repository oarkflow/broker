package broker

import (
	"context"
	"strings"
	"sync"
)

type RouteMode string

const (
	RouteDirect          RouteMode = "direct"
	RouteTopic           RouteMode = "topic"
	RouteFanout          RouteMode = "fanout"
	RouteHeaders         RouteMode = "headers"
	RouteRules           RouteMode = "rules"
	RouteConsistentHash  RouteMode = "consistent_hash"
	RoutePriority        RouteMode = "priority"
	RouteDelay           RouteMode = "delay"
	RouteStream          RouteMode = "stream"
	RouteRoundRobin      RouteMode = "round_robin"
	RouteWeighted        RouteMode = "weighted_round_robin"
	RouteLeastLoaded     RouteMode = "least_loaded"
	RouteRandom          RouteMode = "random"
	RouteCapabilityBased RouteMode = "capability_based"
	RouteAdaptive        RouteMode = "adaptive_load_balance"
	RouteSticky          RouteMode = "sticky_session"
	RouteP2C             RouteMode = "power_of_two_choices"
	RouteSharded         RouteMode = "sharded"
	RouteDLX             RouteMode = "dead_letter_exchange"
)

type Route struct {
	Name        string             `json:"name"`
	Mode        RouteMode          `json:"mode"`
	Match       string             `json:"match"`
	Headers     map[string]string  `json:"headers,omitempty"`
	PriorityMin int                `json:"priority_min,omitempty"`
	TenantID    string             `json:"tenant_id,omitempty"`
	To          []Destination      `json:"to,omitempty"`
	CopyTo      []Destination      `json:"copy_to,omitempty"`
	Enabled     bool               `json:"enabled"`
	Disabled    bool               `json:"disabled,omitempty"`
	Condition   func(Message) bool `json:"-"`

	// Strategy-based routing fields
	Strategy          StrategyType    `json:"strategy,omitempty"`
	Fallback          StrategyType    `json:"fallback,omitempty"`
	Weight            int             `json:"weight,omitempty"`
	HashKey           string          `json:"hash_key,omitempty"`
	StickyKey         string          `json:"sticky_key,omitempty"`
	Capabilities      []string        `json:"capabilities,omitempty"`
	PriorityThreshold int             `json:"priority_threshold,omitempty"`
	Rules             []RoutingRule   `json:"rules,omitempty"`
	StrategyConfig    *StrategyConfig `json:"-"`
}

type Router struct {
	mu           sync.RWMutex
	routes       []Route
	taskRouter   *TaskRouter
	configLoader *RoutingConfigLoader
}

func NewRouter() *Router {
	loader := NewRoutingConfigLoader()
	return &Router{
		configLoader: loader,
		taskRouter:   NewTaskRouter(loader),
	}
}

func (r *Router) Add(route Route) {
	if route.Mode == "" {
		route.Mode = RouteTopic
	}
	if route.Disabled {
		route.Enabled = false
	} else if route.Enabled == false {
		route.Enabled = true
	}
	if route.Strategy == "" {
		route.Strategy = routeModeToStrategy(route.Mode)
	}
	r.mu.Lock()
	r.routes = append(r.routes, route)
	r.mu.Unlock()
}

func (r *Router) Routes() []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Route(nil), r.routes...)
}

func (r *Router) Clear() {
	r.mu.Lock()
	r.routes = nil
	r.mu.Unlock()
}

func (r *Router) Route(ctx context.Context, msg Message) ([]Destination, []Route) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dests := make([]Destination, 0, 4)
	matched := make([]Route, 0, 2)
	subject := msg.RoutingKey
	if subject == "" {
		subject = msg.Topic
	}
	if subject == "" {
		subject = msg.Queue
	}
	for _, route := range r.routes {
		if route.Disabled || !route.Enabled {
			continue
		}
		if route.Match != "" && !TopicMatch(route.Match, subject) {
			continue
		}
		if route.TenantID != "" && route.TenantID != msg.TenantID {
			continue
		}
		if route.PriorityMin != 0 && msg.Priority < route.PriorityMin {
			continue
		}
		ok := true
		for k, v := range route.Headers {
			if msg.Headers[k] != v {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if route.Condition != nil && !route.Condition(msg) {
			continue
		}
		matched = append(matched, route)
		dests = append(dests, route.To...)
		dests = append(dests, route.CopyTo...)
	}
	return dedupeDestinations(dests), matched
}

// RouteWithStrategy selects consumers using the strategy assigned to the matched route.
func (r *Router) RouteWithStrategy(ctx context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, []Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	subject := msg.RoutingKey
	if subject == "" {
		subject = msg.Topic
	}
	if subject == "" {
		subject = msg.Queue
	}
	matched := make([]Route, 0, 2)
	for _, route := range r.routes {
		if route.Disabled || !route.Enabled {
			continue
		}
		if route.Match != "" && !TopicMatch(route.Match, subject) {
			continue
		}
		if route.TenantID != "" && route.TenantID != msg.TenantID {
			continue
		}
		if route.PriorityMin != 0 && msg.Priority < route.PriorityMin {
			continue
		}
		ok := true
		for k, v := range route.Headers {
			if msg.Headers[k] != v {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if route.Condition != nil && !route.Condition(msg) {
			continue
		}
		matched = append(matched, route)
	}
	if len(matched) == 0 {
		return candidates, nil, nil
	}
	if len(candidates) > 0 && (msg.TenantID != "" || hasRestrictedTenantScope(ctx)) {
		filtered := make([]ConsumerCandidate, 0, len(candidates))
		for _, c := range candidates {
			if c.TenantID != "" && msg.TenantID != "" && c.TenantID != msg.TenantID {
				continue
			}
			if !tenantAllowed(ctx, c.TenantID) {
				continue
			}
			filtered = append(filtered, c)
		}
		candidates = filtered
	}
	for _, route := range matched {
		if route.Strategy != "" {
			strategy := r.buildStrategyForRoute(route)
			result, err := strategy.Select(ctx, msg, candidates)
			if err == nil && len(result) > 0 {
				return result, matched, nil
			}
		}
	}
	return candidates, matched, nil
}

// TaskRouter returns the underlying TaskRouter for advanced configuration.
func (r *Router) TaskRouter() *TaskRouter {
	return r.taskRouter
}

// ConfigLoader returns the routing configuration loader.
func (r *Router) ConfigLoader() *RoutingConfigLoader {
	return r.configLoader
}

// AddPlan adds a routing plan to the task router.
func (r *Router) AddPlan(plan RoutingPlan) {
	r.taskRouter.RegisterPlan(plan)
}

// RemovePlan removes a routing plan from the task router.
func (r *Router) RemovePlan(name string) {
	r.taskRouter.UnregisterPlan(name)
}

// LoadRoutingConfig loads a complete routing configuration from bytes.
func (r *Router) LoadRoutingConfig(data []byte) error {
	return r.configLoader.LoadFromBytes(data)
}

func (r *Router) buildStrategyForRoute(route Route) ConsumerStrategy {
	if route.StrategyConfig != nil {
		return NewStrategy(*route.StrategyConfig)
	}
	cfg := StrategyConfig{
		Type:              route.Strategy,
		HashKey:           route.HashKey,
		StickyKey:         route.StickyKey,
		PriorityThreshold: route.PriorityThreshold,
		Capabilities:      route.Capabilities,
		FallbackStrategy:  route.Fallback,
		Rules:             route.Rules,
	}
	return NewStrategy(cfg)
}

func routeModeToStrategy(mode RouteMode) StrategyType {
	switch mode {
	case RouteRoundRobin:
		return StrategyRoundRobin
	case RouteWeighted:
		return StrategyWeightedRoundRobin
	case RouteLeastLoaded:
		return StrategyLeastLoaded
	case RouteRandom:
		return StrategyRandom
	case RouteConsistentHash:
		return StrategyConsistentHash
	case RouteCapabilityBased:
		return StrategyCapabilityBased
	case RouteAdaptive:
		return StrategyAdaptiveLoadBalance
	case RouteSticky:
		return StrategyStickySession
	case RouteP2C:
		return StrategyPowerOfTwoChoices
	case RouteSharded:
		return StrategySharded
	case RouteFanout:
		return StrategyFanOut
	default:
		return StrategyRoundRobin
	}
}

func dedupeDestinations(in []Destination) []Destination {
	if len(in) < 2 {
		return in
	}
	seen := map[string]struct{}{}
	out := in[:0]
	for _, d := range in {
		k := string(d.Kind) + ":" + d.Name
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, d)
	}
	return out
}

func TopicMatch(pattern, subject string) bool {
	if pattern == "" || pattern == subject {
		return true
	}
	pp := strings.Split(pattern, ".")
	ss := strings.Split(subject, ".")
	return matchParts(pp, ss)
}

func matchParts(pattern, subject []string) bool {
	for len(pattern) > 0 {
		p := pattern[0]
		pattern = pattern[1:]
		if p == "#" {
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(subject); i++ {
				if matchParts(pattern, subject[i:]) {
					return true
				}
			}
			return false
		}
		if len(subject) == 0 {
			return false
		}
		if p != "*" && p != subject[0] {
			return false
		}
		subject = subject[1:]
	}
	return len(subject) == 0
}
