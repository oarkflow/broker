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
	MaxStickyBindings int             `json:"max_sticky_bindings,omitempty"`
	Capabilities      []string        `json:"capabilities,omitempty"`
	PriorityThreshold int             `json:"priority_threshold,omitempty"`
	Rules             []RoutingRule   `json:"rules,omitempty"`
	StrategyConfig    *StrategyConfig `json:"-"`
	compiledStrategy  ConsumerStrategy
}

type Router struct {
	mu           sync.RWMutex
	table        *routeTable
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
	route = cloneRoute(route)
	route.compiledStrategy = r.buildStrategyForRoute(route)
	r.mu.Lock()
	r.table = r.table.with(route)
	r.mu.Unlock()
}

// routeTable is an immutable routing snapshot. Mutations build a new table, so
// a routing pass can read one consistent view without holding the router lock
// while user conditions and strategies run.
type routeTable struct {
	routes   []Route
	exact    map[string][]int
	patterns []int
}

func newRouteTable(routes []Route) *routeTable {
	t := &routeTable{routes: routes}
	for i := range routes {
		t.index(i)
	}
	return t
}

// with returns a new table that appends route; the receiver is unchanged.
func (t *routeTable) with(route Route) *routeTable {
	var routes []Route
	if t != nil {
		routes = t.routes
	}
	next := make([]Route, len(routes)+1)
	copy(next, routes)
	next[len(routes)] = route
	return newRouteTable(next)
}

// index separates exact subjects from wildcard/catch-all routes. Only whole
// tokens are wildcards: "orders.cr*eated" is a literal subject.
func (t *routeTable) index(i int) {
	pattern := t.routes[i].Match
	if pattern == "" {
		t.patterns = append(t.patterns, i)
		return
	}
	for _, token := range strings.Split(pattern, ".") {
		if token == "*" || token == "#" {
			t.patterns = append(t.patterns, i)
			return
		}
	}
	if t.exact == nil {
		t.exact = make(map[string][]int)
	}
	t.exact[pattern] = append(t.exact[pattern], i)
}

// candidates returns the routes that could match subject in registration order.
func (t *routeTable) candidates(subject string) []Route {
	if t == nil {
		return nil
	}
	it := routeIndexIterator{exact: t.exact[subject], patterns: t.patterns}
	out := make([]Route, 0, len(it.exact)+len(it.patterns))
	for i, ok := it.next(); ok; i, ok = it.next() {
		out = append(out, t.routes[i])
	}
	return out
}

func (r *Router) current() *routeTable {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.table
}

// Merge the two ordered index lists without allocating or reordering routes.
// Registration order also determines which strategy gets the first selection.
type routeIndexIterator struct {
	exact, patterns []int
}

func (it *routeIndexIterator) next() (int, bool) {
	if len(it.exact) > 0 && (len(it.patterns) == 0 || it.exact[0] < it.patterns[0]) {
		i := it.exact[0]
		it.exact = it.exact[1:]
		return i, true
	}
	if len(it.patterns) > 0 {
		i := it.patterns[0]
		it.patterns = it.patterns[1:]
		return i, true
	}
	return 0, false
}

// Remove deletes a route by name and reports whether one was found. Every Add has
// a matching Remove: remote consumer subscriptions add routes to this global table,
// so without a way to take them out again a fleet of transient workers grew the
// routing table without bound.
func (r *Router) Remove(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.table == nil {
		return false
	}
	for i, route := range r.table.routes {
		if route.Name != name {
			continue
		}
		kept := make([]Route, 0, len(r.table.routes)-1)
		kept = append(kept, r.table.routes[:i]...)
		kept = append(kept, r.table.routes[i+1:]...)
		r.table = newRouteTable(kept)
		return true
	}
	return false
}

// RemovePrefix deletes every route whose name starts with prefix and returns how
// many were removed.
func (r *Router) RemovePrefix(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.table == nil {
		return 0
	}
	kept := make([]Route, 0, len(r.table.routes))
	removed := 0
	for _, route := range r.table.routes {
		if strings.HasPrefix(route.Name, prefix) {
			removed++
			continue
		}
		kept = append(kept, route)
	}
	if removed > 0 {
		r.table = newRouteTable(kept)
	}
	return removed
}

func (r *Router) Routes() []Route {
	table := r.current()
	if table == nil {
		return []Route{}
	}
	out := make([]Route, len(table.routes))
	for i := range table.routes {
		out[i] = cloneRoute(table.routes[i])
	}
	return out
}

func cloneRoute(route Route) Route {
	route.Headers = cloneMap(route.Headers)
	route.To = append([]Destination(nil), route.To...)
	route.CopyTo = append([]Destination(nil), route.CopyTo...)
	route.Capabilities = append([]string(nil), route.Capabilities...)
	route.Rules = cloneRoutingRules(route.Rules)
	if route.StrategyConfig != nil {
		config := *route.StrategyConfig
		config.Capabilities = append([]string(nil), config.Capabilities...)
		config.Rules = cloneRoutingRules(config.Rules)
		config.Metadata = cloneMap(config.Metadata)
		route.StrategyConfig = &config
	}
	return route
}

func cloneRoutingRules(rules []RoutingRule) []RoutingRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]RoutingRule, len(rules))
	copy(out, rules)
	for i := range out {
		out[i].MatchHeaders = cloneMap(out[i].MatchHeaders)
		out[i].TargetConsumers = append([]string(nil), out[i].TargetConsumers...)
		out[i].Metadata = cloneMap(out[i].Metadata)
	}
	return out
}

func (r *Router) Clear() {
	r.mu.Lock()
	r.table = nil
	r.mu.Unlock()
}

func (r *Router) Route(ctx context.Context, msg Message) ([]Destination, []Route) {
	table := r.current() // one consistent view for the whole nested traversal
	dests := make([]Destination, 0, 4)
	matched := make([]Route, 0, 2)
	visitedRoutes := make(map[string]struct{})
	path := make(map[string]struct{})
	if msg.Topic != "" {
		path[msg.Topic] = struct{}{}
	}
	var routeInto func(Message)
	routeInto = func(current Message) {
		subject := msg.RoutingKey
		if subject == "" {
			subject = current.Topic
		}
		if subject == "" {
			subject = current.Queue
		}
		for _, route := range table.candidates(subject) {
			if route.Disabled || !route.Enabled {
				continue
			}
			if route.Match != "" && !TopicMatch(route.Match, subject) {
				continue
			}
			if route.TenantID != "" && !tenantWithinScope(current.TenantID, route.TenantID) {
				continue
			}
			if route.PriorityMin != 0 && current.Priority < route.PriorityMin {
				continue
			}
			ok := true
			for k, v := range route.Headers {
				if current.Headers[k] != v {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			if route.Condition != nil && !route.Condition(cloneMessage(current)) {
				continue
			}
			if _, seen := visitedRoutes[route.Name]; !seen {
				matched = append(matched, cloneRoute(route))
				visitedRoutes[route.Name] = struct{}{}
			}
			for _, destination := range append(append([]Destination(nil), route.To...), route.CopyTo...) {
				if destination.Kind != DestinationExchange || destination.Name == "" {
					dests = append(dests, destination)
					continue
				}
				if _, seen := path[destination.Name]; seen {
					continue
				}
				path[destination.Name] = struct{}{}
				nested := current
				nested.Topic = destination.Name
				routeInto(nested)
				delete(path, destination.Name)
			}
		}
	}
	routeInto(msg)
	return dedupeDestinations(dests), matched
}

// RouteWithStrategy selects consumers using the strategy assigned to the matched route.
func (r *Router) RouteWithStrategy(ctx context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, []Route, error) {
	subject := msg.RoutingKey
	if subject == "" {
		subject = msg.Topic
	}
	if subject == "" {
		subject = msg.Queue
	}
	matched := make([]Route, 0, 2)
	for _, route := range r.current().candidates(subject) {
		if route.Disabled || !route.Enabled {
			continue
		}
		if route.Match != "" && !TopicMatch(route.Match, subject) {
			continue
		}
		if route.TenantID != "" && !tenantWithinScope(msg.TenantID, route.TenantID) {
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
		if route.Condition != nil && !route.Condition(cloneMessage(msg)) {
			continue
		}
		matched = append(matched, cloneRoute(route))
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
			result, err := route.compiledStrategy.Select(ctx, msg, candidates)
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
		MaxStickyBindings: route.MaxStickyBindings,
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
