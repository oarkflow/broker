package broker

import (
	"context"
	"strings"
	"sync"
)

type RouteMode string

const (
	RouteDirect         RouteMode = "direct"
	RouteTopic          RouteMode = "topic"
	RouteFanout         RouteMode = "fanout"
	RouteHeaders        RouteMode = "headers"
	RouteRules          RouteMode = "rules"
	RouteConsistentHash RouteMode = "consistent_hash"
	RoutePriority       RouteMode = "priority"
	RouteDelay          RouteMode = "delay"
	RouteStream         RouteMode = "stream"
)

type Route struct {
	Name        string            `json:"name"`
	Mode        RouteMode         `json:"mode"`
	Match       string            `json:"match"`
	Headers     map[string]string `json:"headers,omitempty"`
	PriorityMin int               `json:"priority_min,omitempty"`
	TenantID    string            `json:"tenant_id,omitempty"`
	To          []Destination     `json:"to,omitempty"`
	CopyTo      []Destination     `json:"copy_to,omitempty"`
	Enabled     bool              `json:"enabled"`
	// Disabled lets callers keep a route present in config/admin output while
	// making it inactive. Enabled defaults to true for backward-compatible zero
	// value route declarations; set Disabled when an explicit off switch is
	// required.
	Disabled  bool               `json:"disabled,omitempty"`
	Condition func(Message) bool `json:"-"`
}

type Router struct {
	mu     sync.RWMutex
	routes []Route
}

func NewRouter() *Router { return &Router{} }
func (r *Router) Add(route Route) {
	if route.Mode == "" {
		route.Mode = RouteTopic
	}
	if route.Disabled {
		route.Enabled = false
	} else if route.Enabled == false {
		// Preserve the original ergonomic zero-value behavior: a route literal with
		// no Enabled field is active. Use Disabled:true for an explicit inactive
		// route.
		route.Enabled = true
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
func (r *Router) Clear() { r.mu.Lock(); r.routes = nil; r.mu.Unlock() }
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
