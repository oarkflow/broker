package broker

import (
	"fmt"
	"strconv"
	"strings"
)

// ConsumerFilter evaluates whether a message matches a consumer filter.
type ConsumerFilter interface {
	Match(msg Message) bool
}

// HeaderMatchFilter matches when all specified key/value pairs match message headers.
type HeaderMatchFilter struct {
	Required map[string]string
}

// Match evaluates whether the message contains all required headers.
func (f *HeaderMatchFilter) Match(msg Message) bool {
	if len(f.Required) == 0 {
		return true
	}
	if msg.Headers == nil {
		return false
	}
	for k, v := range f.Required {
		if msg.Headers[k] != v {
			return false
		}
	}
	return true
}

// Predicate is a single evaluation condition on a message.
type Predicate func(msg Message) bool

// ExpressionFilter evaluates a parsed filter expression.
type ExpressionFilter struct {
	expr string
	root Predicate
}

// Match evaluates the expression against the message.
func (ef *ExpressionFilter) Match(msg Message) bool {
	if ef.root == nil {
		return true
	}
	return ef.root(msg)
}

// ParseFilterExpression parses a filter expression into a MessageFilter.
// Supported syntax:
//   - headers["key"] == "val" / headers["key"] != "val"
//   - headers.has("key")
//   - type == "val" / type != "val"
//   - topic == "val" / topic != "val"
//   - priority > N / priority >= N / priority < N / priority <= N / priority == N
//   - Conjunctions: &&, AND, ||, OR
func ParseFilterExpression(raw string) (*ExpressionFilter, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return &ExpressionFilter{expr: "", root: func(msg Message) bool { return true }}, nil
	}

	// Split by OR first
	orParts := splitTopLevel(trimmed, []string{"||", " OR ", " or "})
	if len(orParts) > 1 {
		predicates := make([]Predicate, len(orParts))
		for i, part := range orParts {
			p, err := ParseFilterExpression(part)
			if err != nil {
				return nil, err
			}
			predicates[i] = p.root
		}
		return &ExpressionFilter{
			expr: trimmed,
			root: func(msg Message) bool {
				for _, p := range predicates {
					if p(msg) {
						return true
					}
				}
				return false
			},
		}, nil
	}

	// Split by AND
	andParts := splitTopLevel(trimmed, []string{"&&", " AND ", " and "})
	if len(andParts) > 1 {
		predicates := make([]Predicate, len(andParts))
		for i, part := range andParts {
			p, err := ParseFilterExpression(part)
			if err != nil {
				return nil, err
			}
			predicates[i] = p.root
		}
		return &ExpressionFilter{
			expr: trimmed,
			root: func(msg Message) bool {
				for _, p := range predicates {
					if !p(msg) {
						return false
					}
				}
				return true
			},
		}, nil
	}

	pred, err := parseSinglePredicate(trimmed)
	if err != nil {
		return nil, err
	}
	return &ExpressionFilter{expr: trimmed, root: pred}, nil
}

func parseSinglePredicate(s string) (Predicate, error) {
	s = strings.TrimSpace(s)

	// headers.has("key")
	if strings.HasPrefix(s, "headers.has(") && strings.HasSuffix(s, ")") {
		key := extractQuoted(s[len("headers.has(") : len(s)-1])
		return func(msg Message) bool {
			if msg.Headers == nil {
				return false
			}
			_, ok := msg.Headers[key]
			return ok
		}, nil
	}

	// Operators: ==, !=, >=, <=, >, <
	ops := []string{"==", "!=", ">=", "<=", ">", "<"}
	for _, op := range ops {
		if idx := strings.Index(s, op); idx != -1 {
			left := strings.TrimSpace(s[:idx])
			right := strings.TrimSpace(s[idx+len(op):])
			rightVal := extractQuoted(right)

			// Field extraction
			switch {
			case left == "type":
				return makeStringPred(op, func(m Message) string { return m.Type }, rightVal)
			case left == "topic":
				return makeStringPred(op, func(m Message) string { return m.Topic }, rightVal)
			case left == "queue":
				return makeStringPred(op, func(m Message) string { return m.Queue }, rightVal)
			case left == "tenant" || left == "tenant_id":
				return makeStringPred(op, func(m Message) string { return m.TenantID }, rightVal)
			case left == "priority":
				targetInt, err := strconv.Atoi(rightVal)
				if err != nil {
					return nil, fmt.Errorf("invalid integer for priority: %s", rightVal)
				}
				return makeIntPred(op, func(m Message) int { return m.Priority }, targetInt)
			case strings.HasPrefix(left, "headers[") && strings.HasSuffix(left, "]"):
				key := extractQuoted(left[len("headers[") : len(left)-1])
				return makeStringPred(op, func(m Message) string {
					if m.Headers == nil {
						return ""
					}
					return m.Headers[key]
				}, rightVal)
			case strings.HasPrefix(left, "header.") || strings.HasPrefix(left, "headers."):
				parts := strings.SplitN(left, ".", 2)
				key := parts[1]
				return makeStringPred(op, func(m Message) string {
					if m.Headers == nil {
						return ""
					}
					return m.Headers[key]
				}, rightVal)
			default:
				return nil, fmt.Errorf("unknown field in predicate: %s", left)
			}
		}
	}

	return nil, fmt.Errorf("unsupported filter predicate: %s", s)
}

func makeStringPred(op string, getter func(Message) string, target string) (Predicate, error) {
	switch op {
	case "==":
		return func(m Message) bool { return getter(m) == target }, nil
	case "!=":
		return func(m Message) bool { return getter(m) != target }, nil
	default:
		return nil, fmt.Errorf("operator %s not supported for string fields", op)
	}
}

func makeIntPred(op string, getter func(Message) int, target int) (Predicate, error) {
	switch op {
	case "==":
		return func(m Message) bool { return getter(m) == target }, nil
	case "!=":
		return func(m Message) bool { return getter(m) != target }, nil
	case ">":
		return func(m Message) bool { return getter(m) > target }, nil
	case ">=":
		return func(m Message) bool { return getter(m) >= target }, nil
	case "<":
		return func(m Message) bool { return getter(m) < target }, nil
	case "<=":
		return func(m Message) bool { return getter(m) <= target }, nil
	default:
		return nil, fmt.Errorf("unknown int operator %s", op)
	}
}

func extractQuoted(s string) string {
	s = strings.TrimSpace(s)
	if (strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return s[1 : len(s)-1]
	}
	return s
}

func splitTopLevel(s string, delims []string) []string {
	for _, delim := range delims {
		if strings.Contains(s, delim) {
			parts := strings.Split(s, delim)
			res := make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					res = append(res, p)
				}
			}
			return res
		}
	}
	return []string{s}
}

// NewConsumerFilter creates a ConsumerFilter from a ConsumerConfig.
func NewConsumerFilter(cfg ConsumerConfig) (ConsumerFilter, error) {
	if cfg.FilterExpression != "" {
		return ParseFilterExpression(cfg.FilterExpression)
	}
	if len(cfg.FilterHeaders) > 0 {
		return &HeaderMatchFilter{Required: cfg.FilterHeaders}, nil
	}
	return nil, nil
}
