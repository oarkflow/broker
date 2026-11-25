package broker

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func BenchmarkRouterExactSubjects(b *testing.B) {
	for _, count := range []int{1, 100, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			r := NewRouter()
			for i := 0; i < count; i++ {
				r.Add(Route{Name: fmt.Sprint(i), Match: fmt.Sprintf("events.%d", i), To: []Destination{{Kind: DestinationQueue, Name: "jobs"}}})
			}
			msg := Message{Topic: fmt.Sprintf("events.%d", count-1)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dests, matched := r.Route(context.Background(), msg)
				if len(dests) != 1 || len(matched) != 1 {
					b.Fatal("unexpected routing result")
				}
			}
		})
	}
}

func TestRouterSubjectIndexSemantics(t *testing.T) {
	r := NewRouter()
	routes := []Route{
		{Name: "unrelated", Match: "other"},
		{Name: "wildcard", Match: "events.*"},
		{Name: "exact", Match: "events.created"},
		{Name: "all"},
		{Name: "hash", Match: "#.created"},
		{Name: "wrong-tenant", Match: "events.created", TenantID: "other"},
		{Name: "wrong-header", Match: "events.created", Headers: map[string]string{"kind": "other"}},
		{Name: "priority", Match: "events.created", PriorityMin: 5},
		{Name: "condition", Match: "events.created", Condition: func(Message) bool { return false }},
		{Name: "disabled", Match: "events.created", Disabled: true},
		{Name: "literal-star", Match: "events.cr*eated"},
	}
	for _, route := range routes {
		route.To = []Destination{{Kind: DestinationQueue, Name: "jobs"}}
		r.Add(route)
	}
	check := func(msg Message, want []string) {
		t.Helper()
		dests, matched := r.Route(context.Background(), msg)
		var names []string
		for _, route := range matched {
			names = append(names, route.Name)
		}
		if !reflect.DeepEqual(names, want) {
			t.Fatalf("matched %v, want %v", names, want)
		}
		if len(want) > 0 && len(dests) != 1 {
			t.Fatalf("destinations not deduplicated: %v", dests)
		}
		_, strategyMatches, err := r.RouteWithStrategy(context.Background(), msg, nil)
		var strategyNames []string
		for _, route := range strategyMatches {
			strategyNames = append(strategyNames, route.Name)
		}
		if err != nil || !reflect.DeepEqual(names, strategyNames) {
			t.Fatalf("strategy matches %v, error %v", strategyNames, err)
		}
	}
	for _, msg := range []Message{
		{Topic: "events.created"},
		{RoutingKey: "events.created", Topic: "other"},
		{Queue: "events.created"},
	} {
		check(msg, []string{"wildcard", "exact", "all", "hash"})
	}
	check(Message{Topic: "events.cr*eated"}, []string{"wildcard", "all", "literal-star"})
	check(Message{Topic: "events.created", TenantID: "other", Headers: map[string]string{"kind": "other"}, Priority: 5},
		[]string{"wildcard", "exact", "all", "hash", "wrong-tenant", "wrong-header", "priority"})
	if !r.Remove("exact") {
		t.Fatal("remove failed")
	}
	check(Message{Topic: "events.created"}, []string{"wildcard", "all", "hash"})
	if got := r.RemovePrefix("wild"); got != 1 {
		t.Fatalf("removed %d routes", got)
	}
	check(Message{Topic: "events.created"}, []string{"all", "hash"})
	r.Clear()
	check(Message{Topic: "events.created"}, nil)
	r.Add(Route{Name: "new", Match: "events.created", To: []Destination{{Kind: DestinationQueue, Name: "jobs"}}})
	check(Message{Topic: "events.created"}, []string{"new"})
}

// Compare indexed lookup to the original linear matcher over edge cases,
// including empty tokens, repeated bindings and multiple hash wildcards.
func TestRouterIndexMatchesLinearScan(t *testing.T) {
	r := NewRouter()
	patterns := []string{"", "#", "*", "events", "events.*", "events.#", "#.created", "#.#", "events.created", "events.created", "events.cr*eated", ".", "events.", "*.#.*"}
	for i, pattern := range patterns {
		r.Add(Route{Name: fmt.Sprint(i), Match: pattern})
	}
	for _, subject := range []string{"", ".", "events", "events.", "events.created", "events.cr*eated", "events.created.more", "other.created", "other"} {
		_, got := r.Route(context.Background(), Message{Topic: subject})
		var want []Route
		for _, route := range r.Routes() {
			if TopicMatch(route.Match, subject) {
				want = append(want, route)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("subject %q: got %v, want %v", subject, got, want)
		}
	}
}

func TestRouterIndexConcurrentMutation(t *testing.T) {
	r := NewRouter()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				msg := Message{Topic: "events.created"}
				r.Route(context.Background(), msg)
				r.RouteWithStrategy(context.Background(), msg, nil)
			}
		})
	}
	for i := 0; i < 100; i++ {
		r.Add(Route{Name: "exact", Match: "events.created"})
		r.Add(Route{Name: "wildcard", Match: "events.*"})
		r.Remove("exact")
		r.RemovePrefix("wild")
		r.Clear()
	}
	wg.Wait()
}
