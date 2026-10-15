package broker

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestRouterRetainsStrategyState(t *testing.T) {
	for _, strategy := range []StrategyType{StrategyRoundRobin, StrategyWeightedRoundRobin} {
		t.Run(string(strategy), func(t *testing.T) {
			r := NewRouter()
			r.Add(Route{Name: "jobs", Match: "jobs", Strategy: strategy})
			candidates := []ConsumerCandidate{{ConsumerID: "a", Weight: 1}, {ConsumerID: "b", Weight: 2}, {ConsumerID: "c", Weight: 3}}
			counts := map[string]int{}
			for i := 0; i < 60; i++ {
				selected, _, err := r.RouteWithStrategy(context.Background(), Message{Topic: "jobs"}, candidates)
				if err != nil || len(selected) != 1 {
					t.Fatalf("selection %v, error %v", selected, err)
				}
				counts[selected[0].ConsumerID]++
			}
			want := map[string]int{"a": 20, "b": 20, "c": 20}
			if strategy == StrategyWeightedRoundRobin {
				want = map[string]int{"a": 10, "b": 20, "c": 30}
			}
			if !reflect.DeepEqual(counts, want) {
				t.Fatalf("distribution %v, want %v", counts, want)
			}
		})
	}
}

func TestRouterKeepsStrategyStateAcrossOtherRouteRemoval(t *testing.T) {
	r := NewRouter()
	r.Add(Route{Name: "remove", Match: "other"})
	r.Add(Route{Name: "keep", Match: "jobs"})
	candidates := []ConsumerCandidate{{ConsumerID: "a"}, {ConsumerID: "b"}}
	selected, _, _ := r.RouteWithStrategy(context.Background(), Message{Topic: "jobs"}, candidates)
	if len(selected) != 1 || selected[0].ConsumerID != "a" {
		t.Fatalf("first selection %v", selected)
	}
	r.Remove("remove")
	selected, _, _ = r.RouteWithStrategy(context.Background(), Message{Topic: "jobs"}, candidates)
	if len(selected) != 1 || selected[0].ConsumerID != "b" {
		t.Fatalf("state reset after route removal: %v", selected)
	}
}

func TestRouterStickyBindingSurvivesLoadChange(t *testing.T) {
	r := NewRouter()
	r.Add(Route{Name: "sticky", Match: "jobs", Strategy: StrategyStickySession})
	candidates := []ConsumerCandidate{{ConsumerID: "a", MaxInFlight: 10}, {ConsumerID: "b", MaxInFlight: 10, InFlight: 5}}
	msg := Message{Topic: "jobs", Key: "customer"}
	selected, _, _ := r.RouteWithStrategy(context.Background(), msg, candidates)
	if len(selected) != 1 || selected[0].ConsumerID != "a" {
		t.Fatalf("initial selection %v", selected)
	}
	candidates[0].InFlight, candidates[1].InFlight = 9, 0
	selected, _, _ = r.RouteWithStrategy(context.Background(), msg, candidates)
	if len(selected) != 1 || selected[0].ConsumerID != "a" {
		t.Fatalf("binding lost after load change: %v", selected)
	}
	candidates[0].InFlight = 10
	selected, _, _ = r.RouteWithStrategy(context.Background(), msg, candidates)
	if len(selected) != 1 || selected[0].ConsumerID != "b" {
		t.Fatalf("saturated consumer was not replaced: %v", selected)
	}
}

func TestRouterConcurrentRoundRobinFairness(t *testing.T) {
	r := NewRouter()
	r.Add(Route{Name: "jobs", Match: "jobs"})
	candidates := []ConsumerCandidate{{ConsumerID: "a"}, {ConsumerID: "b"}, {ConsumerID: "c"}}
	var mu sync.Mutex
	counts := map[string]int{}
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				selected, _, err := r.RouteWithStrategy(context.Background(), Message{Topic: "jobs"}, candidates)
				if err != nil || len(selected) != 1 {
					t.Errorf("selection %v, error %v", selected, err)
					return
				}
				mu.Lock()
				counts[selected[0].ConsumerID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(counts, map[string]int{"a": 200, "b": 200, "c": 200}) {
		t.Fatalf("unfair concurrent distribution: %v", counts)
	}
}

func TestWeightedRoundRobinMembershipAndWeights(t *testing.T) {
	s := NewWeightedRoundRobinStrategy()
	ctx := context.Background()
	candidates := []ConsumerCandidate{{ConsumerID: "a", Weight: 1}, {ConsumerID: "b", Weight: 3}}
	for i := 0; i < 3; i++ {
		s.Select(ctx, Message{}, candidates)
	}
	// Same length, but a different member, order, and weights.
	candidates = []ConsumerCandidate{{ConsumerID: "c", Weight: 2}, {ConsumerID: "a", Weight: 0}}
	counts := map[string]int{}
	for i := 0; i < 30; i++ {
		selected, err := s.Select(ctx, Message{}, candidates)
		if err != nil || len(selected) != 1 {
			t.Fatalf("selection %v, error %v", selected, err)
		}
		counts[selected[0].ConsumerID]++
	}
	if !reflect.DeepEqual(counts, map[string]int{"c": 20, "a": 10}) {
		t.Fatalf("distribution after membership change: %v", counts)
	}
}

func TestRouterFiltersTenantWithoutMatchingRoute(t *testing.T) {
	r := NewRouter()
	candidates := []ConsumerCandidate{{ConsumerID: "a", TenantID: "tenant-a"}, {ConsumerID: "b", TenantID: "tenant-b"}}
	ctx := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Tenants: []string{"tenant-a"}})
	selected, _, err := r.RouteWithStrategy(ctx, Message{Topic: "no-route", TenantID: "tenant-a"}, candidates)
	if err != nil || len(selected) != 1 || selected[0].ConsumerID != "a" {
		t.Fatalf("unmatched route bypassed tenant filtering: %v, %v", selected, err)
	}
}

func TestTaskRouterConcurrentReloadAndSelection(t *testing.T) {
	r := NewTaskRouter(NewRoutingConfigLoader())
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 100; i++ {
			if err := r.Reload(); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Go(func() {
		for i := 0; i < 100; i++ {
			selected, err := r.SelectConsumers(context.Background(), Message{Topic: "unmatched"}, testCandidates())
			if err != nil || len(selected) == 0 {
				t.Errorf("selection %v, error %v", selected, err)
			}
		}
	})
	wg.Wait()
}

func TestStickyBindingsAreBounded(t *testing.T) {
	s := NewStrategy(StrategyConfig{Type: StrategyStickySession, MaxStickyBindings: 2}).(*stickySessionStrategy)
	candidates := []ConsumerCandidate{{ConsumerID: "worker"}}
	for _, key := range []string{"a", "b", "a", "c"} {
		if _, err := s.Select(context.Background(), Message{Key: key}, candidates); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.binding) != 2 || s.binding["a"] != "" || s.binding["b"] != "worker" || s.binding["c"] != "worker" {
		t.Fatalf("unexpected bounded bindings: %v", s.binding)
	}
}
