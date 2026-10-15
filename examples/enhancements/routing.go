package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/broker"
)

func routingExample(ctx context.Context, _ string) error {
	r := broker.NewRouter()
	for _, route := range []broker.Route{
		{Name: "exact", Match: "orders.created", To: []broker.Destination{{Kind: broker.DestinationQueue, Name: "billing"}}},
		{Name: "wildcard", Match: "orders.*", To: []broker.Destination{{Kind: broker.DestinationQueue, Name: "billing"}}},
		{Name: "audit", Match: "#", CopyTo: []broker.Destination{{Kind: broker.DestinationStream, Name: "audit"}}},
	} {
		r.Add(route)
	}
	dests, matched := r.Route(ctx, broker.Message{Topic: "orders.created"})
	if len(dests) != 2 || len(matched) != 3 {
		return fmt.Errorf("unexpected routing: destinations=%v, matched=%v", dests, matched)
	}
	fmt.Printf("  Exact + wildcard: %s, %s, %s; duplicate billing destination removed\n", matched[0].Name, matched[1].Name, matched[2].Name)
	r.Remove("exact")
	r.RemovePrefix("wild")
	_, matched = r.Route(ctx, broker.Message{Topic: "orders.created"})
	if len(matched) != 1 || matched[0].Name != "audit" {
		return fmt.Errorf("route removal left stale bindings")
	}
	r.Clear()

	candidates := []broker.ConsumerCandidate{{ConsumerID: "a", Weight: 0, MaxInFlight: 10}, {ConsumerID: "b", Weight: 2, MaxInFlight: 10}}
	for _, strategy := range []broker.StrategyType{broker.StrategyRoundRobin, broker.StrategyWeightedRoundRobin} {
		r := broker.NewRouter()
		r.Add(broker.Route{Name: "workers", Match: "jobs", Strategy: strategy})
		counts := map[string]int{}
		for i := 0; i < 6; i++ {
			selected, _, err := r.RouteWithStrategy(ctx, broker.Message{Topic: "jobs"}, candidates)
			if err != nil || len(selected) != 1 {
				return fmt.Errorf("selection=%v, error=%v", selected, err)
			}
			counts[selected[0].ConsumerID]++
		}
		fmt.Printf("  %s: a=%d b=%d (nonpositive weight counts as one)\n", strategy, counts["a"], counts["b"])
	}

	r.Add(broker.Route{Name: "sticky", Match: "jobs", Strategy: broker.StrategyStickySession, MaxStickyBindings: 2})
	selectWorker := func(key string) (string, error) {
		selected, _, err := r.RouteWithStrategy(ctx, broker.Message{Topic: "jobs", Key: key}, candidates)
		if err != nil || len(selected) != 1 {
			return "", fmt.Errorf("sticky selection=%v, error=%v", selected, err)
		}
		return selected[0].ConsumerID, nil
	}
	first, err := selectWorker("customer-1")
	if err != nil {
		return err
	}
	candidates[0].InFlight = 9 // b now has more free capacity, but a is still usable.
	sticky, err := selectWorker("customer-1")
	if err != nil || sticky != first {
		return fmt.Errorf("binding was not retained: %q -> %q, %v", first, sticky, err)
	}
	candidates[0].InFlight = 10
	failover, err := selectWorker("customer-1")
	if err != nil || failover != "b" {
		return fmt.Errorf("saturated worker did not fail over: %q, %v", failover, err)
	}
	for _, key := range []string{"customer-2", "customer-3"} {
		if _, err := selectWorker(key); err != nil {
			return err
		}
	}
	candidates[0].InFlight, candidates[1].InFlight = 0, 5
	reassigned, err := selectWorker("customer-1")
	if err != nil || reassigned != "a" {
		return fmt.Errorf("evicted session was not reassigned: %q, %v", reassigned, err)
	}
	fmt.Println("  Sticky: a retained across load change, b on saturation, a after FIFO eviction")

	// Filtering also applies when no route matches the subject.
	filtered, _, err := broker.NewRouter().RouteWithStrategy(ctx, broker.Message{TenantID: "tenant-a"}, []broker.ConsumerCandidate{{ConsumerID: "a", TenantID: "tenant-a"}, {ConsumerID: "b", TenantID: "tenant-b"}})
	if err != nil || len(filtered) != 1 || filtered[0].ConsumerID != "a" {
		return fmt.Errorf("tenant fallback=%v, error=%v", filtered, err)
	}
	loader := broker.NewRoutingConfigLoader()
	tasks := broker.NewTaskRouter(loader)
	if err := loader.LoadFromBytes([]byte(`{"default_strategy":"least_loaded"}`)); err != nil {
		return err
	}
	if err := tasks.Reload(); err != nil {
		return err
	}
	selected, err := tasks.SelectConsumers(ctx, broker.Message{}, candidates)
	if err != nil || len(selected) != 1 || selected[0].ConsumerID != "a" {
		return fmt.Errorf("reloaded fallback=%v, error=%v", selected, err)
	}
	fmt.Println("  Tenant filtering without a match; task-router fallback reloaded")
	return nil
}
