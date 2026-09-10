package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testCandidates() []ConsumerCandidate {
	return []ConsumerCandidate{
		{ConsumerID: "c1", Group: "g1", Weight: 1, MaxInFlight: 10, InFlight: 2, Acked: 80, Failed: 5, Healthy: true},
		{ConsumerID: "c2", Group: "g1", Weight: 2, MaxInFlight: 20, InFlight: 5, Acked: 150, Failed: 10, Healthy: true},
		{ConsumerID: "c3", Group: "g1", Weight: 3, MaxInFlight: 30, InFlight: 15, Acked: 200, Failed: 20, Healthy: true},
	}
}

func testMessage(topic, key string) Message {
	return Message{
		ID:         "msg-1",
		Topic:      topic,
		Key:        key,
		RoutingKey: key,
		Headers:    map[string]string{},
		Metadata:   map[string]string{},
	}
}

func TestRoundRobinStrategy(t *testing.T) {
	s := NewRoundRobinStrategy()
	if s.Name() != StrategyRoundRobin {
		t.Fatalf("expected %s, got %s", StrategyRoundRobin, s.Name())
	}
	candidates := testCandidates()
	ctx := context.Background()
	seen := map[string]int{}
	for i := 0; i < 9; i++ {
		result, err := s.Select(ctx, testMessage("t", "k"), candidates)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 candidate, got %d", len(result))
		}
		seen[result[0].ConsumerID]++
	}
	if seen["c1"] != 3 || seen["c2"] != 3 || seen["c3"] != 3 {
		t.Fatalf("expected even distribution, got %v", seen)
	}
}

func TestRoundRobinEmpty(t *testing.T) {
	s := NewRoundRobinStrategy()
	result, err := s.Select(context.Background(), testMessage("t", "k"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 candidates, got %d", len(result))
	}
}

func TestWeightedRoundRobinStrategy(t *testing.T) {
	s := NewWeightedRoundRobinStrategy()
	if s.Name() != StrategyWeightedRoundRobin {
		t.Fatalf("expected %s, got %s", StrategyWeightedRoundRobin, s.Name())
	}
	candidates := testCandidates()
	ctx := context.Background()
	seen := map[string]int{}
	for i := 0; i < 12; i++ {
		result, err := s.Select(ctx, testMessage("t", "k"), candidates)
		if err != nil {
			t.Fatal(err)
		}
		seen[result[0].ConsumerID]++
	}
	if seen["c1"] == 0 || seen["c2"] == 0 || seen["c3"] == 0 {
		t.Fatalf("all consumers should receive some messages, got %v", seen)
	}
}

func TestLeastLoadedStrategy(t *testing.T) {
	s := NewLeastLoadedStrategy()
	if s.Name() != StrategyLeastLoaded {
		t.Fatalf("expected %s, got %s", StrategyLeastLoaded, s.Name())
	}
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", MaxInFlight: 10, InFlight: 8},
		{ConsumerID: "c2", MaxInFlight: 10, InFlight: 2},
		{ConsumerID: "c3", MaxInFlight: 10, InFlight: 5},
	}
	result, err := s.Select(context.Background(), testMessage("t", "k"), candidates)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].ConsumerID != "c2" {
		t.Fatalf("expected c2 (least loaded), got %s", result[0].ConsumerID)
	}
}

func TestLeastLoadedEmpty(t *testing.T) {
	s := NewLeastLoadedStrategy()
	result, _ := s.Select(context.Background(), testMessage("t", "k"), nil)
	if len(result) != 0 {
		t.Fatal("expected empty result")
	}
}

func TestRandomStrategy(t *testing.T) {
	s := NewRandomStrategy()
	if s.Name() != StrategyRandom {
		t.Fatalf("expected %s, got %s", StrategyRandom, s.Name())
	}
	candidates := testCandidates()
	seen := map[string]int{}
	for i := 0; i < 100; i++ {
		result, _ := s.Select(context.Background(), testMessage("t", "k"), candidates)
		seen[result[0].ConsumerID]++
	}
	if len(seen) != 3 {
		t.Fatalf("expected all 3 consumers, got %v", seen)
	}
}

func TestConsistentHashStrategy(t *testing.T) {
	s := NewConsistentHashStrategy("")
	if s.Name() != StrategyConsistentHash {
		t.Fatalf("expected %s, got %s", StrategyConsistentHash, s.Name())
	}
	candidates := testCandidates()
	ctx := context.Background()
	msg1 := testMessage("t", "key-abc")
	r1, _ := s.Select(ctx, msg1, candidates)
	r2, _ := s.Select(ctx, msg1, candidates)
	if r1[0].ConsumerID != r2[0].ConsumerID {
		t.Fatal("same key should hash to same consumer")
	}
	msg2 := testMessage("t", "key-xyz")
	r3, _ := s.Select(ctx, msg2, candidates)
	if len(r3) == 0 {
		t.Fatal("expected result")
	}
}

func TestCapabilityBasedStrategy(t *testing.T) {
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", Capabilities: []string{"image", "video"}, MaxInFlight: 10},
		{ConsumerID: "c2", Capabilities: []string{"text", "image"}, MaxInFlight: 10},
		{ConsumerID: "c3", Capabilities: []string{"ml", "data"}, MaxInFlight: 10},
	}
	s := NewCapabilityBasedStrategy([]string{"image"})
	if s.Name() != StrategyCapabilityBased {
		t.Fatalf("expected %s, got %s", StrategyCapabilityBased, s.Name())
	}
	msg := testMessage("t", "")
	msg.TaskName = "image"
	result, err := s.Select(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected at least one candidate")
	}
	for _, c := range result {
		hasCap := false
		for _, cap := range c.Capabilities {
			if cap == "image" {
				hasCap = true
				break
			}
		}
		if !hasCap {
			t.Fatalf("consumer %s doesn't have required capability", c.ConsumerID)
		}
	}
}

func TestCapabilityBasedNoMatch(t *testing.T) {
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", Capabilities: []string{"ml"}, MaxInFlight: 10},
	}
	s := NewCapabilityBasedStrategy([]string{"image"})
	result, _ := s.Select(context.Background(), testMessage("t", ""), candidates)
	if len(result) == 0 {
		t.Fatal("should fall back to all candidates")
	}
}

func TestPriorityAwareStrategy(t *testing.T) {
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", MaxInFlight: 10, InFlight: 9},
		{ConsumerID: "c2", MaxInFlight: 10, InFlight: 1},
		{ConsumerID: "c3", MaxInFlight: 10, InFlight: 5},
	}
	s := NewPriorityAwareStrategy(5)
	if s.Name() != StrategyPriorityAware {
		t.Fatalf("expected %s, got %s", StrategyPriorityAware, s.Name())
	}
	msg := testMessage("t", "")
	msg.Priority = 8
	result, _ := s.Select(context.Background(), msg, candidates)
	if result[0].ConsumerID != "c2" {
		t.Fatalf("high priority should go to least loaded, got %s", result[0].ConsumerID)
	}
	msg.Priority = 2
	result, _ = s.Select(context.Background(), msg, candidates)
	if len(result) == 0 {
		t.Fatal("expected result")
	}
}

func TestAdaptiveLoadBalanceStrategy(t *testing.T) {
	s := NewAdaptiveLoadBalanceStrategy()
	if s.Name() != StrategyAdaptiveLoadBalance {
		t.Fatalf("expected %s, got %s", StrategyAdaptiveLoadBalance, s.Name())
	}
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", MaxInFlight: 10, InFlight: 2, Acked: 100, Failed: 5},
		{ConsumerID: "c2", MaxInFlight: 10, InFlight: 8, Acked: 50, Failed: 20},
	}
	result, err := s.Select(context.Background(), testMessage("t", ""), candidates)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].ConsumerID != "c1" {
		t.Fatalf("expected c1, got %s", result[0].ConsumerID)
	}
}

func TestStickySessionStrategy(t *testing.T) {
	s := NewStickySessionStrategy("")
	if s.Name() != StrategyStickySession {
		t.Fatalf("expected %s, got %s", StrategyStickySession, s.Name())
	}
	candidates := testCandidates()
	ctx := context.Background()
	msg := testMessage("t", "session-123")
	r1, _ := s.Select(ctx, msg, candidates)
	r2, _ := s.Select(ctx, msg, candidates)
	if r1[0].ConsumerID != r2[0].ConsumerID {
		t.Fatalf("sticky session should route same key to same consumer: %s != %s", r1[0].ConsumerID, r2[0].ConsumerID)
	}
}

func TestFanOutStrategy(t *testing.T) {
	s := NewFanOutStrategy()
	if s.Name() != StrategyFanOut {
		t.Fatalf("expected %s, got %s", StrategyFanOut, s.Name())
	}
	candidates := testCandidates()
	result, err := s.Select(context.Background(), testMessage("t", ""), candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("fan out should return all healthy candidates, got %d", len(result))
	}
}

func TestFanOutStrategyFiltersUnhealthy(t *testing.T) {
	s := NewFanOutStrategy()
	candidates := []ConsumerCandidate{
		{ConsumerID: "c1", Healthy: true, MaxInFlight: 10},
		{ConsumerID: "c2", Healthy: false, MaxInFlight: 10, InFlight: 10},
		{ConsumerID: "c3", Healthy: true, MaxInFlight: 10},
	}
	result, _ := s.Select(context.Background(), testMessage("t", ""), candidates)
	if len(result) != 2 {
		t.Fatalf("expected 2 healthy candidates, got %d", len(result))
	}
}

func TestRuleBasedStrategy(t *testing.T) {
	rules := []RoutingRule{
		{
			Name:            "high-pri",
			Priority:        100,
			Enabled:         true,
			MatchTopic:      "important.*",
			TargetConsumers: []string{"c1"},
		},
		{
			Name:            "low-pri",
			Priority:        1,
			Enabled:         true,
			TargetConsumers: []string{"c2", "c3"},
		},
	}
	s := NewRuleBasedStrategy(rules)
	if s.Name() != StrategyRuleBased {
		t.Fatalf("expected %s, got %s", StrategyRuleBased, s.Name())
	}
	candidates := testCandidates()
	msg := testMessage("important.data", "")
	result, err := s.Select(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].ConsumerID != "c1" {
		t.Fatalf("expected c1 for important topic, got %s", result[0].ConsumerID)
	}
	msg2 := testMessage("normal.data", "")
	result2, _ := s.Select(context.Background(), msg2, candidates)
	if len(result2) != 2 {
		t.Fatalf("expected 2 candidates for low-pri rule, got %d", len(result2))
	}
}

func TestPowerOfTwoChoicesStrategy(t *testing.T) {
	s := NewPowerOfTwoChoicesStrategy()
	if s.Name() != StrategyPowerOfTwoChoices {
		t.Fatalf("expected %s, got %s", StrategyPowerOfTwoChoices, s.Name())
	}
	candidates := testCandidates()
	seen := map[string]int{}
	for i := 0; i < 100; i++ {
		result, _ := s.Select(context.Background(), testMessage("t", ""), candidates)
		seen[result[0].ConsumerID]++
	}
	if len(seen) < 2 {
		t.Fatalf("expected at least 2 consumers, got %v", seen)
	}
}

func TestPowerOfTwoChoicesEmpty(t *testing.T) {
	s := NewPowerOfTwoChoicesStrategy()
	result, _ := s.Select(context.Background(), testMessage("t", ""), nil)
	if len(result) != 0 {
		t.Fatal("expected empty")
	}
}

func TestPowerOfTwoChoicesSingle(t *testing.T) {
	s := NewPowerOfTwoChoicesStrategy()
	c := []ConsumerCandidate{{ConsumerID: "c1", MaxInFlight: 10}}
	result, _ := s.Select(context.Background(), testMessage("t", ""), c)
	if result[0].ConsumerID != "c1" {
		t.Fatal("expected c1")
	}
}

func TestShardedStrategy(t *testing.T) {
	s := NewShardedStrategy("")
	if s.Name() != StrategySharded {
		t.Fatalf("expected %s, got %s", StrategySharded, s.Name())
	}
	candidates := testCandidates()
	ctx := context.Background()
	msg := testMessage("t", "shard-42")
	r1, _ := s.Select(ctx, msg, candidates)
	r2, _ := s.Select(ctx, msg, candidates)
	if r1[0].ConsumerID != r2[0].ConsumerID {
		t.Fatal("same shard key should go to same consumer")
	}
}

func TestNewStrategyFactory(t *testing.T) {
	types := []StrategyType{
		StrategyRoundRobin,
		StrategyWeightedRoundRobin,
		StrategyLeastLoaded,
		StrategyRandom,
		StrategyConsistentHash,
		StrategyCapabilityBased,
		StrategyPriorityAware,
		StrategyAdaptiveLoadBalance,
		StrategyStickySession,
		StrategyFanOut,
		StrategyRuleBased,
		StrategyPowerOfTwoChoices,
		StrategySharded,
	}
	for _, st := range types {
		s := NewStrategy(StrategyConfig{Type: st})
		if s == nil {
			t.Fatalf("NewStrategy returned nil for %s", st)
		}
		if s.Name() != st {
			t.Fatalf("expected %s, got %s", st, s.Name())
		}
	}
}

func TestNewStrategyDefault(t *testing.T) {
	s := NewStrategy(StrategyConfig{Type: "unknown"})
	if s.Name() != StrategyRoundRobin {
		t.Fatalf("unknown strategy should default to round_robin, got %s", s.Name())
	}
}

func TestParseStrategyType(t *testing.T) {
	tests := []struct {
		input    string
		expected StrategyType
	}{
		{"round_robin", StrategyRoundRobin},
		{"roundrobin", StrategyRoundRobin},
		{"rr", StrategyRoundRobin},
		{"weighted_round_robin", StrategyWeightedRoundRobin},
		{"least_loaded", StrategyLeastLoaded},
		{"random", StrategyRandom},
		{"consistent_hash", StrategyConsistentHash},
		{"capability_based", StrategyCapabilityBased},
		{"priority_aware", StrategyPriorityAware},
		{"adaptive_load_balance", StrategyAdaptiveLoadBalance},
		{"sticky_session", StrategyStickySession},
		{"fan_out", StrategyFanOut},
		{"rule_based", StrategyRuleBased},
		{"power_of_two_choices", StrategyPowerOfTwoChoices},
		{"sharded", StrategySharded},
		{"unknown", StrategyRoundRobin},
	}
	for _, tt := range tests {
		got := parseStrategyType(tt.input)
		if got != tt.expected {
			t.Errorf("parseStrategyType(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestConsumerCandidateLoadScore(t *testing.T) {
	c := ConsumerCandidate{MaxInFlight: 10, InFlight: 5}
	if score := c.LoadScore(); score != 0.5 {
		t.Fatalf("expected 0.5, got %f", score)
	}
	c2 := ConsumerCandidate{MaxInFlight: 10, InFlight: 10}
	if score := c2.LoadScore(); score != 0.0 {
		t.Fatalf("expected 0.0, got %f", score)
	}
	c3 := ConsumerCandidate{MaxInFlight: 0}
	if score := c3.LoadScore(); score != 1.0 {
		t.Fatalf("expected 1.0, got %f", score)
	}
	c4 := ConsumerCandidate{MaxInFlight: 10, InFlight: 15}
	if score := c4.LoadScore(); score != 0.0 {
		t.Fatalf("expected 0.0, got %f", score)
	}
}

func TestConsumerCandidateSuccessRate(t *testing.T) {
	c := ConsumerCandidate{Acked: 80, Failed: 20}
	if rate := c.SuccessRate(); rate != 0.8 {
		t.Fatalf("expected 0.8, got %f", rate)
	}
	c2 := ConsumerCandidate{Acked: 0, Failed: 0}
	if rate := c2.SuccessRate(); rate != 1.0 {
		t.Fatalf("expected 1.0, got %f", rate)
	}
}

func TestStrategyRegistry(t *testing.T) {
	r := NewStrategyRegistry()
	r.Register(NewRoundRobinStrategy())
	r.Register(NewRandomStrategy())
	if len(r.List()) != 2 {
		t.Fatalf("expected 2 strategies, got %d", len(r.List()))
	}
	s, ok := r.Get(StrategyRoundRobin)
	if !ok || s == nil {
		t.Fatal("expected to find round_robin")
	}
	_, ok = r.Get(StrategySharded)
	if ok {
		t.Fatal("should not find sharded")
	}
}

// ---------------------------------------------------------------------------
// RoutingConfig tests
// ---------------------------------------------------------------------------

func TestRoutingConfigLoader(t *testing.T) {
	loader := NewRoutingConfigLoader()
	plan := RoutingPlan{
		Name:     "test-plan",
		Queue:    "tasks.processing",
		Strategy: StrategyLeastLoaded,
		Enabled:  true,
	}
	loader.AddPlan(plan)
	cfg := loader.GetConfig()
	if len(cfg.Plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(cfg.Plans))
	}
	if cfg.Plans[0].Name != "test-plan" {
		t.Fatalf("expected test-plan, got %s", cfg.Plans[0].Name)
	}
}

func TestRoutingConfigLoaderReplace(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{Name: "p1", Queue: "q1", Strategy: StrategyRoundRobin, Enabled: true})
	loader.AddPlan(RoutingPlan{Name: "p1", Queue: "q1-updated", Strategy: StrategyLeastLoaded, Enabled: true})
	cfg := loader.GetConfig()
	if len(cfg.Plans) != 1 {
		t.Fatalf("expected 1 plan after replace, got %d", len(cfg.Plans))
	}
	if cfg.Plans[0].Queue != "q1-updated" {
		t.Fatalf("expected q1-updated, got %s", cfg.Plans[0].Queue)
	}
}

func TestRoutingConfigLoaderRemove(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{Name: "p1", Strategy: StrategyRoundRobin, Enabled: true})
	if !loader.RemovePlan("p1") {
		t.Fatal("expected true")
	}
	if loader.RemovePlan("p1") {
		t.Fatal("expected false")
	}
}

func TestRoutingConfigLoaderGetPlanForQueue(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{Name: "p1", Queue: "q1", Strategy: StrategyRoundRobin, Enabled: true})
	loader.AddPlan(RoutingPlan{Name: "p2", Queue: "q2", Strategy: StrategyRandom, Enabled: true})
	plan, ok := loader.GetPlanForQueue("q1")
	if !ok {
		t.Fatal("expected to find plan")
	}
	if plan.Strategy != StrategyRoundRobin {
		t.Fatalf("expected round_robin, got %s", plan.Strategy)
	}
}

func TestRoutingConfigLoaderGetPlanForTopic(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{Name: "p1", Topic: "events.#", Strategy: StrategyConsistentHash, Enabled: true})
	plan, ok := loader.GetPlanForTopic("events.user.created")
	if !ok {
		t.Fatal("expected to find plan")
	}
	if plan.Strategy != StrategyConsistentHash {
		t.Fatalf("expected consistent_hash, got %s", plan.Strategy)
	}
}

func TestRoutingConfigLoaderLoadFromBytes(t *testing.T) {
	loader := NewRoutingConfigLoader()
	cfg := RoutingConfig{
		DefaultStrategy: StrategyLeastLoaded,
		Plans: []RoutingPlan{
			{Name: "p1", Queue: "q1", Strategy: StrategyWeightedRoundRobin, Enabled: true},
		},
	}
	data, _ := json.Marshal(cfg)
	err := loader.LoadFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	loaded := loader.GetConfig()
	if loaded.DefaultStrategy != StrategyLeastLoaded {
		t.Fatalf("expected least_loaded, got %s", loaded.DefaultStrategy)
	}
}

func TestRoutingConfigLoaderLoadFromFile(t *testing.T) {
	data := []byte(`{"default_strategy":"random","plans":[{"name":"test","queue":"q1","strategy":"round_robin","enabled":true}]}`)
	dir := t.TempDir()
	path := filepath.Join(dir, "routing.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	loader := NewRoutingConfigLoader()
	err := loader.LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := loader.GetConfig()
	if cfg.DefaultStrategy != StrategyRandom {
		t.Fatalf("expected random, got %s", cfg.DefaultStrategy)
	}
}

func TestBuildStrategy(t *testing.T) {
	loader := NewRoutingConfigLoader()
	plan := RoutingPlan{
		Name:     "test",
		Strategy: StrategyConsistentHash,
		HashKey:  "user_id",
	}
	s := loader.BuildStrategy(plan)
	if s.Name() != StrategyConsistentHash {
		t.Fatalf("expected consistent_hash, got %s", s.Name())
	}
}

func TestRoutingRuleMatches(t *testing.T) {
	rule := RoutingRule{
		Name:          "test",
		Enabled:       true,
		MatchTopic:    "orders.*",
		MatchType:     "payment",
		MatchTenantID: "tenant-1",
		MatchPriority: 5,
		MatchHeaders:  map[string]string{"x-region": "us-east"},
	}
	msg := Message{
		Topic:    "orders.created",
		Type:     "payment",
		TenantID: "tenant-1",
		Priority: 10,
		Headers:  map[string]string{"x-region": "us-east"},
	}
	if !rule.Matches(msg) {
		t.Fatal("expected rule to match")
	}
	msg2 := Message{
		Topic:    "orders.created",
		Type:     "refund",
		TenantID: "tenant-1",
		Priority: 10,
		Headers:  map[string]string{"x-region": "us-east"},
	}
	if rule.Matches(msg2) {
		t.Fatal("expected rule not to match (type mismatch)")
	}
}

func TestRoutingRuleMatchesKeyPattern(t *testing.T) {
	rule := RoutingRule{
		Name:            "key-match",
		Enabled:         true,
		MatchKeyPattern: "user.*",
	}
	msg := Message{Key: "user.123"}
	if !rule.Matches(msg) {
		t.Fatal("expected key pattern to match")
	}
	msg2 := Message{Key: "order.456"}
	if rule.Matches(msg2) {
		t.Fatal("expected key pattern not to match")
	}
}

func TestParseBCLConfig(t *testing.T) {
	bcl := BCLRoutingConfig{
		Version: "1.0",
		Rules: []BCLRuleDefinition{
			{
				Name:     "important-rule",
				Priority: 100,
				Enabled:  true,
				When: []BCLCondition{
					{Field: "topic", Operator: "==", Value: "important.*"},
				},
				Then: BCLAction{
					Strategy:  "least_loaded",
					Consumers: []string{"c1"},
				},
			},
		},
		Plans: []BCLPlanDefinition{
			{
				Name:     "important-plan",
				Queue:    "important-queue",
				Strategy: "least_loaded",
				Rules:    []string{"important-rule"},
			},
		},
	}
	cfg, err := ParseBCLConfig(bcl)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(cfg.Plans))
	}
	if cfg.Plans[0].Strategy != StrategyLeastLoaded {
		t.Fatalf("expected least_loaded, got %s", cfg.Plans[0].Strategy)
	}
	if len(cfg.Plans[0].Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(cfg.Plans[0].Rules))
	}
}

func TestTaskRouter(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{
		Name:     "process-plan",
		Queue:    "tasks.process",
		Strategy: StrategyRoundRobin,
		Enabled:  true,
	})
	tr := NewTaskRouter(loader)
	candidates := testCandidates()
	msg := Message{Queue: "tasks.process"}
	result, err := tr.SelectConsumers(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected consumers")
	}
}

func TestTaskRouterFallback(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{
		Name:     "plan1",
		Queue:    "q1",
		Strategy: StrategyRoundRobin,
		Enabled:  true,
	})
	tr := NewTaskRouter(loader)
	candidates := testCandidates()
	msg := Message{Queue: "unknown-queue"}
	result, err := tr.SelectConsumers(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected fallback to return candidates")
	}
}

func TestTaskRouterRegisterPlan(t *testing.T) {
	loader := NewRoutingConfigLoader()
	tr := NewTaskRouter(loader)
	tr.RegisterPlan(RoutingPlan{Name: "new-plan", Queue: "q-new", Strategy: StrategyRandom, Enabled: true})
	plans := tr.ListPlans()
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	tr.UnregisterPlan("new-plan")
	plans = tr.ListPlans()
	if len(plans) != 0 {
		t.Fatalf("expected 0 plans, got %d", len(plans))
	}
}

func TestTaskRouterReload(t *testing.T) {
	loader := NewRoutingConfigLoader()
	tr := NewTaskRouter(loader)
	tr.RegisterPlan(RoutingPlan{Name: "p1", Queue: "q1", Strategy: StrategyRoundRobin, Enabled: true})
	if len(tr.ListPlans()) != 1 {
		t.Fatal("expected 1 plan")
	}
	loader.RemovePlan("p1")
	tr.Reload()
	if len(tr.ListPlans()) != 0 {
		t.Fatal("expected 0 plans after reload")
	}
}

func TestTaskRouterTopicMatch(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{
		Name:     "topic-plan",
		Topic:    "events.*",
		Strategy: StrategyRandom,
		Enabled:  true,
	})
	tr := NewTaskRouter(loader)
	candidates := testCandidates()
	msg := Message{Topic: "events.user.created"}
	result, err := tr.SelectConsumers(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected consumers")
	}
}

func TestTaskRouterTaskNameMatch(t *testing.T) {
	loader := NewRoutingConfigLoader()
	loader.AddPlan(RoutingPlan{
		Name:     "task-plan",
		Queue:    "tasks.ml-inference",
		Strategy: StrategyCapabilityBased,
		Enabled:  true,
	})
	tr := NewTaskRouter(loader)
	candidates := testCandidates()
	msg := Message{TaskName: "ml-inference"}
	result, err := tr.SelectConsumers(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected consumers")
	}
}

// ---------------------------------------------------------------------------
// Router integration tests
// ---------------------------------------------------------------------------

func TestRouterStrategyBasedRoute(t *testing.T) {
	router := NewRouter()
	router.Add(Route{
		Name:     "round-robin-route",
		Mode:     RouteRoundRobin,
		To:       []Destination{{Kind: DestinationQueue, Name: "q1"}},
		Enabled:  true,
		Strategy: StrategyRoundRobin,
	})
	candidates := testCandidates()
	msg := testMessage("t", "k")
	result, matched, err := router.RouteWithStrategy(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(matched) == 0 {
		t.Fatal("expected matched route")
	}
	if len(result) == 0 {
		t.Fatal("expected consumers")
	}
}

func TestRouterRouteWithStrategyNoMatch(t *testing.T) {
	router := NewRouter()
	candidates := testCandidates()
	msg := testMessage("t", "k")
	result, matched, err := router.RouteWithStrategy(context.Background(), msg, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(matched) != 0 {
		t.Fatal("expected no matched routes")
	}
	if len(result) != len(candidates) {
		t.Fatal("expected all candidates returned")
	}
}

func TestRouterLoadRoutingConfig(t *testing.T) {
	router := NewRouter()
	cfg := RoutingConfig{
		DefaultStrategy: StrategyRoundRobin,
		Plans: []RoutingPlan{
			{Name: "p1", Queue: "q1", Strategy: StrategyLeastLoaded, Enabled: true},
		},
	}
	data, _ := json.Marshal(cfg)
	err := router.LoadRoutingConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	plan, ok := router.ConfigLoader().GetPlanForQueue("q1")
	if !ok {
		t.Fatal("expected plan")
	}
	if plan.Strategy != StrategyLeastLoaded {
		t.Fatalf("expected least_loaded, got %s", plan.Strategy)
	}
}

func TestRouterAddRemovePlan(t *testing.T) {
	router := NewRouter()
	router.AddPlan(RoutingPlan{Name: "test", Queue: "q-test", Strategy: StrategyRandom, Enabled: true})
	plan, ok := router.ConfigLoader().GetPlanForQueue("q-test")
	if !ok {
		t.Fatal("expected plan")
	}
	if plan.Name != "test" {
		t.Fatalf("expected test, got %s", plan.Name)
	}
	router.RemovePlan("test")
	_, ok = router.ConfigLoader().GetPlanForQueue("q-test")
	if ok {
		t.Fatal("expected plan to be removed")
	}
}

// ---------------------------------------------------------------------------
// RouteMode to Strategy mapping
// ---------------------------------------------------------------------------

func TestRouteModeToStrategy(t *testing.T) {
	tests := []struct {
		mode     RouteMode
		expected StrategyType
	}{
		{RouteRoundRobin, StrategyRoundRobin},
		{RouteWeighted, StrategyWeightedRoundRobin},
		{RouteLeastLoaded, StrategyLeastLoaded},
		{RouteRandom, StrategyRandom},
		{RouteConsistentHash, StrategyConsistentHash},
		{RouteCapabilityBased, StrategyCapabilityBased},
		{RouteAdaptive, StrategyAdaptiveLoadBalance},
		{RouteSticky, StrategyStickySession},
		{RouteP2C, StrategyPowerOfTwoChoices},
		{RouteSharded, StrategySharded},
		{RouteFanout, StrategyFanOut},
		{RouteDirect, StrategyRoundRobin},
		{RouteTopic, StrategyRoundRobin},
	}
	for _, tt := range tests {
		got := routeModeToStrategy(tt.mode)
		if got != tt.expected {
			t.Errorf("routeModeToStrategy(%q) = %q, want %q", tt.mode, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Thread safety tests
// ---------------------------------------------------------------------------

func TestRoundRobinConcurrency(t *testing.T) {
	s := NewRoundRobinStrategy()
	candidates := testCandidates()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Select(context.Background(), testMessage("t", "k"), candidates)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestWeightedRoundRobinConcurrency(t *testing.T) {
	s := NewWeightedRoundRobinStrategy()
	candidates := testCandidates()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Select(context.Background(), testMessage("t", "k"), candidates)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestAdaptiveLoadBalanceConcurrency(t *testing.T) {
	s := NewAdaptiveLoadBalanceStrategy()
	candidates := testCandidates()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Select(context.Background(), testMessage("t", "k"), candidates)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestStickySessionConcurrency(t *testing.T) {
	s := NewStickySessionStrategy("")
	candidates := testCandidates()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Select(context.Background(), testMessage("t", "key-concurrent"), candidates)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestAll14StrategiesWithTenantFiltering(t *testing.T) {
	strategies := []StrategyType{
		StrategyRoundRobin,
		StrategyWeightedRoundRobin,
		StrategyLeastLoaded,
		StrategyRandom,
		StrategyConsistentHash,
		StrategyCapabilityBased,
		StrategyPriorityAware,
		StrategyAdaptiveLoadBalance,
		StrategyStickySession,
		StrategyFanOut,
		StrategyRuleBased,
		StrategyPowerOfTwoChoices,
		StrategySharded,
		StrategyHealthBased,
	}

	for _, st := range strategies {
		t.Run(string(st), func(t *testing.T) {
			router := NewRouter()
			router.Add(Route{
				Name:     "test-route-" + string(st),
				Match:    "orders.*",
				Strategy: st,
				Enabled:  true,
			})

			candidates := []ConsumerCandidate{
				{ConsumerID: "c-tenant-a-1", TenantID: "tenant-a", Weight: 10, MaxInFlight: 10, Healthy: true, Capabilities: []string{"cap1"}},
				{ConsumerID: "c-tenant-a-2", TenantID: "tenant-a", Weight: 20, MaxInFlight: 10, Healthy: true, Capabilities: []string{"cap1"}},
				{ConsumerID: "c-tenant-b-1", TenantID: "tenant-b", Weight: 100, MaxInFlight: 10, Healthy: true, Capabilities: []string{"cap1"}},
				{ConsumerID: "c-tenant-b-2", TenantID: "tenant-b", Weight: 200, MaxInFlight: 10, Healthy: true, Capabilities: []string{"cap1"}},
			}

			principal := AdminPrincipal{Name: "user-a", Tenants: []string{"tenant-a"}}
			ctx := context.WithValue(context.Background(), adminPrincipalKey{}, principal)

			msg := Message{
				ID:       "msg-a-1",
				Topic:    "orders.created",
				TenantID: "tenant-a",
				Key:      "order-123",
			}

			selected, _, err := router.RouteWithStrategy(ctx, msg, candidates)
			if err != nil {
				t.Fatalf("strategy %s error: %v", st, err)
			}
			if len(selected) == 0 {
				t.Fatalf("strategy %s returned no candidates", st)
			}

			for _, c := range selected {
				if c.TenantID == "tenant-b" {
					t.Fatalf("strategy %s leaked candidate from tenant-b: %s", st, c.ConsumerID)
				}
				if c.TenantID != "tenant-a" {
					t.Fatalf("strategy %s selected invalid tenant candidate: %+v", st, c)
				}
			}
		})
	}
}

