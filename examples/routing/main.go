package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	b, err := broker.New(broker.Config{
		Name:              "routing-demo",
		Mode:              broker.ModeEmbedded,
		DefaultQueue:      "default",
		DefaultVisibility: 30 * time.Second,
		EnableTaskTracking: true,
		EnableNotifications: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	fmt.Println("=== Fastworker Broker — Routing Strategy Demo ===")
	fmt.Println()

	demoRoundRobin(b)
	demoWeightedRoundRobin(b)
	demoLeastLoaded(b)
	demoRandom(b)
	demoConsistentHash(b)
	demoCapabilityBased(b)
	demoPriorityAware(b)
	demoAdaptiveLoadBalance(b)
	demoStickySession(b)
	demoFanOut(b)
	demoPowerOfTwoChoices(b)
	demoSharded(b)
	demoRouteLevelStrategy(b)
	demoBCLRoutingConfig(b)

	fmt.Println()
	fmt.Println("=== All demos complete ===")
	fmt.Println("Press Ctrl+C to exit.")
	<-ctx.Done()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type handlerResult struct {
	Consumer string
	Message  string
	Count    int
}

func makeHandler(name string, counter *atomic.Int64, mu *sync.Mutex, results map[string]*int64) broker.Handler {
	return func(ctx context.Context, d broker.Delivery) error {
		var msg map[string]any
		_ = d.Decode(&msg)
		counter.Add(1)
		mu.Lock()
		c := results[name]
		if c == nil {
			v := int64(0)
			c = &v
			results[name] = c
		}
		*c++
		mu.Unlock()
		payload, _ := json.Marshal(msg)
		fmt.Printf("  [%s] received: %s\n", name, string(payload))
		return nil
	}
}

func publishMsg(b *broker.Broker, topic string, payload any, opts ...broker.PublishOption) {
	pub := b.Publisher()
	_, err := pub.Publish(context.Background(), topic, payload, opts...)
	if err != nil {
		fmt.Printf("  publish error: %v\n", err)
	}
}

func printCounter(name string, counter *atomic.Int64) {
	fmt.Printf("  -> %s processed %d messages\n", name, counter.Load())
}

func printResults(results map[string]*int64) {
	fmt.Printf("  -> Distribution: ")
	parts := make([]string, 0, len(results))
	for k, v := range results {
		parts = append(parts, fmt.Sprintf("%s=%d", k, *v))
	}
	fmt.Println(strings.Join(parts, ", "))
}

// ---------------------------------------------------------------------------
// 1. Round Robin
// ---------------------------------------------------------------------------

func demoRoundRobin(b *broker.Broker) {
	fmt.Println("--- 1. Round Robin ---")
	fmt.Println("Messages distributed evenly: c1 -> c2 -> c3 -> c1 -> ...")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.roundrobin"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for i, name := range []string{"rr-worker-1", "rr-worker-2", "rr-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "rr-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       5,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyRoundRobin,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
		_ = i
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 6; i++ {
		_, err := pub.Publish(context.Background(), "demo.rr", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 2. Weighted Round Robin
// ---------------------------------------------------------------------------

func demoWeightedRoundRobin(b *broker.Broker) {
	fmt.Println("--- 2. Weighted Round Robin ---")
	fmt.Println("Messages distributed by weight: w1=1, w2=2, w3=3")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.weighted"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	configs := []struct {
		name   string
		weight int
	}{
		{"wrr-worker-1", 1},
		{"wrr-worker-2", 2},
		{"wrr-worker-3", 3},
	}

	for _, cfg := range configs {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              cfg.name,
			Group:             "wrr-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyWeightedRoundRobin,
			StrategyConfig: &broker.StrategyConfig{
				Type:   broker.StrategyWeightedRoundRobin,
				Weight: cfg.weight,
			},
		}, makeHandler(cfg.name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 12; i++ {
		_, err := pub.Publish(context.Background(), "demo.wrr", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 3. Least Loaded
// ---------------------------------------------------------------------------

func demoLeastLoaded(b *broker.Broker) {
	fmt.Println("--- 3. Least Loaded ---")
	fmt.Println("Messages go to consumer with most available capacity")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.leastloaded"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	configs := []struct {
		name       string
		maxInFlight int
	}{
		{"ll-worker-1", 5},
		{"ll-worker-2", 20},
		{"ll-worker-3", 10},
	}

	for _, cfg := range configs {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              cfg.name,
			Group:             "ll-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       cfg.maxInFlight,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyLeastLoaded,
		}, makeHandler(cfg.name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 6; i++ {
		_, err := pub.Publish(context.Background(), "demo.ll", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 4. Random
// ---------------------------------------------------------------------------

func demoRandom(b *broker.Broker) {
	fmt.Println("--- 4. Random ---")
	fmt.Println("Messages distributed randomly across consumers")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.random"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"rnd-worker-1", "rnd-worker-2", "rnd-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "rnd-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyRandom,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 9; i++ {
		_, err := pub.Publish(context.Background(), "demo.rnd", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 5. Consistent Hash
// ---------------------------------------------------------------------------

func demoConsistentHash(b *broker.Broker) {
	fmt.Println("--- 5. Consistent Hash ---")
	fmt.Println("Same key always routes to same consumer")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.chash"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"ch-worker-1", "ch-worker-2", "ch-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "ch-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyConsistentHash,
			StrategyConfig: &broker.StrategyConfig{
				Type:    broker.StrategyConsistentHash,
				HashKey: "key",
			},
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	// Same key should go to same consumer
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.ch", map[string]any{"i": i, "key": "user-42"}, broker.WithQueue(queue), broker.WithKey("user-42"))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}
	// Different key may go to different consumer
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.ch", map[string]any{"i": i, "key": "user-99"}, broker.WithQueue(queue), broker.WithKey("user-99"))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 6. Capability Based
// ---------------------------------------------------------------------------

func demoCapabilityBased(b *broker.Broker) {
	fmt.Println("--- 6. Capability Based ---")
	fmt.Println("Messages routed to consumers with matching capabilities")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.capability"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	type capConfig struct {
		name   string
		caps   []string
	}
	configs := []capConfig{
		{"cap-worker-ml", []string{"ml", "gpu"}},
		{"cap-worker-img", []string{"image", "resize"}},
		{"cap-worker-text", []string{"text", "nlp"}},
	}

	for _, cfg := range configs {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              cfg.name,
			Group:             "cap-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyCapabilityBased,
			StrategyConfig: &broker.StrategyConfig{
				Type:         broker.StrategyCapabilityBased,
				Capabilities: cfg.caps,
			},
		}, makeHandler(cfg.name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for _, task := range []string{"image.resize", "ml.inference", "text.summarize"} {
		_, err := pub.Publish(context.Background(), "demo.cap", map[string]any{"task": task},
			broker.WithQueue(queue),
			broker.WithHeader("x-task-name", task),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 7. Priority Aware
// ---------------------------------------------------------------------------

func demoPriorityAware(b *broker.Broker) {
	fmt.Println("--- 7. Priority Aware ---")
	fmt.Println("High-priority messages go to least-loaded consumer")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.priority"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"pa-worker-1", "pa-worker-2", "pa-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "pa-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyPriorityAware,
			StrategyConfig: &broker.StrategyConfig{
				Type:              broker.StrategyPriorityAware,
				PriorityThreshold: 5,
			},
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	// High priority messages
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.pa", map[string]any{"i": i, "level": "high"},
			broker.WithQueue(queue), broker.WithPriority(8),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}
	// Low priority messages
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.pa", map[string]any{"i": i, "level": "low"},
			broker.WithQueue(queue), broker.WithPriority(2),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 8. Adaptive Load Balance
// ---------------------------------------------------------------------------

func demoAdaptiveLoadBalance(b *broker.Broker) {
	fmt.Println("--- 8. Adaptive Load Balance ---")
	fmt.Println("Dynamic weight adjustment based on latency and throughput")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.adaptive"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"alb-worker-1", "alb-worker-2", "alb-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "alb-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyAdaptiveLoadBalance,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 6; i++ {
		_, err := pub.Publish(context.Background(), "demo.alb", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 9. Sticky Session
// ---------------------------------------------------------------------------

func demoStickySession(b *broker.Broker) {
	fmt.Println("--- 9. Sticky Session ---")
	fmt.Println("Same session key always routes to same consumer")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.sticky"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"ss-worker-1", "ss-worker-2", "ss-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "ss-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyStickySession,
			StrategyConfig: &broker.StrategyConfig{
				Type:      broker.StrategyStickySession,
				StickyKey: "session_id",
			},
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	// All messages with same session_id should go to same consumer
	for i := 0; i < 4; i++ {
		_, err := pub.Publish(context.Background(), "demo.ss", map[string]any{"i": i, "session_id": "sess-abc"},
			broker.WithQueue(queue), broker.WithKey("sess-abc"),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}
	// Different session
	for i := 0; i < 2; i++ {
		_, err := pub.Publish(context.Background(), "demo.ss", map[string]any{"i": i, "session_id": "sess-xyz"},
			broker.WithQueue(queue), broker.WithKey("sess-xyz"),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 10. Fan Out
// ---------------------------------------------------------------------------

func demoFanOut(b *broker.Broker) {
	fmt.Println("--- 10. Fan Out ---")
	fmt.Println("Messages broadcast to all healthy consumers")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.fanout"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"fo-worker-1", "fo-worker-2", "fo-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "fo-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyFanOut,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.fo", map[string]any{"i": i, "broadcast": true}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 11. Power of Two Choices
// ---------------------------------------------------------------------------

func demoPowerOfTwoChoices(b *broker.Broker) {
	fmt.Println("--- 11. Power of Two Choices ---")
	fmt.Println("Pick 2 random candidates, select less loaded")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.p2c"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"p2c-worker-1", "p2c-worker-2", "p2c-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "p2c-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyPowerOfTwoChoices,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 9; i++ {
		_, err := pub.Publish(context.Background(), "demo.p2c", map[string]any{"i": i}, broker.WithQueue(queue))
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 12. Sharded
// ---------------------------------------------------------------------------

func demoSharded(b *broker.Broker) {
	fmt.Println("--- 12. Sharded ---")
	fmt.Println("Deterministic shard-based routing by key")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	queue := "demo.sharded"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{
		Name:              queue,
		VisibilityTimeout: 10 * time.Second,
	})

	for _, name := range []string{"sh-worker-1", "sh-worker-2", "sh-worker-3"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "sh-group",
			Queue:             queue,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategySharded,
			StrategyConfig: &broker.StrategyConfig{
				Type:    broker.StrategySharded,
				HashKey: "region",
			},
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	regions := []string{"us-east", "eu-west", "ap-south", "us-east", "eu-west", "ap-south"}
	for i, region := range regions {
		_, err := pub.Publish(context.Background(), "demo.sh", map[string]any{"i": i, "region": region},
			broker.WithQueue(queue),
			broker.WithHeader("region", region),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 13. Route-Level Strategy
// ---------------------------------------------------------------------------

func demoRouteLevelStrategy(b *broker.Broker) {
	fmt.Println("--- 13. Route-Level Strategy ---")
	fmt.Println("Strategy configured on routes, not consumers")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	qHigh := "demo.route.high"
	qLow := "demo.route.low"
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{Name: qHigh, VisibilityTimeout: 10 * time.Second})
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{Name: qLow, VisibilityTimeout: 10 * time.Second})

	b.AddRoute(broker.Route{
		Name:     "high-pri-route",
		Mode:     broker.RoutePriority,
		Match:    "demo.route.*",
		PriorityMin: 5,
		To:       []broker.Destination{{Kind: broker.DestinationQueue, Name: qHigh}},
		Enabled:  true,
		Strategy: broker.StrategyLeastLoaded,
	})
	b.AddRoute(broker.Route{
		Name:     "low-pri-route",
		Mode:     broker.RouteTopic,
		Match:    "demo.route.*",
		To:       []broker.Destination{{Kind: broker.DestinationQueue, Name: qLow}},
		Enabled:  true,
	})

	for _, name := range []string{"rl-worker-high-1", "rl-worker-high-2"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "rl-high",
			Queue:             qHigh,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyLeastLoaded,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	for _, name := range []string{"rl-worker-low-1"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "rl-low",
			Queue:             qLow,
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	// High priority -> routes to qHigh
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.route.event", map[string]any{"i": i, "prio": "high"},
			broker.WithPriority(8),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}
	// Low priority -> routes to qLow
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.route.event", map[string]any{"i": i, "prio": "low"},
			broker.WithPriority(2),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
	fmt.Println()
}

// ---------------------------------------------------------------------------
// 14. BCL Routing Config
// ---------------------------------------------------------------------------

func demoBCLRoutingConfig(b *broker.Broker) {
	fmt.Println("--- 14. BCL Routing Config ---")
	fmt.Println("Load routing rules from JSON config")

	var mu sync.Mutex
	results := map[string]*int64{}
	counter := &atomic.Int64{}

	cfgJSON := `{
		"default_strategy": "round_robin",
		"plans": [
			{
				"name": "ml-plan",
				"queue": "demo.bcl.ml",
				"strategy": "least_loaded",
				"enabled": true
			},
			{
				"name": "text-plan",
				"queue": "demo.bcl.text",
				"strategy": "random",
				"enabled": true
			}
		]
	}`

	err := b.LoadRoutingConfig([]byte(cfgJSON))
	if err != nil {
		fmt.Printf("  config load error: %v\n", err)
		return
	}

	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{Name: "demo.bcl.ml", VisibilityTimeout: 10 * time.Second})
	_ = b.DeclareQueue(context.Background(), broker.QueueConfig{Name: "demo.bcl.text", VisibilityTimeout: 10 * time.Second})

	for _, name := range []string{"bcl-ml-1", "bcl-ml-2"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "bcl-ml",
			Queue:             "demo.bcl.ml",
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyLeastLoaded,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	for _, name := range []string{"bcl-text-1"} {
		sub, err := b.Subscribe(context.Background(), broker.ConsumerConfig{
			Name:              name,
			Group:             "bcl-text",
			Queue:             "demo.bcl.text",
			Concurrency:       1,
			MaxInFlight:       10,
			VisibilityTimeout: 10 * time.Second,
			Strategy:          broker.StrategyRandom,
		}, makeHandler(name, counter, &mu, results))
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	pub := b.Publisher()
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.bcl", map[string]any{"i": i, "type": "ml"},
			broker.WithQueue("demo.bcl.ml"),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), "demo.bcl", map[string]any{"i": i, "type": "text"},
			broker.WithQueue("demo.bcl.text"),
		)
		if err != nil {
			fmt.Printf("  publish error: %v\n", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	printResults(results)
}
