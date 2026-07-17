package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8091", "BST1 address")
	mode := flag.String("mode", "publish", "publish|consume")
	stream := flag.String("stream", "bench", "stream")
	group := flag.String("group", "bench-group", "consumer group")
	partitions := flag.Int("partitions", 8, "partitions")
	concurrency := flag.Int("c", 8, "connections/workers")
	batch := flag.Int("batch", 500, "records per produce request")
	fetch := flag.Int("fetch", 2000, "records per fetch")
	payloadSize := flag.Int("payload", 256, "payload bytes")
	messages := flag.Int64("messages", 1000000, "exact records")
	flag.Parse()

	if *partitions <= 0 || *concurrency <= 0 || *messages <= 0 {
		fmt.Fprintln(os.Stderr, "positive partitions, concurrency, messages required")
		os.Exit(2)
	}
	payload := make([]byte, *payloadSize)
	_, _ = rand.Read(payload)

	start := time.Now()
	var done, errs atomic.Int64
	var wg sync.WaitGroup
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, 100000)
	recordLatency := func(d time.Duration) {
		latMu.Lock()
		latencies = append(latencies, d)
		latMu.Unlock()
	}

	switch *mode {
	case "publish":
		var claimed atomic.Int64
		for worker := 0; worker < *concurrency; worker++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				client, err := broker.DialNativeStream(*addr, 5*time.Second)
				if err != nil {
					errs.Add(1)
					return
				}
				defer client.Close()
				partition := id % *partitions
				for {
					base := claimed.Add(int64(*batch)) - int64(*batch)
					if base >= *messages {
						return
					}
					n := *batch
					if remaining := *messages - base; int64(n) > remaining {
						n = int(remaining)
					}
					records := make([][]byte, n)
					for i := range records {
						records[i] = payload
					}
					t0 := time.Now()
					_, err = client.Produce(*stream, partition, records)
					recordLatency(time.Since(t0))
					if err != nil {
						errs.Add(1)
						continue
					}
					done.Add(int64(n))
					partition = (partition + *concurrency) % *partitions
				}
			}(worker)
		}
	case "consume":
		for worker := 0; worker < *concurrency; worker++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				client, err := broker.DialNativeStream(*addr, 5*time.Second)
				if err != nil {
					errs.Add(1)
					return
				}
				defer client.Close()
				for partition := id; partition < *partitions; partition += *concurrency {
					offset, err := client.Offset(*stream, *group, partition)
					if err != nil {
						errs.Add(1)
						continue
					}
					for {
						t0 := time.Now()
						_, count, next, err := client.FetchRaw(*stream, partition, offset, *fetch, 16<<20)
						recordLatency(time.Since(t0))
						if err != nil {
							errs.Add(1)
							break
						}
						if count == 0 {
							break
						}
						offset = next
						done.Add(int64(count))
						if err = client.Commit(*stream, *group, partition, offset); err != nil {
							errs.Add(1)
							break
						}
					}
				}
			}(worker)
		}
	default:
		fmt.Fprintln(os.Stderr, "mode must be publish or consume")
		os.Exit(2)
	}

	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*q)]
	}
	throughput := float64(done.Load()) / elapsed.Seconds()
	fmt.Printf("mode=%s records=%d errors=%d elapsed=%s msg/s=%.1f requests=%d p50=%s p95=%s p99=%s\n", *mode, done.Load(), errs.Load(), elapsed, throughput, len(latencies), percentile(.50), percentile(.95), percentile(.99))
	fmt.Printf("RESULT_JSON={\"mode\":%q,\"records\":%d,\"errors\":%d,\"elapsed_seconds\":%.6f,\"messages_per_second\":%.3f,\"latency_p50_micros\":%.3f,\"latency_p95_micros\":%.3f,\"latency_p99_micros\":%.3f}\n", *mode, done.Load(), errs.Load(), elapsed.Seconds(), throughput, float64(percentile(.5).Microseconds()), float64(percentile(.95).Microseconds()), float64(percentile(.99).Microseconds()))
}
