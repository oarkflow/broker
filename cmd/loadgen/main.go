// loadgen drives sustained HTTP load against a running brokerd instance and
// reports req/s, msg/s, and latency percentiles for publish and pull+ack
// cycles. Supports batching (-batch) to amortize per-request HTTP/JSON
// overhead across many messages, matching how a real client would push
// throughput rather than issuing one message per round trip.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	ok   bool
	dur  time.Duration
	code int
	msgs int
}

func main() {
	addr := flag.String("addr", "http://localhost:8090", "brokerd base URL")
	token := flag.String("token", "bench-token", "bearer token")
	queue := flag.String("queue", "bench.q", "queue name")
	mode := flag.String("mode", "publish", "publish | pull | roundtrip")
	concurrency := flag.Int("c", 50, "concurrent workers")
	duration := flag.Duration("duration", 15*time.Second, "test duration")
	payloadSize := flag.Int("payload", 256, "payload size in bytes")
	pullLimit := flag.Int("pull-limit", 32, "messages per pull")
	batch := flag.Int("batch", 1, "messages per publish HTTP call (uses /publish/batch when >1)")
	consumerID := flag.String("consumer-id", "", "consumer id; defaults to hostname (unique per container)")
	group := flag.String("group", "loadgen-group", "consumer group for pull/roundtrip mode")
	flag.Parse()

	if *consumerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "loadgen-" + strconv.Itoa(os.Getpid())
		}
		*consumerID = host
	}

	client := &http.Client{Timeout: 10 * time.Second}
	client.Transport = &http.Transport{MaxIdleConnsPerHost: 256}
	payload := randPayload(*payloadSize)

	waitForReady(client, *addr, 30*time.Second)

	// Ensure queue exists.
	declareQueue(client, *addr, *token, *queue)

	fmt.Printf("mode=%s addr=%s queue=%s concurrency=%d duration=%s payload=%dB batch=%d pull-limit=%d consumer-id=%s group=%s\n",
		*mode, *addr, *queue, *concurrency, *duration, *payloadSize, *batch, *pullLimit, *consumerID, *group)

	var (
		results   []result
		resultsMu sync.Mutex
		total     int64
		errs      int64
		totalMsgs int64
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	worker := func(workerID string) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var dur time.Duration
			var code int
			var ok bool
			var msgs int
			switch *mode {
			case "publish":
				dur, code, ok, msgs = doPublish(client, *addr, *token, *queue, payload, *batch)
			case "pull":
				dur, code, ok, msgs = doPull(client, *addr, *token, *queue, *pullLimit, workerID, *group)
			case "roundtrip":
				pdur, pcode, pok, pmsgs := doPublish(client, *addr, *token, *queue, payload, *batch)
				if !pok {
					dur, code, ok, msgs = pdur, pcode, pok, pmsgs
				} else {
					qdur, qcode, qok, qmsgs := doPull(client, *addr, *token, *queue, *pullLimit, workerID, *group)
					dur, code, ok, msgs = pdur+qdur, qcode, qok, pmsgs+qmsgs
				}
			}
			atomic.AddInt64(&total, 1)
			atomic.AddInt64(&totalMsgs, int64(msgs))
			if !ok {
				atomic.AddInt64(&errs, 1)
			}
			resultsMu.Lock()
			results = append(results, result{ok: ok, dur: dur, code: code, msgs: msgs})
			resultsMu.Unlock()
		}
	}

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(*consumerID + "-" + strconv.Itoa(i))
	}
	time.AfterFunc(*duration, func() { close(stop) })
	wg.Wait()
	elapsed := time.Since(start)

	report(results, total, errs, totalMsgs, elapsed)
}

func waitForReady(client *http.Client, addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(addr + "/readyz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "warning: broker did not report ready within timeout, proceeding anyway")
}

func declareQueue(client *http.Client, addr, token, queue string) {
	body, _ := json.Marshal(map[string]any{"name": queue})
	req, _ := http.NewRequest(http.MethodPost, addr+"/queues", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "declare queue:", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// doPublish sends either a single message via /publish or, when batch > 1, a
// slice of messages via /publish/batch — one HTTP round trip either way.
func doPublish(client *http.Client, addr, token, queue string, payload []byte, batch int) (time.Duration, int, bool, int) {
	if batch <= 1 {
		body, _ := json.Marshal(map[string]any{
			"topic":   "bench",
			"queue":   queue,
			"payload": json.RawMessage(payload),
		})
		req, _ := http.NewRequest(http.MethodPost, addr+"/publish", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := client.Do(req)
		dur := time.Since(start)
		if err != nil {
			return dur, 0, false, 0
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		ok := resp.StatusCode >= 200 && resp.StatusCode < 300
		msgs := 0
		if ok {
			msgs = 1
		}
		return dur, resp.StatusCode, ok, msgs
	}

	items := make([]map[string]any, batch)
	for i := range items {
		items[i] = map[string]any{
			"topic":   "bench",
			"queue":   queue,
			"payload": json.RawMessage(payload),
		}
	}
	body, _ := json.Marshal(items)
	req, _ := http.NewRequest(http.MethodPost, addr+"/publish/batch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := client.Do(req)
	dur := time.Since(start)
	if err != nil {
		return dur, 0, false, 0
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dur, resp.StatusCode, false, 0
	}
	var out struct {
		Results []json.RawMessage `json:"results"`
		Errors  []string          `json:"errors"`
	}
	msgs := 0
	if err := json.Unmarshal(data, &out); err == nil {
		for _, e := range out.Errors {
			if e == "" {
				msgs++
			}
		}
	}
	return dur, resp.StatusCode, msgs > 0, msgs
}

// doPull leases up to limit messages in one HTTP call, then acks all of them
// in a single /ack/batch call. Ack failures are counted against msgs so a
// worker that leases but can't durably complete messages shows up as errors
// rather than silently inflating throughput.
func doPull(client *http.Client, addr, token, queue string, limit int, consumerID, group string) (time.Duration, int, bool, int) {
	body, _ := json.Marshal(map[string]any{
		"consumer_id":    consumerID,
		"consumer_group": group,
		"limit":          limit,
	})
	req, _ := http.NewRequest(http.MethodPost, addr+"/queues/"+queue+"/pull", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return time.Since(start), 0, false, 0
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return time.Since(start), resp.StatusCode, false, 0
	}
	var msgs []struct {
		ID         string `json:"id"`
		LeaseToken string `json:"lease_token"`
	}
	if err := json.Unmarshal(data, &msgs); err != nil || len(msgs) == 0 {
		return time.Since(start), resp.StatusCode, true, 0 // empty queue is not an error
	}

	acks := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		acks[i] = map[string]any{
			"message_id":  m.ID,
			"queue":       queue,
			"lease_token": m.LeaseToken,
			"consumer_id": consumerID,
		}
	}
	ackBody, _ := json.Marshal(acks)
	ackReq, _ := http.NewRequest(http.MethodPost, addr+"/ack/batch", bytes.NewReader(ackBody))
	ackReq.Header.Set("Authorization", "Bearer "+token)
	ackReq.Header.Set("Content-Type", "application/json")
	ackResp, err := client.Do(ackReq)
	dur := time.Since(start)
	if err != nil {
		return dur, resp.StatusCode, false, 0
	}
	ackData, _ := io.ReadAll(ackResp.Body)
	ackResp.Body.Close()
	var ackOut struct {
		Acked int `json:"acked"`
	}
	_ = json.Unmarshal(ackData, &ackOut)
	return dur, ackResp.StatusCode, ackOut.Acked == len(msgs), ackOut.Acked
}

func randPayload(n int) []byte {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range buf {
		buf[i] = letters[rng.Intn(len(letters))]
	}
	return []byte(`"` + string(buf) + `"`)
}

func report(results []result, total, errs, totalMsgs int64, elapsed time.Duration) {
	durs := make([]time.Duration, 0, len(results))
	codeCounts := map[int]int{}
	for _, r := range results {
		durs = append(durs, r.dur)
		codeCounts[r.code]++
	}
	slices.Sort(durs)

	pct := func(p float64) time.Duration {
		if len(durs) == 0 {
			return 0
		}
		idx := int(float64(len(durs)) * p)
		if idx >= len(durs) {
			idx = len(durs) - 1
		}
		return durs[idx]
	}

	rps := float64(total) / elapsed.Seconds()
	mps := float64(totalMsgs) / elapsed.Seconds()

	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("total requests : %d\n", total)
	fmt.Printf("total messages : %d\n", totalMsgs)
	fmt.Printf("errors         : %d (%.2f%%)\n", errs, 100*float64(errs)/float64(max64(total, 1)))
	fmt.Printf("elapsed        : %s\n", elapsed)
	fmt.Printf("req/s          : %.1f\n", rps)
	fmt.Printf("msg/s          : %.1f\n", mps)
	if len(durs) > 0 {
		fmt.Printf("latency p50    : %s\n", pct(0.50))
		fmt.Printf("latency p95    : %s\n", pct(0.95))
		fmt.Printf("latency p99    : %s\n", pct(0.99))
		fmt.Printf("latency max    : %s\n", durs[len(durs)-1])
	}
	fmt.Printf("status codes   : %v\n", codeCounts)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
