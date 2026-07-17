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
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

type result struct {
	ok        bool
	dur       time.Duration
	code      int
	msgs      int
	published int
	consumed  int
}

func main() {
	addr := flag.String("addr", "http://localhost:8090", "brokerd base URL")
	token := flag.String("token", "bench-token", "bearer token")
	queue := flag.String("queue", "bench.q", "queue name")
	mode := flag.String("mode", "publish", "publish | pull | roundtrip")
	protocol := flag.String("protocol", "json", "wire protocol: json or frame")
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
	frameCodec := broker.NewFrameCodec(broker.FrameCodecConfig{Type: broker.CodecFWB1, MaxFrameSize: 128 << 20})
	*protocol = strings.ToLower(strings.TrimSpace(*protocol))
	if *protocol != "json" && *protocol != "frame" {
		fmt.Fprintln(os.Stderr, "-protocol must be json or frame")
		os.Exit(2)
	}

	waitForReady(client, *addr, 30*time.Second)

	// Ensure queue exists.
	declareQueue(client, *addr, *token, *queue)

	fmt.Printf("mode=%s protocol=%s addr=%s queue=%s concurrency=%d duration=%s payload=%dB batch=%d pull-limit=%d consumer-id=%s group=%s\n",
		*mode, *protocol, *addr, *queue, *concurrency, *duration, *payloadSize, *batch, *pullLimit, *consumerID, *group)

	var (
		results        []result
		resultsMu      sync.Mutex
		total          int64
		errs           int64
		totalMsgs      int64
		totalPublished int64
		totalConsumed  int64
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
			var published int
			var consumed int
			switch *mode {
			case "publish":
				dur, code, ok, msgs = doPublish(client, frameCodec, *protocol, *addr, *token, *queue, payload, *batch)
				published = msgs
			case "pull":
				dur, code, ok, msgs = doPull(client, frameCodec, *protocol, *addr, *token, *queue, *pullLimit, workerID, *group)
				consumed = msgs
			case "roundtrip":
				pdur, pcode, pok, pmsgs := doPublish(client, frameCodec, *protocol, *addr, *token, *queue, payload, *batch)
				published = pmsgs
				if !pok {
					dur, code, ok, msgs = pdur, pcode, pok, pmsgs
				} else {
					qdur, qcode, qok, qmsgs := doPull(client, frameCodec, *protocol, *addr, *token, *queue, *pullLimit, workerID, *group)
					dur, code, ok, msgs = pdur+qdur, qcode, qok, pmsgs+qmsgs
					consumed = qmsgs
				}
			}
			atomic.AddInt64(&total, 1)
			atomic.AddInt64(&totalMsgs, int64(msgs))
			atomic.AddInt64(&totalPublished, int64(published))
			atomic.AddInt64(&totalConsumed, int64(consumed))
			if !ok {
				atomic.AddInt64(&errs, 1)
			}
			resultsMu.Lock()
			results = append(results, result{ok: ok, dur: dur, code: code, msgs: msgs, published: published, consumed: consumed})
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

	report(results, total, errs, totalMsgs, totalPublished, totalConsumed, elapsed)
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
func doPublish(client *http.Client, codec broker.FrameCodec, protocol, addr, token, queue string, payload []byte, batch int) (time.Duration, int, bool, int) {
	if protocol == "frame" {
		return doPublishFrame(client, codec, addr, token, queue, payload, batch)
	}
	return doPublishJSON(client, addr, token, queue, payload, batch)
}

func doPublishJSON(client *http.Client, addr, token, queue string, payload []byte, batch int) (time.Duration, int, bool, int) {
	if batch <= 1 {
		body, _ := json.Marshal(map[string]any{
			"topic":   "bench",
			"queue":   queue,
			"payload": json.RawMessage(payload),
		})
		req, _ := http.NewRequest(http.MethodPost, addr+"/publish", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "return=minimal")
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
	req.Header.Set("Prefer", "return=minimal")
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
	if resp.StatusCode == http.StatusNoContent {
		return dur, resp.StatusCode, true, batch
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
func doPull(client *http.Client, codec broker.FrameCodec, protocol, addr, token, queue string, limit int, consumerID, group string) (time.Duration, int, bool, int) {
	if protocol == "frame" {
		return doPullFrame(client, codec, addr, token, queue, limit, consumerID, group)
	}
	return doPullJSON(client, addr, token, queue, limit, consumerID, group)
}

func doPullJSON(client *http.Client, addr, token, queue string, limit int, consumerID, group string) (time.Duration, int, bool, int) {
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
	ackReq.Header.Set("Prefer", "return=minimal")
	ackResp, err := client.Do(ackReq)
	dur := time.Since(start)
	if err != nil {
		return dur, resp.StatusCode, false, 0
	}
	ackData, _ := io.ReadAll(ackResp.Body)
	ackResp.Body.Close()
	if ackResp.StatusCode == http.StatusNoContent {
		return dur, ackResp.StatusCode, true, len(msgs)
	}
	var ackOut struct {
		Acked int `json:"acked"`
	}
	_ = json.Unmarshal(ackData, &ackOut)
	return dur, ackResp.StatusCode, ackOut.Acked == len(msgs), ackOut.Acked
}

func doPublishFrame(client *http.Client, codec broker.FrameCodec, addr, token, queue string, payload []byte, batch int) (time.Duration, int, bool, int) {
	if batch <= 0 {
		batch = 1
	}
	messages := make([]broker.Message, batch)
	for i := range messages {
		messages[i] = broker.Message{Topic: "bench", Queue: queue, Payload: payload, ContentType: "application/octet-stream"}
	}
	body, err := broker.EncodeMessageFrameBatch(nil, codec, messages)
	if err != nil {
		return 0, 0, false, 0
	}
	req, _ := http.NewRequest(http.MethodPost, addr+"/v1/publish/frame/batch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/vnd.pool.broker.frames")
	req.Header.Set("Prefer", "return=minimal")
	start := time.Now()
	resp, err := client.Do(req)
	dur := time.Since(start)
	if err != nil {
		return dur, 0, false, 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !ok {
		return dur, resp.StatusCode, false, 0
	}
	return dur, resp.StatusCode, true, batch
}

func doPullFrame(client *http.Client, codec broker.FrameCodec, addr, token, queue string, limit int, consumerID, group string) (time.Duration, int, bool, int) {
	query := url.Values{}
	query.Set("queue", queue)
	query.Set("consumer_id", consumerID)
	query.Set("consumer_group", group)
	query.Set("limit", strconv.Itoa(limit))
	req, _ := http.NewRequest(http.MethodGet, addr+"/v1/poll/frame?"+query.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
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
	messages, err := broker.DecodeMessageFrameBatch(data, codec, limit)
	if err != nil {
		return time.Since(start), resp.StatusCode, false, 0
	}
	if len(messages) == 0 {
		return time.Since(start), resp.StatusCode, true, 0
	}
	acks := make([]broker.AckRequest, len(messages))
	for i := range messages {
		acks[i] = broker.AckRequest{MessageID: messages[i].ID, Queue: queue, LeaseToken: messages[i].LeaseToken, ConsumerID: consumerID}
	}
	ackBody := broker.EncodeAckFrameBatch(nil, acks)
	ackReq, _ := http.NewRequest(http.MethodPost, addr+"/v1/ack/frame", bytes.NewReader(ackBody))
	ackReq.Header.Set("Authorization", "Bearer "+token)
	ackReq.Header.Set("Content-Type", "application/vnd.pool.broker.acks")
	ackReq.Header.Set("Prefer", "return=minimal")
	ackResp, err := client.Do(ackReq)
	dur := time.Since(start)
	if err != nil {
		return dur, 0, false, 0
	}
	io.Copy(io.Discard, ackResp.Body)
	ackResp.Body.Close()
	ok := ackResp.StatusCode >= 200 && ackResp.StatusCode < 300
	if !ok {
		return dur, ackResp.StatusCode, false, 0
	}
	return dur, ackResp.StatusCode, true, len(messages)
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

func report(results []result, total, errs, totalMsgs, totalPublished, totalConsumed int64, elapsed time.Duration) {
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
	fmt.Printf("msg/s combined : %.1f\n", mps)
	fmt.Printf("publish msg/s  : %.1f\n", float64(totalPublished)/elapsed.Seconds())
	fmt.Printf("consume msg/s  : %.1f\n", float64(totalConsumed)/elapsed.Seconds())
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
