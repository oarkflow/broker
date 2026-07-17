// loadgen drives sustained HTTP load against a running brokerd instance and
// reports req/s, msg/s, and latency percentiles for publish and pull+ack
// cycles. Supports batching (-batch) to amortize per-request HTTP/JSON
// overhead across many messages, matching how a real client would push
// throughput rather than issuing one message per round trip.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
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
	transport := flag.String("transport", "http", "http (JSON+HTTP, -addr) | tcp (length-prefixed JSON over a persistent TCP connection, -tcp-addr)")
	tcpAddr := flag.String("tcp-addr", "localhost:50051", "broker TCP transport address, used when -transport=tcp")
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

	fmt.Printf("mode=%s transport=%s addr=%s queue=%s concurrency=%d duration=%s payload=%dB batch=%d pull-limit=%d consumer-id=%s group=%s\n",
		*mode, *transport, *addr, *queue, *concurrency, *duration, *payloadSize, *batch, *pullLimit, *consumerID, *group)

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
		var gc *grpcClient
		if *transport == "tcp" {
			var err error
			gc, err = dialGRPC(*tcpAddr, *token)
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial tcp transport:", err)
				return
			}
			defer gc.close()
		}
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
				if gc != nil {
					dur, code, ok, msgs = doPublishTCP(gc, *queue, payload, *batch)
				} else {
					dur, code, ok, msgs = doPublish(client, *addr, *token, *queue, payload, *batch)
				}
			case "pull":
				if gc != nil {
					dur, code, ok, msgs = doPullTCP(gc, *queue, *pullLimit, workerID)
				} else {
					dur, code, ok, msgs = doPull(client, *addr, *token, *queue, *pullLimit, workerID, *group)
				}
			case "roundtrip":
				var pdur time.Duration
				var pcode int
				var pok bool
				var pmsgs int
				if gc != nil {
					pdur, pcode, pok, pmsgs = doPublishTCP(gc, *queue, payload, *batch)
				} else {
					pdur, pcode, pok, pmsgs = doPublish(client, *addr, *token, *queue, payload, *batch)
				}
				if !pok {
					dur, code, ok, msgs = pdur, pcode, pok, pmsgs
				} else {
					var qdur time.Duration
					var qcode int
					var qok bool
					var qmsgs int
					if gc != nil {
						qdur, qcode, qok, qmsgs = doPullTCP(gc, *queue, *pullLimit, workerID)
					} else {
						qdur, qcode, qok, qmsgs = doPull(client, *addr, *token, *queue, *pullLimit, workerID, *group)
					}
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

// grpcClient speaks the broker's length-prefixed-JSON/TCP transport
// (broker.GRPCServer): a persistent connection carrying one request/response
// pair at a time, avoiding per-request HTTP framing and connection-setup
// overhead. Not safe for concurrent use — each loadgen worker gets its own.
type grpcClient struct {
	conn  net.Conn
	token string
	enc   *json.Encoder
	dec   *json.Decoder
	seq   int64
}

type grpcWireRequest struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Body    json.RawMessage   `json:"body"`
	Headers map[string]string `json:"headers,omitempty"`
}

type grpcWireResponse struct {
	ID     string          `json:"id"`
	Error  string          `json:"error,omitempty"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
}

func dialGRPC(addr, token string) (*grpcClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	// Buffer reads explicitly: json.Decoder reading straight off a net.Conn
	// issues one syscall per read-ahead chunk with no coalescing, unlike
	// Go's http.Transport which buffers internally.
	r := bufio.NewReaderSize(conn, 64<<10)
	return &grpcClient{conn: conn, token: token, enc: json.NewEncoder(conn), dec: json.NewDecoder(r)}, nil
}

func (c *grpcClient) call(method string, body any) (grpcWireResponse, error) {
	c.seq++
	payload, _ := json.Marshal(body)
	req := grpcWireRequest{ID: strconv.FormatInt(c.seq, 10), Method: method, Body: payload}
	if c.token != "" {
		req.Headers = map[string]string{"Authorization": "Bearer " + c.token}
	}
	if err := c.enc.Encode(req); err != nil {
		return grpcWireResponse{}, err
	}
	var resp grpcWireResponse
	if err := c.dec.Decode(&resp); err != nil {
		return grpcWireResponse{}, err
	}
	return resp, nil
}

func (c *grpcClient) close() { c.conn.Close() }

func doPublishTCP(gc *grpcClient, queue string, payload []byte, batch int) (time.Duration, int, bool, int) {
	// The TCP transport has no batch-publish method; -batch is ignored here
	// (kept naive/single-message, matching the "smart without manual
	// tuning" default) and each call publishes one message.
	_ = batch
	start := time.Now()
	resp, err := gc.call("broker.Publish", map[string]any{
		"topic":   "bench",
		"queue":   queue,
		"payload": json.RawMessage(payload),
	})
	dur := time.Since(start)
	if err != nil {
		return dur, 0, false, 0
	}
	ok := resp.Status >= 200 && resp.Status < 300
	msgs := 0
	if ok {
		msgs = 1
	}
	return dur, resp.Status, ok, msgs
}

func doPullTCP(gc *grpcClient, queue string, limit int, consumerID string) (time.Duration, int, bool, int) {
	start := time.Now()
	resp, err := gc.call("broker.Poll", map[string]any{
		"queue":    queue,
		"limit":    limit,
		"consumer": consumerID,
	})
	if err != nil {
		return time.Since(start), 0, false, 0
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return time.Since(start), resp.Status, false, 0
	}
	var msgs []struct {
		ID         string `json:"id"`
		LeaseToken string `json:"lease_token"`
	}
	if err := json.Unmarshal(resp.Body, &msgs); err != nil || len(msgs) == 0 {
		return time.Since(start), resp.Status, true, 0 // empty queue is not an error
	}
	acked := 0
	for _, m := range msgs {
		ackResp, err := gc.call("broker.Ack", map[string]any{
			"message_id":  m.ID,
			"queue":       queue,
			"lease_token": m.LeaseToken,
			"consumer_id": consumerID,
		})
		if err == nil && ackResp.Status >= 200 && ackResp.Status < 300 {
			acked++
		}
	}
	dur := time.Since(start)
	return dur, resp.Status, acked == len(msgs), acked
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
