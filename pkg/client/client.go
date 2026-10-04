package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker"
	fw "github.com/oarkflow/broker/pkg/pool"
)

type Config struct {
	BrokerURL, Token, ConsumerID, Group, Name, Version string
	BrokerURLs                                         []string
	Concurrency, MaxInFlight, PollLimit                int
	HeartbeatInterval, VisibilityTimeout, PollWait     time.Duration
	DisableAutoExtendLeases                            bool
	LeaseExtensionInterval                             time.Duration
	MaxLeaseExtensions                                 int
	HTTPClient                                         *http.Client
	Pool                                               *fw.Pool
	Hooks                                              Hooks
	FrameCodec                                         broker.FrameCodec
	// TLSCertFile is the path to the client TLS certificate for mTLS.
	TLSCertFile string
	// TLSKeyFile is the path to the client TLS private key for mTLS.
	TLSKeyFile string
	// TLSCACertFile is the path to the CA certificate to verify the server.
	TLSCACertFile string
	// InsecureSkipVerify skips server certificate verification (dev only).
	InsecureSkipVerify bool
}
type Hooks struct {
	OnConnecting, OnConnected, OnAuthenticated, OnRegistered, OnSubscribed, OnPollStart, OnHeartbeat, OnReconnect, OnDrain, OnStopped func(Event)
	OnNotification                                                                                                                    func(NotificationEvent)
	OnMessageReceived, AfterHandle, OnHandlerError, OnAck, OnNack, OnReject, OnProgress, OnLeaseExtended                              func(MessageEvent)
	BeforeHandle                                                                                                                      func(MessageEvent) error
}
type Event struct {
	ConsumerID, Group string
	State             broker.ConsumerState
	Error             error
	Time              time.Time
}
type MessageEvent struct {
	Message broker.Message
	Queue   string
	Error   error
	Time    time.Time
}
type NotificationEvent struct {
	Notification broker.Notification
	Time         time.Time
}
type Client struct {
	cfg    Config
	http   *http.Client
	mu     sync.RWMutex
	urls   []string
	active int
}

func New(cfg Config) *Client {
	if cfg.BrokerURL == "" && len(cfg.BrokerURLs) == 0 {
		cfg.BrokerURL = "http://127.0.0.1:8090"
	}
	urls := normalizeBrokerURLs(cfg.BrokerURL, cfg.BrokerURLs)
	if len(urls) == 0 {
		urls = []string{"http://127.0.0.1:8090"}
	}
	cfg.BrokerURL = urls[0]
	cfg.BrokerURLs = urls
	if cfg.ConsumerID == "" {
		cfg.ConsumerID = hostnameID()
	}
	if cfg.Group == "" {
		cfg.Group = cfg.ConsumerID
	}
	if cfg.Name == "" {
		cfg.Name = cfg.ConsumerID
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = cfg.Concurrency
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 30 * time.Second
	}
	if cfg.PollWait <= 0 {
		cfg.PollWait = defaultPollWait
	}
	if cfg.PollLimit <= 0 {
		cfg.PollLimit = 1
	}
	if cfg.FrameCodec == nil {
		cfg.FrameCodec = broker.NewBinaryMessageCodec()
	}
	if cfg.LeaseExtensionInterval <= 0 {
		cfg.LeaseExtensionInterval = cfg.VisibilityTimeout / 2
		if cfg.LeaseExtensionInterval <= 0 {
			cfg.LeaseExtensionInterval = 10 * time.Second
		}
	}
	// Build the HTTP client only after PollWait has a value, so its timeout can
	// exceed the long-poll window.
	//
	// The client used to be constructed first with a hardcoded 30s timeout, which
	// then made the "PollWait + headroom" timeout below unreachable dead code.
	// Since PollWait also defaulted to 30s, every long poll raced the client's own
	// deadline: the server kept the request open for the remainder of its window
	// after the client had already given up, and the worker loop slept and retried.
	// Each attempt therefore left an orphaned handler behind.
	if cfg.HTTPClient == nil {
		transport := tunedTransport()
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				panic(fmt.Sprintf("client: load TLS keypair: %v", err))
			}
			tlsCfg := &tls.Config{
				Certificates:       []tls.Certificate{cert},
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: cfg.InsecureSkipVerify,
			}
			if cfg.TLSCACertFile != "" {
				pem, err := os.ReadFile(cfg.TLSCACertFile)
				if err != nil {
					panic(fmt.Sprintf("client: load CA cert: %v", err))
				}
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(pem) {
					panic("client: no valid certs in CA file")
				}
				tlsCfg.RootCAs = pool
			}
			t := tunedTransport().(*http.Transport)
			t.TLSClientConfig = tlsCfg
			transport = t
		} else if cfg.InsecureSkipVerify {
			t := tunedTransport().(*http.Transport)
			t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
			transport = t
		}
		cfg.HTTPClient = &http.Client{Transport: transport, Timeout: cfg.PollWait + pollTimeoutHeadroom}
	} else if cfg.HTTPClient.Timeout > 0 && cfg.HTTPClient.Timeout <= cfg.PollWait {
		// A caller-supplied client is respected, but a timeout at or below the
		// long-poll window guarantees the race above, so raise it rather than
		// failing every poll at the deadline.
		cfg.HTTPClient.Timeout = cfg.PollWait + pollTimeoutHeadroom
	}
	c := cfg.HTTPClient
	return &Client{cfg: cfg, http: c, urls: urls}
}

// remoteError turns a data-plane error response into an error that behaves like
// the equivalent in-process one.
//
// The server used to answer every failure with http.Error(w, err.Error(), 409),
// so the client could only build a string. That made errors.Is(err,
// broker.ErrNotLeader), errors.Is(err, broker.ErrLeaseLost) and
// broker.ClassifyRetry all unusable for any remote caller, even though the broker
// went to some length to embed the leader address in the message text. The server
// now writes a typed envelope; a server that still answers with plain text falls
// back to the previous string form.
func remoteError(status int, body []byte) error {
	if remote := broker.ParseRemoteError(status, body); remote != nil {
		return remote
	}
	return fmt.Errorf("broker %s: %s", http.StatusText(status), strings.TrimSpace(string(body)))
}

// isRetryableStatus reports whether a status is worth retrying against the next
// broker URL. A typed envelope tells us authoritatively: a not-leader or
// stale-epoch response should be retried elsewhere, whereas a genuine 5xx should
// not be replayed against a different broker.
func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

// defaultPollWait is how long the client asks the broker to hold a poll open.
const defaultPollWait = 30 * time.Second

// pollTimeoutHeadroom is added to PollWait for the HTTP client deadline, so the
// client's timeout only fires after the server would have answered on its own.
const pollTimeoutHeadroom = 10 * time.Second

func (c *Client) Config() Config { return c.cfg }
func (c *Client) Register(ctx context.Context, reg broker.ConsumerRegistration) (broker.ConsumerInfo, error) {
	if reg.ConsumerID == "" {
		reg.ConsumerID = c.cfg.ConsumerID
	}
	if reg.Group == "" {
		reg.Group = c.cfg.Group
	}
	if reg.Name == "" {
		reg.Name = c.cfg.Name
	}
	if reg.Concurrency <= 0 {
		reg.Concurrency = c.cfg.Concurrency
	}
	if reg.MaxInFlight <= 0 {
		reg.MaxInFlight = c.cfg.MaxInFlight
	}
	if reg.HeartbeatEveryNS <= 0 {
		reg.HeartbeatEveryNS = c.cfg.HeartbeatInterval.Nanoseconds()
	}
	if reg.VisibilityTimeout <= 0 {
		reg.VisibilityTimeout = c.cfg.VisibilityTimeout.Nanoseconds()
	}
	var out broker.ConsumerInfo
	err := c.doJSON(ctx, http.MethodPost, "/v1/consumers/register", reg, &out)
	if err == nil && c.cfg.Hooks.OnRegistered != nil {
		c.cfg.Hooks.OnRegistered(Event{ConsumerID: out.ConsumerID, Group: out.Group, State: out.State, Time: time.Now()})
	}
	return out, err
}
func (c *Client) Heartbeat(ctx context.Context) (broker.ConsumerInfo, error) {
	var out broker.ConsumerInfo
	err := c.doJSON(ctx, http.MethodPost, "/v1/consumers/"+c.cfg.ConsumerID+"/heartbeat", nil, &out)
	if err == nil && c.cfg.Hooks.OnHeartbeat != nil {
		c.cfg.Hooks.OnHeartbeat(Event{ConsumerID: out.ConsumerID, Group: out.Group, State: out.State, Time: time.Now()})
	}
	return out, err
}
func (c *Client) Subscribe(ctx context.Context, sub broker.RemoteSubscription) (broker.ConsumerInfo, error) {
	if !sub.AutoCreate {
		sub.AutoCreate = true
	}
	var out broker.ConsumerInfo
	err := c.doJSON(ctx, http.MethodPost, "/v1/consumers/"+c.cfg.ConsumerID+"/subscribe", sub, &out)
	if err == nil && c.cfg.Hooks.OnSubscribed != nil {
		c.cfg.Hooks.OnSubscribed(Event{ConsumerID: out.ConsumerID, Group: out.Group, State: out.State, Time: time.Now()})
	}
	return out, err
}
func (c *Client) Poll(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	if limit <= 0 {
		limit = c.cfg.PollLimit
	}
	req := broker.PollRequest{Queue: queue, ConsumerID: c.cfg.ConsumerID, ConsumerGroup: c.cfg.Group, Limit: limit, WaitNS: c.cfg.PollWait.Nanoseconds(), VisibilityTimeoutNS: c.cfg.VisibilityTimeout.Nanoseconds()}
	if c.cfg.Hooks.OnPollStart != nil {
		c.cfg.Hooks.OnPollStart(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, State: broker.ConsumerPolling, Time: time.Now()})
	}
	var out []broker.Message
	err := c.doJSON(ctx, http.MethodPost, "/v1/poll", req, &out)
	return out, err
}
func (c *Client) Ack(ctx context.Context, m broker.Message) error {
	body := broker.AckRequest{MessageID: m.ID, Queue: m.Queue, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID}
	err := c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/ack", body, nil)
	if err == nil && c.cfg.Hooks.OnAck != nil {
		c.cfg.Hooks.OnAck(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
	}
	return err
}
func (c *Client) Nack(ctx context.Context, m broker.Message, opts ...NackOption) error {
	o := NackOptions{Action: broker.NackRetry, Delay: time.Second, Reason: "handler_error"}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	body := broker.NackRequest{MessageID: m.ID, Queue: m.Queue, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, Action: o.Action, Reason: o.Reason, Error: o.Error, Delay: o.Delay, DLQ: o.DLQ}
	err := c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/nack", body, nil)
	if err == nil && c.cfg.Hooks.OnNack != nil {
		c.cfg.Hooks.OnNack(MessageEvent{Message: m, Queue: m.Queue, Error: errors.New(o.Error), Time: time.Now()})
	}
	return err
}
func (c *Client) Reject(ctx context.Context, m broker.Message, reason string) error {
	body := broker.NackRequest{MessageID: m.ID, Queue: m.Queue, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, Action: broker.NackReject, Reason: reason, Error: reason}
	err := c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/reject", body, nil)
	if err == nil && c.cfg.Hooks.OnReject != nil {
		c.cfg.Hooks.OnReject(MessageEvent{Message: m, Queue: m.Queue, Error: errors.New(reason), Time: time.Now()})
	}
	return err
}
func (c *Client) Extend(ctx context.Context, m broker.Message, d time.Duration) error {
	body := broker.LeaseRequest{MessageID: m.ID, Queue: m.Queue, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, VisibilityTimeout: d}
	err := c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/extend", body, nil)
	if err == nil && c.cfg.Hooks.OnLeaseExtended != nil {
		c.cfg.Hooks.OnLeaseExtended(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
	}
	return err
}
func (c *Client) Progress(ctx context.Context, m broker.Message, p broker.Progress) error {
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "progress": p}
	err := c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/progress", body, nil)
	if err == nil && c.cfg.Hooks.OnProgress != nil {
		c.cfg.Hooks.OnProgress(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
	}
	return err
}
func (c *Client) Respond(ctx context.Context, m broker.Message, response any, ack bool) error {
	raw, err := json.Marshal(response)
	if err != nil {
		return err
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "payload": json.RawMessage(raw), "ack": ack}
	return c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/respond", body, nil)
}
func (c *Client) RespondError(ctx context.Context, m broker.Message, e error, ack bool) error {
	if e == nil {
		e = errors.New("handler error")
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "error": broker.BrokerError{Code: "handler_error", Message: e.Error(), Retryable: true}, "ack": ack}
	return c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/respond", body, nil)
}

func (c *Client) Complete(ctx context.Context, m broker.Message, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "payload": json.RawMessage(raw), "ack": true}
	return c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/complete", body, nil)
}

func (c *Client) Fail(ctx context.Context, m broker.Message, e error) error {
	if e == nil {
		e = errors.New("handler error")
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "error": broker.BrokerError{Code: "handler_error", Message: e.Error(), Retryable: true}, "ack": true}
	return c.doJSON(ctx, http.MethodPost, "/v1/messages/"+m.ID+"/fail", body, nil)
}

func (c *Client) PublishFrame(ctx context.Context, msg broker.Message) (broker.PublishResult, error) {
	frame, err := c.cfg.FrameCodec.EncodeMessage(nil, msg)
	if err != nil {
		return broker.PublishResult{}, err
	}
	var out broker.PublishResult
	err = c.doRaw(ctx, http.MethodPost, "/v1/publish/frame", "application/vnd.pool.broker.frame", frame, &out)
	return out, err
}

func (c *Client) PollFrames(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	if limit <= 0 {
		limit = c.cfg.PollLimit
	}
	req := broker.PollRequest{Queue: queue, ConsumerID: c.cfg.ConsumerID, ConsumerGroup: c.cfg.Group, Limit: limit, WaitNS: c.cfg.PollWait.Nanoseconds(), VisibilityTimeoutNS: c.cfg.VisibilityTimeout.Nanoseconds()}
	data, err := c.doRawBytes(ctx, http.MethodPost, "/v1/poll/frame", "application/json", mustMarshal(req))
	if err != nil {
		return nil, err
	}
	return decodeFrameBatch(data, c.cfg.FrameCodec)
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func decodeFrameBatch(data []byte, codec broker.FrameCodec) ([]broker.Message, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	var out []broker.Message
	for {
		l, err := binary.ReadUvarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		buf := make([]byte, int(l))
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		var msg broker.Message
		if err := codec.DecodeMessage(buf, &msg); err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
}

// encodePayload prepares a publish payload for the JSON wire format.
//
// A []byte or json.RawMessage is sent verbatim as base64 with an explicit
// payload_encoding, because encoding/json would otherwise turn it into a base64
// JSON string and the broker would store the literal quote-and-base64
// characters. That silently corrupted every binary payload, and it disagreed
// with the embedded path, where a []byte payload is stored untouched. Every
// other value keeps the previous json.Marshal behaviour.
func encodePayload(payload any) (json.RawMessage, string, error) {
	switch v := payload.(type) {
	case json.RawMessage:
		return json.RawMessage(v), "json", nil
	case []byte:
		quoted, err := json.Marshal(base64.StdEncoding.EncodeToString(v))
		if err != nil {
			return nil, "", err
		}
		return json.RawMessage(quoted), "base64", nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	return json.RawMessage(raw), "json", nil
}

// applyTo fills the shared wire struct, so every client publish path carries the
// same fields and cannot drift from the broker's PublishHTTPPayload.
func (o PublishOptions) applyTo(req *broker.PublishHTTPPayload) {
	req.Queue = o.Queue
	req.Stream = o.Stream
	req.Type = o.Type
	req.ContentType = o.ContentType
	req.Headers = o.Headers
	req.Metadata = o.Metadata
	req.Key = o.Key
	req.RoutingKey = o.RoutingKey
	req.Priority = o.Priority
	req.TenantID = o.TenantID
	req.CorrelationID = o.CorrelationID
	req.CausationID = o.CausationID
	req.IdempotencyKey = o.IdempotencyKey
	req.MessageID = o.MessageID
	req.Schema = o.Schema
	req.SchemaVersion = o.SchemaVersion
	req.TraceID = o.TraceID
	req.DeduplicationID = o.DeduplicationID
	req.DeduplicationWindowNS = o.DeduplicationWindow.Nanoseconds()
	req.TTLNS = o.TTL.Nanoseconds()
	req.DelayNS = o.Delay.Nanoseconds()
	req.MaxAttempts = o.MaxAttempts
	req.ProducerID = o.ProducerID
	req.Strategy = string(o.Strategy)
}

func (c *Client) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) (broker.PublishResult, error) {
	var o PublishOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	raw, encoding, err := encodePayload(payload)
	if err != nil {
		return broker.PublishResult{}, err
	}
	req := broker.PublishHTTPPayload{Topic: topic, Payload: raw, PayloadEncoding: encoding}
	o.applyTo(&req)
	var out broker.PublishResult
	err = c.doJSON(ctx, http.MethodPost, "/v1/publish", req, &out)
	return out, err
}

func (c *Client) PublishAsync(ctx context.Context, topic string, payload any, opts ...PublishOption) <-chan AsyncPublishResult {
	ch := make(chan AsyncPublishResult, 1)
	go func() {
		r, err := c.Publish(ctx, topic, payload, opts...)
		ch <- AsyncPublishResult{Result: r, Error: err}
		close(ch)
	}()
	return ch
}

type AsyncPublishResult struct {
	Result broker.PublishResult
	Error  error
}

func (c *Client) PublishBatch(ctx context.Context, topic string, payloads []any, opts ...PublishOption) (broker.BatchPublishResult, error) {
	var o PublishOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	req := struct {
		Messages []broker.PublishHTTPPayload `json:"messages"`
	}{Messages: make([]broker.PublishHTTPPayload, 0, len(payloads))}
	for _, p := range payloads {
		raw, encoding, err := encodePayload(p)
		if err != nil {
			return broker.BatchPublishResult{}, err
		}
		msg := broker.PublishHTTPPayload{Topic: topic, Payload: raw, PayloadEncoding: encoding}
		o.applyTo(&msg)
		req.Messages = append(req.Messages, msg)
	}
	var out broker.BatchPublishResult
	err := c.doJSON(ctx, http.MethodPost, "/v1/publish/batch", req, &out)
	return out, err
}

func (c *Client) Command(ctx context.Context, subject string, payload any, response any, timeout time.Duration) error {
	return c.Request(ctx, subject, payload, response, timeout)
}

type AsyncResponse struct {
	Error error
}

func (c *Client) RequestAsync(ctx context.Context, subject string, payload any, response any, timeout time.Duration) <-chan AsyncResponse {
	ch := make(chan AsyncResponse, 1)
	go func() {
		ch <- AsyncResponse{Error: c.Request(ctx, subject, payload, response, timeout)}
		close(ch)
	}()
	return ch
}
func (c *Client) Request(ctx context.Context, subject string, payload any, response any, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	raw, encoding, err := encodePayload(payload)
	if err != nil {
		return err
	}
	req := broker.PublishHTTPPayload{Topic: subject, Payload: raw, PayloadEncoding: encoding, TimeoutNS: timeout.Nanoseconds()}
	var resp broker.Response
	if err := c.doJSON(ctx, http.MethodPost, "/v1/request", req, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	if response != nil {
		return json.Unmarshal(resp.Payload, response)
	}
	return nil
}
func (c *Client) SubmitTask(ctx context.Context, name string, payload any, opts ...TaskOption) (broker.TaskInfo, error) {
	var o TaskOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	raw, encoding, err := encodePayload(payload)
	if err != nil {
		return broker.TaskInfo{}, err
	}
	req := map[string]any{"name": name, "queue": o.Queue, "payload": raw, "payload_encoding": encoding, "tenant_id": o.TenantID, "priority": o.Priority, "max_attempts": o.MaxAttempts, "metadata": o.Metadata}
	var out broker.TaskInfo
	err = c.doJSON(ctx, http.MethodPost, "/v1/tasks/submit", req, &out)
	return out, err
}
func (c *Client) Task(ctx context.Context, id string) (broker.TaskInfo, error) {
	var out broker.TaskInfo
	err := c.doJSON(ctx, http.MethodGet, "/v1/tasks/"+id, nil, &out)
	return out, err
}
func (c *Client) TaskResult(ctx context.Context, id string) (broker.TaskInfo, []byte, error) {
	var out struct {
		Task    broker.TaskInfo `json:"task"`
		Payload json.RawMessage `json:"payload"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/v1/tasks/"+id+"/result", nil, &out)
	return out.Task, []byte(out.Payload), err
}

func (c *Client) CancelTask(ctx context.Context, id, reason string) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/tasks/"+id+"/cancel", map[string]string{"reason": reason}, nil)
}

func (c *Client) RetryTask(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/tasks/"+id+"/retry", nil, nil)
}

func (c *Client) DLQ(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	path := "/v1/dlq"
	if queue != "" {
		path += "/" + queue + "/messages"
	}
	if limit > 0 {
		path += fmt.Sprintf("?limit=%d", limit)
	}
	var out []broker.Message
	err := c.doJSON(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

func (c *Client) ReplayDLQ(ctx context.Context, queue string) error {
	if queue == "" {
		return errors.New("queue required")
	}
	return c.doJSON(ctx, http.MethodPost, "/v1/dlq/"+queue+"/replay", nil, nil)
}

func (c *Client) ReadStream(ctx context.Context, stream, group string, after uint64, limit int) ([]broker.Message, error) {
	path := fmt.Sprintf("/v1/streams/%s/messages?after_offset=%d", stream, after)
	if group != "" {
		path += "&consumer_group=" + group
	}
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	var out []broker.Message
	err := c.doJSON(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

func (c *Client) CommitStreamOffset(ctx context.Context, stream, group string, offset uint64) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/streams/"+stream+"/commit", broker.OffsetCommit{Stream: stream, ConsumerGroup: group, Offset: offset}, nil)
}

func (c *Client) StreamOffset(ctx context.Context, stream, group string) (uint64, error) {
	var out struct {
		Offset uint64 `json:"offset"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/v1/streams/"+stream+"/offset?consumer_group="+group, nil, &out)
	return out.Offset, err
}
func (c *Client) Consumers(ctx context.Context) ([]broker.ConsumerInfo, error) {
	var out []broker.ConsumerInfo
	err := c.doJSON(ctx, http.MethodGet, "/v1/consumers", nil, &out)
	return out, err
}
func (c *Client) Events(ctx context.Context, after string, wait time.Duration) ([]broker.Notification, error) {
	path := "/v1/consumers/" + c.cfg.ConsumerID + "/events"
	qs := []string{}
	if after != "" {
		qs = append(qs, "after="+after)
	}
	if wait > 0 {
		qs = append(qs, fmt.Sprintf("wait=%d", wait.Nanoseconds()))
	}
	if len(qs) > 0 {
		path += "?" + strings.Join(qs, "&")
	}
	var out []broker.Notification
	err := c.doJSON(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

func (c *Client) doRaw(ctx context.Context, method, path, contentType string, bodyBytes []byte, out any) error {
	data, err := c.doRawBytes(ctx, method, path, contentType, bodyBytes)
	if err != nil {
		return err
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *Client) doRawBytes(ctx context.Context, method, path, contentType string, bodyBytes []byte) ([]byte, error) {
	data, _, err := c.doRawStatus(ctx, method, path, contentType, bodyBytes, "")
	return data, err
}

// doRawStatus is doRawBytes that also reports the HTTP status and can send a
// Prefer header. 2xx responses, including 207 multi-status, return their body.
func (c *Client) doRawStatus(ctx context.Context, method, path, contentType string, bodyBytes []byte, prefer string) ([]byte, int, error) {
	urls := c.orderedURLs()
	var last error
	for i, base := range urls {
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(bodyBytes))
		if err != nil {
			last = err
			continue
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if c.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
		}
		if prefer != "" {
			req.Header.Set("Prefer", prefer)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			last = err
			if i == 0 && c.cfg.Hooks.OnReconnect != nil && len(urls) > 1 {
				c.cfg.Hooks.OnReconnect(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, Error: err, Time: time.Now()})
			}
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 500 && len(urls) > 1 && !isRetryableStatus(resp.StatusCode) {
			last = remoteError(resp.StatusCode, data)
			continue
		}
		c.markActive(base)
		if resp.StatusCode >= 300 {
			return nil, resp.StatusCode, remoteError(resp.StatusCode, data)
		}
		return data, resp.StatusCode, nil
	}
	if last == nil {
		last = errors.New("broker unavailable")
	}
	return nil, 0, last
}
func (c *Client) doJSON(ctx context.Context, method, path string, in, out any) error {
	var bodyBytes []byte
	var err error
	if in != nil {
		bodyBytes, err = json.Marshal(in)
		if err != nil {
			return err
		}
	}
	urls := c.orderedURLs()
	var last error
	for i, base := range urls {
		var body io.Reader
		if bodyBytes != nil {
			body = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
		if err != nil {
			last = err
			continue
		}
		if in != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			last = err
			if i == 0 && c.cfg.Hooks.OnReconnect != nil && len(urls) > 1 {
				c.cfg.Hooks.OnReconnect(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, Error: err, Time: time.Now()})
			}
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 500 && len(urls) > 1 && !isRetryableStatus(resp.StatusCode) {
			last = remoteError(resp.StatusCode, data)
			continue
		}
		c.markActive(base)
		if resp.StatusCode >= 300 {
			return remoteError(resp.StatusCode, data)
		}
		if out != nil && len(data) > 0 {
			return json.Unmarshal(data, out)
		}
		return nil
	}
	if last == nil {
		last = errors.New("broker unavailable")
	}
	return last
}

func (c *Client) orderedURLs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.urls) == 0 {
		return []string{strings.TrimRight(c.cfg.BrokerURL, "/")}
	}
	out := make([]string, 0, len(c.urls))
	for i := 0; i < len(c.urls); i++ {
		out = append(out, c.urls[(c.active+i)%len(c.urls)])
	}
	return out
}

func (c *Client) markActive(base string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, u := range c.urls {
		if u == base {
			c.active = i
			c.cfg.BrokerURL = base
			return
		}
	}
}

func normalizeBrokerURLs(primary string, more []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(v string) {
		v = strings.TrimRight(strings.TrimSpace(v), "/")
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	add(primary)
	for _, v := range more {
		add(v)
	}
	return out
}
func hostnameID() string {
	h, _ := os.Hostname()
	if h == "" {
		h = "worker"
	}
	return h + "-" + fmt.Sprint(time.Now().UnixNano())
}

// PublishOptions mirrors the broker's PublishOptions. It previously carried only
// seven fields, so the client could not reach content_type, schema, ttl, delay,
// trace propagation, deduplication, max_attempts or a producer-chosen message
// id, all of which the embedded API supported.
type PublishOptions struct {
	Queue, Stream, Type, Key, RoutingKey, TenantID, IdempotencyKey string
	Headers, Metadata                                              map[string]string
	Priority                                                       int

	ContentType         string
	Schema              string
	SchemaVersion       string
	MessageID           string
	CorrelationID       string
	CausationID         string
	TraceID             string
	DeduplicationID     string
	DeduplicationWindow time.Duration
	TTL                 time.Duration
	Delay               time.Duration
	MaxAttempts         int
	ProducerID          string
	Strategy            broker.StrategyType
}
type PublishOption func(*PublishOptions)

func WithQueue(v string) PublishOption              { return func(o *PublishOptions) { o.Queue = v } }
func WithStream(v string) PublishOption             { return func(o *PublishOptions) { o.Stream = v } }
func WithType(v string) PublishOption               { return func(o *PublishOptions) { o.Type = v } }
func WithHeaders(v map[string]string) PublishOption { return func(o *PublishOptions) { o.Headers = v } }
func WithMetadata(v map[string]string) PublishOption {
	return func(o *PublishOptions) { o.Metadata = v }
}
func WithRoutingKey(v string) PublishOption { return func(o *PublishOptions) { o.RoutingKey = v } }

func WithContentType(v string) PublishOption { return func(o *PublishOptions) { o.ContentType = v } }
func WithSchema(name, version string) PublishOption {
	return func(o *PublishOptions) { o.Schema = name; o.SchemaVersion = version }
}
func WithMessageID(v string) PublishOption { return func(o *PublishOptions) { o.MessageID = v } }
func WithCorrelationID(v string) PublishOption {
	return func(o *PublishOptions) { o.CorrelationID = v }
}
func WithCausationID(v string) PublishOption { return func(o *PublishOptions) { o.CausationID = v } }
func WithTraceID(v string) PublishOption     { return func(o *PublishOptions) { o.TraceID = v } }
func WithDeduplication(id string, window time.Duration) PublishOption {
	return func(o *PublishOptions) { o.DeduplicationID = id; o.DeduplicationWindow = window }
}
func WithTTL(v time.Duration) PublishOption   { return func(o *PublishOptions) { o.TTL = v } }
func WithDelay(v time.Duration) PublishOption { return func(o *PublishOptions) { o.Delay = v } }
func WithMaxAttempts(v int) PublishOption     { return func(o *PublishOptions) { o.MaxAttempts = v } }
func WithProducerID(v string) PublishOption   { return func(o *PublishOptions) { o.ProducerID = v } }
func WithStrategy(v broker.StrategyType) PublishOption {
	return func(o *PublishOptions) { o.Strategy = v }
}
func WithKey(v string) PublishOption      { return func(o *PublishOptions) { o.Key = v } }
func WithPriority(v int) PublishOption    { return func(o *PublishOptions) { o.Priority = v } }
func WithTenantID(v string) PublishOption { return func(o *PublishOptions) { o.TenantID = v } }
func WithIdempotencyKey(v string) PublishOption {
	return func(o *PublishOptions) { o.IdempotencyKey = v }
}

type TaskOptions struct {
	Queue, TenantID       string
	Priority, MaxAttempts int
	Metadata              map[string]string
}
type TaskOption func(*TaskOptions)

func WithTaskQueue(v string) TaskOption               { return func(o *TaskOptions) { o.Queue = v } }
func WithTaskTenantID(v string) TaskOption            { return func(o *TaskOptions) { o.TenantID = v } }
func WithTaskPriority(v int) TaskOption               { return func(o *TaskOptions) { o.Priority = v } }
func WithTaskMaxAttempts(v int) TaskOption            { return func(o *TaskOptions) { o.MaxAttempts = v } }
func WithTaskMetadata(v map[string]string) TaskOption { return func(o *TaskOptions) { o.Metadata = v } }

type NackOptions struct {
	Action             broker.NackAction
	Delay              time.Duration
	DLQ, Reason, Error string
}
type NackOption func(*NackOptions)

func NackDelay(v time.Duration) NackOption { return func(o *NackOptions) { o.Delay = v } }
func NackDLQ(v string) NackOption {
	return func(o *NackOptions) { o.Action = broker.NackDLQ; o.DLQ = v }
}
func NackReason(v string) NackOption { return func(o *NackOptions) { o.Reason = v } }
func NackError(v string) NackOption  { return func(o *NackOptions) { o.Error = v } }

// tunedTransport is http.DefaultTransport with an idle pool sized for many
// concurrent publishers. The default keeps only 2 idle connections per host, so
// hundreds of concurrent requests constantly close and reopen sockets.
func tunedTransport() http.RoundTripper {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 1024
	t.MaxIdleConnsPerHost = 512
	t.MaxConnsPerHost = 0
	t.IdleConnTimeout = 90 * time.Second
	return t
}
