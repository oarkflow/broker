package client

import (
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
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/cluster"
	fw "github.com/oarkflow/broker/pkg/pool"
)

const defaultMaxResponseBytes int64 = 64 << 20

var ErrResponseTooLarge = errors.New("client: broker response exceeds configured size limit")

type Config struct {
	BrokerURL, Token, ConsumerID, Group, Name, Version string
	BrokerURLs                                         []string
	Concurrency, MaxInFlight, PollLimit                int
	HeartbeatInterval, VisibilityTimeout, PollWait     time.Duration
	DisableAutoExtendLeases                            bool
	LeaseExtensionInterval                             time.Duration
	MaxLeaseExtensions                                 int
	// MaxResponseBytes bounds HTTP response body memory use for JSON and raw calls.
	// A non-positive value uses the 64 MiB default.
	MaxResponseBytes int64
	// HTTPClient is an optional caller-built fh client, used as-is. Its
	// timeout must exceed PollWait or long polls will race it.
	HTTPClient *fh.Client
	// HTTPTimeout bounds each exchange of the client New builds. Values at or
	// below PollWait are raised to PollWait plus headroom.
	HTTPTimeout time.Duration
	Pool        *fw.Pool
	Hooks       Hooks
	FrameCodec  broker.FrameCodec
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
	cfg                   Config
	http                  *fh.Client
	headers               *headerSets
	mu                    sync.RWMutex
	urls                  []string
	active                int
	partitionPollSequence atomic.Uint64
}

func New(cfg Config) *Client {
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
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
	if cfg.HTTPTimeout <= cfg.PollWait {
		cfg.HTTPTimeout = cfg.PollWait + pollTimeoutHeadroom
	}
	if cfg.HTTPClient == nil {
		var tlsCfg *tls.Config
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				panic(fmt.Sprintf("client: load TLS keypair: %v", err))
			}
			tlsCfg = &tls.Config{
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
		} else if cfg.InsecureSkipVerify {
			tlsCfg = &tls.Config{InsecureSkipVerify: true}
		}
		// The idle pool is sized for many concurrent workers sharing one
		// broker; connections per host are effectively unbounded.
		cfg.HTTPClient = broker.NewHTTPClient(broker.HTTPClientConfig{
			Timeout:             cfg.HTTPTimeout,
			TLSConfig:           tlsCfg,
			AllowInsecureTLS:    cfg.InsecureSkipVerify,
			MaxIdleConns:        1024,
			MaxIdleConnsPerHost: 512,
			MaxConnsPerHost:     1 << 16,
		})
	}
	c := cfg.HTTPClient
	return &Client{cfg: cfg, http: c, urls: urls, headers: &headerSets{token: cfg.Token}}
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
	return fmt.Errorf("broker %s: %s", fh.StatusReason(status), strings.TrimSpace(string(body)))
}

// isRetryableStatus is the fallback for servers that return an untyped error
// body. Typed broker envelopes use their Retryable field instead.
func isRetryableStatus(status int) bool {
	return status == fh.StatusTooManyRequests || status >= fh.StatusInternalServerError
}

func shouldFailover(method string, status int, body []byte) bool {
	if remote := broker.ParseRemoteError(status, body); remote != nil {
		// A not-leader response is emitted before this node accepts the
		// operation, so replaying it on the named leader is safe. Other retryable
		// errors can have an uncertain commit point and must be returned to the
		// caller for an idempotency-aware decision.
		return remote.Class == broker.RetryNotLeader || remote.Code == broker.ErrorCode(broker.ErrNotLeader)
	}
	readOnly := method == fh.MethodGET || method == fh.MethodHEAD || method == fh.MethodOPTIONS
	return readOnly && isRetryableStatus(status)
}

// prioritizeLeaderURL moves an advertised leader ahead of remaining configured
// brokers. It never follows an arbitrary address from the response: the leader
// must match a URL the client already trusts.
func prioritizeLeaderURL(urls []string, after int, err error) {
	if after+1 >= len(urls) {
		return
	}
	var remote *broker.RemoteError
	if !errors.As(err, &remote) {
		return
	}
	leader, _, _ := remote.RetryHint()
	if leader == "" {
		return
	}
	leaderHost := leader
	if parsed, parseErr := url.Parse(leader); parseErr == nil && parsed.Host != "" {
		leaderHost = parsed.Host
	}
	for i := after + 1; i < len(urls); i++ {
		if strings.TrimRight(urls[i], "/") == strings.TrimRight(leader, "/") {
			urls[after+1], urls[i] = urls[i], urls[after+1]
			return
		}
		parsed, parseErr := url.Parse(urls[i])
		if parseErr == nil && strings.EqualFold(parsed.Host, leaderHost) {
			urls[after+1], urls[i] = urls[i], urls[after+1]
			return
		}
	}
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
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/consumers/register", reg, &out)
	if err == nil && c.cfg.Hooks.OnRegistered != nil {
		c.cfg.Hooks.OnRegistered(Event{ConsumerID: out.ConsumerID, Group: out.Group, State: out.State, Time: time.Now()})
	}
	return out, err
}
func (c *Client) Heartbeat(ctx context.Context) (broker.ConsumerInfo, error) {
	var out broker.ConsumerInfo
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/consumers/"+c.cfg.ConsumerID+"/heartbeat", nil, &out)
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
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/consumers/"+c.cfg.ConsumerID+"/subscribe", sub, &out)
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
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/poll", req, &out)
	return out, err
}

// PollPartition leases work from one assigned queue partition through its
// current leader. Messages carry partition metadata for subsequent Ack/Nack.
func (c *Client) PollPartition(ctx context.Context, queue string, partition, limit int) ([]broker.Message, error) {
	if queue == "" || strings.ContainsAny(queue, "/\\") || partition < 0 {
		return nil, fmt.Errorf("%w: invalid queue partition", broker.ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = c.cfg.PollLimit
	}
	return c.pollPartition(ctx, queue, partition, limit, c.cfg.PollWait)
}

func (c *Client) pollPartition(ctx context.Context, queue string, partition, limit int, wait time.Duration) ([]broker.Message, error) {
	base, err := c.PartitionResourceLeaderURL(ctx, "queue", queue, partition)
	if err != nil {
		return nil, err
	}
	req := broker.PollRequest{Queue: queue, Partition: partition, Partitioned: true, ConsumerID: c.cfg.ConsumerID, ConsumerGroup: c.cfg.Group, Limit: limit, WaitNS: wait.Nanoseconds(), VisibilityTimeoutNS: c.cfg.VisibilityTimeout.Nanoseconds()}
	if c.cfg.Hooks.OnPollStart != nil {
		c.cfg.Hooks.OnPollStart(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, State: broker.ConsumerPolling, Time: time.Now()})
	}
	var out []broker.Message
	err = c.doJSONAt(ctx, base, fh.MethodPOST, "/v1/poll", req, &out)
	return out, err
}

// PollAssignedQueue polls assigned queue partitions with a bounded aggregate
// lease count. If some partition requests fail, successful leases are returned
// together with the joined errors so callers can still process those messages.
func (c *Client) PollAssignedQueue(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	if queue == "" || strings.ContainsAny(queue, "/\\") {
		return nil, fmt.Errorf("%w: invalid queue", broker.ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = c.cfg.PollLimit
	}
	if limit > maxTopicPageSize {
		limit = maxTopicPageSize
	}
	partitions, err := c.assignedResourcePartitions(ctx, "queue", queue)
	if err != nil {
		return nil, err
	}
	if len(partitions) == 0 {
		return c.Poll(ctx, queue, limit)
	}
	start := int(c.partitionPollSequence.Add(1) % uint64(len(partitions)))
	quotient, remainder := limit/len(partitions), limit%len(partitions)
	type result struct {
		partition int
		messages  []broker.Message
		err       error
	}
	semaphore := make(chan struct{}, 16)
	var messages []broker.Message
	var failures []error
	pollRound := func(indexes []int, budgets []int, requestWait time.Duration) []result {
		results := make([]result, len(indexes))
		var wait sync.WaitGroup
		for i, index := range indexes {
			partition, budget := partitions[index], budgets[i]
			results[i].partition = partition
			wait.Add(1)
			go func(resultIndex, partition, budget int) {
				defer wait.Done()
				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					results[resultIndex].err = ctx.Err()
					return
				}
				defer func() { <-semaphore }()
				results[resultIndex].messages, results[resultIndex].err = c.pollPartition(ctx, queue, partition, budget, requestWait)
			}(i, partition, budget)
		}
		wait.Wait()
		return results
	}
	active := make([]int, 0, len(partitions))
	quotas := make([]int, 0, len(partitions))
	for i := range partitions {
		budget := quotient
		if (i-start+len(partitions))%len(partitions) < remainder {
			budget++
		}
		if budget > 0 {
			active = append(active, i)
			quotas = append(quotas, budget)
		}
	}
	results := pollRound(active, quotas, c.cfg.PollWait)
	remaining := limit
	for _, result := range results {
		messages = append(messages, result.messages...)
		remaining -= len(result.messages)
		if result.err != nil {
			failures = append(failures, fmt.Errorf("queue %s partition %d: %w", queue, result.partition, result.err))
			continue
		}
	}
	// Reassign unused lease capacity to partitions that filled their initial
	// share. Retries are non-blocking so sparse partitions do not multiply the
	// configured poll wait, and every round requests at most the remaining total.
	activePartitions := make([]int, 0, len(active))
	for i, result := range results {
		if result.err == nil && len(result.messages) == quotas[i] && quotas[i] > 0 {
			activePartitions = append(activePartitions, result.partition)
		}
	}
	round := 0
	for remaining > 0 && len(activePartitions) > 0 {
		roundStart := (start + round) % len(activePartitions)
		per, extra := remaining/len(activePartitions), remaining%len(activePartitions)
		indexes := make([]int, 0, len(activePartitions))
		budgets := make([]int, 0, len(activePartitions))
		for i, partition := range activePartitions {
			budget := per
			if (i-roundStart+len(activePartitions))%len(activePartitions) < extra {
				budget++
			}
			if budget > 0 {
				indexes = append(indexes, sort.SearchInts(partitions, partition))
				budgets = append(budgets, budget)
			}
		}
		nextActive := make([]int, 0, len(activePartitions))
		more := pollRound(indexes, budgets, 0)
		for i, result := range more {
			messages = append(messages, result.messages...)
			remaining -= len(result.messages)
			if result.err != nil {
				failures = append(failures, fmt.Errorf("queue %s partition %d: %w", queue, result.partition, result.err))
				continue
			}
			if len(result.messages) == budgets[i] && budgets[i] > 0 {
				nextActive = append(nextActive, result.partition)
			}
		}
		activePartitions = nextActive
		round++
	}
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
			if messages[i].Partition == messages[j].Partition {
				return messages[i].ID < messages[j].ID
			}
			return messages[i].Partition < messages[j].Partition
		}
		return messages[i].CreatedAt.Before(messages[j].CreatedAt)
	})
	return messages, errors.Join(failures...)
}

func (c *Client) assignedResourcePartitions(ctx context.Context, kind, name string) ([]int, error) {
	var assignments []struct {
		ID struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Kind      string `json:"kind"`
		} `json:"id"`
	}
	if err := c.doJSON(ctx, fh.MethodGET, "/cluster/partitions", nil, &assignments); err != nil {
		return nil, err
	}
	partitionSet := make(map[int]struct{})
	for _, assignment := range assignments {
		if assignment.ID.Topic == name && assignment.ID.Kind == kind {
			if assignment.ID.Partition < 0 {
				return nil, fmt.Errorf("%w: invalid %s partition assignment for %q", broker.ErrInvalidMessage, kind, name)
			}
			partitionSet[assignment.ID.Partition] = struct{}{}
		}
	}
	partitions := make([]int, 0, len(partitionSet))
	for partition := range partitionSet {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	if len(partitions) > maxTopicPagePartitions {
		return nil, fmt.Errorf("%w: %s %q has %d partitions; client limit is %d", broker.ErrUnsupportedMode, kind, name, len(partitions), maxTopicPagePartitions)
	}
	return partitions, nil
}

// GetClusterTransaction returns one quorum-confirmed controller transaction
// record by ID, including its participant votes, durable decision, and
// resolution progress.
func (c *Client) GetClusterTransaction(ctx context.Context, id string) (*cluster.CoordinatorTransaction, error) {
	if id == "" || len(id) > cluster.MaxTransactionIDBytes {
		return nil, fmt.Errorf("%w: invalid transaction ID", broker.ErrInvalidMessage)
	}
	var transaction cluster.CoordinatorTransaction
	path := "/cluster/transactions?id=" + url.QueryEscape(id)
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &transaction); err != nil {
		return nil, err
	}
	return &transaction, nil
}

// ListClusterTransactionsPage returns one stable, bounded controller
// transaction page. Pass nextCursor as after to continue the scan.
func (c *Client) ListClusterTransactionsPage(ctx context.Context, after string, limit int) ([]*cluster.CoordinatorTransaction, string, error) {
	if len(after) > cluster.MaxTransactionIDBytes {
		return nil, "", fmt.Errorf("%w: invalid transaction cursor", broker.ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = cluster.DefaultTransactionPageSize
	}
	if limit > cluster.MaxTransactionPageSize {
		limit = cluster.MaxTransactionPageSize
	}
	query := url.Values{}
	query.Set("after", after)
	query.Set("limit", strconv.Itoa(limit))
	var page struct {
		Transactions []*cluster.CoordinatorTransaction `json:"transactions"`
		NextCursor   string                            `json:"next_cursor"`
	}
	if err := c.doJSON(ctx, fh.MethodGET, "/cluster/transactions?"+query.Encode(), nil, &page); err != nil {
		return nil, "", err
	}
	return page.Transactions, page.NextCursor, nil
}

// PruneClusterTransactions removes a bounded number of fully resolved
// coordinator records older than before. Unresolved transaction work is never
// eligible for pruning.
func (c *Client) PruneClusterTransactions(ctx context.Context, before time.Time, limit int) (int, error) {
	if before.IsZero() {
		return 0, fmt.Errorf("%w: transaction prune cutoff is required", broker.ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > cluster.MaxTransactionPruneBatch {
		limit = cluster.MaxTransactionPruneBatch
	}
	var result struct {
		Pruned int `json:"pruned"`
	}
	if err := c.doJSON(ctx, fh.MethodPOST, "/cluster/transactions/prune", map[string]any{
		"before": before.UTC(), "limit": limit,
	}, &result); err != nil {
		return 0, err
	}
	return result.Pruned, nil
}

func (c *Client) ListPartitionQueue(ctx context.Context, queue string, partition, limit int) ([]broker.Message, error) {
	if queue == "" || strings.ContainsAny(queue, "/\\") || partition < 0 {
		return nil, fmt.Errorf("%w: invalid queue partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "queue", queue, partition)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/queues/%s/partitions/%d/messages", url.PathEscape(queue), partition)
	if limit > 0 {
		path += fmt.Sprintf("?limit=%d", limit)
	}
	var out []broker.Message
	err = c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &out)
	return out, err
}

// ListAssignedQueue returns a bounded merged view across assigned queue
// partitions. Each partition receives a share of the limit; results are sorted
// by creation time, but this is not a cursor-based global FIFO page. Successful
// messages are returned alongside any per-partition error.
func (c *Client) ListAssignedQueue(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	report, err := c.ListAssignedQueueDetailed(ctx, queue, limit)
	if report == nil {
		return nil, err
	}
	return report.Messages, err
}

// ListAssignedQueueDetailed returns the bounded server-side queue sample with
// the partitions queried, successes, and partial failures.
func (c *Client) ListAssignedQueueDetailed(ctx context.Context, queue string, limit int) (*broker.AssignedQueueMessages, error) {
	if queue == "" || strings.ContainsAny(queue, "/\\") {
		return nil, fmt.Errorf("%w: invalid queue", broker.ErrInvalidMessage)
	}
	if limit <= 0 {
		limit = 100
	} else if limit > maxTopicPageSize {
		limit = maxTopicPageSize
	}
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	var report broker.AssignedQueueMessages
	path := "/queues/" + url.PathEscape(queue) + "/assigned/messages?" + query.Encode()
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &report); err != nil {
		return nil, err
	}
	if report.Queue != queue || len(report.Partitions) == 0 || len(report.Partitions) > maxTopicPagePartitions || len(report.Messages) > limit {
		return nil, fmt.Errorf("%w: server returned an invalid assigned queue listing", broker.ErrInvalidMessage)
	}
	partitionSet := make(map[int]struct{}, len(report.Partitions))
	lastPartition := -1
	for _, partition := range report.Partitions {
		if partition < 0 || partition <= lastPartition {
			return nil, fmt.Errorf("%w: server returned invalid queue partition ordering", broker.ErrInvalidMessage)
		}
		partitionSet[partition] = struct{}{}
		lastPartition = partition
	}
	completed := make(map[int]struct{}, len(report.Completed))
	for _, partition := range report.Completed {
		if _, ok := partitionSet[partition]; !ok {
			return nil, fmt.Errorf("%w: server reported completion for an unassigned queue partition", broker.ErrInvalidMessage)
		}
		if _, duplicate := completed[partition]; duplicate {
			return nil, fmt.Errorf("%w: server duplicated a completed queue partition", broker.ErrInvalidMessage)
		}
		completed[partition] = struct{}{}
	}
	var failures []error
	for partition, reason := range report.Failures {
		if _, ok := partitionSet[partition]; !ok || reason == "" {
			return nil, fmt.Errorf("%w: server returned an invalid queue partition failure", broker.ErrInvalidMessage)
		}
		if _, ok := completed[partition]; ok {
			return nil, fmt.Errorf("%w: server reported both success and failure for a queue partition", broker.ErrInvalidMessage)
		}
		failures = append(failures, fmt.Errorf("queue %s partition %d: %s", queue, partition, reason))
	}
	seenMessages := make(map[string]struct{}, len(report.Messages))
	for _, message := range report.Messages {
		if message.Queue != queue {
			return nil, fmt.Errorf("%w: server returned a message from another queue", broker.ErrInvalidMessage)
		}
		if _, ok := completed[message.Partition]; !ok {
			return nil, fmt.Errorf("%w: server returned a message from an unsuccessful queue partition", broker.ErrInvalidMessage)
		}
		if _, duplicate := seenMessages[message.ID]; duplicate {
			return nil, fmt.Errorf("%w: server returned a duplicate queue message", broker.ErrInvalidMessage)
		}
		seenMessages[message.ID] = struct{}{}
	}
	return &report, errors.Join(failures...)
}

func (c *Client) PausePartitionQueue(ctx context.Context, queue string, partition int) error {
	return c.partitionQueueControl(ctx, queue, partition, "pause", nil)
}

func (c *Client) ResumePartitionQueue(ctx context.Context, queue string, partition int) error {
	return c.partitionQueueControl(ctx, queue, partition, "resume", nil)
}

func (c *Client) PurgePartitionQueue(ctx context.Context, queue string, partition int) (int, error) {
	var out struct {
		Purged int `json:"purged"`
	}
	err := c.partitionQueueControl(ctx, queue, partition, "purge", &out)
	return out.Purged, err
}

func (c *Client) partitionQueueControl(ctx context.Context, queue string, partition int, action string, out any) error {
	if queue == "" || strings.ContainsAny(queue, "/\\") || partition < 0 {
		return fmt.Errorf("%w: invalid queue partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "queue", queue, partition)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/queues/%s/partitions/%d/%s", url.PathEscape(queue), partition, action)
	return c.doJSONAt(ctx, base, fh.MethodPOST, path, nil, out)
}

// QueuePartitionOperationResult identifies which assigned partitions accepted
// a best-effort whole-queue control operation. Failures can coexist with
// successes because queue partitions commit independently.
type QueuePartitionOperationResult = broker.QueuePartitionOperationResult

func (c *Client) PauseAssignedQueue(ctx context.Context, queue string) (*QueuePartitionOperationResult, error) {
	return c.applyAssignedQueueControl(ctx, queue, "pause")
}

func (c *Client) ResumeAssignedQueue(ctx context.Context, queue string) (*QueuePartitionOperationResult, error) {
	return c.applyAssignedQueueControl(ctx, queue, "resume")
}

func (c *Client) PurgeAssignedQueue(ctx context.Context, queue string) (*QueuePartitionOperationResult, error) {
	return c.applyAssignedQueueControl(ctx, queue, "purge")
}

func (c *Client) applyAssignedQueueControl(ctx context.Context, queue, action string) (*QueuePartitionOperationResult, error) {
	if queue == "" || strings.ContainsAny(queue, "/\\") {
		return nil, fmt.Errorf("%w: invalid queue", broker.ErrInvalidMessage)
	}
	if action != "pause" && action != "resume" && action != "purge" {
		return nil, fmt.Errorf("%w: invalid queue control action", broker.ErrInvalidMessage)
	}
	var result QueuePartitionOperationResult
	path := "/queues/" + url.PathEscape(queue) + "/assigned/" + action
	if err := c.doJSON(ctx, fh.MethodPOST, path, nil, &result); err != nil {
		return nil, err
	}
	if len(result.Partitions) == 0 || len(result.Partitions) > maxTopicPagePartitions {
		return nil, fmt.Errorf("%w: server returned an invalid queue operation report", broker.ErrInvalidMessage)
	}
	partitions := make(map[int]struct{}, len(result.Partitions))
	lastPartition := -1
	for _, partition := range result.Partitions {
		if partition < 0 || partition <= lastPartition {
			return nil, fmt.Errorf("%w: server returned invalid queue partition ordering", broker.ErrInvalidMessage)
		}
		partitions[partition] = struct{}{}
		lastPartition = partition
	}
	completed := make(map[int]struct{}, len(result.Completed))
	for _, partition := range result.Completed {
		if _, ok := partitions[partition]; !ok {
			return nil, fmt.Errorf("%w: server reported completion for an unassigned queue partition", broker.ErrInvalidMessage)
		}
		if _, duplicate := completed[partition]; duplicate {
			return nil, fmt.Errorf("%w: server duplicated a completed queue partition", broker.ErrInvalidMessage)
		}
		completed[partition] = struct{}{}
	}
	var failures []error
	for partition, reason := range result.Failures {
		if _, ok := partitions[partition]; !ok || reason == "" {
			return nil, fmt.Errorf("%w: server returned an invalid queue partition failure", broker.ErrInvalidMessage)
		}
		if _, ok := completed[partition]; ok {
			return nil, fmt.Errorf("%w: server reported both success and failure for a queue partition", broker.ErrInvalidMessage)
		}
		failures = append(failures, fmt.Errorf("queue %s partition %d: %s", queue, partition, reason))
	}
	if len(completed)+len(result.Failures) != len(partitions) {
		return nil, fmt.Errorf("%w: server omitted a queue partition operation result", broker.ErrInvalidMessage)
	}
	if action == "purge" {
		if len(result.Purged) != len(completed) {
			return nil, fmt.Errorf("%w: server returned an incomplete purge count map", broker.ErrInvalidMessage)
		}
		for partition := range result.Purged {
			if _, ok := completed[partition]; !ok {
				return nil, fmt.Errorf("%w: server returned a purge count for an unsuccessful partition", broker.ErrInvalidMessage)
			}
		}
	} else if len(result.Purged) != 0 {
		return nil, fmt.Errorf("%w: server returned purge counts for a non-purge operation", broker.ErrInvalidMessage)
	}
	return &result, errors.Join(failures...)
}

func (c *Client) Ack(ctx context.Context, m broker.Message) error {
	body := broker.AckRequest{MessageID: m.ID, Queue: m.Queue, Partition: m.Partition, Partitioned: m.Partitioned, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID}
	path := "/v1/messages/" + url.PathEscape(m.ID) + "/ack"
	var err error
	if m.Partitioned {
		var base string
		base, err = c.PartitionResourceLeaderURL(ctx, "queue", m.Queue, m.Partition)
		if err == nil {
			err = c.doJSONAt(ctx, base, fh.MethodPOST, path, body, nil)
		}
	} else {
		err = c.doJSON(ctx, fh.MethodPOST, path, body, nil)
	}
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
	body := broker.NackRequest{MessageID: m.ID, Queue: m.Queue, Partition: m.Partition, Partitioned: m.Partitioned, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, Action: o.Action, Reason: o.Reason, Error: o.Error, Delay: o.Delay, DLQ: o.DLQ}
	path := "/v1/messages/" + url.PathEscape(m.ID) + "/nack"
	var err error
	if m.Partitioned {
		var base string
		base, err = c.PartitionResourceLeaderURL(ctx, "queue", m.Queue, m.Partition)
		if err == nil {
			err = c.doJSONAt(ctx, base, fh.MethodPOST, path, body, nil)
		}
	} else {
		err = c.doJSON(ctx, fh.MethodPOST, path, body, nil)
	}
	if err == nil && c.cfg.Hooks.OnNack != nil {
		c.cfg.Hooks.OnNack(MessageEvent{Message: m, Queue: m.Queue, Error: errors.New(o.Error), Time: time.Now()})
	}
	return err
}
func (c *Client) Reject(ctx context.Context, m broker.Message, reason string) error {
	body := broker.NackRequest{MessageID: m.ID, Queue: m.Queue, Partition: m.Partition, Partitioned: m.Partitioned, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, Action: broker.NackReject, Reason: reason, Error: reason}
	path := "/v1/messages/" + url.PathEscape(m.ID) + "/reject"
	var err error
	if m.Partitioned {
		var base string
		base, err = c.PartitionResourceLeaderURL(ctx, "queue", m.Queue, m.Partition)
		if err == nil {
			err = c.doJSONAt(ctx, base, fh.MethodPOST, path, body, nil)
		}
	} else {
		err = c.doJSON(ctx, fh.MethodPOST, path, body, nil)
	}
	if err == nil && c.cfg.Hooks.OnReject != nil {
		c.cfg.Hooks.OnReject(MessageEvent{Message: m, Queue: m.Queue, Error: errors.New(reason), Time: time.Now()})
	}
	return err
}
func (c *Client) Extend(ctx context.Context, m broker.Message, d time.Duration) error {
	body := broker.LeaseRequest{MessageID: m.ID, Queue: m.Queue, Partition: m.Partition, Partitioned: m.Partitioned, LeaseToken: m.LeaseToken, ConsumerID: c.cfg.ConsumerID, VisibilityTimeout: d}
	path := "/v1/messages/" + url.PathEscape(m.ID) + "/extend"
	var err error
	if m.Partitioned {
		var base string
		base, err = c.PartitionResourceLeaderURL(ctx, "queue", m.Queue, m.Partition)
		if err == nil {
			err = c.doJSONAt(ctx, base, fh.MethodPOST, path, body, nil)
		}
	} else {
		err = c.doJSON(ctx, fh.MethodPOST, path, body, nil)
	}
	if err == nil && c.cfg.Hooks.OnLeaseExtended != nil {
		c.cfg.Hooks.OnLeaseExtended(MessageEvent{Message: m, Queue: m.Queue, Time: time.Now()})
	}
	return err
}
func (c *Client) Progress(ctx context.Context, m broker.Message, p broker.Progress) error {
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "progress": p}
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/messages/"+m.ID+"/progress", body, nil)
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
	return c.doJSON(ctx, fh.MethodPOST, "/v1/messages/"+m.ID+"/respond", body, nil)
}
func (c *Client) RespondError(ctx context.Context, m broker.Message, e error, ack bool) error {
	if e == nil {
		e = errors.New("handler error")
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "error": broker.BrokerError{Code: "handler_error", Message: e.Error(), Retryable: true}, "ack": ack}
	return c.doJSON(ctx, fh.MethodPOST, "/v1/messages/"+m.ID+"/respond", body, nil)
}

func (c *Client) Complete(ctx context.Context, m broker.Message, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "payload": json.RawMessage(raw), "ack": true}
	return c.doJSON(ctx, fh.MethodPOST, "/v1/messages/"+m.ID+"/complete", body, nil)
}

func (c *Client) Fail(ctx context.Context, m broker.Message, e error) error {
	if e == nil {
		e = errors.New("handler error")
	}
	body := map[string]any{"lease_token": m.LeaseToken, "consumer_id": c.cfg.ConsumerID, "queue": m.Queue, "correlation_id": m.CorrelationID, "error": broker.BrokerError{Code: "handler_error", Message: e.Error(), Retryable: true}, "ack": true}
	return c.doJSON(ctx, fh.MethodPOST, "/v1/messages/"+m.ID+"/fail", body, nil)
}

func (c *Client) PublishFrame(ctx context.Context, msg broker.Message) (broker.PublishResult, error) {
	frame, err := c.cfg.FrameCodec.EncodeMessage(nil, msg)
	if err != nil {
		return broker.PublishResult{}, err
	}
	var out broker.PublishResult
	err = c.doRaw(ctx, fh.MethodPOST, "/v1/publish/frame", "application/vnd.pool.broker.frame", frame, &out)
	return out, err
}

func (c *Client) PollFrames(ctx context.Context, queue string, limit int) ([]broker.Message, error) {
	if limit <= 0 {
		limit = c.cfg.PollLimit
	}
	req := broker.PollRequest{Queue: queue, ConsumerID: c.cfg.ConsumerID, ConsumerGroup: c.cfg.Group, Limit: limit, WaitNS: c.cfg.PollWait.Nanoseconds(), VisibilityTimeoutNS: c.cfg.VisibilityTimeout.Nanoseconds()}
	data, err := c.doRawBytes(ctx, fh.MethodPOST, "/v1/poll/frame", "application/json", mustMarshal(req))
	if err != nil {
		return nil, err
	}
	return decodeFrameBatch(data, c.cfg.FrameCodec)
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// decodeFrameBatch decodes a length-prefixed frame batch. data is a buffer
// the caller owns for the life of the returned messages, so frames are decoded
// in place rather than copied out first.
func decodeFrameBatch(data []byte, codec broker.FrameCodec) ([]broker.Message, error) {
	var out []broker.Message
	for len(data) > 0 {
		l, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, fmt.Errorf("%w: malformed frame length", broker.ErrInvalidMessage)
		}
		data = data[n:]
		if l > uint64(len(data)) {
			return nil, io.ErrUnexpectedEOF
		}
		frame := data[:l:l]
		data = data[l:]
		if out == nil {
			// Size the result from the first frame (its prefix included):
			// batches are homogeneous enough that this avoids regrowth.
			out = make([]broker.Message, 0, (len(data)+len(frame)+n)/(len(frame)+n))
		}
		var msg broker.Message
		if err := codec.DecodeMessage(frame, &msg); err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
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
	req.Partition = o.Partition
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
	err = c.doJSON(ctx, fh.MethodPOST, "/v1/publish", req, &out)
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
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/publish/batch", req, &out)
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
	if err := c.doJSON(ctx, fh.MethodPOST, "/v1/request", req, &resp); err != nil {
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
	err = c.doJSON(ctx, fh.MethodPOST, "/v1/tasks/submit", req, &out)
	return out, err
}
func (c *Client) Task(ctx context.Context, id string) (broker.TaskInfo, error) {
	var out broker.TaskInfo
	err := c.doJSON(ctx, fh.MethodGET, "/v1/tasks/"+id, nil, &out)
	return out, err
}
func (c *Client) TaskResult(ctx context.Context, id string) (broker.TaskInfo, []byte, error) {
	var out struct {
		Task    broker.TaskInfo `json:"task"`
		Payload json.RawMessage `json:"payload"`
	}
	err := c.doJSON(ctx, fh.MethodGET, "/v1/tasks/"+id+"/result", nil, &out)
	return out.Task, []byte(out.Payload), err
}

func (c *Client) CancelTask(ctx context.Context, id, reason string) error {
	return c.doJSON(ctx, fh.MethodPOST, "/v1/tasks/"+id+"/cancel", map[string]string{"reason": reason}, nil)
}

func (c *Client) RetryTask(ctx context.Context, id string) error {
	return c.doJSON(ctx, fh.MethodPOST, "/v1/tasks/"+id+"/retry", nil, nil)
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
	err := c.doJSON(ctx, fh.MethodGET, path, nil, &out)
	return out, err
}

func (c *Client) ReplayDLQ(ctx context.Context, queue string) error {
	if queue == "" {
		return errors.New("queue required")
	}
	return c.doJSON(ctx, fh.MethodPOST, "/v1/dlq/"+queue+"/replay", nil, nil)
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
	err := c.doJSON(ctx, fh.MethodGET, path, nil, &out)
	return out, err
}

func (c *Client) CommitStreamOffset(ctx context.Context, stream, group string, offset uint64) error {
	return c.doJSON(ctx, fh.MethodPOST, "/v1/streams/"+stream+"/commit", broker.OffsetCommit{Stream: stream, ConsumerGroup: group, Offset: offset}, nil)
}

// CommitPartitionStreamOffset commits a consumer checkpoint within one
// assigned stream partition. Offsets are local to each partition.
func (c *Client) CommitPartitionStreamOffset(ctx context.Context, stream, group string, partition int, offset uint64) error {
	if stream == "" || partition < 0 {
		return fmt.Errorf("%w: invalid stream partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "stream", stream, partition)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/streams/%s/partitions/%d/commit", url.PathEscape(stream), partition)
	return c.doJSONAt(ctx, base, fh.MethodPOST, path, broker.OffsetCommit{Stream: stream, ConsumerGroup: group, Offset: offset, Partition: partition, Partitioned: true}, nil)
}

// CommitPartitionStreamOffsetAndAppend atomically commits a partition-local
// consumer checkpoint and appends outputs confined to that same stream
// partition. Set transactionID to make retries idempotent.
func (c *Client) CommitPartitionStreamOffsetAndAppend(ctx context.Context, stream, group string, partition int, offset uint64, transactionID string, entries []broker.AppendEntry) ([]broker.Message, error) {
	if stream == "" || strings.ContainsAny(stream, "/\\") || partition < 0 {
		return nil, fmt.Errorf("%w: invalid stream partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "stream", stream, partition)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/streams/%s/partitions/%d/commit-and-append", url.PathEscape(stream), partition)
	body := struct {
		ConsumerGroup string               `json:"consumer_group"`
		Offset        uint64               `json:"offset"`
		TransactionID string               `json:"transaction_id,omitempty"`
		Entries       []broker.AppendEntry `json:"entries"`
	}{ConsumerGroup: group, Offset: offset, TransactionID: transactionID, Entries: entries}
	var messages []broker.Message
	err = c.doJSONAt(ctx, base, fh.MethodPOST, path, body, &messages)
	return messages, err
}

// PartitionStreamOffset reads a consumer checkpoint from one assigned stream
// partition.
func (c *Client) PartitionStreamOffset(ctx context.Context, stream, group string, partition int) (uint64, error) {
	if stream == "" || partition < 0 {
		return 0, fmt.Errorf("%w: invalid stream partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "stream", stream, partition)
	if err != nil {
		return 0, err
	}
	path := fmt.Sprintf("/v1/streams/%s/partitions/%d/offset?consumer_group=%s", url.PathEscape(stream), partition, url.QueryEscape(group))
	var out struct {
		Offset uint64 `json:"offset"`
	}
	err = c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &out)
	return out.Offset, err
}

// CommitStreamTransaction publishes outputs and advances the input offset in one
// operation. ErrOffsetConflict means another commit changed the expected offset.
func (c *Client) CommitStreamTransaction(ctx context.Context, stream string, tx broker.StreamTransactionHTTPPayload) ([]broker.PublishResult, error) {
	var out []broker.PublishResult
	err := c.doJSON(ctx, fh.MethodPOST, "/v1/streams/"+url.PathEscape(stream)+"/transactions", tx, &out)
	return out, err
}

func (c *Client) StreamOffset(ctx context.Context, stream, group string) (uint64, error) {
	var out struct {
		Offset uint64 `json:"offset"`
	}
	err := c.doJSON(ctx, fh.MethodGET, "/v1/streams/"+stream+"/offset?consumer_group="+group, nil, &out)
	return out.Offset, err
}
func (c *Client) Consumers(ctx context.Context) ([]broker.ConsumerInfo, error) {
	var out []broker.ConsumerInfo
	err := c.doJSON(ctx, fh.MethodGET, "/v1/consumers", nil, &out)
	return out, err
}

// PartitionRoute retrieves the quorum-confirmed leader assignment for one
// partition and any local Raft observation returned by the broker.
func (c *Client) PartitionRoute(ctx context.Context, topic string, partition int) (*broker.PartitionRoute, error) {
	return c.PartitionResourceRoute(ctx, "topic", topic, partition)
}

// PartitionResourceRoute retrieves the route for a topic or stream partition.
func (c *Client) PartitionResourceRoute(ctx context.Context, kind, name string, partition int) (*broker.PartitionRoute, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	path := fmt.Sprintf("/cluster/partitions/%s/%d/route", url.PathEscape(name), partition)
	if kind != "topic" {
		path += "?resource_kind=" + url.QueryEscape(kind)
	}
	var route broker.PartitionRoute
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

func (c *Client) PlanPartitionReassignment(ctx context.Context, kind, name string, partition int, replicas []string, leader string) (*broker.PartitionReassignmentPlan, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	query := url.Values{}
	query.Set("resource_kind", kind)
	query.Set("replicas", strings.Join(replicas, ","))
	if leader != "" {
		query.Set("leader", leader)
	}
	path := fmt.Sprintf("/cluster/partitions/%s/%d/reassignment-plan?%s", url.PathEscape(name), partition, query.Encode())
	base, err := c.PartitionResourceLeaderURL(ctx, kind, name, partition)
	if err != nil {
		return nil, err
	}
	var plan broker.PartitionReassignmentPlan
	err = c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &plan)
	return &plan, err
}

// ReassignPartition advances one safe reassignment phase. Repeat the call with
// the same replica set and leader while Complete is false; WaitingFor reports
// replicas that are still catching up or membership steps awaiting retry.
func (c *Client) ReassignPartition(ctx context.Context, kind, name string, partition int, replicas []string, leader string) (*broker.PartitionReassignmentProgress, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	path := fmt.Sprintf("/cluster/partitions/%s/%d/reassign?resource_kind=%s", url.PathEscape(name), partition, url.QueryEscape(kind))
	base, err := c.PartitionResourceLeaderURL(ctx, kind, name, partition)
	if err != nil {
		return nil, err
	}
	request := struct {
		Replicas []string `json:"replicas"`
		Leader   string   `json:"leader,omitempty"`
	}{Replicas: append([]string(nil), replicas...), Leader: leader}
	var progress broker.PartitionReassignmentProgress
	if err := c.doJSONAt(ctx, base, fh.MethodPOST, path, request, &progress); err != nil {
		return nil, err
	}
	return &progress, nil
}

// CancelPartitionReassignment stops a not-yet-committed reassignment and
// removes its target-only Raft members. Repeat while Complete is false.
func (c *Client) CancelPartitionReassignment(ctx context.Context, kind, name string, partition int) (*broker.PartitionReassignmentProgress, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	path := fmt.Sprintf("/cluster/partitions/%s/%d/cancel-reassignment?resource_kind=%s", url.PathEscape(name), partition, url.QueryEscape(kind))
	base, err := c.PartitionResourceLeaderURL(ctx, kind, name, partition)
	if err != nil {
		return nil, err
	}
	var progress broker.PartitionReassignmentProgress
	if err := c.doJSONAt(ctx, base, fh.MethodPOST, path, nil, &progress); err != nil {
		return nil, err
	}
	return &progress, nil
}

// PlanRaftNodeRemoval lists partition assignments that must be moved before a
// controller Raft peer can be removed.
func (c *Client) PlanRaftNodeRemoval(ctx context.Context, peerID uint64) (*broker.RaftNodeRemovalPlan, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var plan broker.RaftNodeRemovalPlan
	path := fmt.Sprintf("/cluster/nodes/%d/removal-plan", peerID)
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

// PlanClusterUpgrade returns a read-only version inventory and conservative
// controller-peer upgrade order. It does not verify storage or protocol
// compatibility; inspect plan.ManualChecks before using the order.
func (c *Client) PlanClusterUpgrade(ctx context.Context, targetVersion string) (*broker.ClusterUpgradePlan, error) {
	if strings.TrimSpace(targetVersion) == "" {
		return nil, fmt.Errorf("%w: target broker version is required", broker.ErrInvalidMessage)
	}
	var plan broker.ClusterUpgradePlan
	path := "/cluster/upgrade-plan?target_version=" + url.QueryEscape(targetVersion)
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &plan); err != nil {
		return nil, err
	}
	if plan.TargetVersion != targetVersion || plan.CompatibilityVerified {
		return nil, fmt.Errorf("%w: server returned an invalid cluster upgrade plan", broker.ErrInvalidMessage)
	}
	return &plan, nil
}

// BeginRaftNodeDrain prevents new partition assignments from adding a
// controller peer's logical node while existing assignments are moved.
func (c *Client) BeginRaftNodeDrain(ctx context.Context, peerID uint64) (*cluster.NodeInfo, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var response struct {
		Node *cluster.NodeInfo `json:"node"`
	}
	path := fmt.Sprintf("/cluster/nodes/%d/drain", peerID)
	if err := c.doJSON(ctx, fh.MethodPOST, path, struct{}{}, &response); err != nil {
		return nil, err
	}
	if response.Node == nil || response.Node.State != cluster.NodeDraining {
		return nil, fmt.Errorf("%w: drain response does not confirm the draining state", broker.ErrInvalidMessage)
	}
	return response.Node, nil
}

// CompleteRaftNodeDrain marks a drained node left after its partition
// assignments are gone.
func (c *Client) CompleteRaftNodeDrain(ctx context.Context, peerID uint64) (*cluster.NodeInfo, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var response struct {
		Node *cluster.NodeInfo `json:"node"`
	}
	path := fmt.Sprintf("/cluster/nodes/%d/drain/complete", peerID)
	if err := c.doJSON(ctx, fh.MethodPOST, path, struct{}{}, &response); err != nil {
		return nil, err
	}
	if response.Node == nil || response.Node.State != cluster.NodeLeft {
		return nil, fmt.Errorf("%w: drain completion response does not confirm the left state", broker.ErrInvalidMessage)
	}
	return response.Node, nil
}

// RaftMembershipResult describes the result of a controller membership action.
type RaftMembershipResult struct {
	Status   string `json:"status,omitempty"`
	PeerID   uint64 `json:"peer_id,omitempty"`
	Role     string `json:"role,omitempty"`
	LeaderID uint64 `json:"leader_id,omitempty"`
}

// AddRaftLearner adds a non-voting controller peer and returns its assigned
// Raft ID. The peer must catch up before it can be promoted.
func (c *Client) AddRaftLearner(ctx context.Context, address string) (*RaftMembershipResult, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("%w: peer address is required", broker.ErrInvalidMessage)
	}
	var result RaftMembershipResult
	address = strings.TrimSpace(address)
	if err := c.doJSON(ctx, fh.MethodPOST, "/cluster/nodes", map[string]string{"address": address}, &result); err != nil {
		return nil, err
	}
	if result.PeerID == 0 || (result.Role != "learner" && result.Role != "voter") {
		return nil, fmt.Errorf("%w: learner response contains invalid membership data", broker.ErrInvalidMessage)
	}
	return &result, nil
}

// PromoteRaftLearner promotes a caught-up learner to a controller voter.
func (c *Client) PromoteRaftLearner(ctx context.Context, peerID uint64) (*RaftMembershipResult, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var result RaftMembershipResult
	path := fmt.Sprintf("/cluster/nodes/%d/promote", peerID)
	if err := c.doJSON(ctx, fh.MethodPOST, path, struct{}{}, &result); err != nil {
		return nil, err
	}
	if result.PeerID != peerID || result.Role != "voter" {
		return nil, fmt.Errorf("%w: promotion response does not confirm voter membership", broker.ErrInvalidMessage)
	}
	return &result, nil
}

// TransferControllerLeadership moves controller leadership to a caught-up
// voter and waits for the target to become leader.
func (c *Client) TransferControllerLeadership(ctx context.Context, peerID uint64) (*RaftMembershipResult, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var result RaftMembershipResult
	if err := c.doJSON(ctx, fh.MethodPOST, "/cluster/leadership", map[string]uint64{"peer_id": peerID}, &result); err != nil {
		return nil, err
	}
	if result.LeaderID != peerID {
		return nil, fmt.Errorf("%w: leadership response does not confirm the requested leader", broker.ErrInvalidMessage)
	}
	return &result, nil
}

// RemoveRaftNode removes a voter or learner after the server's drain and
// leadership preconditions pass.
func (c *Client) RemoveRaftNode(ctx context.Context, peerID uint64) (*RaftMembershipResult, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("%w: Raft peer ID must be positive", broker.ErrInvalidMessage)
	}
	var result RaftMembershipResult
	path := fmt.Sprintf("/cluster/nodes/%d", peerID)
	if err := c.doJSON(ctx, fh.MethodDELETE, path, nil, &result); err != nil {
		return nil, err
	}
	if result.PeerID != peerID {
		return nil, fmt.Errorf("%w: removal response does not confirm the requested peer", broker.ErrInvalidMessage)
	}
	return &result, nil
}

// PlanPartitionPlacement previews deterministic load-aware initial placement.
func (c *Client) PlanPartitionPlacement(ctx context.Context, kind, name string, partition int) (*broker.PartitionPlacementPlan, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	query := url.Values{}
	query.Set("resource_kind", kind)
	path := fmt.Sprintf("/cluster/partitions/%s/%d/placement-plan?%s", url.PathEscape(name), partition, query.Encode())
	var plan broker.PartitionPlacementPlan
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

// AssignPartitionBalanced asks the controller to select active replicas using
// the current replica and leader counts. The assignment is idempotent.
func (c *Client) AssignPartitionBalanced(ctx context.Context, kind, name string, partition int) (*cluster.PartitionMetadata, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || partition < 0 || (kind != "topic" && kind != "stream" && kind != "queue") {
		return nil, fmt.Errorf("%w: invalid partition resource or index", broker.ErrInvalidMessage)
	}
	request := struct {
		Name         string `json:"name"`
		ResourceKind string `json:"resource_kind"`
		Partition    int    `json:"partition"`
		AutoAssign   bool   `json:"auto_assign"`
	}{Name: name, ResourceKind: kind, Partition: partition, AutoAssign: true}
	var assignment cluster.PartitionMetadata
	if err := c.doJSON(ctx, fh.MethodPOST, "/cluster/partitions", request, &assignment); err != nil {
		return nil, err
	}
	return &assignment, nil
}

// PartitionLeaderURL returns the broker's advertised client URL for a
// partition leader. Raft peer addresses are not valid client endpoints and are
// deliberately not used as a fallback.
func (c *Client) PartitionLeaderURL(ctx context.Context, topic string, partition int) (string, error) {
	return c.PartitionResourceLeaderURL(ctx, "topic", topic, partition)
}

func (c *Client) PartitionResourceLeaderURL(ctx context.Context, kind, name string, partition int) (string, error) {
	route, err := c.PartitionResourceRoute(ctx, kind, name, partition)
	if err != nil {
		return "", err
	}
	if route.LeaderClientAddress == "" {
		return "", fmt.Errorf("partition %s/%d leader has no advertised client address", name, partition)
	}
	parsed, err := url.ParseRequestURI(route.LeaderClientAddress)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("partition %s/%d has an invalid advertised client address", name, partition)
	}
	allowed := false
	for _, configured := range c.orderedURLs() {
		base, parseErr := url.Parse(configured)
		if parseErr == nil && strings.EqualFold(base.Scheme, parsed.Scheme) && strings.EqualFold(base.Host, parsed.Host) {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", fmt.Errorf("partition %s/%d leader client address is outside the configured broker origins", name, partition)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

// ReadPartitionStream reads one stream partition from its assigned client
// endpoint. Offsets are local to that partition.
func (c *Client) ReadPartitionStream(ctx context.Context, stream string, partition int, afterOffset uint64, limit int) ([]broker.Message, error) {
	if stream == "" || strings.ContainsAny(stream, "/\\") || partition < 0 {
		return nil, fmt.Errorf("%w: invalid stream partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "stream", stream, partition)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/streams/%s/partitions/%d/messages", url.PathEscape(stream), partition)
	query := url.Values{}
	if afterOffset > 0 {
		query.Set("after_offset", strconv.FormatUint(afterOffset, 10))
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var messages []broker.Message
	if err := c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// ReadPartitionTopic reads one topic partition from its assigned client
// endpoint. The advertised endpoint must share an origin with a configured
// BrokerURL so the client's bearer token is never sent to an unconfigured host.
func (c *Client) ReadPartitionTopic(ctx context.Context, topic string, partition int, afterID string, limit int) ([]broker.Message, error) {
	if topic == "" || strings.ContainsAny(topic, "/\\") || partition < 0 {
		return nil, fmt.Errorf("%w: invalid topic partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionLeaderURL(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/topics/%s/partitions/%d/messages", url.PathEscape(topic), partition)
	query := url.Values{}
	if afterID != "" {
		query.Set("after", afterID)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var messages []broker.Message
	if err := c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// TopicPage contains one merged page and an opaque cursor for the next page.
type TopicPage struct {
	Messages   []broker.Message `json:"messages"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// StreamPage contains one merged stream page and an opaque per-partition
// offset cursor. Stream offsets are local to each assigned partition.
type StreamPage struct {
	Messages   []broker.Message `json:"messages"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type AssignedStreamLag = broker.AssignedStreamLag

func (c *Client) StreamPartitionLag(ctx context.Context, stream string, partition int, consumerGroup string) (broker.PartitionStreamLag, error) {
	if stream == "" || strings.ContainsAny(stream, "/\\") || partition < 0 {
		return broker.PartitionStreamLag{}, fmt.Errorf("%w: invalid stream partition", broker.ErrInvalidMessage)
	}
	base, err := c.PartitionResourceLeaderURL(ctx, "stream", stream, partition)
	if err != nil {
		return broker.PartitionStreamLag{}, err
	}
	path := fmt.Sprintf("/v1/streams/%s/partitions/%d/lag", url.PathEscape(stream), partition)
	query := url.Values{}
	if consumerGroup != "" {
		query.Set("consumer_group", consumerGroup)
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var report broker.PartitionStreamLag
	err = c.doJSONAt(ctx, base, fh.MethodGET, path, nil, &report)
	return report, err
}

// StreamLagAssigned requests a server-side aggregate. The result retains each
// partition's local head and committed offset because offsets are not
// comparable across partitions.
func (c *Client) StreamLagAssigned(ctx context.Context, stream, consumerGroup string) (*AssignedStreamLag, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if stream == "" || strings.ContainsAny(stream, "/\\") {
		return nil, fmt.Errorf("%w: invalid stream", broker.ErrInvalidMessage)
	}
	query := url.Values{}
	if consumerGroup != "" {
		query.Set("consumer_group", consumerGroup)
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/lag"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var report AssignedStreamLag
	if err := c.doJSON(ctx, fh.MethodGET, path, nil, &report); err != nil {
		return nil, err
	}
	resultCount := len(report.Partitions) + len(report.Failures)
	if report.Stream != stream || report.ConsumerGroup != consumerGroup || resultCount == 0 || resultCount > maxTopicPagePartitions {
		return nil, fmt.Errorf("%w: server returned an invalid assigned stream lag report", broker.ErrUnsupportedMode)
	}
	var total uint64
	lastPartition := -1
	seenPartitions := make(map[int]struct{}, resultCount)
	overflow := false
	for _, partition := range report.Partitions {
		if partition.Stream != stream || partition.ConsumerGroup != consumerGroup || partition.Partition < 0 || partition.Partition <= lastPartition {
			return nil, fmt.Errorf("%w: server returned inconsistent assigned stream lag data", broker.ErrInvalidMessage)
		}
		lastPartition = partition.Partition
		seenPartitions[partition.Partition] = struct{}{}
		if partition.Lag > ^uint64(0)-total {
			overflow = true
			total = ^uint64(0)
		} else if !overflow {
			total += partition.Lag
		}
	}
	var failures []error
	for partition, reason := range report.Failures {
		if partition < 0 || reason == "" {
			return nil, fmt.Errorf("%w: server returned an invalid stream lag failure", broker.ErrInvalidMessage)
		}
		if _, duplicate := seenPartitions[partition]; duplicate {
			return nil, fmt.Errorf("%w: server reported both success and failure for a stream partition", broker.ErrInvalidMessage)
		}
		seenPartitions[partition] = struct{}{}
		failures = append(failures, fmt.Errorf("stream %s partition %d: %s", stream, partition, reason))
	}
	if report.Error != "" && report.Error != "aggregate lag overflows uint64" {
		return nil, fmt.Errorf("%w: server returned an unknown aggregate lag error", broker.ErrInvalidMessage)
	}
	if total != report.TotalLag || (overflow && report.Error == "") || (!overflow && report.Error != "") {
		return nil, fmt.Errorf("%w: server returned an inconsistent aggregate lag", broker.ErrInvalidMessage)
	}
	if report.Error != "" {
		failures = append(failures, fmt.Errorf("stream %s: %s", stream, report.Error))
	}
	return &report, errors.Join(failures...)
}

type topicPageCursor struct {
	Version       int            `json:"version"`
	Topic         string         `json:"topic"`
	Partitions    []int          `json:"partitions"`
	Positions     map[int]string `json:"positions"`
	NextPartition int            `json:"next_partition,omitempty"`
}

type topicPageMessage struct {
	partition int
	position  int
	message   broker.Message
}

const maxTopicPageSize = 10000
const maxTopicCursorEncodedSize = 4 << 20
const maxTopicPagePartitions = 10000

func perPartitionPageLimit(pageLimit, partitionCount int) int {
	if partitionCount <= 0 {
		return 1
	}
	readLimit := (pageLimit + partitionCount - 1) / partitionCount
	maxPerPartition := maxTopicPageSize / partitionCount
	if maxPerPartition < 1 {
		maxPerPartition = 1
	}
	if readLimit > maxPerPartition {
		readLimit = maxPerPartition
	}
	if readLimit < 1 {
		return 1
	}
	return readLimit
}

type streamPageCursor struct {
	Version       int            `json:"version"`
	Stream        string         `json:"stream"`
	Partitions    []int          `json:"partitions"`
	Positions     map[int]uint64 `json:"positions"`
	NextPartition int            `json:"next_partition,omitempty"`
}

type streamPageMessage struct {
	partition int
	position  int
	message   broker.Message
}

// ReadStreamPage merges every assigned stream partition and advances a local
// offset per partition, so equal offsets in different partitions cannot cause
// messages to be skipped between pages.
func (c *Client) ReadStreamPage(ctx context.Context, stream, cursor string, limit int) (*StreamPage, error) {
	if stream == "" || strings.ContainsAny(stream, "/\\") {
		return nil, fmt.Errorf("%w: invalid stream", broker.ErrInvalidMessage)
	}
	if len(cursor) > maxTopicCursorEncodedSize {
		return nil, fmt.Errorf("%w: stream cursor exceeds %d bytes", broker.ErrInvalidMessage, maxTopicCursorEncodedSize)
	}
	if limit <= 0 {
		limit = 100
	} else if limit > maxTopicPageSize {
		limit = maxTopicPageSize
	}
	var assignments []struct {
		ID struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Kind      string `json:"kind"`
		} `json:"id"`
	}
	if err := c.doJSON(ctx, fh.MethodGET, "/cluster/partitions", nil, &assignments); err != nil {
		return nil, err
	}
	partitionSet := make(map[int]struct{})
	for _, assignment := range assignments {
		if assignment.ID.Topic == stream && assignment.ID.Kind == "stream" {
			if assignment.ID.Partition < 0 {
				return nil, fmt.Errorf("%w: invalid stream partition assignment for %q", broker.ErrInvalidMessage, stream)
			}
			partitionSet[assignment.ID.Partition] = struct{}{}
		}
	}
	partitions := make([]int, 0, len(partitionSet))
	for partition := range partitionSet {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	if len(partitions) > maxTopicPagePartitions {
		return nil, fmt.Errorf("%w: stream %q has %d partitions; client page limit is %d", broker.ErrUnsupportedMode, stream, len(partitions), maxTopicPagePartitions)
	}
	if len(partitions) == 0 {
		return &StreamPage{Messages: []broker.Message{}}, nil
	}
	partitionReadLimit := perPartitionPageLimit(limit, len(partitions))
	positions := make(map[int]uint64, len(partitions))
	nextPartition := partitions[0]
	if cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid stream cursor", broker.ErrInvalidMessage)
		}
		var state streamPageCursor
		if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Stream != stream || !samePartitionIndexes(state.Partitions, partitions) {
			return nil, fmt.Errorf("%w: stream cursor does not match current stream assignment", broker.ErrInvalidMessage)
		}
		if _, assigned := partitionSet[state.NextPartition]; !assigned || len(state.Positions) > len(partitions) {
			return nil, fmt.Errorf("%w: stream cursor has invalid partition positions", broker.ErrInvalidMessage)
		}
		nextPartition = state.NextPartition
		for partition, offset := range state.Positions {
			if _, assigned := partitionSet[partition]; !assigned {
				return nil, fmt.Errorf("%w: invalid stream cursor partition %d", broker.ErrInvalidMessage, partition)
			}
			positions[partition] = offset
		}
	}
	type partitionResult struct {
		messages []broker.Message
		err      error
	}
	results := make([]partitionResult, len(partitions))
	semaphore := make(chan struct{}, 16)
	var wait sync.WaitGroup
	for i, partition := range partitions {
		wait.Add(1)
		go func(index, partition int) {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				results[index].err = ctx.Err()
				return
			}
			defer func() { <-semaphore }()
			results[index].messages, results[index].err = c.ReadPartitionStream(ctx, stream, partition, positions[partition], partitionReadLimit)
		}(i, partition)
	}
	wait.Wait()
	merged := make([]streamPageMessage, 0)
	for i, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for position, message := range result.messages {
			merged = append(merged, streamPageMessage{partition: partitions[i], position: position, message: message})
		}
	}
	startIndex := sort.SearchInts(partitions, nextPartition)
	if startIndex == len(partitions) {
		startIndex = 0
	}
	partitionRank := func(partition int) int {
		index := sort.SearchInts(partitions, partition)
		return (index - startIndex + len(partitions)) % len(partitions)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].position == merged[j].position {
			return partitionRank(merged[i].partition) < partitionRank(merged[j].partition)
		}
		return merged[i].position < merged[j].position
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	messages := make([]broker.Message, len(merged))
	for i, entry := range merged {
		messages[i] = entry.message
		positions[entry.partition] = entry.message.Offset
	}
	page := &StreamPage{Messages: messages}
	if len(messages) > 0 {
		lastPartition := merged[len(merged)-1].partition
		lastIndex := sort.SearchInts(partitions, lastPartition)
		state := streamPageCursor{
			Version: 1, Stream: stream, Partitions: partitions, Positions: positions,
			NextPartition: partitions[(lastIndex+1)%len(partitions)],
		}
		data, err := json.Marshal(state)
		if err != nil {
			return nil, err
		}
		nextCursor := base64.RawURLEncoding.EncodeToString(data)
		if len(nextCursor) > maxTopicCursorEncodedSize {
			return nil, fmt.Errorf("%w: stream assignment produces a cursor larger than %d bytes", broker.ErrInvalidMessage, maxTopicCursorEncodedSize)
		}
		page.NextCursor = nextCursor
	}
	return page, nil
}

// ReadTopicPage reads one merged page across every controller-assigned
// partition. Cursor positions advance only for returned messages, so messages
// fetched from a partition but sorted onto the next page are read again instead
// of being skipped. Pages use a deterministic round-robin merge by each
// partition's local position, preserving every partition's order.
func (c *Client) ReadTopicPage(ctx context.Context, topic, cursor string, limit int) (*TopicPage, error) {
	if topic == "" || strings.ContainsAny(topic, "/\\") {
		return nil, fmt.Errorf("%w: invalid topic", broker.ErrInvalidMessage)
	}
	if len(cursor) > maxTopicCursorEncodedSize {
		return nil, fmt.Errorf("%w: topic cursor exceeds %d bytes", broker.ErrInvalidMessage, maxTopicCursorEncodedSize)
	}
	if limit <= 0 {
		limit = 100
	} else if limit > maxTopicPageSize {
		limit = maxTopicPageSize
	}
	var assignments []struct {
		ID struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Kind      string `json:"kind"`
		} `json:"id"`
	}
	if err := c.doJSON(ctx, fh.MethodGET, "/cluster/partitions", nil, &assignments); err != nil {
		return nil, err
	}
	partitionSet := make(map[int]struct{})
	for _, assignment := range assignments {
		if assignment.ID.Topic == topic && (assignment.ID.Kind == "" || assignment.ID.Kind == "topic") {
			if assignment.ID.Partition < 0 {
				return nil, fmt.Errorf("%w: invalid partition assignment for topic %q", broker.ErrInvalidMessage, topic)
			}
			partitionSet[assignment.ID.Partition] = struct{}{}
		}
	}
	partitions := make([]int, 0, len(partitionSet))
	for partition := range partitionSet {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	if len(partitions) > maxTopicPagePartitions {
		return nil, fmt.Errorf("%w: topic %q has %d partitions; client page limit is %d", broker.ErrUnsupportedMode, topic, len(partitions), maxTopicPagePartitions)
	}
	if len(partitions) == 0 {
		return &TopicPage{Messages: []broker.Message{}}, nil
	}
	partitionReadLimit := perPartitionPageLimit(limit, len(partitions))
	positions := make(map[int]string, len(partitions))
	nextPartition := partitions[0]
	if cursor != "" {
		data, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: invalid topic cursor", broker.ErrInvalidMessage)
		}
		var state topicPageCursor
		if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Topic != topic || !samePartitionIndexes(state.Partitions, partitions) {
			return nil, fmt.Errorf("%w: topic cursor does not match current topic assignment", broker.ErrInvalidMessage)
		}
		if _, assigned := partitionSet[state.NextPartition]; !assigned {
			return nil, fmt.Errorf("%w: topic cursor has an invalid round-robin position", broker.ErrInvalidMessage)
		}
		if len(state.Positions) > len(partitions) {
			return nil, fmt.Errorf("%w: topic cursor has too many partition positions", broker.ErrInvalidMessage)
		}
		nextPartition = state.NextPartition
		for partition, messageID := range state.Positions {
			if _, assigned := partitionSet[partition]; !assigned || messageID == "" {
				return nil, fmt.Errorf("%w: invalid topic cursor position", broker.ErrInvalidMessage)
			}
			positions[partition] = messageID
		}
	}
	type partitionResult struct {
		messages []broker.Message
		err      error
	}
	results := make([]partitionResult, len(partitions))
	semaphore := make(chan struct{}, 16)
	var wait sync.WaitGroup
	for i, partition := range partitions {
		wait.Add(1)
		go func(index, partition int) {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				results[index].err = ctx.Err()
				return
			}
			defer func() { <-semaphore }()
			results[index].messages, results[index].err = c.ReadPartitionTopic(ctx, topic, partition, positions[partition], partitionReadLimit)
		}(i, partition)
	}
	wait.Wait()
	merged := make([]topicPageMessage, 0)
	for i, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for position, message := range result.messages {
			merged = append(merged, topicPageMessage{partition: partitions[i], position: position, message: message})
		}
	}
	startIndex := sort.SearchInts(partitions, nextPartition)
	if startIndex == len(partitions) {
		startIndex = 0
	}
	partitionRank := func(partition int) int {
		index := sort.SearchInts(partitions, partition)
		return (index - startIndex + len(partitions)) % len(partitions)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		a, b := merged[i], merged[j]
		if a.position == b.position {
			return partitionRank(a.partition) < partitionRank(b.partition)
		}
		return a.position < b.position
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	messages := make([]broker.Message, len(merged))
	for i, entry := range merged {
		messages[i] = entry.message
		positions[entry.partition] = entry.message.ID
	}
	page := &TopicPage{Messages: messages}
	if len(messages) > 0 {
		lastPartition := merged[len(merged)-1].partition
		lastIndex := sort.SearchInts(partitions, lastPartition)
		state := topicPageCursor{
			Version: 1, Topic: topic, Partitions: partitions, Positions: positions,
			NextPartition: partitions[(lastIndex+1)%len(partitions)],
		}
		data, err := json.Marshal(state)
		if err != nil {
			return nil, err
		}
		nextCursor := base64.RawURLEncoding.EncodeToString(data)
		if len(nextCursor) > maxTopicCursorEncodedSize {
			return nil, fmt.Errorf("%w: topic assignment produces a cursor larger than %d bytes", broker.ErrInvalidMessage, maxTopicCursorEncodedSize)
		}
		page.NextCursor = nextCursor
	}
	return page, nil
}

// ListTopic returns the first bounded round-robin page of all
// controller-assigned topic partitions. Use ReadTopicPage when the caller
// needs a continuation cursor.
func (c *Client) ListTopic(ctx context.Context, topic string, limit int) ([]broker.Message, error) {
	page, err := c.ReadTopicPage(ctx, topic, "", limit)
	if err != nil {
		return nil, err
	}
	return page.Messages, nil
}

func samePartitionIndexes(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
	err := c.doJSON(ctx, fh.MethodGET, path, nil, &out)
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
		resp, err := c.send(ctx, method, base+path, bodyBytes, c.headers.get(contentType, prefer))
		if err != nil {
			last = err
			if i == 0 && c.cfg.Hooks.OnReconnect != nil && len(urls) > 1 {
				c.cfg.Hooks.OnReconnect(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, Error: err, Time: time.Now()})
			}
			continue
		}
		if resp.Raw.ContentLength > c.cfg.MaxResponseBytes {
			_ = resp.Close()
			return nil, 0, fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, c.cfg.MaxResponseBytes)
		}
		data, readErr := readBoundedResponseHint(resp.Reader(), c.cfg.MaxResponseBytes, resp.Raw.ContentLength)
		_ = resp.Close()
		if readErr != nil {
			return nil, 0, readErr
		}
		if resp.StatusCode() >= 300 {
			last = remoteError(resp.StatusCode(), data)
			if len(urls) > 1 && shouldFailover(method, resp.StatusCode(), data) {
				prioritizeLeaderURL(urls, i, last)
				continue
			}
			c.markActive(base)
			return nil, resp.StatusCode(), last
		}
		c.markActive(base)
		return data, resp.StatusCode(), nil
	}
	if last == nil {
		last = errors.New("broker unavailable")
	}
	return nil, 0, last
}
func (c *Client) doJSON(ctx context.Context, method, path string, in, out any) error {
	return c.doJSONBases(ctx, c.orderedURLs(), method, path, in, out)
}

func (c *Client) doJSONAt(ctx context.Context, base, method, path string, in, out any) error {
	base = strings.TrimRight(base, "/")
	err := c.doJSONBases(ctx, []string{base}, method, path, in, out)
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || (remote.Class != broker.RetryNotLeader && remote.Code != broker.ErrorCode(broker.ErrNotLeader)) {
		return err
	}
	leader, _, _ := remote.RetryHint()
	trustedBase := c.trustedLeaderBase(leader)
	if trustedBase == "" || trustedBase == base {
		return err
	}
	return c.doJSONBases(ctx, []string{trustedBase}, method, path, in, out)
}

// trustedLeaderBase accepts a redirect only when its origin matches a broker
// URL explicitly configured by the caller. It returns that configured base,
// rather than retaining any path or other URL components from the response.
func (c *Client) trustedLeaderBase(leader string) string {
	parsedLeader, err := url.ParseRequestURI(leader)
	if err != nil || (parsedLeader.Scheme != "http" && parsedLeader.Scheme != "https") || parsedLeader.Host == "" ||
		parsedLeader.User != nil || parsedLeader.RawQuery != "" || parsedLeader.Fragment != "" {
		return ""
	}
	for _, configured := range c.orderedURLs() {
		parsedConfigured, err := url.Parse(configured)
		if err == nil && strings.EqualFold(parsedConfigured.Scheme, parsedLeader.Scheme) && strings.EqualFold(parsedConfigured.Host, parsedLeader.Host) {
			return strings.TrimRight(configured, "/")
		}
	}
	return ""
}

func (c *Client) doJSONBases(ctx context.Context, urls []string, method, path string, in, out any) error {
	var bodyBytes []byte
	contentType := ""
	if in != nil {
		reqBuf, err := encodeJSONBody(in)
		if err != nil {
			return err
		}
		// The body is only read while the request is in flight; every attempt
		// below completes before this function returns.
		defer putRequestBuffer(reqBuf)
		bodyBytes, contentType = reqBuf.Bytes(), "application/json"
	}
	headers := c.headers.get(contentType, "")
	respBuf := getRequestBuffer()
	defer putRequestBuffer(respBuf)
	var last error
	for i, base := range urls {
		resp, err := c.send(ctx, method, base+path, bodyBytes, headers)
		if err != nil {
			last = err
			if i == 0 && c.cfg.Hooks.OnReconnect != nil && len(urls) > 1 {
				c.cfg.Hooks.OnReconnect(Event{ConsumerID: c.cfg.ConsumerID, Group: c.cfg.Group, Error: err, Time: time.Now()})
			}
			continue
		}
		if resp.Raw.ContentLength > c.cfg.MaxResponseBytes {
			_ = resp.Close()
			return fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, c.cfg.MaxResponseBytes)
		}
		respBuf.Reset()
		readErr := readBoundedInto(respBuf, resp.Reader(), c.cfg.MaxResponseBytes, resp.Raw.ContentLength)
		status := resp.StatusCode()
		_ = resp.Close()
		if readErr != nil {
			return readErr
		}
		data := respBuf.Bytes()
		if status >= 300 {
			// remoteError keeps text from the body, so give it its own copy.
			last = remoteError(status, bytes.Clone(data))
			if len(urls) > 1 && shouldFailover(method, status, data) {
				prioritizeLeaderURL(urls, i, last)
				continue
			}
			c.markActive(base)
			return last
		}
		c.markActive(base)
		if out != nil && len(data) > 0 {
			// json.Unmarshal copies everything it keeps, so the pooled buffer
			// can be reused afterwards.
			return json.Unmarshal(data, out)
		}
		return nil
	}
	if last == nil {
		last = errBrokerUnavailable
	}
	return last
}

func (c *Client) orderedURLs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.urls) == 1 {
		// Callers only reorder the slice when it has more than one entry
		// (prioritizeLeaderURL), so the single-broker slice can be shared.
		return c.urls
	}
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
	Priority, Partition                                            int

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
func WithPartition(v int) PublishOption   { return func(o *PublishOptions) { o.Partition = v } }
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
