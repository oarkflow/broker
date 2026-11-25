package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/oarkflow/fh"

	"github.com/oarkflow/broker/cluster"
)

type AdminServer struct {
	Broker      *Broker
	Distributed *DistributedBroker

	// Token is a convenience single-token credential with admin:* access.
	// For production, prefer ACL with named tokens and narrow permissions.
	Token string
	ACL   *ACL

	// InsecureDev explicitly disables authentication. It is intended only for
	// local tests and examples. Production brokerd refuses to start this way
	// unless --insecure-dev is provided.
	InsecureDev bool

	// MaxBodyBytes limits JSON request bodies. Defaults to 1 MiB.
	MaxBodyBytes int64

	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	TLSCertFile       string
	TLSKeyFile        string
	ClientCAFile      string
	RequireClientCert bool

	// AllowedOrigins is the list of allowed CORS origins.
	// Default: same-origin only (""). Set to ["*"] for public APIs.
	AllowedOrigins []string

	Audit AdminAuditLogger
}

var insecureDevWarnedAt atomic.Int64

// warnInsecureDevOnce logs the dev-mode warning at most once a minute. Logging it
// on every request cost a write syscall per call and flooded the log.
func warnInsecureDevOnce() {
	now := time.Now().UnixNano()
	last := insecureDevWarnedAt.Load()
	if last != 0 && now-last < int64(time.Minute) {
		return
	}
	if insecureDevWarnedAt.CompareAndSwap(last, now) {
		log.Printf("[SECURITY WARNING] InsecureDev mode is active - all requests authenticated as admin. Do NOT use in production!")
	}
}

func (a AdminServer) effectiveACL() *ACL {
	if a.ACL != nil {
		return a.ACL
	}
	if a.Token != "" {
		return NewACL(AdminToken{Name: "admin", Token: a.Token, Permissions: []string{PermissionAdminAll}})
	}
	return NewACL()
}

func isMutation(method string) bool {
	return method == fh.MethodPOST || method == fh.MethodPUT || method == fh.MethodPATCH || method == fh.MethodDELETE
}

func (a AdminServer) health(c fh.Ctx) error { return c.SendString("ok\n") }
func (a AdminServer) ready(c fh.Ctx) error {
	if a.Broker == nil || a.Broker.State() != BrokerRunning {
		return httpError(c, "not ready", fh.StatusServiceUnavailable)
	}
	if a.Broker.IsStoreFenced() {
		return httpError(c, "not ready", fh.StatusServiceUnavailable)
	}
	if check := a.Broker.checkStore(c.Context()); check.Status == "fail" {
		return httpError(c, "not ready", fh.StatusServiceUnavailable)
	}
	if a.Distributed != nil {
		if err := a.Distributed.PartitionReadiness(c.Context()); err != nil {
			return httpError(c, "not ready", fh.StatusServiceUnavailable)
		}
	}
	return c.SendString("ready\n")
}
func (a AdminServer) stats(c fh.Ctx) error {
	st, err := a.Broker.store.Stats(c.Context())
	if hasRestrictedTenantScope(c.Context()) {
		// WAL group-commit timing and batch counters are process-wide. Returning
		// them here would expose activity from other tenants through /stats.
		st.WALGroupCommit = WALGroupCommitStats{}
		filteredQueues := make(map[string]int)
		for k, v := range st.Queues {
			if tenantAllowed(c.Context(), k) {
				filteredQueues[k] = v
			}
		}
		st.Queues = filteredQueues

		filteredTopics := make(map[string]int)
		for k, v := range st.Topics {
			if tenantAllowed(c.Context(), k) {
				filteredTopics[k] = v
			}
		}
		st.Topics = filteredTopics

		filteredStreams := make(map[string]int)
		for k, v := range st.Streams {
			if tenantAllowed(c.Context(), k) {
				filteredStreams[k] = v
			}
		}
		st.Streams = filteredStreams

		filteredDLQ := make(map[string]int)
		for k, v := range st.DLQ {
			if tenantAllowed(c.Context(), k) {
				filteredDLQ[k] = v
			}
		}
		st.DLQ = filteredDLQ

		if st.Offsets != nil {
			filteredOffsets := make(map[string]map[string]uint64)
			for k, v := range st.Offsets {
				if tenantAllowed(c.Context(), k) {
					filteredOffsets[k] = v
				}
			}
			st.Offsets = filteredOffsets
		}
	}
	return writeBrokerJSON(c, map[string]any{"broker_state": a.Broker.State(), "store": st, "error": errString(err)})
}
func (a AdminServer) metrics(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global metrics") {
		return nil
	}
	if a.Broker.promMetrics == nil {
		return httpError(c, "prometheus metrics not enabled", 503)
	}
	a.Broker.promMetrics.UpdateGauges()
	st, _ := a.Broker.store.Stats(c.Context())
	a.Broker.promMetrics.UpdateStoreStats(st)
	return a.Broker.promMetrics.PromHandler()(c)
}
func (a AdminServer) lag(c fh.Ctx) error {
	stream := queryString(c, "stream")
	if stream == "" {
		return httpError(c, "stream parameter required", 400)
	}
	report, err := a.Broker.StreamLagReport(c.Context(), stream)
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, report)
}
func (a AdminServer) audit(c fh.Ctx) error {
	if a.Broker.auditLogger == nil {
		return writeBrokerJSON(c, []AuditEvent{})
	}
	history := a.Broker.auditLogger.History(queryLimit(c, 100))
	if hasRestrictedTenantScope(c.Context()) {
		filtered := make([]AuditEvent, 0, len(history))
		for _, ev := range history {
			if ev.TenantID != "" && tenantAllowed(c.Context(), ev.TenantID) {
				filtered = append(filtered, ev)
			}
		}
		return writeBrokerJSON(c, filtered)
	}
	return writeBrokerJSON(c, history)
}
func (a AdminServer) topics(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global topic configuration") {
		return nil
	}
	if c.Method() == fh.MethodPOST {
		var cfg TopicConfig
		if decodeJSON(c, &cfg) != nil {
			return httpError(c, "invalid json", 400)
		}
		return writeBrokerErr(c, a.Broker.DeclareTopic(c.Context(), cfg))
	}
	st, _ := a.Broker.store.Stats(c.Context())
	return writeBrokerJSON(c, st.Topics)
}
func (a AdminServer) queues(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global queue configuration") {
		return nil
	}
	if c.Method() == fh.MethodPOST {
		var cfg QueueConfig
		if decodeJSON(c, &cfg) != nil {
			return httpError(c, "invalid json", 400)
		}
		return writeBrokerErr(c, a.Broker.DeclareQueue(c.Context(), cfg))
	}
	st, _ := a.Broker.store.Stats(c.Context())
	return writeBrokerJSON(c, st.Queues)
}

// identifierParam returns a validated identifier route parameter. On failure it
// writes a 400 naming kind and returns ok=false.
func identifierParam(c fh.Ctx, param, kind string) (string, bool, error) {
	name, err := sanitizeIdentifier(pathParam(c, param))
	if err != nil {
		return "", false, httpError(c, "invalid "+kind+": "+err.Error(), 400)
	}
	return name, true, nil
}

// partitionParam returns the validated :partition route parameter. On failure
// it writes the broker invalid-message error and returns ok=false.
func partitionParam(c fh.Ctx) (int, bool, error) {
	partition, ok := pathPartition(c, "partition")
	if !ok {
		return 0, false, WriteBrokerError(c, ErrInvalidMessage)
	}
	return partition, true, nil
}

// ---- /queues/:queue ----

func (a AdminServer) queueAssignedMessages(c fh.Ctx) error {
	name, ok, err := identifierParam(c, "queue", "queue name")
	if !ok {
		return err
	}
	if rejectScopedControlPlane(c, "assigned queue listings") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	report, err := a.Distributed.ListAssignedQueue(c.Context(), name, queryLimit(c, 100))
	if report == nil {
		return WriteBrokerError(c, err)
	}
	status := fh.StatusOK
	if len(report.Failures) > 0 {
		status = fh.StatusMultiStatus
	}
	return writeBrokerJSONStatus(c, status, report)
}

func (a AdminServer) queueAssignedControl(action string) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		name, ok, err := identifierParam(c, "queue", "queue name")
		if !ok {
			return err
		}
		if rejectScopedControlPlane(c, "assigned queue controls") {
			return nil
		}
		if a.Distributed == nil {
			return WriteBrokerError(c, ErrUnsupportedMode)
		}
		result, err := a.Distributed.ApplyAssignedQueueControl(c.Context(), name, action)
		if result == nil {
			return WriteBrokerError(c, err)
		}
		status := fh.StatusOK
		if len(result.Failures) > 0 {
			status = fh.StatusMultiStatus
		}
		return writeBrokerJSONStatus(c, status, result)
	}
}

// queuePartitionTarget resolves the queue and partition for partition routes.
// Every partition route except the message listing is a control operation that
// tenant-scoped credentials may not perform.
func (a AdminServer) queuePartitionTarget(c fh.Ctx, control bool) (string, int, bool, error) {
	name, ok, err := identifierParam(c, "queue", "queue name")
	if !ok {
		return "", 0, false, err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return "", 0, false, err
	}
	if control && rejectScopedControlPlane(c, "partition queue controls") {
		return "", 0, false, nil
	}
	return name, partition, true, nil
}

func (a AdminServer) queuePartitionMessages(c fh.Ctx) error {
	name, partition, ok, err := a.queuePartitionTarget(c, false)
	if !ok {
		return err
	}
	messages, err := a.Broker.ListPartitionQueue(c.Context(), name, partition, queryLimit(c, 100))
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, messages)
}

func (a AdminServer) queuePartitionPause(c fh.Ctx) error {
	name, partition, ok, err := a.queuePartitionTarget(c, true)
	if !ok {
		return err
	}
	return writeBrokerErr(c, a.Broker.PausePartitionQueue(c.Context(), name, partition))
}

func (a AdminServer) queuePartitionResume(c fh.Ctx) error {
	name, partition, ok, err := a.queuePartitionTarget(c, true)
	if !ok {
		return err
	}
	return writeBrokerErr(c, a.Broker.ResumePartitionQueue(c.Context(), name, partition))
}

func (a AdminServer) queuePartitionPurge(c fh.Ctx) error {
	name, partition, ok, err := a.queuePartitionTarget(c, true)
	if !ok {
		return err
	}
	purged, err := a.Broker.PurgePartitionQueue(c.Context(), name, partition)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"ok": true, "purged": purged, "partition": partition})
}

func (a AdminServer) queueMessages(c fh.Ctx) error {
	name, ok, err := identifierParam(c, "queue", "queue name")
	if !ok {
		return err
	}
	if a.Distributed != nil {
		assigned, err := a.Distributed.hasResourcePartitions(c.Context(), cluster.PartitionResourceQueue, name)
		if err != nil {
			return WriteBrokerError(c, err)
		}
		if assigned {
			return WriteBrokerError(c, fmt.Errorf("%w: partitioned queue messages must be read through partition-specific consumers", ErrUnsupportedMode))
		}
	}
	msgs, err := a.Broker.store.ListQueue(c.Context(), name, queryLimit(c, 100))
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, msgs)
}

func (a AdminServer) queuePull(c fh.Ctx) error {
	name, ok, err := identifierParam(c, "queue", "queue name")
	if !ok {
		return err
	}
	var req struct {
		ConsumerID        string `json:"consumer_id"`
		ConsumerGroup     string `json:"consumer_group"`
		Limit             int    `json:"limit"`
		VisibilityTimeout int64  `json:"visibility_timeout_ns"`
	}
	if c.Method() == fh.MethodPOST {
		_ = decodeJSON(c, &req)
	}
	if req.ConsumerID == "" {
		req.ConsumerID = firstNonEmpty(queryString(c, "consumer"), "admin")
	}
	if req.ConsumerGroup == "" {
		req.ConsumerGroup = firstNonEmpty(queryString(c, "group"), req.ConsumerID)
	}
	if req.Limit <= 0 {
		req.Limit = queryLimit(c, 1)
	}
	vis := time.Duration(req.VisibilityTimeout)
	msgs, err := a.Broker.store.Lease(c.Context(), LeaseRequest{Queue: name, ConsumerID: req.ConsumerID, ConsumerGroup: req.ConsumerGroup, Limit: req.Limit, VisibilityTimeout: vis})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, msgs)
}

// queueControlTarget resolves the queue for a global queue control.
func (a AdminServer) queueControlTarget(c fh.Ctx) (string, bool, error) {
	name, ok, err := identifierParam(c, "queue", "queue name")
	if !ok {
		return "", false, err
	}
	if rejectScopedControlPlane(c, "global queue controls") {
		return "", false, nil
	}
	return name, true, nil
}

func (a AdminServer) queuePause(c fh.Ctx) error {
	name, ok, err := a.queueControlTarget(c)
	if !ok {
		return err
	}
	return writeBrokerErr(c, a.Broker.PauseQueue(c.Context(), name))
}

func (a AdminServer) queueResume(c fh.Ctx) error {
	name, ok, err := a.queueControlTarget(c)
	if !ok {
		return err
	}
	return writeBrokerErr(c, a.Broker.ResumeQueue(c.Context(), name))
}

func (a AdminServer) queuePurge(c fh.Ctx) error {
	name, ok, err := a.queueControlTarget(c)
	if !ok {
		return err
	}
	n, err := a.Broker.PurgeQueue(c.Context(), name)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"ok": true, "purged": n})
}

// ---- /streams/:stream ----

func streamParam(c fh.Ctx) (string, bool, error) {
	return identifierParam(c, "stream", "stream name")
}

func streamGroup(c fh.Ctx) string {
	return firstNonEmpty(queryString(c, "group"), queryString(c, "consumer_group"))
}

func (a AdminServer) streamPartitionLag(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return err
	}
	if rejectScopedControlPlane(c, "partition stream lag reports") {
		return nil
	}
	group := firstNonEmpty(queryString(c, "consumer_group"), queryString(c, "group"))
	report, err := a.Broker.PartitionStreamLag(c.Context(), name, partition, group)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, report)
}

func (a AdminServer) streamLag(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	if rejectScopedControlPlane(c, "global stream lag reports") {
		return nil
	}
	if a.Distributed != nil {
		report, err := a.Distributed.StreamLagAssigned(c.Context(), name, firstNonEmpty(queryString(c, "consumer_group"), queryString(c, "group")))
		if report != nil {
			status := fh.StatusOK
			if len(report.Failures) > 0 || report.Error != "" {
				status = fh.StatusMultiStatus
			}
			return writeBrokerJSONStatus(c, status, report)
		}
		if err != nil && !errors.Is(err, errNoAssignedStreamPartitions) {
			return WriteBrokerError(c, err)
		}
		if err == nil {
			return WriteBrokerError(c, ErrBrokerUnavailable)
		}
	}
	report, err := a.Broker.StreamLagReport(c.Context(), name)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, report)
}

func (a AdminServer) streamPartitionMessages(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return err
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	afterRaw := firstNonEmpty(queryString(c, "after_offset"), queryString(c, "after"))
	after, err := strconv.ParseUint(afterRaw, 10, 64)
	if afterRaw != "" && err != nil {
		return WriteBrokerError(c, errf(ErrInvalidMessage, "invalid stream partition offset"))
	}
	messages, err := a.Distributed.ReadPartitionStream(c.Context(), name, partition, after, queryLimit(c, 100))
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, messages)
}

func (a AdminServer) streamPartitionCommitAndAppend(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return err
	}
	var req struct {
		ConsumerGroup string        `json:"consumer_group"`
		Offset        uint64        `json:"offset"`
		TenantID      string        `json:"tenant_id,omitempty"`
		TransactionID string        `json:"transaction_id,omitempty"`
		Entries       []AppendEntry `json:"entries"`
	}
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	if len(req.Entries) > 0 && !a.effectiveACL().Authorize(principalFromContext(c.Context()), PermissionPublish) {
		return httpError(c, "forbidden", fh.StatusForbidden)
	}
	commit := OffsetCommit{Stream: name, ConsumerGroup: req.ConsumerGroup, Offset: req.Offset, TenantID: req.TenantID, TransactionID: req.TransactionID, Partition: partition, Partitioned: true}
	messages, err := a.Broker.CommitOffsetAndAppend(c.Context(), commit, req.Entries)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, messages)
}

func (a AdminServer) streamPartitionCommit(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return err
	}
	var req OffsetCommit
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	req.Stream, req.Partition, req.Partitioned = name, partition, true
	return writeBrokerErr(c, a.Broker.CommitOffset(c.Context(), req))
}

func (a AdminServer) streamPartitionOffset(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	partition, ok, err := partitionParam(c)
	if !ok {
		return err
	}
	group := streamGroup(c)
	off, err := a.Broker.ConsumerOffset(c.Context(), OffsetCommit{Stream: name, ConsumerGroup: group, Partition: partition, Partitioned: true})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"stream": name, "group": group, "partition": partition, "offset": off})
}

func (a AdminServer) streamMessages(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	after, _ := strconv.ParseUint(firstNonEmpty(queryString(c, "after"), queryString(c, "after_offset")), 10, 64)
	msgs, err := a.Broker.ReadStream(c.Context(), StreamReadRequest{Stream: name, ConsumerGroup: streamGroup(c), AfterOffset: after, Limit: queryLimit(c, 100)})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, msgs)
}

func (a AdminServer) streamCommit(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	var req OffsetCommit
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	req.Stream = name
	return writeBrokerErr(c, a.Broker.CommitOffset(c.Context(), req))
}

func (a AdminServer) streamOffset(c fh.Ctx) error {
	name, ok, err := streamParam(c)
	if !ok {
		return err
	}
	group := streamGroup(c)
	off, err := a.Broker.ConsumerOffset(c.Context(), OffsetCommit{Stream: name, ConsumerGroup: group})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"stream": name, "group": group, "offset": off})
}

// ---- /dlq/:queue ----

func (a AdminServer) dlqMessages(c fh.Ctx) error {
	msgs, err := a.Broker.store.ListDLQ(c.Context(), pathParam(c, "queue"), queryLimit(c, 100))
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, msgs)
}

func (a AdminServer) dlqReplay(c fh.Ctx) error {
	name := pathParam(c, "queue")
	dest := queryString(c, "queue")
	if dest == "" {
		dest = strings.TrimSuffix(name, ".dlq")
	}
	res, err := a.Broker.store.Move(c.Context(), MoveRequest{SourceType: DestinationQueue, Source: name, DestinationType: DestinationQueue, Destination: dest, Mode: MoveReplay, ResetAttempts: true, ResetError: true, ResetLease: true, Reason: "dlq replay", Actor: "admin"})
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, res)
}

func (a AdminServer) dlqReplayAllRoute(c fh.Ctx) error {
	return a.dlqReplayAll(c, pathParam(c, "queue"))
}
func (a AdminServer) dlqErrorsRoute(c fh.Ctx) error  { return a.dlqErrors(c, pathParam(c, "queue")) }
func (a AdminServer) dlqArchiveRoute(c fh.Ctx) error { return a.dlqArchive(c, pathParam(c, "queue")) }
func (a AdminServer) dlqExportRoute(c fh.Ctx) error  { return a.dlqExport(c, pathParam(c, "queue")) }

// ---- /messages/:id ----

func messageIDParam(c fh.Ctx) (string, bool, error) {
	return identifierParam(c, "id", "message id")
}

func (a AdminServer) messageHistory(c fh.Ctx) error {
	id, ok, err := messageIDParam(c)
	if !ok {
		return err
	}
	h, err := a.Broker.store.History(c.Context(), id)
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, h)
}

func (a AdminServer) messageAck(c fh.Ctx) error {
	id, ok, err := messageIDParam(c)
	if !ok {
		return err
	}
	var req AckRequest
	_ = decodeJSON(c, &req)
	req.MessageID = id
	return writeBrokerErr(c, a.Broker.store.Ack(c.Context(), req))
}

func (a AdminServer) messageNack(c fh.Ctx) error {
	id, ok, err := messageIDParam(c)
	if !ok {
		return err
	}
	var req NackRequest
	_ = decodeJSON(c, &req)
	req.MessageID = id
	if req.Action == "" {
		req.Action = NackRequeue
	}
	return writeBrokerErr(c, a.Broker.store.Nack(c.Context(), req))
}

func (a AdminServer) messageGet(c fh.Ctx) error {
	id, ok, err := messageIDParam(c)
	if !ok {
		return err
	}
	m, found, err := a.Broker.store.Get(c.Context(), id)
	if err != nil {
		return httpError(c, err.Error(), 500)
	}
	if !found {
		return notFound(c)
	}
	return writeBrokerJSON(c, m)
}

// ackBatch acks many messages (possibly across queues/consumers) in one HTTP
// call, amortizing per-request network and JSON overhead for consumers.
func (a AdminServer) ackBatch(c fh.Ctx) error {
	var reqs []AckRequest
	if decodeJSON(c, &reqs) != nil {
		return httpError(c, "invalid json", 400)
	}
	var batchErrs []error
	if store, ok := a.Broker.store.(BatchAckStore); ok {
		batchErrs = store.AckBatch(c.Context(), reqs)
	} else {
		batchErrs = make([]error, len(reqs))
		for i := range reqs {
			batchErrs[i] = a.Broker.store.Ack(c.Context(), reqs[i])
		}
	}
	errs := make([]string, len(reqs))
	acked := 0
	for i, err := range batchErrs {
		if err != nil {
			errs[i] = err.Error()
			continue
		}
		acked++
		a.Broker.Consumers().AddStats(reqs[i].ConsumerID, 0, 1, 0, 0, "")
	}
	if acked == len(reqs) && preferMinimal(c) {
		return c.SendStatus(fh.StatusNoContent)
	}
	if acked != len(reqs) {
		c.Set("Content-Type", "application/json")
		c.Status(fh.StatusMultiStatus)
	}
	return writeBrokerJSON(c, map[string]any{"acked": acked, "errors": errs})
}
func (a AdminServer) streams(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global stream configuration") {
		return nil
	}
	if c.Method() == fh.MethodPOST {
		var cfg StreamConfig
		if decodeJSON(c, &cfg) != nil {
			return httpError(c, "invalid json", 400)
		}
		return writeBrokerErr(c, a.Broker.DeclareStream(c.Context(), cfg))
	}
	st, _ := a.Broker.store.Stats(c.Context())
	return writeBrokerJSON(c, map[string]any{"streams": st.Streams, "offsets": st.Offsets})
}
func (a AdminServer) dlq(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global dead-letter queues") {
		return nil
	}
	st, _ := a.Broker.store.Stats(c.Context())
	return writeBrokerJSON(c, st.DLQ)
}
func (a AdminServer) publish(c fh.Ctx) error {
	var req struct {
		Topic   string            `json:"topic"`
		Queue   string            `json:"queue"`
		Stream  string            `json:"stream"`
		Type    string            `json:"type"`
		Payload json.RawMessage   `json:"payload"`
		Headers map[string]string `json:"headers"`
	}
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	opts := []PublishOption{WithHeaders(req.Headers), WithType(req.Type)}
	if req.Queue != "" {
		opts = append(opts, WithQueue(req.Queue))
	}
	if req.Stream != "" {
		opts = append(opts, WithStream(req.Stream))
	}
	res, err := a.Broker.Publisher().Publish(c.Context(), req.Topic, []byte(req.Payload), opts...)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, res)
}

// publishBatch accepts an array of publish requests (each may target a
// different topic/queue/stream) in one HTTP call, amortizing per-request
// network and JSON overhead across many messages.
func (a AdminServer) publishBatch(c fh.Ctx) error {
	var items []struct {
		Topic   string            `json:"topic"`
		Queue   string            `json:"queue"`
		Stream  string            `json:"stream"`
		Type    string            `json:"type"`
		Payload json.RawMessage   `json:"payload"`
		Headers map[string]string `json:"headers"`
	}
	if decodeJSON(c, &items) != nil {
		return httpError(c, "invalid json", 400)
	}
	pub := a.Broker.Publisher()
	if preferMinimal(c) {
		messages := make([]Message, len(items))
		for i, req := range items {
			messages[i] = Message{Topic: req.Topic, Queue: req.Queue, Stream: req.Stream, Type: req.Type, Payload: []byte(req.Payload), Headers: req.Headers}
		}
		errs, first := pub.publishMessagesMinimal(c.Context(), messages)
		return writeMinimalPublishResponse(c, errs, first)
	}
	batchItems := make([]PublishBatchItem, len(items))
	for i, req := range items {
		opts := []PublishOption{WithHeaders(req.Headers), WithType(req.Type)}
		if req.Queue != "" {
			opts = append(opts, WithQueue(req.Queue))
		}
		if req.Stream != "" {
			opts = append(opts, WithStream(req.Stream))
		}
		batchItems[i] = PublishBatchItem{Topic: req.Topic, Payload: []byte(req.Payload), Options: opts}
	}
	results, publishErrs, firstErr := pub.publishItemsAligned(c.Context(), batchItems)
	errs := make([]string, len(publishErrs))
	failed := false
	for i, err := range publishErrs {
		if err != nil {
			failed = true
			errs[i] = err.Error()
		}
	}
	if firstErr != nil && !failed {
		return WriteBrokerError(c, firstErr)
	}
	if failed {
		c.Set("Content-Type", "application/json")
		c.Status(fh.StatusMultiStatus)
	}
	return writeBrokerJSON(c, map[string]any{"results": results, "errors": errs})
}
func (a AdminServer) move(c fh.Ctx) error {
	var req MoveRequest
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	res, err := a.Broker.store.Move(c.Context(), req)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, res)
}
func (a AdminServer) notifications(c fh.Ctx) error {
	return writeBrokerJSON(c, visibleNotifications(c.Context(), a.Broker.notify.History(queryLimit(c, 100))))
}
func (a AdminServer) events(c fh.Ctx) error {
	// Capture the request context before streaming: the subscription callback
	// runs on the notification bus goroutine and must not touch the pooled Ctx.
	reqCtx := c.Context()
	limit := queryLimit(c, 25)
	return a.streamNotifications(c, reqCtx, limit)
}

// streamNotifications streams tenant-visible notifications as Server-Sent
// Events until the client disconnects.
func (a AdminServer) streamNotifications(c fh.Ctx, reqCtx context.Context, historyLimit int) error {
	id := "sse-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ch := make(chan Notification, 128)
	a.Broker.Notifications().Subscribe(id, CallbackFunc(func(ctx context.Context, n Notification) error {
		if !tenantAllowed(reqCtx, n.TenantID) {
			return nil
		}
		select {
		case ch <- n:
		default:
		}
		return nil
	}))
	defer a.Broker.Notifications().Unsubscribe(id)
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	return c.Stream(func(sw *fh.StreamWriter) error {
		for _, n := range a.Broker.Notifications().History(historyLimit) {
			if tenantAllowed(reqCtx, n.TenantID) {
				if err := writeSSE(sw, n); err != nil {
					return err
				}
			}
		}
		if err := sw.Flush(); err != nil {
			return err
		}
		// Comment heartbeats let a write fail promptly after a client leaves.
		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case n := <-ch:
				if err := writeSSE(sw, n); err != nil {
					return err
				}
			case <-heartbeat.C:
				if _, err := io.WriteString(sw, ": ping\n\n"); err != nil {
					return err
				}
			case <-reqCtx.Done():
				return nil
			}
			if err := sw.Flush(); err != nil {
				return err
			}
		}
	})
}
func (a AdminServer) routes(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global routes") {
		return nil
	}
	if c.Method() == fh.MethodPOST {
		var route Route
		if decodeJSON(c, &route) != nil {
			return httpError(c, "invalid json", 400)
		}
		a.Broker.AddRoute(route)
		return writeBrokerJSON(c, map[string]bool{"ok": true})
	}
	return writeBrokerJSON(c, a.Broker.router.Routes())
}
func rejectScopedControlPlane(c fh.Ctx, resource string) bool {
	if !hasRestrictedTenantScope(c.Context()) {
		return false
	}
	httpError(c, "tenant-scoped credentials cannot access "+resource, fh.StatusForbidden)
	return true
}
func visibleNotifications(ctx context.Context, history []Notification) []Notification {
	if !hasRestrictedTenantScope(ctx) {
		return history
	}
	out := make([]Notification, 0, len(history))
	for _, n := range history {
		if tenantAllowed(ctx, n.TenantID) {
			out = append(out, n)
		}
	}
	return out
}
func writeSSE(w io.Writer, n Notification) error {
	b, _ := json.Marshal(n)
	_, err := fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", n.Type, n.ID, b)
	return err
}
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sanitizeIdentifier validates that an identifier contains only safe characters.
// Returns the identifier if valid, or an error if invalid.
func sanitizeIdentifier(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	if len(s) > 256 {
		return "", fmt.Errorf("identifier too long: %d > 256", len(s))
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return "", fmt.Errorf("identifier contains invalid character: %d", c)
		}
		if c == '/' || c == '\\' || c == '\x00' {
			return "", fmt.Errorf("identifier contains forbidden character: %c", c)
		}
	}
	return s, nil
}

func (a AdminServer) clusterPartitionsList(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster partitions")
	if !ok {
		return err
	}
	partitions, err := cq.ListPartitionsLinearizable(c.Context())
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partitions)
}

func (a AdminServer) clusterPartitionsAssign(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster partitions")
	if !ok {
		return err
	}
	var body struct {
		Topic        string                        `json:"topic,omitempty"`
		Name         string                        `json:"name,omitempty"`
		ResourceKind cluster.PartitionResourceKind `json:"resource_kind,omitempty"`
		Partition    int                           `json:"partition"`
		Replicas     []string                      `json:"replicas"`
		Leader       string                        `json:"leader"`
		AutoAssign   bool                          `json:"auto_assign,omitempty"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	name := firstNonEmpty(body.Name, body.Topic)
	kind := body.ResourceKind
	if kind == "" {
		kind = cluster.PartitionResourceTopic
	}
	var partition *cluster.PartitionMetadata
	if body.AutoAssign {
		if len(body.Replicas) != 0 || body.Leader != "" {
			return WriteBrokerError(c, fmt.Errorf("auto_assign cannot be combined with explicit replicas or leader"))
		}
		partition, err = a.Distributed.AssignResourcePartitionBalanced(c.Context(), kind, name, body.Partition)
	} else {
		err = a.Distributed.AssignResourcePartition(c.Context(), kind, name, body.Partition, body.Replicas, body.Leader)
		if err == nil {
			partition, err = cq.GetResourcePartitionLinearizable(c.Context(), kind, name, body.Partition)
		}
	}
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partition)
}

// clusterPartitionTarget identifies the partition addressed by
// /cluster/partitions/:name/:partition routes.
type clusterPartitionTarget struct {
	cq        *cluster.ControllerQuorum
	kind      cluster.PartitionResourceKind
	name      string
	partition int
}

// resolveClusterPartition validates the route and returns ok=false after
// writing a response when the request cannot proceed.
func (a AdminServer) resolveClusterPartition(c fh.Ctx) (clusterPartitionTarget, bool, error) {
	if rejectScopedControlPlane(c, "cluster partitions") {
		return clusterPartitionTarget{}, false, nil
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		return clusterPartitionTarget{}, false, WriteBrokerError(c, ErrUnsupportedMode)
	}
	name := pathParam(c, "name")
	partition, ok := pathPartition(c, "partition")
	if name == "" || !ok {
		return clusterPartitionTarget{}, false, WriteBrokerError(c, ErrInvalidMessage)
	}
	kind := cluster.PartitionResourceKind(queryString(c, "resource_kind"))
	if kind == "" {
		kind = cluster.PartitionResourceTopic
	}
	if kind != cluster.PartitionResourceTopic && kind != cluster.PartitionResourceStream && kind != cluster.PartitionResourceQueue {
		return clusterPartitionTarget{}, false, WriteBrokerError(c, ErrInvalidMessage)
	}
	return clusterPartitionTarget{cq: a.Distributed.ControllerQuorum(), kind: kind, name: name, partition: partition}, true, nil
}

func (a AdminServer) clusterPartitionGet(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	partition, err := t.cq.GetResourcePartitionLinearizable(c.Context(), t.kind, t.name, t.partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partition)
}

func (a AdminServer) clusterPartitionUpdateISR(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	var body struct {
		ISR []string `json:"isr"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	if _, err := a.Distributed.UpdateResourcePartitionISR(c.Context(), t.kind, t.name, t.partition, body.ISR); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "updated", "name": t.name, "resource_kind": t.kind, "partition": t.partition})
}

func (a AdminServer) clusterPartitionISR(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	partitionGroupID := (cluster.PartitionGroupID{Topic: t.name, Partition: t.partition, Kind: t.kind}).Canonical()
	group, found := a.Distributed.PartitionGroup(partitionGroupID)
	if !found {
		return WriteBrokerError(c, ErrBrokerUnavailable)
	}
	if !group.IsLeader() {
		return WriteBrokerError(c, ErrNotLeader)
	}
	return writeBrokerJSON(c, map[string]any{
		"id":           partitionGroupID,
		"leader_id":    group.LeaderID(),
		"leader_epoch": group.LeaderEpoch(),
		"isr":          group.ISR(),
		"replicas":     group.Replicas(),
	})
}

func (a AdminServer) clusterPartitionRoute(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	route, err := a.Distributed.PartitionResourceRoute(c.Context(), t.kind, t.name, t.partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, route)
}

func (a AdminServer) clusterPartitionReassignmentPlan(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	replicas := strings.Split(queryString(c, "replicas"), ",")
	if len(replicas) == 1 && replicas[0] == "" {
		replicas = nil
	}
	for i := range replicas {
		replicas[i] = strings.TrimSpace(replicas[i])
	}
	plan, err := a.Distributed.PlanResourcePartitionReassignment(c.Context(), t.kind, t.name, t.partition, replicas, queryString(c, "leader"))
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, plan)
}

func (a AdminServer) clusterPartitionReassign(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	var body struct {
		Replicas []string `json:"replicas"`
		Leader   string   `json:"leader,omitempty"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	progress, err := a.Distributed.ReassignResourcePartition(c.Context(), t.kind, t.name, t.partition, body.Replicas, body.Leader)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, progress)
}

func (a AdminServer) clusterPartitionCancelReassignment(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	progress, err := a.Distributed.CancelResourcePartitionReassignment(c.Context(), t.kind, t.name, t.partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, progress)
}

func (a AdminServer) clusterPartitionPlacementPlan(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	plan, err := a.Distributed.PlanResourcePartitionPlacement(c.Context(), t.kind, t.name, t.partition)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, plan)
}

func (a AdminServer) clusterPartitionEpoch(c fh.Ctx) error {
	t, ok, err := a.resolveClusterPartition(c)
	if !ok {
		return err
	}
	var body struct {
		Leader string `json:"leader,omitempty"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	if body.Leader == "" {
		return a.writeControllerError(c, cluster.ErrPartitionTransferRequired)
	}
	partition, err := a.Distributed.TransferResourcePartitionLeader(c.Context(), t.kind, t.name, t.partition, body.Leader)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, partition)
}

func (a AdminServer) writeControllerError(c fh.Ctx, err error) error {
	switch {
	case errors.Is(err, cluster.ErrNotLeader), errors.Is(err, cluster.ErrLeaderLost):
		var retry *RetryError
		if errors.As(err, &retry) && retry.Class == RetryNotLeader {
			// Preserve a resource-specific redirect. Replacing it with the
			// controller leader can route partition operations to the wrong group.
			break
		}
		if a.Distributed != nil {
			err = NotLeaderError(a.Distributed.controllerLeaderClientAddress(), a.Distributed.raftTerm())
		} else {
			err = ErrNotLeader
		}
	case errors.Is(err, cluster.ErrQuorumLost), errors.Is(err, cluster.ErrQuorumNotMet), errors.Is(err, cluster.ErrPartitioned):
		err = fmt.Errorf("%w: %v", ErrQuorumUnavailable, err)
	case errors.Is(err, cluster.ErrNodeNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterNodeNotFound, err)
	case errors.Is(err, cluster.ErrClosed):
		err = fmt.Errorf("%w: %v", ErrBrokerUnavailable, err)
	case errors.Is(err, cluster.ErrConfChangePending):
		err = fmt.Errorf("%w: %v", ErrClusterChangePending, err)
	case errors.Is(err, cluster.ErrStaleEpoch), errors.Is(err, cluster.ErrTermMismatch):
		err = fmt.Errorf("%w: %v", ErrStaleEpoch, err)
	case errors.Is(err, cluster.ErrUnknownReplica):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrPartitionNotFound):
		err = fmt.Errorf("%w: %v", ErrPartitionNotFound, err)
	case errors.Is(err, cluster.ErrSchemaNotFound):
		err = fmt.Errorf("%w: %v", ErrSchemaNotFound, err)
	case errors.Is(err, cluster.ErrInvalidSchema):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrACLPrincipalNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterACLPrincipalNotFound, err)
	case errors.Is(err, cluster.ErrTransactionNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterTransactionNotFound, err)
	case errors.Is(err, cluster.ErrInvalidTransaction):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidACLPrincipal):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrNodeInUse):
		err = fmt.Errorf("%w: %v", ErrClusterNodeInUse, err)
	case errors.Is(err, cluster.ErrNodeUnavailable):
		err = fmt.Errorf("%w: %v", ErrClusterNodeUnavailable, err)
	case errors.Is(err, cluster.ErrPartitionTransferRequired):
		err = fmt.Errorf("%w: %v", ErrPartitionTransferRequired, err)
	case errors.Is(err, cluster.ErrReplicaLagging):
		err = fmt.Errorf("%w: %v", ErrClusterNodeUnavailable, err)
	case errors.Is(err, cluster.ErrPartitionReassignmentRequired):
		err = fmt.Errorf("%w: %v", ErrPartitionReassignmentRequired, err)
	case errors.Is(err, cluster.ErrInvalidPartitionAssignment), errors.Is(err, cluster.ErrInvalidISR):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeState):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeClientAddress):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeFailureDomain):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeCapacityWeight):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeBrokerVersion):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, context.DeadlineExceeded):
		err = fmt.Errorf("%w: %v", ErrRequestTimeout, err)
	case errors.Is(err, context.Canceled):
		err = fmt.Errorf("%w: %v", ErrCancelled, err)
	}
	return WriteBrokerError(c, err)
}

func (a AdminServer) isAllowedOrigin(origin string) bool {
	if len(a.AllowedOrigins) == 0 {
		// Default: same-origin (no CORS header = browser default same-origin policy)
		return false
	}
	for _, allowed := range a.AllowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

func (a AdminServer) clusterStatus(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster topology") {
		return nil
	}
	if c.Method() != fh.MethodGET {
		return httpError(c, "method not allowed", fh.StatusMethodNotAllowed)
	}
	resp := map[string]any{
		"mode":    string(a.Broker.cfg.Mode),
		"node_id": a.Broker.cfg.NodeID,
	}
	if a.Broker.Cluster() != nil {
		resp["nodes"] = a.Broker.Cluster().Nodes()
		resp["assignments"] = a.Broker.Cluster().Assignments()
	}
	if a.Distributed != nil && a.Distributed.raftCtrl != nil {
		resp["raft"] = distributedRaftStatus(a.Distributed)
		resp["raft_groups"] = distributedRaftGroupStatuses(a.Distributed)
		resp["local_partition_progress"] = a.Distributed.LivePartitionProgress()
	}
	if a.Distributed != nil && a.Distributed.ControllerQuorum() != nil {
		stats, err := a.Distributed.ControllerQuorum().StatsLinearizable(c.Context())
		if err != nil {
			resp["controller_metadata_read_error"] = err.Error()
		} else {
			resp["controller_metadata"] = stats
		}
	}
	return writeBrokerJSON(c, resp)
}

func (a AdminServer) clusterTransactionPrune(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster transactions") {
		return nil
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var body struct {
		Before time.Time `json:"before"`
		Limit  int       `json:"limit,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(c.Body()))
	decoder.DisallowUnknownFields()
	writeDecodeError := func(err error) error {
		if requestBodyTooLarge(err) {
			return WriteBrokerError(c, err)
		}
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if err := decoder.Decode(&body); err != nil {
		return writeDecodeError(err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return writeDecodeError(err)
	}
	if body.Before.IsZero() || body.Limit < 0 {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	pruned, err := a.Distributed.ControllerQuorum().PruneResolvedTransactions(c.Context(), body.Before, body.Limit)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"pruned": pruned})
}

// clusterTransaction returns one quorum-confirmed coordinator record. It is
// keyed by query parameter because transaction IDs may contain path separators.
func (a AdminServer) clusterTransaction(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster transactions") {
		return nil
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	id := queryString(c, "id")
	cq := a.Distributed.ControllerQuorum()
	if id != "" {
		if len(id) > cluster.MaxTransactionIDBytes || !utf8.ValidString(id) {
			return WriteBrokerError(c, ErrInvalidMessage)
		}
		transaction, err := cq.GetTransactionLinearizable(c.Context(), id)
		if err != nil {
			return a.writeControllerError(c, err)
		}
		return writeBrokerJSON(c, transaction)
	}
	limit := cluster.DefaultTransactionPageSize
	if rawLimit := queryString(c, "limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 0 {
			return WriteBrokerError(c, ErrInvalidMessage)
		}
		limit = parsed
	}
	transactions, next, err := cq.ListTransactionsPageLinearizable(c.Context(), queryString(c, "after"), limit)
	if err != nil {
		if errors.Is(err, cluster.ErrInvalidTransaction) {
			return WriteBrokerError(c, ErrInvalidMessage)
		}
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"transactions": transactions, "next_cursor": next})
}

func (a AdminServer) clusterUpgradePlan(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster upgrade planning") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	plan, err := a.Distributed.PlanClusterUpgrade(c.Context(), queryString(c, "target_version"))
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, plan)
}

// distributedRaftGroupStatuses exposes bounded local progress for application
// groups beside the controller group's status. Group IDs are returned in
// stable order so operators can diff snapshots without map-order noise.
func distributedRaftGroupStatuses(db *DistributedBroker) []map[string]any {
	groupIDs := db.RaftGroupIDs()
	groups := make([]map[string]any, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		controller, ok := db.RaftGroup(groupID)
		if !ok {
			continue
		}
		status := controller.Status()
		commitIndex := status.HardState.GetCommit()
		memberIDs := make(map[uint64]struct{})
		voters := make(map[uint64]struct{})
		for _, voterGroup := range status.Config.Voters {
			for id := range voterGroup {
				memberIDs[id] = struct{}{}
				voters[id] = struct{}{}
			}
		}
		for id := range status.Config.Learners {
			memberIDs[id] = struct{}{}
		}
		for id := range status.Config.LearnersNext {
			memberIDs[id] = struct{}{}
		}
		jointPending := status.Config.AutoLeave || (len(status.Config.Voters) > 1 && len(status.Config.Voters[1]) != 0)
		ids := make([]uint64, 0, len(memberIDs))
		for id := range memberIDs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		members := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			address, _ := controller.PeerAddress(id)
			member := map[string]any{"peer_id": id, "address": address}
			_, member["voter"] = voters[id]
			_, member["learner"] = status.Config.Learners[id]
			_, member["learner_next"] = status.Config.LearnersNext[id]
			if progress, ok := status.Progress[id]; ok {
				lag := uint64(0)
				if commitIndex > progress.Match {
					lag = commitIndex - progress.Match
				}
				member["match_index"] = progress.Match
				member["next_index"] = progress.Next
				member["lag"] = lag
			}
			members = append(members, member)
		}
		groupInfo := map[string]any{
			"group_id":      groupID,
			"leader_id":     controller.LeaderID(),
			"is_leader":     controller.IsLeader(),
			"term":          status.HardState.GetTerm(),
			"commit_index":  commitIndex,
			"applied_index": status.Applied,
			"joint_pending": jointPending,
			"members":       members,
		}
		if strings.HasPrefix(groupID, "partition/") {
			var partitionID cluster.PartitionGroupID
			if err := partitionID.UnmarshalText([]byte(strings.TrimPrefix(groupID, "partition/"))); err == nil {
				if partitionGroup, ok := db.PartitionGroup(partitionID); ok {
					waits, failures, duration := partitionGroup.MinISRWaitStats()
					groupInfo["replication_factor"] = partitionGroup.ReplicationFactor()
					groupInfo["minimum_isr"] = partitionGroup.MinimumISR()
					groupInfo["live_isr"] = partitionGroup.ISR()
					groupInfo["min_isr_wait_metrics_scope"] = "this_node"
					groupInfo["min_isr_wait_count"] = waits
					groupInfo["min_isr_wait_failures"] = failures
					groupInfo["min_isr_wait_duration_ms"] = float64(duration) / float64(time.Millisecond)
				}
			}
		}
		groups = append(groups, groupInfo)
	}
	return groups
}

func distributedRaftStatus(db *DistributedBroker) map[string]any {
	status := db.raftCtrl.Status()
	commitIndex := status.HardState.GetCommit()
	term := status.HardState.GetTerm()

	voterGroups := make([][]uint64, 0, len(status.Config.Voters))
	voterIDs := make(map[uint64]struct{})
	for _, group := range status.Config.Voters {
		ids := make([]uint64, 0, len(group))
		for id := range group {
			ids = append(ids, id)
			voterIDs[id] = struct{}{}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > 0 {
			voterGroups = append(voterGroups, ids)
		}
	}
	learnerIDs := sortedRaftIDs(status.Config.Learners)
	learnerNextIDs := sortedRaftIDs(status.Config.LearnersNext)
	memberIDs := make(map[uint64]struct{})
	for id := range voterIDs {
		memberIDs[id] = struct{}{}
	}
	for _, id := range learnerIDs {
		memberIDs[id] = struct{}{}
	}
	for _, id := range learnerNextIDs {
		memberIDs[id] = struct{}{}
	}
	ids := make([]uint64, 0, len(memberIDs))
	for id := range memberIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	learners := make(map[uint64]struct{}, len(learnerIDs))
	for _, id := range learnerIDs {
		learners[id] = struct{}{}
	}
	learnersNext := make(map[uint64]struct{}, len(learnerNextIDs))
	for _, id := range learnerNextIDs {
		learnersNext[id] = struct{}{}
	}
	members := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		address, _ := db.raftCtrl.PeerAddress(id)
		member := map[string]any{"peer_id": id, "address": address}
		_, member["voter"] = voterIDs[id]
		_, member["learner"] = learners[id]
		_, member["learner_next"] = learnersNext[id]
		if progress, ok := status.Progress[id]; ok {
			lag := uint64(0)
			if commitIndex > progress.Match {
				lag = commitIndex - progress.Match
			}
			member["match_index"] = progress.Match
			member["next_index"] = progress.Next
			member["lag"] = lag
		}
		members = append(members, member)
	}
	return map[string]any{
		"state":         db.RaftState(),
		"leader":        db.Leader(),
		"leader_id":     db.raftCtrl.LeaderID(),
		"is_leader":     db.IsLeader(),
		"term":          term,
		"commit_index":  commitIndex,
		"applied_index": status.Applied,
		"auto_leave":    status.Config.AutoLeave,
		"voter_groups":  voterGroups,
		"learners":      learnerIDs,
		"learners_next": learnerNextIDs,
		"members":       members,
	}
}

func sortedRaftIDs(ids map[uint64]struct{}) []uint64 {
	result := make([]uint64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (a AdminServer) clusterNodes(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster membership") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var body struct {
		Address string `json:"address"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	peerID, err := a.Distributed.AddRaftLearner(c.Context(), body.Address)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	role := a.Distributed.RaftPeerRole(peerID)
	result := "learner_added"
	if role != "learner" {
		result = "already_member"
	}
	return writeBrokerJSON(c, map[string]any{"status": result, "peer_id": peerID, "role": role})
}

// controllerQuorum returns the controller quorum for a metadata route, or
// ok=false after writing a response when the route cannot be served.
func (a AdminServer) controllerQuorum(c fh.Ctx, resource string) (*cluster.ControllerQuorum, bool, error) {
	if rejectScopedControlPlane(c, resource) {
		return nil, false, nil
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		return nil, false, WriteBrokerError(c, ErrUnsupportedMode)
	}
	return a.Distributed.ControllerQuorum(), true, nil
}

// clusterMetadataNodesList lists the replicated logical node registry. These
// records are controller metadata and are distinct from Raft voter membership.
func (a AdminServer) clusterMetadataNodesList(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata nodes")
	if !ok {
		return err
	}
	nodes, err := cq.GetNodesLinearizable(c.Context())
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, nodes)
}

func (a AdminServer) clusterMetadataNodesRegister(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata nodes")
	if !ok {
		return err
	}
	var body struct {
		ID            string            `json:"id"`
		Address       string            `json:"address"`
		ClientAddress string            `json:"client_address,omitempty"`
		State         string            `json:"state,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	id, err := sanitizeIdentifier(body.ID)
	if err != nil || strings.TrimSpace(body.Address) == "" || len(body.Address) > 1024 {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	node := &cluster.NodeInfo{
		ID: id, Address: strings.TrimSpace(body.Address), ClientAddress: strings.TrimSpace(body.ClientAddress),
		State: cluster.NodeState(strings.TrimSpace(body.State)), Metadata: body.Metadata,
	}
	if err := cq.RegisterNode(c.Context(), node); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "registered", "id": id})
}

func (a AdminServer) clusterMetadataNodeDeregister(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata nodes")
	if !ok {
		return err
	}
	id, err := sanitizeIdentifier(pathParam(c, "node"))
	if err != nil {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if err := cq.DeregisterNode(c.Context(), id); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "deregistered", "id": id})
}

func (a AdminServer) clusterMetadataSchemasList(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata schemas")
	if !ok {
		return err
	}
	name := queryString(c, "name")
	if name != "" {
		validated, err := sanitizeIdentifier(name)
		if err != nil {
			return WriteBrokerError(c, ErrInvalidMessage)
		}
		name = validated
	}
	schemas, err := cq.ListSchemasLinearizable(c.Context(), name)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, schemas)
}

func (a AdminServer) clusterMetadataSchemasRegister(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata schemas")
	if !ok {
		return err
	}
	var definition SchemaDefinition
	if err := decodeJSON(c, &definition); err != nil {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if err := cq.RegisterSchema(c.Context(), cluster.SchemaDefinition{
		Name: definition.Name, Version: definition.Version, ContentType: definition.ContentType,
		Required: definition.Required, AllowUnknown: definition.AllowUnknown,
		CompatibleWith: definition.CompatibleWith, Deprecated: definition.Deprecated,
		Description: definition.Description,
	}); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "registered", "name": definition.Name, "version": firstNonEmpty(definition.Version, "v1")})
}

// schemaTarget validates the :name/:version schema route parameters.
func (a AdminServer) schemaTarget(c fh.Ctx) (*cluster.ControllerQuorum, string, string, bool, error) {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata schemas")
	if !ok {
		return nil, "", "", false, err
	}
	name, nameErr := sanitizeIdentifier(pathParam(c, "name"))
	version, versionErr := sanitizeIdentifier(pathParam(c, "version"))
	if nameErr != nil || versionErr != nil {
		return nil, "", "", false, WriteBrokerError(c, ErrInvalidMessage)
	}
	return cq, name, version, true, nil
}

func (a AdminServer) clusterMetadataSchema(c fh.Ctx) error {
	cq, name, version, ok, err := a.schemaTarget(c)
	if !ok {
		return err
	}
	definition, err := cq.GetSchemaLinearizable(c.Context(), name, version)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, definition)
}

func (a AdminServer) clusterMetadataSchemaDeprecate(c fh.Ctx) error {
	cq, name, version, ok, err := a.schemaTarget(c)
	if !ok {
		return err
	}
	if err := cq.DeprecateSchema(c.Context(), name, version); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "deprecated", "name": name, "version": version})
}

type clusterACLPrincipalView struct {
	Name                  string    `json:"name"`
	CertificateIdentities []string  `json:"certificate_identities,omitempty"`
	Permissions           []string  `json:"permissions,omitempty"`
	Tenants               []string  `json:"tenants,omitempty"`
	RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
	Burst                 int       `json:"burst,omitempty"`
	MaxConcurrent         int       `json:"max_concurrent,omitempty"`
	ExpiresAt             time.Time `json:"expires_at,omitempty"`
	Revoked               bool      `json:"revoked,omitempty"`
	UpdatedAt             time.Time `json:"updated_at"`
}

func aclPrincipalView(principal cluster.ACLPrincipal) clusterACLPrincipalView {
	return clusterACLPrincipalView{
		Name: principal.Name, CertificateIdentities: principal.CertificateIdentities,
		Permissions: principal.Permissions, Tenants: principal.Tenants,
		RequestsPerSecond: principal.RequestsPerSecond, Burst: principal.Burst,
		MaxConcurrent: principal.MaxConcurrent, ExpiresAt: principal.ExpiresAt,
		Revoked: principal.Revoked, UpdatedAt: principal.UpdatedAt,
	}
}

func (a AdminServer) clusterMetadataACLsList(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata ACLs")
	if !ok {
		return err
	}
	principals, err := cq.ListACLPrincipalsLinearizable(c.Context())
	if err != nil {
		return a.writeControllerError(c, err)
	}
	views := make([]clusterACLPrincipalView, 0, len(principals))
	for _, principal := range principals {
		views = append(views, aclPrincipalView(principal))
	}
	return writeBrokerJSON(c, views)
}

func (a AdminServer) clusterMetadataACLsPut(c fh.Ctx) error {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata ACLs")
	if !ok {
		return err
	}
	var body struct {
		Name                  string    `json:"name"`
		Token                 string    `json:"token,omitempty"`
		CertificateIdentities []string  `json:"certificate_identities,omitempty"`
		Permissions           []string  `json:"permissions,omitempty"`
		Tenants               []string  `json:"tenants,omitempty"`
		RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
		Burst                 int       `json:"burst,omitempty"`
		MaxConcurrent         int       `json:"max_concurrent,omitempty"`
		ExpiresAt             time.Time `json:"expires_at,omitempty"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	principal := cluster.ACLPrincipal{
		Name: body.Name, CertificateIdentities: body.CertificateIdentities,
		Permissions: body.Permissions, Tenants: body.Tenants,
		RequestsPerSecond: body.RequestsPerSecond, Burst: body.Burst,
		MaxConcurrent: body.MaxConcurrent, ExpiresAt: body.ExpiresAt,
	}
	if err := cq.PutACLPrincipal(c.Context(), principal, body.Token); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "stored", "name": body.Name})
}

func (a AdminServer) aclTarget(c fh.Ctx) (*cluster.ControllerQuorum, string, bool, error) {
	cq, ok, err := a.controllerQuorum(c, "cluster metadata ACLs")
	if !ok {
		return nil, "", false, err
	}
	name, err := sanitizeIdentifier(pathParam(c, "name"))
	if err != nil {
		return nil, "", false, WriteBrokerError(c, ErrInvalidMessage)
	}
	return cq, name, true, nil
}

func (a AdminServer) clusterMetadataACL(c fh.Ctx) error {
	cq, name, ok, err := a.aclTarget(c)
	if !ok {
		return err
	}
	principal, err := cq.GetACLPrincipalLinearizable(c.Context(), name)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, aclPrincipalView(principal))
}

func (a AdminServer) clusterMetadataACLRevoke(c fh.Ctx) error {
	cq, name, ok, err := a.aclTarget(c)
	if !ok {
		return err
	}
	if err := cq.RevokeACLPrincipal(c.Context(), name); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "revoked", "name": name})
}

// clusterNodePeer resolves the :node Raft peer ID for membership routes.
func (a AdminServer) clusterNodePeer(c fh.Ctx) (uint64, bool, error) {
	if rejectScopedControlPlane(c, "cluster membership") {
		return 0, false, nil
	}
	if a.Distributed == nil {
		return 0, false, WriteBrokerError(c, ErrUnsupportedMode)
	}
	peerID, err := strconv.ParseUint(c.Params("node"), 10, 64)
	if err != nil || peerID == 0 {
		return 0, false, WriteBrokerError(c, ErrInvalidMessage)
	}
	return peerID, true, nil
}

func (a AdminServer) clusterNodeDrain(c fh.Ctx) error {
	peerID, ok, err := a.clusterNodePeer(c)
	if !ok {
		return err
	}
	node, err := a.Distributed.BeginRaftNodeDrain(c.Context(), peerID)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "draining", "node": node})
}

func (a AdminServer) clusterNodeDrainComplete(c fh.Ctx) error {
	peerID, ok, err := a.clusterNodePeer(c)
	if !ok {
		return err
	}
	node, err := a.Distributed.CompleteRaftNodeDrain(c.Context(), peerID)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "left", "node": node})
}

func (a AdminServer) clusterNodeRemovalPlan(c fh.Ctx) error {
	peerID, ok, err := a.clusterNodePeer(c)
	if !ok {
		return err
	}
	plan, err := a.Distributed.PlanRaftNodeRemoval(c.Context(), peerID)
	if err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, plan)
}

func (a AdminServer) clusterNodePromote(c fh.Ctx) error {
	peerID, ok, err := a.clusterNodePeer(c)
	if !ok {
		return err
	}
	if err := a.Distributed.PromoteRaftLearner(c.Context(), peerID); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "promoted", "peer_id": peerID, "role": "voter"})
}

func (a AdminServer) clusterNodeRemove(c fh.Ctx) error {
	peerID, ok, err := a.clusterNodePeer(c)
	if !ok {
		return err
	}
	if err := a.Distributed.RemoveRaftNode(c.Context(), peerID); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "ok", "peer_id": peerID})
}

func (a AdminServer) clusterLeadership(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "cluster leadership") {
		return nil
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	if err := decodeJSON(c, &body); err != nil {
		return WriteBrokerError(c, err)
	}
	if body.PeerID == 0 {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if err := a.Distributed.TransferRaftLeadership(c.Context(), body.PeerID); err != nil {
		return a.writeControllerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"status": "transferred", "leader_id": body.PeerID})
}
