package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/fh"
)

type PollRequest struct {
	Queue               string `json:"queue"`
	Partition           int    `json:"partition,omitempty"`
	Partitioned         bool   `json:"partitioned,omitempty"`
	ConsumerID          string `json:"consumer_id"`
	ConsumerGroup       string `json:"consumer_group"`
	TenantID            string `json:"tenant_id,omitempty"`
	Limit               int    `json:"limit"`
	WaitNS              int64  `json:"wait_ns,omitempty"`
	VisibilityTimeoutNS int64  `json:"visibility_timeout_ns,omitempty"`
}

// PublishHTTPPayload is the request body for /v1/publish, /v1/publish/batch and
// /v1/request. Keys are snake_case, matching PollRequest and the wire format
// documented in docs/CODEC_FORMAT.md.
//
// This struct previously carried no tags, so encoding/json only ever matched the
// Go field names. Because Go's fallback matching is case-insensitive it happened
// to accept "Topic", but the underscore in "tenant_id" broke it: documented
// clients silently lost tenant scoping, routing keys, correlation ids,
// idempotency keys and request timeouts. UnmarshalJSON still accepts the old
// spellings so existing clients keep working.
type PublishHTTPPayload struct {
	Topic          string            `json:"topic"`
	Queue          string            `json:"queue,omitempty"`
	Stream         string            `json:"stream,omitempty"`
	Type           string            `json:"type,omitempty"`
	Payload        json.RawMessage   `json:"payload"`
	Headers        map[string]string `json:"headers,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Key            string            `json:"key,omitempty"`
	RoutingKey     string            `json:"routing_key,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Partition      int               `json:"partition,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TimeoutNS      int64             `json:"timeout_ns,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`

	// The fields below had no wire representation at all, so they were settable
	// through the embedded Publisher and silently dropped by every HTTP publish
	// path. Durations use nanoseconds to match timeout_ns and
	// visibility_timeout_ns.
	MessageID             string `json:"message_id,omitempty"`
	ContentType           string `json:"content_type,omitempty"`
	Schema                string `json:"schema,omitempty"`
	SchemaVersion         string `json:"schema_version,omitempty"`
	TraceID               string `json:"trace_id,omitempty"`
	CausationID           string `json:"causation_id,omitempty"`
	DeduplicationID       string `json:"deduplication_id,omitempty"`
	DeduplicationWindowNS int64  `json:"deduplication_window_ns,omitempty"`
	TTLNS                 int64  `json:"ttl_ns,omitempty"`
	DelayNS               int64  `json:"delay_ns,omitempty"`
	MaxAttempts           int    `json:"max_attempts,omitempty"`
	ProducerID            string `json:"producer_id,omitempty"`
	Strategy              string `json:"strategy,omitempty"`

	// PayloadEncoding selects how Payload is interpreted. "json" is the default
	// and means the payload already is JSON. "base64" means Payload is a base64
	// JSON string holding bytes that are not valid JSON.
	//
	// Without this, a []byte payload published by pkg/client was marshalled by
	// encoding/json into a base64 JSON *string*, and the server then stored the
	// literal quote-and-base64 characters. Six arbitrary bytes were stored as
	// eight ASCII bytes. The embedded path stores []byte verbatim, so this is
	// also what makes the two agree on binary payloads.
	PayloadEncoding string `json:"payload_encoding,omitempty"`
}

// PayloadBytes returns the payload as the publisher should store it, honouring
// PayloadEncoding.
func (p PublishHTTPPayload) PayloadBytes() ([]byte, error) {
	if p.PayloadEncoding == "" || p.PayloadEncoding == "json" {
		return []byte(p.Payload), nil
	}
	if p.PayloadEncoding == "base64" {
		// Payload is a JSON string holding base64 text.
		var encoded string
		if err := json.Unmarshal(p.Payload, &encoded); err != nil {
			return nil, fmt.Errorf("payload_encoding base64: %w", err)
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("payload_encoding base64: %w", err)
		}
		return decoded, nil
	}
	return nil, errf(ErrInvalidMessage, "unknown payload_encoding %q", p.PayloadEncoding)
}

// PublishOptions converts a publish request body into publisher options.
//
// Every option-based HTTP publish path goes through this one function. The
// mapping used to be written out separately in each handler, so any field added
// to PublishOptions had to be threaded through all of them by hand. That is how
// content_type, schema, schema_version, trace_id, causation_id,
// deduplication_id, ttl, delay, max_attempts, producer_id, strategy and a
// producer-chosen message_id ended up settable embedded and dropped remotely.
func (p PublishHTTPPayload) PublishOptions() []PublishOption {
	opts := make([]PublishOption, 0, 20)
	add := func(o PublishOption) { opts = append(opts, o) }
	if len(p.Headers) > 0 {
		add(WithHeaders(p.Headers))
	}
	if len(p.Metadata) > 0 {
		add(WithMetadata(p.Metadata))
	}
	for _, o := range []PublishOption{
		WithType(p.Type),
		WithContentType(p.ContentType),
		WithKey(p.Key),
		WithRoutingKey(p.RoutingKey),
		WithPriority(p.Priority),
		WithPartition(p.Partition),
		WithTenantID(p.TenantID),
		WithCorrelationID(p.CorrelationID),
		WithCausationID(p.CausationID),
		WithTraceID(p.TraceID),
		WithIdempotencyKey(p.IdempotencyKey),
		WithMessageID(p.MessageID),
		WithProducerID(p.ProducerID),
	} {
		add(o)
	}
	if p.Schema != "" || p.SchemaVersion != "" {
		add(WithSchema(p.Schema, p.SchemaVersion))
	}
	if p.DeduplicationID != "" {
		add(WithDeduplication(p.DeduplicationID, time.Duration(p.DeduplicationWindowNS)))
	}
	if p.TTLNS > 0 {
		add(WithTTL(time.Duration(p.TTLNS)))
	}
	if p.DelayNS > 0 {
		add(WithDelay(time.Duration(p.DelayNS)))
	}
	if p.MaxAttempts > 0 {
		add(WithMaxAttempts(p.MaxAttempts))
	}
	if p.Strategy != "" {
		add(WithStrategy(StrategyType(p.Strategy)))
	}
	if p.Queue != "" {
		add(WithQueue(p.Queue))
	}
	if p.Stream != "" {
		add(WithStream(p.Stream))
	}
	return opts
}

// legacyPublishHTTPPayload holds only the keys whose Go field names do not
// match their snake_case tags case-insensitively. Single-word keys (topic,
// queue, stream, type, payload, headers, metadata, key, priority) already match
// without help.
type legacyPublishHTTPPayload struct {
	RoutingKey     string
	TenantID       string
	CorrelationID  string
	IdempotencyKey string
	TimeoutNS      int64
}

// legacyPublishKeys are the exact byte sequences that trigger the compatibility
// pass. Checking for them keeps the documented wire format on a single decode.
var legacyPublishKeys = [][]byte{
	[]byte(`"RoutingKey"`),
	[]byte(`"TenantID"`),
	[]byte(`"CorrelationID"`),
	[]byte(`"IdempotencyKey"`),
	[]byte(`"TimeoutNS"`),
}

func (p *PublishHTTPPayload) UnmarshalJSON(data []byte) error {
	type payload PublishHTTPPayload
	var decoded payload
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*p = PublishHTTPPayload(decoded)
	legacy := false
	for _, key := range legacyPublishKeys {
		if bytes.Contains(data, key) {
			legacy = true
			break
		}
	}
	if !legacy {
		return nil
	}
	var old legacyPublishHTTPPayload
	if err := json.Unmarshal(data, &old); err != nil {
		return err
	}
	if p.RoutingKey == "" {
		p.RoutingKey = old.RoutingKey
	}
	if p.TenantID == "" {
		p.TenantID = old.TenantID
	}
	if p.CorrelationID == "" {
		p.CorrelationID = old.CorrelationID
	}
	if p.IdempotencyKey == "" {
		p.IdempotencyKey = old.IdempotencyKey
	}
	if p.TimeoutNS == 0 {
		p.TimeoutNS = old.TimeoutNS
	}
	return nil
}

// decodePublishHTTPPayload reads a publish request body. It is the same as
// json.Decoder.Decode but needs the raw bytes so PublishHTTPPayload can apply
// its key compatibility rules.
func decodePublishHTTPPayload(c fh.Ctx, dst *PublishHTTPPayload) error {
	return json.Unmarshal(c.Body(), dst)
}

// registerDataPlaneRoutes mounts the /v1 consumer and producer API.
//
// The permissions mirror the data plane's established authorization model.
// Note that /v1/broker lifecycle changes and /v1/dlq operations have always
// required only broker:read; the operator-facing /dlq routes use dlq:* scopes.
func (a AdminServer) registerDataPlaneRoutes(v1 adminRouter) {
	v1.post("/publish", PermissionPublish, a.v1Publish)
	v1.post("/publish/batch", PermissionPublish, a.v1PublishBatch)
	v1.post("/publish/frame", PermissionPublish, a.v1PublishFrame)
	v1.post("/publish/frame/batch", PermissionPublish, a.v1PublishFrameBatch)
	v1.post("/request", PermissionPublish, a.v1Request)

	v1.get("/broker", PermissionBrokerRead, a.v1BrokerState)
	v1.post("/broker", PermissionBrokerRead, a.v1BrokerLifecycle)
	v1.put("/broker", PermissionBrokerRead, a.v1BrokerLifecycle)

	v1.get("/topics/:topic", PermissionBrokerRead, a.v1Topic)
	v1.get("/topics/:topic/partitions/:partition/messages", PermissionBrokerRead, a.v1TopicPartition)

	v1.get("/consumers", PermissionConsumerRead, a.v1ConsumerList)
	v1.post("/consumers/register", PermissionConsumerRegister, a.v1ConsumerRegister)
	// Without this, GET would fall through to /consumers/:id with id "register".
	v1.get("/consumers/register", PermissionConsumerRegister, allowOnly(fh.MethodPOST))
	consumer := v1.group("/consumers/:id")
	consumer.get("", PermissionConsumerRead, a.v1ConsumerGet)
	consumer.get("/events", PermissionConsumerRead, a.v1ConsumerEvents)
	consumer.post("/heartbeat", PermissionConsumerControl, a.v1ConsumerHeartbeat)
	consumer.post("/subscribe", PermissionConsumerControl, a.v1ConsumerSubscribe)
	consumer.post("/pause", PermissionConsumerControl, a.v1ConsumerPause)
	consumer.post("/resume", PermissionConsumerControl, a.v1ConsumerResume)
	consumer.post("/drain", PermissionConsumerControl, a.v1ConsumerDrain)
	consumer.post("/ban", PermissionConsumerBan, a.v1ConsumerBan)
	consumer.post("/unban", PermissionConsumerUnban, a.v1ConsumerUnban)
	consumer.post("/unregister", PermissionConsumerControl, a.v1ConsumerUnregister)
	consumer.delete("/unregister", PermissionConsumerControl, a.v1ConsumerUnregister)

	for _, path := range []string{"/poll", "/poll/batch"} {
		v1.get(path, PermissionConsumerPoll, a.v1Poll)
		v1.post(path, PermissionConsumerPoll, a.v1Poll)
	}
	v1.get("/poll/frame", PermissionConsumerPoll, a.v1PollFrame)
	v1.post("/poll/frame", PermissionConsumerPoll, a.v1PollFrame)
	v1.post("/ack/frame", PermissionMessageAck, a.v1AckFrameBatch)

	msg := v1.group("/messages/:id")
	msg.post("/ack", PermissionMessageAck, a.v1MessageAck)
	msg.post("/nack", PermissionMessageNack, a.v1MessageNack("nack"))
	msg.post("/reject", PermissionMessageNack, a.v1MessageNack("reject"))
	msg.post("/extend", PermissionMessageRead, a.v1MessageExtend)
	msg.post("/progress", PermissionMessageRead, a.v1MessageProgress)
	msg.post("/respond", PermissionMessageRead, a.v1MessageRespond("respond"))
	msg.post("/complete", PermissionMessageRead, a.v1MessageRespond("complete"))
	msg.post("/fail", PermissionMessageRead, a.v1MessageRespond("fail"))

	v1.get("/tasks", PermissionTaskRead, a.v1TaskList)
	v1.post("/tasks/submit", PermissionTaskWrite, a.v1TaskSubmit)
	// Without this, GET would fall through to /tasks/:id with id "submit".
	v1.get("/tasks/submit", PermissionTaskWrite, allowOnly(fh.MethodPOST))
	task := v1.group("/tasks/:id")
	task.get("", PermissionTaskRead, a.v1TaskGet)
	task.get("/result", PermissionTaskRead, a.v1TaskResult)
	task.get("/logs", PermissionTaskRead, a.v1TaskLogs)
	task.post("/cancel", PermissionTaskWrite, a.v1TaskCancel)
	task.post("/retry", PermissionTaskWrite, a.v1TaskRetry)

	v1.get("/streams", PermissionStreamRead, a.streams)
	v1.post("/streams", PermissionStreamWrite, a.streams)
	a.registerStreamRoutes(v1.group("/streams/:stream"), PermissionStreamCommit)

	v1.get("/dlq", PermissionBrokerRead, a.dlq)
	v1.get("/dlq/dashboard", PermissionBrokerRead, a.dlqDashboard)
	a.registerDLQRoutes(v1.group("/dlq/:queue"), PermissionBrokerRead, PermissionBrokerRead)

	v1.get("/events", PermissionNotification, a.events)

	v1.get("/sagas", PermissionTaskRead, a.v1SagaList)
	v1.post("/sagas", PermissionTaskWrite, a.v1SagaCreate)
	v1.get("/sagas/:id", PermissionTaskRead, a.v1Saga)

	v1.get("/groups", PermissionBrokerRead, a.v1Groups)
	v1.get("/groups/:group", PermissionBrokerRead, a.v1Group)
}

func (a AdminServer) v1ConsumerList(c fh.Ctx) error {
	return writeBrokerJSON(c, a.Broker.Consumers().ListContext(c.Context()))
}

func (a AdminServer) v1TaskList(c fh.Ctx) error {
	return writeBrokerJSON(c, a.Broker.ListTasks(c.Context()))
}

func (a AdminServer) v1SagaList(c fh.Ctx) error {
	return writeBrokerJSON(c, a.Broker.SagaEngine().ListSagasContext(c.Context()))
}

func (a AdminServer) v1Saga(c fh.Ctx) error {
	s, ok := a.Broker.SagaEngine().GetSagaContext(c.Context(), pathParam(c, "id"))
	if !ok {
		return notFound(c)
	}
	return writeBrokerJSON(c, &s)
}

func (a AdminServer) v1Groups(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global consumer groups") {
		return nil
	}
	return writeBrokerJSON(c, a.Broker.GroupRebalancer().ListGroups())
}

func (a AdminServer) v1Group(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "global consumer groups") {
		return nil
	}
	g := a.Broker.GroupRebalancer().GetGroup(pathParam(c, "group"))
	if g == nil {
		return notFound(c)
	}
	return writeBrokerJSON(c, g)
}

func (a AdminServer) v1BrokerState(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "broker lifecycle") {
		return nil
	}
	return writeBrokerJSON(c, map[string]any{"state": a.Broker.State()})
}

// brokerLifecycleActions maps a lifecycle action name to the broker operation.
var brokerLifecycleActions = map[string]func(*Broker, context.Context) error{
	"pause":  (*Broker).Pause,
	"resume": (*Broker).Resume,
}

func (a AdminServer) v1BrokerLifecycle(c fh.Ctx) error {
	if rejectScopedControlPlane(c, "broker lifecycle") {
		return nil
	}
	var req struct {
		Action string `json:"action"`
		Reason string `json:"reason,omitempty"`
	}
	if err := decodeJSON(c, &req); err != nil {
		return WriteBrokerError(c, errf(ErrInvalidMessage, "invalid body: %v", err))
	}
	action, ok := brokerLifecycleActions[req.Action]
	if !ok {
		return WriteBrokerError(c, errf(ErrInvalidMessage, "action must be pause or resume, got %q", req.Action))
	}
	if err := action(a.Broker, c.Context()); err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]any{"state": a.Broker.State(), "reason": req.Reason})
}

func (a AdminServer) v1TopicPartition(c fh.Ctx) error {
	name, err := sanitizeIdentifier(pathParam(c, "topic"))
	partition, ok := pathPartition(c, "partition")
	if err != nil || !ok {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	if a.Distributed == nil {
		return WriteBrokerError(c, ErrUnsupportedMode)
	}
	msgs, err := a.Distributed.ReadPartitionTopic(c.Context(), name, partition, queryString(c, "after"), queryLimit(c, 100))
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, msgs)
}

func (a AdminServer) v1Topic(c fh.Ctx) error {
	name, err := sanitizeIdentifier(pathParam(c, "topic"))
	if err != nil {
		return WriteBrokerError(c, ErrInvalidMessage)
	}
	msgs, err := a.Broker.ReadTopic(c.Context(), name, queryString(c, "after"), queryLimit(c, 100))
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, msgs)
}

func (a AdminServer) v1PublishFrame(c fh.Ctx) error {
	// Copy the body: decoded frames may alias it, and fh recycles request
	// buffers once the handler returns.
	data := c.BodyCopy()
	var msg Message
	if err := a.Broker.FrameCodec().DecodeMessage(data, &msg); err != nil {
		return httpError(c, err.Error(), 400)
	}
	topic := msg.Topic
	opts := []PublishOption{WithMessageID(msg.ID), WithHeaders(msg.Headers), WithMetadata(msg.Metadata), WithType(msg.Type), WithContentType(msg.ContentType), WithKey(msg.Key), WithRoutingKey(msg.RoutingKey), WithPriority(msg.Priority), WithTenantID(msg.TenantID), WithProducerID(msg.ProducerID), WithCorrelationID(msg.CorrelationID), WithCausationID(msg.CausationID), WithIdempotencyKey(msg.IdempotencyKey)}
	if msg.Queue != "" {
		opts = append(opts, WithQueue(msg.Queue))
	}
	if msg.Stream != "" {
		opts = append(opts, WithStream(msg.Stream))
	}
	if !msg.AvailableAt.IsZero() && msg.AvailableAt.After(time.Now()) {
		opts = append(opts, WithRunAt(msg.AvailableAt))
	}
	if !msg.ExpiresAt.IsZero() && msg.ExpiresAt.After(time.Now()) {
		opts = append(opts, WithTTL(time.Until(msg.ExpiresAt)))
	}
	res, err := a.Broker.Publisher().Publish(c.Context(), topic, msg.Payload, opts...)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, res)
}

func publishOptionsFromFrame(msg Message) []PublishOption {
	opts := []PublishOption{WithMessageID(msg.ID), WithHeaders(msg.Headers), WithMetadata(msg.Metadata), WithType(msg.Type), WithContentType(msg.ContentType), WithKey(msg.Key), WithRoutingKey(msg.RoutingKey), WithPriority(msg.Priority), WithTenantID(msg.TenantID), WithProducerID(msg.ProducerID), WithCorrelationID(msg.CorrelationID), WithCausationID(msg.CausationID), WithIdempotencyKey(msg.IdempotencyKey)}
	if msg.Queue != "" {
		opts = append(opts, WithQueue(msg.Queue))
	}
	if msg.Stream != "" {
		opts = append(opts, WithStream(msg.Stream))
	}
	if !msg.AvailableAt.IsZero() && msg.AvailableAt.After(time.Now()) {
		opts = append(opts, WithRunAt(msg.AvailableAt))
	}
	if !msg.ExpiresAt.IsZero() && msg.ExpiresAt.After(time.Now()) {
		opts = append(opts, WithTTL(time.Until(msg.ExpiresAt)))
	}
	return opts
}

func (a AdminServer) v1PublishFrameBatch(c fh.Ctx) error {
	messages, err := readFrameBatch(bytes.NewReader(c.BodyCopy()), a.Broker.FrameCodec(), 1<<20, int64(len(c.Body())))
	if err != nil {
		return httpError(c, err.Error(), fh.StatusBadRequest)
	}
	if len(messages) == 0 {
		return httpError(c, "empty frame batch", fh.StatusBadRequest)
	}
	if preferMinimal(c) {
		errs, first := a.Broker.Publisher().publishMessagesMinimal(c.Context(), messages)
		return writeMinimalPublishResponse(c, errs, first)
	}
	items := make([]PublishBatchItem, len(messages))
	for i := range messages {
		items[i] = PublishBatchItem{Topic: messages[i].Topic, Payload: messages[i].Payload, Options: publishOptionsFromFrame(messages[i])}
	}
	results, errs, first := a.Broker.Publisher().publishItemsAligned(c.Context(), items)
	errorsOut := make([]string, len(errs))
	failed := 0
	for i, err := range errs {
		if err != nil {
			errorsOut[i] = err.Error()
			failed++
		}
	}
	if failed > 0 {
		c.Set("Content-Type", "application/json")
		c.Status(fh.StatusMultiStatus)
	}
	return writeBrokerJSON(c, map[string]any{"results": results, "errors": errorsOut, "failed": failed, "error": errorString(first)})
}

// readFrameBatch decodes length-prefixed frames. The request body is buffered
// once (sizeHint is its Content-Length, 0 if unknown; the HTTP layer already
// bounds it), frames are counted with a length-only pass, and the result slice
// is allocated exactly. DecodeMessage copies everything it keeps out of the
// frame, so decoding straight from the buffer is safe. Sizing the slice by
// guesswork zeroed far more Message memory than the batch needed.
func readFrameBatch(reader io.Reader, codec FrameCodec, maxMessages int, sizeHint int64) ([]Message, error) {
	if maxMessages <= 0 {
		maxMessages = 1 << 20
	}
	var buf bytes.Buffer
	if sizeHint > 0 {
		// Never trust the header for allocation size beyond a modest cap.
		buf.Grow(int(min(sizeHint, 8<<20)) + bytes.MinRead)
	}
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}
	body := buf.Bytes()

	count := 0
	for off := 0; off < len(body); {
		length, n := binary.Uvarint(body[off:])
		if n <= 0 {
			return nil, fmt.Errorf("read frame length: %w", io.ErrUnexpectedEOF)
		}
		if length == 0 || length > 128<<20 {
			return nil, errf(ErrInvalidMessage, "invalid frame length %d", length)
		}
		if count >= maxMessages {
			return nil, errf(ErrBackpressure, "frame batch exceeds %d messages", maxMessages)
		}
		off += n
		if uint64(len(body)-off) < length {
			return nil, fmt.Errorf("read frame payload: %w", io.ErrUnexpectedEOF)
		}
		off += int(length)
		count++
	}

	messages := make([]Message, 0, count)
	for off := 0; off < len(body); {
		length, n := binary.Uvarint(body[off:])
		off += n
		frame := body[off : off+int(length)]
		off += int(length)
		var message Message
		if err := codec.DecodeMessage(frame, &message); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func EncodeMessageFrameBatch(dst []byte, codec FrameCodec, messages []Message) ([]byte, error) {
	for i := range messages {
		frame, err := codec.EncodeMessage(nil, messages[i])
		if err != nil {
			return nil, err
		}
		dst = binary.AppendUvarint(dst, uint64(len(frame)))
		dst = append(dst, frame...)
	}
	return dst, nil
}

func DecodeMessageFrameBatch(data []byte, codec FrameCodec, maxMessages int) ([]Message, error) {
	return readFrameBatch(bytes.NewReader(data), codec, maxMessages, int64(len(data)))
}

func EncodeAckFrameBatch(dst []byte, requests []AckRequest) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(requests)))
	for _, request := range requests {
		dst = appendFrameString(dst, request.MessageID)
		dst = appendFrameString(dst, request.Queue)
		dst = appendFrameString(dst, request.LeaseToken)
		dst = appendFrameString(dst, request.ConsumerID)
	}
	return dst
}

func appendFrameString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func decodeAckFrameBatch(reader io.Reader, maxBytes int64) ([]AckRequest, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrMessageTooLarge
	}
	count, n := binary.Uvarint(data)
	if n <= 0 || count > 1<<20 {
		return nil, errf(ErrInvalidMessage, "invalid ack batch")
	}
	data = data[n:]
	requests := make([]AckRequest, int(count))
	for i := range requests {
		values := []*string{&requests[i].MessageID, &requests[i].Queue, &requests[i].LeaseToken, &requests[i].ConsumerID}
		for _, target := range values {
			length, read := binary.Uvarint(data)
			if read <= 0 {
				return nil, errf(ErrInvalidMessage, "invalid ack string length")
			}
			data = data[read:]
			if length > uint64(len(data)) {
				return nil, io.ErrUnexpectedEOF
			}
			*target = string(data[:length])
			data = data[length:]
		}
	}
	if len(data) != 0 {
		return nil, errf(ErrInvalidMessage, "trailing ack data")
	}
	return requests, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeMinimalPublishResponse(c fh.Ctx, errs []error, first error) error {
	failed := 0
	errorsOut := make([]string, len(errs))
	for i, err := range errs {
		if err != nil {
			failed++
			errorsOut[i] = err.Error()
		}
	}
	if failed == 0 {
		return c.SendStatus(fh.StatusNoContent)
	}
	return writeBrokerJSONStatus(c, fh.StatusMultiStatus, map[string]any{"errors": errorsOut, "failed": failed, "error": errorString(first)})
}

func (a AdminServer) v1AckFrameBatch(c fh.Ctx) error {
	requests, err := decodeAckFrameBatch(bytes.NewReader(c.BodyCopy()), 1<<20)
	if err != nil {
		return httpError(c, err.Error(), fh.StatusBadRequest)
	}
	if len(requests) == 0 {
		return httpError(c, "empty ack batch", fh.StatusBadRequest)
	}
	errs := make([]error, len(requests))
	if batchStore, ok := a.Broker.store.(BatchAckStore); ok {
		errs = batchStore.AckBatch(c.Context(), requests)
	} else {
		for i := range requests {
			errs[i] = a.Broker.store.Ack(c.Context(), requests[i])
		}
	}
	acked := 0
	errorsOut := make([]string, len(errs))
	for i, err := range errs {
		if err == nil {
			acked++
			a.Broker.Consumers().AddStats(requests[i].ConsumerID, 0, 1, 0, 0, "")
		} else {
			errorsOut[i] = err.Error()
		}
	}
	if acked == len(requests) && preferMinimal(c) {
		return c.SendStatus(fh.StatusNoContent)
	}
	if acked != len(requests) {
		c.Set("Content-Type", "application/json")
		c.Status(fh.StatusMultiStatus)
	}
	return writeBrokerJSON(c, map[string]any{"acked": acked, "errors": errorsOut})
}

// messageFromHTTPPayload builds a Message for the "Prefer: return=minimal"
// publish path, which bypasses PublishOptions and hands the store an owned
// Message directly.
//
// It folds PublishHTTPPayload.PublishOptions rather than listing fields again,
// so the two remote publish paths cannot drift apart. The transformations here
// mirror preparePublish: delay becomes AvailableAt, TTL becomes ExpiresAt, a
// deduplication window is recorded in metadata, and a strategy becomes the
// x-routing-strategy header.
func messageFromHTTPPayload(p PublishHTTPPayload) Message {
	var o PublishOptions
	for _, opt := range p.PublishOptions() {
		opt(&o)
	}
	payload, _ := p.PayloadBytes()
	now := time.Now()
	msg := Message{
		Topic:           p.Topic,
		Queue:           o.Queue,
		Stream:          o.Stream,
		Type:            o.Type,
		ContentType:     o.ContentType,
		Schema:          o.Schema,
		SchemaVersion:   o.SchemaVersion,
		Payload:         payload,
		Headers:         o.Headers,
		Metadata:        o.Metadata,
		Key:             o.Key,
		RoutingKey:      o.RoutingKey,
		Priority:        o.Priority,
		TenantID:        o.TenantID,
		TraceID:         o.TraceID,
		CorrelationID:   o.CorrelationID,
		CausationID:     o.CausationID,
		IdempotencyKey:  o.IdempotencyKey,
		DeduplicationID: o.DeduplicationID,
		ProducerID:      o.ProducerID,
		ProducerEpoch:   o.ProducerEpoch,
		CreatedAt:       now,
		AvailableAt:     now,
	}
	if o.MessageID != "" {
		msg.ID = o.MessageID
	}
	if o.Delay > 0 {
		msg.AvailableAt = now.Add(o.Delay)
	}
	if o.TTL > 0 {
		msg.ExpiresAt = now.Add(o.TTL)
	}
	if o.MaxAttempts > 0 {
		msg.MaxAttempts = o.MaxAttempts
	}
	if o.DeduplicationWindow > 0 {
		if msg.Metadata == nil {
			msg.Metadata = map[string]string{}
		}
		msg.Metadata["_dedup_window_ns"] = fmt.Sprintf("%d", o.DeduplicationWindow.Nanoseconds())
	}
	if o.Strategy != "" {
		if msg.Headers == nil {
			msg.Headers = map[string]string{}
		}
		msg.Headers["x-routing-strategy"] = string(o.Strategy)
	}
	return msg
}

func (a AdminServer) v1Publish(c fh.Ctx) error {
	var req PublishHTTPPayload
	if decodePublishHTTPPayload(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	payload, err := req.PayloadBytes()
	if err != nil {
		return httpError(c, err.Error(), 400)
	}
	opts := req.PublishOptions()
	res, err := a.Broker.Publisher().Publish(c.Context(), req.Topic, payload, opts...)
	if err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, res)
}
func (a AdminServer) v1PublishBatch(c fh.Ctx) error {
	var req struct {
		Messages []PublishHTTPPayload `json:"messages"`
	}
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	if preferMinimal(c) {
		messages := make([]Message, len(req.Messages))
		for i := range req.Messages {
			messages[i] = messageFromHTTPPayload(req.Messages[i])
		}
		errs, first := a.Broker.Publisher().publishMessagesMinimal(c.Context(), messages)
		return writeMinimalPublishResponse(c, errs, first)
	}
	items := make([]PublishBatchItem, len(req.Messages))
	for i, m := range req.Messages {
		payload, perr := m.PayloadBytes()
		if perr != nil {
			return httpError(c, perr.Error(), 400)
		}
		items[i] = PublishBatchItem{Topic: m.Topic, Payload: payload, Options: m.PublishOptions()}
	}
	res, err := a.Broker.Publisher().PublishItems(c.Context(), items)
	if err != nil && len(res.Errors) == 0 {
		return WriteBrokerError(c, err)
	}
	if len(res.Errors) > 0 {
		c.Set("Content-Type", "application/json")
		c.Status(fh.StatusMultiStatus)
	}
	return writeBrokerJSON(c, res)
}
func (a AdminServer) v1Request(c fh.Ctx) error {
	var req PublishHTTPPayload
	if decodePublishHTTPPayload(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	if req.Topic == "" {
		return httpError(c, "topic/subject required", 400)
	}
	timeout := time.Duration(req.TimeoutNS)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	resp, err := a.Broker.Publisher().requestWithOptions(c.Context(), req.Topic, []byte(req.Payload), RequestOptions{Timeout: timeout, CorrelationID: req.CorrelationID, TenantID: req.TenantID, IdempotencyKey: req.IdempotencyKey})
	if err != nil {
		return httpError(c, err.Error(), 504)
	}
	return writeBrokerJSON(c, resp)
}
func (a AdminServer) v1ConsumerRegister(c fh.Ctx) error {
	var req ConsumerRegistration
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	info, err := a.Broker.RegisterRemoteConsumer(c.Context(), req)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return httpError(c, err.Error(), fh.StatusForbidden)
		}
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, info)
}

// parsePollRequest reads a poll request from either the JSON body or the query
// string, so both poll endpoints normalise identically.
func parsePollRequest(c fh.Ctx) (PollRequest, bool) {
	var req PollRequest
	if c.Method() == fh.MethodPOST {
		_ = decodeJSON(c, &req)
	}
	if req.Queue == "" {
		req.Queue = queryString(c, "queue")
	}
	if req.ConsumerID == "" {
		req.ConsumerID = firstNonEmpty(queryString(c, "consumer_id"), queryString(c, "consumer"))
	}
	if req.ConsumerGroup == "" {
		req.ConsumerGroup = firstNonEmpty(queryString(c, "consumer_group"), queryString(c, "group"))
	}
	if req.Limit <= 0 {
		req.Limit = queryLimit(c, 1)
	}
	if req.ConsumerID == "" || req.Queue == "" {
		_ = httpError(c, "consumer_id and queue required", 400)
		return req, false
	}
	return req, true
}

// consumerBlocked reports whether a registered consumer is in a state that must
// not receive messages. Unregistered consumers are not blocked, because the
// stateless poll endpoints do not require prior registration.
func (a AdminServer) consumerBlocked(consumerID string) bool {
	info, ok := a.Broker.Consumers().Get(consumerID)
	if !ok {
		return false
	}
	return info.Paused || info.Draining || info.Banned ||
		info.State == ConsumerPaused || info.State == ConsumerDraining ||
		info.State == ConsumerStopped || info.State == ConsumerBanned
}

// leaseForConsumer runs the guarded lease loop behind both poll endpoints.
//
// The JSON and frame endpoints used to carry separate copies of this logic, and
// the frame copy was missing the banned, paused and draining guard along with the
// task start and the message.leased notification. A banned consumer polling
// through /v1/poll/frame therefore kept receiving messages. Sharing one
// implementation is the fix; the endpoints now differ only in how they encode
// the result.
//
// It returns the leased messages, or ok=false when it has already written the
// response.
func (a AdminServer) leaseForConsumer(c fh.Ctx, req PollRequest) ([]Message, bool) {
	if a.Distributed != nil {
		assigned, err := a.Distributed.hasResourcePartitions(c.Context(), cluster.PartitionResourceQueue, req.Queue)
		if err != nil {
			WriteBrokerError(c, err)
			return nil, false
		}
		if assigned && !req.Partitioned {
			WriteBrokerError(c, fmt.Errorf("%w: queue %q requires a partition-specific poll", ErrUnsupportedMode, req.Queue))
			return nil, false
		}
	} else if req.Partitioned {
		WriteBrokerError(c, ErrUnsupportedMode)
		return nil, false
	}
	tenant, err := resolveTenant(c.Context(), req.TenantID)
	if err != nil {
		httpError(c, err.Error(), fh.StatusForbidden)
		return nil, false
	}
	if a.consumerBlocked(req.ConsumerID) {
		return []Message{}, true
	}
	wait := time.Duration(req.WaitNS)
	if wait <= 0 {
		wait = queryDuration(c, "wait", 0)
	}
	deadline := time.Now().Add(wait)
	vis := time.Duration(req.VisibilityTimeoutNS)
	if vis <= 0 {
		vis = queryDuration(c, "visibility_timeout", 0)
	}
	leaseReq := LeaseRequest{
		Queue:             req.Queue,
		Partition:         req.Partition,
		Partitioned:       req.Partitioned,
		ConsumerID:        req.ConsumerID,
		ConsumerGroup:     firstNonEmpty(req.ConsumerGroup, req.ConsumerID),
		Limit:             req.Limit,
		VisibilityTimeout: vis,
		TenantID:          tenant,
	}
	// Wait for work on three things: a store signal that a message became
	// available, a short fallback tick for state this signal cannot cover (lease
	// expiry sweeping, queue resume, messages promoted out of cold storage), and
	// request cancellation.
	//
	// Previously the loop slept a fixed 25ms between attempts, so a message
	// published while a long poll was already waiting sat unclaimed for up to 25ms.
	// That is a hard floor on pickup latency for every remote consumer, and it is
	// paid on the common path where the message is available immediately.
	var idle *time.Timer
	defer func() {
		if idle != nil {
			idle.Stop()
		}
	}()
	var ready <-chan struct{}
	if signaler, ok := a.Broker.store.(QueueSignaler); ok {
		ready = signaler.QueueReady(req.Queue)
	}
	for {
		msgs, err := a.Broker.store.Lease(c.Context(), leaseReq)
		if err != nil {
			WriteBrokerError(c, err)
			return nil, false
		}
		if len(msgs) > 0 || wait <= 0 || time.Now().After(deadline) {
			if len(msgs) > 0 {
				group := firstNonEmpty(req.ConsumerGroup, req.ConsumerID)
				notify := a.Broker.cfg.EnableNotifications || a.Broker.cfg.EnableConsumerEventTracking
				leased := msgs
				active := make([]Message, 0, len(leased))
				for i := range leased {
					leased[i].ConsumerID = req.ConsumerID
					leased[i].ConsumerGroup = group
					leased[i].Status = MessageProcessing
					taskID := firstNonEmpty(leased[i].TaskID, leased[i].Headers["x-task-id"])
					if taskID != "" {
						run, err := a.Broker.startTask(c.Context(), taskID)
						if err != nil {
							for _, item := range leased {
								_ = a.Broker.store.Nack(c.Context(), NackRequest{MessageID: item.ID, Queue: req.Queue, Partition: req.Partition, Partitioned: req.Partitioned, ConsumerID: req.ConsumerID, LeaseToken: item.LeaseToken, Action: NackRequeue})
							}
							WriteBrokerError(c, err)
							return nil, false
						}
						if !run {
							if err := a.Broker.store.Ack(c.Context(), AckRequest{MessageID: leased[i].ID, Queue: req.Queue, Partition: req.Partition, Partitioned: req.Partitioned, LeaseToken: leased[i].LeaseToken, ConsumerID: req.ConsumerID}); err != nil {
								for _, item := range leased {
									_ = a.Broker.store.Nack(c.Context(), NackRequest{MessageID: item.ID, Queue: req.Queue, Partition: req.Partition, Partitioned: req.Partitioned, ConsumerID: req.ConsumerID, LeaseToken: item.LeaseToken, Action: NackRequeue})
								}
								WriteBrokerError(c, err)
								return nil, false
							}
							a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
							a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: leased[i].ID, TaskID: taskID, Queue: req.Queue, Consumer: req.ConsumerID, ConsumerGroup: group, TenantID: leased[i].TenantID, Timestamp: time.Now()})
							continue
						}
					}
					active = append(active, leased[i])
				}
				msgs = active
				if notify {
					for i := range msgs {
						taskID := firstNonEmpty(msgs[i].TaskID, msgs[i].Headers["x-task-id"])
						a.Broker.emitConsumerNotification(req.ConsumerID, Notification{
							Type: "message.leased", Source: a.Broker.cfg.Name, Severity: SeverityInfo,
							MessageID: msgs[i].ID, TaskID: taskID, Topic: msgs[i].Topic, Queue: req.Queue,
							Consumer: req.ConsumerID, ConsumerGroup: group, TenantID: msgs[i].TenantID,
							Timestamp: time.Now(),
							Data:      map[string]any{"lease_until": msgs[i].LeaseUntil, "attempts": msgs[i].Attempts},
						})
					}
				}
				a.Broker.Consumers().AddStats(req.ConsumerID, int64(len(msgs)), 0, 0, 0, "")
			}
			return msgs, true
		}
		if idle == nil {
			idle = time.NewTimer(pollRetryInterval)
		} else {
			idle.Reset(pollRetryInterval)
		}
		if ready == nil {
			// The queue may not have existed when the poll started. Retry the
			// lookup so a later append is not missed; until then the fallback tick
			// still bounds the wait.
			if signaler, ok := a.Broker.store.(QueueSignaler); ok {
				ready = signaler.QueueReady(req.Queue)
			}
		}
		select {
		case <-ready:
		case <-idle.C:
		case <-c.Context().Done():
			return nil, false
		}
	}
}

// pollRetryInterval is how long a long poll waits before re-checking the queue.
const pollRetryInterval = 25 * time.Millisecond

func (a AdminServer) v1PollFrame(c fh.Ctx) error {
	req, ok := parsePollRequest(c)
	if !ok {
		return nil
	}
	msgs, ok := a.leaseForConsumer(c, req)
	if !ok {
		return nil
	}
	writeFrameBatch(c, a.Broker.FrameCodec(), msgs)
	return nil
}

func writeFrameBatch(c fh.Ctx, codec FrameCodec, msgs []Message) error {
	body, err := EncodeMessageFrameBatch(nil, codec, msgs)
	if err != nil {
		return httpError(c, err.Error(), 500)
	}
	c.Set("Content-Type", "application/vnd.pool.broker.frames")
	return c.Send(body)
}

func (a AdminServer) v1Poll(c fh.Ctx) error {
	req, ok := parsePollRequest(c)
	if !ok {
		return nil
	}
	msgs, ok := a.leaseForConsumer(c, req)
	if !ok {
		return nil
	}
	return writeBrokerJSON(c, msgs)
}

// ---- /v1/messages/:id ----

func (a AdminServer) v1MessageAck(c fh.Ctx) error {
	id := pathParam(c, "id")
	var req AckRequest
	_ = decodeJSON(c, &req)
	req.MessageID = id
	// Validate the lease token before the store does, so the failure is
	// reported as ErrLeaseLost with the same code and status the frame ack
	// endpoint and every in-process caller see. This path used to answer 403
	// with bespoke text while /v1/ack/frame answered 409 with ErrLeaseLost, so
	// the same logical failure was indistinguishable to a client.
	m, hadMessage, lookupErr := a.Broker.store.Get(c.Context(), id)
	if req.Partitioned && a.Distributed != nil {
		m, hadMessage, lookupErr = a.Distributed.GetPartitionQueueMessage(c.Context(), req.Queue, req.Partition, id)
	}
	if lookupErr != nil {
		return WriteBrokerError(c, lookupErr)
	}
	if hadMessage && m.LeaseToken != "" {
		if req.LeaseToken == "" {
			return WriteBrokerError(c, errf(ErrLeaseLost, "lease token required for leased message %s", id))
		}
		if req.LeaseToken != m.LeaseToken {
			return WriteBrokerError(c, errf(ErrLeaseLost, "invalid lease token for message %s", id))
		}
	}
	if err := a.Broker.store.Ack(c.Context(), req); err != nil {
		return WriteBrokerError(c, err)
	}
	a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
	if hadMessage {
		if err := a.Broker.completeTask(c.Context(), m, nil, nil); err != nil {
			return WriteBrokerError(c, fmt.Errorf("message %s was acknowledged but task completion could not be persisted: %w", id, err))
		}
		a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now()})
	} else {
		a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, Queue: req.Queue, Consumer: req.ConsumerID, Timestamp: time.Now()})
	}
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

// v1MessageNack serves nack and reject; op is the route verb.
func (a AdminServer) v1MessageNack(op string) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		id := pathParam(c, "id")
		var req NackRequest
		_ = decodeJSON(c, &req)
		req.MessageID = id
		// Validate the lease token before the store does, so the failure is
		// reported as ErrLeaseLost with the same code and status the frame ack
		// endpoint and every in-process caller see. This path used to answer 403
		// with bespoke text while /v1/ack/frame answered 409 with ErrLeaseLost, so
		// the same logical failure was indistinguishable to a client.
		m, hadMessage, lookupErr := a.Broker.store.Get(c.Context(), id)
		if req.Partitioned && a.Distributed != nil {
			m, hadMessage, lookupErr = a.Distributed.GetPartitionQueueMessage(c.Context(), req.Queue, req.Partition, id)
		}
		if lookupErr != nil {
			return WriteBrokerError(c, lookupErr)
		}
		if hadMessage && m.LeaseToken != "" {
			if req.LeaseToken == "" {
				return WriteBrokerError(c, errf(ErrLeaseLost, "lease token required for leased message %s", id))
			}
			if req.LeaseToken != m.LeaseToken {
				return WriteBrokerError(c, errf(ErrLeaseLost, "invalid lease token for message %s", id))
			}
		}
		if req.Action == "" {
			req.Action = NackRequeue
		}
		if op == "reject" {
			req.Action = NackReject
		}
		req.IncrementAttempt = true
		if err := a.Broker.store.Nack(c.Context(), req); err != nil {
			return WriteBrokerError(c, err)
		}
		a.Broker.Consumers().AddStats(req.ConsumerID, 0, 0, 1, 0, req.Error)
		if req.Action == NackReject || req.Action == NackDLQ {
			if hadMessage {
				tid := firstNonEmpty(m.TaskID, m.Headers["x-task-id"])
				if tid != "" {
					if taskErr := a.Broker.failTask(c.Context(), tid, errf(ErrPermanent, "%s", firstNonEmpty(req.Error, req.Reason))); taskErr != nil {
						return WriteBrokerError(c, fmt.Errorf("message %s was rejected but task failure could not be persisted: %w", id, taskErr))
					}
				}
			}
		} else if hadMessage {
			tid := firstNonEmpty(m.TaskID, m.Headers["x-task-id"])
			if tid != "" {
				if taskErr := a.Broker.retryTask(c.Context(), tid, fmt.Errorf("%s", firstNonEmpty(req.Error, firstNonEmpty(req.Reason, "message requeued")))); taskErr != nil {
					return WriteBrokerError(c, fmt.Errorf("message %s was requeued but task retry state could not be persisted: %w", id, taskErr))
				}
			}
		}
		if hadMessage {
			a.Broker.notify.Publish(Notification{Type: "message." + op, Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityWarn, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"action": req.Action, "reason": firstNonEmpty(req.Reason, req.Error)}})
		}
		return writeBrokerJSON(c, map[string]bool{"ok": true})
	}
}

func (a AdminServer) v1MessageExtend(c fh.Ctx) error {
	id := pathParam(c, "id")
	var req LeaseRequest
	_ = decodeJSON(c, &req)
	req.MessageID = id
	var m Message
	if req.Partitioned && a.Distributed != nil {
		var getErr error
		m, _, getErr = a.Distributed.GetPartitionQueueMessage(c.Context(), req.Queue, req.Partition, id)
		if getErr != nil {
			return WriteBrokerError(c, getErr)
		}
	} else {
		m, _, _ = a.Broker.store.Get(c.Context(), id)
	}
	if err := a.Broker.store.ExtendLease(c.Context(), req); err != nil {
		return WriteBrokerError(c, err)
	}
	a.Broker.notify.Publish(Notification{Type: "message.lease_extended", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityDebug, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"extend_by": req.VisibilityTimeout.String()}})
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

func (a AdminServer) v1MessageProgress(c fh.Ctx) error {
	id := pathParam(c, "id")
	var req struct {
		LeaseToken string   `json:"lease_token"`
		ConsumerID string   `json:"consumer_id"`
		Queue      string   `json:"queue"`
		Progress   Progress `json:"progress"`
	}
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	m, ok, err := a.Broker.store.Get(c.Context(), id)
	if err != nil {
		return httpError(c, err.Error(), 500)
	}
	if !ok {
		return notFound(c)
	}
	if m.LeaseToken != "" && (req.LeaseToken == "" || req.LeaseToken != m.LeaseToken) {
		return httpError(c, ErrLeaseLost.Error(), 409)
	}
	m.Status = MessageProgressing
	if m.TaskID == "" {
		m.TaskID = m.Headers["x-task-id"]
		m.TaskName = m.Headers["x-task-name"]
	}
	if err := a.Broker.store.Update(c.Context(), m); err != nil {
		return WriteBrokerError(c, err)
	}
	if req.Progress.UpdatedAt.IsZero() {
		req.Progress.UpdatedAt = time.Now()
	}
	if m.TaskID != "" {
		if err := a.Broker.updateTaskProgress(c.Context(), m.TaskID, req.Progress); err != nil {
			return WriteBrokerError(c, err)
		}
	}
	a.Broker.notify.Publish(Notification{Type: "message.progress", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: m.TaskID, Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"percent": req.Progress.Percent, "message": req.Progress.Message}})
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

// v1MessageRespond serves respond, complete and fail; op is the route verb.
func (a AdminServer) v1MessageRespond(op string) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		id := pathParam(c, "id")
		var req struct {
			LeaseToken    string            `json:"lease_token"`
			ConsumerID    string            `json:"consumer_id"`
			Queue         string            `json:"queue"`
			CorrelationID string            `json:"correlation_id"`
			Payload       json.RawMessage   `json:"payload"`
			Error         *BrokerError      `json:"error"`
			Headers       map[string]string `json:"headers"`
			Ack           bool              `json:"ack"`
		}
		if decodeJSON(c, &req) != nil {
			return httpError(c, "invalid json", 400)
		}
		m, ok, err := a.Broker.store.Get(c.Context(), id)
		if err != nil {
			return httpError(c, err.Error(), 500)
		}
		if !ok {
			return notFound(c)
		}
		if m.LeaseToken != "" && (req.LeaseToken == "" || req.LeaseToken != m.LeaseToken) {
			return httpError(c, ErrLeaseLost.Error(), 409)
		}
		corr := firstNonEmpty(req.CorrelationID, m.CorrelationID)
		resp := Response{MessageID: a.Broker.nextID("resp"), CorrelationID: corr, Payload: []byte(req.Payload), Headers: req.Headers, Status: ResponseOK, Error: req.Error}
		if req.Error != nil || op == "fail" {
			resp.Status = ResponseError
		}
		if req.Ack || op == "complete" {
			if err := a.Broker.store.Ack(c.Context(), AckRequest{MessageID: id, Queue: req.Queue, LeaseToken: req.LeaseToken, ConsumerID: req.ConsumerID}); err != nil {
				return WriteBrokerError(c, err)
			}
			a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
			var taskErr error
			if req.Error != nil || op == "fail" {
				taskErr = a.Broker.completeTask(c.Context(), m, nil, req.Error)
			} else {
				taskErr = a.Broker.completeTask(c.Context(), m, []byte(req.Payload), nil)
			}
			if taskErr != nil {
				return WriteBrokerError(c, fmt.Errorf("message %s was acknowledged but task state could not be persisted: %w", id, taskErr))
			}
		}
		delivered := false
		if corr != "" {
			delivered = a.Broker.completeResponse(resp)
		}
		eventType := "message.responded"
		severity := SeverityInfo
		if op == "complete" {
			eventType = "task.completed"
		}
		if req.Error != nil || op == "fail" {
			eventType = "message.response_failed"
			if op == "fail" {
				eventType = "task.failed"
			}
			severity = SeverityError
		}
		a.Broker.notify.Publish(Notification{Type: eventType, Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: severity, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"correlation_id": corr, "delivered": delivered}})
		return writeBrokerJSON(c, map[string]any{"ok": true, "delivered": delivered})
	}
}

// ---- /v1/consumers/:id ----

// consumerParam returns the :id consumer, hiding consumers outside a
// tenant-scoped principal's tenants as not found.
func (a AdminServer) consumerParam(c fh.Ctx) (string, bool, error) {
	id := pathParam(c, "id")
	if hasRestrictedTenantScope(c.Context()) {
		info, ok := a.Broker.Consumers().Get(id)
		if !ok || !tenantAllowed(c.Context(), info.TenantID) {
			return "", false, notFound(c)
		}
	}
	return id, true, nil
}

func (a AdminServer) v1ConsumerGet(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, found := a.Broker.Consumers().Get(id)
	if !found || !tenantAllowed(c.Context(), info.TenantID) {
		return notFound(c)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerHeartbeat(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, err := a.Broker.RemoteConsumerHeartbeat(c.Context(), id)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return httpError(c, err.Error(), fh.StatusForbidden)
		}
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerSubscribe(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	var sub RemoteSubscription
	if decodeJSON(c, &sub) != nil {
		return httpError(c, "invalid json", 400)
	}
	if !sub.AutoCreate {
		sub.AutoCreate = true
	}
	info, err := a.Broker.RemoteConsumerSubscribe(c.Context(), id, sub)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return httpError(c, err.Error(), fh.StatusForbidden)
		}
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerPause(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, err := a.Broker.SetRemoteConsumerState(c.Context(), id, ConsumerPaused, "remote pause")
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return httpError(c, err.Error(), fh.StatusForbidden)
		}
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerResume(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, err := a.Broker.SetRemoteConsumerState(c.Context(), id, ConsumerReady, "remote resume")
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return httpError(c, err.Error(), fh.StatusForbidden)
		}
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerDrain(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, err := a.Broker.SetRemoteConsumerState(c.Context(), id, ConsumerDraining, "remote drain")
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerBan(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	reason := queryString(c, "reason")
	if reason == "" {
		reason = "remote ban"
	}
	info, err := a.Broker.BanRemoteConsumer(c.Context(), id, reason)
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerUnban(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	info, err := a.Broker.UnbanRemoteConsumer(c.Context(), id)
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1ConsumerUnregister(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	if err := a.Broker.UnregisterRemoteConsumer(c.Context(), id); err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

func (a AdminServer) v1ConsumerEvents(c fh.Ctx) error {
	id, ok, err := a.consumerParam(c)
	if !ok {
		return err
	}
	after := queryString(c, "after")
	wait := queryDuration(c, "wait", 0)
	deadline := time.Now().Add(wait)
	for {
		evs := a.Broker.Consumers().Events(id, after, queryLimit(c, 100))
		if len(evs) > 0 || wait <= 0 || time.Now().After(deadline) {
			return writeBrokerJSON(c, evs)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-c.Context().Done():
			return nil
		}
	}
}

// ---- /v1/tasks/:id ----

// taskParam loads the :id task, hiding tasks outside the caller's tenants.
func (a AdminServer) taskParam(c fh.Ctx) (taskHandle, TaskInfo, bool, error) {
	h := taskHandle{b: a.Broker, id: pathParam(c, "id")}
	info, err := h.Info(c.Context())
	if err != nil {
		return h, info, false, httpError(c, err.Error(), 404)
	}
	if !tenantAllowed(c.Context(), info.TenantID) {
		return h, info, false, httpError(c, "task not found", fh.StatusNotFound)
	}
	return h, info, true, nil
}

func (a AdminServer) v1TaskGet(c fh.Ctx) error {
	_, info, ok, err := a.taskParam(c)
	if !ok {
		return err
	}
	return writeBrokerJSON(c, info)
}

func (a AdminServer) v1TaskResult(c fh.Ctx) error {
	_, info, ok, err := a.taskParam(c)
	if !ok {
		return err
	}
	return writeBrokerJSON(c, map[string]any{"task": info, "payload": json.RawMessage(info.Result)})
}

func (a AdminServer) v1TaskCancel(c fh.Ctx) error {
	h, _, ok, err := a.taskParam(c)
	if !ok {
		return err
	}
	var req struct{ Reason string }
	_ = decodeJSON(c, &req)
	if err := h.Cancel(c.Context(), req.Reason); err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

func (a AdminServer) v1TaskRetry(c fh.Ctx) error {
	h, _, ok, err := a.taskParam(c)
	if !ok {
		return err
	}
	if err := h.Retry(c.Context()); err != nil {
		return WriteBrokerError(c, err)
	}
	return writeBrokerJSON(c, map[string]bool{"ok": true})
}

func (a AdminServer) v1TaskLogs(c fh.Ctx) error {
	h, _, ok, err := a.taskParam(c)
	if !ok {
		return err
	}
	logs, err := h.Logs(c.Context())
	if err != nil {
		return httpError(c, err.Error(), 404)
	}
	return writeBrokerJSON(c, logs)
}
func (a AdminServer) v1TaskSubmit(c fh.Ctx) error {
	var req struct {
		Name        string            `json:"name"`
		Queue       string            `json:"queue"`
		TenantID    string            `json:"tenant_id"`
		Payload     json.RawMessage   `json:"payload"`
		Priority    int               `json:"priority"`
		MaxAttempts int               `json:"max_attempts"`
		Metadata    map[string]string `json:"metadata"`
	}
	if decodeJSON(c, &req) != nil {
		return httpError(c, "invalid json", 400)
	}
	tenant, err := resolveTenant(c.Context(), req.TenantID)
	if err != nil {
		return httpError(c, err.Error(), fh.StatusForbidden)
	}
	req.TenantID = tenant
	h, err := a.Broker.Publisher().SubmitTask(c.Context(), req.Name, []byte(req.Payload), WithTaskQueue(req.Queue), WithTaskTenantID(req.TenantID), WithTaskPriority(req.Priority), WithTaskMaxAttempts(req.MaxAttempts), WithTaskMetadata(req.Metadata))
	if err != nil {
		return WriteBrokerError(c, err)
	}
	info, _ := h.Info(c.Context())
	return writeBrokerJSON(c, info)
}
func (a AdminServer) v1SagaCreate(c fh.Ctx) error {
	var def SagaDefinition
	if decodeJSON(c, &def) != nil {
		return httpError(c, "invalid json", 400)
	}
	if def.Name == "" {
		return httpError(c, "saga name required", 400)
	}
	if len(def.Steps) == 0 {
		return httpError(c, "saga steps required", 400)
	}
	inst, err := a.Broker.SagaEngine().CreateSagaChecked(c.Context(), def)
	if err != nil {
		return httpError(c, err.Error(), fh.StatusForbidden)
	}
	if err := a.Broker.SagaEngine().ExecuteSaga(c.Context(), inst.ID); err != nil {
		return WriteBrokerError(c, err)
	}
	snapshot := inst.clone()
	return writeBrokerJSON(c, &snapshot)
}
