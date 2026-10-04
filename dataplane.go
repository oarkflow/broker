package broker

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type PollRequest struct {
	Queue               string `json:"queue"`
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
func decodePublishHTTPPayload(r *http.Request, dst *PublishHTTPPayload) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, dst)
}

func (a AdminServer) dataPlane(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1")
	if path == "" {
		path = "/"
	}
	switch {
	case path == "/publish":
		a.v1Publish(w, r)
	case path == "/publish/frame":
		a.v1PublishFrame(w, r)
	case path == "/publish/frame/batch":
		a.v1PublishFrameBatch(w, r)
	case path == "/publish/batch":
		a.v1PublishBatch(w, r)
	case path == "/request":
		a.v1Request(w, r)
	case path == "/broker":
		a.v1BrokerLifecycle(w, r)
	case strings.HasPrefix(path, "/topics/"):
		a.v1Topic(w, r, strings.TrimPrefix(path, "/topics/"))
	case path == "/consumers" && r.Method == http.MethodGet:
		writeBrokerJSON(w, a.Broker.Consumers().ListContext(r.Context()))
	case path == "/consumers/register":
		a.v1ConsumerRegister(w, r)
	case strings.HasPrefix(path, "/consumers/"):
		a.v1Consumer(w, r, strings.TrimPrefix(path, "/consumers/"))
	case path == "/poll" || path == "/poll/batch":
		a.v1Poll(w, r)
	case path == "/poll/frame":
		a.v1PollFrame(w, r)
	case path == "/ack/frame":
		a.v1AckFrameBatch(w, r)
	case strings.HasPrefix(path, "/messages/"):
		a.v1Message(w, r, strings.TrimPrefix(path, "/messages/"))
	case path == "/tasks" && r.Method == http.MethodGet:
		writeBrokerJSON(w, a.Broker.ListTasks(r.Context()))
	case path == "/tasks/submit":
		a.v1TaskSubmit(w, r)
	case strings.HasPrefix(path, "/tasks/"):
		a.v1Task(w, r, strings.TrimPrefix(path, "/tasks/"))
	case path == "/streams":
		a.streams(w, r)
	case strings.HasPrefix(path, "/streams/"):
		r2 := new(http.Request)
		*r2 = *r
		r2.URL = cloneURL(r.URL)
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/v1")
		a.stream(w, r2)
	case path == "/dlq":
		a.dlq(w, r)
	case strings.HasPrefix(path, "/dlq/"):
		r2 := new(http.Request)
		*r2 = *r
		r2.URL = cloneURL(r.URL)
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/v1")
		a.dlqQueue(w, r2)
	case path == "/events":
		a.events(w, r)
	case path == "/sagas":
		if r.Method == http.MethodPost {
			a.v1SagaCreate(w, r)
		} else {
			writeBrokerJSON(w, a.Broker.SagaEngine().ListSagasContext(r.Context()))
		}
	case strings.HasPrefix(path, "/sagas/"):
		a.v1Saga(w, r, strings.TrimPrefix(path, "/sagas/"))
	case path == "/groups":
		if rejectScopedControlPlane(w, r, "global consumer groups") {
			return
		}
		writeBrokerJSON(w, a.Broker.GroupRebalancer().ListGroups())
	case strings.HasPrefix(path, "/groups/"):
		if rejectScopedControlPlane(w, r, "global consumer groups") {
			return
		}
		group := strings.TrimPrefix(path, "/groups/")
		g := a.Broker.GroupRebalancer().GetGroup(group)
		if g == nil {
			http.NotFound(w, r)
			return
		}
		writeBrokerJSON(w, g)
	default:
		http.NotFound(w, r)
	}
}

func (a AdminServer) v1PublishFrame(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	var msg Message
	if err := a.Broker.FrameCodec().DecodeMessage(data, &msg); err != nil {
		http.Error(w, err.Error(), 400)
		return
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
	res, err := a.Broker.Publisher().Publish(r.Context(), topic, msg.Payload, opts...)
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	writeBrokerJSON(w, res)
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

func (a AdminServer) v1PublishFrameBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	messages, err := readFrameBatch(r.Body, a.Broker.FrameCodec(), 1<<20, r.ContentLength)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(messages) == 0 {
		http.Error(w, "empty frame batch", http.StatusBadRequest)
		return
	}
	if preferMinimal(r) {
		errs, first := a.Broker.Publisher().publishMessagesMinimal(r.Context(), messages)
		writeMinimalPublishResponse(w, errs, first)
		return
	}
	items := make([]PublishBatchItem, len(messages))
	for i := range messages {
		items[i] = PublishBatchItem{Topic: messages[i].Topic, Payload: messages[i].Payload, Options: publishOptionsFromFrame(messages[i])}
	}
	results, errs, first := a.Broker.Publisher().publishItemsAligned(r.Context(), items)
	errorsOut := make([]string, len(errs))
	failed := 0
	for i, err := range errs {
		if err != nil {
			errorsOut[i] = err.Error()
			failed++
		}
	}
	if failed > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
	}
	writeBrokerJSON(w, map[string]any{"results": results, "errors": errorsOut, "failed": failed, "error": errorString(first)})
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

func preferMinimal(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Prefer")), "return=minimal")
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeMinimalPublishResponse(w http.ResponseWriter, errs []error, first error) {
	failed := 0
	errorsOut := make([]string, len(errs))
	for i, err := range errs {
		if err != nil {
			failed++
			errorsOut[i] = err.Error()
		}
	}
	if failed == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMultiStatus)
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": errorsOut, "failed": failed, "error": errorString(first)})
}

func (a AdminServer) v1AckFrameBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	requests, err := decodeAckFrameBatch(r.Body, 1<<20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(requests) == 0 {
		http.Error(w, "empty ack batch", http.StatusBadRequest)
		return
	}
	errs := make([]error, len(requests))
	if batchStore, ok := a.Broker.store.(BatchAckStore); ok {
		errs = batchStore.AckBatch(r.Context(), requests)
	} else {
		for i := range requests {
			errs[i] = a.Broker.store.Ack(r.Context(), requests[i])
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
	if acked == len(requests) && preferMinimal(r) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if acked != len(requests) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
	}
	writeBrokerJSON(w, map[string]any{"acked": acked, "errors": errorsOut})
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

func (a AdminServer) v1Publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req PublishHTTPPayload
	if decodePublishHTTPPayload(r, &req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	payload, err := req.PayloadBytes()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	opts := req.PublishOptions()
	res, err := a.Broker.Publisher().Publish(r.Context(), req.Topic, payload, opts...)
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	writeBrokerJSON(w, res)
}
func (a AdminServer) v1PublishBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req struct {
		Messages []PublishHTTPPayload `json:"messages"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	if preferMinimal(r) {
		messages := make([]Message, len(req.Messages))
		for i := range req.Messages {
			messages[i] = messageFromHTTPPayload(req.Messages[i])
		}
		errs, first := a.Broker.Publisher().publishMessagesMinimal(r.Context(), messages)
		writeMinimalPublishResponse(w, errs, first)
		return
	}
	items := make([]PublishBatchItem, len(req.Messages))
	for i, m := range req.Messages {
		payload, perr := m.PayloadBytes()
		if perr != nil {
			http.Error(w, perr.Error(), 400)
			return
		}
		items[i] = PublishBatchItem{Topic: m.Topic, Payload: payload, Options: m.PublishOptions()}
	}
	res, _ := a.Broker.Publisher().PublishItems(r.Context(), items)
	writeBrokerJSON(w, res)
}
func (a AdminServer) v1Request(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req PublishHTTPPayload
	if decodePublishHTTPPayload(r, &req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	if req.Topic == "" {
		http.Error(w, "topic/subject required", 400)
		return
	}
	timeout := time.Duration(req.TimeoutNS)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	resp, err := a.Broker.Publisher().requestWithOptions(r.Context(), req.Topic, []byte(req.Payload), RequestOptions{Timeout: timeout, CorrelationID: req.CorrelationID, TenantID: req.TenantID, IdempotencyKey: req.IdempotencyKey})
	if err != nil {
		http.Error(w, err.Error(), 504)
		return
	}
	writeBrokerJSON(w, resp)
}
func (a AdminServer) v1ConsumerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req ConsumerRegistration
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	info, err := a.Broker.RegisterRemoteConsumer(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		WriteBrokerError(w, err)
		return
	}
	writeBrokerJSON(w, info)
}

// v1BrokerLifecycle exposes broker-wide pause and resume. Queue-level pause already
// existed under /queues, but there was no way to stop the whole broker over HTTP.
func (a AdminServer) v1BrokerLifecycle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeBrokerJSON(w, map[string]any{"state": a.Broker.State()})
	case http.MethodPost, http.MethodPut:
		var req struct {
			Action string `json:"action"`
			Reason string `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteBrokerError(w, errf(ErrInvalidMessage, "invalid body: %v", err))
			return
		}
		var err error
		switch req.Action {
		case "pause":
			err = a.Broker.Pause(r.Context())
		case "resume":
			err = a.Broker.Resume(r.Context())
		default:
			WriteBrokerError(w, errf(ErrInvalidMessage, "action must be pause or resume, got %q", req.Action))
			return
		}
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"state": a.Broker.State(), "reason": req.Reason})
	default:
		methodBroker(w)
	}
}

// v1Topic exposes topic retention. Topics had no read path over HTTP, so an operator
// could see only counts, never what had actually been published.
func (a AdminServer) v1Topic(w http.ResponseWriter, r *http.Request, tail string) {
	name := strings.Trim(tail, "/")
	if name == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodBroker(w)
		return
	}
	msgs, err := a.Broker.ReadTopic(r.Context(), name, r.URL.Query().Get("after"), queryLimit(r, 100))
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	writeBrokerJSON(w, msgs)
}

func (a AdminServer) v1Consumer(w http.ResponseWriter, r *http.Request, tail string) {
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	if len(parts) == 1 {
		if r.Method == http.MethodGet {
			if info, ok := a.Broker.Consumers().Get(id); ok {
				if !tenantAllowed(r.Context(), info.TenantID) {
					http.NotFound(w, r)
					return
				}
				writeBrokerJSON(w, info)
				return
			}
			http.NotFound(w, r)
			return
		}
		methodBroker(w)
		return
	}
	switch parts[1] {
	case "heartbeat":
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		info, err := a.Broker.RemoteConsumerHeartbeat(r.Context(), id)
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "subscribe":
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		var sub RemoteSubscription
		if json.NewDecoder(r.Body).Decode(&sub) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		if !sub.AutoCreate {
			sub.AutoCreate = true
		}
		info, err := a.Broker.RemoteConsumerSubscribe(r.Context(), id, sub)
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, info)
	case "pause":
		info, err := a.Broker.SetRemoteConsumerState(r.Context(), id, ConsumerPaused, "remote pause")
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "resume":
		info, err := a.Broker.SetRemoteConsumerState(r.Context(), id, ConsumerReady, "remote resume")
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "drain":
		info, err := a.Broker.SetRemoteConsumerState(r.Context(), id, ConsumerDraining, "remote drain")
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "ban":
		reason := r.URL.Query().Get("reason")
		if reason == "" {
			reason = "remote ban"
		}
		info, err := a.Broker.BanRemoteConsumer(r.Context(), id, reason)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "unban":
		info, err := a.Broker.UnbanRemoteConsumer(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "unregister":
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			methodBroker(w)
			return
		}
		if err := a.Broker.UnregisterRemoteConsumer(r.Context(), id); err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "events":
		after := r.URL.Query().Get("after")
		wait := queryDuration(r, "wait", 0)
		deadline := time.Now().Add(wait)
		for {
			evs := a.Broker.Consumers().Events(id, after, queryLimit(r, 100))
			if len(evs) > 0 || wait <= 0 || time.Now().After(deadline) {
				writeBrokerJSON(w, evs)
				return
			}
			select {
			case <-time.After(100 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
	default:
		http.NotFound(w, r)
	}
}

// parsePollRequest reads a poll request from either the JSON body or the query
// string, so both poll endpoints normalise identically.
func parsePollRequest(w http.ResponseWriter, r *http.Request) (PollRequest, bool) {
	var req PollRequest
	if r.Method == http.MethodPost && r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	q := r.URL.Query()
	if req.Queue == "" {
		req.Queue = q.Get("queue")
	}
	if req.ConsumerID == "" {
		req.ConsumerID = firstNonEmpty(q.Get("consumer_id"), q.Get("consumer"))
	}
	if req.ConsumerGroup == "" {
		req.ConsumerGroup = firstNonEmpty(q.Get("consumer_group"), q.Get("group"))
	}
	if req.Limit <= 0 {
		req.Limit = queryLimit(r, 1)
	}
	if req.ConsumerID == "" || req.Queue == "" {
		http.Error(w, "consumer_id and queue required", 400)
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
func (a AdminServer) leaseForConsumer(w http.ResponseWriter, r *http.Request, req PollRequest) ([]Message, bool) {
	tenant, err := resolveTenant(r.Context(), req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return nil, false
	}
	if a.consumerBlocked(req.ConsumerID) {
		return []Message{}, true
	}
	wait := time.Duration(req.WaitNS)
	if wait <= 0 {
		wait = queryDuration(r, "wait", 0)
	}
	deadline := time.Now().Add(wait)
	vis := time.Duration(req.VisibilityTimeoutNS)
	if vis <= 0 {
		vis = queryDuration(r, "visibility_timeout", 0)
	}
	leaseReq := LeaseRequest{
		Queue:             req.Queue,
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
		msgs, err := a.Broker.store.Lease(r.Context(), leaseReq)
		if err != nil {
			WriteBrokerError(w, err)
			return nil, false
		}
		if len(msgs) > 0 || wait <= 0 || time.Now().After(deadline) {
			if len(msgs) > 0 {
				a.Broker.Consumers().AddStats(req.ConsumerID, int64(len(msgs)), 0, 0, 0, "")
				group := firstNonEmpty(req.ConsumerGroup, req.ConsumerID)
				notify := a.Broker.cfg.EnableNotifications || a.Broker.cfg.EnableConsumerEventTracking
				for i := range msgs {
					msgs[i].ConsumerID = req.ConsumerID
					msgs[i].ConsumerGroup = group
					msgs[i].Status = MessageProcessing
					taskID := firstNonEmpty(msgs[i].TaskID, msgs[i].Headers["x-task-id"])
					if taskID != "" {
						a.Broker.startTask(r.Context(), taskID)
					}
					if notify {
						a.Broker.emitConsumerNotification(req.ConsumerID, Notification{
							Type: "message.leased", Source: a.Broker.cfg.Name, Severity: SeverityInfo,
							MessageID: msgs[i].ID, TaskID: taskID, Topic: msgs[i].Topic, Queue: req.Queue,
							Consumer: req.ConsumerID, ConsumerGroup: group, TenantID: msgs[i].TenantID,
							Timestamp: time.Now(),
							Data:      map[string]any{"lease_until": msgs[i].LeaseUntil, "attempts": msgs[i].Attempts},
						})
					}
				}
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
		case <-r.Context().Done():
			return nil, false
		}
	}
}

// pollRetryInterval is how long a long poll waits before re-checking the queue.
const pollRetryInterval = 25 * time.Millisecond

func (a AdminServer) v1PollFrame(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodBroker(w)
		return
	}
	req, ok := parsePollRequest(w, r)
	if !ok {
		return
	}
	msgs, ok := a.leaseForConsumer(w, r, req)
	if !ok {
		return
	}
	writeFrameBatch(w, a.Broker.FrameCodec(), msgs)
}

func writeFrameBatch(w http.ResponseWriter, codec FrameCodec, msgs []Message) {
	body, err := EncodeMessageFrameBatch(nil, codec, msgs)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.pool.broker.frames")
	_, _ = w.Write(body)
}

func (a AdminServer) v1Poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodBroker(w)
		return
	}
	req, ok := parsePollRequest(w, r)
	if !ok {
		return
	}
	msgs, ok := a.leaseForConsumer(w, r, req)
	if !ok {
		return
	}
	writeBrokerJSON(w, msgs)
}

func (a AdminServer) v1Message(w http.ResponseWriter, r *http.Request, tail string) {
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id, op := parts[0], parts[1]
	switch op {
	case "ack":
		var req AckRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.MessageID = id
		// Validate the lease token before the store does, so the failure is
		// reported as ErrLeaseLost with the same code and status the frame ack
		// endpoint and every in-process caller see. This path used to answer 403
		// with bespoke text while /v1/ack/frame answered 409 with ErrLeaseLost, so
		// the same logical failure was indistinguishable to a client.
		m, hadMessage, _ := a.Broker.store.Get(r.Context(), id)
		if hadMessage && m.LeaseToken != "" {
			if req.LeaseToken == "" {
				WriteBrokerError(w, errf(ErrLeaseLost, "lease token required for leased message %s", id))
				return
			}
			if req.LeaseToken != m.LeaseToken {
				WriteBrokerError(w, errf(ErrLeaseLost, "invalid lease token for message %s", id))
				return
			}
		}
		if err := a.Broker.store.Ack(r.Context(), req); err != nil {
			WriteBrokerError(w, err)
			return
		}
		a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
		if hadMessage {
			a.Broker.completeTask(r.Context(), m, nil, nil)
			a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now()})
		} else {
			a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, Queue: req.Queue, Consumer: req.ConsumerID, Timestamp: time.Now()})
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "nack", "reject":
		var req NackRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.MessageID = id
		// Validate the lease token before the store does, so the failure is
		// reported as ErrLeaseLost with the same code and status the frame ack
		// endpoint and every in-process caller see. This path used to answer 403
		// with bespoke text while /v1/ack/frame answered 409 with ErrLeaseLost, so
		// the same logical failure was indistinguishable to a client.
		m, hadMessage, _ := a.Broker.store.Get(r.Context(), id)
		if hadMessage && m.LeaseToken != "" {
			if req.LeaseToken == "" {
				WriteBrokerError(w, errf(ErrLeaseLost, "lease token required for leased message %s", id))
				return
			}
			if req.LeaseToken != m.LeaseToken {
				WriteBrokerError(w, errf(ErrLeaseLost, "invalid lease token for message %s", id))
				return
			}
		}
		if req.Action == "" {
			req.Action = NackRequeue
		}
		if op == "reject" {
			req.Action = NackReject
		}
		req.IncrementAttempt = true
		m, hadMessage, _ = a.Broker.store.Get(r.Context(), id)
		if err := a.Broker.store.Nack(r.Context(), req); err != nil {
			WriteBrokerError(w, err)
			return
		}
		a.Broker.Consumers().AddStats(req.ConsumerID, 0, 0, 1, 0, req.Error)
		if req.Action == NackReject || req.Action == NackDLQ {
			if hadMessage {
				tid := firstNonEmpty(m.TaskID, m.Headers["x-task-id"])
				if tid != "" {
					a.Broker.failTask(r.Context(), tid, errf(ErrPermanent, "%s", firstNonEmpty(req.Error, req.Reason)))
				}
			}
		}
		if hadMessage {
			a.Broker.notify.Publish(Notification{Type: "message." + op, Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityWarn, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"action": req.Action, "reason": firstNonEmpty(req.Reason, req.Error)}})
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "extend":
		var req LeaseRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.MessageID = id
		m, _, _ := a.Broker.store.Get(r.Context(), id)
		if err := a.Broker.store.ExtendLease(r.Context(), req); err != nil {
			WriteBrokerError(w, err)
			return
		}
		a.Broker.notify.Publish(Notification{Type: "message.lease_extended", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityDebug, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"extend_by": req.VisibilityTimeout.String()}})
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "progress":
		var req struct {
			LeaseToken string   `json:"lease_token"`
			ConsumerID string   `json:"consumer_id"`
			Queue      string   `json:"queue"`
			Progress   Progress `json:"progress"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		m, ok, err := a.Broker.store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		if m.LeaseToken != "" && (req.LeaseToken == "" || req.LeaseToken != m.LeaseToken) {
			http.Error(w, ErrLeaseLost.Error(), 409)
			return
		}
		m.Status = MessageProgressing
		if m.TaskID == "" {
			m.TaskID = m.Headers["x-task-id"]
			m.TaskName = m.Headers["x-task-name"]
		}
		_ = a.Broker.store.Update(r.Context(), m)
		if req.Progress.UpdatedAt.IsZero() {
			req.Progress.UpdatedAt = time.Now()
		}
		if m.TaskID != "" {
			a.Broker.updateTaskProgress(r.Context(), m.TaskID, req.Progress)
		}
		a.Broker.notify.Publish(Notification{Type: "message.progress", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: m.TaskID, Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, TenantID: m.TenantID, Timestamp: time.Now(), Data: map[string]any{"percent": req.Progress.Percent, "message": req.Progress.Message}})
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "respond", "complete", "fail":
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
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		m, ok, err := a.Broker.store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		if m.LeaseToken != "" && (req.LeaseToken == "" || req.LeaseToken != m.LeaseToken) {
			http.Error(w, ErrLeaseLost.Error(), 409)
			return
		}
		corr := firstNonEmpty(req.CorrelationID, m.CorrelationID)
		resp := Response{MessageID: a.Broker.nextID("resp"), CorrelationID: corr, Payload: []byte(req.Payload), Headers: req.Headers, Status: ResponseOK, Error: req.Error}
		if req.Error != nil || op == "fail" {
			resp.Status = ResponseError
		}
		delivered := false
		if corr != "" {
			delivered = a.Broker.completeResponse(resp)
		}
		if req.Error != nil || op == "fail" {
			a.Broker.completeTask(r.Context(), m, nil, req.Error)
		} else {
			a.Broker.completeTask(r.Context(), m, []byte(req.Payload), nil)
		}
		if req.Ack || op == "complete" {
			_ = a.Broker.store.Ack(r.Context(), AckRequest{MessageID: id, Queue: req.Queue, LeaseToken: req.LeaseToken, ConsumerID: req.ConsumerID})
			a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
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
		writeBrokerJSON(w, map[string]any{"ok": true, "delivered": delivered})
	default:
		http.NotFound(w, r)
	}
}
func (a AdminServer) v1TaskSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req struct {
		Name        string            `json:"name"`
		Queue       string            `json:"queue"`
		TenantID    string            `json:"tenant_id"`
		Payload     json.RawMessage   `json:"payload"`
		Priority    int               `json:"priority"`
		MaxAttempts int               `json:"max_attempts"`
		Metadata    map[string]string `json:"metadata"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	tenant, err := resolveTenant(r.Context(), req.TenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	req.TenantID = tenant
	h, err := a.Broker.Publisher().SubmitTask(r.Context(), req.Name, []byte(req.Payload), WithTaskQueue(req.Queue), WithTaskTenantID(req.TenantID), WithTaskPriority(req.Priority), WithTaskMaxAttempts(req.MaxAttempts), WithTaskMetadata(req.Metadata))
	if err != nil {
		WriteBrokerError(w, err)
		return
	}
	info, _ := h.Info(r.Context())
	writeBrokerJSON(w, info)
}
func (a AdminServer) v1Task(w http.ResponseWriter, r *http.Request, tail string) {
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	h := taskHandle{b: a.Broker, id: parts[0]}
	info, err := h.Info(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	if !tenantAllowed(r.Context(), info.TenantID) {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if len(parts) == 1 {
		writeBrokerJSON(w, info)
		return
	}
	switch parts[1] {
	case "result":
		writeBrokerJSON(w, map[string]any{"task": info, "payload": json.RawMessage(info.Result)})
	case "cancel":
		var req struct{ Reason string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if err := h.Cancel(r.Context(), req.Reason); err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "retry":
		if err := h.Retry(r.Context()); err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "logs":
		logs, err := h.Logs(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, logs)
	default:
		http.NotFound(w, r)
	}
}
func (a AdminServer) v1SagaCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var def SagaDefinition
	if json.NewDecoder(r.Body).Decode(&def) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	if def.Name == "" {
		http.Error(w, "saga name required", 400)
		return
	}
	if len(def.Steps) == 0 {
		http.Error(w, "saga steps required", 400)
		return
	}
	inst, err := a.Broker.SagaEngine().CreateSagaChecked(r.Context(), def)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if err := a.Broker.SagaEngine().ExecuteSaga(r.Context(), inst.ID); err != nil {
		WriteBrokerError(w, err)
		return
	}
	snapshot := inst.clone()
	writeBrokerJSON(w, &snapshot)
}

func (a AdminServer) v1Saga(w http.ResponseWriter, r *http.Request, tail string) {
	id := strings.Trim(tail, "/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	s, ok := a.Broker.SagaEngine().GetSagaContext(r.Context(), id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeBrokerJSON(w, &s)
}

func queryDuration(r *http.Request, key string, d time.Duration) time.Duration {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return d
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(n)
	}
	if v, err := time.ParseDuration(raw); err == nil {
		return v
	}
	return d
}

func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	v := *u
	return &v
}
