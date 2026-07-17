package broker

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type PollRequest struct {
	Queue, ConsumerID, ConsumerGroup string
	Limit                            int
	WaitNS, VisibilityTimeoutNS      int64
}
type PublishHTTPPayload struct {
	Topic, Queue, Stream, Type string
	Payload                    json.RawMessage
	Headers, Metadata          map[string]string
	Key, RoutingKey            string
	Priority                   int
	TenantID, CorrelationID    string
	TimeoutNS                  int64
	IdempotencyKey             string
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
	case path == "/publish/batch":
		a.v1PublishBatch(w, r)
	case path == "/request":
		a.v1Request(w, r)
	case path == "/consumers" && r.Method == http.MethodGet:
		writeBrokerJSON(w, a.Broker.Consumers().List())
	case path == "/consumers/register":
		a.v1ConsumerRegister(w, r)
	case strings.HasPrefix(path, "/consumers/"):
		a.v1Consumer(w, r, strings.TrimPrefix(path, "/consumers/"))
	case path == "/poll" || path == "/poll/batch":
		a.v1Poll(w, r)
	case path == "/poll/frame":
		a.v1PollFrame(w, r)
	case strings.HasPrefix(path, "/messages/"):
		a.v1Message(w, r, strings.TrimPrefix(path, "/messages/"))
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
		a.dlqQueue(w, r)
	case path == "/events":
		a.events(w, r)
	case path == "/sagas":
		if r.Method == http.MethodPost {
			a.v1SagaCreate(w, r)
		} else {
			writeBrokerJSON(w, a.Broker.SagaEngine().ListSagas())
		}
	case strings.HasPrefix(path, "/sagas/"):
		a.v1Saga(w, r, strings.TrimPrefix(path, "/sagas/"))
	case path == "/groups":
		writeBrokerJSON(w, a.Broker.GroupRebalancer().ListGroups())
	case strings.HasPrefix(path, "/groups/"):
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
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, res)
}
func (a AdminServer) v1Publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req PublishHTTPPayload
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	opts := []PublishOption{WithHeaders(req.Headers), WithMetadata(req.Metadata), WithType(req.Type), WithKey(req.Key), WithRoutingKey(req.RoutingKey), WithPriority(req.Priority), WithTenantID(req.TenantID), WithCorrelationID(req.CorrelationID), WithIdempotencyKey(req.IdempotencyKey)}
	if req.Queue != "" {
		opts = append(opts, WithQueue(req.Queue))
	}
	if req.Stream != "" {
		opts = append(opts, WithStream(req.Stream))
	}
	res, err := a.Broker.Publisher().Publish(r.Context(), req.Topic, []byte(req.Payload), opts...)
	if err != nil {
		http.Error(w, err.Error(), 409)
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
	results := []PublishResult{}
	errs := []string{}
	for _, m := range req.Messages {
		opts := []PublishOption{WithHeaders(m.Headers), WithMetadata(m.Metadata), WithType(m.Type), WithKey(m.Key), WithRoutingKey(m.RoutingKey), WithPriority(m.Priority), WithTenantID(m.TenantID), WithCorrelationID(m.CorrelationID), WithIdempotencyKey(m.IdempotencyKey)}
		if m.Queue != "" {
			opts = append(opts, WithQueue(m.Queue))
		}
		if m.Stream != "" {
			opts = append(opts, WithStream(m.Stream))
		}
		res, err := a.Broker.Publisher().Publish(r.Context(), m.Topic, []byte(m.Payload), opts...)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		results = append(results, res)
	}
	writeBrokerJSON(w, BatchPublishResult{Results: results, Errors: errs})
}
func (a AdminServer) v1Request(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req PublishHTTPPayload
	if json.NewDecoder(r.Body).Decode(&req) != nil {
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
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, info)
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
			http.Error(w, err.Error(), 409)
			return
		}
		writeBrokerJSON(w, info)
	case "pause":
		info, err := a.Broker.SetRemoteConsumerState(r.Context(), id, ConsumerPaused, "remote pause")
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
	case "resume":
		info, err := a.Broker.SetRemoteConsumerState(r.Context(), id, ConsumerReady, "remote resume")
		if err != nil {
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

func (a AdminServer) v1PollFrame(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodBroker(w)
		return
	}
	var req PollRequest
	if r.Method == http.MethodPost && r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Queue == "" {
		req.Queue = r.URL.Query().Get("queue")
	}
	if req.ConsumerID == "" {
		req.ConsumerID = firstNonEmpty(r.URL.Query().Get("consumer_id"), r.URL.Query().Get("consumer"))
	}
	if req.ConsumerGroup == "" {
		req.ConsumerGroup = firstNonEmpty(r.URL.Query().Get("consumer_group"), r.URL.Query().Get("group"))
	}
	if req.Limit <= 0 {
		req.Limit = queryLimit(r, defaultAdaptivePullLimit)
	}
	if req.ConsumerID == "" || req.Queue == "" {
		http.Error(w, "consumer_id and queue required", 400)
		return
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
	for {
		msgs, err := a.Broker.store.Lease(r.Context(), LeaseRequest{Queue: req.Queue, ConsumerID: req.ConsumerID, ConsumerGroup: firstNonEmpty(req.ConsumerGroup, req.ConsumerID), Limit: req.Limit, VisibilityTimeout: vis})
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		if len(msgs) > 0 || wait <= 0 || time.Now().After(deadline) {
			if len(msgs) > 0 {
				a.Broker.Consumers().AddStats(req.ConsumerID, int64(len(msgs)), 0, 0, 0, "")
				for i := range msgs {
					msgs[i].ConsumerID = req.ConsumerID
					msgs[i].ConsumerGroup = firstNonEmpty(req.ConsumerGroup, req.ConsumerID)
					msgs[i].Status = MessageProcessing
					_ = a.Broker.store.Update(r.Context(), msgs[i])
				}
			}
			writeFrameBatch(w, a.Broker.FrameCodec(), msgs)
			return
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
}

func writeFrameBatch(w http.ResponseWriter, codec FrameCodec, msgs []Message) {
	w.Header().Set("Content-Type", "application/vnd.pool.broker.frames")
	for _, msg := range msgs {
		frame, err := codec.EncodeMessage(nil, msg)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], uint64(len(frame)))
		_, _ = w.Write(buf[:n])
		_, _ = w.Write(frame)
	}
}
func (a AdminServer) v1Poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodBroker(w)
		return
	}
	var req PollRequest
	if r.Method == http.MethodPost && r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Queue == "" {
		req.Queue = r.URL.Query().Get("queue")
	}
	if req.ConsumerID == "" {
		req.ConsumerID = firstNonEmpty(r.URL.Query().Get("consumer_id"), r.URL.Query().Get("consumer"))
	}
	if req.ConsumerGroup == "" {
		req.ConsumerGroup = firstNonEmpty(r.URL.Query().Get("consumer_group"), r.URL.Query().Get("group"))
	}
	if req.Limit <= 0 {
		req.Limit = queryLimit(r, defaultAdaptivePullLimit)
	}
	if req.ConsumerID == "" || req.Queue == "" {
		http.Error(w, "consumer_id and queue required", 400)
		return
	}
	if info, ok := a.Broker.Consumers().Get(req.ConsumerID); ok && (info.Paused || info.Draining || info.Banned || info.State == ConsumerPaused || info.State == ConsumerDraining || info.State == ConsumerStopped || info.State == ConsumerBanned) {
		writeBrokerJSON(w, []Message{})
		return
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
	for {
		msgs, err := a.Broker.store.Lease(r.Context(), LeaseRequest{Queue: req.Queue, ConsumerID: req.ConsumerID, ConsumerGroup: firstNonEmpty(req.ConsumerGroup, req.ConsumerID), Limit: req.Limit, VisibilityTimeout: vis})
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		if len(msgs) > 0 || wait <= 0 || time.Now().After(deadline) {
			if len(msgs) > 0 {
				a.Broker.Consumers().AddStats(req.ConsumerID, int64(len(msgs)), 0, 0, 0, "")
				for i := range msgs {
					msgs[i].ConsumerID = req.ConsumerID
					msgs[i].ConsumerGroup = firstNonEmpty(req.ConsumerGroup, req.ConsumerID)
					msgs[i].Status = MessageProcessing
					_ = a.Broker.store.Update(r.Context(), msgs[i])
					if tid := firstNonEmpty(msgs[i].TaskID, msgs[i].Headers["x-task-id"]); tid != "" {
						a.Broker.startTask(r.Context(), tid)
					}
					a.Broker.emitConsumerNotification(req.ConsumerID, Notification{Type: "message.leased", Source: a.Broker.cfg.Name, Severity: SeverityInfo, MessageID: msgs[i].ID, TaskID: firstNonEmpty(msgs[i].TaskID, msgs[i].Headers["x-task-id"]), Topic: msgs[i].Topic, Queue: req.Queue, Consumer: req.ConsumerID, ConsumerGroup: req.ConsumerGroup, Timestamp: time.Now(), Data: map[string]any{"lease_until": msgs[i].LeaseUntil, "attempts": msgs[i].Attempts}})
				}
			}
			writeBrokerJSON(w, msgs)
			return
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
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
		// Validate lease token if message is leased
		m, hadMessage, _ := a.Broker.store.Get(r.Context(), id)
		if hadMessage && m.LeaseToken != "" && req.LeaseToken == "" {
			http.Error(w, "lease token required for leased message", 403)
			return
		}
		if hadMessage && m.LeaseToken != "" && req.LeaseToken != m.LeaseToken {
			http.Error(w, "invalid lease token", 403)
			return
		}
		if err := a.Broker.store.Ack(r.Context(), req); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		a.Broker.Consumers().AddStats(req.ConsumerID, 0, 1, 0, 0, "")
		if hadMessage {
			a.Broker.completeTask(r.Context(), m, nil, nil)
			a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, Timestamp: time.Now()})
		} else {
			a.Broker.notify.Publish(Notification{Type: "message.acked", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, Queue: req.Queue, Consumer: req.ConsumerID, Timestamp: time.Now()})
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "nack", "reject":
		var req NackRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.MessageID = id
		// Validate lease token if message is leased
		m, hadMessage, _ := a.Broker.store.Get(r.Context(), id)
		if hadMessage && m.LeaseToken != "" && req.LeaseToken == "" {
			http.Error(w, "lease token required for leased message", 403)
			return
		}
		if hadMessage && m.LeaseToken != "" && req.LeaseToken != m.LeaseToken {
			http.Error(w, "invalid lease token", 403)
			return
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
			http.Error(w, err.Error(), 409)
			return
		}
		a.Broker.Consumers().AddStats(req.ConsumerID, 0, 0, 1, 0, req.Error)
		if req.Action == NackReject || req.Action == NackDLQ {
			if hadMessage {
				tid := firstNonEmpty(m.TaskID, m.Headers["x-task-id"])
				if tid != "" {
					a.Broker.failTask(r.Context(), tid, errf(ErrPermanent, firstNonEmpty(req.Error, req.Reason)))
				}
			}
		}
		if hadMessage {
			a.Broker.notify.Publish(Notification{Type: "message." + op, Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityWarn, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, Timestamp: time.Now(), Data: map[string]any{"action": req.Action, "reason": firstNonEmpty(req.Reason, req.Error)}})
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "extend":
		var req LeaseRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.MessageID = id
		m, _, _ := a.Broker.store.Get(r.Context(), id)
		if err := a.Broker.store.ExtendLease(r.Context(), req); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		a.Broker.notify.Publish(Notification{Type: "message.lease_extended", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityDebug, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, Timestamp: time.Now(), Data: map[string]any{"extend_by": req.VisibilityTimeout.String()}})
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
		a.Broker.notify.Publish(Notification{Type: "message.progress", Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: SeverityInfo, MessageID: id, TaskID: m.TaskID, Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, Timestamp: time.Now(), Data: map[string]any{"percent": req.Progress.Percent, "message": req.Progress.Message}})
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
		a.Broker.notify.Publish(Notification{Type: eventType, Source: firstNonEmpty(req.ConsumerID, "remote-consumer"), Severity: severity, MessageID: id, TaskID: firstNonEmpty(m.TaskID, m.Headers["x-task-id"]), Topic: m.Topic, Queue: firstNonEmpty(req.Queue, m.Queue), Consumer: req.ConsumerID, ConsumerGroup: m.ConsumerGroup, Timestamp: time.Now(), Data: map[string]any{"correlation_id": corr, "delivered": delivered}})
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
	h, err := a.Broker.Publisher().SubmitTask(r.Context(), req.Name, []byte(req.Payload), WithTaskQueue(req.Queue), WithTaskTenantID(req.TenantID), WithTaskPriority(req.Priority), WithTaskMaxAttempts(req.MaxAttempts), WithTaskMetadata(req.Metadata))
	if err != nil {
		http.Error(w, err.Error(), 409)
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
	if len(parts) == 1 {
		info, err := h.Info(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, info)
		return
	}
	switch parts[1] {
	case "result":
		info, err := h.Info(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, map[string]any{"task": info, "payload": json.RawMessage(info.Result)})
	case "cancel":
		var req struct{ Reason string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if err := h.Cancel(r.Context(), req.Reason); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		writeBrokerJSON(w, map[string]bool{"ok": true})
	case "retry":
		if err := h.Retry(r.Context()); err != nil {
			http.Error(w, err.Error(), 409)
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
	inst := a.Broker.SagaEngine().CreateSaga(r.Context(), def)
	if err := a.Broker.SagaEngine().ExecuteSaga(r.Context(), inst.ID); err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, inst.clone())
}

func (a AdminServer) v1Saga(w http.ResponseWriter, r *http.Request, tail string) {
	id := strings.Trim(tail, "/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	s, ok := a.Broker.SagaEngine().GetSaga(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeBrokerJSON(w, s)
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
