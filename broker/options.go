package broker

import "time"

type PublishOptions struct {
	MessageID           string
	Key                 string
	RoutingKey          string
	Type                string
	ContentType         string
	Headers             map[string]string
	Metadata            map[string]string
	TraceID             string
	CorrelationID       string
	CausationID         string
	IdempotencyKey      string
	DeduplicationID     string
	DeduplicationWindow time.Duration
	Priority            int
	Delay               time.Duration
	RunAt               time.Time
	TTL                 time.Duration
	TenantID            string
	ProducerID          string
	Queue               string
	Stream              string
	ConfirmLevel        ConfirmLevel
	RequireDestination  bool
	MaxAttempts         int
	RetryPolicy         RetryPolicy
}

type PublishOption func(*PublishOptions)

type ConfirmLevel string

const (
	ConfirmAccepted   ConfirmLevel = "accepted"
	ConfirmValidated  ConfirmLevel = "validated"
	ConfirmRouted     ConfirmLevel = "routed"
	ConfirmEnqueued   ConfirmLevel = "enqueued"
	ConfirmPersisted  ConfirmLevel = "persisted"
	ConfirmReplicated ConfirmLevel = "replicated"
	ConfirmCommitted  ConfirmLevel = "committed"
	ConfirmDelivered  ConfirmLevel = "delivered"
	ConfirmProcessed  ConfirmLevel = "processed"
)

func WithMessageID(v string) PublishOption  { return func(o *PublishOptions) { o.MessageID = v } }
func WithKey(v string) PublishOption        { return func(o *PublishOptions) { o.Key = v } }
func WithRoutingKey(v string) PublishOption { return func(o *PublishOptions) { o.RoutingKey = v } }
func WithType(v string) PublishOption       { return func(o *PublishOptions) { o.Type = v } }
func WithHeader(k, v string) PublishOption {
	return func(o *PublishOptions) {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers[k] = v
	}
}
func WithHeaders(v map[string]string) PublishOption {
	return func(o *PublishOptions) { o.Headers = cloneMap(v) }
}
func WithMetadata(v map[string]string) PublishOption {
	return func(o *PublishOptions) { o.Metadata = cloneMap(v) }
}
func WithTraceID(v string) PublishOption { return func(o *PublishOptions) { o.TraceID = v } }
func WithCorrelationID(v string) PublishOption {
	return func(o *PublishOptions) { o.CorrelationID = v }
}
func WithCausationID(v string) PublishOption { return func(o *PublishOptions) { o.CausationID = v } }
func WithIdempotencyKey(v string) PublishOption {
	return func(o *PublishOptions) { o.IdempotencyKey = v }
}
func WithDeduplication(id string, window time.Duration) PublishOption {
	return func(o *PublishOptions) { o.DeduplicationID = id; o.DeduplicationWindow = window }
}
func WithPriority(v int) PublishOption        { return func(o *PublishOptions) { o.Priority = v } }
func WithDelay(v time.Duration) PublishOption { return func(o *PublishOptions) { o.Delay = v } }
func WithRunAt(v time.Time) PublishOption     { return func(o *PublishOptions) { o.RunAt = v } }
func WithTTL(v time.Duration) PublishOption   { return func(o *PublishOptions) { o.TTL = v } }
func WithTenantID(v string) PublishOption     { return func(o *PublishOptions) { o.TenantID = v } }
func WithProducerID(v string) PublishOption   { return func(o *PublishOptions) { o.ProducerID = v } }
func WithQueue(v string) PublishOption        { return func(o *PublishOptions) { o.Queue = v } }
func WithStream(v string) PublishOption       { return func(o *PublishOptions) { o.Stream = v } }
func WithConfirmLevel(v ConfirmLevel) PublishOption {
	return func(o *PublishOptions) { o.ConfirmLevel = v }
}
func WithRequireDestination(v bool) PublishOption {
	return func(o *PublishOptions) { o.RequireDestination = v }
}
func WithMaxAttempts(v int) PublishOption { return func(o *PublishOptions) { o.MaxAttempts = v } }
func WithRetryPolicy(v RetryPolicy) PublishOption {
	return func(o *PublishOptions) { o.RetryPolicy = v }
}

type RequestOptions struct {
	Timeout          time.Duration
	ReplyTo          string
	CorrelationID    string
	ContentType      string
	MaxResponseBytes int64
	RequireHandler   bool
	RetryPolicy      RetryPolicy
	IdempotencyKey   string
	Headers          map[string]string
	TenantID         string
}

type RequestOption func(*RequestOptions)

func WithTimeout(v time.Duration) RequestOption { return func(o *RequestOptions) { o.Timeout = v } }
func WithReplyTo(v string) RequestOption        { return func(o *RequestOptions) { o.ReplyTo = v } }
func WithRequireHandler(v bool) RequestOption {
	return func(o *RequestOptions) { o.RequireHandler = v }
}
func WithRequestHeader(k, v string) RequestOption {
	return func(o *RequestOptions) {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers[k] = v
	}
}
func WithRequestCorrelationID(v string) RequestOption {
	return func(o *RequestOptions) { o.CorrelationID = v }
}
func WithRequestIdempotencyKey(v string) RequestOption {
	return func(o *RequestOptions) { o.IdempotencyKey = v }
}
func WithRequestTenantID(v string) RequestOption { return func(o *RequestOptions) { o.TenantID = v } }

type TaskOptions struct {
	ID             string
	Queue          string
	Priority       int
	Delay          time.Duration
	RunAt          time.Time
	Timeout        time.Duration
	MaxAttempts    int
	RetryPolicy    RetryPolicy
	Metadata       map[string]string
	IdempotencyKey string
	TenantID       string
	ProgressTopic  string
	ResultTopic    string
}

type TaskOption func(*TaskOptions)

func WithTaskID(v string) TaskOption               { return func(o *TaskOptions) { o.ID = v } }
func WithTaskQueue(v string) TaskOption            { return func(o *TaskOptions) { o.Queue = v } }
func WithTaskPriority(v int) TaskOption            { return func(o *TaskOptions) { o.Priority = v } }
func WithTaskDelay(v time.Duration) TaskOption     { return func(o *TaskOptions) { o.Delay = v } }
func WithTaskRunAt(v time.Time) TaskOption         { return func(o *TaskOptions) { o.RunAt = v } }
func WithTaskTimeout(v time.Duration) TaskOption   { return func(o *TaskOptions) { o.Timeout = v } }
func WithTaskMaxAttempts(v int) TaskOption         { return func(o *TaskOptions) { o.MaxAttempts = v } }
func WithTaskRetryPolicy(v RetryPolicy) TaskOption { return func(o *TaskOptions) { o.RetryPolicy = v } }
func WithTaskMetadata(v map[string]string) TaskOption {
	return func(o *TaskOptions) { o.Metadata = cloneMap(v) }
}
func WithTaskIdempotencyKey(v string) TaskOption {
	return func(o *TaskOptions) { o.IdempotencyKey = v }
}
func WithTaskTenantID(v string) TaskOption      { return func(o *TaskOptions) { o.TenantID = v } }
func WithTaskProgressTopic(v string) TaskOption { return func(o *TaskOptions) { o.ProgressTopic = v } }
func WithTaskResultTopic(v string) TaskOption   { return func(o *TaskOptions) { o.ResultTopic = v } }

type TxOption func(*TxOptions)
type TxOptions struct{}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
