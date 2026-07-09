package broker

import (
	"context"
	"time"

	"github.com/oarkflow/broker/pkg/pool"
)

type Mode string

const (
	ModeEmbedded    Mode = "embedded"
	ModeStandalone  Mode = "standalone"
	ModeDistributed Mode = "distributed"
)

type MessageStatus string

const (
	MessageCreated        MessageStatus = "created"
	MessageValidated      MessageStatus = "validated"
	MessageAccepted       MessageStatus = "accepted"
	MessageRouted         MessageStatus = "routed"
	MessagePersisted      MessageStatus = "persisted"
	MessageScheduled      MessageStatus = "scheduled"
	MessageAvailable      MessageStatus = "available"
	MessageQueued         MessageStatus = "queued"
	MessageLeased         MessageStatus = "leased"
	MessageDelivered      MessageStatus = "delivered"
	MessageProcessing     MessageStatus = "processing"
	MessageProgressing    MessageStatus = "progressing"
	MessageAcked          MessageStatus = "acked"
	MessageCompleted      MessageStatus = "completed"
	MessageFailed         MessageStatus = "failed"
	MessageRetryScheduled MessageStatus = "retry_scheduled"
	MessageDeadLettered   MessageStatus = "dead_lettered"
	MessageCancelled      MessageStatus = "cancelled"
	MessageExpired        MessageStatus = "expired"
	MessageMoved          MessageStatus = "moved"
	MessageCopied         MessageStatus = "copied"
	MessageReplayed       MessageStatus = "replayed"
	MessageArchived       MessageStatus = "archived"
	MessagePurged         MessageStatus = "purged"
	MessageQuarantined    MessageStatus = "quarantined"
	MessageParked         MessageStatus = "parked"
)

type BrokerState string

const (
	BrokerConfigured   BrokerState = "configured"
	BrokerInitializing BrokerState = "initializing"
	BrokerRecovering   BrokerState = "recovering"
	BrokerStarting     BrokerState = "starting"
	BrokerReady        BrokerState = "ready"
	BrokerRunning      BrokerState = "running"
	BrokerDegraded     BrokerState = "degraded"
	BrokerPaused       BrokerState = "paused"
	BrokerDraining     BrokerState = "draining"
	BrokerStopping     BrokerState = "stopping"
	BrokerStopped      BrokerState = "stopped"
	BrokerFailed       BrokerState = "failed"
)

type QueueState string

const (
	QueueDeclared QueueState = "declared"
	QueueCreated  QueueState = "created"
	QueueActive   QueueState = "active"
	QueuePaused   QueueState = "paused"
	QueueDraining QueueState = "draining"
	QueueFull     QueueState = "full"
	QueueBlocked  QueueState = "blocked"
	QueueDeleted  QueueState = "deleted"
	QueuePurged   QueueState = "purged"
	QueueArchived QueueState = "archived"
)

type ConsumerState string

const (
	ConsumerRegistered    ConsumerState = "registered"
	ConsumerJoining       ConsumerState = "joining"
	ConsumerAssigned      ConsumerState = "assigned"
	ConsumerReady         ConsumerState = "ready"
	ConsumerPolling       ConsumerState = "polling"
	ConsumerIdle          ConsumerState = "idle"
	ConsumerProcessing    ConsumerState = "processing"
	ConsumerBackpressured ConsumerState = "backpressured"
	ConsumerPaused        ConsumerState = "paused"
	ConsumerDraining      ConsumerState = "draining"
	ConsumerLeaving       ConsumerState = "leaving"
	ConsumerDisconnected  ConsumerState = "disconnected"
	ConsumerFailed        ConsumerState = "failed"
	ConsumerBanned        ConsumerState = "banned"
	ConsumerStopped       ConsumerState = "stopped"
)

type TaskStatus string

const (
	TaskCreated      TaskStatus = "created"
	TaskValidated    TaskStatus = "validated"
	TaskScheduled    TaskStatus = "scheduled"
	TaskQueued       TaskStatus = "queued"
	TaskLeased       TaskStatus = "leased"
	TaskRunning      TaskStatus = "running"
	TaskProgressing  TaskStatus = "progressing"
	TaskWaiting      TaskStatus = "waiting"
	TaskBlocked      TaskStatus = "blocked"
	TaskRetrying     TaskStatus = "retrying"
	TaskSucceeded    TaskStatus = "succeeded"
	TaskFailed       TaskStatus = "failed"
	TaskCancelled    TaskStatus = "cancelled"
	TaskExpired      TaskStatus = "expired"
	TaskDeadLettered TaskStatus = "dead_lettered"
	TaskCompensating TaskStatus = "compensating"
	TaskCompensated  TaskStatus = "compensated"
	TaskArchived     TaskStatus = "archived"
)

type DestinationKind string

const (
	DestinationTopic   DestinationKind = "topic"
	DestinationQueue   DestinationKind = "queue"
	DestinationStream  DestinationKind = "stream"
	DestinationDLQ     DestinationKind = "dlq"
	DestinationArchive DestinationKind = "archive"
)

type Destination struct {
	Kind DestinationKind `json:"kind"`
	Name string          `json:"name"`
}

type Message struct {
	ID               string            `json:"id"`
	ParentID         string            `json:"parent_id,omitempty"`
	Topic            string            `json:"topic,omitempty"`
	Queue            string            `json:"queue,omitempty"`
	Stream           string            `json:"stream,omitempty"`
	Partition        int               `json:"partition,omitempty"`
	Offset           uint64            `json:"offset,omitempty"`
	Key              string            `json:"key,omitempty"`
	RoutingKey       string            `json:"routing_key,omitempty"`
	Type             string            `json:"type,omitempty"`
	ContentType      string            `json:"content_type,omitempty"`
	Schema           string            `json:"schema,omitempty"`
	SchemaVersion    string            `json:"schema_version,omitempty"`
	Payload          []byte            `json:"payload,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	TraceID          string            `json:"trace_id,omitempty"`
	SpanID           string            `json:"span_id,omitempty"`
	CorrelationID    string            `json:"correlation_id,omitempty"`
	CausationID      string            `json:"causation_id,omitempty"`
	RequestID        string            `json:"request_id,omitempty"`
	ReplyTo          string            `json:"reply_to,omitempty"`
	ReplyGroup       string            `json:"reply_group,omitempty"`
	IdempotencyKey   string            `json:"idempotency_key,omitempty"`
	DeduplicationID  string            `json:"deduplication_id,omitempty"`
	Checksum         string            `json:"checksum,omitempty"`
	Priority         int               `json:"priority,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	AvailableAt      time.Time         `json:"available_at,omitempty"`
	ExpiresAt        time.Time         `json:"expires_at,omitempty"`
	Attempts         int               `json:"attempts,omitempty"`
	MaxAttempts      int               `json:"max_attempts,omitempty"`
	Redeliveries     int               `json:"redeliveries,omitempty"`
	TenantID         string            `json:"tenant_id,omitempty"`
	ProducerID       string            `json:"producer_id,omitempty"`
	ConsumerID       string            `json:"consumer_id,omitempty"`
	ConsumerGroup    string            `json:"consumer_group,omitempty"`
	LeaseToken       string            `json:"lease_token,omitempty"`
	LeaseUntil       time.Time         `json:"lease_until,omitempty"`
	OriginalTopic    string            `json:"original_topic,omitempty"`
	OriginalQueue    string            `json:"original_queue,omitempty"`
	OriginalStream   string            `json:"original_stream,omitempty"`
	OriginalOffset   uint64            `json:"original_offset,omitempty"`
	Status           MessageStatus     `json:"status"`
	LastError        string            `json:"last_error,omitempty"`
	DeadLetterReason string            `json:"dead_letter_reason,omitempty"`
	TaskID           string            `json:"task_id,omitempty"`
	TaskName         string            `json:"task_name,omitempty"`
}

type MessageHistory struct {
	MessageID string        `json:"message_id"`
	Status    MessageStatus `json:"status"`
	Event     string        `json:"event"`
	Source    string        `json:"source,omitempty"`
	Target    string        `json:"target,omitempty"`
	Reason    string        `json:"reason,omitempty"`
	Actor     string        `json:"actor,omitempty"`
	Error     string        `json:"error,omitempty"`
	Time      time.Time     `json:"time"`
}

type AckMode string

const (
	AckAuto   AckMode = "auto"
	AckManual AckMode = "manual"
)

type OrderMode string

const (
	OrderNone      OrderMode = "none"
	OrderFIFO      OrderMode = "fifo"
	OrderKey       OrderMode = "key"
	OrderPartition OrderMode = "partition"
	OrderTenant    OrderMode = "tenant"
	OrderGlobal    OrderMode = "global"
)

type BackpressurePolicy string

const (
	BackpressureBlock      BackpressurePolicy = "block"
	BackpressureReject     BackpressurePolicy = "reject"
	BackpressureDropOldest BackpressurePolicy = "drop_oldest"
	BackpressureDropNewest BackpressurePolicy = "drop_newest"
	BackpressureSpillDisk  BackpressurePolicy = "spill_disk"
	BackpressureDLQ        BackpressurePolicy = "dlq"
)

type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Initial     time.Duration `json:"initial"`
	Max         time.Duration `json:"max"`
	Factor      float64       `json:"factor"`
	Jitter      bool          `json:"jitter"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, Initial: 50 * time.Millisecond, Max: 5 * time.Second, Factor: 2, Jitter: true}
}

type QueueConfig struct {
	Name              string             `json:"name"`
	Workers           int                `json:"workers"`
	MaxInFlight       int                `json:"max_inflight"`
	MaxDepth          int                `json:"max_depth"`
	VisibilityTimeout time.Duration      `json:"visibility_timeout"`
	AckMode           AckMode            `json:"ack_mode"`
	RetryPolicy       RetryPolicy        `json:"retry_policy"`
	DLQ               string             `json:"dlq"`
	Ordering          OrderMode          `json:"ordering"`
	Backpressure      BackpressurePolicy `json:"backpressure"`
}

type TopicConfig struct {
	Name       string        `json:"name"`
	Partitions int           `json:"partitions"`
	Retention  time.Duration `json:"retention"`
	Paused     bool          `json:"paused"`
}

type StreamConfig struct {
	Name       string        `json:"name"`
	Partitions int           `json:"partitions"`
	Retention  time.Duration `json:"retention"`
	Compact    bool          `json:"compact"`
}

type ConsumerConfig struct {
	Name              string        `json:"name"`
	Group             string        `json:"group"`
	Queue             string        `json:"queue"`
	Topic             string        `json:"topic"`
	Pattern           string        `json:"pattern"`
	Concurrency       int           `json:"concurrency"`
	MaxInFlight       int           `json:"max_inflight"`
	BatchSize         int           `json:"batch_size"`
	BatchWait         time.Duration `json:"batch_wait"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	AckMode           AckMode       `json:"ack_mode"`
	RetryPolicy       RetryPolicy   `json:"retry_policy"`
	DLQ               string        `json:"dlq"`
	Ordered           bool          `json:"ordered"`
	OrderBy           OrderMode     `json:"order_by"`
	Timeout           time.Duration `json:"timeout"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

type Config struct {
	Name                  string              `json:"name"`
	Mode                  Mode                `json:"mode"`
	NodeID                string              `json:"node_id"`
	Store                 Store               `json:"-"`
	StateStore            StateStore          `json:"-"`
	Pool                  *pool.Pool    `json:"-"`
	OwnPool               bool                `json:"own_pool"`
	Codec                 Codec               `json:"-"`
	FrameCodec            *BinaryMessageCodec `json:"-"`
	SchemaRegistry        SchemaRegistry      `json:"-"`
	DefaultQueue          string              `json:"default_queue"`
	DefaultVisibility     time.Duration       `json:"default_visibility"`
	DefaultRetry          RetryPolicy         `json:"default_retry"`
	DefaultDLQ            string              `json:"default_dlq"`
	MaxMessageSize        int                 `json:"max_message_size"`
	Backpressure          BackpressurePolicy  `json:"backpressure"`
	EnableNotifications   bool                `json:"enable_notifications"`
	EnableRequestResponse bool                `json:"enable_request_response"`
	EnableTaskTracking    bool                `json:"enable_task_tracking"`
	DrainTimeout          time.Duration       `json:"drain_timeout"`
	PollInterval          time.Duration       `json:"poll_interval"`
	LeaseSweepInterval    time.Duration       `json:"lease_sweep_interval"`
	RemoteConsumerTimeout time.Duration       `json:"remote_consumer_timeout"`
	Hooks                 Hooks               `json:"-"`
}

type Handler func(context.Context, Delivery) error

type BatchHandler func(context.Context, []Delivery) error

type TaskHandler func(context.Context, TaskDelivery) (any, error)
