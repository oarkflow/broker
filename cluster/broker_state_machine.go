package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// DeclareQueueCommand mirrors broker.QueueConfig for replication.
type DeclareQueueCommand struct {
	Name              string        `json:"name"`
	Workers           int           `json:"workers"`
	MaxInFlight       int           `json:"max_inflight"`
	MaxDepth          int           `json:"max_depth"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	AckMode           string        `json:"ack_mode"`
	RetryMaxAttempts  int           `json:"retry_max_attempts"`
	RetryInitial      time.Duration `json:"retry_initial"`
	RetryMax          time.Duration `json:"retry_max"`
	RetryFactor       float64       `json:"retry_factor"`
	DLQ               string        `json:"dlq"`
	Ordering          string        `json:"ordering"`
	Backpressure      string        `json:"backpressure"`
	Strategy          string        `json:"strategy,omitempty"`
}

// DeclareTopicCommand mirrors broker.TopicConfig for replication.
type DeclareTopicCommand struct {
	Name       string        `json:"name"`
	Partitions int           `json:"partitions"`
	Retention  time.Duration `json:"retention"`
	Paused     bool          `json:"paused"`
}

// DeclareStreamCommand mirrors broker.StreamConfig for replication.
type DeclareStreamCommand struct {
	Name       string        `json:"name"`
	Partitions int           `json:"partitions"`
	Retention  time.Duration `json:"retention"`
	Compact    bool          `json:"compact"`
}

// CommandDestination mirrors broker.Destination for replication.
type CommandDestination struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// AppendCommand contains the message fields and destinations for replication.
// Fields mirror the broker.Message struct.
type AppendCommand struct {
	ID               string              `json:"id"`
	ParentID         string              `json:"parent_id,omitempty"`
	Topic            string              `json:"topic,omitempty"`
	Queue            string              `json:"queue,omitempty"`
	Stream           string              `json:"stream,omitempty"`
	Partition        int                 `json:"partition,omitempty"`
	Offset           uint64              `json:"offset,omitempty"`
	Key              string              `json:"key,omitempty"`
	RoutingKey       string              `json:"routing_key,omitempty"`
	Type             string              `json:"type,omitempty"`
	ContentType      string              `json:"content_type,omitempty"`
	Schema           string              `json:"schema,omitempty"`
	SchemaVersion    string              `json:"schema_version,omitempty"`
	Payload          []byte              `json:"payload,omitempty"`
	Headers          map[string]string   `json:"headers,omitempty"`
	Metadata         map[string]string   `json:"metadata,omitempty"`
	TraceID          string              `json:"trace_id,omitempty"`
	SpanID           string              `json:"span_id,omitempty"`
	CorrelationID    string              `json:"correlation_id,omitempty"`
	CausationID      string              `json:"causation_id,omitempty"`
	RequestID        string              `json:"request_id,omitempty"`
	ReplyTo          string              `json:"reply_to,omitempty"`
	ReplyGroup       string              `json:"reply_group,omitempty"`
	IdempotencyKey   string              `json:"idempotency_key,omitempty"`
	DeduplicationID  string              `json:"deduplication_id,omitempty"`
	Checksum         string              `json:"checksum,omitempty"`
	Priority         int                 `json:"priority,omitempty"`
	CreatedAt        time.Time           `json:"created_at"`
	AvailableAt      time.Time           `json:"available_at,omitempty"`
	ExpiresAt        time.Time           `json:"expires_at,omitempty"`
	Attempts         int                 `json:"attempts,omitempty"`
	MaxAttempts      int                 `json:"max_attempts,omitempty"`
	Redeliveries     int                 `json:"redeliveries,omitempty"`
	TenantID         string              `json:"tenant_id,omitempty"`
	ProducerID       string              `json:"producer_id,omitempty"`
	ConsumerID       string              `json:"consumer_id,omitempty"`
	ConsumerGroup    string              `json:"consumer_group,omitempty"`
	LeaseToken       string              `json:"lease_token,omitempty"`
	LeaseUntil       time.Time           `json:"lease_until,omitempty"`
	OriginalTopic    string              `json:"original_topic,omitempty"`
	OriginalQueue    string              `json:"original_queue,omitempty"`
	OriginalStream   string              `json:"original_stream,omitempty"`
	OriginalOffset   uint64              `json:"original_offset,omitempty"`
	Status           string              `json:"status"`
	LastError        string              `json:"last_error,omitempty"`
	DeadLetterReason string              `json:"dead_letter_reason,omitempty"`
	TaskID           string              `json:"task_id,omitempty"`
	TaskName         string              `json:"task_name,omitempty"`
	Destinations     []CommandDestination `json:"destinations"`
}

// AckCommand mirrors broker.AckRequest for replication.
type AckCommand struct {
	MessageID  string `json:"message_id"`
	Queue      string `json:"queue"`
	LeaseToken string `json:"lease_token"`
	ConsumerID string `json:"consumer_id"`
}

// NackCommand mirrors broker.NackRequest for replication.
type NackCommand struct {
	MessageID        string `json:"message_id"`
	Queue            string `json:"queue"`
	LeaseToken       string `json:"lease_token"`
	ConsumerID       string `json:"consumer_id"`
	Action           string `json:"action"`
	Reason           string `json:"reason"`
	Error            string `json:"error"`
	Delay            time.Duration `json:"delay"`
	DLQ              string `json:"dlq"`
	ResetLease       bool   `json:"reset_lease"`
	IncrementAttempt bool   `json:"increment_attempt"`
}

// BrokerStore is the subset of the broker's Store interface needed for
// replicated state machine operations. The broker package satisfies this
// via its MemoryStore / FileWALStore / BinaryFileWALStore.
type BrokerStore interface {
	ReplicateDeclareQueue(ctx context.Context, cmd DeclareQueueCommand) error
	ReplicateDeclareTopic(ctx context.Context, cmd DeclareTopicCommand) error
	ReplicateDeclareStream(ctx context.Context, cmd DeclareStreamCommand) error
	ReplicateAppend(ctx context.Context, cmd AppendCommand) error
	ReplicateAck(ctx context.Context, cmd AckCommand) error
	ReplicateNack(ctx context.Context, cmd NackCommand) error
	ReplicatePauseQueue(ctx context.Context, name string) error
	ReplicateResumeQueue(ctx context.Context, name string) error
	ReplicatePurgeQueue(ctx context.Context, name string) (int, error)
}

// BrokerCommandType identifies the broker operation to replicate.
type BrokerCommandType string

const (
	BrokerCmdDeclareQueue  BrokerCommandType = "declare_queue"
	BrokerCmdDeclareTopic  BrokerCommandType = "declare_topic"
	BrokerCmdDeclareStream BrokerCommandType = "declare_stream"
	BrokerCmdAppend        BrokerCommandType = "append"
	BrokerCmdAck           BrokerCommandType = "ack"
	BrokerCmdNack          BrokerCommandType = "nack"
	BrokerCmdPauseQueue    BrokerCommandType = "pause_queue"
	BrokerCmdResumeQueue   BrokerCommandType = "resume_queue"
	BrokerCmdPurgeQueue    BrokerCommandType = "purge_queue"
)

// BrokerCommand is the serialized command embedded in a Raft LogEntry.Data.
type BrokerCommand struct {
	Type    BrokerCommandType `json:"type"`
	Payload json.RawMessage   `json:"payload"`
}

// BrokerStateMachine implements StateMachine by forwarding committed entries
// to a broker store. It runs on every node in the cluster so that the local
// store stays consistent with the Raft log.
type BrokerStateMachine struct {
	mu    sync.RWMutex
	store BrokerStore
}

// NewBrokerStateMachine creates a new BrokerStateMachine.
func NewBrokerStateMachine(store BrokerStore) *BrokerStateMachine {
	return &BrokerStateMachine{
		store: store,
	}
}

// Apply applies a committed Raft log entry to the local broker store.
func (sm *BrokerStateMachine) Apply(entry *LogEntry) error {
	if entry.Type != EntryNormal {
		return nil
	}

	var cmd BrokerCommand
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("broker sm: unmarshal command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch cmd.Type {
	case BrokerCmdDeclareQueue:
		return sm.applyDeclareQueue(ctx, cmd.Payload)
	case BrokerCmdDeclareTopic:
		return sm.applyDeclareTopic(ctx, cmd.Payload)
	case BrokerCmdDeclareStream:
		return sm.applyDeclareStream(ctx, cmd.Payload)
	case BrokerCmdAppend:
		return sm.applyAppend(ctx, cmd.Payload)
	case BrokerCmdAck:
		return sm.applyAck(ctx, cmd.Payload)
	case BrokerCmdNack:
		return sm.applyNack(ctx, cmd.Payload)
	case BrokerCmdPauseQueue:
		return sm.applyPauseQueue(ctx, cmd.Payload)
	case BrokerCmdResumeQueue:
		return sm.applyResumeQueue(ctx, cmd.Payload)
	case BrokerCmdPurgeQueue:
		return sm.applyPurgeQueue(ctx, cmd.Payload)
	default:
		return fmt.Errorf("broker sm: unknown command type: %s", cmd.Type)
	}
}

func (sm *BrokerStateMachine) applyDeclareQueue(ctx context.Context, payload json.RawMessage) error {
	var cmd DeclareQueueCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateDeclareQueue(ctx, cmd)
}

func (sm *BrokerStateMachine) applyDeclareTopic(ctx context.Context, payload json.RawMessage) error {
	var cmd DeclareTopicCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateDeclareTopic(ctx, cmd)
}

func (sm *BrokerStateMachine) applyDeclareStream(ctx context.Context, payload json.RawMessage) error {
	var cmd DeclareStreamCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateDeclareStream(ctx, cmd)
}

func (sm *BrokerStateMachine) applyAppend(ctx context.Context, payload json.RawMessage) error {
	var cmd AppendCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateAppend(ctx, cmd)
}

func (sm *BrokerStateMachine) applyAck(ctx context.Context, payload json.RawMessage) error {
	var cmd AckCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateAck(ctx, cmd)
}

func (sm *BrokerStateMachine) applyNack(ctx context.Context, payload json.RawMessage) error {
	var cmd NackCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return sm.store.ReplicateNack(ctx, cmd)
}

func (sm *BrokerStateMachine) applyPauseQueue(ctx context.Context, payload json.RawMessage) error {
	var name string
	if err := json.Unmarshal(payload, &name); err != nil {
		return err
	}
	return sm.store.ReplicatePauseQueue(ctx, name)
}

func (sm *BrokerStateMachine) applyResumeQueue(ctx context.Context, payload json.RawMessage) error {
	var name string
	if err := json.Unmarshal(payload, &name); err != nil {
		return err
	}
	return sm.store.ReplicateResumeQueue(ctx, name)
}

func (sm *BrokerStateMachine) applyPurgeQueue(ctx context.Context, payload json.RawMessage) error {
	var name string
	if err := json.Unmarshal(payload, &name); err != nil {
		return err
	}
	_, err := sm.store.ReplicatePurgeQueue(ctx, name)
	return err
}

// Snapshot returns a snapshot of the state machine.
// For the broker state machine, snapshots are managed by the store itself;
// this is a placeholder that signals "no extra state to snapshot".
func (sm *BrokerStateMachine) Snapshot() ([]byte, error) {
	return json.Marshal(map[string]string{"type": "broker_state_machine"})
}

// Restore restores the state machine from a snapshot.
func (sm *BrokerStateMachine) Restore(snapshot []byte) error {
	// The broker store handles its own restore; this is a no-op.
	return nil
}

// EncodeBrokerCommand serializes a BrokerCommand for embedding in a LogEntry.
func EncodeBrokerCommand(cmd *BrokerCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

// MakeBrokerCommand creates a BrokerCommand with the given type and payload.
func MakeBrokerCommand(cmdType BrokerCommandType, payload any) (*BrokerCommand, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &BrokerCommand{
		Type:    cmdType,
		Payload: data,
	}, nil
}
