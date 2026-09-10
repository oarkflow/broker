package broker

import (
	"fmt"
	"time"
)

// PerformanceProfile selects a documented set of durability and resource
// trade-offs. ProfileDurable is the production default.
type PerformanceProfile string

const (
	ProfileDurable     PerformanceProfile = "durable"
	ProfileLatency     PerformanceProfile = "latency"
	ProfileThroughput  PerformanceProfile = "throughput"
	ProfileLightweight PerformanceProfile = "lightweight"
)

// AckPolicy is the acknowledgement requested by a publisher.
type AckPolicy string

const (
	AckNone      AckPolicy = "none"
	AckLeader    AckPolicy = "leader"
	AckQuorum    AckPolicy = "quorum"
	AckAll       AckPolicy = "all"
	AckProcessed AckPolicy = "processed"
)

// ReplicationPolicy defines the persistence quorum for a partition group.
type ReplicationPolicy struct {
	ReplicationFactor int       `json:"replication_factor"`
	MinInSyncReplicas int       `json:"min_in_sync_replicas"`
	Acknowledgement   AckPolicy `json:"acknowledgement"`
}

// ProfileDefaults exposes the concrete policy selected by a named profile.
type ProfileDefaults struct {
	Profile               PerformanceProfile `json:"profile"`
	Ack                   AckPolicy          `json:"ack"`
	Replication           ReplicationPolicy  `json:"replication"`
	PollInterval          time.Duration      `json:"poll_interval"`
	NotificationRetention int                `json:"notification_retention"`
}

func DefaultsForProfile(profile PerformanceProfile) (ProfileDefaults, error) {
	switch profile {
	case "", ProfileDurable:
		return ProfileDefaults{Profile: ProfileDurable, Ack: AckQuorum, Replication: ReplicationPolicy{ReplicationFactor: 3, MinInSyncReplicas: 2, Acknowledgement: AckQuorum}, PollInterval: 5 * time.Millisecond, NotificationRetention: 1000}, nil
	case ProfileLatency:
		return ProfileDefaults{Profile: profile, Ack: AckLeader, Replication: ReplicationPolicy{ReplicationFactor: 3, MinInSyncReplicas: 1, Acknowledgement: AckLeader}, PollInterval: time.Millisecond, NotificationRetention: 250}, nil
	case ProfileThroughput:
		return ProfileDefaults{Profile: profile, Ack: AckQuorum, Replication: ReplicationPolicy{ReplicationFactor: 3, MinInSyncReplicas: 2, Acknowledgement: AckQuorum}, PollInterval: 10 * time.Millisecond, NotificationRetention: 1000}, nil
	case ProfileLightweight:
		return ProfileDefaults{Profile: profile, Ack: AckLeader, Replication: ReplicationPolicy{ReplicationFactor: 1, MinInSyncReplicas: 1, Acknowledgement: AckLeader}, PollInterval: 10 * time.Millisecond, NotificationRetention: 0}, nil
	default:
		return ProfileDefaults{}, fmt.Errorf("%w: unknown performance profile %q", ErrInvalidMessage, profile)
	}
}

// DurabilityScope states where an acknowledged record was made durable.
type DurabilityScope string

const (
	DurabilityMemory      DurabilityScope = "memory"
	DurabilityLocalFsync  DurabilityScope = "local_fsync"
	DurabilityQuorumFsync DurabilityScope = "quorum_fsync"
)

// NodeConfig identifies one broker node.
type NodeConfig struct {
	ID            string `json:"id"`
	AdvertiseAddr string `json:"advertise_addr,omitempty"`
	Zone          string `json:"zone,omitempty"`
	Rack          string `json:"rack,omitempty"`
}

// ClusterConfig defines replicated broker behavior. ReplicationFactor defaults
// to three and MinInSyncReplicas defaults to a majority.
type ClusterConfig struct {
	Enabled           bool     `json:"enabled"`
	ClusterID         string   `json:"cluster_id,omitempty"`
	Peers             []string `json:"peers,omitempty"`
	ReplicationFactor int      `json:"replication_factor,omitempty"`
	MinInSyncReplicas int      `json:"min_in_sync_replicas,omitempty"`
}

// StorageConfig contains durable-store policy independent of a concrete Store.
type StorageConfig struct {
	Directory       string `json:"directory,omitempty"`
	SegmentBytes    int64  `json:"segment_bytes,omitempty"`
	MaxDiskBytes    int64  `json:"max_disk_bytes,omitempty"`
	Sync            bool   `json:"sync"`
	EncryptionKeyID string `json:"encryption_key_id,omitempty"`
}

// SecurityConfig centralizes the production listener security policy.
type SecurityConfig struct {
	InsecureDev       bool `json:"insecure_dev,omitempty"`
	RequireTLS        bool `json:"require_tls,omitempty"`
	RequireClientCert bool `json:"require_client_cert,omitempty"`
}

// ListenerConfig names all supported listener addresses. Empty addresses
// disable the corresponding listener.
type ListenerConfig struct {
	HTTP      string `json:"http,omitempty"`
	NativeRPC string `json:"native_rpc,omitempty"`
	Native    string `json:"native_stream,omitempty"`
	AMQP      string `json:"amqp,omitempty"`
	MQTT      string `json:"mqtt,omitempty"`
	GRPC      string `json:"grpc,omitempty"`
	WebSocket string `json:"websocket,omitempty"`
	SSE       string `json:"sse,omitempty"`
}

// ConfirmationStore reports the strongest confirmation its completed writes
// actually provide. It is intentionally separate from Store for compatibility
// with third-party stores; unannotated stores are treated as memory-only.
type ConfirmationStore interface {
	ConfirmationLevel() ConfirmLevel
}

func confirmationLevel(store Store) ConfirmLevel {
	if s, ok := store.(ConfirmationStore); ok {
		return s.ConfirmationLevel()
	}
	return ConfirmEnqueued
}

func confirmationRank(level ConfirmLevel) int {
	switch level {
	case ConfirmAccepted:
		return 1
	case ConfirmValidated:
		return 2
	case ConfirmRouted:
		return 3
	case ConfirmEnqueued:
		return 4
	case ConfirmPersisted:
		return 5
	case ConfirmReplicated:
		return 6
	case ConfirmCommitted:
		return 7
	case ConfirmDelivered:
		return 8
	case ConfirmProcessed:
		return 9
	default:
		return 0
	}
}

func validateConfirmation(requested ConfirmLevel, store Store) error {
	if requested == "" {
		return nil
	}
	achieved := confirmationLevel(store)
	if confirmationRank(requested) > confirmationRank(achieved) {
		return fmt.Errorf("%w: requested confirmation %q, store only guarantees %q", ErrUnsupportedMode, requested, achieved)
	}
	return nil
}
