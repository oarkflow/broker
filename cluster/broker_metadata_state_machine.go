package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
)

const brokerMetadataSnapshotFormat = "broker-metadata-state-v1"

// BrokerMetadataStateMachine applies broker data and controller metadata
// commands through one Raft log while keeping their snapshots independently
// versioned. Legacy snapshots produced by BrokerStateMachine are accepted.
type BrokerMetadataStateMachine struct {
	broker   StateMachine
	metadata StateMachine
}

func NewBrokerMetadataStateMachine(broker, metadata StateMachine) (*BrokerMetadataStateMachine, error) {
	if broker == nil || metadata == nil {
		return nil, errors.New("broker metadata state machine requires both components")
	}
	return &BrokerMetadataStateMachine{broker: broker, metadata: metadata}, nil
}

func (sm *BrokerMetadataStateMachine) Apply(entry *LogEntry) error {
	if entry.Type != EntryNormal {
		return nil
	}
	var command map[string]json.RawMessage
	if err := json.Unmarshal(entry.Data, &command); err != nil {
		return fmt.Errorf("decode composite Raft command: %w", err)
	}
	if _, ok := command["type"]; ok {
		return sm.broker.Apply(entry)
	}
	if _, ok := command["op"]; ok {
		return sm.metadata.Apply(entry)
	}
	return errors.New("composite Raft command has no recognized discriminator")
}

func (sm *BrokerMetadataStateMachine) Snapshot() ([]byte, error) {
	brokerSnapshot, err := sm.broker.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("snapshot broker state: %w", err)
	}
	metadataSnapshot, err := sm.metadata.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("snapshot controller metadata: %w", err)
	}
	return json.Marshal(struct {
		Format   string `json:"format"`
		Broker   []byte `json:"broker"`
		Metadata []byte `json:"metadata"`
	}{Format: brokerMetadataSnapshotFormat, Broker: brokerSnapshot, Metadata: metadataSnapshot})
}

func (sm *BrokerMetadataStateMachine) Restore(data []byte) error {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode Raft snapshot: %w", err)
	}
	var format string
	if rawFormat, ok := envelope["format"]; ok {
		if err := json.Unmarshal(rawFormat, &format); err != nil {
			format = ""
		}
	}
	if format != brokerMetadataSnapshotFormat {
		// Before the composite format was introduced, the snapshot contained
		// only broker data. Clear controller metadata when restoring that format.
		if err := sm.broker.Restore(data); err != nil {
			return fmt.Errorf("restore legacy broker state: %w", err)
		}
		if err := sm.metadata.Restore([]byte(`{}`)); err != nil {
			return fmt.Errorf("clear legacy controller metadata: %w", err)
		}
		return nil
	}
	var snapshot struct {
		Format   string `json:"format"`
		Broker   []byte `json:"broker"`
		Metadata []byte `json:"metadata"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode composite Raft snapshot: %w", err)
	}
	if len(snapshot.Broker) == 0 || len(snapshot.Metadata) == 0 {
		return errors.New("composite Raft snapshot is missing a component")
	}
	if err := sm.broker.Restore(snapshot.Broker); err != nil {
		return fmt.Errorf("restore broker state: %w", err)
	}
	if err := sm.metadata.Restore(snapshot.Metadata); err != nil {
		return fmt.Errorf("restore controller metadata: %w", err)
	}
	return nil
}
