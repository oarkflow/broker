package cluster

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// Raft states.
type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
	StateDead
)

// Node states.
type NodeState string

const (
	NodeJoining  NodeState = "joining"
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeLeft     NodeState = "left"
	NodeFailed   NodeState = "failed"
)

func (s RaftState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Log entry types.
type EntryType uint8

const (
	EntryNormal    EntryType = 0
	EntryConfig    EntryType = 1
	EntrySnapshot  EntryType = 2
)

// Raft message types.
type MessageType uint8

const (
	MsgAppendEntries    MessageType = 0
	MsgAppendEntriesResp MessageType = 1
	MsgRequestVote      MessageType = 2
	MsgRequestVoteResp  MessageType = 3
	MsgInstallSnapshot  MessageType = 4
	MsgInstallSnapshotResp MessageType = 5
	MsgHeartbeat        MessageType = 6
	MsgHeartbeatResp    MessageType = 7
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    uint64    `json:"term"`
	Index   uint64    `json:"index"`
	Type    EntryType `json:"type"`
	Data    []byte    `json:"data"`
	Command []byte    `json:"command,omitempty"`
}

// AppendEntriesRequest is sent by the leader to replicate log entries.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to AppendEntries.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// The index of the last entry the follower has (for fast backup)
	LastLogIndex uint64 `json:"last_log_index"`
}

// RequestVoteRequest is sent by candidates to gather votes.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the reply to RequestVote.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// InstallSnapshotRequest is sent by the leader to install a snapshot.
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotResponse is the reply to InstallSnapshot.
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// RaftMessage wraps all Raft RPC messages.
type RaftMessage struct {
	CorrelationID       uint64                   `json:"correlation_id,omitempty"`
	Type                MessageType              `json:"type"`
	AppendEntries       *AppendEntriesRequest    `json:"append_entries,omitempty"`
	AppendEntriesResp   *AppendEntriesResponse   `json:"append_entries_resp,omitempty"`
	RequestVote         *RequestVoteRequest      `json:"request_vote,omitempty"`
	RequestVoteResp     *RequestVoteResponse     `json:"request_vote_resp,omitempty"`
	InstallSnapshot     *InstallSnapshotRequest  `json:"install_snapshot,omitempty"`
	InstallSnapshotResp *InstallSnapshotResponse `json:"install_snapshot_resp,omitempty"`
}

// EncodeMessage encodes a RaftMessage to bytes.
func EncodeMessage(msg *RaftMessage) ([]byte, error) {
	// Simple JSON encoding for now; can switch to protobuf later
	return encodeJSON(msg)
}

// DecodeMessage decodes a RaftMessage from bytes.
func DecodeMessage(data []byte) (*RaftMessage, error) {
	msg := &RaftMessage{}
	if err := decodeJSON(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// EncodeLogEntry encodes a LogEntry to bytes.
func EncodeLogEntry(entry *LogEntry) ([]byte, error) {
	return encodeJSON(entry)
}

// DecodeLogEntry decodes a LogEntry from bytes.
func DecodeLogEntry(data []byte) (*LogEntry, error) {
	entry := &LogEntry{}
	if err := decodeJSON(data, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

// EncodeUint64 encodes a uint64 to bytes (big-endian).
func EncodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// DecodeUint64 decodes a uint64 from bytes (big-endian).
func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Common errors.
var (
	ErrNotLeader       = errors.New("cluster: not leader")
	ErrLeaderLost      = errors.New("cluster: leader lost")
	ErrNodeNotFound    = errors.New("cluster: node not found")
	ErrPartitioned     = errors.New("cluster: node partitioned")
	ErrSnapshotFailed  = errors.New("cluster: snapshot failed")
	ErrLogCorrupted    = errors.New("cluster: log corrupted")
	ErrTermMismatch    = errors.New("cluster: term mismatch")
	ErrQuorumLost      = errors.New("cluster: quorum lost")
	ErrSplitBrain      = errors.New("cluster: split-brain detected")
)

// StateMachine defines the interface for the replicated state machine.
type StateMachine interface {
	// Apply applies a committed log entry to the state machine.
	Apply(entry *LogEntry) error
	// Snapshot returns a snapshot of the current state.
	Snapshot() ([]byte, error)
	// Restore restores the state machine from a snapshot.
	Restore(snapshot []byte) error
}

// Transport defines the interface for inter-node communication.
type Transport interface {
	// Send sends a message to a specific node.
	Send(nodeID string, msg *RaftMessage) error
	// SendAppendEntries sends AppendEntries to a node.
	SendAppendEntries(nodeID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	// SendRequestVote sends RequestVote to a node.
	SendRequestVote(nodeID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	// SendInstallSnapshot sends a snapshot to a node.
	SendInstallSnapshot(nodeID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	// Listen starts listening for incoming messages.
	Listen(addr string, handler func(from string, msg *RaftMessage)) error
	// Close stops the transport.
	Close() error
}

// Snapshot represents a point-in-time snapshot of the Raft state.
type Snapshot struct {
	Term              uint64 `json:"term"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// JSON encoding helpers - using a simple JSON encoder to avoid external deps.
func encodeJSON(v any) ([]byte, error) {
	return encodeValue(v)
}

func decodeJSON(data []byte, v any) error {
	return decodeValue(data, v)
}

func encodeValue(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodeValue(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// WriteBytes writes a byte slice to a writer with length prefix.
func WriteBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// ReadBytes reads a length-prefixed byte slice from a reader.
func ReadBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length > 100*1024*1024 { // 100MB limit
		return nil, errors.New("data too large")
	}
	data := make([]byte, length)
	_, err := io.ReadFull(r, data)
	return data, err
}
