package cluster

import (
	"fmt"
	"log"
	"sync"
	"unicode/utf8"
)

// RaftMultiplexer dispatches multiple independent Raft groups over one
// authenticated transport listener. Each group still has its own controller,
// membership, state machine, and (when GroupID is set) durable storage path.
type RaftMultiplexer struct {
	transport Transport

	mu           sync.RWMutex
	controllers  map[string]*RaftController
	identityRefs map[string]int
	started      bool
}

type raftGroupTransport struct {
	mux     *RaftMultiplexer
	groupID string
}

func NewRaftMultiplexer(transport Transport) (*RaftMultiplexer, error) {
	if transport == nil {
		return nil, fmt.Errorf("Raft multiplexer requires a transport")
	}
	return &RaftMultiplexer{
		transport:    transport,
		controllers:  make(map[string]*RaftController),
		identityRefs: make(map[string]int),
	}, nil
}

// GroupTransport returns a transport view for one group. The empty ID is
// reserved for the legacy controller group. Peer identity admission is
// reference-counted across groups, so removing a peer from one group does not
// disconnect it from another group that still uses it.
func (m *RaftMultiplexer) GroupTransport(groupID string) (Transport, error) {
	if len(groupID) > MaxRaftGroupIDBytes || !utf8.ValidString(groupID) {
		return nil, fmt.Errorf("Raft group transport requires a valid group ID of at most %d bytes", MaxRaftGroupIDBytes)
	}
	return &raftGroupTransport{mux: m, groupID: groupID}, nil
}

// Register adds a group controller. Controllers may be added while the shared
// listener is active; group IDs must be unique, including the empty ID used by
// legacy controllers.
func (m *RaftMultiplexer) Register(controller *RaftController) error {
	if controller == nil {
		return fmt.Errorf("cannot register a nil Raft controller")
	}
	groupTransport, ok := controller.transport.(*raftGroupTransport)
	if !ok || groupTransport.mux != m || groupTransport.groupID != controller.groupID {
		return fmt.Errorf("Raft controller group %q uses a different transport", controller.groupID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.controllers[controller.groupID]; exists {
		return fmt.Errorf("Raft group %q is already registered", controller.groupID)
	}
	retained := make([]string, 0, len(controller.peers))
	for peerID, address := range controller.peers {
		if peerID == controller.id {
			continue
		}
		if err := m.retainPeerIdentityLocked(address); err != nil {
			for _, retainedAddress := range retained {
				m.releasePeerIdentityLocked(retainedAddress)
			}
			return err
		}
		retained = append(retained, address)
	}
	m.controllers[controller.groupID] = controller
	return nil
}

// Unregister removes a stopped group controller from message dispatch.
func (m *RaftMultiplexer) Unregister(controller *RaftController) error {
	if controller == nil {
		return fmt.Errorf("cannot unregister a nil Raft controller")
	}
	controller.nodeMu.Lock()
	defer controller.nodeMu.Unlock()
	if controller.running.Load() {
		return fmt.Errorf("Raft group %q must be stopped before unregistering", controller.groupID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.controllers[controller.groupID] != controller {
		return fmt.Errorf("Raft group %q is not registered with this multiplexer", controller.groupID)
	}
	for peerID, address := range controller.peers {
		if peerID != controller.id {
			m.releasePeerIdentityLocked(address)
		}
	}
	delete(m.controllers, controller.groupID)
	return nil
}

func (m *RaftMultiplexer) retainPeerIdentityLocked(address string) error {
	if validator, ok := m.transport.(interface{ ValidatePeerIdentity(string) error }); ok {
		if err := validator.ValidatePeerIdentity(address); err != nil {
			return err
		}
	}
	if m.identityRefs[address] == 0 {
		if identities, ok := m.transport.(interface{ RetainPeerIdentity(string) error }); ok {
			if err := identities.RetainPeerIdentity(address); err != nil {
				return err
			}
		}
	}
	m.identityRefs[address]++
	return nil
}

func (m *RaftMultiplexer) releasePeerIdentityLocked(address string) {
	refs := m.identityRefs[address]
	if refs <= 1 {
		delete(m.identityRefs, address)
		if identities, ok := m.transport.(interface{ ReleasePeerIdentity(string) }); ok {
			identities.ReleasePeerIdentity(address)
		} else if identities, ok := m.transport.(interface{ RemovePeerIdentity(string) }); ok {
			identities.RemovePeerIdentity(address)
		}
		return
	}
	m.identityRefs[address] = refs - 1
}

func (t *raftGroupTransport) Send(nodeID string, msg *RaftMessage) error {
	if msg == nil {
		return fmt.Errorf("cannot send a nil Raft message")
	}
	copyMessage := *msg
	if copyMessage.GroupID != "" && copyMessage.GroupID != t.groupID {
		return fmt.Errorf("message group %q does not match transport group %q", copyMessage.GroupID, t.groupID)
	}
	copyMessage.GroupID = t.groupID
	return t.mux.transport.Send(nodeID, &copyMessage)
}

func (t *raftGroupTransport) SendAppendEntries(nodeID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return t.mux.transport.SendAppendEntries(nodeID, req)
}

func (t *raftGroupTransport) SendRequestVote(nodeID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return t.mux.transport.SendRequestVote(nodeID, req)
}

func (t *raftGroupTransport) SendInstallSnapshot(nodeID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return t.mux.transport.SendInstallSnapshot(nodeID, req)
}

func (t *raftGroupTransport) Listen(string, func(string, *RaftMessage)) error {
	return fmt.Errorf("group transports use the shared RaftMultiplexer listener")
}

func (t *raftGroupTransport) Close() error { return nil }

func (t *raftGroupTransport) ValidatePeerIdentity(address string) error {
	if validator, ok := t.mux.transport.(interface{ ValidatePeerIdentity(string) error }); ok {
		return validator.ValidatePeerIdentity(address)
	}
	return nil
}

func (t *raftGroupTransport) AllowPeerIdentity(address string) error {
	m := t.mux
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retainPeerIdentityLocked(address)
}

func (t *raftGroupTransport) RemovePeerIdentity(address string) {
	m := t.mux
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releasePeerIdentityLocked(address)
}

// Listen attaches one transport listener and routes incoming messages by group.
func (m *RaftMultiplexer) Listen(addr string) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return fmt.Errorf("Raft multiplexer has already started")
	}
	if len(m.controllers) == 0 {
		m.mu.Unlock()
		return fmt.Errorf("Raft multiplexer has no registered groups")
	}
	m.started = true
	m.mu.Unlock()

	err := m.transport.Listen(addr, func(from string, msg *RaftMessage) {
		if err := m.HandleMessage(from, msg); err != nil {
			log.Printf("[raft] rejected multiplexed peer message from %q: %v", from, err)
		}
	})
	if err != nil {
		m.mu.Lock()
		m.started = false
		m.mu.Unlock()
	}
	return err
}

// HandleMessage dispatches a decoded peer message to its registered group.
func (m *RaftMultiplexer) HandleMessage(from string, msg *RaftMessage) error {
	if msg == nil {
		return fmt.Errorf("cannot dispatch a nil Raft message")
	}
	m.mu.RLock()
	controller := m.controllers[msg.GroupID]
	m.mu.RUnlock()
	if controller == nil {
		return fmt.Errorf("Raft message references unknown group %q", msg.GroupID)
	}
	return controller.HandleMessage(from, msg)
}
