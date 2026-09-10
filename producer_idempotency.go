package broker

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrProducerFenced is returned when a publish has an epoch strictly less than the registered epoch.
	ErrProducerFenced = errors.New("producer fenced: epoch is older than current epoch")
	// ErrOutOfOrderSequence is returned when a sequence gap is detected.
	ErrOutOfOrderSequence = errors.New("out of order sequence: sequence number gap detected")
)

// ProducerState tracks the current epoch and sequence for a specific producer ID.
type ProducerState struct {
	ProducerID    string    `json:"producer_id"`
	Epoch         int32     `json:"epoch"`
	LastSequence  uint64    `json:"last_sequence"`
	LastMessageID string    `json:"last_message_id"`
	Initialized   bool      `json:"initialized"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// ProducerRegistry maintains monotonic sequence tracking and epoch fencing per producer ID.
type ProducerRegistry struct {
	mu        sync.RWMutex
	producers map[string]*ProducerState
}

// NewProducerRegistry creates an in-memory producer registry.
func NewProducerRegistry() *ProducerRegistry {
	return &ProducerRegistry{
		producers: make(map[string]*ProducerState),
	}
}

// RegisterProducer registers or increments a producer's epoch.
func (r *ProducerRegistry) RegisterProducer(producerID string, epoch int32) (*ProducerState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.producers[producerID]
	if !exists {
		state = &ProducerState{
			ProducerID:  producerID,
			Epoch:       epoch,
			Initialized: false,
			UpdatedAt:   time.Now(),
		}
		r.producers[producerID] = state
		return state, nil
	}

	if epoch < state.Epoch {
		return nil, ErrProducerFenced
	}

	if epoch > state.Epoch {
		state.Epoch = epoch
		state.LastSequence = 0
		state.Initialized = false
		state.UpdatedAt = time.Now()
	}

	return state, nil
}

// GetProducer returns the current state for a producer.
func (r *ProducerRegistry) GetProducer(producerID string) (*ProducerState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state, ok := r.producers[producerID]
	if !ok {
		return nil, false
	}
	cp := *state
	return &cp, true
}

// CheckAndRecord verifies the epoch and sequence of a message and updates the state.
func (r *ProducerRegistry) CheckAndRecord(producerID string, epoch int32, seq uint64, msgID string) (isDuplicate bool, existingID string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.producers[producerID]
	if !exists {
		state = &ProducerState{
			ProducerID:    producerID,
			Epoch:         epoch,
			LastSequence:  seq,
			LastMessageID: msgID,
			Initialized:   true,
			UpdatedAt:     time.Now(),
		}
		r.producers[producerID] = state
		return false, "", nil
	}

	if epoch < state.Epoch {
		return false, "", ErrProducerFenced
	}

	if epoch > state.Epoch {
		state.Epoch = epoch
		state.LastSequence = seq
		state.LastMessageID = msgID
		state.Initialized = true
		state.UpdatedAt = time.Now()
		return false, "", nil
	}

	// Same epoch
	if !state.Initialized {
		state.LastSequence = seq
		state.LastMessageID = msgID
		state.Initialized = true
		state.UpdatedAt = time.Now()
		return false, "", nil
	}

	if seq <= state.LastSequence {
		// Duplicate publish (retry)
		return true, state.LastMessageID, nil
	}

	if seq > state.LastSequence+1 {
		return false, "", ErrOutOfOrderSequence
	}

	// In-order expected sequence: seq == state.LastSequence + 1
	state.LastSequence = seq
	state.LastMessageID = msgID
	state.UpdatedAt = time.Now()
	return false, "", nil
}

// WithProducer configures ProducerID, ProducerEpoch, and ProducerSequence on publish options.
func WithProducer(id string, epoch int32, seq uint64) PublishOption {
	return func(o *PublishOptions) {
		o.ProducerID = id
		o.ProducerEpoch = epoch
		s := seq
		o.ProducerSequence = &s
	}
}

// WithProducerEpoch sets the producer epoch on publish options.
func WithProducerEpoch(epoch int32) PublishOption {
	return func(o *PublishOptions) {
		o.ProducerEpoch = epoch
	}
}

// WithProducerSequence sets the monotonic producer sequence on publish options.
func WithProducerSequence(seq uint64) PublishOption {
	return func(o *PublishOptions) {
		s := seq
		o.ProducerSequence = &s
	}
}

// EnsureProducerRegistry makes sure the broker has an initialized ProducerRegistry.
func (b *Broker) ProducerRegistry() *ProducerRegistry {
	if b.producerRegistry == nil {
		b.producerRegistry = NewProducerRegistry()
	}
	return b.producerRegistry
}
