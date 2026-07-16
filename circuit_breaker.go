package broker

import (
	"context"
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"   // normal operation
	CircuitOpen     CircuitState = "open"     // rejecting requests
	CircuitHalfOpen CircuitState = "half_open" // testing recovery
)

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	// Threshold is the number of consecutive failures before opening the circuit.
	// Default: 5.
	Threshold int

	// Timeout is how long the circuit stays open before transitioning to half-open.
	// Default: 30 seconds.
	Timeout time.Duration

	// HalfOpenMax is the number of test requests allowed in half-open state.
	// Default: 3.
	HalfOpenMax int

	// SuccessThreshold is the number of consecutive successes in half-open state
	// before closing the circuit. Default: 2.
	SuccessThreshold int
}

func defaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Threshold:        5,
		Timeout:          30 * time.Second,
		HalfOpenMax:      3,
		SuccessThreshold: 2,
	}
}

// CircuitBreaker provides automatic failure detection and recovery.
type CircuitBreaker struct {
	mu              sync.Mutex
	cfg             CircuitBreakerConfig
	state           CircuitState
	failures        int
	successes       int
	halfOpenCount   int
	lastFailureTime time.Time
}

// NewCircuitBreaker creates a new circuit breaker with the given config.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 5
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	return &CircuitBreaker{
		cfg:   cfg,
		state: CircuitClosed,
	}
}

// Allow checks if a request is allowed through the circuit breaker.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.cfg.Timeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCount = 0
			cb.successes = 0
			return true
		}
		return false
	case CircuitHalfOpen:
		return cb.halfOpenCount < cb.cfg.HalfOpenMax
	}
	return false
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.cfg.SuccessThreshold {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.successes = 0
		}
	case CircuitClosed:
		cb.failures = 0
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailureTime = time.Now()
	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.cfg.Threshold {
			cb.state = CircuitOpen
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.halfOpenCount = 0
		cb.successes = 0
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen && time.Since(cb.lastFailureTime) > cb.cfg.Timeout {
		cb.state = CircuitHalfOpen
		cb.halfOpenCount = 0
		cb.successes = 0
	}
	return cb.state
}

// Reset manually resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	cb.state = CircuitClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenCount = 0
	cb.mu.Unlock()
}

// IsOpen returns true if the circuit is open (rejecting requests).
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.State() == CircuitOpen
}

// StoreCircuitBreaker wraps a Store with circuit breaker protection.
type StoreCircuitBreaker struct {
	store Store
	cb    *CircuitBreaker
}

// NewStoreCircuitBreaker wraps a store with circuit breaker protection.
func NewStoreCircuitBreaker(store Store, cfg CircuitBreakerConfig) *StoreCircuitBreaker {
	return &StoreCircuitBreaker{
		store: store,
		cb:    NewCircuitBreaker(cfg),
	}
}

// State returns the circuit breaker state.
func (scb *StoreCircuitBreaker) State() CircuitState {
	return scb.cb.State()
}

func (scb *StoreCircuitBreaker) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.DeclareQueue(ctx, cfg)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.DeclareTopic(ctx, cfg)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.DeclareStream(ctx, cfg)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	msgs, err := scb.store.Append(ctx, msg, dests)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msgs, err
}

func (scb *StoreCircuitBreaker) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	msgs, err := scb.store.Lease(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msgs, err
}

func (scb *StoreCircuitBreaker) Ack(ctx context.Context, req AckRequest) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.Ack(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) Nack(ctx context.Context, req NackRequest) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.Nack(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) ExtendLease(ctx context.Context, req LeaseRequest) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.ExtendLease(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) Update(ctx context.Context, msg Message) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.Update(ctx, msg)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) Get(ctx context.Context, id string) (Message, bool, error) {
	if !scb.cb.Allow() {
		return Message{}, false, ErrBrokerUnavailable
	}
	msg, ok, err := scb.store.Get(ctx, id)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msg, ok, err
}

func (scb *StoreCircuitBreaker) History(ctx context.Context, id string) ([]MessageHistory, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	h, err := scb.store.History(ctx, id)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return h, err
}

func (scb *StoreCircuitBreaker) ListQueue(ctx context.Context, name string, limit int) ([]Message, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	msgs, err := scb.store.ListQueue(ctx, name, limit)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msgs, err
}

func (scb *StoreCircuitBreaker) ListDLQ(ctx context.Context, name string, limit int) ([]Message, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	msgs, err := scb.store.ListDLQ(ctx, name, limit)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msgs, err
}

func (scb *StoreCircuitBreaker) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	if !scb.cb.Allow() {
		return MoveResult{}, ErrBrokerUnavailable
	}
	res, err := scb.store.Move(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return res, err
}

func (scb *StoreCircuitBreaker) PauseQueue(ctx context.Context, name string) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.PauseQueue(ctx, name)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) ResumeQueue(ctx context.Context, name string) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.ResumeQueue(ctx, name)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) PurgeQueue(ctx context.Context, name string) (int, error) {
	if !scb.cb.Allow() {
		return 0, ErrBrokerUnavailable
	}
	n, err := scb.store.PurgeQueue(ctx, name)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return n, err
}

func (scb *StoreCircuitBreaker) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	if !scb.cb.Allow() {
		return nil, ErrBrokerUnavailable
	}
	msgs, err := scb.store.ReadStream(ctx, req)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return msgs, err
}

func (scb *StoreCircuitBreaker) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	if !scb.cb.Allow() {
		return ErrBrokerUnavailable
	}
	err := scb.store.CommitOffset(ctx, commit)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return err
}

func (scb *StoreCircuitBreaker) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	if !scb.cb.Allow() {
		return 0, ErrBrokerUnavailable
	}
	off, err := scb.store.ConsumerOffset(ctx, commit)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return off, err
}

func (scb *StoreCircuitBreaker) Stats(ctx context.Context) (StoreStats, error) {
	if !scb.cb.Allow() {
		return StoreStats{}, ErrBrokerUnavailable
	}
	stats, err := scb.store.Stats(ctx)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return stats, err
}

func (scb *StoreCircuitBreaker) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	if !scb.cb.Allow() {
		return 0, ErrBrokerUnavailable
	}
	n, err := scb.store.SweepLeases(ctx, now)
	if err != nil {
		scb.cb.RecordFailure()
	} else {
		scb.cb.RecordSuccess()
	}
	return n, err
}

func (scb *StoreCircuitBreaker) Close() error {
	return scb.store.Close()
}
