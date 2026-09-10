package broker

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrClosed             = errors.New("broker: closed")
	ErrStopped            = errors.New("broker: stopped")
	ErrPaused             = errors.New("broker: paused")
	ErrQueueNotFound      = errors.New("broker: queue not found")
	ErrTopicNotFound      = errors.New("broker: topic not found")
	ErrStreamNotFound     = errors.New("broker: stream not found")
	ErrMessageNotFound    = errors.New("broker: message not found")
	ErrInvalidMessage     = errors.New("broker: invalid message")
	ErrMessageTooLarge    = errors.New("broker: message too large")
	ErrNoDestination      = errors.New("broker: no route destination")
	ErrNoSubscribers      = errors.New("broker: no subscribers")
	ErrDuplicateMessage   = errors.New("broker: duplicate message")
	ErrBackpressure       = errors.New("broker: backpressure limit reached")
	ErrPublishTimeout     = errors.New("broker: publish timeout")
	ErrRequestTimeout     = errors.New("broker: request timeout")
	ErrNoHandler          = errors.New("broker: no handler")
	ErrUnauthorized       = errors.New("broker: unauthorized")
	ErrLeaseLost          = errors.New("broker: lease lost")
	ErrPermanent          = errors.New("broker: permanent error")
	ErrTransactionClosed  = errors.New("broker: transaction closed")
	ErrUnsupportedMode    = errors.New("broker: unsupported mode")
	ErrBrokerUnavailable  = errors.New("broker: unavailable")
	ErrCallbackFailed     = errors.New("broker: callback failed")
	ErrCancelled          = errors.New("broker: cancelled")
	ErrConsumerBanned     = errors.New("broker: consumer banned")
	ErrRateLimited        = errors.New("broker: rate limited")
	ErrNotLeader          = errors.New("broker: not leader")
	ErrStaleEpoch         = errors.New("broker: stale leader epoch")
	ErrQuorumUnavailable  = errors.New("broker: quorum unavailable")
	ErrThrottled          = errors.New("broker: throttled")
	ErrTransactionAborted = errors.New("broker: transaction aborted")
	ErrStorageUnavailable = errors.New("broker: storage unavailable")
)

type RetryClassification string

const (
	RetryNotLeader          RetryClassification = "not_leader"
	RetryStaleEpoch         RetryClassification = "stale_epoch"
	RetryQuorumUnavailable  RetryClassification = "quorum_unavailable"
	RetryThrottled          RetryClassification = "throttled"
	RetryTransactionAborted RetryClassification = "transaction_aborted"
	RetryStorageUnavailable RetryClassification = "storage_unavailable"
)

// RetryError carries machine-readable routing and retry metadata.
type RetryError struct {
	Class       RetryClassification `json:"class"`
	Message     string              `json:"message,omitempty"`
	Leader      string              `json:"leader,omitempty"`
	LeaderEpoch uint64              `json:"leader_epoch,omitempty"`
	RetryAfter  time.Duration       `json:"retry_after,omitempty"`
	Cause       error               `json:"-"`
}

func (e *RetryError) Error() string {
	if e.Message != "" {
		return string(e.Class) + ": " + e.Message
	}
	return string(e.Class)
}

func (e *RetryError) Unwrap() error { return e.Cause }

func ClassifyRetry(err error) (RetryClassification, bool) {
	var classified *RetryError
	if errors.As(err, &classified) {
		return classified.Class, true
	}
	switch {
	case errors.Is(err, ErrNotLeader):
		return RetryNotLeader, true
	case errors.Is(err, ErrStaleEpoch):
		return RetryStaleEpoch, true
	case errors.Is(err, ErrQuorumUnavailable):
		return RetryQuorumUnavailable, true
	case errors.Is(err, ErrThrottled), errors.Is(err, ErrRateLimited), errors.Is(err, ErrBackpressure):
		return RetryThrottled, true
	case errors.Is(err, ErrTransactionAborted):
		return RetryTransactionAborted, true
	case errors.Is(err, ErrStorageUnavailable), errors.Is(err, ErrBrokerUnavailable):
		return RetryStorageUnavailable, true
	default:
		return "", false
	}
}

type BrokerError struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Temporary bool              `json:"temporary,omitempty"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

func (e BrokerError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

type PermanentError struct{ Err error }

func (e PermanentError) Error() string {
	if e.Err == nil {
		return ErrPermanent.Error()
	}
	return e.Err.Error()
}
func (e PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return PermanentError{Err: err}
}

func IsPermanent(err error) bool {
	var p PermanentError
	return errors.As(err, &p)
}

func errf(base error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", base, fmt.Sprintf(format, args...))
}
