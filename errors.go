package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

var (
	ErrClosed           = errors.New("broker: closed")
	ErrStopped          = errors.New("broker: stopped")
	ErrPaused           = errors.New("broker: paused")
	ErrQueueNotFound    = errors.New("broker: queue not found")
	ErrTopicNotFound    = errors.New("broker: topic not found")
	ErrStreamNotFound   = errors.New("broker: stream not found")
	ErrMessageNotFound  = errors.New("broker: message not found")
	ErrInvalidMessage   = errors.New("broker: invalid message")
	ErrMessageTooLarge  = errors.New("broker: message too large")
	ErrNoDestination    = errors.New("broker: no route destination")
	ErrNoSubscribers    = errors.New("broker: no subscribers")
	ErrDuplicateMessage = errors.New("broker: duplicate message")
	ErrBackpressure     = errors.New("broker: backpressure limit reached")
	ErrPublishTimeout   = errors.New("broker: publish timeout")
	ErrRequestTimeout   = errors.New("broker: request timeout")
	ErrNoHandler        = errors.New("broker: no handler")
	ErrUnauthorized     = errors.New("broker: unauthorized")
	// ErrUnauthenticated is a missing or invalid credential, as distinct from
	// ErrUnauthorized which is an authenticated principal lacking permission.
	// The two need separating so a client can tell "authenticate again" from
	// "you are not allowed", which is also how the HTTP status differs.
	ErrUnauthenticated    = errors.New("broker: unauthenticated")
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

// errorCodes maps each broker sentinel to a stable wire code. The code is what a
// remote client matches on, because an error string is not a contract: the HTTP
// data plane used to answer every failure with http.Error(w, err.Error(), 409),
// which flattened authorization failures, lease losses and leader changes into
// indistinguishable text. A client could not recover ErrNotLeader or reach
// ClassifyRetry at all.
var errorCodes = map[error]string{
	ErrClosed:             "closed",
	ErrStopped:            "stopped",
	ErrPaused:             "paused",
	ErrQueueNotFound:      "queue_not_found",
	ErrTopicNotFound:      "topic_not_found",
	ErrStreamNotFound:     "stream_not_found",
	ErrMessageNotFound:    "message_not_found",
	ErrInvalidMessage:     "invalid_message",
	ErrMessageTooLarge:    "message_too_large",
	ErrNoDestination:      "no_destination",
	ErrNoSubscribers:      "no_subscribers",
	ErrDuplicateMessage:   "duplicate_message",
	ErrBackpressure:       "backpressure",
	ErrPublishTimeout:     "publish_timeout",
	ErrRequestTimeout:     "request_timeout",
	ErrNoHandler:          "no_handler",
	ErrUnauthorized:       "unauthorized",
	ErrUnauthenticated:    "unauthenticated",
	ErrLeaseLost:          "lease_lost",
	ErrPermanent:          "permanent",
	ErrTransactionClosed:  "transaction_closed",
	ErrUnsupportedMode:    "unsupported_mode",
	ErrBrokerUnavailable:  "broker_unavailable",
	ErrCallbackFailed:     "callback_failed",
	ErrCancelled:          "cancelled",
	ErrConsumerBanned:     "consumer_banned",
	ErrRateLimited:        "rate_limited",
	ErrNotLeader:          "not_leader",
	ErrStaleEpoch:         "stale_epoch",
	ErrQuorumUnavailable:  "quorum_unavailable",
	ErrThrottled:          "throttled",
	ErrTransactionAborted: "transaction_aborted",
	ErrStorageUnavailable: "storage_unavailable",
}

// errorSentinels is the reverse mapping, used by clients to recover a comparable
// sentinel from a wire code.
var errorSentinels = func() map[string]error {
	out := make(map[string]error, len(errorCodes))
	for sentinel, code := range errorCodes {
		out[code] = sentinel
	}
	return out
}()

// ErrorCode returns the stable wire code for err, or "internal" when err does not
// wrap a known sentinel.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var be BrokerError
	if errors.As(err, &be) && be.Code != "" {
		return be.Code
	}
	for sentinel, code := range errorCodes {
		if errors.Is(err, sentinel) {
			return code
		}
	}
	return "internal"
}

// SentinelForCode returns the broker sentinel a wire code refers to.
func SentinelForCode(code string) (error, bool) {
	sentinel, ok := errorSentinels[code]
	return sentinel, ok
}

// HTTPStatusForError maps a broker error onto a status code. Everything the data
// plane previously reported as 409 is separated here so a client can tell an
// authorization failure from a conflict, a missing resource from a bad request,
// and a retryable condition from a permanent one.
func HTTPStatusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if IsPermanent(err) {
		return http.StatusBadRequest
	}
	switch {
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrConsumerBanned):
		return http.StatusForbidden
	case errors.Is(err, ErrLeaseLost):
		return http.StatusConflict
	case errors.Is(err, ErrInvalidMessage), errors.Is(err, ErrPermanent), errors.Is(err, ErrUnsupportedMode):
		return http.StatusBadRequest
	case errors.Is(err, ErrMessageTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrQueueNotFound), errors.Is(err, ErrTopicNotFound),
		errors.Is(err, ErrStreamNotFound), errors.Is(err, ErrMessageNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrDuplicateMessage):
		return http.StatusConflict
	case errors.Is(err, ErrBackpressure), errors.Is(err, ErrRateLimited), errors.Is(err, ErrThrottled):
		return http.StatusTooManyRequests
	case errors.Is(err, ErrPublishTimeout), errors.Is(err, ErrRequestTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, ErrBrokerUnavailable), errors.Is(err, ErrStorageUnavailable),
		errors.Is(err, ErrQuorumUnavailable), errors.Is(err, ErrNotLeader),
		errors.Is(err, ErrStaleEpoch), errors.Is(err, ErrStopped):
		return http.StatusServiceUnavailable
	default:
		return http.StatusConflict
	}
}

// wireError is the error envelope written by the data plane.
type wireError struct {
	Error wireErrorBody `json:"error"`
}

type wireErrorBody struct {
	Code      string              `json:"code"`
	Message   string              `json:"message"`
	Retryable bool                `json:"retryable,omitempty"`
	Class     RetryClassification `json:"class,omitempty"`
	Details   map[string]string   `json:"details,omitempty"`
}

// WriteBrokerError writes err as a JSON envelope with a meaningful status code.
//
// The body is additive: a client that does not understand it still sees a
// non-empty message, and WriteBrokerError keeps the message text identical to
// err.Error() so existing string-matching clients keep working.
func WriteBrokerError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	class, retryable := ClassifyRetry(err)
	body := wireErrorBody{
		Code:      ErrorCode(err),
		Message:   err.Error(),
		Retryable: retryable,
		Class:     class,
	}
	var leader *RetryError
	if errors.As(err, &leader) {
		if leader.Leader != "" {
			if body.Details == nil {
				body.Details = map[string]string{}
			}
			body.Details["leader"] = leader.Leader
		}
		if leader.LeaderEpoch != 0 {
			if body.Details == nil {
				body.Details = map[string]string{}
			}
			body.Details["leader_epoch"] = fmt.Sprintf("%d", leader.LeaderEpoch)
		}
		if leader.RetryAfter > 0 {
			if body.Details == nil {
				body.Details = map[string]string{}
			}
			body.Details["retry_after"] = leader.RetryAfter.String()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(HTTPStatusForError(err))
	_ = json.NewEncoder(w).Encode(wireError{Error: body})
}

// RemoteError is a broker error recovered from the wire. It unwraps to the
// matching broker sentinel, so errors.Is(err, broker.ErrNotLeader) behaves the
// same for an embedded caller and a remote one.
type RemoteError struct {
	Status    int
	Code      string
	Message   string
	Retryable bool
	Class     RetryClassification
	Details   map[string]string
}

func (e *RemoteError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "broker: " + e.Code
}

// Unwrap maps the wire code back to its sentinel.
func (e *RemoteError) Unwrap() error {
	if sentinel, ok := SentinelForCode(e.Code); ok {
		return sentinel
	}
	return nil
}

// RetryHint returns the leader address the broker advertised, if any, so a client
// can redirect instead of parsing the message text.
func (e *RemoteError) RetryHint() (leader string, epoch uint64, retryAfter time.Duration) {
	if e.Details == nil {
		return "", 0, 0
	}
	leader = e.Details["leader"]
	if v, err := strconv.ParseUint(e.Details["leader_epoch"], 10, 64); err == nil {
		epoch = v
	}
	if v, err := time.ParseDuration(e.Details["retry_after"]); err == nil {
		retryAfter = v
	}
	return leader, epoch, retryAfter
}

// ParseRemoteError decodes a data-plane error response. It returns nil when the
// body is not an envelope, so a server that still answers with plain text keeps
// working.
func ParseRemoteError(status int, body []byte) *RemoteError {
	var env wireError
	if err := json.Unmarshal(body, &env); err != nil || env.Error.Code == "" {
		return nil
	}
	return &RemoteError{
		Status:    status,
		Code:      env.Error.Code,
		Message:   env.Error.Message,
		Retryable: env.Error.Retryable,
		Class:     env.Error.Class,
		Details:   env.Error.Details,
	}
}
