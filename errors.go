package fastworker

import "errors"

var (
	ErrClosed        = errors.New("fastworker: pool closed")
	ErrTerminated    = errors.New("fastworker: pool terminated")
	ErrPaused        = errors.New("fastworker: pool paused")
	ErrQueueFull     = errors.New("fastworker: queue full")
	ErrTimeout       = errors.New("fastworker: timeout")
	ErrCancelled     = errors.New("fastworker: cancelled")
	ErrDuplicate     = errors.New("fastworker: duplicate job")
	ErrRateLimited   = errors.New("fastworker: rate limited")
	ErrInvalidConfig = errors.New("fastworker: invalid config")
	ErrPermanent     = errors.New("fastworker: permanent error")
)

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
func IsPermanent(err error) bool { var p PermanentError; return errors.As(err, &p) }
