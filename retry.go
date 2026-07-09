package fastworker

import "errors"

type RetryableError struct{ Err error }

func (e RetryableError) Error() string {
	if e.Err == nil {
		return "retryable"
	}
	return e.Err.Error()
}
func (e RetryableError) Unwrap() error { return e.Err }
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return RetryableError{Err: err}
}
func IsRetryable(err error) bool { var r RetryableError; return errors.As(err, &r) }
