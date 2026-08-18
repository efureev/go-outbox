package core

import (
	"errors"
	"fmt"
)

// PermanentError marks a publish failure that retrying cannot fix. The
// dispatcher sends such a message straight to StatusFailed instead of spending
// the whole attempt budget on it: retrying an unroutable message or an unknown
// stream five times, each after a longer backoff, reaches the same conclusion
// an hour later.
//
// Permanent, in practice: an unknown stream or driver, a broker rejecting the
// payload as too large, an unroutable publish (RabbitMQ basic.return), an
// unknown topic with auto-creation disabled, and authentication failures.
// Everything else is retryable.
type PermanentError struct {
	Reason string
	Err    error
}

func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "permanent: " + e.Reason
	}

	return fmt.Sprintf("permanent (%s): %v", e.Reason, e.Err)
}

func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a non-retryable failure.
func Permanent(reason string, err error) error {
	return &PermanentError{Reason: reason, Err: err}
}

// IsPermanent reports whether err — or anything it wraps — is permanent.
func IsPermanent(err error) bool {
	var pe *PermanentError

	return errors.As(err, &pe)
}

// Sentinel errors for conditions the dispatcher and store distinguish.
var (
	// ErrUnknownStream is returned when a message names a stream that is not
	// configured. It is permanent by construction: no amount of retrying will
	// make the stream appear.
	ErrUnknownStream = errors.New("unknown stream")
	// ErrUnknownDriver is returned when a configured stream points at a driver
	// that was never built.
	ErrUnknownDriver = errors.New("unknown driver")
	// ErrLeaseLost reports that a write-back matched fewer rows than it was
	// given, meaning the lease was reclaimed by another instance mid-flight.
	// It is not a failure of the message — the row belongs to someone else now.
	ErrLeaseLost = errors.New("lease lost")
)
