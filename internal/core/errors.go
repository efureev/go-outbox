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

// UnavailableError marks a failure to reach the broker at all: no live
// connection, a channel closed underneath the publish, a confirmation that
// never arrived because the socket had gone. The broker never saw the message,
// so it never refused it — and charging the message an attempt for that spends
// its budget on somebody else's outage. At the default backoff the whole budget
// is gone in fifteen minutes, so a twenty-minute restart leaves a table full of
// failed rows that only ever needed to wait.
//
// A message failing this way returns to pending on the ordinary backoff with
// its attempt counter untouched, and is marked deferred until it either goes
// through or exceeds DispatchConfig.MaxDefer. What raises the alarm is the age
// of the backlog and outbox_messages_deferred_total, which is the pair an
// operator should be woken by in any case.
//
// The classification has to be conservative in one specific direction: a
// per-message problem mistaken for an outage never advances its counter and so
// never reaches failed. When a driver cannot tell the two apart, the retryable
// answer is the safe one.
type UnavailableError struct {
	Reason string
	Err    error
}

func (e *UnavailableError) Error() string {
	if e.Err == nil {
		return "unavailable: " + e.Reason
	}

	return fmt.Sprintf("unavailable (%s): %v", e.Reason, e.Err)
}

func (e *UnavailableError) Unwrap() error { return e.Err }

// Unavailable wraps err as a failure to reach the broker.
func Unavailable(reason string, err error) error {
	return &UnavailableError{Reason: reason, Err: err}
}

// IsUnavailable reports whether err — or anything it wraps — is a failure to
// reach the broker.
//
// A permanent error wins over this one: a payload the broker would reject
// stays permanent even if the connection also happened to drop, because the
// next attempt would reach the same conclusion. Callers classify permanence
// first for that reason.
func IsUnavailable(err error) bool {
	var ue *UnavailableError

	return errors.As(err, &ue)
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
