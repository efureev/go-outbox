package core

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestIsPermanentSeesThroughWrapping(t *testing.T) {
	base := errors.New("no such exchange")
	err := fmt.Errorf("publish: %w", Permanent("unroutable", base))

	if !IsPermanent(err) {
		t.Error("IsPermanent should see through wrapping")
	}
	if !errors.Is(err, base) {
		t.Error("the original cause should stay reachable with errors.Is")
	}
}

func TestIsPermanentRejectsOrdinaryErrors(t *testing.T) {
	if IsPermanent(fmt.Errorf("connection reset: %w", errors.New("EOF"))) {
		t.Error("an ordinary error must be retryable")
	}
	if IsPermanent(nil) {
		t.Error("nil is not a permanent failure")
	}
}

func TestPermanentErrorMessageNamesTheReason(t *testing.T) {
	err := Permanent("payload too large", errors.New("limit is 1MiB"))

	want := "permanent (payload too large): limit is 1MiB"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestStatusString(t *testing.T) {
	for status, want := range map[Status]string{
		StatusPending:    "pending",
		StatusProcessing: "processing",
		StatusSent:       "sent",
		StatusFailed:     "failed",
		Status(9):        "unknown",
	} {
		if got := status.String(); got != want {
			t.Errorf("Status(%d).String() = %q, want %q", status, got, want)
		}
	}
}

func TestUnavailableSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("publishing batch: %w", Unavailable("rabbitmq unreachable", io.ErrUnexpectedEOF))

	if !IsUnavailable(err) {
		t.Error("a wrapped unavailable error is no longer recognised, so the message would be charged an attempt")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Error("the cause is not reachable through the wrapper")
	}
}

// The two classifications answer different questions and must not be confused:
// permanence decides whether to retry at all, unavailability whether the
// attempt counts. An ordinary error is neither.
func TestClassificationsAreIndependent(t *testing.T) {
	cases := map[string]struct {
		err                  error
		permanent, unavailab bool
	}{
		"plain":       {errors.New("broker nacked"), false, false},
		"permanent":   {Permanent("unroutable", nil), true, false},
		"unavailable": {Unavailable("no connection", nil), false, true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := IsPermanent(tc.err); got != tc.permanent {
				t.Errorf("IsPermanent = %v, want %v", got, tc.permanent)
			}
			if got := IsUnavailable(tc.err); got != tc.unavailab {
				t.Errorf("IsUnavailable = %v, want %v", got, tc.unavailab)
			}
		})
	}
}

// A message the broker would reject is failed whether or not the connection
// also dropped — retrying reaches the same verdict, and deferring it forever
// would mean it never reaches failed at all. The dispatcher reads permanence
// first for that reason, and this records the intent.
func TestPermanenceOutranksUnavailability(t *testing.T) {
	err := Permanent("payload too large", Unavailable("connection lost", nil))

	if !IsPermanent(err) {
		t.Fatal("permanence was lost")
	}
	if !IsUnavailable(err) {
		t.Fatal("this test would be meaningless if the inner classification were not also present")
	}
}

// Valid is what stands between the database's CHECK constraint and a status
// read back from a row. Both ends of the range matter: the set is closed, and a
// predicate that is wrong by one at either end lets through a value the schema
// would reject.
func TestStatusValidAcceptsExactlyTheFourStatuses(t *testing.T) {
	valid := []Status{
		StatusPending, StatusProcessing, StatusSent, StatusFailed,
	}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("%s (%d) is not valid, and it is one of the four", s, s)
		}
	}

	// Immediately outside at each end, plus the far cases.
	for _, s := range []Status{-1, 4, -128, 127} {
		if s.Valid() {
			t.Errorf("status %d is accepted as valid", s)
		}
	}
}

// The two error messages are what an operator reads in `outbox failed` and in
// the log, so both forms of each have to say which class the failure is in.
// A wrapped cause is the common case; a bare reason happens when the classifier
// itself is the only thing that knows.
func TestErrorMessagesNameTheirClass(t *testing.T) {
	cause := errors.New("connection reset")

	cases := []struct {
		name string
		err  error
		want []string
		not  string
	}{
		{
			name: "permanent, with a cause",
			err:  Permanent("unroutable", cause),
			want: []string{"permanent", "unroutable", "connection reset"},
			not:  "unavailable",
		},
		{
			name: "permanent, on its own",
			err:  Permanent("unknown stream", nil),
			want: []string{"permanent", "unknown stream"},
			not:  "%!v(MISSING)",
		},
		{
			name: "unavailable, with a cause",
			err:  Unavailable("dial failed", cause),
			want: []string{"unavailable", "dial failed", "connection reset"},
			not:  "permanent",
		},
		{
			name: "unavailable, on its own",
			err:  Unavailable("broker down", nil),
			want: []string{"unavailable", "broker down"},
			not:  "permanent",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := c.err.Error()

			for _, want := range c.want {
				if !strings.Contains(msg, want) {
					t.Errorf("%q does not contain %q", msg, want)
				}
			}
			if strings.Contains(msg, c.not) {
				t.Errorf("%q contains %q, which belongs to the other form", msg, c.not)
			}
			// A nil cause must not leave the verb dangling in the output.
			if strings.Contains(msg, "<nil>") {
				t.Errorf("%q renders a nil cause", msg)
			}
		})
	}
}
