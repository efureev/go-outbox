package postgres

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/efureev/go-outbox/internal/core"
)

func pgErr(code, message string) error {
	return &pgconn.PgError{Code: code, Message: message}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		parentLive bool
		permanent  bool
		deferred   bool
	}{
		{
			name:      "the inbox does not exist",
			err:       pgErr("42P01", `relation "billing.inbox" does not exist`),
			permanent: true,
		},
		{
			name:      "no right to write to it",
			err:       pgErr("42501", "permission denied for table inbox"),
			permanent: true,
		},
		{
			name:      "a column the consumer removed",
			err:       pgErr("42703", `column "topic" does not exist`),
			permanent: true,
		},
		{
			// The row does not fit this table and will not fit it tomorrow.
			name:      "a NOT NULL the message cannot satisfy",
			err:       pgErr("23502", `null value in column "payload"`),
			permanent: true,
		},
		{
			name:      "a unique index other than the one ON CONFLICT handles",
			err:       pgErr("23505", "duplicate key value violates unique constraint"),
			permanent: true,
		},
		{
			name:     "the server is out of connections",
			err:      pgErr("53300", "sorry, too many clients already"),
			deferred: true,
		},
		{
			name:     "the server is shutting down",
			err:      pgErr("57P01", "terminating connection due to administrator command"),
			deferred: true,
		},
		{
			// The database answered, and what it said is "I am busy" — not
			// "this message is wrong".
			name:     "statement_timeout fired",
			err:      pgErr("57014", "canceling statement due to statement timeout"),
			deferred: true,
		},
		{
			name: "a deadlock, which retrying is the remedy for",
			err:  pgErr("40P01", "deadlock detected"),
		},
		{
			name: "a serialization failure",
			err:  pgErr("40001", "could not serialize access"),
		},
		{
			name:     "the connection is gone",
			err:      pgErr("08006", "connection failure"),
			deferred: true,
		},
		{
			name:     "nothing answered at all",
			err:      &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			deferred: true,
		},
		{
			name:       "our own write deadline fired",
			err:        context.DeadlineExceeded,
			parentLive: true,
			deferred:   true,
		},
		{
			// Otherwise every message in flight during a shutdown is recorded
			// as an outage and waits out a deferral it never earned.
			name:       "the same deadline during a shutdown is not an outage",
			err:        context.DeadlineExceeded,
			parentLive: false,
		},
		{
			name: "an unrecognised error stays retryable",
			err:  errors.New("something else went wrong"),
		},
		{
			name: "nil stays nil",
			err:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.err, tc.parentLive)

			if tc.err == nil {
				if got != nil {
					t.Fatalf("classify(nil) = %v", got)
				}

				return
			}

			if core.IsPermanent(got) != tc.permanent {
				t.Errorf("IsPermanent = %v, want %v (%v)", core.IsPermanent(got), tc.permanent, got)
			}
			if core.IsUnavailable(got) != tc.deferred {
				t.Errorf("IsUnavailable = %v, want %v (%v)", core.IsUnavailable(got), tc.deferred, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("classification lost the cause: %v", got)
			}
		})
	}
}

// The expensive direction, stated as a test. A per-message problem mistaken for
// an outage never advances its attempt counter and so never reaches failed —
// it is retried forever against a database that will refuse it every time.
func TestARefusalIsNeverMistakenForAnOutage(t *testing.T) {
	for _, code := range []string{"42P01", "42501", "23502", "23514", "23505", "22001"} {
		got := classify(pgErr(code, "refused"), true)

		if core.IsUnavailable(got) {
			t.Errorf("SQLSTATE %s was classified as an outage, so it would retry forever", code)
		}
	}
}

func TestEncodeHeaders(t *testing.T) {
	empty, err := encodeHeaders(core.Message{})
	if err != nil {
		t.Fatalf("encodeHeaders: %v", err)
	}
	if empty != "{}" {
		t.Errorf("no headers encoded as %q, want an empty JSON object", empty)
	}

	got, err := encodeHeaders(core.Message{Headers: map[string]string{"traceparent": "00-ab-cd-01"}})
	if err != nil {
		t.Fatalf("encodeHeaders: %v", err)
	}
	if got != `{"traceparent":"00-ab-cd-01"}` {
		t.Errorf("headers = %s", got)
	}
}
