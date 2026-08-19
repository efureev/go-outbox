package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/efureev/go-outbox/internal/core"
)

// classify decides what a failed insert means.
//
// PostgreSQL says more about its own failures than a broker does: every server
// error carries a SQLSTATE, and the first two characters are a class the
// standard defines. That makes this the least guessy classifier of the three
// drivers — most of it is reading what the server already said.
//
// The rule that governs the doubtful cases is the same as everywhere else:
// erring towards Unavailable is the expensive direction, because a per-message
// problem mistaken for an outage never advances its attempt counter and so
// never reaches failed. Anything not positively an availability problem stays
// retryable.
//
// parentLive separates our own write deadline from the caller's cancellation. A
// deadline firing while the caller still runs means the database did not answer
// in time; the same error during a shutdown means the process is stopping.
func classify(err error, parentLive bool) error {
	if err == nil {
		return nil
	}

	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return classifyServer(pg, err)
	}

	// Below here the server never answered, so it never judged the message.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if parentLive && errors.Is(err, context.DeadlineExceeded) {
			return core.Unavailable("insert timed out", err)
		}

		return err
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return core.Unavailable("cannot connect", err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return core.Unavailable("network", err)
	}

	return err
}

// classifyServer reads the SQLSTATE the server sent.
func classifyServer(pg *pgconn.PgError, err error) error {
	switch pg.Code {
	// The destination is gone or was never there, the credentials do not carry
	// the right, the column list does not match. Retrying reaches the same
	// answer, and the fix is a migration or a grant.
	case "42P01", // undefined_table
		"42703", // undefined_column
		"42501", // insufficient_privilege
		"3F000", // invalid_schema_name
		"42804", // datatype_mismatch
		"42P02": // undefined_parameter
		return core.Permanent(pg.Code+" "+pg.Message, err)

	// The server is there and cannot take the write. None of it is a verdict
	// on this message.
	case "53300", // too_many_connections
		"53100", // disk_full
		"53200", // out_of_memory
		"57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03", // cannot_connect_now
		"58030": // io_error
		return core.Unavailable(pg.Code+" "+pg.Message, err)

	// A statement cancelled by statement_timeout. The database answered, but
	// what it said is "I am busy", not "this message is wrong".
	case "57014": // query_canceled
		return core.Unavailable(pg.Code+" "+pg.Message, err)
	}

	// Everything else by class, which is what the two leading characters mean.
	switch class(pg.Code) {
	case "23":
		// Integrity constraint violation: a NOT NULL, a CHECK, a foreign key,
		// or a unique index other than the one ON CONFLICT already handles.
		// The row does not fit this table and will not fit it tomorrow.
		return core.Permanent(pg.Code+" "+pg.Message, err)
	case "40", "55":
		// Serialization failure, deadlock, a lock not available. Retrying is
		// exactly the remedy, and the attempt is fairly spent.
		return err
	case "08":
		// Connection exception.
		return core.Unavailable(pg.Code+" "+pg.Message, err)
	case "22", "42":
		// Data exception and syntax/access violation. Both are about this
		// statement and this row.
		return core.Permanent(pg.Code+" "+pg.Message, err)
	}

	return err
}

func class(code string) string {
	if len(code) < 2 {
		return ""
	}

	return code[:2]
}

// encodeHeaders renders a message's headers for the jsonb column.
//
// A string rather than a byte slice: both reach a jsonb column intact through
// pgx, but the column is text-shaped in the statement and cast on the server,
// which keeps the parameter type unambiguous.
func encodeHeaders(msg core.Message) (string, error) {
	if len(msg.Headers) == 0 {
		return "{}", nil
	}

	raw, err := json.Marshal(msg.Headers)
	if err != nil {
		// Not the database's fault and not fixable by retrying: this message
		// carries headers that are not encodable.
		return "", core.Permanent("headers are not encodable", fmt.Errorf("message %s: %w", msg.ID, err))
	}

	return string(raw), nil
}
