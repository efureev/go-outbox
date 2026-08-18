// Package core holds the outbox domain: what a message is, how a failure is
// classified and how the next attempt is scheduled. It depends on nothing in
// this program, so the rules can be read — and tested — without a database or
// a broker in the way.
package core

import (
	"time"
)

// Status is the lifecycle state of an outbox row. It is stored as a SMALLINT
// with a CHECK constraint rather than as text: the set is closed, and the
// schema should say so.
type Status int16

const (
	// StatusPending — ready to be claimed once available_at has passed.
	StatusPending Status = 0
	// StatusProcessing — claimed by an instance holding a lease.
	StatusProcessing Status = 1
	// StatusSent — accepted by the broker. Terminal.
	StatusSent Status = 2
	// StatusFailed — attempts exhausted, or a permanent error. Terminal until
	// an explicit requeue.
	StatusFailed Status = 3
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusProcessing:
		return "processing"
	case StatusSent:
		return "sent"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Valid reports whether s is one of the four defined statuses.
func (s Status) Valid() bool { return s >= StatusPending && s <= StatusFailed }

// Message is one outbox row as the dispatcher sees it: everything needed to
// publish, plus the bookkeeping needed to write the outcome back.
type Message struct {
	ID       string
	Stream   string
	Topic    string
	Payload  []byte
	Headers  map[string]string
	Target   Target
	Attempts int

	// CreatedAt is read back from the database so delivery lag is measured
	// against the database clock rather than this process's. The two disagree,
	// and a lag metric that spans both absorbs the difference silently.
	CreatedAt time.Time
}

// Target is the routing envelope stored in the target JSONB column. Unknown
// keys are preserved by the database and ignored here; that is the extension
// point.
type Target struct {
	// Key is the partition key. Kafka uses it; RabbitMQ ignores it.
	Key string `json:"key,omitempty"`
	// Version, when above zero, appends a "vN" suffix to the effective topic
	// name. It lived in a dedicated SMALLINT column before, which read as if
	// it versioned the row rather than the topic name.
	Version int `json:"version,omitempty"`
	// Exchange and RoutingKey are RabbitMQ-only. Empty Exchange means the
	// default exchange, and an empty RoutingKey means the topic name, so a
	// message that says nothing about routing still goes somewhere sensible.
	Exchange   string `json:"exchange,omitempty"`
	RoutingKey string `json:"routing_key,omitempty"`
}

// Lease identifies one claim. The token is what makes a write-back safe when
// several instances run: only the holder of the current token may finalize a
// row, so an instance whose lease expired mid-flight cannot overwrite the
// outcome recorded by whoever reclaimed the row.
type Lease struct {
	Token string
	Owner string
	Until time.Time
}

// Outcome is the result of publishing one message, on its way back to the
// database.
type Outcome struct {
	ID string
	// Err is nil on success.
	Err error
	// Permanent marks a failure that retrying cannot fix, so the message goes
	// straight to StatusFailed instead of burning the attempt budget.
	Permanent bool
	// Delay is how long to wait before the next attempt. Computed in Go rather
	// than in SQL so the backoff policy — including its jitter — stays
	// testable.
	Delay time.Duration
}
