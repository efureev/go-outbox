// Package events defines the domain events the dispatcher publishes on the
// in-process bus, and the topics they travel on.
//
// The granularity is one event per pipeline iteration, not one per message.
// That is what keeps the bus off the hot path: a batch of two hundred messages
// produces two hundred broker round trips and exactly one event. The event
// still carries per-message detail, so an observer can fill a latency histogram
// without the publisher having to know a histogram exists.
package events

import (
	"time"

	"github.com/efureev/msghub/v3"
)

// Topics. Names are stable; they appear in msghub's own diagnostics.
var (
	TopicIteration  = msghub.NewTopic[Iteration]("outbox.iteration")
	TopicReclaimed  = msghub.NewTopic[Reclaimed]("outbox.reclaimed")
	TopicStats      = msghub.NewTopic[Stats]("outbox.stats")
	TopicRetention  = msghub.NewTopic[Retention]("outbox.retention")
	TopicDeadLetter = msghub.NewTopic[DeadLetter]("outbox.dead_letter")
)

// Publish is the outcome of publishing one message, as seen by the pipeline.
type Publish struct {
	ID       string
	Duration time.Duration
	// Err is nil when the broker accepted the message.
	Err error
	// Permanent marks a failure retrying cannot fix.
	Permanent bool
}

// Delivery is one message the broker accepted, with the lag measured by the
// database clock rather than this process's.
type Delivery struct {
	ID  string
	Lag time.Duration
}

// Terminal is one message that stopped being retried, and why.
type Terminal struct {
	ID       string
	Attempts int
	// Permanent distinguishes a failure that could not be retried from one that
	// ran out of attempts. Both end in the same status; only one of them means
	// the broker was ever the problem.
	Permanent bool
}

// Iteration is one complete claim-publish-write-back cycle of one pipeline.
type Iteration struct {
	Stream string
	Driver string

	// Claimed is the batch size. A batch that keeps arriving full means the
	// dispatcher is not keeping up.
	Claimed int
	// Retries counts claimed messages that had already failed at least once.
	Retries int

	Publishes []Publish
	Delivered []Delivery
	Requeued  []string
	Failed    []Terminal

	// Conflicts counts write-backs rejected because the lease had been
	// reclaimed by another replica mid-flight. It should be zero; anything else
	// means the lease is shorter than the work.
	Conflicts int

	// Released counts claims handed back untouched during a shutdown.
	Released int

	Duration time.Duration
}

// Reclaimed reports leases whose owner never released them.
type Reclaimed struct {
	Stream  string
	Owner   string
	Overdue time.Duration
}

// Stats is a gauge sample.
type Stats struct {
	Pending       int64
	Processing    int64
	Failed        int64
	OldestPending time.Duration
}

// Retention reports a sweep of delivered rows.
type Retention struct {
	Deleted int64
}

// DeadLetter reports an attempt to forward a failed message to the dead-letter
// destination.
type DeadLetter struct {
	Stream string
	ID     string
	Err    error
}
