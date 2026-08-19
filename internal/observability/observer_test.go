package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/events"
)

// Twenty metrics ship with this program, and until now nothing exercised the
// code that fills them. A counter that is never incremented and a counter that
// is incremented into the wrong label look identical on a dashboard: empty.

func newMetrics(t *testing.T) *Metrics {
	t.Helper()

	return New(prometheus.NewRegistry(), config.BrokerConfig{
		Streams: map[string]config.StreamConfig{"local": {Driver: "rmq"}},
		Drivers: map[string]config.DriverConfig{"rmq": nil},
	})
}

func iteration() events.Iteration {
	return events.Iteration{
		Stream: "local", Driver: "rmq",
		Claimed: 5, Retries: 2, Duration: 250 * time.Millisecond,
	}
}

func TestClaimsAreSplitByFirstAttemptAndRetry(t *testing.T) {
	m := newMetrics(t)

	ev := iteration() // five claimed, two of them retries
	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	initial := testutil.ToFloat64(m.MessagesClaimed.WithLabelValues("local", "rmq", AttemptInitial))
	retry := testutil.ToFloat64(m.MessagesClaimed.WithLabelValues("local", "rmq", AttemptRetry))

	if initial != 3 || retry != 2 {
		t.Errorf("initial = %v, retry = %v; want 3 and 2", initial, retry)
	}
}

// Retried and deferred are counted apart on purpose: one is the dispatcher
// working through a rejection, the other is it waiting for a broker. An alert
// that cannot tell them apart fires on the wrong one.
func TestRetriesAndDeferralsAreCountedApart(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Requeued = []string{"a", "b"}
	ev.Deferred = []string{"c"}

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	if got := testutil.ToFloat64(m.MessagesRetried.WithLabelValues("local", "rmq")); got != 2 {
		t.Errorf("retried = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.MessagesDeferred.WithLabelValues("local", "rmq")); got != 1 {
		t.Errorf("deferred = %v, want 1", got)
	}
}

// A message failed for outrunning MaxDefer was never rejected and its attempt
// counter reads zero. Reporting it as exhausted sends whoever reads the metric
// looking for a rejection that never happened.
func TestFailuresAreLabelledByTheirRealReason(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Failed = []events.Terminal{
		{ID: "a", Permanent: true},
		{ID: "b"},
		{ID: "c", Deferred: true},
	}

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	for reason, want := range map[string]float64{
		ReasonPermanent:   1,
		ReasonExhausted:   1,
		ReasonUnreachable: 1,
	} {
		got := testutil.ToFloat64(m.MessagesFailed.WithLabelValues("local", "rmq", reason))
		if got != want {
			t.Errorf("failed{reason=%q} = %v, want %v", reason, got, want)
		}
	}
}

func TestDeliveriesAndLagAreRecorded(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Delivered = []events.Delivery{
		{ID: "a", Lag: time.Second},
		{ID: "b", Lag: 2 * time.Second},
	}

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	if got := testutil.ToFloat64(m.MessagesDispatched.WithLabelValues("local", "rmq")); got != 2 {
		t.Errorf("dispatched = %v, want 2", got)
	}
}

func TestPublishErrorsCarryTheirClass(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Publishes = []events.Publish{
		{ID: "a"},
		{ID: "b", Err: errors.New("nacked")},
		{ID: "c", Err: errors.New("gone"), Permanent: true},
		{ID: "d", Err: errors.New("unreachable"), Deferred: true},
	}

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	for kind, want := range map[string]float64{
		KindRetryable:   1,
		KindPermanent:   1,
		KindUnavailable: 1,
	} {
		got := testutil.ToFloat64(m.BrokerErrors.WithLabelValues("local", "rmq", "publish", kind))
		if got != want {
			t.Errorf("broker_errors{kind=%q} = %v, want %v", kind, got, want)
		}
	}
}

func TestLeaseConflictsAreCounted(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Conflicts = 3

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	if got := testutil.ToFloat64(m.LeaseConflicts.WithLabelValues("local")); got != 3 {
		t.Errorf("lease_conflicts = %v, want 3", got)
	}
}

// A stream name arrives from a row a producer wrote. Letting it through as a
// label value unchecked lets a producer mint unbounded time series.
func TestUnknownLabelsCollapseIntoOneBucket(t *testing.T) {
	m := newMetrics(t)

	ev := iteration()
	ev.Stream, ev.Driver = "invented-by-a-producer", "invented-too"

	if err := m.onIteration(context.Background(), ev); err != nil {
		t.Fatalf("onIteration: %v", err)
	}

	got := testutil.ToFloat64(m.MessagesClaimed.WithLabelValues(UnknownLabel, UnknownLabel, AttemptInitial))
	if got != 3 {
		t.Errorf("an unconfigured stream did not collapse into %q: %v", UnknownLabel, got)
	}
}

func TestReclaimedIsRecordedWithItsAge(t *testing.T) {
	m := newMetrics(t)

	err := m.onReclaimed(context.Background(),
		events.Reclaimed{Stream: "local", Owner: "replica-2", Overdue: 30 * time.Second})
	if err != nil {
		t.Fatalf("onReclaimed: %v", err)
	}

	if got := testutil.ToFloat64(m.MessagesReclaimed.WithLabelValues("local")); got != 1 {
		t.Errorf("reclaimed = %v, want 1", got)
	}
}

func TestStatsFillTheGauges(t *testing.T) {
	m := newMetrics(t)

	err := m.onStats(context.Background(), events.Stats{
		Pending: 10, Processing: 2, Failed: 1, Deferred: 4,
		OldestPending: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("onStats: %v", err)
	}

	for status, want := range map[string]float64{"pending": 10, "processing": 2, "failed": 1} {
		if got := testutil.ToFloat64(m.MessagesByStatus.WithLabelValues(status)); got != want {
			t.Errorf("by_status{%s} = %v, want %v", status, got, want)
		}
	}
	if got := testutil.ToFloat64(m.MessagesDeferredNow); got != 4 {
		t.Errorf("deferred gauge = %v, want 4", got)
	}
	if got := testutil.ToFloat64(m.OldestPendingAge); got != 90 {
		t.Errorf("oldest_pending_age = %v, want 90", got)
	}
}

// This gauge is the signal that outlives the condition it reports: once claims
// stop nothing is published, so the deferral counter goes quiet exactly when the
// outage is most established.
func TestTheBreakerGaugeFollowsTheStream(t *testing.T) {
	m := newMetrics(t)

	if err := m.onBreaker(context.Background(),
		events.Breaker{Stream: "local", Driver: "rmq", Paused: true}); err != nil {
		t.Fatalf("onBreaker: %v", err)
	}
	if got := testutil.ToFloat64(m.StreamPaused.WithLabelValues("local")); got != 1 {
		t.Fatalf("stream_paused = %v, want 1", got)
	}

	if err := m.onBreaker(context.Background(),
		events.Breaker{Stream: "local", Driver: "rmq", Paused: false}); err != nil {
		t.Fatalf("onBreaker: %v", err)
	}
	if got := testutil.ToFloat64(m.StreamPaused.WithLabelValues("local")); got != 0 {
		t.Errorf("stream_paused = %v after recovery, want 0", got)
	}
}

func TestRetentionAndPartitions(t *testing.T) {
	m := newMetrics(t)

	if err := m.onRetention(context.Background(), events.Retention{Deleted: 5000}); err != nil {
		t.Fatalf("onRetention: %v", err)
	}
	if got := testutil.ToFloat64(m.RetentionDeleted); got != 5000 {
		t.Errorf("retention_deleted = %v", got)
	}

	err := m.onPartitions(context.Background(),
		events.Partitions{Created: 3, Dropped: 2, DefaultRows: 7})
	if err != nil {
		t.Fatalf("onPartitions: %v", err)
	}
	if got := testutil.ToFloat64(m.PartitionsDropped); got != 2 {
		t.Errorf("partitions_dropped = %v, want 2", got)
	}
	// Should be zero in a healthy deployment; it is reported so somebody notices.
	if got := testutil.ToFloat64(m.DefaultPartitionRows); got != 7 {
		t.Errorf("default_partition_rows = %v, want 7", got)
	}
}

func TestDeadLetterIsCountedByOutcome(t *testing.T) {
	m := newMetrics(t)

	if err := m.onDeadLetter(context.Background(), events.DeadLetter{Stream: "local"}); err != nil {
		t.Fatalf("onDeadLetter: %v", err)
	}
	if err := m.onDeadLetter(context.Background(),
		events.DeadLetter{Stream: "local", Err: errors.New("gone")}); err != nil {
		t.Fatalf("onDeadLetter: %v", err)
	}

	if got := testutil.ToFloat64(m.DLQPublished.WithLabelValues("local", ResultSuccess)); got != 1 {
		t.Errorf("dlq success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.DLQPublished.WithLabelValues("local", ResultError)); got != 1 {
		t.Errorf("dlq error = %v, want 1", got)
	}
}

func TestErrorKindAndAttempt(t *testing.T) {
	cases := []struct {
		permanent, deferred bool
		want                string
	}{
		{true, false, KindPermanent},
		{false, true, KindUnavailable},
		{false, false, KindRetryable},
		// Permanence wins: a message the broker would reject stays permanent
		// even when the connection also dropped.
		{true, true, KindPermanent},
	}

	for _, tc := range cases {
		if got := ErrorKind(tc.permanent, tc.deferred); got != tc.want {
			t.Errorf("ErrorKind(%v, %v) = %q, want %q", tc.permanent, tc.deferred, got, tc.want)
		}
	}

	if Attempt(0) != AttemptInitial || Attempt(3) != AttemptRetry {
		t.Error("Attempt does not distinguish a first try from a retry")
	}
}

// Absent series and zero series are different things to an alert expression, so
// everything the configuration implies exists before the first message.
func TestSeriesExistBeforeTheFirstMessage(t *testing.T) {
	m := newMetrics(t)

	for _, sample := range []float64{
		testutil.ToFloat64(m.MessagesDispatched.WithLabelValues("local", "rmq")),
		testutil.ToFloat64(m.MessagesRetried.WithLabelValues("local", "rmq")),
		testutil.ToFloat64(m.MessagesDeferred.WithLabelValues("local", "rmq")),
		testutil.ToFloat64(m.StreamPaused.WithLabelValues("local")),
		testutil.ToFloat64(m.LeaseConflicts.WithLabelValues("local")),
	} {
		if sample != 0 {
			t.Errorf("a pre-created series is not zero: %v", sample)
		}
	}
}
