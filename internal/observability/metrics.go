// Package observability holds the Prometheus metric set.
//
// Two properties are deliberate. The metrics live on an explicit registry
// passed in by the caller rather than on promauto globals registered at init,
// so a test can build its own and read it back. And the label values are
// bounded by the configuration: a stream name arrives from a row a producer
// wrote, and letting it become a label value unchecked lets a producer mint
// unbounded time series.
package observability

import (
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/efureev/go-outbox/internal/config"
)

const namespace = "outbox"

// UnknownLabel is the single bucket every unconfigured stream or driver name
// collapses into.
const UnknownLabel = "__unknown__"

// Claim sources and outcomes, as label values.
const (
	AttemptInitial = "initial"
	AttemptRetry   = "retry"

	ResultSuccess = "success"
	ResultError   = "error"

	ReasonPermanent = "permanent"
	ReasonExhausted = "attempts_exhausted"

	KindPermanent = "permanent"
	KindRetryable = "retryable"
)

// Metrics is the whole metric set. Recording through it rather than through
// package globals is what keeps the registry injectable.
type Metrics struct {
	// known bounds the stream and driver label values to what was configured.
	knownStreams []string
	knownDrivers []string

	MessagesClaimed    *prometheus.CounterVec
	MessagesDispatched *prometheus.CounterVec
	MessagesRetried    *prometheus.CounterVec
	MessagesFailed     *prometheus.CounterVec
	MessagesReclaimed  *prometheus.CounterVec

	// LeaseConflicts counts write-backs that matched fewer rows than they were
	// given, meaning another replica had already reclaimed the lease. It should
	// sit at zero; a rising value means the lease is shorter than the time a
	// batch actually takes, which is the condition under which a message is
	// published twice.
	LeaseConflicts *prometheus.CounterVec

	DLQPublished *prometheus.CounterVec

	PublishDuration   *prometheus.HistogramVec
	IterationDuration *prometheus.HistogramVec
	BatchSize         *prometheus.HistogramVec
	DeliveryLag       *prometheus.HistogramVec
	ReclaimedAge      *prometheus.HistogramVec

	// OldestPendingAge is the age of the oldest undelivered message. It is the
	// single most useful backlog signal: a count says how much is waiting, this
	// says how long the oldest has waited.
	OldestPendingAge prometheus.Gauge
	MessagesByStatus *prometheus.GaugeVec

	BrokerErrors     *prometheus.CounterVec
	DBErrors         *prometheus.CounterVec
	RetentionDeleted prometheus.Counter
}

// New builds the metric set on reg and pre-creates the series for every
// configured stream and driver.
func New(reg prometheus.Registerer, brokers config.BrokerConfig) *Metrics {
	m := &Metrics{
		knownStreams: brokers.StreamNames(),
		knownDrivers: driverNames(brokers),
	}

	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets}, labels)
	}

	m.MessagesClaimed = counter("messages_claimed_total",
		"Messages taken into processing, by stream, driver and whether this is a first attempt or a retry.",
		"stream", "driver", "attempt")
	m.MessagesDispatched = counter("messages_dispatched_total",
		"Messages accepted by a broker.", "stream", "driver")
	m.MessagesRetried = counter("messages_retried_total",
		"Messages returned to pending after a retryable failure.", "stream", "driver")
	m.MessagesFailed = counter("messages_failed_total",
		"Messages moved to failed, either permanently or after exhausting their attempts.",
		"stream", "driver", "reason")
	m.MessagesReclaimed = counter("messages_reclaimed_total",
		"Expired leases returned to pending.", "stream")
	m.LeaseConflicts = counter("lease_conflicts_total",
		"Write-backs rejected because the lease had been reclaimed by another instance.", "stream")
	m.DLQPublished = counter("dlq_published_total",
		"Dead-letter publications, by outcome.", "stream", "result")
	m.BrokerErrors = counter("broker_errors_total",
		"Broker operation errors, by stage and whether retrying can help.",
		"stream", "driver", "stage", "kind")
	m.DBErrors = counter("db_errors_total",
		"Database operation errors, by operation.", "op")

	m.PublishDuration = histogram("publish_duration_seconds",
		"Time spent publishing one message to a broker.",
		[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		"stream", "driver", "result")
	m.IterationDuration = histogram("iteration_duration_seconds",
		"Time taken by one claim-publish-write-back cycle.",
		prometheus.DefBuckets, "stream")
	m.BatchSize = histogram("batch_size",
		"Number of messages returned by one claim. A batch that is consistently full means the "+
			"dispatcher is behind.",
		[]float64{0, 1, 5, 10, 25, 50, 100, 200, 500, 1000}, "stream")
	m.DeliveryLag = histogram("delivery_lag_seconds",
		"Time from a message being written by a producer to it being accepted by a broker.",
		lagBuckets, "stream", "driver")
	m.ReclaimedAge = histogram("reclaimed_processing_age_seconds",
		"How long an expired lease had been overdue when it was reclaimed.",
		lagBuckets, "stream")

	m.OldestPendingAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "oldest_pending_age_seconds",
		Help: "Age of the oldest message still waiting to be delivered.",
	})
	m.MessagesByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "messages_by_status",
		Help: "Rows in each non-terminal status. Delivered rows are counted by " +
			"outbox_messages_dispatched_total instead: counting them here would mean scanning the " +
			"whole table on every refresh.",
	}, []string{"status"})
	m.RetentionDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "retention_deleted_total",
		Help: "Delivered rows removed by the retention sweep.",
	})

	reg.MustRegister(
		m.MessagesClaimed, m.MessagesDispatched, m.MessagesRetried, m.MessagesFailed,
		m.MessagesReclaimed, m.LeaseConflicts, m.DLQPublished, m.BrokerErrors, m.DBErrors,
		m.PublishDuration, m.IterationDuration, m.BatchSize, m.DeliveryLag, m.ReclaimedAge,
		m.OldestPendingAge, m.MessagesByStatus, m.RetentionDeleted,
	)

	m.preCreate()

	return m
}

var lagBuckets = []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400}

// preCreate materialises the series that exist by configuration, so a scrape
// before the first message reports zeros rather than absent series — the
// difference between "nothing has failed" and "the metric does not exist yet",
// which an alert expression cannot otherwise tell apart.
func (m *Metrics) preCreate() {
	for _, stream := range m.knownStreams {
		m.MessagesReclaimed.WithLabelValues(stream)
		m.LeaseConflicts.WithLabelValues(stream)

		for _, driver := range m.knownDrivers {
			m.MessagesDispatched.WithLabelValues(stream, driver)
			m.MessagesRetried.WithLabelValues(stream, driver)
			for _, attempt := range []string{AttemptInitial, AttemptRetry} {
				m.MessagesClaimed.WithLabelValues(stream, driver, attempt)
			}
			for _, reason := range []string{ReasonPermanent, ReasonExhausted} {
				m.MessagesFailed.WithLabelValues(stream, driver, reason)
			}
		}
	}

	for _, status := range []string{"pending", "processing", "failed"} {
		m.MessagesByStatus.WithLabelValues(status)
	}
}

// Stream bounds a stream name to the configured set.
func (m *Metrics) Stream(name string) string {
	if slices.Contains(m.knownStreams, name) {
		return name
	}

	return UnknownLabel
}

// Driver bounds a driver name to the configured set.
func (m *Metrics) Driver(name string) string {
	if slices.Contains(m.knownDrivers, name) {
		return name
	}

	return UnknownLabel
}

// ObserveStatusCounts publishes the gauge snapshot taken by the janitor.
func (m *Metrics) ObserveStatusCounts(pending, processing, failed int64, oldestPending time.Duration) {
	m.MessagesByStatus.WithLabelValues("pending").Set(float64(pending))
	m.MessagesByStatus.WithLabelValues("processing").Set(float64(processing))
	m.MessagesByStatus.WithLabelValues("failed").Set(float64(failed))
	m.OldestPendingAge.Set(oldestPending.Seconds())
}

// Attempt labels a claim by whether the message has failed before.
func Attempt(attempts int) string {
	if attempts > 0 {
		return AttemptRetry
	}

	return AttemptInitial
}

// ErrorKind labels a broker error by whether retrying can help.
func ErrorKind(permanent bool) string {
	if permanent {
		return KindPermanent
	}

	return KindRetryable
}

func driverNames(brokers config.BrokerConfig) []string {
	names := make([]string, 0, len(brokers.Drivers))
	for name := range brokers.Drivers {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}
