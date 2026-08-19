package config

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Validate checks the whole configuration and reports every problem at once.
// Stopping at the first one turns fixing a misconfigured deployment into a
// sequence of restarts, each revealing one more mistake.
func (c Config) Validate() error { return c.validate(nil) }

// validateAdmin checks only what a one-shot administrative command touches: the
// database it connects to, and the identifiers that are interpolated into SQL
// rather than passed as parameters. Everything else belongs to a running
// dispatcher, which these commands are not.
func (c Config) validateAdmin() error {
	var errs []error

	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	c.validateDB(add)

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("invalid configuration:\n  - %s", joinErrors(errs, "\n  - "))
}

// validate accepts an error carried over from assembling the routing table, so
// that a driver problem and a threshold problem are reported together.
func (c Config) validate(prior error) error {
	var errs []error

	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if prior != nil {
		errs = append(errs, fmt.Errorf("broker configuration: %w", prior))
	}

	c.validateApp(add)
	c.validateLog(add)
	c.validateDB(add)
	c.validateDispatch(add)
	c.validateJanitor(add)
	c.validatePorts(add)
	c.validateOTel(add)
	c.validateBrokers(add, prior != nil)

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("invalid configuration:\n  - %s", joinErrors(errs, "\n  - "))
}

func (c Config) validateOTel(add func(string, ...any)) {
	if c.OTel.Sampling < 0 || c.OTel.Sampling > 1 {
		add("OUTBOX_OTEL_SAMPLING must be between 0 and 1, got %v", c.OTel.Sampling)
	}
	// A scheme here is the mistake this catches: the OTLP/HTTP exporter takes a
	// host and port, and silently appends its own path to anything else.
	if e := c.OTel.Endpoint; strings.Contains(e, "://") {
		add("OUTBOX_OTEL_ENDPOINT must be host:port without a scheme, got %q", e)
	}
}

func (c Config) validateApp(add func(string, ...any)) {
	if strings.TrimSpace(c.App.Name) == "" {
		add("OUTBOX_APP_NAME must not be empty")
	}
	if c.App.ShutdownTimeout <= 0 {
		add("OUTBOX_APP_SHUTDOWN_TIMEOUT must be positive, got %s", c.App.ShutdownTimeout)
	}
}

var logLevels = []string{"trace", "debug", "info", "warn", "error", "fatal", "panic"}

func (c Config) validateLog(add func(string, ...any)) {
	if !slices.Contains(logLevels, strings.ToLower(c.Log.Level)) {
		add("OUTBOX_LOG_LEVEL %q is not one of %s", c.Log.Level, strings.Join(logLevels, ", "))
	}
	if f := strings.ToLower(c.Log.Format); f != "console" && f != "text" && f != "json" {
		add("OUTBOX_LOG_FORMAT %q is not one of console, text, json", c.Log.Format)
	}
}

// identifier matches an unquoted PostgreSQL identifier. Schema and table names
// reach SQL through string interpolation — a placeholder cannot stand in for an
// identifier — so they are checked here rather than trusted.
var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (c Config) validateDB(add func(string, ...any)) {
	if c.DB.DSN == "" {
		if c.DB.User == "" {
			add("either OUTBOX_DB_DSN or OUTBOX_DB_USER must be set")
		}
		if c.DB.Name == "" {
			add("either OUTBOX_DB_DSN or OUTBOX_DB_NAME must be set")
		}
		if c.DB.Port < 1 || c.DB.Port > 65535 {
			add("OUTBOX_DB_PORT must be between 1 and 65535, got %d", c.DB.Port)
		}
	}

	if !identifier.MatchString(c.DB.Schema) {
		add("OUTBOX_DB_SCHEMA %q is not a valid lower-case unquoted identifier", c.DB.Schema)
	}
	if !identifier.MatchString(c.DB.Table) {
		add("OUTBOX_DB_TABLE %q is not a valid lower-case unquoted identifier", c.DB.Table)
	}

	if c.DB.MaxConns < 1 {
		add("OUTBOX_DB_MAX_CONNS must be at least 1, got %d", c.DB.MaxConns)
	}
	if c.DB.MinConns < 0 {
		add("OUTBOX_DB_MIN_CONNS must not be negative, got %d", c.DB.MinConns)
	}
	if c.DB.MinConns > c.DB.MaxConns {
		add("OUTBOX_DB_MIN_CONNS (%d) must not exceed OUTBOX_DB_MAX_CONNS (%d)", c.DB.MinConns, c.DB.MaxConns)
	}
	if c.DB.ConnectTimeout <= 0 {
		add("OUTBOX_DB_CONNECT_TIMEOUT must be positive, got %s", c.DB.ConnectTimeout)
	}
}

func (c Config) validateDispatch(add func(string, ...any)) {
	d := c.Dispatch

	if d.BatchSize < 1 {
		add("OUTBOX_DISPATCH_BATCH_SIZE must be at least 1, got %d", d.BatchSize)
	}
	if d.Workers < 1 {
		add("OUTBOX_DISPATCH_WORKERS must be at least 1, got %d", d.Workers)
	}
	// A max-attempts of zero would make the comparison in the failure path read
	// "attempts >= -1", which holds for every row: every message would fail
	// permanently on its first error.
	if d.MaxAttempts < 1 {
		add("OUTBOX_DISPATCH_MAX_ATTEMPTS must be at least 1, got %d", d.MaxAttempts)
	}
	// A negative window would make every first deferral immediately terminal,
	// which is the behaviour this option exists to prevent. Zero is the way to
	// turn it off.
	if d.MaxDefer < 0 {
		add("OUTBOX_DISPATCH_MAX_DEFER must not be negative, got %s", d.MaxDefer)
	}
	// A window shorter than one backoff step fails a message on its second
	// deferral whatever the outage looks like, which reads as "unbounded" to
	// whoever set it and behaves as "one retry".
	if d.MaxDefer > 0 && d.BackoffBase > 0 && d.MaxDefer < d.BackoffBase {
		add("OUTBOX_DISPATCH_MAX_DEFER (%s) must not be below OUTBOX_DISPATCH_BACKOFF_BASE (%s), "+
			"otherwise a deferred message fails before it is retried once", d.MaxDefer, d.BackoffBase)
	}
	if d.PauseMax < 0 {
		add("OUTBOX_DISPATCH_PAUSE_MAX must not be negative, got %s", d.PauseMax)
	}
	if d.PollInterval <= 0 {
		add("OUTBOX_DISPATCH_POLL_INTERVAL must be positive, got %s", d.PollInterval)
	}
	if d.LeaseTTL <= 0 {
		add("OUTBOX_DISPATCH_LEASE_TTL must be positive, got %s", d.LeaseTTL)
	}
	if d.PublishTimeout <= 0 {
		add("OUTBOX_DISPATCH_PUBLISH_TIMEOUT must be positive, got %s", d.PublishTimeout)
	}
	// A lease shorter than a single publish is guaranteed to expire in flight,
	// so every batch would be reclaimed and republished by another replica.
	if d.LeaseTTL > 0 && d.PublishTimeout > 0 && d.LeaseTTL <= d.PublishTimeout {
		add("OUTBOX_DISPATCH_LEASE_TTL (%s) must exceed OUTBOX_DISPATCH_PUBLISH_TIMEOUT (%s), "+
			"otherwise every claim expires mid-flight", d.LeaseTTL, d.PublishTimeout)
	}
	if d.BackoffBase <= 0 {
		add("OUTBOX_DISPATCH_BACKOFF_BASE must be positive, got %s", d.BackoffBase)
	}
	if d.BackoffMax > 0 && d.BackoffMax < d.BackoffBase {
		add("OUTBOX_DISPATCH_BACKOFF_MAX (%s) must not be below OUTBOX_DISPATCH_BACKOFF_BASE (%s)",
			d.BackoffMax, d.BackoffBase)
	}
	if d.BackoffJitter < 0 || d.BackoffJitter > 1 {
		add("OUTBOX_DISPATCH_BACKOFF_JITTER must be between 0 and 1, got %v", d.BackoffJitter)
	}
	if d.NotifyEnabled && strings.TrimSpace(d.NotifyChannel) == "" {
		add("OUTBOX_DISPATCH_NOTIFY_CHANNEL must not be empty when notifications are enabled")
	}
	if d.NotifyEnabled && !identifier.MatchString(d.NotifyChannel) {
		add("OUTBOX_DISPATCH_NOTIFY_CHANNEL %q is not a valid identifier", d.NotifyChannel)
	}
}

func (c Config) validateJanitor(add func(string, ...any)) {
	if !c.Janitor.Enabled {
		return
	}

	j := c.Janitor
	if j.ReclaimInterval <= 0 {
		add("OUTBOX_JANITOR_RECLAIM_INTERVAL must be positive, got %s", j.ReclaimInterval)
	}
	if j.StatsInterval <= 0 {
		add("OUTBOX_JANITOR_STATS_INTERVAL must be positive, got %s", j.StatsInterval)
	}
	if j.Retention > 0 {
		if j.RetentionInterval <= 0 {
			add("OUTBOX_JANITOR_RETENTION_INTERVAL must be positive, got %s", j.RetentionInterval)
		}
		if j.RetentionBatch < 1 {
			add("OUTBOX_JANITOR_RETENTION_BATCH must be at least 1, got %d", j.RetentionBatch)
		}
	}
}

func (c Config) validatePorts(add func(string, ...any)) {
	check := func(name string, port int) {
		if port < 1 || port > 65535 {
			add("%s must be between 1 and 65535, got %d", name, port)
		}
	}

	if c.HTTP.Enabled {
		check("OUTBOX_HTTP_PORT", c.HTTP.Port)
	}
	if c.Metrics.Enabled {
		check("OUTBOX_METRICS_PORT", c.Metrics.Port)
		if !strings.HasPrefix(c.Metrics.Path, "/") {
			add("OUTBOX_METRICS_PATH %q must start with /", c.Metrics.Path)
		}
	}
	if c.HTTP.Enabled && c.Metrics.Enabled && c.HTTP.Port == c.Metrics.Port {
		add("OUTBOX_HTTP_PORT and OUTBOX_METRICS_PORT must differ, both are %d", c.HTTP.Port)
	}
}

func (c Config) validateBrokers(add func(string, ...any), brokersFailed bool) {
	// When the routing table could not be assembled at all, its own error is
	// already in the report; repeating "no streams" here adds noise.
	if len(c.Brokers.Streams) == 0 && !brokersFailed {
		add("at least one stream must be configured through OUTBOX_STREAMS")
	}

	for name, stream := range c.Brokers.Streams {
		if _, ok := c.Brokers.Drivers[stream.Driver]; !ok {
			add("stream %q refers to driver %q, which is not configured", name, stream.Driver)
		}
	}

	if !c.DLQ.Enabled {
		return
	}

	if c.DLQ.Stream == "" {
		add("OUTBOX_DLQ_STREAM must be set when the dead-letter queue is enabled")
	} else if _, ok := c.Brokers.Streams[strings.ToLower(c.DLQ.Stream)]; !ok {
		add("OUTBOX_DLQ_STREAM %q is not among the configured streams", c.DLQ.Stream)
	}
	if strings.TrimSpace(c.DLQ.Topic) == "" {
		add("OUTBOX_DLQ_TOPIC must not be empty when the dead-letter queue is enabled")
	}
}

func joinErrors(errs []error, sep string) string {
	parts := make([]string, len(errs))
	for i, err := range errs {
		parts[i] = err.Error()
	}

	return strings.Join(parts, sep)
}
