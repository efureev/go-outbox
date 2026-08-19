// Package config loads and validates the whole configuration before anything
// else starts.
//
// Two properties matter more than the field list. First, every duration is a
// time.Duration parsed at load time rather than a string parsed later inside a
// running loop: a typo in an interval must stop the process at boot, not
// produce a service that comes up and is permanently unready. Second,
// validation reports every problem at once — a container that restarts to
// reveal the next misconfigured variable wastes an operator's afternoon.
package config

import (
	"time"
)

// Config is the complete configuration. Every key lives under the OUTBOX_
// namespace; nested struct tags supply the section.
type Config struct {
	App      AppConfig      `env:"APP"`
	Log      LogConfig      `env:"LOG"`
	DB       DBConfig       `env:"DB"`
	Dispatch DispatchConfig `env:"DISPATCH"`
	Janitor  JanitorConfig  `env:"JANITOR"`
	HTTP     HTTPConfig     `env:"HTTP"`
	Metrics  MetricsConfig  `env:"METRICS"`
	DLQ      DLQConfig      `env:"DLQ"`

	// Brokers is assembled separately: stream and driver names are not known
	// until the environment is read, so they cannot be struct fields.
	Brokers BrokerConfig `env:"-"`
}

type AppConfig struct {
	Name string `env:"NAME,default=outbox"`
	Env  string `env:"ENV,default=prod"`
	// Instance identifies this process in the owner column of a claimed row,
	// so an operator can tell which replica holds a lease. It defaults to the
	// hostname, which is the pod name under Kubernetes.
	Instance string `env:"INSTANCE"`
	// ShutdownTimeout bounds the whole teardown sequence.
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT,default=30s"`
}

func (c AppConfig) IsProduction() bool { return c.Env == "prod" || c.Env == "production" }

type LogConfig struct {
	Level string `env:"LEVEL,default=info"`
	// Format is console, text or json. Console is for a terminal; json is what
	// a log collector wants.
	Format string `env:"FORMAT,default=json"`
	Caller bool   `env:"CALLER,default=false"`
}

type DBConfig struct {
	// DSN is the primary way to configure the connection. When empty the
	// components below are assembled into one — through pgx's own parser, so a
	// password containing a space or a quote survives, which string
	// concatenation did not.
	DSN      string `env:"DSN"`
	Host     string `env:"HOST,default=localhost"`
	Port     int    `env:"PORT,default=5432"`
	User     string `env:"USER"`
	Password string `env:"PASSWORD"`
	Name     string `env:"NAME"`
	SSLMode  string `env:"SSL_MODE,default=disable"`

	// Schema and Table make the outbox table addressable, so the dispatcher can
	// live alongside whatever naming the producer's database already uses.
	Schema string `env:"SCHEMA,default=outbox"`
	Table  string `env:"TABLE,default=messages"`

	MaxConns         int32         `env:"MAX_CONNS,default=10"`
	MinConns         int32         `env:"MIN_CONNS,default=2"`
	ConnectTimeout   time.Duration `env:"CONNECT_TIMEOUT,default=5s"`
	StatementTimeout time.Duration `env:"STATEMENT_TIMEOUT,default=30s"`
	MaxConnLifetime  time.Duration `env:"MAX_CONN_LIFETIME,default=1h"`
	MaxConnIdleTime  time.Duration `env:"MAX_CONN_IDLE_TIME,default=30m"`
	AutoMigrate      bool          `env:"AUTO_MIGRATE,default=false"`
	MigrationLockKey int64         `env:"MIGRATION_LOCK_KEY,default=8090211501"`
}

type DispatchConfig struct {
	PollInterval time.Duration `env:"POLL_INTERVAL,default=5s"`
	BatchSize    int           `env:"BATCH_SIZE,default=200"`
	// Workers is the publish concurrency per stream. Each stream gets its own
	// pipeline, so a broker that is down delays only its own messages.
	Workers int `env:"WORKERS,default=8"`
	// LeaseTTL is how long a claim stays valid. It must exceed the time a
	// batch normally takes to publish; a lease that expires mid-flight shows
	// up as outbox_lease_conflicts_total.
	LeaseTTL time.Duration `env:"LEASE_TTL,default=2m"`
	// MaxAttempts is how many times a broker may reject a message before it is
	// given up on. It counts rejections, not minutes: a broker that could not
	// be reached at all never saw the message, and that failure defers the
	// message instead of spending an attempt on it.
	MaxAttempts int `env:"MAX_ATTEMPTS,default=5"`
	// MaxDefer bounds how long an unreachable broker may hold a message back
	// before it fails anyway, measured from the first deferral rather than from
	// the row's creation.
	//
	// Zero — the default — means unbounded: the message waits out the outage,
	// and outbox_oldest_pending_age_seconds is what raises the alarm. Set it
	// only when a stream has a deadline of its own and a message delivered late
	// is worth less than one that visibly failed.
	MaxDefer      time.Duration `env:"MAX_DEFER,default=0s"`
	BackoffBase   time.Duration `env:"BACKOFF_BASE,default=1m"`
	BackoffMax    time.Duration `env:"BACKOFF_MAX,default=1h"`
	BackoffJitter float64       `env:"BACKOFF_JITTER,default=0.2"`
	// PauseMax is the longest the dispatcher waits before trying a stream whose
	// broker is unreachable again.
	//
	// Finding a broker gone stops the pipeline claiming for that stream: the
	// pause starts at one poll interval and doubles up to this ceiling, and one
	// ordinary claim is let through each time it elapses. Retries are already
	// self-limiting — a deferred message is rescheduled a backoff into the
	// future — but new messages are not, and every insert that arrives during an
	// outage would otherwise be claimed, attempted and written back at once.
	//
	// The default matches the ceiling the RabbitMQ supervisor backs off to
	// between reconnection attempts. Trying more often than the driver itself
	// retries only produces trials that find the connection still being rebuilt,
	// so a shorter pause buys no recovery speed — the driver's own backoff, not
	// this, is what bounds how soon a returning broker is noticed.
	//
	// Zero disables it: the loop keeps claiming throughout an outage, which is
	// the behaviour before this existed.
	PauseMax time.Duration `env:"PAUSE_MAX,default=30s"`
	// PublishTimeout bounds a single publish call.
	PublishTimeout time.Duration `env:"PUBLISH_TIMEOUT,default=15s"`
	// WriteBackTimeout bounds the database call that records the outcome. It
	// is deliberately generous: losing this write means republishing the batch.
	WriteBackTimeout time.Duration `env:"WRITE_BACK_TIMEOUT,default=30s"`

	// NotifyEnabled turns on LISTEN/NOTIFY wakeups. With it off the dispatcher
	// polls only, which is what a deployment without trigger privileges needs.
	NotifyEnabled  bool          `env:"NOTIFY_ENABLED,default=true"`
	NotifyChannel  string        `env:"NOTIFY_CHANNEL,default=outbox_new"`
	NotifyDebounce time.Duration `env:"NOTIFY_DEBOUNCE,default=50ms"`
	// NotifyJitter spreads the wakeup across replicas: without it every
	// instance claims at the same millisecond after each insert.
	NotifyJitter time.Duration `env:"NOTIFY_JITTER,default=100ms"`
}

type JanitorConfig struct {
	Enabled bool `env:"ENABLED,default=true"`
	// ReclaimInterval is how often expired leases are returned to pending.
	ReclaimInterval time.Duration `env:"RECLAIM_INTERVAL,default=30s"`
	// StatsInterval drives the gauge refresh. It is deliberately slower than the
	// poll loop: counting rows on every iteration is a query every few seconds
	// against a table that only grows.
	StatsInterval time.Duration `env:"STATS_INTERVAL,default=30s"`

	// Retention is how long a delivered row is kept. Zero disables purging.
	Retention         time.Duration `env:"RETENTION,default=168h"`
	RetentionInterval time.Duration `env:"RETENTION_INTERVAL,default=5m"`
	// RetentionBatch bounds one DELETE so the transaction stays short and the
	// table does not lock up behind a multi-million-row delete.
	RetentionBatch int `env:"RETENTION_BATCH,default=5000"`
	// LockKey namespaces the advisory lock that keeps singleton work — reclaim,
	// stats, retention — on exactly one replica per cycle.
	//
	// It is 32 bits because that is what the two-argument form of
	// pg_try_advisory_lock takes: the second argument distinguishes the three
	// tasks, so they do not exclude one another.
	LockKey int32 `env:"LOCK_KEY,default=809021150"`
}

type HTTPConfig struct {
	Enabled         bool          `env:"ENABLED,default=true"`
	Port            int           `env:"PORT,default=8085"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT,default=10s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT,default=30s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT,default=10s"`
	// AdminToken guards the mutating endpoints. Without one they are not
	// registered at all — an admin API reachable by anyone who can route to
	// the pod is worse than no admin API.
	AdminToken string `env:"ADMIN_TOKEN"`
}

type MetricsConfig struct {
	Enabled bool   `env:"ENABLED,default=true"`
	Port    int    `env:"PORT,default=9100"`
	Path    string `env:"PATH,default=/metrics"`
}

// DLQConfig routes messages that reached StatusFailed to a dead-letter
// destination. The row stays in the table either way — the DLQ is a signal to
// whoever consumes it, not a substitute for the record.
type DLQConfig struct {
	Enabled bool   `env:"ENABLED,default=false"`
	Stream  string `env:"STREAM"`
	Topic   string `env:"TOPIC,default=outbox.dead-letter"`
}
