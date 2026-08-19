package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every numeric rule in validate.go states a boundary in its own error message:
// "must be at least 1", "must be positive", "between 1 and 65535". A test that
// only feeds each rule something obviously wrong proves the rule fires, not that
// it fires in the right place — and a rule that rejects its own documented
// minimum is the more expensive mistake, because it refuses a configuration the
// documentation told the operator to write.
//
// So each rule is exercised twice: once just outside the boundary, where it must
// complain and name the variable, and once exactly on it, where the whole
// configuration must load.
//
// This table came out of mutation testing. Forty of the surviving mutants in
// this package were a comparison shifted by one, all of them alive because
// nothing ever presented the boundary value itself.
//
// Seven survive `make mutation` now, and none of them should be chased:
//
//   - three are the `> 0` halves of a compound rule (MAX_DEFER against
//     BACKOFF_BASE, LEASE_TTL against PUBLISH_TIMEOUT). They exist only to
//     suppress a second complaint about a value another rule has already
//     rejected, so the mutant adds a redundant message to a configuration that
//     fails either way. Killing it means asserting an exact list of messages,
//     which is fitting the test to the tool;
//   - two are os.Hostname(), unkillable without replacing the syscall;
//   - one is `len(other) <= len(self)` in the driver-prefix search: two distinct
//     names of equal length cannot be a prefix of one another, so both branches
//     reach the same answer;
//   - one is inside LoadAdmin's own error path, which needs an unreadable file
//     the test already covers from the other direction.
//
// Gremlins also reports four false negatives here: three are arithmetic inside
// const declarations, which lie in no coverage block, and one is a condition in
// a `case` clause whose block starts after it.

type boundaryRule struct {
	name string
	// rejected is what must fail, and fragment is what the message must contain
	// so the operator knows which variable to fix.
	rejected []string
	fragment string
	// accepted is the same setting at exactly the boundary the message names,
	// plus whatever companion settings keep the rest of the configuration valid.
	accepted []string
}

var boundaryRules = []boundaryRule{
	{
		name:     "app shutdown timeout",
		rejected: []string{"OUTBOX_APP_SHUTDOWN_TIMEOUT=0s"},
		fragment: "OUTBOX_APP_SHUTDOWN_TIMEOUT",
		accepted: []string{"OUTBOX_APP_SHUTDOWN_TIMEOUT=1ns"},
	},
	{
		name:     "otel sampling, below the range",
		rejected: []string{"OUTBOX_OTEL_SAMPLING=-0.000001"},
		fragment: "OUTBOX_OTEL_SAMPLING",
		accepted: []string{"OUTBOX_OTEL_SAMPLING=0"},
	},
	{
		name:     "otel sampling, above the range",
		rejected: []string{"OUTBOX_OTEL_SAMPLING=1.000001"},
		fragment: "OUTBOX_OTEL_SAMPLING",
		accepted: []string{"OUTBOX_OTEL_SAMPLING=1"},
	},
	{
		name:     "partitions ahead",
		rejected: []string{"OUTBOX_JANITOR_PARTITION_AHEAD=-1"},
		fragment: "OUTBOX_JANITOR_PARTITION_AHEAD",
		accepted: []string{"OUTBOX_JANITOR_PARTITION_AHEAD=0"},
	},

	// Database.
	{
		name:     "database port, below the range",
		rejected: []string{"OUTBOX_DB_PORT=0"},
		fragment: "OUTBOX_DB_PORT",
		accepted: []string{"OUTBOX_DB_PORT=1"},
	},
	{
		name:     "database port, above the range",
		rejected: []string{"OUTBOX_DB_PORT=65536"},
		fragment: "OUTBOX_DB_PORT",
		accepted: []string{"OUTBOX_DB_PORT=65535"},
	},
	{
		name:     "max connections",
		rejected: []string{"OUTBOX_DB_MAX_CONNS=0", "OUTBOX_DB_MIN_CONNS=0"},
		fragment: "OUTBOX_DB_MAX_CONNS",
		accepted: []string{"OUTBOX_DB_MAX_CONNS=1", "OUTBOX_DB_MIN_CONNS=0"},
	},
	{
		name:     "min connections",
		rejected: []string{"OUTBOX_DB_MIN_CONNS=-1"},
		fragment: "OUTBOX_DB_MIN_CONNS",
		accepted: []string{"OUTBOX_DB_MIN_CONNS=0"},
	},
	{
		// The pool may be exactly as small as it is large; one more and it can
		// never open.
		name:     "min connections against max",
		rejected: []string{"OUTBOX_DB_MIN_CONNS=5", "OUTBOX_DB_MAX_CONNS=4"},
		fragment: "must not exceed",
		accepted: []string{"OUTBOX_DB_MIN_CONNS=4", "OUTBOX_DB_MAX_CONNS=4"},
	},
	{
		name:     "connect timeout",
		rejected: []string{"OUTBOX_DB_CONNECT_TIMEOUT=0s"},
		fragment: "OUTBOX_DB_CONNECT_TIMEOUT",
		accepted: []string{"OUTBOX_DB_CONNECT_TIMEOUT=1ns"},
	},

	// Dispatch.
	{
		name:     "batch size",
		rejected: []string{"OUTBOX_DISPATCH_BATCH_SIZE=0"},
		fragment: "OUTBOX_DISPATCH_BATCH_SIZE",
		accepted: []string{"OUTBOX_DISPATCH_BATCH_SIZE=1"},
	},
	{
		name:     "workers",
		rejected: []string{"OUTBOX_DISPATCH_WORKERS=0"},
		fragment: "OUTBOX_DISPATCH_WORKERS",
		accepted: []string{"OUTBOX_DISPATCH_WORKERS=1"},
	},
	{
		// One attempt is a legitimate choice — deliver once, then let a human
		// look at it — and the comparison in the failure path only breaks below
		// that.
		name:     "max attempts",
		rejected: []string{"OUTBOX_DISPATCH_MAX_ATTEMPTS=0"},
		fragment: "OUTBOX_DISPATCH_MAX_ATTEMPTS",
		accepted: []string{"OUTBOX_DISPATCH_MAX_ATTEMPTS=1"},
	},
	{
		// Zero is how the window is turned off, so it has to be accepted.
		name:     "max defer",
		rejected: []string{"OUTBOX_DISPATCH_MAX_DEFER=-1ns"},
		fragment: "OUTBOX_DISPATCH_MAX_DEFER",
		accepted: []string{"OUTBOX_DISPATCH_MAX_DEFER=0s"},
	},
	{
		// A window equal to one backoff step is the shortest that still lets a
		// deferred message be retried once.
		name:     "max defer against the backoff base",
		rejected: []string{"OUTBOX_DISPATCH_MAX_DEFER=59s", "OUTBOX_DISPATCH_BACKOFF_BASE=1m"},
		fragment: "OUTBOX_DISPATCH_MAX_DEFER",
		accepted: []string{"OUTBOX_DISPATCH_MAX_DEFER=1m", "OUTBOX_DISPATCH_BACKOFF_BASE=1m"},
	},
	{
		name:     "pause max",
		rejected: []string{"OUTBOX_DISPATCH_PAUSE_MAX=-1ns"},
		fragment: "OUTBOX_DISPATCH_PAUSE_MAX",
		accepted: []string{"OUTBOX_DISPATCH_PAUSE_MAX=0s"},
	},
	{
		name:     "poll interval",
		rejected: []string{"OUTBOX_DISPATCH_POLL_INTERVAL=0s"},
		fragment: "OUTBOX_DISPATCH_POLL_INTERVAL",
		accepted: []string{"OUTBOX_DISPATCH_POLL_INTERVAL=1ns"},
	},
	{
		name:     "lease ttl",
		rejected: []string{"OUTBOX_DISPATCH_LEASE_TTL=0s"},
		fragment: "OUTBOX_DISPATCH_LEASE_TTL",
		accepted: []string{"OUTBOX_DISPATCH_LEASE_TTL=2ns", "OUTBOX_DISPATCH_PUBLISH_TIMEOUT=1ns"},
	},
	{
		name:     "publish timeout",
		rejected: []string{"OUTBOX_DISPATCH_PUBLISH_TIMEOUT=0s"},
		fragment: "OUTBOX_DISPATCH_PUBLISH_TIMEOUT",
		accepted: []string{"OUTBOX_DISPATCH_PUBLISH_TIMEOUT=1ns"},
	},
	{
		// Equal is not enough: a lease exactly as long as one publish expires
		// the instant it finishes. One nanosecond more is.
		name:     "lease against the publish timeout",
		rejected: []string{"OUTBOX_DISPATCH_LEASE_TTL=15s", "OUTBOX_DISPATCH_PUBLISH_TIMEOUT=15s"},
		fragment: "must exceed",
		accepted: []string{"OUTBOX_DISPATCH_LEASE_TTL=15000000001ns", "OUTBOX_DISPATCH_PUBLISH_TIMEOUT=15s"},
	},
	{
		name:     "backoff base",
		rejected: []string{"OUTBOX_DISPATCH_BACKOFF_BASE=0s"},
		fragment: "OUTBOX_DISPATCH_BACKOFF_BASE",
		accepted: []string{"OUTBOX_DISPATCH_BACKOFF_BASE=1ns"},
	},
	{
		// A ceiling equal to the base means "do not grow", which is a choice.
		name:     "backoff max against the base",
		rejected: []string{"OUTBOX_DISPATCH_BACKOFF_MAX=59s", "OUTBOX_DISPATCH_BACKOFF_BASE=1m"},
		fragment: "OUTBOX_DISPATCH_BACKOFF_MAX",
		accepted: []string{"OUTBOX_DISPATCH_BACKOFF_MAX=1m", "OUTBOX_DISPATCH_BACKOFF_BASE=1m"},
	},
	{
		name:     "backoff jitter, below the range",
		rejected: []string{"OUTBOX_DISPATCH_BACKOFF_JITTER=-0.000001"},
		fragment: "OUTBOX_DISPATCH_BACKOFF_JITTER",
		accepted: []string{"OUTBOX_DISPATCH_BACKOFF_JITTER=0"},
	},
	{
		name:     "backoff jitter, above the range",
		rejected: []string{"OUTBOX_DISPATCH_BACKOFF_JITTER=1.000001"},
		fragment: "OUTBOX_DISPATCH_BACKOFF_JITTER",
		accepted: []string{"OUTBOX_DISPATCH_BACKOFF_JITTER=1"},
	},

	// Janitor.
	{
		name:     "reclaim interval",
		rejected: []string{"OUTBOX_JANITOR_RECLAIM_INTERVAL=0s"},
		fragment: "OUTBOX_JANITOR_RECLAIM_INTERVAL",
		accepted: []string{"OUTBOX_JANITOR_RECLAIM_INTERVAL=1ns"},
	},
	{
		name:     "stats interval",
		rejected: []string{"OUTBOX_JANITOR_STATS_INTERVAL=0s"},
		fragment: "OUTBOX_JANITOR_STATS_INTERVAL",
		accepted: []string{"OUTBOX_JANITOR_STATS_INTERVAL=1ns"},
	},
	{
		name:     "retention interval",
		rejected: []string{"OUTBOX_JANITOR_RETENTION_INTERVAL=0s"},
		fragment: "OUTBOX_JANITOR_RETENTION_INTERVAL",
		accepted: []string{"OUTBOX_JANITOR_RETENTION_INTERVAL=1ns"},
	},
	{
		name:     "retention batch",
		rejected: []string{"OUTBOX_JANITOR_RETENTION_BATCH=0"},
		fragment: "OUTBOX_JANITOR_RETENTION_BATCH",
		accepted: []string{"OUTBOX_JANITOR_RETENTION_BATCH=1"},
	},

	// Ports.
	{
		name:     "http port, below the range",
		rejected: []string{"OUTBOX_HTTP_PORT=0"},
		fragment: "OUTBOX_HTTP_PORT",
		accepted: []string{"OUTBOX_HTTP_PORT=1"},
	},
	{
		name:     "http port, above the range",
		rejected: []string{"OUTBOX_HTTP_PORT=65536"},
		fragment: "OUTBOX_HTTP_PORT",
		accepted: []string{"OUTBOX_HTTP_PORT=65535"},
	},
}

func TestEveryRuleFiresExactlyAtItsBoundary(t *testing.T) {
	for _, rule := range boundaryRules {
		t.Run(rule.name+"/just outside", func(t *testing.T) {
			_, err := LoadFrom(env(t, minimal(rule.rejected...)...))
			if err == nil {
				t.Fatalf("%v was accepted", rule.rejected)
			}
			if !strings.Contains(err.Error(), rule.fragment) {
				t.Errorf("the message does not name %q, so nobody knows what to fix:\n%v",
					rule.fragment, err)
			}
		})

		t.Run(rule.name+"/exactly on it", func(t *testing.T) {
			if _, err := LoadFrom(env(t, minimal(rule.accepted...)...)); err != nil {
				t.Errorf("%v is the documented minimum and was refused:\n%v", rule.accepted, err)
			}
		})
	}
}

// Retention off switches off the two rules that only apply underneath it. A
// deployment that keeps everything forever should not have to set a retention
// interval it will never use.
func TestRetentionOffSuspendsItsOwnRules(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_JANITOR_RETENTION=0s",
		"OUTBOX_JANITOR_RETENTION_INTERVAL=0s",
		"OUTBOX_JANITOR_RETENTION_BATCH=0",
	)...))
	if err != nil {
		t.Errorf("retention off still demanded its own settings:\n%v", err)
	}
}

// The janitor switched off suspends all of its rules, for the same reason.
func TestADisabledJanitorIsNotValidated(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_JANITOR_ENABLED=false",
		"OUTBOX_JANITOR_RECLAIM_INTERVAL=0s",
		"OUTBOX_JANITOR_STATS_INTERVAL=0s",
		"OUTBOX_JANITOR_RETENTION_INTERVAL=0s",
		"OUTBOX_JANITOR_RETENTION_BATCH=0",
	)...))
	if err != nil {
		t.Errorf("a disabled janitor was still validated:\n%v", err)
	}
}

// A port nobody is going to bind is not a configuration error.
func TestADisabledListenerHasNoPortToValidate(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_HTTP_ENABLED=false",
		"OUTBOX_HTTP_PORT=0",
	)...))
	if err != nil {
		t.Errorf("a disabled listener was refused over its port:\n%v", err)
	}
}

// A DSN supplies host, port, user and database at once, so the fields it
// replaces stop being required — and stop being checked.
func TestADSNSuspendsTheFieldsItReplaces(t *testing.T) {
	_, err := LoadFrom(env(t,
		"OUTBOX_DB_DSN=postgres://outbox:secret@db:5432/app?sslmode=disable",
		"OUTBOX_DB_PORT=0",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://guest:guest@localhost:5672/",
	))
	if err != nil {
		t.Errorf("a DSN did not suspend the fields it replaces:\n%v", err)
	}
}

// Notifications off suspend the channel rules; on, the channel has to be an
// identifier, because it reaches SQL through interpolation like the schema does.
func TestTheNotifyChannelIsOnlyCheckedWhenItIsUsed(t *testing.T) {
	if _, err := LoadFrom(env(t, minimal(
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=false",
		"OUTBOX_DISPATCH_NOTIFY_CHANNEL=",
	)...)); err != nil {
		t.Errorf("a channel nobody listens on was validated:\n%v", err)
	}

	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=true",
		"OUTBOX_DISPATCH_NOTIFY_CHANNEL=Bad Channel; DROP TABLE",
	)...))
	if err == nil {
		t.Fatal("a channel that is not an identifier was accepted")
	}
	if !strings.Contains(err.Error(), "OUTBOX_DISPATCH_NOTIFY_CHANNEL") {
		t.Errorf("the message does not name the variable:\n%v", err)
	}
}

// The driver settings have boundaries of their own, and they are checked at load
// time rather than in Validate — a driver that never opens is a startup failure
// either way, but the message should name the driver and the key.
var driverBoundaryRules = []boundaryRule{
	{
		name:     "rabbitmq channels, below the range",
		rejected: []string{"OUTBOX_DRIVER_RMQ_CHANNELS=0"},
		fragment: "CHANNELS",
		accepted: []string{"OUTBOX_DRIVER_RMQ_CHANNELS=1"},
	},
	{
		name:     "rabbitmq channels, above the range",
		rejected: []string{"OUTBOX_DRIVER_RMQ_CHANNELS=129"},
		fragment: "CHANNELS",
		accepted: []string{"OUTBOX_DRIVER_RMQ_CHANNELS=128"},
	},
}

func TestEveryDriverRuleFiresExactlyAtItsBoundary(t *testing.T) {
	for _, rule := range driverBoundaryRules {
		t.Run(rule.name+"/just outside", func(t *testing.T) {
			_, err := LoadFrom(env(t, minimal(rule.rejected...)...))
			if err == nil {
				t.Fatalf("%v was accepted", rule.rejected)
			}
			if !strings.Contains(err.Error(), rule.fragment) {
				t.Errorf("the message does not name %q:\n%v", rule.fragment, err)
			}
		})

		t.Run(rule.name+"/exactly on it", func(t *testing.T) {
			if _, err := LoadFrom(env(t, minimal(rule.accepted...)...)); err != nil {
				t.Errorf("%v is the documented limit and was refused:\n%v", rule.accepted, err)
			}
		})
	}
}

// The postgres driver's own limits, on a configuration that reaches its builder.
func TestThePostgresDriverBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		setting  string
		rejected string
		accepted []string
	}{
		{"write timeout", "OUTBOX_DRIVER_INB_WRITE_TIMEOUT", "0s", []string{"1ns"}},
		{"max connections", "OUTBOX_DRIVER_INB_MAX_CONNS", "0", []string{"1", "64"}},
		{"max connections, above the range", "OUTBOX_DRIVER_INB_MAX_CONNS", "65", []string{"64"}},
	}

	for _, c := range cases {
		t.Run(c.name+"/just outside", func(t *testing.T) {
			_, err := LoadFrom(env(t, postgresEnv(c.setting+"="+c.rejected)...))
			if err == nil {
				t.Fatalf("%s=%s was accepted", c.setting, c.rejected)
			}
			if !strings.Contains(err.Error(), strings.TrimPrefix(c.setting, "OUTBOX_DRIVER_INB_")) {
				t.Errorf("the message does not name the key:\n%v", err)
			}
		})

		for _, ok := range c.accepted {
			t.Run(c.name+"/at "+ok, func(t *testing.T) {
				if _, err := LoadFrom(env(t, postgresEnv(c.setting+"="+ok)...)); err != nil {
					t.Errorf("%s=%s is within the documented range and was refused:\n%v",
						c.setting, ok, err)
				}
			})
		}
	}
}

// Zero is how the backoff ceiling is switched off — the delay then doubles
// without bound, which is a choice a deployment is allowed to make. A rule that
// read it as "a ceiling below the base" would refuse the one configuration the
// option exists to express.
func TestABackoffWithoutACeilingIsAllowed(t *testing.T) {
	if _, err := LoadFrom(env(t, minimal(
		"OUTBOX_DISPATCH_BACKOFF_MAX=0s",
		"OUTBOX_DISPATCH_BACKOFF_BASE=1m",
	)...)); err != nil {
		t.Errorf("an unbounded backoff was refused:\n%v", err)
	}
}

// All three formats are documented, so all three have to load. A rule listing
// them is exactly the kind that drifts from the documentation without anybody
// noticing.
func TestEveryDocumentedLogFormatLoads(t *testing.T) {
	for _, format := range []string{"console", "text", "json", "JSON", "Console"} {
		if _, err := LoadFrom(env(t, minimal("OUTBOX_LOG_FORMAT="+format)...)); err != nil {
			t.Errorf("format %q is documented and was refused:\n%v", format, err)
		}
	}

	_, err := LoadFrom(env(t, minimal("OUTBOX_LOG_FORMAT=xml")...))
	if err == nil {
		t.Fatal("an unknown log format was accepted")
	}
	if !strings.Contains(err.Error(), "console, text, json") {
		t.Errorf("the message does not list what is allowed:\n%v", err)
	}
}

func TestEveryDocumentedLogLevelLoads(t *testing.T) {
	for _, level := range []string{"trace", "debug", "info", "warn", "error", "fatal", "panic", "INFO"} {
		if _, err := LoadFrom(env(t, minimal("OUTBOX_LOG_LEVEL="+level)...)); err != nil {
			t.Errorf("level %q is documented and was refused:\n%v", level, err)
		}
	}

	if _, err := LoadFrom(env(t, minimal("OUTBOX_LOG_LEVEL=verbose")...)); err == nil {
		t.Error("an unknown log level was accepted")
	}
}

// A dead-letter queue with no topic to publish to would fail on the first
// forward rather than at startup, which is the wrong end of the deployment to
// find out.
func TestADeadLetterQueueNeedsSomewhereToPublish(t *testing.T) {
	dlq := func(extra ...string) []string {
		return minimal(append([]string{
			"OUTBOX_STREAMS=local,dlq",
			"OUTBOX_STREAM_DLQ_DRIVER=rmq",
			"OUTBOX_DLQ_ENABLED=true",
			"OUTBOX_DLQ_STREAM=dlq",
		}, extra...)...)
	}

	if _, err := LoadFrom(env(t, dlq("OUTBOX_DLQ_TOPIC=outbox.dead-letter")...)); err != nil {
		t.Fatalf("a complete dead-letter configuration was refused:\n%v", err)
	}

	// An unset variable falls back to the default topic, so the only way to
	// reach the rule is whitespace — which looks set to whoever wrote it and is
	// not a topic any broker will take.
	if _, err := LoadFrom(env(t, dlq("OUTBOX_DLQ_TOPIC=")...)); err != nil {
		t.Errorf("an unset topic did not fall back to the default:\n%v", err)
	}

	_, err := LoadFrom(env(t, dlq("OUTBOX_DLQ_TOPIC=   ")...))
	if err == nil {
		t.Fatal("a dead-letter queue with a whitespace topic was accepted")
	}
	if !strings.Contains(err.Error(), "OUTBOX_DLQ_TOPIC") {
		t.Errorf("the message does not name the variable:\n%v", err)
	}
}

// The DSN is assembled from the parts when none was given, and the sslmode is
// only appended when there is one to append — an empty value in the query string
// is not the same as its absence to libpq.
func TestConnStringOmitsAnEmptySSLMode(t *testing.T) {
	with := DBConfig{Host: "db", Port: 5432, User: "u", Password: "p", Name: "app", SSLMode: "require"}
	if got := with.ConnString(); !strings.Contains(got, "sslmode=require") {
		t.Errorf("ConnString = %q, want it to carry the sslmode", got)
	}

	without := with
	without.SSLMode = ""
	got := without.ConnString()

	if strings.Contains(got, "sslmode") {
		t.Errorf("ConnString = %q, want no sslmode at all rather than an empty one", got)
	}
	if !strings.Contains(got, "db:5432") || !strings.Contains(got, "/app") {
		t.Errorf("ConnString = %q, want the host and database intact", got)
	}
}

func kafkaEnv(extra ...string) []string {
	return append([]string{
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=global",
		"OUTBOX_STREAM_GLOBAL_DRIVER=kafka",
		"OUTBOX_DRIVER_KAFKA_TYPE=kafka",
		"OUTBOX_DRIVER_KAFKA_BROKERS=kafka-1:9092,kafka-2:9092",
	}, extra...)
}

// Half a credential is the realistic misconfiguration — a username in the
// manifest and a password expected from a secret that did not mount. Either
// half missing has to be refused, or the driver connects anonymously to a
// cluster that was meant to be authenticated.
func TestKafkaRefusesHalfACredential(t *testing.T) {
	sasl := func(user, pass string) []string {
		return kafkaEnv(
			"OUTBOX_DRIVER_KAFKA_SECURITY_PROTOCOL=SASL_SSL",
			"OUTBOX_DRIVER_KAFKA_SASL_MECHANISM=SCRAM-SHA-512",
			"OUTBOX_DRIVER_KAFKA_SASL_USERNAME="+user,
			"OUTBOX_DRIVER_KAFKA_SASL_PASSWORD="+pass,
		)
	}

	cases := map[string][2]string{
		"neither":          {"", ""},
		"a username alone": {"svc", ""},
		"a password alone": {"", "hunter2"},
	}

	for name, pair := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadFrom(env(t, sasl(pair[0], pair[1])...))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), "SASL_USERNAME") ||
				!strings.Contains(err.Error(), "SASL_PASSWORD") {
				t.Errorf("the message does not name both halves:\n%v", err)
			}
		})
	}

	if _, err := LoadFrom(env(t, sasl("svc", "hunter2")...)); err != nil {
		t.Errorf("a complete credential was refused:\n%v", err)
	}
}

// One attempt is a legitimate setting for a driver whose failures the dispatcher
// is going to retry anyway.
func TestKafkaMaxAttemptsBoundary(t *testing.T) {
	if _, err := LoadFrom(env(t, kafkaEnv("OUTBOX_DRIVER_KAFKA_MAX_ATTEMPTS=1")...)); err != nil {
		t.Errorf("one attempt is the documented minimum and was refused:\n%v", err)
	}

	_, err := LoadFrom(env(t, kafkaEnv("OUTBOX_DRIVER_KAFKA_MAX_ATTEMPTS=0")...))
	if err == nil {
		t.Fatal("zero attempts was accepted")
	}
	if !strings.Contains(err.Error(), "MAX_ATTEMPTS") {
		t.Errorf("the message does not name the key:\n%v", err)
	}
}

// Validate is exported and takes a Config somebody assembled rather than loaded.
// That is the only way to reach the empty-routing-table rule: OUTBOX_STREAMS
// defaults to one stream, and a stream whose driver is missing fails while the
// table is being built, so loading either produces streams or produces its own
// error. Through the struct, neither happens.
func TestValidateOnAHandBuiltConfigStillWantsAStream(t *testing.T) {
	var cfg Config

	err := cfg.Validate()
	if err == nil {
		t.Fatal("an empty configuration was accepted")
	}
	if !strings.Contains(err.Error(), "OUTBOX_STREAMS") {
		t.Errorf("nothing said a stream is needed:\n%v", err)
	}

	// And it reports the rest at once rather than stopping at the first, which
	// is the whole point of collecting them.
	for _, want := range []string{"OUTBOX_APP_NAME", "OUTBOX_DB_USER", "OUTBOX_DISPATCH_BATCH_SIZE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s is not among the problems reported:\n%v", want, err)
		}
	}
}

func TestIsProductionAcceptsBothSpellings(t *testing.T) {
	for _, env := range []string{"prod", "production"} {
		if !(AppConfig{Env: env}).IsProduction() {
			t.Errorf("Env=%q is not production", env)
		}
	}
	for _, env := range []string{"", "dev", "staging", "PROD", "prod "} {
		if (AppConfig{Env: env}).IsProduction() {
			t.Errorf("Env=%q is treated as production", env)
		}
	}
}

// A CA certificate arrives base64-encoded in an environment variable, which is
// two encodings deep and two ways to be wrong. Both have to be caught here: a
// PEM that only fails when the TLS handshake does is a startup that looks
// healthy and delivers nothing.
func TestDecodePEM(t *testing.T) {
	const cert = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`

	cases := []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "absent is not an error", in: ""},
		{name: "valid", in: base64.StdEncoding.EncodeToString([]byte(cert))},
		{name: "not base64", in: "this is not base64!!", wantErr: "base64"},
		{name: "base64 of something else", in: base64.StdEncoding.EncodeToString([]byte("hello")), wantErr: "PEM"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := decodePEM(c.in)

			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("decodePEM: %v", err)
				}
				if c.in == "" && out != nil {
					t.Errorf("an absent certificate decoded to %d bytes", len(out))
				}
				if c.in != "" && len(out) == 0 {
					t.Error("a valid certificate decoded to nothing")
				}

				return
			}

			if err == nil {
				t.Fatalf("%q was accepted", c.in)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("the error does not say which encoding failed: %v", err)
			}
		})
	}
}

// The three SASL settings only make sense together, and the mechanism without
// the protocol is the half nobody expects to be caught: the driver would connect
// in plaintext while the manifest looks authenticated.
func TestAMechanismWithoutASASLProtocolIsRejected(t *testing.T) {
	_, err := LoadFrom(env(t, kafkaEnv(
		"OUTBOX_DRIVER_KAFKA_SECURITY_PROTOCOL=PLAINTEXT",
		"OUTBOX_DRIVER_KAFKA_SASL_MECHANISM=SCRAM-SHA-512",
	)...))
	if err == nil {
		t.Fatal("a SASL mechanism over a plaintext protocol was accepted")
	}
	if !strings.Contains(err.Error(), "SASL_MECHANISM") {
		t.Errorf("the message does not name the setting:\n%v", err)
	}
}

// Load reads .env files and then the process environment, and the precedence
// between them is the thing an operator relies on when overriding one setting
// for one run.
func TestLoadPrefersTheProcessEnvironmentOverAFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")

	err := os.WriteFile(path, []byte(strings.Join([]string{
		"OUTBOX_DB_USER=from-file",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://guest:guest@localhost:5672/",
		"OUTBOX_APP_NAME=from-file",
	}, "\n")+"\n"), 0o600)
	if err != nil {
		t.Fatalf("write the file: %v", err)
	}

	t.Setenv("OUTBOX_APP_NAME", "from-environment")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.App.Name != "from-environment" {
		t.Errorf("App.Name = %q; the process environment must win", cfg.App.Name)
	}
	if cfg.DB.User != "from-file" {
		t.Errorf("DB.User = %q; a setting only the file has must survive", cfg.DB.User)
	}
}

// A missing .env is the normal case in a container, where everything arrives as
// environment variables. It must not be an error.
func TestLoadWithoutAFile(t *testing.T) {
	for _, k := range []string{
		"OUTBOX_DB_USER", "OUTBOX_DB_NAME", "OUTBOX_STREAMS",
		"OUTBOX_STREAM_LOCAL_DRIVER", "OUTBOX_DRIVER_RMQ_TYPE", "OUTBOX_DRIVER_RMQ_DSN",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("OUTBOX_DB_USER", "outbox")
	t.Setenv("OUTBOX_DB_NAME", "app")
	t.Setenv("OUTBOX_STREAMS", "local")
	t.Setenv("OUTBOX_STREAM_LOCAL_DRIVER", "rmq")
	t.Setenv("OUTBOX_DRIVER_RMQ_TYPE", "rabbitmq")
	t.Setenv("OUTBOX_DRIVER_RMQ_DSN", "amqp://guest:guest@localhost:5672/")

	missing := filepath.Join(t.TempDir(), "nothing-here.env")

	if _, err := Load(missing); err != nil {
		t.Errorf("Load without a file: %v", err)
	}
	if _, err := LoadAdmin(missing); err != nil {
		t.Errorf("LoadAdmin without a file: %v", err)
	}
}

// A file that exists and cannot be read is a different case, and silently
// carrying on with it would start a dispatcher configured by half of what the
// operator wrote.
func TestLoadReportsAFileItCannotParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.env")

	if err := os.WriteFile(path, []byte("this is not = a = valid line\x00\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Skipf("cannot make the file unreadable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	if _, err := Load(path); err == nil {
		t.Error("an unreadable file was ignored")
	}
}
