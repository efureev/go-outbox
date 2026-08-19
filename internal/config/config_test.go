package config

import (
	"strings"
	"testing"
	"time"

	envi "github.com/efureev/envi/v2"
)

// env builds a source from key=value pairs, so a test never touches the
// process environment.
func env(t *testing.T, kv ...string) *envi.Env {
	t.Helper()

	e := envi.New()
	for _, pair := range kv {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("malformed pair %q", pair)
		}
		e.Set(k, v)
	}

	return e
}

func minimal(extra ...string) []string {
	return append([]string{
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://guest:guest@localhost:5672/",
	}, extra...)
}

func TestLoadFromAppliesDefaults(t *testing.T) {
	cfg, err := LoadFrom(env(t, minimal()...))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	if cfg.App.Name != "outbox" {
		t.Errorf("App.Name = %q, want outbox", cfg.App.Name)
	}
	if cfg.Dispatch.PollInterval != 5*time.Second {
		t.Errorf("PollInterval = %s, want 5s", cfg.Dispatch.PollInterval)
	}
	if cfg.Dispatch.LeaseTTL != 2*time.Minute {
		t.Errorf("LeaseTTL = %s, want 2m", cfg.Dispatch.LeaseTTL)
	}
	if cfg.DB.Schema != "outbox" || cfg.DB.Table != "messages" {
		t.Errorf("table = %s.%s, want outbox.messages", cfg.DB.Schema, cfg.DB.Table)
	}
	if cfg.App.Instance == "" {
		t.Error("App.Instance should default to the hostname")
	}
}

func TestLoadFromParsesDurationsAtLoadTime(t *testing.T) {
	_, err := LoadFrom(env(t, minimal("OUTBOX_DISPATCH_POLL_INTERVAL=nonsense")...))
	if err == nil {
		t.Fatal("a malformed duration must fail at load, not inside the running loop")
	}
	if !strings.Contains(err.Error(), "POLL_INTERVAL") {
		t.Errorf("error should name the offending key, got: %v", err)
	}
}

// Collecting driver settings by string prefix would let the driver "rmq" absorb
// every OUTBOX_DRIVER_RMQ_LOCAL_* variable, pass its "is this configured" check
// on the strength of them, and fail later with an unrelated message. Each driver
// must read exactly its own settings.
func TestDriverNamesSharingAPrefixStaySeparate(t *testing.T) {
	cfg, err := LoadFrom(env(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=one,two",
		"OUTBOX_STREAM_ONE_DRIVER=rmq",
		"OUTBOX_STREAM_TWO_DRIVER=rmq_local",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://shared:5672/",
		"OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://local:5672/",
		"OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=dev",
	))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	shared, ok := cfg.Brokers.Drivers["rmq"].(*RabbitMQDriver)
	if !ok {
		t.Fatalf("driver rmq is %T, want *RabbitMQDriver", cfg.Brokers.Drivers["rmq"])
	}
	if shared.DSN != "amqp://shared:5672/" {
		t.Errorf("driver rmq took DSN %q, want the shared one", shared.DSN)
	}
	if shared.Naming().Prefix != "" {
		t.Errorf("driver rmq took prefix %q from rmq_local", shared.Naming().Prefix)
	}

	local, ok := cfg.Brokers.Drivers["rmq_local"].(*RabbitMQDriver)
	if !ok {
		t.Fatalf("driver rmq_local is %T, want *RabbitMQDriver", cfg.Brokers.Drivers["rmq_local"])
	}
	if local.DSN != "amqp://local:5672/" || local.Naming().Prefix != "dev" {
		t.Errorf("driver rmq_local = %q/%q, want amqp://local:5672/ and dev", local.DSN, local.Naming().Prefix)
	}
}

func TestUnknownDriverKeyIsRejected(t *testing.T) {
	_, err := LoadFrom(env(t, minimal("OUTBOX_DRIVER_RMQ_DNS=amqp://typo/")...))
	if err == nil {
		t.Fatal("a misspelled driver key must fail at boot rather than do nothing")
	}
	if !strings.Contains(err.Error(), "OUTBOX_DRIVER_RMQ_DNS") {
		t.Errorf("error should name the offending key, got: %v", err)
	}
}

func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_DISPATCH_BATCH_SIZE=0",
		"OUTBOX_DISPATCH_MAX_ATTEMPTS=0",
		"OUTBOX_DISPATCH_WORKERS=0",
	)...))
	if err == nil {
		t.Fatal("expected validation to fail")
	}

	for _, want := range []string{"BATCH_SIZE", "MAX_ATTEMPTS", "WORKERS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s so one restart reveals every problem; got:\n%v", want, err)
		}
	}
}

// A lease that expires before a single publish can time out guarantees that
// every batch is reclaimed mid-flight.
func TestLeaseShorterThanPublishTimeoutIsRejected(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_DISPATCH_LEASE_TTL=5s",
		"OUTBOX_DISPATCH_PUBLISH_TIMEOUT=15s",
	)...))
	if err == nil || !strings.Contains(err.Error(), "LEASE_TTL") {
		t.Fatalf("want a LEASE_TTL complaint, got: %v", err)
	}
}

func TestSchemaIdentifierIsValidated(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(`OUTBOX_DB_SCHEMA=tech"; DROP TABLE users; --`)...))
	if err == nil || !strings.Contains(err.Error(), "SCHEMA") {
		t.Fatalf("schema names reach SQL by interpolation and must be validated, got: %v", err)
	}
}

func TestStreamWithoutDriverIsRejected(t *testing.T) {
	_, err := LoadFrom(env(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local,orphan",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=amqp://localhost:5672/",
	))
	if err == nil || !strings.Contains(err.Error(), "orphan") {
		t.Fatalf("want a complaint about the stream with no driver, got: %v", err)
	}
}

func TestKafkaSASLRequiresCredentials(t *testing.T) {
	_, err := LoadFrom(env(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=global",
		"OUTBOX_STREAM_GLOBAL_DRIVER=kafka",
		"OUTBOX_DRIVER_KAFKA_TYPE=kafka",
		"OUTBOX_DRIVER_KAFKA_BROKERS=kafka-1:9092,kafka-2:9092",
		"OUTBOX_DRIVER_KAFKA_SECURITY_PROTOCOL=SASL_SSL",
		"OUTBOX_DRIVER_KAFKA_SASL_MECHANISM=SCRAM-SHA-512",
	))
	if err == nil || !strings.Contains(err.Error(), "SASL_USERNAME") {
		t.Fatalf("want a complaint about the missing credentials, got: %v", err)
	}
}

func TestNamingFormat(t *testing.T) {
	tests := []struct {
		name    string
		naming  Naming
		topic   string
		version int
		want    string
	}{
		{"bare", Naming{PrefixSep: "_", VersionSep: "_"}, "user.created", 0, "user.created"},
		{"prefixed", Naming{Prefix: "prod", PrefixSep: "_", VersionSep: "_"}, "user.created", 0, "prod_user.created"},
		{"versioned", Naming{Prefix: "prod", PrefixSep: "_", VersionSep: "_"}, "user.created", 2, "prod_user.created_v2"},
		{"kafka style", Naming{Prefix: "dev", PrefixSep: ".", VersionSep: "."}, "events", 3, "dev.events.v3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.naming.Format(tt.topic, tt.version); got != tt.want {
				t.Errorf("Format() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDLQStreamMustExist(t *testing.T) {
	_, err := LoadFrom(env(t, minimal(
		"OUTBOX_DLQ_ENABLED=true",
		"OUTBOX_DLQ_STREAM=nowhere",
	)...))
	if err == nil || !strings.Contains(err.Error(), "nowhere") {
		t.Fatalf("want a complaint about the unknown DLQ stream, got: %v", err)
	}
}

// Several drivers of the same type, each its own connection: the routing
// arrangement behind "publish to four streams across three RabbitMQ instances".
func TestSeveralBrokersOfTheSameType(t *testing.T) {
	cfg, err := LoadFrom(env(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=app",
		"OUTBOX_STREAMS=local,test,global,tetra",

		"OUTBOX_STREAM_LOCAL_DRIVER=rmq_local",
		"OUTBOX_STREAM_TEST_DRIVER=rmq_test",
		"OUTBOX_STREAM_GLOBAL_DRIVER=rmq_global",
		"OUTBOX_STREAM_TETRA_DRIVER=rmq_tetra",

		"OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://a:5672/",
		"OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=loc",

		"OUTBOX_DRIVER_RMQ_TEST_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_TEST_DSN=amqp://b:5672/",
		"OUTBOX_DRIVER_RMQ_TEST_PREFIX=tst",

		"OUTBOX_DRIVER_RMQ_GLOBAL_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_GLOBAL_DSN=amqp://c:5672/",
		"OUTBOX_DRIVER_RMQ_GLOBAL_PREFIX=glb",

		// Back to the first instance, but a driver of its own.
		"OUTBOX_DRIVER_RMQ_TETRA_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_TETRA_DSN=amqp://a:5672/",
		"OUTBOX_DRIVER_RMQ_TETRA_PREFIX=ttr",
	))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	if got := len(cfg.Brokers.Streams); got != 4 {
		t.Errorf("%d streams configured, want 4", got)
	}
	if got := len(cfg.Brokers.Drivers); got != 4 {
		t.Errorf("%d drivers built, want 4 — one connection each", got)
	}

	// Every stream reaches the instance it names, with the naming that belongs
	// to that driver and no other.
	for _, want := range []struct{ stream, driver, dsn, prefix string }{
		{"local", "rmq_local", "amqp://a:5672/", "loc"},
		{"test", "rmq_test", "amqp://b:5672/", "tst"},
		{"global", "rmq_global", "amqp://c:5672/", "glb"},
		{"tetra", "rmq_tetra", "amqp://a:5672/", "ttr"},
	} {
		driver, ok := cfg.Brokers.DriverFor(want.stream)
		if !ok || driver != want.driver {
			t.Errorf("stream %q resolves to driver %q, want %q", want.stream, driver, want.driver)

			continue
		}

		d, ok := cfg.Brokers.Drivers[driver].(*RabbitMQDriver)
		if !ok {
			t.Errorf("driver %q is %T, want *RabbitMQDriver", driver, cfg.Brokers.Drivers[driver])

			continue
		}
		if d.DSN != want.dsn {
			t.Errorf("driver %q points at %q, want %q", driver, d.DSN, want.dsn)
		}
		if got := d.Naming().Prefix; got != want.prefix {
			t.Errorf("driver %q uses prefix %q, want %q", driver, got, want.prefix)
		}
	}
}
