package config

import (
	"errors"
	"fmt"
	"iter"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// DriverType names a broker implementation.
type DriverType string

const (
	DriverRabbitMQ DriverType = "rabbitmq"
	DriverKafka    DriverType = "kafka"
)

// Source is the value lookup the broker configuration reads from: the merged
// view of the .env files and the process environment. Keys arrive normalised
// to upper case.
type Source interface {
	Lookup(key string) (string, bool)
	All() iter.Seq2[string, string]
}

// BrokerConfig is the routing table: which streams exist, and which driver
// each one publishes through.
type BrokerConfig struct {
	Streams map[string]StreamConfig
	Drivers map[string]DriverConfig
}

// StreamNames returns the configured stream names, sorted, for bounding metric
// label cardinality at startup.
func (c BrokerConfig) StreamNames() []string {
	names := make([]string, 0, len(c.Streams))
	for name := range c.Streams {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

// DriverFor returns the driver name a stream publishes through.
func (c BrokerConfig) DriverFor(stream string) (string, bool) {
	s, ok := c.Streams[strings.ToLower(stream)]
	if !ok {
		return "", false
	}

	return s.Driver, true
}

type StreamConfig struct {
	Name   string
	Driver string
}

// DriverConfig is what every driver configuration has in common.
type DriverConfig interface {
	Name() string
	Type() DriverType
	Naming() Naming
	// Endpoint reports where the driver connects, with any credentials removed.
	// It is what lets an operator confirm that a stream reaches the instance
	// they meant: the driver name and prefix alone look identical whether the
	// DSN points at the right broker or the wrong one.
	Endpoint() string
}

// Naming assembles the effective topic or queue name from its parts:
//
//	[prefix + prefixSep] + topic + [versionSep + "v" + version]
type Naming struct {
	Prefix     string
	PrefixSep  string
	VersionSep string
}

// Format returns the effective name the broker sees. A consumer must subscribe
// to this name, not to the bare topic stored in the row.
func (n Naming) Format(topic string, version int) string {
	name := topic
	if version > 0 {
		name = topic + n.VersionSep + "v" + strconv.Itoa(version)
	}
	if n.Prefix == "" {
		return name
	}

	return n.Prefix + n.PrefixSep + name
}

// RabbitMQDriver configures one AMQP connection.
type RabbitMQDriver struct {
	name   string
	naming Naming

	DSN string
	// Channels is the size of the publish channel pool. A single channel behind
	// a mutex serialises every publish and puts a ceiling on throughput that no
	// number of workers can lift.
	Channels int
	// Declare makes the publisher declare a queue before publishing to it. Off
	// by default: broker topology belongs to whoever owns the broker, and the
	// declaration costs a round trip the publish path does not need.
	Declare bool
	// Mandatory asks the broker to return an unroutable message instead of
	// silently discarding it, which turns a misrouted publish into a visible
	// permanent error.
	Mandatory      bool
	PublishTimeout time.Duration
	ReconnectDelay time.Duration
}

func (d *RabbitMQDriver) Name() string   { return d.name }
func (*RabbitMQDriver) Type() DriverType { return DriverRabbitMQ }
func (d *RabbitMQDriver) Naming() Naming { return d.naming }

// Endpoint is the DSN with its user info stripped, so it can be logged and
// served over HTTP without leaking the password.
func (d *RabbitMQDriver) Endpoint() string { return redactDSN(d.DSN) }

// KafkaDriver configures one Kafka writer.
type KafkaDriver struct {
	name   string
	naming Naming

	Brokers          []string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	SSLCaPEM         []byte
	SSLCertPEM       []byte
	SSLKeyPEM        []byte

	Compression  string
	RequiredAcks string
	MaxAttempts  int
	WriteTimeout time.Duration
	// AllowAutoTopicCreation stays off by default: a typo in a topic name
	// should fail loudly rather than create a topic nobody consumes.
	AllowAutoTopicCreation bool
}

func (d *KafkaDriver) Name() string   { return d.name }
func (*KafkaDriver) Type() DriverType { return DriverKafka }
func (d *KafkaDriver) Naming() Naming { return d.naming }

// Endpoint lists the configured brokers; Kafka addresses carry no credentials.
func (d *KafkaDriver) Endpoint() string { return strings.Join(d.Brokers, ",") }

const (
	keyStreams      = "OUTBOX_STREAMS"
	streamPrefixFmt = "OUTBOX_STREAM_%s_"
	driverPrefixFmt = "OUTBOX_DRIVER_%s_"

	defaultStream = "local"
)

// Keys understood inside a DRIVER_<NAME>_ namespace. Lookup is exact and the
// set is closed. Matching by string prefix instead would let the driver "rmq"
// absorb every variable belonging to "rmq_local", pass its "is this driver
// configured" check on the strength of them, and then fail with an unrelated
// message.
var (
	commonDriverKeys = []string{"TYPE", "PREFIX", "PREFIX_SEP", "VERSION_SEP"}

	rabbitDriverKeys = []string{
		"DSN", "CHANNELS", "DECLARE", "MANDATORY", "PUBLISH_TIMEOUT", "RECONNECT_DELAY",
	}

	kafkaDriverKeys = []string{
		"BROKERS", "SECURITY_PROTOCOL", "SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD",
		"SSL_CA_PEM_B64", "SSL_CERT_PEM_B64", "SSL_KEY_PEM_B64",
		"COMPRESSION", "REQUIRED_ACKS", "MAX_ATTEMPTS", "WRITE_TIMEOUT", "ALLOW_AUTO_TOPIC_CREATION",
	}
)

// Default separators. Kafka names are conventionally dotted, AMQP queue names
// underscored. Changing either renames every topic a consumer subscribes to, so
// they are settled rather than tunable-by-default.
const (
	defaultRabbitPrefixSep  = "_"
	defaultRabbitVersionSep = "_"
	defaultKafkaPrefixSep   = "."
	defaultKafkaVersionSep  = "."
)

// loadBrokers assembles the routing table from src.
func loadBrokers(src Source) (BrokerConfig, error) {
	var empty BrokerConfig

	raw, _ := src.Lookup(keyStreams)
	names := parseList(raw)
	if len(names) == 0 {
		names = []string{defaultStream}
	}

	streams := make(map[string]StreamConfig, len(names))
	driverNames := make([]string, 0, len(names))

	for _, name := range names {
		// Streams are matched case-insensitively at publish time, so the name is
		// normalised once, here. Normalising in one place and not the other
		// surfaces as an empty driver label on every metric for a stream written
		// in mixed case — a symptom well removed from its cause.
		stream := strings.ToLower(name)

		key := fmt.Sprintf(streamPrefixFmt, envToken(stream)) + "DRIVER"
		driver := strings.ToLower(strings.TrimSpace(lookup(src, key)))
		if driver == "" {
			return empty, fmt.Errorf("stream %q: %s is not set", stream, key)
		}

		streams[stream] = StreamConfig{Name: stream, Driver: driver}
		if !slices.Contains(driverNames, driver) {
			driverNames = append(driverNames, driver)
		}
	}

	if err := rejectCollidingNames(driverNames); err != nil {
		return empty, err
	}

	// Every driver is attempted, and the failures are reported together: with
	// several drivers configured, stopping at the first one turns fixing the
	// routing table into one restart per mistake.
	drivers := make(map[string]DriverConfig, len(driverNames))

	var errs []error

	for _, name := range driverNames {
		d, err := loadDriver(src, name, driverNames)
		if err != nil {
			errs = append(errs, fmt.Errorf("driver %q: %w", name, err))

			continue
		}
		drivers[name] = d
	}

	if len(errs) > 0 {
		return empty, errors.Join(errs...)
	}

	return BrokerConfig{Streams: streams, Drivers: drivers}, nil
}

// rejectCollidingNames catches driver names that differ only in a character
// envToken normalises away: "rmq-local" and "rmq_local" both address
// OUTBOX_DRIVER_RMQ_LOCAL_, so they would silently become two drivers reading
// one settings block — two connections to the same broker, where the operator
// meant two brokers.
func rejectCollidingNames(names []string) error {
	seen := make(map[string]string, len(names))

	for _, name := range names {
		token := envToken(name)
		if other, clash := seen[token]; clash {
			return fmt.Errorf(
				"drivers %q and %q both read %s* — rename one so their settings are distinguishable",
				other, name, fmt.Sprintf(driverPrefixFmt, token))
		}
		seen[token] = name
	}

	return nil
}

func loadDriver(src Source, name string, allNames []string) (DriverConfig, error) {
	ns := fmt.Sprintf(driverPrefixFmt, envToken(name))

	get := func(key string) string {
		return strings.TrimSpace(lookup(src, ns+key))
	}

	rawType := strings.ToLower(get("TYPE"))
	if rawType == "" {
		return nil, fmt.Errorf("%sTYPE is not set", ns)
	}

	dType, err := parseDriverType(rawType)
	if err != nil {
		return nil, err
	}

	var known []string
	switch dType {
	case DriverRabbitMQ:
		known = append(slices.Clone(commonDriverKeys), rabbitDriverKeys...)
	case DriverKafka:
		known = append(slices.Clone(commonDriverKeys), kafkaDriverKeys...)
	}

	if err := rejectUnknownKeys(src, ns, name, allNames, known); err != nil {
		return nil, err
	}

	naming := Naming{Prefix: get("PREFIX")}

	switch dType {
	case DriverRabbitMQ:
		naming.PrefixSep = orDefault(get("PREFIX_SEP"), defaultRabbitPrefixSep)
		naming.VersionSep = orDefault(get("VERSION_SEP"), defaultRabbitVersionSep)

		return buildRabbitDriver(name, naming, get)

	case DriverKafka:
		naming.PrefixSep = orDefault(get("PREFIX_SEP"), defaultKafkaPrefixSep)
		naming.VersionSep = orDefault(get("VERSION_SEP"), defaultKafkaVersionSep)

		return buildKafkaDriver(name, naming, get)
	}

	return nil, fmt.Errorf("unsupported driver type %q", dType)
}

// rejectUnknownKeys turns a typo into a startup error instead of a setting
// that silently does nothing. A variable is only reported when it cannot
// belong to another, longer driver name sharing this prefix.
func rejectUnknownKeys(src Source, ns, name string, allNames, known []string) error {
	var unknown []string

	for key := range src.All() {
		suffix, ok := strings.CutPrefix(key, ns)
		if !ok || suffix == "" {
			continue
		}
		if slices.Contains(known, suffix) {
			continue
		}
		if belongsToOtherDriver(key, name, allNames) {
			continue
		}
		unknown = append(unknown, key)
	}

	if len(unknown) == 0 {
		return nil
	}

	slices.Sort(unknown)

	return fmt.Errorf("unknown setting(s) %s; supported keys are %s",
		strings.Join(unknown, ", "), strings.Join(prefixAll(ns, known), ", "))
}

// belongsToOtherDriver reports whether key sits in the namespace of a declared
// driver whose name is longer than this one — "rmq" must not complain about
// OUTBOX_DRIVER_RMQ_LOCAL_DSN when "rmq_local" is also configured.
func belongsToOtherDriver(key, self string, allNames []string) bool {
	for _, other := range allNames {
		if other == self || len(other) <= len(self) {
			continue
		}
		if strings.HasPrefix(key, fmt.Sprintf(driverPrefixFmt, envToken(other))) {
			return true
		}
	}

	return false
}

func prefixAll(ns string, keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = ns + k
	}
	slices.Sort(out)

	return out
}

func parseDriverType(s string) (DriverType, error) {
	switch s {
	case "rmq", "rabbit", "rabbitmq", "amqp":
		return DriverRabbitMQ, nil
	case "kafka":
		return DriverKafka, nil
	default:
		return "", fmt.Errorf("unsupported driver type %q (want rabbitmq or kafka)", s)
	}
}

// envToken converts a stream or driver name into the form it takes inside an
// environment variable name.
func envToken(name string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(name), "-", "_"))
}

func lookup(src Source, key string) string {
	v, _ := src.Lookup(key)

	return v
}

func parseList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}

	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

// redactDSN removes the user info from a URL so it can be logged or served.
// A DSN that will not parse is reported as unavailable rather than echoed, on
// the chance that the unparsed text is exactly the password.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "unparseable"
	}

	u.User = nil

	return u.String()
}
