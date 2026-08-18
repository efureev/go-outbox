// Package kafka publishes to Kafka.
//
// The whole batch goes in one WriteMessages call. kafka-go reports per-message
// failures through a positional error slice, so nothing is given up by batching
// — whereas one call per message with RequiredAcks=all pays a full broker round
// trip for each.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// Publisher writes to Kafka.
type Publisher struct {
	cfg    *config.KafkaDriver
	writer *kafka.Writer
	log    *slog.Logger
}

// New builds the writer and verifies that at least one broker answers.
func New(ctx context.Context, cfg *config.KafkaDriver, log *slog.Logger) (*Publisher, error) {
	transport, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}

	if err := probe(ctx, cfg, transport); err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(cfg.Brokers...),
		Transport: transport,
		// Hash keeps every message with the same partition key on the same
		// partition, which is the only ordering guarantee Kafka can offer here.
		Balancer:               &kafka.Hash{},
		RequiredAcks:           requiredAcks(cfg.RequiredAcks),
		Compression:            compression(cfg.Compression),
		MaxAttempts:            cfg.MaxAttempts,
		WriteTimeout:           cfg.WriteTimeout,
		ReadTimeout:            cfg.WriteTimeout,
		AllowAutoTopicCreation: cfg.AllowAutoTopicCreation,
		// Synchronous: the dispatcher may only mark a row delivered once the
		// broker has acknowledged it. Async would return before that and turn
		// every write into a possible silent loss.
		Async: false,
		// One call carries the whole batch, so there is nothing to wait for.
		BatchTimeout: time.Millisecond,
	}

	return &Publisher{cfg: cfg, writer: writer, log: log}, nil
}

// Publish writes the batch and returns one result per message.
func (p *Publisher) Publish(ctx context.Context, msgs []core.Message) []error {
	results := make([]error, len(msgs))
	if len(msgs) == 0 {
		return results
	}

	records := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		dest := broker.Resolve(p.cfg.Naming(), msg)

		records[i] = kafka.Message{
			Topic:   dest.Topic,
			Key:     dest.Key,
			Value:   msg.Payload,
			Headers: headers(msg),
			Time:    msg.CreatedAt,
		}
	}

	writeCtx, cancel := context.WithTimeout(ctx, p.cfg.WriteTimeout)
	defer cancel()

	err := p.writer.WriteMessages(writeCtx, records...)
	if err == nil {
		return results
	}

	// A positional error slice means one bad message does not condemn the rest
	// of the batch to a pointless retry.
	var partial kafka.WriteErrors
	if errors.As(err, &partial) && len(partial) == len(msgs) {
		for i, e := range partial {
			results[i] = classify(e)
		}

		return results
	}

	// Anything else applies to the batch as a whole.
	shared := classify(err)
	for i := range results {
		results[i] = shared
	}

	return results
}

// headers carries the row's headers plus the message id, so a consumer has an
// identifier to deduplicate on without reaching into a payload the dispatcher
// treats as opaque bytes.
func headers(msg core.Message) []kafka.Header {
	out := make([]kafka.Header, 0, len(msg.Headers)+1)
	out = append(out, kafka.Header{Key: "message_id", Value: []byte(msg.ID)})

	for k, v := range msg.Headers {
		if k == "message_id" {
			continue
		}
		out = append(out, kafka.Header{Key: k, Value: []byte(v)})
	}

	return out
}

// classify decides whether retrying can help.
//
// Kafka's protocol errors say so themselves, and the ones that do not retry
// cleanly — a topic that does not exist, a payload above the broker's limit, a
// rejected credential — burn the entire retry budget to reach a conclusion
// available on the first attempt.
func classify(err error) error {
	if err == nil {
		return nil
	}

	var kerr kafka.Error
	if errors.As(err, &kerr) {
		switch kerr {
		case kafka.MessageSizeTooLarge,
			kafka.RecordListTooLarge,
			kafka.InvalidTopic,
			kafka.InvalidRecord,
			kafka.TopicAuthorizationFailed,
			kafka.ClusterAuthorizationFailed,
			kafka.SASLAuthenticationFailed,
			kafka.UnsupportedForMessageFormat:
			return core.Permanent(kerr.Title(), err)
		case kafka.UnknownTopicOrPartition:
			// Retrying helps only if something else is going to create the
			// topic, and auto-creation is off by default precisely so a typo
			// does not quietly mint one.
			return core.Permanent("unknown topic", err)
		default:
			// Every other protocol error is classified by the protocol itself.
			if !kerr.Temporary() {
				return core.Permanent(kerr.Title(), err)
			}
		}
	}

	return err
}

// probe checks that at least one broker answers, so a bad address fails at
// startup rather than on the first message. Every broker is tried: dialling
// only the first listed one would refuse to start against a healthy cluster
// whose first node happened to be down.
func probe(ctx context.Context, cfg *config.KafkaDriver, transport *kafka.Transport) error {
	dialer := &kafka.Dialer{
		Timeout:       5 * time.Second,
		TLS:           transport.TLS,
		SASLMechanism: transport.SASL,
	}

	var errs []error
	for _, addr := range cfg.Brokers {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := dialer.DialContext(dialCtx, "tcp", addr)
		cancel()

		if err == nil {
			_ = conn.Close()

			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
	}

	return fmt.Errorf("kafka: no broker answered: %w", errors.Join(errs...))
}

// HealthCheck asks the cluster for its metadata.
func (p *Publisher) HealthCheck(ctx context.Context) error {
	client := &kafka.Client{
		Addr:      p.writer.Addr,
		Timeout:   5 * time.Second,
		Transport: p.writer.Transport,
	}

	if _, err := client.Metadata(ctx, &kafka.MetadataRequest{}); err != nil {
		return fmt.Errorf("kafka: metadata: %w", err)
	}

	return nil
}

// Close flushes and shuts the writer down.
func (p *Publisher) Close(context.Context) error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("kafka: close writer: %w", err)
	}

	return nil
}

func requiredAcks(s string) kafka.RequiredAcks {
	switch strings.ToLower(s) {
	case "none":
		return kafka.RequireNone
	case "one":
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}

func compression(s string) kafka.Compression {
	switch strings.ToLower(s) {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return 0
	}
}

func newTransport(cfg *config.KafkaDriver) (*kafka.Transport, error) {
	tlsCfg, err := tlsConfig(cfg)
	if err != nil {
		return nil, err
	}

	mechanism, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}

	return &kafka.Transport{TLS: tlsCfg, SASL: mechanism, DialTimeout: 5 * time.Second}, nil
}

func tlsConfig(cfg *config.KafkaDriver) (*tls.Config, error) {
	needed := strings.EqualFold(cfg.SecurityProtocol, "SSL") ||
		strings.EqualFold(cfg.SecurityProtocol, "SASL_SSL") ||
		len(cfg.SSLCaPEM) > 0 ||
		len(cfg.SSLCertPEM) > 0

	if !needed {
		return nil, nil
	}

	out := &tls.Config{MinVersion: tls.VersionTLS12}

	if len(cfg.SSLCaPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.SSLCaPEM) {
			return nil, errors.New("kafka: the CA bundle contains no usable certificate")
		}
		out.RootCAs = pool
	}

	if len(cfg.SSLCertPEM) > 0 && len(cfg.SSLKeyPEM) > 0 {
		cert, err := tls.X509KeyPair(cfg.SSLCertPEM, cfg.SSLKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("kafka: client certificate: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}

	return out, nil
}

func saslMechanism(cfg *config.KafkaDriver) (sasl.Mechanism, error) {
	if cfg.SASLMechanism == "" {
		return nil, nil
	}

	switch strings.ToUpper(cfg.SASLMechanism) {
	case "PLAIN":
		return plain.Mechanism{Username: cfg.SASLUsername, Password: cfg.SASLPassword}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
}

var _ broker.Publisher = (*Publisher)(nil)
