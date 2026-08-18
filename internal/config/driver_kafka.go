package config

import (
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Accepted values, kept as closed sets so a typo fails at boot.
var (
	kafkaProtocols   = []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
	kafkaMechanisms  = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
	kafkaCompression = []string{"none", "gzip", "snappy", "lz4", "zstd"}
	kafkaAcks        = []string{"none", "one", "all"}
)

const (
	defaultKafkaMaxAttempts  = 3
	defaultKafkaWriteTimeout = 15 * time.Second
)

func buildKafkaDriver(name string, naming Naming, get func(string) string) (*KafkaDriver, error) {
	brokers := parseList(get("BROKERS"))
	if len(brokers) == 0 {
		return nil, errors.New("BROKERS is required")
	}

	protocol := strings.ToUpper(get("SECURITY_PROTOCOL"))
	if protocol != "" && !slices.Contains(kafkaProtocols, protocol) {
		return nil, fmt.Errorf("SECURITY_PROTOCOL %q is not one of %s", protocol, strings.Join(kafkaProtocols, ", "))
	}

	mechanism := strings.ToUpper(get("SASL_MECHANISM"))
	if mechanism != "" && !slices.Contains(kafkaMechanisms, mechanism) {
		return nil, fmt.Errorf("SASL_MECHANISM %q is not one of %s", mechanism, strings.Join(kafkaMechanisms, ", "))
	}

	username, password := get("SASL_USERNAME"), get("SASL_PASSWORD")

	// The three SASL settings only make sense together; either all of them are
	// present or none is.
	switch {
	case strings.HasPrefix(protocol, "SASL"):
		if mechanism == "" {
			return nil, fmt.Errorf("SECURITY_PROTOCOL=%s requires SASL_MECHANISM", protocol)
		}
		if username == "" || password == "" {
			return nil, fmt.Errorf("SECURITY_PROTOCOL=%s requires SASL_USERNAME and SASL_PASSWORD", protocol)
		}
	case mechanism != "":
		return nil, errors.New("SASL_MECHANISM is set but SECURITY_PROTOCOL is not a SASL_* protocol")
	}

	caPEM, err := decodePEM(get("SSL_CA_PEM_B64"))
	if err != nil {
		return nil, fmt.Errorf("SSL_CA_PEM_B64: %w", err)
	}
	certPEM, err := decodePEM(get("SSL_CERT_PEM_B64"))
	if err != nil {
		return nil, fmt.Errorf("SSL_CERT_PEM_B64: %w", err)
	}
	keyPEM, err := decodePEM(get("SSL_KEY_PEM_B64"))
	if err != nil {
		return nil, fmt.Errorf("SSL_KEY_PEM_B64: %w", err)
	}
	if (len(certPEM) == 0) != (len(keyPEM) == 0) {
		return nil, errors.New("SSL_CERT_PEM_B64 and SSL_KEY_PEM_B64 must be set together")
	}

	compression := strings.ToLower(orDefault(get("COMPRESSION"), "none"))
	if !slices.Contains(kafkaCompression, compression) {
		return nil, fmt.Errorf("COMPRESSION %q is not one of %s", compression, strings.Join(kafkaCompression, ", "))
	}

	acks := strings.ToLower(orDefault(get("REQUIRED_ACKS"), "all"))
	if !slices.Contains(kafkaAcks, acks) {
		return nil, fmt.Errorf("REQUIRED_ACKS %q is not one of %s", acks, strings.Join(kafkaAcks, ", "))
	}

	maxAttempts, err := intOrDefault(get("MAX_ATTEMPTS"), defaultKafkaMaxAttempts)
	if err != nil {
		return nil, fmt.Errorf("MAX_ATTEMPTS: %w", err)
	}
	if maxAttempts < 1 {
		return nil, fmt.Errorf("MAX_ATTEMPTS must be at least 1, got %d", maxAttempts)
	}

	writeTimeout, err := durationOrDefault(get("WRITE_TIMEOUT"), defaultKafkaWriteTimeout)
	if err != nil {
		return nil, fmt.Errorf("WRITE_TIMEOUT: %w", err)
	}

	autoCreate, err := boolOrDefault(get("ALLOW_AUTO_TOPIC_CREATION"), false)
	if err != nil {
		return nil, fmt.Errorf("ALLOW_AUTO_TOPIC_CREATION: %w", err)
	}

	return &KafkaDriver{
		name:                   name,
		naming:                 naming,
		Brokers:                brokers,
		SecurityProtocol:       protocol,
		SASLMechanism:          mechanism,
		SASLUsername:           username,
		SASLPassword:           password,
		SSLCaPEM:               caPEM,
		SSLCertPEM:             certPEM,
		SSLKeyPEM:              keyPEM,
		Compression:            compression,
		RequiredAcks:           acks,
		MaxAttempts:            maxAttempts,
		WriteTimeout:           writeTimeout,
		AllowAutoTopicCreation: autoCreate,
	}, nil
}

func decodePEM(src string) ([]byte, error) {
	if src == "" {
		return nil, nil
	}

	raw, err := base64.StdEncoding.DecodeString(src)
	if err != nil {
		return nil, fmt.Errorf("not valid base64: %w", err)
	}
	if block, _ := pem.Decode(raw); block == nil {
		return nil, errors.New("decodes to something that is not PEM")
	}

	return raw, nil
}
