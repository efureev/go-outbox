package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultRabbitChannels       = 4
	defaultRabbitPublishTimeout = 15 * time.Second
	defaultRabbitReconnectDelay = time.Second
	maxRabbitChannels           = 128
)

func buildRabbitDriver(name string, naming Naming, get func(string) string) (*RabbitMQDriver, error) {
	dsn := get("DSN")
	if dsn == "" {
		return nil, errors.New("DSN is required")
	}
	if !strings.HasPrefix(dsn, "amqp://") && !strings.HasPrefix(dsn, "amqps://") {
		return nil, errors.New("DSN must start with amqp:// or amqps://")
	}

	channels, err := intOrDefault(get("CHANNELS"), defaultRabbitChannels)
	if err != nil {
		return nil, fmt.Errorf("CHANNELS: %w", err)
	}
	if channels < 1 || channels > maxRabbitChannels {
		return nil, fmt.Errorf("CHANNELS must be between 1 and %d, got %d", maxRabbitChannels, channels)
	}

	declare, err := boolOrDefault(get("DECLARE"), false)
	if err != nil {
		return nil, fmt.Errorf("DECLARE: %w", err)
	}

	mandatory, err := boolOrDefault(get("MANDATORY"), true)
	if err != nil {
		return nil, fmt.Errorf("MANDATORY: %w", err)
	}

	publishTimeout, err := durationOrDefault(get("PUBLISH_TIMEOUT"), defaultRabbitPublishTimeout)
	if err != nil {
		return nil, fmt.Errorf("PUBLISH_TIMEOUT: %w", err)
	}

	reconnectDelay, err := durationOrDefault(get("RECONNECT_DELAY"), defaultRabbitReconnectDelay)
	if err != nil {
		return nil, fmt.Errorf("RECONNECT_DELAY: %w", err)
	}

	return &RabbitMQDriver{
		name:           name,
		naming:         naming,
		DSN:            dsn,
		Channels:       channels,
		Declare:        declare,
		Mandatory:      mandatory,
		PublishTimeout: publishTimeout,
		ReconnectDelay: reconnectDelay,
	}, nil
}
