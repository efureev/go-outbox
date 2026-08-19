package app

import (
	"testing"

	"github.com/efureev/go-outbox/internal/config"
)

// The routing table is one field of stream:driver pairs rather than two fields
// listing names, because two lists have to be paired up by hand — and get it
// wrong the moment a stream is added to one of them.
func TestStreamSummaryPairsStreamsWithDrivers(t *testing.T) {
	brokers := config.BrokerConfig{
		Streams: map[string]config.StreamConfig{
			"local":  {Name: "local", Driver: "rmq_local"},
			"global": {Name: "global", Driver: "kfk"},
			"tetra":  {Name: "tetra", Driver: "rmq_tetra"},
		},
	}

	// Sorted, so two restarts of the same configuration produce the same line
	// and a diff of two log captures shows only what really changed.
	want := "global:kfk,local:rmq_local,tetra:rmq_tetra"
	if got := streamSummary(brokers); got != want {
		t.Errorf("streamSummary() = %q, want %q", got, want)
	}
}

func TestStreamSummaryHandlesASingleStream(t *testing.T) {
	brokers := config.BrokerConfig{
		Streams: map[string]config.StreamConfig{"local": {Name: "local", Driver: "rmq"}},
	}

	if got := streamSummary(brokers); got != "local:rmq" {
		t.Errorf("streamSummary() = %q, want local:rmq", got)
	}
}

func TestStreamSummaryOfNothingIsEmpty(t *testing.T) {
	if got := streamSummary(config.BrokerConfig{}); got != "" {
		t.Errorf("streamSummary() = %q, want an empty string", got)
	}
}
