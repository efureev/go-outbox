package rabbitmq

import (
	"errors"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/efureev/go-outbox/internal/core"
)

// live builds a publisher whose supervisor either has a usable connection or
// does not. A zero-value amqp.Connection reports itself open, which is all
// classify asks of it.
func publisherWith(connected bool) *Publisher {
	c := &Conn{}
	if connected {
		c.current = &connection{conn: &amqp.Connection{}}
	}

	return &Publisher{conn: c}
}

func TestClassifyWithNoConnection(t *testing.T) {
	p := publisherWith(false)

	cases := map[string]error{
		"the supervisor has nothing to publish through": ErrNotConnected,
		"the channel closed underneath the publish":     fmt.Errorf("publish: %w", amqp.ErrClosed),
		// This is the case that matters most and looks least like an outage: the
		// write went into a socket that had already gone, so what fails is a
		// confirmation deadline rather than the publish.
		"a confirmation that never arrived": errors.New("awaiting confirmation: context deadline exceeded"),
	}

	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			if got := p.classify(err); !core.IsUnavailable(got) {
				t.Errorf("not classified as unreachable, so the message would spend an attempt: %v", got)
			}
		})
	}
}

// A broker that answers and says no is not an outage. Getting this wrong is the
// expensive direction: a per-message problem read as unavailable never advances
// its counter, so it never reaches failed and retries until somebody notices.
func TestClassifyLeavesRejectionsAlone(t *testing.T) {
	p := publisherWith(true)

	nacked := errors.New("rabbitmq: broker nacked 6f1b")
	if got := p.classify(nacked); core.IsUnavailable(got) {
		t.Errorf("a broker nack was read as an outage: %v", got)
	}

	unroutable := core.Permanent("unroutable", errors.New("NO_ROUTE"))
	got := p.classify(unroutable)
	if core.IsUnavailable(got) {
		t.Errorf("an unroutable message was read as an outage: %v", got)
	}
	if !core.IsPermanent(got) {
		t.Errorf("permanence was lost: %v", got)
	}
}

// Permanence survives a broker that is also gone: the next attempt reaches the
// same verdict, and deferring it would mean it never fails at all.
func TestClassifyKeepsPermanenceWhileDisconnected(t *testing.T) {
	p := publisherWith(false)

	got := p.classify(core.Permanent("payload too large", nil))

	if !core.IsPermanent(got) {
		t.Errorf("permanence was lost while disconnected: %v", got)
	}
	if core.IsUnavailable(got) {
		t.Errorf("a permanent failure was downgraded to an outage: %v", got)
	}
}

func TestClassifyPassesNilThrough(t *testing.T) {
	if got := publisherWith(false).classify(nil); got != nil {
		t.Errorf("classify(nil) = %v", got)
	}
}
