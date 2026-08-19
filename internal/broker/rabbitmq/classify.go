package rabbitmq

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/efureev/go-outbox/internal/core"
)

// classify separates a broker that refused a message from a broker that was not
// there to refuse it.
//
// The distinction decides whether the message spends an attempt. It has to lean
// one way: a per-message problem mistaken for an outage never advances its
// counter and so never reaches failed, while an outage mistaken for a
// per-message problem merely costs an attempt. Everything not positively
// identified as unreachable therefore stays retryable.
//
// Two signals are positive. The named errors say so outright — the supervisor
// has no connection, or the channel closed underneath the publish. And a
// confirmation that never arrived, on a connection that is no longer live, is
// the same event seen from the other side: the message was written into a
// socket that had already gone. Checking the connection rather than enumerating
// AMQP's error shapes is what keeps this honest, because the failure that
// matters most here does not arrive as an error at all — it arrives as a
// deadline expiring on a confirmation nobody is left to send.
func (p *Publisher) classify(err error) error {
	if err == nil || core.IsPermanent(err) {
		return err
	}

	if errors.Is(err, ErrNotConnected) || errors.Is(err, amqp.ErrClosed) {
		return core.Unavailable("rabbitmq unreachable", err)
	}

	if !p.conn.live() {
		return core.Unavailable("rabbitmq unreachable", err)
	}

	return err
}
