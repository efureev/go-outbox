package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/efureev/go-outbox/internal/config"
)

// returnBuffer is deliberately larger than the one in-flight publish a channel
// ever has. The client library sends a return with a blocking send from the
// connection's reader goroutine, so a full buffer would stall every channel on
// the connection, not just this one.
const returnBuffer = 16

// publishChannel is one confirm-mode AMQP channel, together with the return
// stream that tells an unroutable message from a delivered one.
//
// A channel is held by exactly one publisher at a time, taken from and given
// back to a pool, so nothing here needs a lock.
type publishChannel struct {
	ch      *amqp.Channel
	returns <-chan amqp.Return
}

func newPublishChannel(conn *amqp.Connection, cfg *config.RabbitMQDriver) (*publishChannel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	// Confirm mode is what makes "the broker has it" mean anything. Without it,
	// a successful publish says only that the bytes left this process.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()

		return nil, fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}

	pc := &publishChannel{ch: ch}
	if cfg.Mandatory {
		pc.returns = ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	}

	return pc, nil
}

// takeReturn reports the reason the broker gave back this message, if it did.
//
// It is read synchronously, after the confirmation for the same message has
// resolved, and that ordering is what makes it reliable. The library dispatches
// frames from one goroutine in the order they arrive; RabbitMQ sends
// basic.return before basic.ack for an unroutable mandatory publish; and the
// send into this buffered channel therefore completes before the confirmation
// is resolved. Reading it here cannot miss a return that belongs to the message
// just confirmed.
//
// Collecting returns in a background goroutine instead — the obvious shape —
// reintroduces exactly the race this avoids: the confirmation can resolve
// before the collector has recorded the return, and an unroutable message is
// then reported as delivered.
func (pc *publishChannel) takeReturn(messageID string) (string, bool) {
	if pc.returns == nil {
		return "", false
	}

	for {
		select {
		case ret, ok := <-pc.returns:
			if !ok {
				return "", false
			}
			if ret.MessageId == messageID {
				return fmt.Sprintf("%d %s", ret.ReplyCode, ret.ReplyText), true
			}
			// A return for some other message means a previous publish left one
			// behind. Draining it keeps the buffer clear; it cannot be reported
			// now because that publish has already been answered.
		default:
			return "", false
		}
	}
}

// drainReturns clears anything left over from an earlier publish, so a stale
// return cannot be mistaken for this message's.
func (pc *publishChannel) drainReturns() {
	if pc.returns == nil {
		return
	}

	for {
		select {
		case _, ok := <-pc.returns:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (pc *publishChannel) close() error { return pc.ch.Close() }
