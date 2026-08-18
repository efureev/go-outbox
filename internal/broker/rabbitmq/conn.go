// Package rabbitmq publishes through AMQP 0-9-1.
//
// Three things here differ from the previous version, and each was a defect
// rather than a preference:
//
//   - Confirmations are per message, through the deferred confirmation the
//     client library returns from a publish. The previous version shared one
//     NotifyPublish channel across every publish and read a single value after
//     each one. When a publish timed out, its confirmation stayed in the
//     channel and the *next* message read it — and was marked delivered
//     without the broker ever having confirmed it. That is message loss, and it
//     is silent.
//
//   - The connection is supervised. The previous version tested
//     "connection == nil" to decide whether to reconnect, and that field never
//     became nil once set, so a connection closed by the broker was never
//     noticed; recovery happened only as a side effect of a queue declaration
//     failing.
//
//   - Queues are declared at most once per name, and not at all by default.
//     The previous version declared on every single publish, paying a round
//     trip per message for topology that belongs to whoever owns the broker.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/efureev/go-outbox/internal/config"
)

// ErrNotConnected is returned while the supervisor is re-establishing the
// connection. It is retryable: the message stays pending and the next attempt
// finds a live connection.
var ErrNotConnected = errors.New("rabbitmq: not connected")

// connection holds a live AMQP connection and its pool of publish channels.
type connection struct {
	conn     *amqp.Connection
	channels chan *publishChannel
	all      []*publishChannel
}

// Conn supervises one AMQP connection: it dials, keeps a pool of confirm-mode
// channels, and redials when the broker closes it.
type Conn struct {
	cfg *config.RabbitMQDriver
	log *slog.Logger

	mu      sync.RWMutex
	current *connection

	closeOnce sync.Once
	closed    chan struct{}
	// redial carries a request to rebuild the connection. It is buffered so a
	// failing publish can signal without blocking on the supervisor.
	redial chan struct{}

	// declared caches the queue names already declared on this connection, so
	// declaration costs one round trip per name rather than one per message.
	declared sync.Map
}

// Dial establishes the initial connection and starts the supervisor.
func Dial(ctx context.Context, cfg *config.RabbitMQDriver, log *slog.Logger) (*Conn, error) {
	c := &Conn{
		cfg:    cfg,
		log:    log,
		closed: make(chan struct{}),
		redial: make(chan struct{}, 1),
	}

	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	// The supervisor outlives this call: it has to keep redialling long after
	// the context that established the first connection is done.
	//nolint:contextcheck // reconnection is not bounded by the startup context
	go c.supervise()

	return c, nil
}

func (c *Conn) connect(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.PublishTimeout)
	defer cancel()

	conn, err := amqp.DialConfig(c.cfg.DSN, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(dialCtx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("rabbitmq: dial: %w", err)
	}

	channels := make(chan *publishChannel, c.cfg.Channels)
	all := make([]*publishChannel, 0, c.cfg.Channels)

	for range c.cfg.Channels {
		pc, err := newPublishChannel(conn, c.cfg)
		if err != nil {
			_ = conn.Close()

			return err
		}
		all = append(all, pc)
		channels <- pc
	}

	// A fresh connection has declared nothing.
	c.declared.Clear()

	c.mu.Lock()
	c.current = &connection{conn: conn, channels: channels, all: all}
	c.mu.Unlock()

	// The broker closing the connection is the signal the previous version had
	// no way to observe.
	notify := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err, ok := <-notify
		if !ok {
			return
		}
		c.log.Warn("rabbitmq connection closed by the broker", slog.Any("error", err))
		c.requestRedial()
	}()

	c.log.Info("rabbitmq connected", slog.Int("channels", c.cfg.Channels))

	return nil
}

func (c *Conn) requestRedial() {
	c.mu.Lock()
	c.current = nil
	c.mu.Unlock()

	select {
	case c.redial <- struct{}{}:
	default:
		// A redial is already queued; one is enough.
	}
}

// supervise rebuilds the connection whenever it drops, backing off between
// attempts so a broker that is down is not hammered.
func (c *Conn) supervise() {
	for {
		select {
		case <-c.closed:
			return
		case <-c.redial:
		}

		delay := c.cfg.ReconnectDelay
		for {
			select {
			case <-c.closed:
				return
			case <-time.After(delay):
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PublishTimeout)
			err := c.connect(ctx)
			cancel()

			if err == nil {
				break
			}

			c.log.Warn("rabbitmq reconnect failed", slog.Any("error", err), slog.Duration("retry_in", delay))

			if delay < maxReconnectDelay {
				delay *= 2
			}
		}
	}
}

const maxReconnectDelay = 30 * time.Second

// acquire takes a channel from the pool, or reports that there is no live
// connection to take one from.
func (c *Conn) acquire(ctx context.Context) (*publishChannel, *connection, error) {
	c.mu.RLock()
	conn := c.current
	c.mu.RUnlock()

	if conn == nil {
		return nil, nil, ErrNotConnected
	}

	select {
	case ch := <-conn.channels:
		return ch, conn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (conn *connection) release(ch *publishChannel) {
	select {
	case conn.channels <- ch:
	default:
		// The pool is sized to the number of channels, so this cannot fill up;
		// the default arm exists so a mistake cannot deadlock a publish.
	}
}

// Close stops the supervisor and releases the connection.
func (c *Conn) Close(context.Context) error {
	var err error

	c.closeOnce.Do(func() {
		close(c.closed)

		c.mu.Lock()
		conn := c.current
		c.current = nil
		c.mu.Unlock()

		if conn == nil {
			return
		}

		for _, ch := range conn.all {
			_ = ch.close()
		}
		// Closing the connection closes its channels too, so a channel error
		// above is not worth reporting; the connection is what matters.
		err = conn.conn.Close()
	})

	return err
}

// HealthCheck reports whether there is a live connection.
func (c *Conn) HealthCheck(context.Context) error {
	c.mu.RLock()
	conn := c.current
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	if conn.conn.IsClosed() {
		return ErrNotConnected
	}

	return nil
}
