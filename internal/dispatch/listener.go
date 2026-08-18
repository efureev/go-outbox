package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
)

// Waker is what a listener nudges: one pipeline.
type Waker interface {
	Stream() string
	Wake()
}

// Listener turns PostgreSQL notifications into pipeline wakeups, so a message
// is picked up milliseconds after it is written instead of on the next poll
// tick.
//
// It is a latency optimisation and nothing more. NOTIFY is best-effort and is
// lost when the listening connection drops, so the poll loop stays on as
// reconciliation. Losing a notification costs one poll interval; it never costs
// a message.
type Listener struct {
	pool      *pgxpool.Pool
	cfg       config.DispatchConfig
	pipelines map[string]Waker
	log       *slog.Logger
}

// NewListener wires a listener to the pipelines it can wake.
func NewListener(pool *pgxpool.Pool, cfg config.DispatchConfig, pipelines []Waker, log *slog.Logger) *Listener {
	byStream := make(map[string]Waker, len(pipelines))
	for _, p := range pipelines {
		byStream[p.Stream()] = p
	}

	return &Listener{
		pool:      pool,
		cfg:       cfg,
		pipelines: byStream,
		log:       log.With(slog.String("channel", cfg.NotifyChannel)),
	}
}

const (
	listenerRetryDelay    = time.Second
	listenerMaxRetryDelay = 30 * time.Second
)

// Run listens until ctx is canceled, reconnecting when the connection drops.
func (l *Listener) Run(ctx context.Context) error {
	delay := listenerRetryDelay

	for {
		err := l.listen(ctx)

		// A shutdown is the ordinary way this ends, and the error it produces
		// describes the cancellation rather than a fault.
		if ctx.Err() != nil {
			return nil //nolint:nilerr // the context was canceled; there is nothing to report
		}

		if err != nil {
			l.log.Warn("notification listener dropped, reconnecting",
				slog.Any("error", err), slog.Duration("retry_in", delay))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		if delay < listenerMaxRetryDelay {
			delay *= 2
		}
	}
}

// listen holds one connection and waits on it until it fails.
func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire a connection to listen on: %w", err)
	}
	defer conn.Release()

	// The identifier is validated by config.Validate, which is what makes
	// interpolating it here safe: LISTEN takes no parameters.
	if _, err := conn.Exec(ctx, `LISTEN "`+l.cfg.NotifyChannel+`"`); err != nil {
		return fmt.Errorf("listen on %s: %w", l.cfg.NotifyChannel, err)
	}

	l.log.Info("listening for new messages")

	// Anything written while the connection was down was not announced, so
	// every pipeline gets one nudge on (re)connect rather than waiting for the
	// reconciliation tick.
	l.wakeAll()

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}

		streams := map[string]struct{}{notification.Payload: {}}
		l.coalesce(ctx, conn.Conn().WaitForNotification, streams)
		l.wake(ctx, streams)
	}
}

// pgxWait is the shape of pgx's WaitForNotification, taken as a parameter so
// the coalescing window can be exercised without a database.
type pgxWait func(ctx context.Context) (*pgconn.Notification, error)

// coalesce gathers everything that arrives within the debounce window into one
// wakeup.
//
// A producer inserting a thousand rows in one transaction produces a thousand
// notifications. Claiming once per notification would mean a thousand queries
// to collect what one claim already returns, so the window trades a few
// milliseconds of latency for that.
//
// A timeout here does not harm the connection: pgx closes a connection on any
// receive error except a timeout, and the deadline the context watcher sets is
// cleared when the context is unwatched.
func (l *Listener) coalesce(ctx context.Context, wait pgxWait, streams map[string]struct{}) {
	if l.cfg.NotifyDebounce <= 0 {
		return
	}

	deadline := time.Now().Add(l.cfg.NotifyDebounce)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}

		windowCtx, cancel := context.WithTimeout(ctx, remaining)
		n, err := wait(windowCtx)
		cancel()

		if err != nil {
			// A timeout closes the window; anything else is reported by the
			// next call in the outer loop.
			return
		}

		streams[n.Payload] = struct{}{}
	}
}

// wake nudges the pipelines named by the notifications.
func (l *Listener) wake(ctx context.Context, streams map[string]struct{}) {
	l.sleepJitter(ctx)

	for stream := range streams {
		p, ok := l.pipelines[stream]
		if !ok {
			// A stream nobody dispatches. The row will sit pending, and the
			// backlog metrics are where that shows up.
			continue
		}
		p.Wake()
	}
}

func (l *Listener) wakeAll() {
	for _, p := range l.pipelines {
		p.Wake()
	}
}

// sleepJitter spreads the wakeup across replicas.
//
// Every instance listening on the same channel is woken by the same insert, so
// without this they all claim at the same millisecond. SKIP LOCKED makes the
// losers cheap rather than wrong, but a few milliseconds of spread avoids the
// stampede altogether.
func (l *Listener) sleepJitter(ctx context.Context) {
	if l.cfg.NotifyJitter <= 0 {
		return
	}

	// Spreading replicas over a few milliseconds is not a security decision,
	// so the cheap generator is the right one.
	delay := time.Duration(rand.Int64N(int64(l.cfg.NotifyJitter))) //nolint:gosec // scheduling jitter, not a secret

	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}
