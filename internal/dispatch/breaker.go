package dispatch

import "time"

// breaker keeps a pipeline from claiming work for a broker that has just proved
// unreachable.
//
// Without it the loop keeps claiming a batch, failing to publish it and writing
// the failure back for the whole duration of an outage — three round-trips to
// PostgreSQL per batch to learn something the connection supervisor already
// knows. Retries alone are self-limiting, because a deferred message is
// rescheduled a backoff into the future; new messages are not. Every insert
// that arrives while a broker is down wakes the pipeline through LISTEN/NOTIFY
// and is claimed, attempted and written back at once. That is the load this
// removes, and it is proportional to how busy the producer is rather than to
// how long the outage lasts.
//
// It is half-open by construction: rather than probing the connection, it lets
// one ordinary claim through after the pause and reads the result. Publishing
// is the capability that matters, and a health check is only a proxy for it —
// one that can be green while the exchange the messages need is not there.
//
// State lives entirely inside Run's goroutine, so it needs no lock. Nothing
// outside observes it directly: what an operator sees comes from the events the
// pipeline emits when it opens and closes.
type breaker struct {
	// first is the initial pause, and the unit the pause doubles from.
	first time.Duration
	// ceiling bounds the pause. Zero disables the breaker entirely, which is
	// what makes it possible to prove it is the thing making a difference.
	ceiling time.Duration

	// until is when the next claim may be attempted. Zero while closed.
	until time.Time
	pause time.Duration
}

func newBreaker(first, ceiling time.Duration) *breaker {
	if first <= 0 {
		first = time.Second
	}

	return &breaker{first: first, ceiling: ceiling}
}

// open reports whether claims are currently being held back.
func (b *breaker) open() bool { return !b.until.IsZero() }

// blocks reports whether this tick should skip claiming altogether.
func (b *breaker) blocks(now time.Time) bool {
	return b.open() && now.Before(b.until)
}

// observe folds the outcome of one cycle into the breaker's state and reports
// whether that changed whether claims are held back.
//
// Three readings, and the third is the one worth spelling out:
//
//   - the broker could not be reached, and nothing got through: hold claims,
//     doubling the pause each time a trial finds it still down;
//   - something got through: the broker is back, so stop holding;
//   - the claim came back empty: nothing was learned about the broker, but
//     nothing is being held back either. Closing here keeps a quiet period from
//     leaving a stale pause in front of the first message that arrives after
//     it — the message would otherwise wait out a ceiling nobody is using.
func (b *breaker) observe(now time.Time, res Result) (changed bool) {
	if b.ceiling <= 0 {
		return false
	}

	was := b.open()

	switch {
	case res.unreachable():
		if b.pause <= 0 {
			b.pause = b.first
		} else if next := b.pause * 2; next <= b.ceiling {
			b.pause = next
		} else {
			b.pause = b.ceiling
		}
		b.until = now.Add(b.pause)
	default:
		b.until, b.pause = time.Time{}, 0
	}

	return was != b.open()
}

// wait is how long until the next claim may be attempted, for logging.
func (b *breaker) wait() time.Duration { return b.pause }
