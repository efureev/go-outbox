package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// fakeStore records what a pipeline asks of the database.
type fakeStore struct {
	mu sync.Mutex

	batches [][]core.Message

	claims   []core.Lease
	acked    [][]string
	nacked   [][]core.Outcome
	released [][]string

	ackResult  store.AckResult
	nackResult store.NackResult
	claimErr   error
	limits     core.RetryLimits
}

func (f *fakeStore) Claim(_ context.Context, _ string, _ int, lease core.Lease) ([]core.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.claimErr != nil {
		return nil, f.claimErr
	}

	f.claims = append(f.claims, lease)

	if len(f.batches) == 0 {
		return nil, nil
	}

	batch := f.batches[0]
	f.batches = f.batches[1:]

	return batch, nil
}

func (f *fakeStore) Ack(_ context.Context, ids []string, _ string) (store.AckResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.acked = append(f.acked, ids)

	if len(f.ackResult.Delivered) > 0 || f.ackResult.Conflicts > 0 {
		return f.ackResult, nil
	}

	res := store.AckResult{}
	for _, id := range ids {
		res.Delivered = append(res.Delivered, store.Delivered{ID: id, Lag: time.Second})
	}

	return res, nil
}

func (f *fakeStore) Nack(
	_ context.Context,
	outcomes []core.Outcome,
	_ string,
	limits core.RetryLimits,
) (store.NackResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nacked = append(f.nacked, outcomes)
	f.limits = limits

	if len(f.nackResult.Outcomes) > 0 || f.nackResult.Conflicts > 0 {
		return f.nackResult, nil
	}

	// A stand-in for what the SQL does: permanence fails at once, a deferral
	// goes back untouched, anything else advances the counter.
	res := store.NackResult{}
	for _, o := range outcomes {
		out := store.NackOutcome{ID: o.ID, Status: core.StatusPending, Attempts: 1}
		switch {
		case o.Permanent:
			out.Status = core.StatusFailed
		case o.Deferred:
			out.Attempts, out.Deferred = 0, true
		}
		res.Outcomes = append(res.Outcomes, out)
	}

	return res, nil
}

func (f *fakeStore) ReleaseLease(_ context.Context, ids []string, _ string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.released = append(f.released, ids)

	return len(ids), nil
}

// fakeRouter answers with a scripted result per message id.
type fakeRouter struct {
	mu      sync.Mutex
	errFor  map[string]error
	batches [][]string
	block   chan struct{}
}

func (r *fakeRouter) Publish(ctx context.Context, _ string, msgs []core.Message) []error {
	if r.block != nil {
		select {
		case <-r.block:
		case <-ctx.Done():
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make([]string, len(msgs))
	out := make([]error, len(msgs))

	for i, m := range msgs {
		seen[i] = m.ID
		out[i] = r.errFor[m.ID]
	}
	r.batches = append(r.batches, seen)

	return out
}

func (r *fakeRouter) DriverFor(string) (string, bool) { return "test-driver", true }

// recorder captures the events a pipeline emits.
type recorder struct {
	mu         sync.Mutex
	iterations []events.Iteration
	breakers   []events.Breaker
}

func (r *recorder) Iteration(_ context.Context, ev events.Iteration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.iterations = append(r.iterations, ev)
}

func (r *recorder) Breaker(_ context.Context, ev events.Breaker) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.breakers = append(r.breakers, ev)
}

func (r *recorder) last(t *testing.T) events.Iteration {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.iterations) == 0 {
		t.Fatal("no iteration was emitted")
	}

	return r.iterations[len(r.iterations)-1]
}

func testConfig() config.Config {
	cfg := config.Config{}
	cfg.App.Instance = "test-instance"
	cfg.Dispatch = config.DispatchConfig{
		PollInterval:     10 * time.Millisecond,
		BatchSize:        10,
		Workers:          3,
		LeaseTTL:         time.Minute,
		MaxAttempts:      5,
		BackoffBase:      time.Minute,
		BackoffMax:       time.Hour,
		PublishTimeout:   5 * time.Second,
		WriteBackTimeout: 5 * time.Second,
	}

	return cfg
}

func batch(n int, attempts int) []core.Message {
	msgs := make([]core.Message, n)
	for i := range msgs {
		msgs[i] = core.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Stream:    "local",
			Topic:     "t",
			Payload:   []byte("{}"),
			Attempts:  attempts,
			CreatedAt: time.Now(),
		}
	}

	return msgs
}

func newPipeline(st Store, router Router, rec Emitter) *Pipeline {
	return New("local", st, router, rec, testConfig(), logging.Nop())
}

func TestRunOnceAcksEveryDeliveredMessage(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(5, 0)}}
	rec := &recorder{}

	p := newPipeline(st, &fakeRouter{errFor: map[string]error{}}, rec)

	res, err := p.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Claimed != 5 {
		t.Fatalf("claimed = %d, want 5", res.Claimed)
	}

	if len(st.acked) != 1 || len(st.acked[0]) != 5 {
		t.Fatalf("acked = %v, want one call with five ids", st.acked)
	}
	if len(st.nacked) != 0 {
		t.Errorf("nothing failed, yet Nack was called with %v", st.nacked)
	}

	ev := rec.last(t)
	if len(ev.Delivered) != 5 {
		t.Errorf("event reports %d delivered, want 5", len(ev.Delivered))
	}
	if ev.Driver != "test-driver" {
		t.Errorf("event driver = %q, want test-driver", ev.Driver)
	}
}

// Every claim carries a token of its own, and it is that token — not the set of
// ids — that authorises the write-back.
func TestEachClaimMintsItsOwnLease(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(1, 0), batch(1, 0)}}
	p := newPipeline(st, &fakeRouter{errFor: map[string]error{}}, &recorder{})

	for range 2 {
		if _, err := p.RunOnce(t.Context()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
	}

	if len(st.claims) != 2 {
		t.Fatalf("%d claims recorded, want 2", len(st.claims))
	}
	if st.claims[0].Token == st.claims[1].Token {
		t.Error("two claims share a lease token; a stale write-back would then be accepted as current")
	}
	for _, l := range st.claims {
		if l.Owner != "test-instance" {
			t.Errorf("lease owner = %q, want the instance name so an operator can see who holds it", l.Owner)
		}
		if !l.Until.After(time.Now()) {
			t.Error("lease expires in the past")
		}
	}
}

func TestFailedMessagesAreNackedWithABackoff(t *testing.T) {
	msgs := batch(3, 2)
	st := &fakeStore{batches: [][]core.Message{msgs}}

	router := &fakeRouter{errFor: map[string]error{
		"msg-1": errors.New("broker unavailable"),
	}}

	p := newPipeline(st, router, &recorder{})

	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(st.acked) != 1 || len(st.acked[0]) != 2 {
		t.Errorf("acked = %v, want the two that succeeded", st.acked)
	}
	if len(st.nacked) != 1 || len(st.nacked[0]) != 1 {
		t.Fatalf("nacked = %v, want the one that failed", st.nacked)
	}

	got := st.nacked[0][0]
	if got.ID != "msg-1" {
		t.Errorf("nacked %q, want msg-1", got.ID)
	}
	if got.Permanent {
		t.Error("an ordinary broker error must stay retryable")
	}
	// Three attempts made, so the delay is the third step: 4 x base.
	if want := 4 * time.Minute; got.Delay != want {
		t.Errorf("delay = %s, want %s for a message on its third attempt", got.Delay, want)
	}
}

func TestPermanentFailuresAreMarkedAsSuch(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(1, 0)}}
	router := &fakeRouter{errFor: map[string]error{
		"msg-0": core.Permanent("unroutable", errors.New("no such exchange")),
	}}
	rec := &recorder{}

	p := newPipeline(st, router, rec)

	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(st.nacked) != 1 || !st.nacked[0][0].Permanent {
		t.Fatalf("nacked = %+v, want the outcome marked permanent", st.nacked)
	}

	ev := rec.last(t)
	if len(ev.Failed) != 1 || !ev.Failed[0].Permanent {
		t.Errorf("event failures = %+v, want one marked permanent", ev.Failed)
	}
}

// The whole point of the ownership check is that it can fire; when it does, it
// has to reach the metric rather than disappear.
func TestLeaseConflictsAreReported(t *testing.T) {
	st := &fakeStore{
		batches:   [][]core.Message{batch(2, 0)},
		ackResult: store.AckResult{Conflicts: 2},
	}
	rec := &recorder{}

	p := newPipeline(st, &fakeRouter{errFor: map[string]error{}}, rec)

	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if got := rec.last(t).Conflicts; got != 2 {
		t.Errorf("event reports %d conflicts, want 2", got)
	}
}

// A shutdown that lands mid-batch must hand the untried tail back, so another
// replica takes it now rather than after the lease expires.
func TestShutdownReleasesUnattemptedClaims(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(9, 0)}}

	blocked := make(chan struct{})
	router := &fakeRouter{errFor: map[string]error{}, block: blocked}

	p := newPipeline(st, router, &recorder{})

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		defer close(done)

		_, _ = p.RunOnce(ctx)
	}()

	// Let the first chunks start, then shut down and let them finish.
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(blocked)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not return after cancellation")
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	// Whatever was not attempted must have been handed back rather than left
	// leased.
	attempted := 0
	for _, ids := range st.acked {
		attempted += len(ids)
	}
	for _, outcomes := range st.nacked {
		attempted += len(outcomes)
	}
	releasedCount := 0
	for _, ids := range st.released {
		releasedCount += len(ids)
	}

	if attempted+releasedCount != 9 {
		t.Errorf("%d messages accounted for (%d written back, %d released), want all 9",
			attempted+releasedCount, attempted, releasedCount)
	}
}

// A full batch means there is a backlog. Sleeping through the poll interval
// anyway would cap throughput at one batch per interval.
func TestRunLoopDoesNotSleepOnAFullBatch(t *testing.T) {
	full := batch(10, 0) // BatchSize is 10
	st := &fakeStore{batches: [][]core.Message{full, full, full}}

	cfg := testConfig()
	cfg.Dispatch.PollInterval = time.Hour // any sleep would hang the test

	p := New("local", st, &fakeRouter{errFor: map[string]error{}}, &recorder{}, cfg, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		st.mu.Lock()
		drained := len(st.batches) == 0
		st.mu.Unlock()

		if drained {
			break
		}

		select {
		case <-deadline:
			t.Fatal("the loop waited for a tick between full batches instead of continuing")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

func TestWakeInterruptsTheIdleWait(t *testing.T) {
	st := &fakeStore{}

	cfg := testConfig()
	cfg.Dispatch.PollInterval = time.Hour

	p := New("local", st, &fakeRouter{errFor: map[string]error{}}, &recorder{}, cfg, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// One claim happens immediately; the loop then waits for an hour unless
	// woken.
	time.Sleep(20 * time.Millisecond)

	st.mu.Lock()
	before := len(st.claims)
	st.mu.Unlock()

	p.Wake()

	deadline := time.After(5 * time.Second)
	for {
		st.mu.Lock()
		after := len(st.claims)
		st.mu.Unlock()

		if after > before {
			break
		}

		select {
		case <-deadline:
			t.Fatal("Wake did not bring the loop out of its idle wait")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

func TestWakeNeverBlocks(t *testing.T) {
	p := newPipeline(&fakeStore{}, &fakeRouter{errFor: map[string]error{}}, &recorder{})

	done := make(chan struct{})
	go func() {
		defer close(done)

		// Far more signals than the channel can hold; the notification listener
		// must never be blocked by a busy pipeline.
		for range 1000 {
			p.Wake()
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wake blocked")
	}
}

func TestRunOnceReportsClaimFailures(t *testing.T) {
	st := &fakeStore{claimErr: errors.New("connection refused")}

	p := newPipeline(st, &fakeRouter{errFor: map[string]error{}}, &recorder{})

	if _, err := p.RunOnce(t.Context()); err == nil {
		t.Fatal("a failing claim must be reported")
	}
}

func TestEmptyClaimEmitsNothing(t *testing.T) {
	rec := &recorder{}

	p := newPipeline(&fakeStore{}, &fakeRouter{errFor: map[string]error{}}, rec)

	res, err := p.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Claimed != 0 {
		t.Errorf("claimed = %d, want 0", res.Claimed)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.iterations) != 0 {
		t.Errorf("an idle iteration emitted %d events; nothing happened", len(rec.iterations))
	}
}

// An unreachable broker must not be charged to the message. Without this the
// default backoff spends the whole attempt budget in fifteen minutes, so a
// longer restart moves every message in flight to failed even though the broker
// never saw one of them.
func TestUnreachableBrokerDefersInsteadOfFailing(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(3, 0)}}
	rec := &recorder{}

	down := core.Unavailable("rabbitmq unreachable", errors.New("no connection"))
	router := &fakeRouter{errFor: map[string]error{"msg-0": down, "msg-1": down, "msg-2": down}}

	if _, err := newPipeline(st, router, rec).RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(st.nacked) != 1 {
		t.Fatalf("Nack calls = %d, want 1", len(st.nacked))
	}
	for _, o := range st.nacked[0] {
		if !o.Deferred {
			t.Errorf("%s was not marked deferred, so it would spend an attempt", o.ID)
		}
		if o.Permanent {
			t.Errorf("%s was marked permanent", o.ID)
		}
		if o.Delay <= 0 {
			t.Errorf("%s was rescheduled with no delay, so the outage would be retried in a tight loop", o.ID)
		}
	}

	ev := rec.last(t)
	if len(ev.Deferred) != 3 {
		t.Errorf("event reports %d deferred, want 3", len(ev.Deferred))
	}
	// Requeued and Deferred both mean "back to pending", and separating them is
	// the difference between an alert that fires on a broker outage and one that
	// fires on messages the broker is rejecting.
	if len(ev.Requeued) != 0 {
		t.Errorf("deferred messages were also reported as retried: %v", ev.Requeued)
	}
	if len(ev.Failed) != 0 {
		t.Errorf("an outage produced failures: %v", ev.Failed)
	}
	for _, pub := range ev.Publishes {
		if !pub.Deferred {
			t.Errorf("publish result for %s does not carry the classification", pub.ID)
		}
	}
}

// A broker that answers and refuses still exhausts the budget: that is what the
// counter is for, and a deferral that swallowed rejections would mean nothing
// ever reached failed.
func TestRejectionsStillSpendAttempts(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(1, 0)}}
	rec := &recorder{}

	router := &fakeRouter{errFor: map[string]error{"msg-0": errors.New("broker nacked")}}

	if _, err := newPipeline(st, router, rec).RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if st.nacked[0][0].Deferred {
		t.Error("a rejection was recorded as a deferral")
	}
	if ev := rec.last(t); len(ev.Requeued) != 1 || len(ev.Deferred) != 0 {
		t.Errorf("requeued = %v, deferred = %v; want one retry and no deferral", ev.Requeued, ev.Deferred)
	}
}

// Both bounds travel with the write-back. The store cannot read the
// configuration, so a limit left behind here silently reverts to zero — which
// for MaxAttempts means every message fails on its first error.
func TestWriteBackCarriesBothRetryLimits(t *testing.T) {
	st := &fakeStore{batches: [][]core.Message{batch(1, 0)}}

	cfg := testConfig()
	cfg.Dispatch.MaxDefer = 90 * time.Minute
	p := New("local", st, &fakeRouter{errFor: map[string]error{"msg-0": errors.New("nope")}},
		&recorder{}, cfg, logging.Nop())

	if _, err := p.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	want := core.RetryLimits{MaxAttempts: cfg.Dispatch.MaxAttempts, MaxDefer: 90 * time.Minute}
	if st.limits != want {
		t.Errorf("limits = %+v, want %+v", st.limits, want)
	}
}
