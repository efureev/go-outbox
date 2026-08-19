//go:build integration

package integration

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/core"
)

func TestClaimLeasesMessages(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 3)

	l := lease("worker-a", time.Minute)

	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d messages, want 3", len(claimed))
	}

	for _, m := range claimed {
		r := f.row(t, m.ID)
		if r.Status != core.StatusProcessing {
			t.Errorf("message %s is %s, want processing", m.ID, r.Status)
		}
		if r.LeaseToken == nil || *r.LeaseToken != l.Token {
			t.Errorf("message %s carries lease %v, want %s", m.ID, r.LeaseToken, l.Token)
		}
		if r.Owner == nil || *r.Owner != "worker-a" {
			t.Errorf("message %s owner = %v, want worker-a", m.ID, r.Owner)
		}
	}
}

func TestClaimRespectsLimitAndStream(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 5)
	f.seed(t, "global", 4)

	claimed, err := f.Store.Claim(t.Context(), "local", 2, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want the limit of 2", len(claimed))
	}
	for _, m := range claimed {
		if m.Stream != "local" {
			t.Errorf("claimed a %s message from the local pipeline", m.Stream)
		}
	}

	// A stream with no configured pipeline of its own is untouched.
	if got := f.countByStatus(t, core.StatusPending); got != 7 {
		t.Errorf("%d messages left pending, want 7", got)
	}
}

func TestClaimSkipsMessagesNotYetDue(t *testing.T) {
	f := newFixture(t)
	id := f.insert(t, "local", "later", []byte(`{}`), nil)

	if _, err := f.Pool.Exec(t.Context(),
		`UPDATE `+quoted(f.Schema)+`.messages SET available_at = now() + interval '1 hour' WHERE id = $1`,
		id); err != nil {
		t.Fatalf("postpone: %v", err)
	}

	claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d messages that are not due yet", len(claimed))
	}
}

// Several replicas claiming at once must take disjoint batches, and between
// them must take every message exactly once.
func TestConcurrentClaimsDoNotOverlap(t *testing.T) {
	f := newFixture(t)

	const (
		total   = 500
		workers = 6
	)
	f.seed(t, "local", total)

	var (
		mu   sync.Mutex
		seen = map[string]string{} // message id -> the worker that claimed it
		wg   sync.WaitGroup
	)

	for w := range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			owner := "worker-" + string(rune('a'+w))
			for {
				claimed, err := f.Store.Claim(t.Context(), "local", 25, lease(owner, time.Minute))
				if err != nil {
					t.Errorf("claim by %s: %v", owner, err)

					return
				}
				if len(claimed) == 0 {
					return
				}

				mu.Lock()
				for _, m := range claimed {
					if prev, dup := seen[m.ID]; dup {
						t.Errorf("message %s claimed twice: by %s and by %s", m.ID, prev, owner)
					}
					seen[m.ID] = owner
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if len(seen) != total {
		t.Errorf("%d of %d messages were claimed; none may be lost", len(seen), total)
	}
	if got := f.countByStatus(t, core.StatusPending); got != 0 {
		t.Errorf("%d messages left pending after every worker drained", got)
	}
}

func TestAckMarksDelivered(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 2)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	res, err := f.Store.Ack(t.Context(), ids(claimed), l.Token)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(res.Delivered) != 2 {
		t.Fatalf("acked %d messages, want 2", len(res.Delivered))
	}
	if res.Conflicts != 0 {
		t.Errorf("Conflicts = %d, want 0", res.Conflicts)
	}
	for _, d := range res.Delivered {
		if d.Lag < 0 {
			t.Errorf("message %s reports a negative delivery lag %s", d.ID, d.Lag)
		}
	}

	for _, m := range claimed {
		r := f.row(t, m.ID)
		if r.Status != core.StatusSent {
			t.Errorf("message %s is %s, want sent", m.ID, r.Status)
		}
		if r.Dispatched == nil {
			t.Errorf("message %s has no dispatched_at", m.ID)
		}
		if r.LeaseToken != nil {
			t.Errorf("message %s still carries a lease after ack", m.ID)
		}
	}
}

// The regression that matters most.
//
// Writing outcomes back with "WHERE id = ANY($ids)" and no ownership check lets
// an instance whose lease expired mid-flight overwrite the status of a row
// another replica has already reclaimed and delivered — resurrecting a
// delivered message and republishing it, indefinitely.
func TestExpiredLeaseCannotOverwriteAnotherInstancesResult(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	// Instance A claims, then stalls for longer than its lease.
	leaseA := lease("instance-a", time.Minute)
	claimedA, err := f.Store.Claim(t.Context(), "local", 10, leaseA)
	if err != nil {
		t.Fatalf("claim by A: %v", err)
	}
	if len(claimedA) != 1 {
		t.Fatalf("A claimed %d messages, want 1", len(claimedA))
	}
	f.expire(t, claimedA[0].ID)

	// The janitor returns the expired lease to the queue.
	reclaimed, err := f.Store.Reclaim(t.Context(), 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].Owner != "instance-a" {
		t.Fatalf("reclaimed %+v, want one lease previously held by instance-a", reclaimed)
	}

	// Instance B claims it and delivers it.
	leaseB := lease("instance-b", time.Minute)
	claimedB, err := f.Store.Claim(t.Context(), "local", 10, leaseB)
	if err != nil {
		t.Fatalf("claim by B: %v", err)
	}
	if len(claimedB) != 1 {
		t.Fatalf("B claimed %d messages, want 1", len(claimedB))
	}
	if _, err := f.Store.Ack(t.Context(), ids(claimedB), leaseB.Token); err != nil {
		t.Fatalf("ack by B: %v", err)
	}

	// Only now does A finish, and report a failure for a message that is
	// already delivered and no longer its own.
	res, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID:    claimedA[0].ID,
		Err:   errors.New("broker refused"),
		Delay: time.Minute,
	}}, leaseA.Token, limits(5))
	if err != nil {
		t.Fatalf("nack by A: %v", err)
	}

	if len(res.Outcomes) != 0 {
		t.Errorf("A's write-back changed %d rows; it holds no lease and must change none", len(res.Outcomes))
	}
	if res.Conflicts != 1 {
		t.Errorf("Conflicts = %d, want 1 so the condition is visible as a metric", res.Conflicts)
	}

	final := f.row(t, claimedA[0].ID)
	if final.Status != core.StatusSent {
		t.Errorf("message ended as %s; B delivered it, so it must stay sent", final.Status)
	}
	if final.Attempts != 0 {
		t.Errorf("attempts = %d; A must not have incremented the counter", final.Attempts)
	}
}

func TestAckWithAStaleTokenChangesNothing(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	res, err := f.Store.Ack(t.Context(), ids(claimed), lease("impostor", time.Minute).Token)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(res.Delivered) != 0 || res.Conflicts != 1 {
		t.Errorf("ack with a foreign token delivered %d and reported %d conflicts, want 0 and 1",
			len(res.Delivered), res.Conflicts)
	}
	if r := f.row(t, claimed[0].ID); r.Status != core.StatusProcessing {
		t.Errorf("message is %s, want it left in processing", r.Status)
	}
}

func TestNackRetriesUntilAttemptsAreExhausted(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	const maxAttempts = 3

	var lastStatus core.Status
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		l := lease("a", time.Minute)
		claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
		if err != nil {
			t.Fatalf("claim on attempt %d: %v", attempt, err)
		}
		if len(claimed) != 1 {
			t.Fatalf("attempt %d claimed %d messages, want 1", attempt, len(claimed))
		}
		if claimed[0].Attempts != attempt-1 {
			t.Errorf("attempt %d sees attempts = %d, want %d", attempt, claimed[0].Attempts, attempt-1)
		}

		// A zero delay keeps the message immediately due, so the loop can run
		// without waiting out a backoff.
		res, err := f.Store.Nack(t.Context(), []core.Outcome{{
			ID: claimed[0].ID, Err: errors.New("broker down"), Delay: 0,
		}}, l.Token, limits(maxAttempts))
		if err != nil {
			t.Fatalf("nack on attempt %d: %v", attempt, err)
		}
		if len(res.Outcomes) != 1 {
			t.Fatalf("attempt %d wrote back %d outcomes, want 1", attempt, len(res.Outcomes))
		}
		lastStatus = res.Outcomes[0].Status
	}

	if lastStatus != core.StatusFailed {
		t.Errorf("after %d attempts the message is %s, want failed", maxAttempts, lastStatus)
	}
}

func TestNackAppliesTheGivenDelay(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	before := time.Now()
	if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID: claimed[0].ID, Err: errors.New("timeout"), Delay: 90 * time.Second,
	}}, l.Token, limits(5)); err != nil {
		t.Fatalf("nack: %v", err)
	}

	r := f.row(t, claimed[0].ID)
	if r.Status != core.StatusPending {
		t.Fatalf("message is %s, want pending for a retry", r.Status)
	}
	if r.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", r.Attempts)
	}
	if r.LastError == nil || *r.LastError != "timeout" {
		t.Errorf("last_error = %v, want the failure text to be recorded", r.LastError)
	}

	// The delay lands roughly where asked; the clocks differ by less than the
	// slack allowed here.
	wantAt := before.Add(90 * time.Second)
	if diff := r.AvailableAt.Sub(wantAt); diff < -10*time.Second || diff > 10*time.Second {
		t.Errorf("available_at is %s off the requested delay", diff)
	}

	// And it is not claimable until then.
	again, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("claimed %d messages that are still in backoff", len(again))
	}
}

// A permanent failure must not spend the remaining attempts on an error that
// retrying cannot fix.
func TestPermanentFailureSkipsTheRetryBudget(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	res, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID:        claimed[0].ID,
		Err:       core.Permanent("unroutable", errors.New("no such exchange")),
		Permanent: true,
	}}, l.Token, limits(5))
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	if len(res.Outcomes) != 1 || res.Outcomes[0].Status != core.StatusFailed {
		t.Fatalf("outcomes = %+v, want a single failed message", res.Outcomes)
	}

	r := f.row(t, claimed[0].ID)
	if r.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: the other four were never spent", r.Attempts)
	}
}

// The regression on the recovery path.
//
// Requeueing has to reset the attempt counter and the availability time along
// with the status. An UPDATE that changes only the status leaves a row that is
// nominally pending, is never selected by the claim query again, and still
// carries an exhausted attempt counter.
func TestRequeueMakesAFailedMessageDeliverableAgain(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	const maxAttempts = 1

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID: claimed[0].ID, Err: errors.New("gave up"),
	}}, l.Token, limits(maxAttempts)); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if r := f.row(t, claimed[0].ID); r.Status != core.StatusFailed {
		t.Fatalf("setup: message is %s, want failed", r.Status)
	}

	requeued, err := f.Store.Requeue(t.Context(), []string{claimed[0].ID})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(requeued) != 1 {
		t.Fatalf("requeued %d messages, want 1", len(requeued))
	}

	r := f.row(t, claimed[0].ID)
	if r.Status != core.StatusPending {
		t.Errorf("message is %s, want pending", r.Status)
	}
	if r.Attempts != 0 {
		t.Errorf("attempts = %d, want the counter reset so the message gets a real second chance", r.Attempts)
	}

	// The part that is easy to get wrong: it has to be claimable again.
	again, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim after requeue: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("claimed %d messages after requeue; a requeued message must be deliverable again", len(again))
	}
}

func TestRequeueIgnoresMessagesThatDidNotFail(t *testing.T) {
	f := newFixture(t)
	seeded := f.seed(t, "local", 1)

	requeued, err := f.Store.Requeue(t.Context(), seeded)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(requeued) != 0 {
		t.Errorf("requeued %d pending messages; only failed ones may be touched", len(requeued))
	}
}

func TestReleaseLeaseReturnsWorkImmediately(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 3)

	l := lease("a", time.Hour)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	released, err := f.Store.ReleaseLease(t.Context(), ids(claimed), l.Token)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 3 {
		t.Fatalf("released %d, want 3", released)
	}

	// Available at once, rather than after the hour-long lease expires.
	again, err := f.Store.Claim(t.Context(), "local", 10, lease("b", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(again) != 3 {
		t.Errorf("another instance claimed %d of the released messages, want 3", len(again))
	}
}

func TestReclaimReportsTheOverdueInterval(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 2)

	l := lease("dead-worker", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	f.expire(t, ids(claimed)...)

	reclaimed, err := f.Store.Reclaim(t.Context(), 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 2 {
		t.Fatalf("reclaimed %d, want 2", len(reclaimed))
	}
	for _, r := range reclaimed {
		if r.Owner != "dead-worker" {
			t.Errorf("reclaimed lease reports owner %q, want dead-worker", r.Owner)
		}
		if r.Overdue <= 0 {
			t.Errorf("overdue = %s, want a positive interval", r.Overdue)
		}
	}

	if got := f.countByStatus(t, core.StatusPending); got != 2 {
		t.Errorf("%d messages pending after reclaim, want 2", got)
	}
}

func TestReclaimLeavesLiveLeasesAlone(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 2)

	if _, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Hour)); err != nil {
		t.Fatalf("claim: %v", err)
	}

	reclaimed, err := f.Store.Reclaim(t.Context(), 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Errorf("reclaimed %d live leases", len(reclaimed))
	}
}

func TestStatsCountsBacklogWithoutTouchingDeliveredRows(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 5)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 2, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.Store.Ack(t.Context(), ids(claimed[:1]), l.Token); err != nil {
		t.Fatalf("ack: %v", err)
	}

	st, err := f.Store.Stats(t.Context())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}

	if st.Pending != 3 {
		t.Errorf("Pending = %d, want 3", st.Pending)
	}
	if st.Processing != 1 {
		t.Errorf("Processing = %d, want 1", st.Processing)
	}
	if st.Failed != 0 {
		t.Errorf("Failed = %d, want 0", st.Failed)
	}
	if st.OldestPending <= 0 {
		t.Errorf("OldestPending = %s, want a positive age", st.OldestPending)
	}
}

func TestPurgeRemovesOnlyDeliveredRowsPastRetention(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 4)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 2, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.Store.Ack(t.Context(), ids(claimed), l.Token); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Nothing is old enough yet.
	removed, err := f.Store.Purge(t.Context(), time.Hour, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 0 {
		t.Errorf("purged %d rows inside the retention window", removed)
	}

	if _, err := f.Pool.Exec(t.Context(),
		`UPDATE `+quoted(f.Schema)+`.messages SET dispatched_at = now() - interval '2 hours' WHERE status = 2`,
	); err != nil {
		t.Fatalf("age the delivered rows: %v", err)
	}

	removed, err = f.Store.Purge(t.Context(), time.Hour, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 2 {
		t.Errorf("purged %d rows, want 2", removed)
	}
	if got := f.countByStatus(t, core.StatusPending); got != 2 {
		t.Errorf("%d pending rows survived, want 2 — retention must not touch undelivered work", got)
	}
}

func TestPurgeHonoursItsBatchLimit(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 6)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 6, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.Store.Ack(t.Context(), ids(claimed), l.Token); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := f.Pool.Exec(t.Context(),
		`UPDATE `+quoted(f.Schema)+`.messages SET dispatched_at = now() - interval '2 hours'`,
	); err != nil {
		t.Fatalf("age rows: %v", err)
	}

	removed, err := f.Store.Purge(t.Context(), time.Hour, 4)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 4 {
		t.Errorf("purged %d rows in one call, want the batch limit of 4", removed)
	}
}

func TestAdvisoryLockKeepsSingletonWorkOnOneInstance(t *testing.T) {
	f := newFixture(t)

	const class, key = 42, 7

	release, ok, err := f.Store.TryLock(t.Context(), class, key)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("the first caller must get the lock")
	}

	_, second, err := f.Store.TryLock(t.Context(), class, key)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if second {
		t.Error("a second caller got the same lock; singleton work would run twice")
	}

	release()

	releaseAgain, third, err := f.Store.TryLock(t.Context(), class, key)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !third {
		t.Error("the lock was not released")
	}
	releaseAgain()
}

// The schema states the ownership rule as an invariant, so a query that forgets
// it cannot leave the table in a state where a row is being processed by nobody.
func TestSchemaRejectsALeaselessProcessingRow(t *testing.T) {
	f := newFixture(t)
	seeded := f.seed(t, "local", 1)

	_, err := f.Pool.Exec(t.Context(),
		`UPDATE `+quoted(f.Schema)+`.messages SET status = 1 WHERE id = $1`, seeded[0])
	if err == nil {
		t.Fatal("a processing row with no lease must violate the check constraint")
	}
}

func quoted(s string) string { return `"` + s + `"` }

// limits builds the ordinary retry bounds: a rejection budget, and no ceiling
// on how long an unreachable broker may hold a message back. Zero is the
// default in production too.
func limits(maxAttempts int) core.RetryLimits {
	return core.RetryLimits{MaxAttempts: maxAttempts}
}

// The rule the whole deferral path exists to enforce. Charging a message an
// attempt for a broker it never reached spends its budget on somebody else's
// outage: at the default backoff the whole budget is gone in fifteen minutes,
// so a longer restart used to leave a table full of failed rows.
func TestDeferralDoesNotSpendAnAttempt(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	res, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID:       claimed[0].ID,
		Err:      core.Unavailable("rabbitmq unreachable", errors.New("no connection")),
		Deferred: true,
		Delay:    90 * time.Second,
	}}, l.Token, limits(1))
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	// MaxAttempts is 1, so an ordinary failure here would be terminal.
	if len(res.Outcomes) != 1 || res.Outcomes[0].Status != core.StatusPending {
		t.Fatalf("outcomes = %+v, want a single pending message", res.Outcomes)
	}
	if !res.Outcomes[0].Deferred {
		t.Error("the write-back does not report the message as deferred")
	}

	r := f.row(t, claimed[0].ID)
	if r.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", r.Attempts)
	}
	if r.DeferredSince == nil {
		t.Error("deferred_since was not recorded, so no window can ever run out")
	}
	if !r.AvailableAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("available_at = %s, want the given delay applied", r.AvailableAt)
	}
	if r.LastError == nil || *r.LastError == "" {
		t.Error("the reason for the wait was not recorded")
	}
}

// The window bounds a continuous outage, not the lifetime of the row. Restamping
// it on every deferral would mean it never elapses; taking it from created_at
// would fail an old message on its first deferral, having waited none of it.
func TestTheDeferralWindowStartsAtTheFirstDeferral(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	var first time.Time
	for i := range 2 {
		l := lease("a", time.Minute)
		claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if len(claimed) != 1 {
			t.Fatalf("claim %d returned %d messages, want 1", i, len(claimed))
		}

		if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
			ID: claimed[0].ID, Err: errors.New("broker gone"), Deferred: true, Delay: 0,
		}}, l.Token, limits(5)); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}

		r := f.row(t, claimed[0].ID)
		if r.DeferredSince == nil {
			t.Fatalf("deferral %d recorded no marker", i)
		}
		if i == 0 {
			first = *r.DeferredSince

			continue
		}
		if !r.DeferredSince.Equal(first) {
			t.Errorf("the window restarted on the second deferral (%s then %s), so it would never elapse",
				first, *r.DeferredSince)
		}
	}
}

// With a window configured, a broker that never comes back does eventually end
// the wait — and the row stops claiming to be deferred once it has.
func TestDeferralBeyondTheWindowFails(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 1)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	unreachable := core.Outcome{
		ID: claimed[0].ID, Err: errors.New("broker gone"), Deferred: true, Delay: 0,
	}
	bounded := core.RetryLimits{MaxAttempts: 5, MaxDefer: time.Hour}

	if _, err := f.Store.Nack(t.Context(), []core.Outcome{unreachable}, l.Token, bounded); err != nil {
		t.Fatalf("first nack: %v", err)
	}
	// Stand in for an outage that has been running for two hours.
	if _, err := f.Pool.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %q.messages SET deferred_since = now() - interval '2 hours' WHERE id = $1`, f.Schema),
		claimed[0].ID); err != nil {
		t.Fatalf("age the deferral: %v", err)
	}

	l2 := lease("a", time.Minute)
	again, err := f.Store.Claim(t.Context(), "local", 10, l2)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	unreachable.ID = again[0].ID

	res, err := f.Store.Nack(t.Context(), []core.Outcome{unreachable}, l2.Token, bounded)
	if err != nil {
		t.Fatalf("second nack: %v", err)
	}

	if len(res.Outcomes) != 1 || res.Outcomes[0].Status != core.StatusFailed {
		t.Fatalf("outcomes = %+v, want a single failed message", res.Outcomes)
	}

	r := f.row(t, claimed[0].ID)
	if r.DeferredSince != nil {
		t.Error("a failed message still claims to be waiting on a broker")
	}
	if r.Attempts != 0 {
		t.Errorf("attempts = %d, want 0: it was never rejected, only never reached", r.Attempts)
	}
}

// Every path that ends the wait has to clear the marker. One that outlives the
// outage it describes is worse than none: the next outage inherits a window
// that has already elapsed and fails the message on its first deferral.
func TestEndingTheWaitClearsTheDeferralMarker(t *testing.T) {
	deferOnce := func(t *testing.T, f *fixture) string {
		t.Helper()

		l := lease("a", time.Minute)
		claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
			ID: claimed[0].ID, Err: errors.New("broker gone"), Deferred: true, Delay: 0,
		}}, l.Token, limits(5)); err != nil {
			t.Fatalf("defer: %v", err)
		}
		if f.row(t, claimed[0].ID).DeferredSince == nil {
			t.Fatal("setup: nothing was deferred, so this proves nothing")
		}

		return claimed[0].ID
	}

	t.Run("delivery", func(t *testing.T) {
		f := newFixture(t)
		f.seed(t, "local", 1)
		id := deferOnce(t, f)

		l := lease("a", time.Minute)
		claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if _, err := f.Store.Ack(t.Context(), ids(claimed), l.Token); err != nil {
			t.Fatalf("ack: %v", err)
		}

		if f.row(t, id).DeferredSince != nil {
			t.Error("a delivered message still carries a deferral marker")
		}
	})

	t.Run("a reason the broker actually gave", func(t *testing.T) {
		f := newFixture(t)
		f.seed(t, "local", 1)
		id := deferOnce(t, f)

		l := lease("a", time.Minute)
		claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
			ID: claimed[0].ID, Err: errors.New("broker nacked"), Delay: 0,
		}}, l.Token, limits(5)); err != nil {
			t.Fatalf("nack: %v", err)
		}

		r := f.row(t, id)
		if r.DeferredSince != nil {
			t.Error("the message is no longer waiting on the broker, yet still says it is")
		}
		if r.Attempts != 1 {
			t.Errorf("attempts = %d, want 1: this failure was a rejection", r.Attempts)
		}
	})
}

// The gauge that separates a backlog which is moving slowly from one that is
// not moving at all.
func TestStatsCountsWhatIsWaitingOnABroker(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 3)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if _, err := f.Store.Nack(t.Context(), []core.Outcome{{
		ID: claimed[0].ID, Err: errors.New("broker gone"), Deferred: true, Delay: 0,
	}}, l.Token, limits(5)); err != nil {
		t.Fatalf("nack: %v", err)
	}

	st, err := f.Store.Stats(t.Context())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Deferred != 1 {
		t.Errorf("Deferred = %d, want 1", st.Deferred)
	}
}
