//go:build integration

package integration

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/efureev/go-outbox/internal/core"
)

// The database cost of moving one message, with the pipeline, the workers, the
// event bus and every destination taken out.
//
// BenchmarkDrain measures the dispatcher against a destination and cannot say
// which of the two is the limit. This can: whatever a driver does, every message
// still costs one share of a claim and one share of a write-back, and no
// destination can be faster than that.
//
//	go test -tags integration -run '^$' -bench BenchmarkStore -benchtime 20000x ./test/integration/
//
// b.N is the message count. Seeding, migrations and the schema are outside the
// timer.

// leaseFor builds a lease long enough that nothing expires mid-benchmark.
func leaseFor(owner string) core.Lease {
	return core.Lease{Token: uuid.NewString(), Owner: owner, Until: time.Now().Add(time.Hour)}
}

// BenchmarkStoreClaim is the claim alone: FOR UPDATE SKIP LOCKED, the lease
// stamp, and the rows coming back. Nothing is written back, so the messages stay
// leased and each iteration takes fresh ones.
func BenchmarkStoreClaim(b *testing.B) {
	for _, batch := range []int{25, 100, 200, 500} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkStoreClaim(b, batch)
		})
	}
}

func benchmarkStoreClaim(b *testing.B, batch int) {
	b.Helper()

	f := newFixture(b)
	seedBulk(b, f, b.N)

	lease := leaseFor("bench-claim")
	ctx := b.Context()

	b.ResetTimer()

	claimed := 0
	claims := 0

	for claimed < b.N {
		msgs, err := f.Store.Claim(ctx, "local", batch, lease)
		if err != nil {
			b.Fatalf("claim: %v", err)
		}
		if len(msgs) == 0 {
			b.Fatalf("the table ran dry after %d of %d messages", claimed, b.N)
		}

		claimed += len(msgs)
		claims++
	}

	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(claims), "claims")
}

// BenchmarkStoreClaimAck is the whole round trip a delivered message makes
// through the database. The difference from BenchmarkStoreClaim is what the
// write-back costs.
func BenchmarkStoreClaimAck(b *testing.B) {
	for _, batch := range []int{25, 100, 200, 500} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkStoreRoundTrip(b, batch, false)
		})
	}
}

// BenchmarkStoreClaimNack is the same round trip for a message that failed.
//
// It is not a curiosity: the failure path is the one under load during an
// outage, exactly when the dispatcher is least able to afford being slow, and
// its statement is much larger than the ack's — a CTE that classifies each
// message and computes a status, an availability time and a deferral marker per
// row. Whether that costs anything measurable is worth knowing.
func BenchmarkStoreClaimNack(b *testing.B) {
	for _, batch := range []int{25, 100, 200, 500} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkStoreRoundTrip(b, batch, true)
		})
	}
}

func benchmarkStoreRoundTrip(b *testing.B, batch int, fail bool) {
	b.Helper()

	f := newFixture(b)
	seedBulk(b, f, b.N)

	ctx := b.Context()
	limits := core.RetryLimits{MaxAttempts: 100}

	b.ResetTimer()

	done := 0
	claims := 0

	for done < b.N {
		lease := leaseFor("bench-roundtrip")

		msgs, err := f.Store.Claim(ctx, "local", batch, lease)
		if err != nil {
			b.Fatalf("claim: %v", err)
		}
		if len(msgs) == 0 {
			b.Fatalf("the table ran dry after %d of %d messages", done, b.N)
		}

		if fail {
			outcomes := make([]core.Outcome, len(msgs))
			for i, m := range msgs {
				outcomes[i] = core.Outcome{
					ID:    m.ID,
					Err:   errBenchPublish,
					Delay: time.Hour, // Far enough out that nothing is reclaimed mid-run.
				}
			}
			if _, err := f.Store.Nack(ctx, outcomes, lease.Token, limits); err != nil {
				b.Fatalf("nack: %v", err)
			}
		} else {
			ids := make([]string, len(msgs))
			for i, m := range msgs {
				ids[i] = m.ID
			}
			if _, err := f.Store.Ack(ctx, ids, lease.Token); err != nil {
				b.Fatalf("ack: %v", err)
			}
		}

		done += len(msgs)
		claims++
	}

	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(float64(claims), "claims")
}

var errBenchPublish = errors.New("benchmark: the broker refused")
