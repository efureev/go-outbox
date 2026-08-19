# Roadmap

[Русская версия](Roadmap.ru.md)

1.4.0 is released. What follows is what I would build next, in the order I would
build it — with the reasoning for the *order*, not just the ordering. Each item
states what it changes, why it earns its place, and what it costs.

This is a proposal, not a commitment. Dates are deliberately absent: the
sequence is the useful part, and the version numbers below say where an item
would land rather than when.

## Status

| Item | Status |
|---|---|
| An outage must not spend the retry budget | **Shipped in 1.2.0** |
| Stop claiming for a stream whose broker is unreachable | **Shipped in 1.3.0** |
| CLI parity with the admin API | **Shipped in 1.4.0** |
| A Grafana dashboard as JSON | **Shipped in 1.4.0** |
| Supply-chain hygiene in CI | **Shipped in 1.4.0** |
| OpenTelemetry spans | **Done**, not yet released |
| Table partitioning, and a soak target | **Done**, not yet released |
| NATS JetStream, Redis Streams, a `database/sql` client | Planned — 1.7 |
| Per-key ordering | Planned — 2.0 |

What is deliberately **not** on the list, and the reasoning for each refusal, is
in [Not planned, and why](#not-planned-and-why).

---

## Shipped

**An outage no longer spends the retry budget** — 1.2.0. Every failure used to
advance the attempt counter, so at the default backoff a twenty-minute broker
restart moved every message in flight to `failed` and asked an operator to
requeue them by hand — although the broker never saw one of them. Failures to
reach a broker are now classified apart from failures the broker reported. Such
a message returns to `pending` with its attempt counter untouched and waits out
the outage; `OUTBOX_DISPATCH_MAX_DEFER` bounds the wait for streams that would
rather fail than be late. The attempt counter measures rejections, not minutes.

**Claiming stops while a broker is down** — 1.3.0. The saving is not in the
retries: a deferred message is already rescheduled a backoff into the future, so
retrying an outage is self-limiting. It is in the arrivals — every insert during
an outage used to wake the pipeline through `LISTEN`/`NOTIFY` and become a
failed publish at once.

Two notes on how these landed, because both differ from what this page
predicted.

The breaker reads the result of an ordinary claim rather than the connection
supervisor's state, as sketched. Publishing is the capability that matters, and
a health check is only a proxy for it — one that can be green while the exchange
the messages need is not there.

And the two shipped as 1.2.0 and 1.3.0 rather than inside one release, which is
why the milestones below start at 1.5. Version numbers on a roadmap are a guess
at grouping, and that one was wrong in a way worth leaving visible.

**The operational round** — 1.4.0. Three items an operator touches, rather than
anything the dispatcher does differently.

- `outbox stats`, `outbox failed` and `outbox requeue` reach the database
  directly, so requeueing no longer needs curl, a token and a reachable pod.
  They also check less of the configuration than the dispatcher does, which was
  the part worth getting right: the moment you need to see what stopped is often
  the moment the routing table is what is wrong.
- [`dashboards/outbox.json`](../dashboards/outbox.json), a Grafana dashboard
  checked against the metric set by a test, so a renamed metric cannot leave an
  empty panel behind — and an empty panel reads as good news.
- `govulncheck` on every CI run, a CycloneDX SBOM per platform attached to each
  release, and keyless cosign signatures on the checksums and on the image by
  digest.

Details in [the changelog](../CHANGELOG.md) and
[Operations](Operations.md#claiming-stops-while-a-broker-is-down).

---

## 1.5 — closing the hole in the trace

The producer's span ends at commit. The consumer's span starts at receive.
Between them is a gap exactly the width of the outbox lag — the one interval
nobody can currently see, and the first thing anyone asks about when a message
arrives late.

The dispatcher already carries `traceparent` through headers and emits no span
of its own. Adding `outbox.publish`, linked to the producer's context, closes
the hole and makes lag attributable to a stage: waiting to be claimed, waiting
for the broker, or waiting on a retry.

**Done, not yet released**, and the cost was worse than this page guessed. It
estimated "roughly four modules"; the measured price is four direct
dependencies, sixteen indirect, and **6.3 MB on a 15.5 MB binary** — 41% more
image, paid whether or not a collector is ever configured. The throughput
estimate held: with no endpoint set the publish loop starts no span, at zero
allocations.

Worth it, on the same reasoning as before — tracing is the most-requested thing
from operators — but worth recording that the number was wrong, because the next
estimate on this page is written by the same hand.

---

## 1.6 — volume

**Done, not yet released**, and one claim on this page was wrong.

Partitioning ships as an opt-in schema rather than an alternative migration set:
apply [`migrations/partitioned/messages.sql`](../migrations/partitioned/messages.sql)
first and the released migrations run over it unchanged. The dispatcher notices
the shape of the table and drops partitions instead of deleting rows.

This page called it "the largest item here that changes no semantics". It does
change one. PostgreSQL requires a unique constraint on a partitioned table to
include the partition key, so `id` alone cannot be the primary key and becomes
`(id, created_at)` — the database stops enforcing that an id appears once across
the whole table. Nothing breaks, because consumers deduplicate on the message id
under at-least-once delivery, but it is a guarantee given up.

The claim query held up: about 0.25 ms across 31 daily partitions against
0.18 ms unpartitioned, because the partial indexes on older partitions are empty
and merging across them costs almost nothing.

`make soak` runs the resilience scenarios under load for as long as it is told
to, behind its own build tag.

Details in [the changelog](../CHANGELOG.md) and
[Operations](Operations.md#partitioning-past-roughly-ten-million-rows-a-day).

---

## 1.7 — more destinations

`broker.Publisher` exists precisely so that this is cheap, and a driver is the
clearest way to prove the interface was worth having.

**NATS JetStream** first: its publish-ack maps directly onto the confirmed
publication the dispatcher already requires, so the driver is about three
hundred lines plus integration tests. **Redis Streams** next — `XADD` is
inherently acknowledged. **SQS/SNS** after that, if anyone asks.

**A `database/sql` producer client.** `pkg/outboxclient` is pgx-only, which
excludes every team on `sqlx`, `gorm` or the standard library. The insert is six
columns; the client is thin. It widens adoption for an afternoon of work.

---

## 2.0 — ordering

The one guarantee the dispatcher explicitly declines to make
([README](../README.md), [PublicContract](PublicContract.md)). For a great many
domains that is fine. For the rest — `order.created` must not overtake
`order.updated` for the same order — it is disqualifying, and no amount of
tuning elsewhere compensates.

**The shape.** Opt-in per stream. A real `key` column, not a JSON path, with its
own index. The claim refuses a message whose key still has an unfinished
predecessor:

```sql
AND NOT EXISTS (
    SELECT 1 FROM messages e
     WHERE e.key = m.key
       AND e.status IN (0, 1)
       AND (e.available_at, e.id) < (m.available_at, m.id))
```

Parallelism moves from *within* a key to *across* keys: one message in flight
per key, as many keys concurrently as there are workers. For most workloads that
costs little, because most workloads have many keys.

**The part that needs to be said out loud.** With ordering on, a failed message
blocks its key permanently. That is precisely the semantics being asked for — a
gap in an ordered stream is worse than a stall — but it demands an explicit
"skip the head" operation, a `outbox_blocked_keys` gauge, and an alert. Ordering
without those is a trap.

A new column and a changed claim query make this a major version.

---

## Not planned, and why

Saying no is part of a roadmap.

**Other databases.** The design leans on `FOR UPDATE SKIP LOCKED`,
`LISTEN`/`NOTIFY`, partial indexes and advisory locks. MySQL 8 has the first and
none of the rest. A port would fork `internal/store` and halve the confidence in
both branches. PostgreSQL is the home.

**A web UI.** The stats and failed endpoints plus Grafana already cover the
question. A UI is a permanent maintenance burden and a permanent authentication
surface bolted onto a sidecar whose whole appeal is that it is boring.

**Routing or transformation rules.** That is what exchanges and stream
processors are for. An outbox that rewrites payloads is an ESB with extra steps.

**Exactly-once.** Not honestly deliverable across a network. At-least-once plus
a documented deduplication key is the truthful contract, and truthful beats
impressive.

**Consuming.** This is a dispatcher. A consumer framework is a different
product with a different shape.

---

## How to read the order

Items are sequenced by value divided by cost, with one override: anything that
can lose data or manufacture a 3 a.m. page comes first regardless of how
interesting it is. That is why an error classification precedes distributed
tracing, and why ordering — the most requested feature on the page — comes last.
It is the most expensive, and the least likely to hurt anyone by being absent,
because its absence is documented.
