# Roadmap

[Русская версия](Roadmap.ru.md)

1.3.0 is released. What follows is what I would build next, in the order I would
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
| CLI parity with the admin API | Planned — 1.4 |
| A Grafana dashboard as JSON | Planned — 1.4 |
| Supply-chain hygiene in CI | Planned — 1.4 |
| OpenTelemetry spans | Planned — 1.5 |
| Table partitioning, and a soak target | Planned — 1.6 |
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
why the milestones below start at 1.4. Version numbers on a roadmap are a guess
at grouping, and this one was wrong in a way worth leaving visible.

Details in [the changelog](../CHANGELOG.md) and
[Operations](Operations.md#claiming-stops-while-a-broker-is-down).

---

## 1.4 — finishing the operational round

The two items that made an outage survivable have shipped. What is left of this
round is the part an operator touches: the tools they reach for at three in the
morning, and the evidence that what they are running is what was built.

**CLI parity with the admin API.** `outbox failed`, `outbox requeue`,
`outbox stats` as subcommands, reading the same DSN as the daemon. Requeuing
needs curl and a token today, or psql. The binary is already inside the
container: `docker exec app-outbox-1 outbox failed --stream local` is what
somebody will actually reach for at three in the morning.

**A Grafana dashboard as JSON.** We ship alert rules but no dashboard, so every
adopter builds the same six panels. Backlog, oldest pending age, dispatch rate
per stream, failure rate, reclaims, publish latency quantiles.

**Supply-chain hygiene in CI.** `govulncheck` on every run, an SBOM attached to
the release, cosign signatures on the image and the binaries. Cheap, and it
stops mattering only if nobody else ever runs this.

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

**Cost, stated honestly.** OpenTelemetry adds roughly four modules to a
dependency list currently held at ten direct entries. That is a real price and I
would pay it: tracing is the single most-requested thing from operators, and the
exporter costs nothing when `OUTBOX_OTEL_ENDPOINT` is unset.

---

## 1.6 — volume

**Table partitioning.** Documented in Operations, not shipped. Above roughly ten
million rows a day the retention sweep becomes the bottleneck: a chunked
`DELETE` generates dead tuples faster than autovacuum reclaims them, and the
table stops shrinking. Daily range partitioning by `created_at` turns retention
into `DROP TABLE` — constant time, no vacuum.

The care is in the claim query: it filters on `status`, `stream` and
`available_at`, none of which is the partition key, so it must not be allowed to
degrade into a scan of every live partition. Shipping this means an alternative
migration set, a janitor task that creates partitions ahead and drops them
behind, and a benchmark proving claim latency is unchanged. It is the largest
item here that changes no semantics.

**A soak target.** `make soak` — the resilience scenarios run for an hour under
continuous load rather than for the seconds CI allows. Not a CI job; the thing
you run before trusting a release with someone's money.

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
