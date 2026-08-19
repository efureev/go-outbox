# Benchmarks

Back to the [README](../README.md).

Every figure here comes from a benchmark in the repository, and every benchmark
is named so you can run it yourself. Nothing below is an estimate.

## How to read these

PostgreSQL and RabbitMQ in Docker on one machine, Go 1.26, Apple M5 Pro,
medians of three runs at the message counts `make bench` uses. **Compare runs on one machine; do not read the absolute
figures as a capacity plan.** A drain benchmark measures this process, a
database and a broker together, on whatever hardware is to hand.

```sh
make up
make bench            # everything
make bench-store      # just the one you want
```

The benchmarks carry the `integration` build tag, so `go test ./...` never sees
them and CI pays nothing for them.

## The dispatcher's own floor

`BenchmarkStore*` — claim and write-back against the database, with the
pipeline, the workers, the event bus and every destination taken out. Whatever
a driver does, every message still costs one share of a claim and one share of a
write-back, and no destination can be faster than this.

| Batch | Claim only | Claim + ack | Claim + nack |
|---|---|---|---|
| 25 | 15 500 msg/s | 6 200 msg/s | 6 600 msg/s |
| 100 | 53 200 msg/s | 25 200 msg/s | 24 200 msg/s |
| 200 | 75 700 msg/s | 38 700 msg/s | 34 700 msg/s |
| 500 | 118 600 msg/s | 63 200 msg/s | 48 800 msg/s |

Three things worth carrying away.

**The write-back costs about as much as the claim.** Acknowledging roughly
halves throughput at every batch size — 47% of the claim-only figure at 100, 53%
at 500. That is the price of the guarantee, a row being `sent` only after the
destination confirmed it, and it is not a tuning knob.

**Batching is the single biggest lever on this path**, and it has not flattened
by 500. Going from a batch of 100 to 500 amortises the round trip over five
times as many rows and moves throughput two and a half times.

**Failing costs more than succeeding, but only at large batches.** At 100 and
below the two are within run-to-run noise of each other; at 500 the nack is
20–30% slower depending on the run. The nack statement classifies each message
and computes a status, an availability time and a deferral marker per row, where
the ack sets four columns. It is worth knowing because the failure path is the
one under load during an outage — precisely when the dispatcher can least afford
to be slow — and it is one reason the circuit breaker earns its place: an
unreachable broker is cheaper to stop claiming for than to keep failing
against.

## Destinations, compared

`BenchmarkDrainDestination` — the same drain, the same dispatcher, four workers,
batches of 200, three destinations.

| Destination | Throughput | Share of the ceiling |
|---|---|---|
| none — the dispatcher and its own table | ~40 700 msg/s | 100% |
| `postgres` — the inbox driver | ~17 700 msg/s | ~44% |
| `rabbitmq` — publisher confirms on | ~6 500 msg/s | ~16% |

The `rabbitmq` row agrees with the changelog's ~7 300 msg/s for the same shape,
and `none` with its ~46 000: same benchmark harness, same defaults.

**A table is roughly three times faster than a broker here**, which is the
measurement behind the claim that a broker is not a mandatory dependency of the
product ([PostgresDestination](PostgresDestination.ru.md)). Delivery is one
`INSERT` for the whole batch against a durable commit, versus a publish and a
confirm per message.

Two honest caveats. The inbox table is in the same database as the outbox, which
is the cheapest arrangement rather than the representative one — a real
deployment puts it in another database and adds a network hop. And both
destinations are containers on the same host, so neither is being handicapped by
a network the other avoids.

What the numbers do **not** say: that a table is a better destination. It has no
fan-out. See the [verdict](PostgresDestination.ru.md).

## The producer's cost

`BenchmarkEnqueue` — one iteration is one business transaction: begin, enqueue
_n_ messages, commit. This is the cost that sits inside a user's request.

Per message, in microseconds:

| Messages per transaction | `outboxclient` batch | `outboxclient` loop | `outboxsql` loop |
|---|---|---|---|
| 1 | 1320 | 1836 | 1668 |
| 10 | 128 | 201 | 204 |
| 100 | 33 | 116 | 116 |
| 500 | 25 | 111 | 109 |

Three arms rather than two, so two effects can be told apart. The gap between
the two loops is the driver; the gap between `batch` and its own loop is pgx's
batch protocol. Both loops go through pgx on the wire — `outboxsql` is opened
with `sql.Open("pgx", …)` — so the comparison isolates the layer, not the
network.

**The batch protocol is the whole difference. The driver is not.** At the same
number of round trips, `database/sql` and pgx are indistinguishable: 116 against
116 microseconds at a hundred messages, 109 against 111 at five hundred. The
spread within a single arm across runs is wider than the gap between the arms.

This corrects the advice that used to be in
[use case 6](usecases/6-database-sql.md). It said a producer writing hundreds of
messages at a time should be on `pkg/outboxclient` and pgx. Measured, being on
pgx buys nothing on its own — what buys something is calling `EnqueueBatch`
there, which is one round trip instead of _n_.

**At a handful of messages the choice does not matter**, as the document
claimed. At one message per transaction all three arms are 1.3–1.8 ms, dominated
by the commit; the client is in the noise. At ten, the difference across a whole
transaction is under a millisecond.

**At hundreds it does.** Five hundred messages cost 55 ms through a loop against
12.5 ms through the batch — 43 ms added to a business transaction that a user is
waiting on.

## Insert to broker

`BenchmarkNotifyLatency` — one message at a time through an otherwise idle
pipeline, which is the latency a producer actually sees.

| | Result |
|---|---|
| Shipped defaults | ~105 ms |
| Debounce and jitter minimised | ~5 ms |

Latency at the defaults is the debounce window plus the mean of the replica
jitter, and both are deliberate: they turn a burst of inserts into a couple of
claims and keep replicas from all waking at the same millisecond. Trading them
away takes delivery to roughly five milliseconds, at the cost of both
properties.

## Tuning, from the sweeps

`BenchmarkDrain`, `BenchmarkDrainBatchSize`, `BenchmarkDrainChannels`.

**Throughput is bounded by the smaller of the worker count and the driver's
channel pool.** Eight workers over the default four channels performs the same
as four workers over four; widening the pool to match moves it by around 60%.
Raise `WORKERS` and `CHANNELS` together or neither.

**Without a destination in the way the dispatcher never becomes the limit**, so
what a broker deployment is tuning is the publish path, not this process. With a
table as the destination that stops being true — see the floor above.
