# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-08-19

First release.

A Transactional Outbox dispatcher: a producer writes a message to a database table inside the same
transaction as the business change it describes, and this service reads those rows and publishes
them to RabbitMQ or Kafka. Delivery is at-least-once, a row becomes `sent` only after the broker
acknowledges it, and any number of replicas may run against one table.

### Added

- **Lease ownership on every write.** A claim stamps a row with a token; every statement that
  finalizes a row requires that token to match. Claiming concurrently is straightforward —
  `FOR UPDATE SKIP LOCKED` gives disjoint batches — but recording the outcome is not: without the
  token, a replica whose lease expired mid-flight overwrites the status of a row another replica
  has already reclaimed and delivered, resurrecting a delivered message indefinitely. The
  invariant is enforced by the schema as well as the queries: a row is leased exactly while it is
  processing, and `outbox_lease_conflicts_total` counts the times the check fires.

- **Confirmed publication.** RabbitMQ publishes wait on a per-message deferred confirmation and go
  through a pool of channels, so workers publish concurrently rather than queueing behind one
  channel and one mutex. Kafka writes the whole batch in one `WriteMessages` call and maps the
  positional error slice back onto individual messages, with `acks=all` by default.

- **Permanent failures are distinguished from retryable ones.** An unroutable message, an unknown
  stream, a payload above the broker's limit or a rejected credential fails at once instead of
  spending five attempts and an hour of backoff reaching the same conclusion. Retryable failures
  back off exponentially with a ceiling and jitter, so a broker coming back up is not met by every
  message that failed while it was down, all due at the same instant.

- **`LISTEN`/`NOTIFY` wakeups.** A trigger announces each insert and the relevant pipeline wakes
  within milliseconds. A burst of inserts is coalesced into one wakeup, and a small jitter keeps
  replicas from all claiming at the same millisecond. The poll loop stays on as reconciliation,
  because `NOTIFY` is best-effort: losing one costs a poll interval, never a message. It can be
  disabled entirely where the database role cannot create triggers.

- **One pipeline per stream**, so a broker that is down delays only its own messages. The loop is
  adaptive: a full batch means there is a backlog, so the next iteration starts at once rather
  than sleeping out the poll interval.

- **Graceful drain.** On `SIGTERM` pipelines stop claiming, finish the batch in flight, record its
  outcome, and hand back anything they never started — so another replica takes that work
  immediately instead of waiting out the lease.

- **Housekeeping**: expired leases returned to the queue, backlog gauges sampled, and delivered
  rows swept in bounded chunks after a configurable retention. Each cycle takes a PostgreSQL
  advisory lock, so it runs on one replica per cycle however many are deployed.

- **Dead-letter forwarding.** A message that stops being retried can be forwarded to a destination
  a consumer watches, carrying its original topic, stream, attempt count and permanence as
  headers. The row stays in the table either way: the dead-letter topic is a signal, not the
  record.

- **Metrics on an injected registry**, not on package globals, so a test can build its own and read
  it back. Series for every configured stream and driver are created at startup, so a scrape
  before the first message reports a zero rather than nothing — a distinction an alert expression
  cannot otherwise make. Label values are bounded by the configuration, so a producer cannot mint
  unbounded time series by writing an unknown name into the `stream` column.
  `outbox_oldest_pending_age_seconds` is the metric to hold delivery to.

- **HTTP endpoints** on `net/http`: `/health`, `/ready`, `/api/v1/stats`,
  `/api/v1/messages/failed` for inspecting what stopped and why, and
  `POST /api/v1/messages/requeue` for putting it back. The mutating endpoint is registered only
  when a token is configured to guard it.

- **`outbox.requeue` and `outbox.requeue_failed_before`** as database functions, so an operator in
  psql, the admin endpoint and the CLI all take the same path. Requeueing has to reset the attempt
  counter and the availability time along with the status; a hand-written UPDATE that changes only
  the status leaves a row that is nominally pending and is never selected again.

- **`pkg/outboxclient`**, which takes the caller's transaction as an argument so the transactional
  part is not something to remember, and generates UUIDv7 identifiers so the primary key index
  stays append-ordered.

- **Configuration read and validated before anything connects.** Every duration is parsed at load
  time, and validation reports every problem at once — a misconfigured deployment takes one
  restart to diagnose rather than one per mistake. Driver settings are looked up by exact key from
  a closed set, so a misspelled key is a startup error rather than a setting that silently does
  nothing.

- **An embedded migration runner**: forward-only, one transaction per file, an advisory lock around
  the run so replicas starting together apply each migration exactly once, and a recorded checksum
  per file. The checksum is what makes an edited migration an error rather than a silent
  divergence between a fresh install and an upgraded one.

### Performance

From `make bench` — PostgreSQL and RabbitMQ in Docker on one machine, Go 1.26, Apple M5 Pro,
batches of 200, medians of three runs. Compare runs on one machine rather than reading the absolute
figures as a capacity plan.

| | Result |
|---|---|
| Drain, dispatcher and PostgreSQL only | ~46 000 msg/s |
| Drain via RabbitMQ, 4 workers over 4 channels | ~7 300 msg/s |
| Drain via RabbitMQ, 8 workers over 8 channels | ~12 000 msg/s |
| Insert to broker, shipped defaults | ~105 ms |
| Insert to broker, debounce and jitter minimised | ~5 ms |

Two things the sweeps say that are worth carrying into a deployment.

**Throughput is bounded by the smaller of the worker count and the driver's channel pool.** Eight
workers over the default four channels performs the same as four workers over four; widening the
pool to match moves it by around 60%. Raise `WORKERS` and `CHANNELS` together or neither. Without a
broker in the way the dispatcher itself never becomes the limit, so what is being tuned is the
publish path, not this process.

**Latency at the defaults is the debounce window plus the mean of the replica jitter**, and both
are deliberate: they turn a burst of inserts into a couple of claims and keep replicas from all
waking at the same millisecond. Trading them away takes delivery to roughly five milliseconds, at
the cost of both properties.

### Requirements

- Go 1.26
- PostgreSQL 13 or newer
- RabbitMQ 3.8+ and/or Kafka 2.4+
