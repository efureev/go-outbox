# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **An `outbox.publish` span per message, closing the gap in the producer's trace.** A producer's
  span ends when its transaction commits and a consumer's starts when the broker hands it a
  message; between them is an interval exactly the width of the outbox lag, which a metric can size
  but not explain. The span is parented to the producer's `traceparent` and re-injected into the
  message's headers, so a trace reads producer → `outbox.publish` → consumer, in one trace, with
  the wait visible as the space in front of the middle span.

  A producer that never traced still gets a span and its consumer a header to continue from:
  requiring the producer to have traced first would make this useful only where it was needed
  least.

  Configured by `OUTBOX_OTEL_ENDPOINT` (OTLP/HTTP), with `OUTBOX_OTEL_INSECURE` and
  `OUTBOX_OTEL_SAMPLING`. Sampling defers to the producer's decision when there is one, so a trace
  sampled at the source does not lose its middle here.

### Changed

- **With tracing on, the `traceparent` reaching the broker names the dispatcher's span** rather
  than the producer's. The trace id is unchanged — nothing leaves the producer's trace, only the
  parent moves — which is what puts the dispatcher between the two ends instead of beside them.
  With tracing off, which is the default, the header is passed through untouched exactly as before.

- **The image is 29 MB, up from 21 MB.** The OpenTelemetry SDK and its OTLP encoder add 6.3 MB to a
  15.5 MB binary whether or not a collector is ever configured, and that is the real price of this
  release. What it does not cost is throughput: with no endpoint set the publish loop checks one
  boolean and starts no span, at 0 allocations and about 5 ns per message. A recorded span costs
  about 1.9 µs and 17 allocations. gRPC appears in `go.mod` as an indirect requirement of the OTLP
  proto module, but no package of it is imported and none of it is linked in.


## [1.4.0] — 2026-08-19

The operational round: the tools an operator reaches for, and the evidence that what they are
running is what was built.

### Added

- **`outbox stats`, `outbox failed` and `outbox requeue`** — the admin API's operations as
  subcommands, over the database connection instead of over HTTP. The HTTP route needs a reachable
  pod, a token and a JSON body; what is to hand during an incident is a shell in the container the
  binary already lives in. Both paths run the same store calls, so neither can drift into being the
  one that does it correctly.

  Authorisation differs on purpose. The endpoints are guarded by `OUTBOX_HTTP_ADMIN_TOKEN` because
  anything that can route to the pod can call them; the commands are guarded by holding the database
  credentials, which is a stronger thing to have.

  `outbox failed -stream local`, `outbox requeue <id>...`, `outbox requeue -before <RFC3339>`, and
  `-json` on any of them for a pipe into `jq`.

- **`govulncheck` on every CI run**, and `make vuln` so it is the same command locally. It reports
  only the vulnerabilities the code actually reaches, so a finding is something to act on rather
  than a line in an advisory feed. It runs as its own job: a vulnerable dependency is a fact about
  the module, not a failing test, and should not be discovered by whoever happens to be reading a
  red test run.

- **A CycloneDX SBOM per platform, attached to every release.** Generated from the compiled binaries
  rather than from the source tree, so each one lists what was actually linked into the artefact it
  describes. `make dist SBOM=1` writes them beside the archives, and `make sbom` writes one for a
  local build; the default stays off so a developer's `make dist` needs no extra tool.

- **Keyless cosign signatures on the release and the image.** `SHA256SUMS.cosign.bundle` covers
  every archive and every SBOM through the checksum file, and the image is signed by digest — a tag
  is a name that can be moved, so a signature against one says nothing about what anybody pulls.
  There is no key to distribute or to lose: cosign gets a short-lived certificate against the
  workflow's OIDC identity, and what a verifier establishes is that this repository, on this
  workflow, produced the artefact. Verification commands are in
  [docs/UseCases.md](docs/UseCases.md#install).

  Tool versions are pinned, and cosign's own download is checked against a recorded hash. Fetching a
  signing tool over the network without checking what came back would be an odd way to start
  signing things.

- **A Grafana dashboard**, [`dashboards/outbox.json`](dashboards/outbox.json). Alert rules shipped
  without one, so every adopter built the same panels from the same metric reference. Thirteen
  panels in four rows, with a `stream` variable for narrowing to one broker when only one of them is
  the problem.

  It is checked against the code rather than against a screenshot: a test walks every query in the
  file and fails if it names a metric the dispatcher does not register, or filters on a label that
  metric does not carry. Both render an empty panel, which reads as "nothing is happening" and is
  indistinguishable from good news until somebody needs it.

- **`?stream=` on `GET /api/v1/messages/failed`**, so working through one broker's backlog does not
  mean paging through everybody else's. The CLI needed the filter first; adding it to the endpoint
  too is what keeps parity a fact rather than a claim.

### Changed

- **Administrative commands check only the configuration they use.** `migrate`, `stats`, `failed`
  and `requeue` need a database and nothing else, and previously refused to run on a broken routing
  table. The moment an operator most needs to see what stopped is often the moment the routing table
  is what is wrong, and a tool that answers "your broker is misconfigured" to the question "what
  failed?" is useless precisely then. The dispatcher itself is unchanged: it still refuses to start
  without a routing table, because it cannot deliver without one.

## [1.3.0] — 2026-08-19

### Added

- **The dispatcher stops claiming for a stream whose broker is unreachable.** Finding a broker gone
  used to change nothing about the loop: it kept claiming a batch, failing to publish it and writing
  the failure back for the whole outage.

  What that cost was not the retries. A deferred message is rescheduled a backoff into the future,
  so retrying an outage is self-limiting. New messages are not — every insert arriving while the
  broker is down wakes the pipeline through `LISTEN`/`NOTIFY` and was claimed, attempted and written
  back at once. The load removed is proportional to how busy the producer is rather than to how long
  the outage lasts.

  The pause starts at one poll interval and doubles up to `OUTBOX_DISPATCH_PAUSE_MAX` (`30s`), and
  one ordinary claim is let through each time it elapses. The trial is a real batch rather than a
  health check on purpose: publishing is the capability that matters, and a health check is only a
  proxy for it — one that can be green while the exchange the messages need is not there. A wake-up
  is not allowed past the pause, since it carries no information the breaker does not already have.

  The ceiling matches the delay the RabbitMQ supervisor backs off to between reconnection attempts,
  so pausing adds nothing to how soon a returning broker is noticed. `0` restores the previous
  behaviour, which is also how the tests prove the pause is what makes the difference.

- **`outbox_stream_paused{stream}`**, `1` while a stream has stopped claiming. This is not
  decoration: while claims are held back nothing is published, so `outbox_messages_deferred_total`
  stops advancing precisely when an outage is most established. The gauge is the signal that
  outlives the condition it reports.

### Changed

- The shipped `OutboxBrokerUnreachable` alert now joins `outbox_stream_paused` with the deferral
  rate. On the rate alone it would have cleared itself a minute into every outage it exists to
  report.
- `Pipeline.RunOnce` returns a `dispatch.Result` — claimed, delivered and deferred — instead of a
  bare count, so the run loop can tell an outage from a batch that simply failed.


## [1.2.0] — 2026-08-19

### Added

- **An unreachable broker no longer spends a message's retry budget.** Every failure used to
  advance the attempt counter, which conflated two events deserving opposite responses. A broker
  that looks at a message and refuses it should exhaust a budget — retrying will not change its
  mind. A broker that cannot be reached never saw the message, and charging it for that outage
  spent the budget on somebody else's problem: at the default backoff the whole budget is gone in
  fifteen minutes, so a twenty-minute restart left a table full of `failed` rows that only ever
  needed to wait, and an operator requeueing them by hand.

  Failures to reach a broker are now classified separately. Such a message returns to `pending`
  with its attempt counter untouched, marked with a new `deferred_since` column, and is retried on
  the ordinary backoff until the broker comes back — however long that takes. The attempt counter
  measures rejections, not minutes.

  The classification is deliberately conservative: a per-message problem mistaken for an outage
  would never advance its counter and so never reach `failed`, so anything not positively
  identified as unreachable stays retryable. For RabbitMQ that means the named connection errors
  plus the case that matters most and looks least like an outage — a confirmation deadline expiring
  on a connection that is no longer live. For Kafka it means the availability codes the protocol
  reports (`LeaderNotAvailable`, `NotEnoughReplicas`, and others), network errors, and the write
  timeout, but only while the caller is still running so a shutdown is not recorded as an outage.

- **`OUTBOX_DISPATCH_MAX_DEFER`** bounds how long an unreachable broker may hold a message back
  before it fails anyway, measured from the first deferral rather than from the row's creation — an
  old message meeting its first outage has waited none of it. The default is `0`, meaning
  unbounded, because a message delivered late is worth more than one failed by a timeout. A message
  failed this way is reported as `reason="unreachable"` rather than `attempts_exhausted`: it was
  never rejected, and its attempt counter still reads zero.

- **Two metrics for the condition.** `outbox_messages_deferred_total{stream,driver}` counts
  messages put back without spending an attempt, and the `outbox_messages_deferred` gauge is how
  many are waiting right now. Together with `outbox_oldest_pending_age_seconds` they separate a
  backlog that is moving slowly from one that is not moving at all. `outbox_broker_errors_total`
  gains a `kind="unavailable"` label, and a starting `OutboxBrokerUnreachable` alert ships in
  [docs/MetricsAndAlerts.md](docs/MetricsAndAlerts.md).

### Changed

- `attempts` now counts times a broker rejected a message, not publish attempts made. A message
  that waited out an hour-long outage and then went through records zero attempts.
- `GET /api/v1/stats` reports `messages.deferred` and `settings.max_defer`, and the `ready` line at
  startup carries `max_defer`.
- The `outbox_publish_errors` alert expression filters `kind="retryable"`, which no longer matches
  an outage; `OutboxBrokerUnreachable` covers that case.

### Removed

- `OUTBOX_HTTP_PPROF_TOKEN`. The field was declared in the configuration and read by nothing —
  pprof was never registered — so the variable did nothing whether it was set or not. Behaviour is
  unchanged; the name is simply gone from the configuration surface.

### Database

- Migration `0004_deferral.sql` adds the `deferred_since` column, a partial index over it, and
  replaces the two `requeue` functions so they clear it along with everything else they reset. It
  is additive: existing rows read `NULL`, which is what "nothing is waiting on a broker" means.


## [1.1.0] — 2026-08-19

Fixes found by reviewing the 1.0.0 release, and the tests that now hold them in place.

### Added

- **A breakable TCP proxy in the integration tests**, and five resilience scenarios built on it:
  two of three brokers failing and recovering, PostgreSQL going away, everything going away at
  once, an outage outlasting the retry budget, and a broker unreachable at startup. Severing a
  connection is a truer fault than stopping a container, and it is reversible mid-test.
- **`Endpoint()` on every driver**, reported by `GET /api/v1/stats` with credentials stripped.
  Without it two drivers pointed at different brokers rendered identically, so the response could
  not be used to confirm that a stream reaches the instance it was meant to.
- **The three-RabbitMQ use case** in [docs/UseCases.md](docs/UseCases.md), and a README rewritten
  to lead with what the dispatcher does rather than with how it is built.

### Fixed

- **Colliding driver names are rejected at startup.** `rmq-local` and `rmq_local` normalised to the
  same environment prefix, so one driver's configuration silently overwrote the other's.
- **Every driver is now attempted before configuration fails.** Stopping at the first bad driver
  turned fixing a routing table with several brokers into one restart per mistake; the failures are
  reported together.
- **The status gauges are sampled by every replica.** They were refreshed only by whichever
  instance held the janitor's advisory lock, so the backlog appeared to be zero on every other
  replica — and on all of them whenever the lock holder was between sweeps.
- **`driversOf` returns a stable order**, so the stats response does not reshuffle between scrapes.

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

### Distribution

- **Container image** on `ghcr.io/efureev/go-outbox`, built for `linux/amd64` and
  `linux/arm64` on every tag. The Dockerfile cross-compiles rather than emulating, so a
  multi-platform build needs no QEMU.
- **Prebuilt archives** for Linux and macOS, amd64 and arm64, with `SHA256SUMS`, attached to the
  GitHub release. The release notes are the changelog entry for the version, so the two cannot
  drift.
- **`go install github.com/efureev/go-outbox/cmd/outbox@latest`** for anyone who has Go, though it
  stamps no version.

### Requirements

- Go 1.26
- PostgreSQL 13 or newer
- RabbitMQ 3.8+ and/or Kafka 2.4+
