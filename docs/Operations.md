# Operations

English | [Русский](Operations.ru.md)

## Deploying

The dispatcher is a sidecar: it uses the producer's database and the producer's
broker, and brings neither of its own.

```yaml
# A Kubernetes deployment, in outline.
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: outbox
          image: go-outbox:1.0.0
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
            - name: OUTBOX_STREAMS
              value: local
            - name: OUTBOX_STREAM_LOCAL_DRIVER
              value: rmq
            - name: OUTBOX_DRIVER_RMQ_TYPE
              value: rabbitmq
            - name: OUTBOX_DRIVER_RMQ_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: amqp}}
          ports:
            - {name: http, containerPort: 8085}
            - {name: metrics, containerPort: 9100}
          livenessProbe:
            httpGet: {path: /health, port: http}
          readinessProbe:
            httpGet: {path: /ready, port: http}
```

`/health` deliberately does not probe dependencies: a database blip should not
make an orchestrator kill a process that would recover on its own. `/ready`
does, so a replica that cannot reach its database stops being advertised.

### Database permissions

```sql
GRANT USAGE ON SCHEMA outbox TO outbox_dispatcher;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox.messages TO outbox_dispatcher;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA outbox TO outbox_dispatcher;
```

`DELETE` is for the retention sweep; without it the table grows without bound.
The role does not need `CREATE` unless `OUTBOX_DB_AUTO_MIGRATE` is on.

### Migrations

Run them from a job rather than at startup in production, so schema changes are
a deliberate step:

```bash
outbox migrate status
outbox migrate up
```

Several replicas may run `migrate up` at once: an advisory lock serialises them
and each migration is applied exactly once. Migration files are immutable after
release, and their checksums are recorded; editing one is refused rather than
silently skipped.

## Scaling

Add replicas. Claims are taken with `FOR UPDATE SKIP LOCKED` and carry a lease
token every write-back must present, so replicas divide the work and cannot
overwrite each other's results. The periodic housekeeping takes an advisory lock,
so it stays on one replica per cycle however many are running.

The knobs, in the order worth reaching for:

1. **`OUTBOX_DISPATCH_WORKERS`** together with the driver's channel pool.
   Throughput is bounded by whichever of the two is smaller, so raising one
   alone does nothing: eight workers over the default four RabbitMQ channels
   performs the same as four over four, while widening the pool to match moves
   it by around 60%. Raise `WORKERS` and `CHANNELS` together, or neither.
2. **`OUTBOX_DISPATCH_BATCH_SIZE`** — messages per claim. Larger amortises the
   claim across more messages; too large and a batch stops fitting inside the
   lease.
3. **Replicas** — when one process cannot keep up, or for availability.

`make bench` sweeps all three against a local stack, which is the way to find
where the limit sits on your own hardware rather than on someone else's.

Raising `WORKERS` or `BATCH_SIZE` lengthens a batch, so keep
`OUTBOX_DISPATCH_LEASE_TTL` comfortably above the time one takes.
`outbox_lease_conflicts_total` is the alarm for having got that wrong; it should
be zero.

Each replica holds one pooled connection for the notification listener, so size
`OUTBOX_DB_MAX_CONNS` with room to spare.

## When something is wrong

### Messages are not being delivered

```bash
curl -s localhost:8085/api/v1/stats | jq .messages
```

- **`pending` rising, `oldest_pending_age` rising** — not keeping up, or not
  running. Check `rate(outbox_messages_dispatched_total[5m])`: zero with a
  backlog means the pipeline is stopped or the broker is refusing everything.
- **`processing` rising and staying** — replicas are dying mid-batch. Check
  `outbox_messages_reclaimed_total` and the logs of whoever `owner` names.
- **`failed` rising** — see below.

### Messages are failing

From a shell inside the container, which is where the binary already is:

```bash
outbox failed -limit 20
outbox failed -stream local -json | jq '.messages[] | {id, topic, last_error}'
```

`last_error` carries the broker's own words. Once the cause is fixed:

```bash
outbox requeue 3b73a835-1aee-45e7-9dc1-36f38ace64e9
outbox requeue -before 2026-01-01T00:00:00Z -limit 1000
```

Or over HTTP, for the same operations from somewhere the binary is not:

```bash
curl -s 'localhost:8085/api/v1/messages/failed?limit=20&stream=local' \
  | jq '.messages[] | {id, topic, attempts, last_error}'

curl -X POST -H "Authorization: Bearer $TOKEN" \
  localhost:8085/api/v1/messages/requeue \
  -d '{"failed_before":"2026-01-01T00:00:00Z","limit":1000}'
```

Both run the same store calls, so neither can become the one that does it
correctly. What differs is how each is authorised, and deliberately: the
endpoints need `OUTBOX_HTTP_ADMIN_TOKEN` because anything that can route to the
pod can call them, while the commands need the database credentials, which is a
stronger thing to be holding.

The commands also check less of the configuration than the dispatcher does —
enough to reach the database, and no more. The moment you most need to see what
stopped is often the moment the routing table is what is wrong, and a tool that
answers "your broker is misconfigured" to the question "what failed?" is useless
precisely then.

`outbox_messages_failed_total{reason="permanent"}` rising instead points at
configuration — an unroutable exchange, an unknown topic, a stream that is not
configured — rather than at an outage. Retrying would not have helped, and did
not happen.

### Duplicate deliveries

Duplicates are permitted by at-least-once and consumers must tolerate them. A
*rise* in them is a symptom:

- `outbox_lease_conflicts_total` above zero — the lease is shorter than the work.
  Raise `LEASE_TTL` or lower `BATCH_SIZE`.
- `outbox_messages_reclaimed_total` rising — replicas are being killed mid-batch.
  Check the shutdown budget: `OUTBOX_APP_SHUTDOWN_TIMEOUT` must fit inside the
  orchestrator's grace period, or a clean drain is cut short.

### The table is growing

Check `OUTBOX_JANITOR_RETENTION` is not `0` and that
`outbox_retention_deleted_total` is moving. The sweep needs `DELETE` on the
table, and it only removes delivered rows — a growing `failed` population is a
different problem, and the failed listing is where to look.

### Partitioning, past roughly ten million rows a day

At that volume the sweep stops keeping up: a chunked `DELETE` creates dead
tuples faster than autovacuum reclaims them, so the table keeps growing while
apparently being cleaned. Range-partitioning by `created_at` turns the same work
into `DROP TABLE` — a catalogue change and an unlink, costing the same whatever
the partition held.

It is opt-in and it is a deliberate migration. Apply the shipped schema against
an empty database *before* the ordinary migrations, which then run over it
unchanged:

```bash
psql "$DSN" -f migrations/partitioned/messages.sql
outbox migrate up
```

The dispatcher notices the shape of the table by itself. Retention switches from
deleting rows to dropping partitions, and the janitor keeps
`OUTBOX_JANITOR_PARTITION_AHEAD` days (`3`) created in front of itself. Every
query it runs is unchanged: partitioning is transparent to DML.

**A partition is only dropped when everything in it has been delivered** and the
most recent delivery is past retention. Both halves matter and neither follows
from the partition's bounds: those are on `created_at` while retention is on
`dispatched_at`, so a partition full of week-old messages may still hold one that
failed and is waiting for somebody. Dropping by age alone would delete it.

**The default partition is why a missing daily partition is a warning rather
than an outage.** A row that fits no partition is a failed `INSERT`, and that
`INSERT` is inside the producer's business transaction — so a janitor that
stopped running would not delay messages, it would roll back whatever the
application was doing. Rows that land there are counted by
`outbox_default_partition_rows` and reported in the log, because until they are
moved the proper partition for their range cannot be created.

Two things it costs, both worth knowing before deciding:

- **The primary key changes.** PostgreSQL requires a unique constraint on a
  partitioned table to include the partition key, so `id` alone cannot be the
  primary key and becomes `(id, created_at)`. The database no longer enforces
  that an id appears once across the whole table — only once per day. Consumers
  already deduplicate on the message id under at-least-once delivery, so nothing
  breaks, but it is a guarantee given up rather than a detail.
- **Planning gets more expensive, execution barely moves.** Measured on 405k
  rows across 31 daily partitions: the claim query executes in about 0.25 ms
  against 0.18 ms unpartitioned, because the partial indexes on older partitions
  are empty and merging across them costs almost nothing. Planning goes from
  0.4 ms to 2 ms — paid once, since pgx prepares its statements, after which it
  is 0.29 ms.

Below ten million rows a day the ordinary table is simpler and behaves better.

### Shutdown

`SIGTERM` starts a graceful drain: pipelines stop claiming, finish the batch in
flight, record its outcome, and hand back anything they never started so another
replica takes it immediately. The process exits `128 + signum`.

If the shutdown budget is exceeded the logs say so, and the leases involved
expire on their own — nothing is lost, but delivery of those messages waits out
`LEASE_TTL`.

## What happens when a dependency fails

Each of these is covered by a test in `test/integration/resilience_test.go`, which
severs the connection through a proxy rather than describing what ought to happen.

| Failure | Behaviour |
|---|---|
| A broker goes down while running | Contained. Only the streams pointed at it stall; the rest keep publishing. Their messages retry with backoff and nothing is lost. The affected stream also stops claiming, so an outage does not turn every incoming message into a failed publish. |
| That broker comes back | Reconnects on its own, with no restart and no intervention. The supervisor redials with an exponential delay up to 30s. |
| PostgreSQL goes down | Delivery stalls; the process stays up and does not spin. Claims fail once per poll interval and are logged. `/ready` reports unhealthy, so an orchestrator stops routing to it. |
| PostgreSQL comes back | The pool reconnects, the notification listener re-listens, delivery resumes. |
| Everything goes down at once | The two above, together. Recovery needs no particular order. |
| An outage longer than the retry budget | Nothing. The budget counts rejections, and a broker that cannot be reached has not rejected anything: the messages wait, marked deferred, and go out when it returns. Set `OUTBOX_DISPATCH_MAX_DEFER` if you would rather they failed. |
| A broker that answers and refuses | Retried with backoff until `MAX_ATTEMPTS` is spent, then `failed`, with the broker's own words in `last_error`. They stay in the table and wait for a requeue. |
| A broker unreachable **at startup** | The process refuses to start. This is the asymmetry to know about: a broker that dies while running is contained, but one that is already dead when the dispatcher boots stops every stream, not just its own. |

### Claiming stops while a broker is down

Finding a broker gone stops the pipeline claiming for that stream. The pause
starts at one poll interval and doubles up to `OUTBOX_DISPATCH_PAUSE_MAX`
(`30s`), and one ordinary claim is let through each time it elapses — publishing
is the capability that matters, so the trial is a real batch rather than a
health check, which can be green while the exchange the messages need is not
there.

What this saves is not the retries. A deferred message is already rescheduled a
backoff into the future, so retrying an outage is self-limiting. New messages
are not: every insert that arrives while the broker is down wakes the pipeline
through `LISTEN`/`NOTIFY` and would otherwise be claimed, attempted and written
back at once. The load removed is proportional to how busy the producer is
rather than to how long the outage lasts.

The ceiling matches the delay the RabbitMQ supervisor backs off to between
reconnection attempts, so pausing adds nothing to how soon a returning broker is
noticed — the driver's own backoff already bounds that. `0` disables the pause
entirely.

### How long an outage the defaults survive

Indefinitely, by default.

An unreachable broker never saw the message, so the message is not charged for
the visit. It returns to `pending` with its attempt counter untouched, marked
with a `deferred_since` timestamp, and is tried again on the ordinary backoff
until the broker comes back. The attempt counter measures **rejections, not
minutes**.

That distinction is the whole point. The two failures deserve opposite
responses:

| What happened | Response | Why |
|---|---|---|
| The broker looked at the message and refused it | Spend an attempt. After `MAX_ATTEMPTS`, `failed`. | Retrying forever will not change its mind, and the message needs to become visible. |
| The broker could not be reached at all | Delay, but spend nothing. | Nothing was learned about the message. Failing it makes an outage into an operator's problem twice. |

The retry budget still exists and still bounds the first case. On the defaults —
`MAX_ATTEMPTS=5`, `BACKOFF_BASE=1m`, `BACKOFF_MAX=1h` — that is 1 + 2 + 4 + 8
minutes of rejections, fifteen in all, before a message is given up on. Raising
`OUTBOX_DISPATCH_MAX_ATTEMPTS` is cheap because each extra attempt doubles the
delay before it, but it no longer buys tolerance for an outage: it buys patience
with a broker that is actively saying no.

#### What to watch while a broker is down

```promql
outbox_stream_paused                     # 1: this stream has stopped claiming
outbox_messages_deferred                 # how many are waiting on a broker right now
outbox_oldest_pending_age_seconds        # how long the oldest has waited
rate(outbox_messages_deferred_total[5m]) # rising: messages are still being attempted
```

The fourth is a trap on its own, and worth understanding before writing an alert
on it. Once `outbox_stream_paused` goes to `1` the dispatcher stops claiming for
that stream, so it publishes nothing, so it defers nothing — and the deferral
rate falls to zero exactly when the outage is most established. The gauge is
what stays true; the rate covers the interval before claims stop. The shipped
`OutboxBrokerUnreachable` rule joins the two for that reason.

The backlog age is the one to alert on. A backlog that is growing while
`outbox_messages_deferred` sits at zero means the dispatcher is behind and will
catch up; the same backlog with a deferred count equal to it means nothing will
move until somebody fixes the broker.

#### Bounding the wait

Waiting forever is right for most streams and wrong for a few. A message that is
only useful for the next ten minutes is worth less delivered late than visibly
failed, and for those:

```bash
OUTBOX_DISPATCH_MAX_DEFER=30m
```

A message held back continuously for longer than that is failed, with the reason
recorded and `outbox_messages_failed_total{reason="unreachable"}` incremented —
a separate label from `attempts_exhausted`, because such a message was never
rejected and its attempt counter still reads zero. Reporting it as exhausted
would send whoever reads it looking for a rejection that never happened.

The window measures a continuous outage, not the age of the row: it starts at
the first deferral and is cleared the moment the message goes through or fails
for a reason the broker actually gave. An old message meeting its first outage
has waited none of it.

The default is `0`, which means unbounded.

## Latency

With `OUTBOX_DISPATCH_NOTIFY_ENABLED=true` a trigger announces each insert and
the relevant pipeline wakes within milliseconds. The poll loop stays on as
reconciliation, because `NOTIFY` is best-effort and is lost when the listening
connection drops — losing one costs a poll interval, never a message.

Where the database role cannot create triggers, set it to `false`. Everything
works; delivery is then bounded by `OUTBOX_DISPATCH_POLL_INTERVAL`.
