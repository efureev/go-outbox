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

```bash
curl -s 'localhost:8085/api/v1/messages/failed?limit=20' | jq '.messages[] | {id, topic, attempts, last_error}'
```

`last_error` carries the broker's own words. Once the cause is fixed:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  localhost:8085/api/v1/messages/requeue \
  -d '{"failed_before":"2026-01-01T00:00:00Z","limit":1000}'
```

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

Beyond roughly ten million rows a day, consider range-partitioning by
`created_at` and dropping whole partitions instead. The dispatcher does not
require it and does not ship it.

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
| A broker goes down while running | Contained. Only the streams pointed at it stall; the rest keep publishing. Their messages retry with backoff and nothing is lost. |
| That broker comes back | Reconnects on its own, with no restart and no intervention. The supervisor redials with an exponential delay up to 30s. |
| PostgreSQL goes down | Delivery stalls; the process stays up and does not spin. Claims fail once per poll interval and are logged. `/ready` reports unhealthy, so an orchestrator stops routing to it. |
| PostgreSQL comes back | The pool reconnects, the notification listener re-listens, delivery resumes. |
| Everything goes down at once | The two above, together. Recovery needs no particular order. |
| An outage longer than the retry budget | Messages stop being retried and land in `failed`, with the broker's own words in `last_error`. They stay in the table and wait for a requeue. |
| A broker unreachable **at startup** | The process refuses to start. This is the asymmetry to know about: a broker that dies while running is contained, but one that is already dead when the dispatcher boots stops every stream, not just its own. |

### How long an outage the defaults survive

The retry budget is the sum of the backoff delays before the attempts run out. On
the defaults — `MAX_ATTEMPTS=5`, `BACKOFF_BASE=1m`, `BACKOFF_MAX=1h` — that is
1 + 2 + 4 + 8 minutes:

```
15 minutes
```

An outage shorter than that is absorbed entirely: the messages sit in `pending`
and go out when the broker returns. An outage longer than that consumes the
budget, and whatever was still undelivered ends in `failed` awaiting
`POST /api/v1/messages/requeue`.

Buy more time by raising `OUTBOX_DISPATCH_MAX_ATTEMPTS`, which is cheap because
each extra attempt doubles the delay before it:

| `MAX_ATTEMPTS` | Outage survived |
|---|---|
| 5 (default) | 15 minutes |
| 8 | 2 hours |
| 10 | 4 hours |

The cost is that a genuinely undeliverable message — one the broker will always
reject — takes correspondingly longer to reach `failed` and become visible. It
does not cost throughput: a message in backoff is not claimed, so it occupies
nothing but a row.

## Latency

With `OUTBOX_DISPATCH_NOTIFY_ENABLED=true` a trigger announces each insert and
the relevant pipeline wakes within milliseconds. The poll loop stays on as
reconciliation, because `NOTIFY` is best-effort and is lost when the listening
connection drops — losing one costs a poll interval, never a message.

Where the database role cannot create triggers, set it to `false`. Everything
works; delivery is then bounded by `OUTBOX_DISPATCH_POLL_INTERVAL`.
