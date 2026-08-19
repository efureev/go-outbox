# Spec: more destinations (milestone 1.6)

[Русская версия](DriverSpec.ru.md)

What it takes to add a driver, whether each candidate is worth having, and what
it costs in binary size. The figures are measured rather than estimated: the
previous estimate on the [roadmap](Roadmap.md) was wrong by a multiple, and
there is no reason to repeat that.

## Summary

Measured on linux/amd64, `go build -trimpath -ldflags '-s -w'`, against tag
v1.5.0 (21.87 MB). Each library's probe is reachable from `main`, without which
the linker discards it wholesale and the measurement reads zero.

| Candidate | Binary | Growth | Modules | Verdict |
|---|---|---|---|---|
| `database/sql` client | 21.87 MB | **0** | 0 | **Done** — `pkg/outboxsql` |
| NATS JetStream | 23.45 MB | **+1.58 MB** | +3 | **Do** |
| SQS | 25.11 MB | +3.24 MB | +15 | On request |
| Redis Streams | 28.20 MB | **+6.33 MB** | +2 | **Not without a caveat** |

The roadmap's order should change. It put NATS first and Redis second; Redis in
fact costs four times what NATS does and offers the weakest delivery guarantee of
the four. The `database/sql` client is free to the daemon and should therefore
come before everything.

Image growth equals binary growth: the `scratch` image holds nothing but the
binary and the root certificates. Redis Streams would take the image from 29 MB
to 35 MB — 22% more, for a broker whose durability is weaker than the one the
dispatcher promises.

## What a driver has to provide

One contract, in [`internal/broker/broker.go`](../internal/broker/broker.go):

```go
type Publisher interface {
	Publish(ctx context.Context, msgs []core.Message) []error
	HealthCheck(ctx context.Context) error
	Close(ctx context.Context) error
}
```

The positional error slice is not decoration: one bad message must not condemn
the whole batch to a pointless retry. Every error lands in one of three classes,
and choosing between them is the most consequential decision in any driver:

| Class | Meaning | What the dispatcher does |
|---|---|---|
| `core.Permanent` | the broker looked and refused for good | straight to `failed`, no attempt spent |
| `core.Unavailable` | the broker could not be reached | back to `pending`, **attempt counter untouched** |
| anything else | refused, but retrying may help | backoff, attempt spent |

Erring towards `Unavailable` is the expensive direction: a per-message problem
mistaken for an outage never advances its counter and so never reaches `failed`.
Anything not positively identified as unreachable stays retryable.

### The file list, per driver

1. [`internal/config/broker.go`](../internal/config/broker.go) — the `DriverType`
   constant, `parseDriverType` (~line 381) and two `switch` arms (~296 and ~309).
2. `internal/config/driver_<name>.go` — the configuration struct implementing
   `DriverConfig`: `Name`, `Type`, `Naming`, `Endpoint` (the last must strip
   credentials; it is served over HTTP).
3. `internal/broker/<name>/` — the `Publisher` plus a `classify.go` modelled on
   [rabbitmq's](../internal/broker/rabbitmq/classify.go).
4. [`internal/app/brokers.go`](../internal/app/brokers.go) — an arm in the type
   switch (~line 72).
5. `docker-compose.yml` and `.github/workflows/test.yml` — the broker as a
   service, without which there is nothing to write integration tests against.
6. `docs/Config.md` and `.ru.md` — the environment variables. The documentation
   cross-check fails on an undocumented one.
7. `test/integration/` — tests against the real broker, including a run through
   the breakable proxy in
   [`proxy_test.go`](../test/integration/proxy_test.go): the unavailability
   classification cannot be verified any other way.

---

## 1. A `database/sql` producer client

**Done**, as [`pkg/outboxsql`](../pkg/outboxsql); the recipe is
[use case 6](usecases/6-database-sql.md). What follows is what was specified,
kept for the reasoning.

**Worth it: yes, and first.** No cost to the daemon, and it is the only item in
this milestone that widens not the list of brokers but the list of people who can
write to the outbox at all.

**Size: +0 MB.** Verified: the daemon does not import `pkg/outboxclient`
(`go list -deps ./cmd/outbox` does not contain it), so the client is not linked
into the binary at all — neither now nor afterwards.

### The problem

`pkg/outboxclient` requires pgx:

```go
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
```

That excludes everybody on `database/sql`, `sqlx` or `gorm`, whose `ExecContext`
returns a `sql.Result` rather than a `pgconn.CommandTag`.

### The constraint that decides the design

**The new client must live in its own package.** Put it in `pkg/outboxclient`
and importing that package still drags pgx in — which removes the entire benefit.
`pkg/outboxsql` is proposed.

### Specification

```go
package outboxsql

// Execer is what database/sql hands out. *sql.DB, *sql.Tx, *sqlx.DB and
// *sqlx.Tx all satisfy it; passing a transaction is the entire point.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func New(schema, table string) (*Client, error)
func Default() *Client
func (c *Client) Enqueue(ctx context.Context, db Execer, msg Message) error
func (c *Client) EnqueueBatch(ctx context.Context, db Execer, msgs []Message) error
```

- The SQL and the identifier validation carry over unchanged: `$1…$n`
  placeholders are the same for pgx and `lib/pq`, so the statement text does not
  move.
- `Message` and `Target` are duplicated rather than imported from
  `outboxclient`: importing would put pgx back in the graph.
- `EnqueueBatch` is `ExecContext` in a loop inside the caller's transaction, not
  a `pgx.Batch`. One round trip per message instead of one per batch; that is
  the price of compatibility and the documentation has to say so.

### The thing that must be verified

**`JSONB` encoding.** Measured rather than feared: `[]byte` and `string` both
reach a `jsonb` column intact through `lib/pq` and `pgx/v5/stdlib`, and a payload
of arbitrary non-UTF-8 bytes round-trips through `bytea` under both. The
implementation passes the JSON columns as `string` anyway, because a `[]byte` is
the shape a driver is entitled to encode as `bytea` and the package cannot know
which driver it is talking to. Both drivers are covered by integration tests.

### Tests

- Integration: insert through `database/sql` + `lib/pq`, then read the same row
  back through `Store.Claim` — the daemon seeing what a foreign client wrote.
- The same through `pgx/v5/stdlib`, because that is the likeliest way it is used.
- A round trip for `headers`/`target`: non-empty JSON reaches the broker intact.

---

## 2. NATS JetStream

**Worth it: yes.** Of the three broker candidates it fits the dispatcher's
contract best and costs the least.

**Size: +1.58 MB** (23.45 MB), three modules: `nats.go`, `nkeys`, `nuid`.

### Why it fits

`js.PublishMsg(ctx, msg)` returns a `*jetstream.PubAck` — a synchronous,
per-message acknowledgement from the stream. That is precisely what the
dispatcher requires: a row becomes `sent` only once the broker says it has the
message. No deferred-confirmation machinery as in AMQP, no mapping a batch back
onto positions as in Kafka. It is the simplest of the three drivers to write.

It also brings something the others do not: **deduplication in the stream**.
JetStream discards a repeat carrying the same `Nats-Msg-Id` inside its
`duplicate_window`. The outbox row id maps straight onto it, so republishing
after a replica dies stops being a duplicate *in the stream* — while delivery
stays at-least-once. That is worth recording in the public contract as a
per-driver advantage.

### Error classification

| Error | Class |
|---|---|
| `jetstream.ErrNoStreamResponse`, `nats.ErrNoResponders` | **Permanent** — no stream matches the subject, and retrying will not create one |
| `jetstream.ErrInvalidJSAck`, exceeding `max_msg_size` | **Permanent** |
| `nats.ErrConnectionClosed`, `nats.ErrConnectionDraining` | **Unavailable** |
| `context.DeadlineExceeded` while the parent context is live | **Unavailable** |
| everything else | retryable |

### The trap, to be decided deliberately

`nats.go` **reconnects by itself and buffers publishes while disconnected**
(`ReconnectBufSize`, 8 MB by default). For an ordinary application that is a
convenience; for this dispatcher it is a hazard. A publish can "succeed" locally
by landing in the client's buffer, and the row becomes `sent` although the broker
never saw the message.

The fix: **turn the buffer off** — `nats.ReconnectBufSize(-1)`. A publish with no
connection then returns an error, the error classifies as `Unavailable`, and
everything already built takes over: the message is deferred without spending an
attempt, the breaker stops claiming, `outbox_stream_paused` goes to one. The
library's own reconnection stays, which is useful; only the buffer goes.

This is the first thing to test through the breakable proxy.

### Configuration

```
OUTBOX_DRIVER_<NAME>_TYPE=nats
OUTBOX_DRIVER_<NAME>_DSN=nats://user:pass@host:4222
OUTBOX_DRIVER_<NAME>_STREAM=EVENTS        # the JetStream stream
OUTBOX_DRIVER_<NAME>_PUBLISH_TIMEOUT=15s
OUTBOX_DRIVER_<NAME>_PREFIX=...           # the shared Naming, as elsewhere
```

`Endpoint()` returns the DSN with credentials removed, as
[`redactDSN`](../internal/config/broker.go) does for RabbitMQ.

Open question: **whether to create the stream**. RabbitMQ has `DECLARE`;
JetStream's equivalent is `CreateOrUpdateStream`. The proposal is **not to**, by
default: a stream's configuration — retention, replicas, `duplicate_window` — is
the cluster owner's decision, not a sidecar's. A missing stream must then be
`Permanent`, so a typo in a subject is not deferred forever.

### Tests

- `docker-compose.yml`: `nats:2-alpine` with `-js`, and the service in CI.
- Round trip: publish → consume, headers and `Nats-Msg-Id` intact.
- Through the proxy: severed → `Unavailable` → returned without spending an
  attempt → recovery.
- A stream that does not exist → `Permanent`, straight to `failed`.
- Deduplication: two publishes of one id inside the window → one message in the
  stream.

---

## 3. Redis Streams

**Do not — or do it with an explicit caveat in the public contract.**

**Size: +6.33 MB** (28.20 MB) — the most expensive candidate, four times NATS.
`go-redis` carries the whole Redis command surface and the linker cannot drop it,
because those are methods on a reachable type: an unstripped build links 11,160
symbols from the package.

### Why it fits badly

`XADD` returns an id synchronously, which is fine as far as it goes. The problem
is deeper, and it is in what the service promises.

The dispatcher promises that **a row becomes `sent` only once the broker says it
has the message**. For RabbitMQ that is a publisher confirm after a disk write,
for Kafka `acks=all`, for JetStream the stream's acknowledgement. Redis defaults
to `appendfsync everysec`: an acknowledged `XADD` can be lost if the process dies
within the last second. The same sentence is therefore weaker on Redis, and
weaker in exactly the part the transactional outbox pattern exists for.

Second: `MAXLEN`/`XTRIM` can remove entries nobody consumed. A broker that can
silently discard an accepted message is an odd destination for a service whose
one job is not to lose messages.

### If it is built anyway

Then, explicitly:

- A paragraph in [`docs/PublicContract.md`](PublicContract.md) saying that with
  this driver "confirmed" means "accepted by Redis", not "written to disk", and
  that the guarantee depends on `appendfsync`.
- Check `appendonly` and `appendfsync` at startup through `CONFIG GET` and **log
  a warning** when it is not `always`. Quietly weakening a guarantee is worse
  than not offering it.
- Weigh the +6.33 MB separately. The README's "boring to operate" rests partly on
  image size, and 29 → 35 MB is a visible part of it.

Recommendation: wait until somebody asks, then ask them why Redis Streams rather
than NATS. The answer is often "we already run Redis" — which is not an argument
about delivery.

---

## 4. SQS/SNS

**On request, as the roadmap says.**

**Size: +3.24 MB** (25.11 MB), fifteen modules — AWS SDK v2 is modular and so
cheaper than its reputation, but fifteen `go.mod` entries are fifteen entries.

It fits well: `SendMessage` acknowledges synchronously, and `SendMessageBatch`
takes up to ten messages and returns `Successful`/`Failed` positionally — the
shape of the contract already. Classification: `InvalidMessageContents` and
`UnsupportedOperation` → `Permanent`; timeouts, throttling, `ServiceUnavailable`
and network errors → `Unavailable`.

One difficulty the others do not have: **authentication**. The AWS provider chain
— environment, profile, IRSA, IMDS — does not reduce to a single DSN, while this
project's configuration is flat `OUTBOX_*` variables throughout. The proposal is
not to invent anything: rely on the SDK's standard chain and keep only the region
and the queue URL in configuration.

Leave it until asked. The cost is not the megabytes but the fifteen modules whose
advisories somebody has to follow, for a driver nobody may use.

---

## Proposed order

1. **The `database/sql` client** — free, and it widens who can write to the
   outbox at all. An afternoon.
2. **NATS JetStream** — the cheapest broker and the best fit for the contract.
   It also settles whether `broker.Publisher` earned its place: if a third driver
   needs no change to it, the abstraction was right.
3. **SQS** — when asked.
4. **Redis Streams** — when asked, and when the reason has been given.
