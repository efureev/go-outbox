# Configuration

English | [Русский](Config.ru.md)

Every setting is an environment variable under the `OUTBOX_` prefix. Values are also read from a `.env` file when one is
present; the environment wins.

Everything is read and validated at startup. A malformed duration or an out-of-range threshold stops the process, with
**every** problem listed at once — so a misconfigured deployment takes one restart to diagnose rather than one per
mistake.

## Application

| Variable                      | Default  | Meaning                                                                                               |
|-------------------------------|----------|-------------------------------------------------------------------------------------------------------|
| `OUTBOX_APP_NAME`             | `outbox` | Reported as `application_name` to PostgreSQL, which is where it shows up in `pg_stat_activity`.       |
| `OUTBOX_APP_ENV`              | `prod`   | Environment label.                                                                                    |
| `OUTBOX_APP_INSTANCE`         | hostname | Recorded in the `owner` column of a claimed row, so an operator can tell which replica holds a lease. |
| `OUTBOX_APP_SHUTDOWN_TIMEOUT` | `30s`    | Bounds the whole teardown.                                                                            |

## Logging

| Variable            | Default | Meaning                                                                                                                                                                 |
|---------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OUTBOX_LOG_LEVEL`  | `info`  | `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `panic`.                                                                                                            |
| `OUTBOX_LOG_FORMAT` | `json`  | `console` for a terminal, `text` for flat `key=value`, `json` for a collector.                                                                                          |
| `OUTBOX_LOG_CALLER` | `false` | Record the call site. Useful while debugging; for dependencies it resolves to a path inside the Go module cache, which names a library version rather than a subsystem. |

### What every line carries

| Field       | Meaning                                                                                                                                                                                                      |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `module`    | The subsystem: `db`, `brokers`, `dispatch`, `janitor`, `metrics`, `http`, `hub`. `grep module=db` returns everything about one subsystem — both what the lifecycle manager did to it and what it did itself. |
| `component` | Set to `lifecycle` on the framework's own logging, which belongs to no single module. A separate key from `module` on purpose: those lines carry a `module` attribute naming the *subject* of the action.    |
| `instance`  | The replica, the same value a claim writes to the `owner` column — so a log line and the row it is about name the same process.                                                                              |

In the **console** format the subsystem is rendered ahead of the message instead of as a field, because carrying the
same name twice on one line is noise:

```
15:04:05.128 INF [db] database pool opened instance=outbox-7d9f max_conns=10
15:04:05.130 INF [lifecycle] starting module instance=outbox-7d9f module=dispatch
```

There the token to grep for is `[db]`. The **json** format has no prefix — the message stays unadorned for a collector
and the name stays in the `module` field.

### The line to read first

One line is emitted once the whole graph is up, recording the configuration the process actually came up with:

```
15:04:05.128 INF [outbox] ready instance=outbox-7d9f version=1.0.0 streams=global:kfk,local:rmq
    batch_size=200 workers=8 poll_interval=5s lease_ttl=2m max_attempts=5 max_defer=unbounded
    notify=true retention=168h
```

`GET /api/v1/stats` answers the same question for a process that is still running. This answers it for one that has
since been replaced — which is the version an incident usually needs.

### What a log line costs

The dispatcher writes **nothing per message**. Claiming, publishing and writing the outcome back emit no log lines at
all; observability on that path is the Prometheus counters fed by the domain events. A guard test drains 100 and 1000
messages at the `trace` level and fails if the line count differs, because a
`Debug` inside the publish loop reads like an improvement and costs an allocation per message even with the level
switched off.

Measured with `make bench-logging`, per line, with `instance` and `module` bound:

|                                    | ns/op | allocs/op |
|------------------------------------|------:|----------:|
| `json`, no record attributes       |  ~345 |         0 |
| `json`, three record attributes    |  ~445 |         3 |
| `console`, no record attributes    |  ~228 |         1 |
| `console`, three record attributes |  ~391 |         4 |
| below the level threshold          |   ~15 |         1 |

Bound fields are encoded once when the logger is built, so `instance` on every line costs a memory copy rather than an
allocation. Each attribute passed at the call site costs one allocation, and the console prefix costs one more — which
is why it is the development format.

### Timestamps

| Format    | Layout                          | Example                            |
|-----------|---------------------------------|------------------------------------|
| `console` | `15:04:05.000`                  | `15:04:05.128`                     |
| `text`    | `2006-01-02T15:04:05.000Z07:00` | `2026-08-19T15:04:05.128+03:00`    |
| `json`    | RFC3339 with nanoseconds        | `2026-08-19T15:04:05.128533+03:00` |

Milliseconds are carried everywhere on purpose: delivery latency here is measured in them, and a timestamp without them
cannot order a claim, its publish and its write-back.

## Database

| Variable                                                                   | Default                                      | Meaning                                                                                                                                          |
|----------------------------------------------------------------------------|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `OUTBOX_DB_DSN`                                                            | —                                            | The whole connection string. Preferred: the components below are assembled through a URL builder, but a DSN removes the question.                |
| `OUTBOX_DB_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_NAME` / `_SSL_MODE` | `localhost` / `5432` / — / — / — / `disable` | Used when `DSN` is empty. Escaped correctly, so a password containing a space or a quote is fine.                                                |
| `OUTBOX_DB_SCHEMA`                                                         | `outbox`                                     | Schema holding the table. Must be a lower-case unquoted identifier.                                                                              |
| `OUTBOX_DB_TABLE`                                                          | `messages`                                   | Table name. Same rule.                                                                                                                           |
| `OUTBOX_DB_MAX_CONNS`                                                      | `10`                                         | Pool size. One connection is held by the notification listener for as long as it runs, so leave room.                                            |
| `OUTBOX_DB_MIN_CONNS`                                                      | `2`                                          |                                                                                                                                                  |
| `OUTBOX_DB_CONNECT_TIMEOUT`                                                | `5s`                                         |                                                                                                                                                  |
| `OUTBOX_DB_STATEMENT_TIMEOUT`                                              | `30s`                                        | Applied to pooled connections. Migrations use a separate connection without it.                                                                  |
| `OUTBOX_DB_MAX_CONN_LIFETIME`                                              | `1h`                                         |                                                                                                                                                  |
| `OUTBOX_DB_MAX_CONN_IDLE_TIME`                                             | `30m`                                        |                                                                                                                                                  |
| `OUTBOX_DB_AUTO_MIGRATE`                                                   | `false`                                      | Apply pending migrations at startup. Off by default: in production the schema usually belongs to the producer's deployment, not to this sidecar. |
| `OUTBOX_DB_MIGRATION_LOCK_KEY`                                             | `8090211501`                                 | Advisory lock held for the duration of a migration run, so replicas starting together apply them exactly once.                                   |

## Streams and drivers

A **stream** is the logical destination a producer names in a row. A **driver**
is a connection to one broker. Several streams may share a driver.

```dotenv
OUTBOX_STREAMS=local,global

OUTBOX_STREAM_LOCAL_DRIVER=rmq
OUTBOX_STREAM_GLOBAL_DRIVER=kfk

OUTBOX_DRIVER_RMQ_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_DSN=amqp://user:pass@rabbit:5672/

OUTBOX_DRIVER_KFK_TYPE=kafka
OUTBOX_DRIVER_KFK_BROKERS=kafka-1:9092,kafka-2:9092
```

Driver settings are looked up by exact key from a closed set. A misspelled key is a startup error, not a setting that
silently does nothing — and a driver whose name is a prefix of another (`rmq` and `rmq_local`) reads only its own
variables.

### Several brokers of the same kind

Nothing ties a driver to a broker *type* — a driver is one connection, and there may be as many as there are brokers to
reach. Four streams over three separate RabbitMQ instances is four driver blocks, each with its own DSN:

```dotenv
OUTBOX_STREAMS=local,test,global,tetra

OUTBOX_STREAM_LOCAL_DRIVER=rmq_local
OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=loc

# …and so on for rmq_test, rmq_global and rmq_tetra.
```

Two drivers may point at the same broker; they stay independent, each with its own connection, channel pool and naming.
Driver names that differ only in a `-` versus a
`_` are rejected at startup, because both address the same `OUTBOX_DRIVER_…` block.

The worked example, with what to watch out for, is
[use case 5](UseCases.md#5-four-streams-across-three-rabbitmq-instances).

### Common to every driver

| Key           | Default                     | Meaning                                                                   |
|---------------|-----------------------------|---------------------------------------------------------------------------|
| `TYPE`        | —                           | `rabbitmq` (or `rmq`, `amqp`) or `kafka`. Required.                       |
| `PREFIX`      | —                           | Prepended to every topic name, for sharing a broker between environments. |
| `PREFIX_SEP`  | `_` for AMQP, `.` for Kafka | Separator between prefix and topic.                                       |
| `VERSION_SEP` | `_` for AMQP, `.` for Kafka | Separator before the `vN` suffix.                                         |

The effective name a consumer must subscribe to is

```
[prefix + prefix_sep] + topic + [version_sep + "v" + version]
```

so `PREFIX=prod`, topic `user.created` and `target.version = 2` give
`prod_user.created_v2` on AMQP.

### RabbitMQ

| Key               | Default | Meaning                                                                                                                                                  |
|-------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DSN`             | —       | `amqp://…` or `amqps://…`. Required.                                                                                                                     |
| `CHANNELS`        | `4`     | Publish channel pool size, which is the publish concurrency per connection.                                                                              |
| `DECLARE`         | `false` | Declare a queue before publishing to it. Off by default: broker topology belongs to whoever owns the broker.                                             |
| `MANDATORY`       | `true`  | Ask the broker to return an unroutable message rather than discard it, which turns a misroute into a visible permanent failure instead of a silent loss. |
| `PUBLISH_TIMEOUT` | `15s`   | Bounds one publish and its confirmation.                                                                                                                 |
| `RECONNECT_DELAY` | `1s`    | First reconnect delay; doubles up to 30s.                                                                                                                |

### Kafka

| Key                                                       | Default | Meaning                                                                                                 |
|-----------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------|
| `BROKERS`                                                 | —       | Comma-separated. Required. Startup succeeds if **any** of them answers.                                 |
| `SECURITY_PROTOCOL`                                       | —       | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.                                                       |
| `SASL_MECHANISM`                                          | —       | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Required with a `SASL_*` protocol, along with the two below. |
| `SASL_USERNAME` / `SASL_PASSWORD`                         | —       |                                                                                                         |
| `SSL_CA_PEM_B64` / `SSL_CERT_PEM_B64` / `SSL_KEY_PEM_B64` | —       | Base64-encoded PEM. Certificate and key must be given together.                                         |
| `COMPRESSION`                                             | `none`  | `gzip`, `snappy`, `lz4`, `zstd`.                                                                        |
| `REQUIRED_ACKS`                                           | `all`   | `none`, `one`, `all`. Anything but `all` weakens the delivery guarantee.                                |
| `MAX_ATTEMPTS`                                            | `3`     | Client-side retries within one write.                                                                   |
| `WRITE_TIMEOUT`                                           | `15s`   |                                                                                                         |
| `ALLOW_AUTO_TOPIC_CREATION`                               | `false` | Off so a typo in a topic name fails loudly instead of creating a topic nobody consumes.                 |

## Dispatch

| Variable                             | Default      | Meaning                                                                                                                                                                                |
|--------------------------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OUTBOX_DISPATCH_BATCH_SIZE`         | `200`        | Messages per claim. A batch that keeps arriving full means there is a backlog — which is what `outbox_batch_size` shows.                                                               |
| `OUTBOX_DISPATCH_WORKERS`            | `8`          | Publish concurrency per stream.                                                                                                                                                        |
| `OUTBOX_DISPATCH_POLL_INTERVAL`      | `5s`         | How often to look for work when nothing wakes the pipeline. With notifications on this is reconciliation, not the main path.                                                           |
| `OUTBOX_DISPATCH_LEASE_TTL`          | `2m`         | How long a claim stays valid. Must exceed `PUBLISH_TIMEOUT`; if it expires mid-flight the batch is reclaimed and published twice, which is what `outbox_lease_conflicts_total` counts. |
| `OUTBOX_DISPATCH_MAX_ATTEMPTS`       | `5`          | How many times a broker may **reject** a message before it is failed. Being unreachable is not a rejection and costs nothing here.                                                     |
| `OUTBOX_DISPATCH_MAX_DEFER`          | `0`          | How long an unreachable broker may hold a message back before it fails anyway, measured from the first deferral. `0` means unbounded, which is the right answer for most streams.      |
| `OUTBOX_DISPATCH_BACKOFF_BASE`       | `1m`         | First retry delay. Doubles per attempt.                                                                                                                                                |
| `OUTBOX_DISPATCH_BACKOFF_MAX`        | `1h`         | Ceiling. Without one, doubling a minute passes a day by the eleventh attempt.                                                                                                          |
| `OUTBOX_DISPATCH_BACKOFF_JITTER`     | `0.2`        | Fraction to spread the delay by. Without it, everything that failed while a broker was down becomes due at the same instant when it returns.                                           |
| `OUTBOX_DISPATCH_PUBLISH_TIMEOUT`    | `15s`        | Bounds one publish.                                                                                                                                                                    |
| `OUTBOX_DISPATCH_WRITE_BACK_TIMEOUT` | `30s`        | Bounds recording the outcome. Deliberately generous: losing this write means republishing the batch.                                                                                   |
| `OUTBOX_DISPATCH_NOTIFY_ENABLED`     | `true`       | Wake on insert through `LISTEN`/`NOTIFY`. Turn off where the database role cannot create triggers; everything still works on polling.                                                  |
| `OUTBOX_DISPATCH_NOTIFY_CHANNEL`     | `outbox_new` | Must match the channel the trigger was created with.                                                                                                                                   |
| `OUTBOX_DISPATCH_NOTIFY_DEBOUNCE`    | `50ms`       | Window over which a burst of inserts is collapsed into one wakeup.                                                                                                                     |
| `OUTBOX_DISPATCH_NOTIFY_JITTER`      | `100ms`      | Spread across replicas, so one insert does not make every replica claim at the same millisecond.                                                                                       |

## Housekeeping

Each cycle takes a PostgreSQL advisory lock, so it runs on one replica per cycle however many are deployed.

| Variable                            | Default     | Meaning                                                                                                                  |
|-------------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------|
| `OUTBOX_JANITOR_ENABLED`            | `true`      |                                                                                                                          |
| `OUTBOX_JANITOR_RECLAIM_INTERVAL`   | `30s`       | How often expired leases are returned to the queue.                                                                      |
| `OUTBOX_JANITOR_STATS_INTERVAL`     | `30s`       | Gauge refresh. Deliberately slower than the poll loop: the counts come from partial indexes, but they are still queries. |
| `OUTBOX_JANITOR_RETENTION`          | `168h`      | How long a delivered row is kept. `0` disables purging — and the table then grows without bound.                         |
| `OUTBOX_JANITOR_RETENTION_INTERVAL` | `5m`        |                                                                                                                          |
| `OUTBOX_JANITOR_RETENTION_BATCH`    | `5000`      | Rows per DELETE, so the transaction stays short.                                                                         |
| `OUTBOX_JANITOR_LOCK_KEY`           | `809021150` | Advisory lock namespace.                                                                                                 |

## Interfaces

| Variable                       | Default    | Meaning                                                                                   |
|--------------------------------|------------|-------------------------------------------------------------------------------------------|
| `OUTBOX_HTTP_ENABLED`          | `true`     |                                                                                           |
| `OUTBOX_HTTP_PORT`             | `8085`     |                                                                                           |
| `OUTBOX_HTTP_ADMIN_TOKEN`      | —          | Guards `POST /api/v1/messages/requeue`. Without it the endpoint is not registered at all. |
| `OUTBOX_HTTP_READ_TIMEOUT`     | `10s`      |                                                                                           |
| `OUTBOX_HTTP_WRITE_TIMEOUT`    | `30s`      |                                                                                           |
| `OUTBOX_HTTP_SHUTDOWN_TIMEOUT` | `10s`      |                                                                                           |
| `OUTBOX_METRICS_ENABLED`       | `true`     |                                                                                           |
| `OUTBOX_METRICS_PORT`          | `9100`     | Must differ from the HTTP port.                                                           |
| `OUTBOX_METRICS_PATH`          | `/metrics` |                                                                                           |

## Dead-letter forwarding

When a message stops being retried it can be forwarded to a destination a consumer watches. The row stays in the table
either way: the dead-letter topic is a signal, not the record.

| Variable             | Default              | Meaning                                |
|----------------------|----------------------|----------------------------------------|
| `OUTBOX_DLQ_ENABLED` | `false`              |                                        |
| `OUTBOX_DLQ_STREAM`  | —                    | Must be one of the configured streams. |
| `OUTBOX_DLQ_TOPIC`   | `outbox.dead-letter` |                                        |

A forwarded message keeps its payload and gains
`x-outbox-original-topic`, `x-outbox-original-stream`, `x-outbox-attempts` and
`x-outbox-permanent` headers.
