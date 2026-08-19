# Four streams across three RabbitMQ instances

English | [Русский](5-multi-rabbitmq.ru.md)

Back to the [use case index](../UseCases.md).


Not a deployment target but a routing arrangement: one dispatcher feeding several
brokers at once. A driver is one connection, and there may be as many as there are
brokers to reach — so separating environments, tenants or blast radius is a matter
of configuration.

Here `local` and `tetra` share an instance through drivers of their own, `test` and
`global` each get one to themselves.

```dotenv
OUTBOX_STREAMS=local,test,global,tetra

OUTBOX_STREAM_LOCAL_DRIVER=rmq_local
OUTBOX_STREAM_TEST_DRIVER=rmq_test
OUTBOX_STREAM_GLOBAL_DRIVER=rmq_global
OUTBOX_STREAM_TETRA_DRIVER=rmq_tetra

OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=loc
OUTBOX_DRIVER_RMQ_LOCAL_DECLARE=true

OUTBOX_DRIVER_RMQ_TEST_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TEST_DSN=amqp://user:pass@rabbit-b:5672/
OUTBOX_DRIVER_RMQ_TEST_PREFIX=tst
OUTBOX_DRIVER_RMQ_TEST_DECLARE=true

OUTBOX_DRIVER_RMQ_GLOBAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_GLOBAL_DSN=amqp://user:pass@rabbit-c:5672/
OUTBOX_DRIVER_RMQ_GLOBAL_PREFIX=glb
OUTBOX_DRIVER_RMQ_GLOBAL_DECLARE=true

# Back to the first instance, but a driver of its own: a separate connection, its
# own channel pool, its own naming.
OUTBOX_DRIVER_RMQ_TETRA_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TETRA_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_TETRA_PREFIX=ttr
OUTBOX_DRIVER_RMQ_TETRA_DECLARE=true
```

`DECLARE` is on here so a fresh set of brokers has somewhere to publish. Leave it
off once the consumers own the topology: with `MANDATORY` on by default, a queue
nobody declared makes the broker return the message and the dispatcher record a
permanent failure — visible, but not delivered.

A producer picks its destination with one column:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES (gen_random_uuid(), 'tetra', 'orders.placed', convert_to('{}', 'UTF8'), '{}');
```

which lands where the prefix says it will:

| Stream | Driver | Instance | Queue |
|---|---|---|---|
| `local` | `rmq_local` | rabbit-a | `loc_orders.placed` |
| `test` | `rmq_test` | rabbit-b | `tst_orders.placed` |
| `global` | `rmq_global` | rabbit-c | `glb_orders.placed` |
| `tetra` | `rmq_tetra` | rabbit-a | `ttr_orders.placed` |

`GET /api/v1/stats` reports the mapping as it was actually parsed, each driver with
the address it connects to and its credentials removed — which is the quickest way
to confirm a stream reaches the instance you meant, and not merely that it resolved
to a driver of the right name.

### Trying it locally

```yaml
# The credentials matter: the built-in guest account is refused from anything but
# loopback, and a connection through a published port is not loopback.
x-rabbit: &rabbit
  image: rabbitmq:4-alpine
  environment:
    RABBITMQ_DEFAULT_USER: outbox
    RABBITMQ_DEFAULT_PASS: outbox

services:
  rabbit-a: {<<: *rabbit, ports: ["55672:5672"]}
  rabbit-b: {<<: *rabbit, ports: ["55673:5672"]}
  rabbit-c: {<<: *rabbit, ports: ["55674:5672"]}
```

Point the three DSNs at `amqp://outbox:outbox@localhost:55672/` and the two ports
after it, insert one message per stream, and each instance ends up holding exactly
its own.

### What goes wrong here

- **Counting connections once instead of per driver.** `CHANNELS` and the publish
  concurrency belong to a driver, not to the process. Four drivers at the default
  four channels is sixteen AMQP channels, and `OUTBOX_DISPATCH_WORKERS` applies to
  every stream's pipeline separately — so this arrangement runs the default eight
  workers against four channels four times over. Half of each pipeline's workers
  wait for a channel; raise `CHANNELS` and `WORKERS` together or leave both.
- **A driver name that is a prefix of another.** `rmq` alongside `rmq_local` is
  handled — settings are matched by exact key, not by string prefix — but it reads
  badly. Give drivers names that do not nest.
- **Assuming one broker being down is contained.** Once running, it is: each stream
  has its own pipeline, so the others keep publishing. It is not contained at
  startup — every driver connects before the dispatcher starts, so one unreachable
  broker means none of the streams run. And it is never contained for the table,
  which keeps growing while that stream's messages retry. Watch
  `max(outbox_oldest_pending_age_seconds)` alongside
  `outbox_messages_retried_total{stream=…}`, which is the one that names the culprit.
- **Forgetting that a prefix is part of the consumer contract.** Change
  `OUTBOX_DRIVER_*_PREFIX` and every consumer of that stream is subscribed to a queue
  nobody publishes to any more.
