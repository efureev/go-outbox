# Laravel, PostgreSQL 18 and RabbitMQ under Docker Compose

English | [Русский](1-laravel-docker.ru.md)

Back to the [use case index](../UseCases.md).


A local development setup: a Laravel application writes outbox rows, the
dispatcher runs beside it as a sidecar, and a consumer reads from RabbitMQ.

### Compose

```yaml
services:
  app:                                  # your Laravel container
    build: .
    environment:
      DB_CONNECTION: pgsql
      DB_HOST: postgres
      DB_PORT: 5432
      DB_DATABASE: app
      DB_USERNAME: app
      DB_PASSWORD: secret
    depends_on:
      postgres: {condition: service_healthy}

  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_DB: app
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
    # PostgreSQL 18 wants a single mount one level up from the data directory.
    volumes:
      - postgres-data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d app"]
      interval: 2s
      retries: 15

  rabbitmq:
    image: rabbitmq:4-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: app
      RABBITMQ_DEFAULT_PASS: secret
    ports: ["15672:15672"]              # management UI
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      retries: 15

  outbox:
    image: ghcr.io/efureev/go-outbox:1.0.0
    environment:
      OUTBOX_DB_DSN: postgres://app:secret@postgres:5432/app?sslmode=disable
      OUTBOX_DB_SCHEMA: outbox
      # Fine locally. In production apply migrations as a deliberate step.
      OUTBOX_DB_AUTO_MIGRATE: "true"
      OUTBOX_LOG_FORMAT: console
      OUTBOX_STREAMS: local
      OUTBOX_STREAM_LOCAL_DRIVER: rmq
      OUTBOX_DRIVER_RMQ_TYPE: rabbitmq
      OUTBOX_DRIVER_RMQ_DSN: amqp://app:secret@rabbitmq:5672/
      # The dispatcher declares queues so a fresh checkout has somewhere to
      # publish. Turn this off once the consumer owns the topology.
      OUTBOX_DRIVER_RMQ_DECLARE: "true"
    ports:
      - "8085:8085"                     # /api/v1/stats
      - "9100:9100"                     # /metrics
    depends_on:
      postgres: {condition: service_healthy}
      rabbitmq: {condition: service_healthy}

volumes:
  postgres-data: {}
```

### Writing a message from Laravel

`payload` is `BYTEA`, and PDO will not turn a PHP string into one on its own:
bind it through `convert_to(?, 'UTF8')`. The two JSON columns need an explicit
`::jsonb`. Everything else has a server default and belongs to the dispatcher.

```php
<?php

namespace App\Outbox;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class Outbox
{
    /**
     * Queue a message for delivery. Call it inside the transaction that carries
     * the business change — that is the whole point of the pattern.
     */
    public function publish(
        string $topic,
        array $payload,
        string $stream = 'local',
        array $target = [],
        array $headers = [],
    ): string {
        // A time-ordered id keeps the primary key index append-ordered, and it
        // is what a consumer deduplicates on. Str::uuid() is v4 — do not use it
        // here. Before Laravel 12: \Ramsey\Uuid\Uuid::uuid7()->toString().
        $id = (string) Str::uuid7();

        DB::insert(
            'INSERT INTO outbox.messages (id, stream, topic, payload, headers, target)
             VALUES (?, ?, ?, convert_to(?, \'UTF8\'), ?::jsonb, ?::jsonb)',
            [
                $id,
                $stream,
                $topic,
                json_encode($payload, JSON_THROW_ON_ERROR),
                json_encode($headers + ['content-type' => 'application/json'], JSON_THROW_ON_ERROR),
                json_encode((object) $target, JSON_THROW_ON_ERROR),
            ],
        );

        return $id;
    }
}
```

Used the way it is meant to be used:

```php
DB::transaction(function () use ($request, $outbox) {
    $order = Order::create($request->validated());

    $outbox->publish(
        topic: 'order.placed',
        payload: ['order_id' => $order->id, 'total' => (string) $order->total],
        target: ['key' => (string) $order->customer_id],   // Kafka partition key
    );
});
```

If the transaction rolls back, so does the message. That is the guarantee, and
it is the only thing a producer has to get right.

### Consuming

The dispatcher applies the driver's prefix and version suffix, so the consumer
subscribes to the **effective** name, not to what is in the `topic` column.
`GET http://localhost:8085/api/v1/stats` reports each driver's prefix and
separators, so the name can be derived without reading anyone's configuration.

With no prefix configured, `order.placed` is the queue name. Deduplicate on the
AMQP `message_id` property, which carries the outbox row id.

### What goes wrong here

- **Writing outside the transaction.** An Eloquent observer on `created` fires
  inside `Order::create`'s implicit transaction, but a queued job or an event
  listener dispatched with `afterCommit` does not. If the row is written after
  the commit, a crash in between loses the message — which is the failure the
  outbox exists to prevent.
- **Binding `payload` directly.** `DB::table('outbox.messages')->insert([...])`
  sends a text parameter for a `BYTEA` column and PostgreSQL refuses it. Use the
  raw statement above.
- **Hand-writing the schema as a Laravel migration.** It will drift from the one
  the dispatcher expects. Let `outbox migrate up` own the `outbox` schema, and
  keep Laravel's migrations to Laravel's tables.
- **Expecting a Laravel queue worker to consume these.** Laravel's queue driver
  expects its own serialized job envelope. This dispatcher publishes your bytes
  unchanged; either consume with `php-amqplib` directly, or make the payload a
  Laravel job envelope yourself.

---
