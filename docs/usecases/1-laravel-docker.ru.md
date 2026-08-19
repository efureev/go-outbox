# Laravel, PostgreSQL 18 и RabbitMQ под Docker Compose

[English](1-laravel-docker.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).


Локальная разработка: приложение на Laravel пишет строки в outbox, диспетчер
работает рядом сайдкаром, потребитель читает из RabbitMQ.

### Compose

```yaml
services:
  app:                                  # ваш контейнер с Laravel
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
    # PostgreSQL 18 ожидает монтирование на уровень выше каталога данных.
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
    ports: ["15672:15672"]              # веб-интерфейс управления
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      retries: 15

  outbox:
    image: ghcr.io/efureev/go-outbox:1.0.0
    environment:
      OUTBOX_DB_DSN: postgres://app:secret@postgres:5432/app?sslmode=disable
      OUTBOX_DB_SCHEMA: outbox
      # Локально — нормально. В проде миграции применяют отдельным шагом.
      OUTBOX_DB_AUTO_MIGRATE: "true"
      OUTBOX_LOG_FORMAT: console
      OUTBOX_STREAMS: local
      OUTBOX_STREAM_LOCAL_DRIVER: rmq
      OUTBOX_DRIVER_RMQ_TYPE: rabbitmq
      OUTBOX_DRIVER_RMQ_DSN: amqp://app:secret@rabbitmq:5672/
      # Диспетчер объявляет очереди, чтобы свежий чекаут имел куда публиковать.
      # Выключите, когда топологией начнёт владеть потребитель.
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

### Запись сообщения из Laravel

`payload` — это `BYTEA`, и PDO сам строку PHP в него не превратит: привязывайте
через `convert_to(?, 'UTF8')`. Двум JSON-колонкам нужен явный `::jsonb`. У всего
остального есть серверные значения по умолчанию, и принадлежит оно диспетчеру.

```php
<?php

namespace App\Outbox;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class Outbox
{
    /**
     * Поставить сообщение в очередь на доставку. Вызывайте внутри транзакции,
     * несущей бизнес-изменение, — в этом весь смысл паттерна.
     */
    public function publish(
        string $topic,
        array $payload,
        string $stream = 'local',
        array $target = [],
        array $headers = [],
    ): string {
        // Упорядоченный по времени id сохраняет индекс первичного ключа
        // append-упорядоченным, и по нему же потребитель дедуплицирует.
        // Str::uuid() — это v4, здесь он не подходит.
        // До Laravel 12: \Ramsey\Uuid\Uuid::uuid7()->toString().
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

Использование ровно так, как задумано:

```php
DB::transaction(function () use ($request, $outbox) {
    $order = Order::create($request->validated());

    $outbox->publish(
        topic: 'order.placed',
        payload: ['order_id' => $order->id, 'total' => (string) $order->total],
        target: ['key' => (string) $order->customer_id],   // ключ партиции Kafka
    );
});
```

Откатится транзакция — откатится и сообщение. Это и есть гарантия, и это
единственное, что продюсер обязан сделать правильно.

### Потребление

Диспетчер применяет префикс драйвера и суффикс версии, поэтому потребитель
подписывается на **эффективное** имя, а не на то, что лежит в колонке `topic`.
`GET http://localhost:8085/api/v1/stats` отдаёт префикс и разделители каждого
драйвера — имя можно вывести, не читая чужой конфигурационный файл.

Без настроенного префикса имя очереди — `order.placed`. Дедуплицируйте по
AMQP-свойству `message_id`: в нём лежит id строки outbox.

### Что здесь ломается

- **Запись вне транзакции.** Eloquent-обсервер на `created` срабатывает внутри
  неявной транзакции `Order::create`, а вот отложенная джоба или слушатель
  события с `afterCommit` — уже нет. Если строка пишется после коммита, падение
  в промежутке теряет сообщение — то есть ровно тот отказ, ради предотвращения
  которого outbox и существует.
- **Прямая привязка `payload`.** `DB::table('outbox.messages')->insert([...])`
  отправляет текстовый параметр в колонку `BYTEA`, и PostgreSQL его отвергает.
  Используйте сырой запрос выше.
- **Рукописная схема в виде миграции Laravel.** Она разойдётся с той, которую
  ожидает диспетчер. Пусть схемой `outbox` владеет `outbox migrate up`, а
  миграции Laravel остаются для таблиц Laravel.
- **Расчёт на то, что эти сообщения прочитает воркер очередей Laravel.** Драйвер
  очередей Laravel ожидает собственный сериализованный конверт джобы. Диспетчер
  публикует ваши байты без изменений: либо читайте напрямую через `php-amqplib`,
  либо сами формируйте payload как конверт джобы Laravel.

---
