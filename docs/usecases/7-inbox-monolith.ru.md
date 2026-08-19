# Модульный монолит: инбокс вместо брокера

[English](7-inbox-monolith.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).

Приложение разбито на ограниченные контексты — заказы, биллинг, уведомления, — но
база одна. Заказы публикуют событие, биллинг его читает. **Брокера нет вообще.**

Диспетчер доставляет в таблицу: `OUTBOX_DRIVER_*_TYPE=postgres`. Продюсер об этом
не знает — он пишет строку в outbox ровно так же, как писал бы для RabbitMQ.

### Инбокс

Таблицу создаёт её владелец — тот контекст, который будет из неё читать. Полный
пример с обоснованием — в
[migrations/inbox/messages.sql](../../migrations/inbox/messages.sql).

```sql
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE billing.inbox
(
    -- Пишет диспетчер. Только INSERT.
    id      UUID PRIMARY KEY,   -- id строки outbox; на нём держится дедупликация
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,     -- эффективное имя: с префиксом и версией
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ваши колонки. Диспетчер о них не знает.
    processed_at TIMESTAMPTZ
);

CREATE INDEX inbox_unprocessed_idx ON billing.inbox (received_at)
    WHERE processed_at IS NULL;
```

`PRIMARY KEY (id)` — не украшение. Доставка at-least-once: вставка сюда и пометка
строки outbox как `sent` — два коммита, и реплика может умереть между ними.
Первичный ключ делает повтор безвредным, потому что драйвер вставляет с
`ON CONFLICT (id) DO NOTHING` и считает конфликт доставкой.

### Конфигурация

```dotenv
OUTBOX_DB_DSN=postgres://app:secret@localhost:5432/app?sslmode=disable
OUTBOX_DB_SCHEMA=outbox

OUTBOX_STREAMS=billing,notifications

OUTBOX_STREAM_BILLING_DRIVER=inb_billing
OUTBOX_DRIVER_INB_BILLING_TYPE=postgres
OUTBOX_DRIVER_INB_BILLING_DSN=              # пусто: база самого диспетчера
OUTBOX_DRIVER_INB_BILLING_SCHEMA=billing
OUTBOX_DRIVER_INB_BILLING_TABLE=inbox

OUTBOX_STREAM_NOTIFICATIONS_DRIVER=inb_notify
OUTBOX_DRIVER_INB_NOTIFY_TYPE=postgres
OUTBOX_DRIVER_INB_NOTIFY_SCHEMA=notifications
OUTBOX_DRIVER_INB_NOTIFY_TABLE=inbox
```

Пустой `DSN` означает базу, из которой диспетчер и так читает свой outbox. Ничего
дублировать не надо, и две строки подключения не разъедутся.

### Продюсер не меняется

```go
// Заказы. Про billing.inbox здесь не знают и знать не должны.
err := outbox.Enqueue(ctx, tx, outboxclient.Message{
    Stream:  "billing",
    Topic:   "order.paid",
    Payload: payload,
})
```

Стрим `billing` сегодня уходит в таблицу, завтра — в RabbitMQ, послезавтра в
Kafka. Переключение происходит переменной окружения диспетчера, а не правкой
кода приложения.

### Чтение инбокса

Это ваша работа, а не диспетчера. Он вставляет и всё; ни цикла обработки, ни
lease, ни фреймворка консьюмера здесь нет — и не будет.

```sql
-- Простейший вариант, внутри вашей транзакции.
SELECT id, topic, payload, headers
  FROM billing.inbox
 WHERE processed_at IS NULL
 ORDER BY received_at
 LIMIT 100
   FOR UPDATE SKIP LOCKED;
```

Обработали — проставили `processed_at` в той же транзакции. Дедупликация вам не
нужна: `id` уникален, и повтор до вас просто не доедет.

### Локальный контур

Побочная выгода, которая на практике часто оказывается главной: всё приложение
целиком поднимается одной командой.

```bash
docker compose up postgres
```

Ни RabbitMQ, ни Redpanda в `docker-compose.yml` разработчика. То же самое в CI:
интеграционные тесты не требуют брокера, потому что его нет.

### Что здесь ломается

- **Забытая уборка инбокса.** Наш janitor чистит **свою** таблицу — outbox.
  Инбокс он не трогает, и тот растёт молча: ни метрики, ни лога с нашей стороны
  не появится, потому что мы туда не смотрим. Это единственный отказ, который
  проявляется через полгода, а не сразу. Чистите сами:
  `DELETE FROM billing.inbox WHERE processed_at < now() - interval '7 days'`.
- **Ожидание fan-out.** Инбокс — точка-точка. Чтобы событие попало и в биллинг,
  и в уведомления, продюсер пишет **две строки** — по одной на стрим. Появление
  третьего потребителя станет правкой кода продюсера. Если веер нужен, нужен
  брокер.
- **Отсутствие `PRIMARY KEY (id)` на инбоксе.** Тогда дедупликации нет, и повтор
  после смерти реплики станет вторым сообщением. Драйвер этого не заметит: он
  вставляет и получает успех.
- **Схема и таблица, не совпадающие с конфигурацией.** Диспетчер откажется
  стартовать — таблица должна существовать, он её не создаёт. Это сделано
  нарочно: адресат, которого нет, — ошибка конфигурации, и ей место на старте.
- **Попытка указать адресатом саму таблицу outbox.** Отвергается на старте: это
  доставка самому себе, где каждое опубликованное сообщение становится новым
  сообщением к публикации.

---
