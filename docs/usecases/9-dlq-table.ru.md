# Мёртвые письма в таблицу, а не в топик

[English](9-dlq-table.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).

Сообщения, которые перестали переотправляться, складываются в таблицу — их
читают глазами и `SELECT`'ом, а не подписчиком.

Мотив простой: очередь мёртвых писем в брокере почти всегда оказывается очередью,
на которую никто не подписан. Разбирает её человек, а человеку удобнее `WHERE` и
`ORDER BY`, чем консьюмер, написанный ради того, чтобы посмотреть.

Работает это **без единой правки в коде**: форвардер мёртвых писем публикует в
обычный стрим через тот же роутер, поэтому достаточно направить DLQ-стрим на
драйвер типа `postgres`.

### Таблица

```sql
CREATE TABLE ops.dead_letter
(
    id      UUID PRIMARY KEY,
    stream  TEXT  NOT NULL,
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Форвардер кладёт обстоятельства в заголовки; по ним и разбирают.
CREATE INDEX dead_letter_origin_idx
    ON ops.dead_letter ((headers ->> 'x-outbox-original-stream'), received_at);
```

### Конфигурация

```dotenv
OUTBOX_DLQ_ENABLED=true
OUTBOX_DLQ_STREAM=dead_letter
OUTBOX_DLQ_TOPIC=outbox.dead-letter

OUTBOX_STREAMS=events,dead_letter

OUTBOX_STREAM_DEAD_LETTER_DRIVER=dlq_table
OUTBOX_DRIVER_DLQ_TABLE_TYPE=postgres
OUTBOX_DRIVER_DLQ_TABLE_DSN=            # пусто: база самого диспетчера
OUTBOX_DRIVER_DLQ_TABLE_SCHEMA=ops
OUTBOX_DRIVER_DLQ_TABLE_TABLE=dead_letter
```

### Как это выглядит при разборе

Форвардер добавляет к сообщению обстоятельства его смерти:

```sql
SELECT headers ->> 'x-outbox-original-stream' AS stream,
       headers ->> 'x-outbox-original-topic'  AS topic,
       headers ->> 'x-outbox-attempts'        AS attempts,
       headers ->> 'x-outbox-permanent'       AS permanent,
       count(*)
  FROM ops.dead_letter
 WHERE received_at > now() - interval '1 day'
 GROUP BY 1, 2, 3, 4
 ORDER BY count(*) DESC;
```

Один запрос отвечает на вопрос, ради которого в брокере пришлось бы написать
консьюмера: что именно и почему перестало отправляться за сутки.

### Это сигнал, а не запись

Строка в outbox остаётся на месте со статусом `failed` — она и есть источник
истины. Таблица мёртвых писем существует, чтобы на неё **посмотрели**; возвращают
сообщения в очередь по-прежнему через `outbox requeue` или
`POST /api/v1/messages/requeue`, и работают они с outbox, а не с этой таблицей.

Отсюда и следствие: удалить строку отсюда безопасно. Ничего не потеряется.

### Что здесь ломается

- **Попытка возвращать в очередь из этой таблицы.** Не сработает: requeue
  меняет статус в outbox. Здесь лежит копия для чтения.
- **`OUTBOX_DLQ_STREAM`, не входящий в `OUTBOX_STREAMS`.** Отвергается на старте
  при разборе конфигурации, вместе со всеми остальными ошибками сразу.
- **Забытая уборка.** Как и любой инбокс, эта таблица наша только на запись.
  Чистить её — ваша задача, и здесь это проще всего: строки старше месяца можно
  удалять без оглядки, потому что источник истины в outbox.
- **Ожидание, что сюда попадёт всё упавшее.** Сюда попадает то, что **перестало
  переотправляться**. Сообщение, которое ждёт следующей попытки или отложено
  из-за недоступного брокера, здесь не появится — и не должно.

---
