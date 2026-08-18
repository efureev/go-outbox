# Переезд с v2

[English](Migration-v2.md) | Русский

Это переписывание с нуля, а не обновление. Меняются схема, конфигурация и путь
модуля. Ниже — что изменилось, почему, и как перевести работающую инсталляцию.

## Почему не обновление на месте

Четыре дефекта v2 нельзя исправить, не меняя таблицу.

**Запись результата не проверяла владение lease.** `UpdateSentMessages` и
`UpdateFailedMessages` обновляли строки только по id. Реплика, у которой
processing timeout истёк во время публикации, перетирала статус строки, которую
другая реплика уже перезахватила и доставила, — доставленное сообщение
воскресало и уходило повторно, и так по кругу. Исправление требует lease-токена,
а это новая колонка.

**Документированная процедура восстановления не работала.** И
`PublicContract.md`, и `Integration.md` предписывали потребителю выполнить:

```sql
UPDATE tech.outbox_messages SET status='pending', last_try_at=NULL
WHERE id = $1 AND status='failed';
```

Переход в `failed` при этом выставлял `next_retry_at` в `NULL`, а claim-запрос
требовал `next_retry_at IS NOT NULL`. Сообщение, реанимированное по инструкции,
больше никогда не выбиралось. `retry_count` тоже оставался на максимуме, так что
даже с исправленным `next_retry_at` оно упало бы с первой же ошибки.

**Подтверждения RabbitMQ могли перепутаться.** Один канал `NotifyPublish`
использовался всеми публикациями, и после каждой читалось ровно одно значение.
Когда публикация упиралась в таймаут, её подтверждение оставалось в очереди, и
**следующее** сообщение забирало его себе — и помечалось доставленным, ни разу
не будучи подтверждённым брокером. Это тихая потеря сообщений.

**Таблица росла бесконечно, и её пересчитывали на каждом опросе.**
`CountByStatuses` выполнял `GROUP BY status` по всей таблице каждые
`APP_POLLING_INTERVAL`, а доставленные строки не удалял никто.

Вдобавок потолок пропускной способности был структурным: один батч за интервал
опроса, публикуемый последовательно под мьютексом единственного канала, с
round-trip'ом `QueueDeclare` на каждое сообщение. Сто сообщений за десять
секунд — независимо от железа под ними.

## Что изменилось

| v2 | v3 | Почему |
|---|---|---|
| `tech.outbox_messages` | `outbox.messages`, оба параметра настраиваемы | Схема была захардкожена в SQL. |
| `queue` | `topic` | В Kafka это топик; `queue` подходило только к AMQP. |
| `message TEXT` | `payload BYTEA` | Бинарные payload'ы доходят неповреждёнными. |
| `status VARCHAR(255)` | `status SMALLINT` + CHECK | Набор закрыт, а 255 байт утверждали обратное. |
| `retry_count` | `attempts` | Считаются попытки, включая первую, которая повтором не является. |
| `next_retry_at` | `available_at NOT NULL` | Никогда не NULL, поэтому NULL больше не может сделать строку недостижимой. |
| `processing_started_at` | `lease_token`, `lease_until`, `owner` | Владение, а не просто отметка времени. |
| колонка `version SMALLINT` | `target.version` | Она версионировала имя топика, а не строку. |
| — | `headers JSONB` | Trace-контекст, content type и всё, что нужно потребителю. |
| — | `last_error` | Почему сообщение встало, без чтения логов. |
| `APP_*`, `BROKER_*`, `DRIVER_*` | всё под `OUTBOX_` | Одно пространство имён. |
| длительности строками, парсинг при использовании | `time.Duration`, парсинг при загрузке | Опечатка останавливает процесс, а не порождает вечно unready сервис. |
| gin, uber/fx, шесть внутренних пакетов | net/http, appmod | Переносимость. |

Что осталось прежним: модель стримов и драйверов, правило именования топиков
(`prefix + sep + topic + sep + vN`) вместе с его умолчаниями по драйверам, и
доставка at-least-once.

## Перевод инсталляции

Две версии могут работать бок о бок, потому что читают разные таблицы. Это
безопасный путь.

**1. Создайте новую схему рядом со старой.**

```bash
OUTBOX_DB_SCHEMA=outbox go run ./cmd/outbox migrate up
```

**2. Переключите продюсеров на новую таблицу.** Измените INSERT — или перейдите
на `pkg/outboxclient`, — сохранив запись внутри той же бизнес-транзакции:

```sql
-- v2
INSERT INTO tech.outbox_messages (id, status, queue, message, target, created_at)
VALUES ($1, 'pending', $2, $3, $4::jsonb, NOW());

-- v3: без status, без created_at, и stream — отдельная колонка
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES ($1, $2, $3, $4, $5::jsonb);
```

`stream` переезжает из `target` в собственную колонку; всё прочее в `target`
переносится без изменений.

**3. Держите оба диспетчера запущенными**, пока в таблицу v2 не перестанут
поступать строки и её backlog не дойдёт до нуля:

```sql
SELECT status, count(*) FROM tech.outbox_messages
WHERE status IN ('pending', 'processing') GROUP BY status;
```

**4. Остановите v2 и разберитесь с тем, что осталось.** Всё, что застряло в
`failed`, можно перенести, а не потерять:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
SELECT id,
       lower(target ->> 'stream'),
       queue || CASE WHEN version > 0 THEN '_v' || version ELSE '' END,
       convert_to(message, 'UTF8'),
       target - 'stream'
FROM tech.outbox_messages
WHERE status = 'failed';
```

Обратите внимание: суффикс версии здесь вплавляется в имя топика. Выставить
вместо этого `target.version` было бы эквивалентно; вплавление избавляет от
зависимости от того, совпадает ли разделитель у нового драйвера со старым.

**5. Удалите старую таблицу**, когда к ней никто не обращался в течение периода
хранения.

## Если запустить обе версии нельзя

Возможен и одномоментный переход: остановить продюсеров, дать v2 разгрестись до
нуля, перенести строки, запустить v3, запустить продюсеров. Ценой будет окно, в
котором ни одна бизнес-транзакция, пишущая outbox-сообщение, не сможет
закоммититься, — обычно это обходится дороже.

**Не** направляйте v3 на таблицу v2. Нужных ей колонок там нет, а гарантии,
которые она даёт, опираются именно на них.

## Конфигурация

| v2 | v3 |
|---|---|
| `APP_POLLING_INTERVAL` | `OUTBOX_DISPATCH_POLL_INTERVAL` |
| `APP_PROCESSING_TIMEOUT` | `OUTBOX_DISPATCH_LEASE_TTL` |
| `APP_LIMIT_PER_ITERATION` | `OUTBOX_DISPATCH_BATCH_SIZE` |
| `APP_BASE_DELAY` (секунды) | `OUTBOX_DISPATCH_BACKOFF_BASE` (длительность) |
| `APP_MAX_RETRY_COUNT` | `OUTBOX_DISPATCH_MAX_ATTEMPTS` |
| `BROKER_STREAMS` | `OUTBOX_STREAMS` |
| `STREAM_<S>_DRIVER` | `OUTBOX_STREAM_<S>_DRIVER` |
| `DRIVER_<D>_*` | `OUTBOX_DRIVER_<D>_*` |
| `DB_*` | `OUTBOX_DB_*` |
| `PROM_PORT`, `PROM_PATH` | `OUTBOX_METRICS_PORT`, `OUTBOX_METRICS_PATH` |
| `APP_AUTH_SECRET` | удалено; см. `OUTBOX_HTTP_ADMIN_TOKEN` |

Новое, что стоит выставить осознанно: `OUTBOX_JANITOR_RETENTION` (в v2 ретенции
не было вовсе, и таблица росла вечно) и `OUTBOX_DISPATCH_BACKOFF_MAX` (в v2
удвоение шло без потолка).

## Метрики

Имена изменились достаточно, чтобы дашборды пришлось править. Соответствие:

| v2 | v3 |
|---|---|
| `outbox_messages_claimed_total{source,…}` | `outbox_messages_claimed_total{…}` + `outbox_messages_reclaimed_total` |
| `outbox_messages_processed_total{status="sent"}` | `outbox_messages_dispatched_total` |
| `outbox_messages_processed_total{status="requeued"}` | `outbox_messages_retried_total` |
| `outbox_messages_failed_total` | то же, теперь с `stream`, `driver` и `reason` |
| `outbox_processing_errors_total{stage}` | `outbox_db_errors_total{op}` |
| `outbox_broker_operation_errors_total` | `outbox_broker_errors_total`, теперь с `kind` |
| `outbox_pending_messages` | удалена; дублировала `outbox_messages_by_status` |
| — | `outbox_oldest_pending_age_seconds`, `outbox_lease_conflicts_total`, `outbox_batch_size` |
