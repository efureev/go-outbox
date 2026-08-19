# Метрики и алерты

[English](MetricsAndAlerts.md) | Русский

Метрики Prometheus отдаются на порту `OUTBOX_METRICS_PORT` (по умолчанию 9100)
по пути `OUTBOX_METRICS_PATH`.

Два свойства, которые стоит знать до чтения списка. Серии для каждого сконфигурированного стрима и драйвера создаются на
старте, поэтому скрейп до первого сообщения отдаёт ноль, а не пустоту — а это разница между «ничего не падало» и
«метрики ещё не существует», которую выражение алерта иначе различить не может. И значения меток `stream` и `driver`
ограничены конфигурацией: имя, которого в конфигурации нет, схлопывается в `__unknown__`, так что продюсер не может
наплодить неограниченное число временных рядов, записав в колонку
`stream` что ему вздумается.

## Доставка

| Метрика                            | Тип       | Метки                   | Смысл                                                                                                                                                |
|------------------------------------|-----------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `outbox_messages_claimed_total`    | counter   | stream, driver, attempt | Сообщений взято в обработку. `attempt` — `initial` или `retry`.                                                                                      |
| `outbox_messages_dispatched_total` | counter   | stream, driver          | Сообщений принято брокером.                                                                                                                          |
| `outbox_messages_retried_total`    | counter   | stream, driver          | Сообщений возвращено в pending после отказа брокера.                                                                                                 |
| `outbox_messages_deferred_total`   | counter   | stream, driver          | Сообщений возвращено в pending **без траты попытки**, потому что до брокера не достучались. Растёт при неподвижном `retried_total` — это подпись аварии, а не отказов. |
| `outbox_messages_failed_total`     | counter   | stream, driver, reason  | Сообщений, которые встали. `reason` — `permanent`, `attempts_exhausted` или `unreachable`; последнее означает, что брокер не вернулся за `MAX_DEFER`, и счётчик попыток по-прежнему нулевой. |
| `outbox_delivery_lag_seconds`      | histogram | stream, driver          | Время от записи сообщения продюсером до приёма брокером. Считается целиком по часам БД, поэтому не вбирает в себя расхождение часов процесса и базы. |
| `outbox_publish_duration_seconds`  | histogram | stream, driver, result  | Время публикации.                                                                                                                                    |

## Backlog

| Метрика                             | Тип       | Метки  | Смысл                                                                                                                                                                  |
|-------------------------------------|-----------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `outbox_oldest_pending_age_seconds` | gauge     | —      | Сколько ждёт самое старое недоставленное сообщение. Самый ясный сигнал отставания доставки: счётчик говорит, сколько ждёт, а это — как долго.                          |
| `outbox_messages_by_status`         | gauge     | status | Строк в каждом нетерминальном статусе. Доставленные намеренно отсутствуют — считать их означает их сканировать, а `outbox_messages_dispatched_total` их уже покрывает. |
| `outbox_messages_deferred`          | gauge     | —                       | Сколько строк прямо сейчас ждёт недоступного брокера. Подмножество pending и processing, а не отдельный статус. Отличает бэклог, который движется медленно, от того, который не движется вовсе. |
| `outbox_batch_size`                 | histogram | stream | Сообщений на один захват. Стабильно упирается в максимум — диспетчер не успевает.                                                                                      |
| `outbox_iteration_duration_seconds` | histogram | stream | Один цикл «захват — публикация — запись результата».                                                                                                                   |

## Владение и восстановление

| Метрика                                   | Тип       | Метки  | Смысл                                                                                                                                                                            |
|-------------------------------------------|-----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `outbox_lease_conflicts_total`            | counter   | stream | Записей результата, отклонённых из-за того, что lease уже перезахватила другая реплика. **Должно быть ноль.** Иначе lease короче реальной работы и сообщения публикуются дважды. |
| `outbox_messages_reclaimed_total`         | counter   | stream | Истёкших lease возвращено в очередь. Ненулевое значение означает, что реплики умирают посреди батча либо lease слишком короткий.                                                 |
| `outbox_reclaimed_processing_age_seconds` | histogram | stream | Насколько lease был просрочен в момент перезахвата. Полезно, чтобы подбирать `LEASE_TTL` по фактам, а не на глаз.                                                                |

## Ошибки и служебные задачи

| Метрика                          | Тип     | Метки                       | Смысл                                                       |
|----------------------------------|---------|-----------------------------|-------------------------------------------------------------|
| `outbox_broker_errors_total`     | counter | stream, driver, stage, kind | Отказы брокера. `kind` разделяет `permanent` и `retryable`. |
| `outbox_db_errors_total`         | counter | op                          | Отказы базы, по операциям.                                  |
| `outbox_dlq_published_total`     | counter | stream, result              | Попытки пересылки в dead-letter.                            |
| `outbox_retention_deleted_total` | counter | —                           | Доставленных строк удалено при очистке.                     |

## Как читать их вместе

- `broker_errors_total` растёт вместе с `publish_duration_seconds` — брокер.
- `db_errors_total` растёт в одиночку — база или сам сервис, но не брокер.
- `delivery_lag_seconds` растёт при ровном `publish_duration_seconds` — это backlog, а не медленный брокер. Смотрите
  `batch_size` и `oldest_pending_age`.
- `messages_reclaimed_total` растёт — реплики умирают посреди батча либо
  `LEASE_TTL` выставлен ниже реального времени обработки батча.
- `lease_conflicts_total` больше нуля — та же причина, но теперь она уже стоит дублирующих публикаций.
- `messages_failed_total{reason="permanent"}` растёт — ошибка конфигурации или маршрутизации, а не авария. Повторы не
  помогут и не выполняются.

## Стартовые алерты

Пороги — отправная точка. Откалибруйте их под SLA доставки, размер батча и допустимый уровень повторов.

```yaml
groups:
  - name: outbox
    rules:
      - alert: OutboxBacklogAgeHigh
        expr: outbox_oldest_pending_age_seconds > 300
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: Самое старое недоставленное сообщение ждёт больше пяти минут.

      - alert: OutboxLeaseConflicts
        expr: increase(outbox_lease_conflicts_total[15m]) > 0
        labels: { severity: warning }
        annotations:
          summary: Lease истёк в процессе публикации, сообщения ушли дважды.
          description: >
            OUTBOX_DISPATCH_LEASE_TTL короче реального времени обработки батча.
            Увеличьте его либо уменьшите OUTBOX_DISPATCH_BATCH_SIZE.

      - alert: OutboxMessagesFailing
        expr: increase(outbox_messages_failed_total[15m]) > 0
        labels: { severity: warning }
        annotations:
          summary: Сообщения перестали переотправляться и требуют внимания.
          description: >
            Разберите их через GET /api/v1/messages/failed, затем верните в
            очередь через POST /api/v1/messages/requeue, устранив причину.

      - alert: OutboxPermanentFailures
        expr: increase(outbox_messages_failed_total{reason="permanent"}[15m]) > 0
        labels: { severity: warning }
        annotations:
          summary: Сообщения отвергаются сразу — это указывает на конфигурацию.

      - alert: OutboxPublishErrorsHigh
        expr: sum(rate(outbox_broker_errors_total{kind="retryable"}[5m])) > 1
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: Устойчивые отказы публикации в брокер.

      - alert: OutboxBrokerUnreachable
        expr: sum by (stream) (rate(outbox_messages_deferred_total[5m])) > 0
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: Брокер недоступен десять минут, и его стрим стоит.
          description: >
            Сообщения целы, счётчики попыток не тронуты — они уйдут сами, когда
            брокер вернётся. Возвращать в очередь ничего не нужно. Сколько именно
            ждёт, показывает outbox_messages_deferred.

      - alert: OutboxReclaimingLeases
        expr: increase(outbox_messages_reclaimed_total[15m]) > 0
        labels: { severity: warning }
        annotations:
          summary: Lease истекают — реплики умирают посреди батча либо lease слишком короткий.

      - alert: OutboxDatabaseErrors
        expr: increase(outbox_db_errors_total[5m]) > 0
        labels: { severity: warning }
        annotations:
          summary: Диспетчер не может достучаться до своей базы или работать с ней.

      - alert: OutboxNotDispatching
        expr: >
          outbox_messages_by_status{status="pending"} > 0
          and rate(outbox_messages_dispatched_total[10m]) == 0
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: Есть backlog, и при этом ничего не доставляется.
```
