# Metrics and alerts

English | [Русский](MetricsAndAlerts.ru.md)

Prometheus metrics are served on `OUTBOX_METRICS_PORT` (default 9100) at
`OUTBOX_METRICS_PATH`.

Two properties worth knowing before reading the list. Series for every
configured stream and driver are created at startup, so a scrape before the
first message reports a zero rather than nothing — the difference between
"nothing has failed" and "the metric does not exist yet", which an alert
expression cannot otherwise tell apart. And the `stream` and `driver` label
values are bounded by the configuration: a name that is not configured collapses
into `__unknown__`, so a producer cannot mint unbounded time series by writing
whatever it likes into the `stream` column.

## Delivery

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `outbox_messages_claimed_total` | counter | stream, driver, attempt | Messages taken into processing. `attempt` is `initial` or `retry`. |
| `outbox_messages_dispatched_total` | counter | stream, driver | Messages a broker accepted. |
| `outbox_messages_retried_total` | counter | stream, driver | Messages returned to pending after a broker rejected them. |
| `outbox_messages_deferred_total` | counter | stream, driver | Messages returned to pending **without spending an attempt**, because the broker could not be reached. Rising while `retried_total` stays flat is the signature of an outage rather than of messages being refused. |
| `outbox_messages_failed_total` | counter | stream, driver, reason | Messages that stopped. `reason` is `permanent`, `attempts_exhausted`, or `unreachable` — the last means the broker never came back within `MAX_DEFER`, so the attempt counter still reads zero. |
| `outbox_delivery_lag_seconds` | histogram | stream, driver | Time from a producer writing a message to a broker accepting it. Measured entirely by the database clock, so it does not absorb the difference between this process's clock and the database's. |
| `outbox_publish_duration_seconds` | histogram | stream, driver, result | Time spent publishing. |

## Backlog

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `outbox_oldest_pending_age_seconds` | gauge | — | How long the oldest undelivered message has waited. The clearest signal that delivery is falling behind: a count says how much is waiting, this says how long. |
| `outbox_messages_by_status` | gauge | status | Rows in each non-terminal status. Delivered rows are deliberately absent — counting them means scanning them, and `outbox_messages_dispatched_total` already covers it. |
| `outbox_messages_deferred` | gauge | — | Rows waiting on a broker that could not be reached, right now. A subset of pending and processing, not a status of its own. It is what separates a backlog that is moving slowly from one that is not moving at all. |
| `outbox_stream_paused` | gauge | stream | `1` while the dispatcher has stopped claiming for a stream because its broker is unreachable. Claims resume on their own once one gets through. This is the signal that outlives the condition it reports: while it is `1`, nothing is published, so `outbox_messages_deferred_total` stops advancing. |
| `outbox_batch_size` | histogram | stream | Messages per claim. Consistently at the configured maximum means the dispatcher is behind. |
| `outbox_iteration_duration_seconds` | histogram | stream | One claim-publish-write-back cycle. |

## Ownership and recovery

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `outbox_lease_conflicts_total` | counter | stream | Write-backs rejected because another replica had reclaimed the lease. **This should be zero.** Anything else means the lease is shorter than the work, and messages are being published twice. |
| `outbox_messages_reclaimed_total` | counter | stream | Expired leases returned to the queue. Nonzero means replicas are dying mid-batch, or the lease is too short. |
| `outbox_reclaimed_processing_age_seconds` | histogram | stream | How overdue a lease was when it was reclaimed. Useful for choosing `LEASE_TTL` from evidence rather than by guess. |

## Errors and housekeeping

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `outbox_broker_errors_total` | counter | stream, driver, stage, kind | Broker failures. `kind` separates `permanent` from `retryable`. |
| `outbox_db_errors_total` | counter | op | Database failures, by operation. |
| `outbox_dlq_published_total` | counter | stream, result | Dead-letter forwarding attempts. |
| `outbox_retention_deleted_total` | counter | — | Delivered rows removed by the sweep. |

## Reading them together

- `broker_errors_total` rising with `publish_duration_seconds` — the broker.
- `db_errors_total` rising alone — the database or this service, not the broker.
- `delivery_lag_seconds` rising while `publish_duration_seconds` is flat — a
  backlog, not a slow broker. Check `batch_size` and `oldest_pending_age`.
- `messages_reclaimed_total` rising — replicas dying mid-batch, or `LEASE_TTL`
  set below the time a batch actually takes.
- `lease_conflicts_total` above zero — the same cause, now actually costing
  duplicate publications.
- `messages_failed_total{reason="permanent"}` rising — a configuration or
  routing mistake, not an outage. Retrying will not help and is not happening.

## Starting alert rules

Thresholds are a starting point. Calibrate them against the delivery SLA, the
batch size and the acceptable retry rate.

```yaml
groups:
  - name: outbox
    rules:
      - alert: OutboxBacklogAgeHigh
        expr: outbox_oldest_pending_age_seconds > 300
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: The oldest undelivered message has been waiting over five minutes.

      - alert: OutboxLeaseConflicts
        expr: increase(outbox_lease_conflicts_total[15m]) > 0
        labels: {severity: warning}
        annotations:
          summary: A lease expired mid-flight, so messages were published twice.
          description: >
            OUTBOX_DISPATCH_LEASE_TTL is shorter than the time a batch actually
            takes. Raise it, or lower OUTBOX_DISPATCH_BATCH_SIZE.

      - alert: OutboxMessagesFailing
        expr: increase(outbox_messages_failed_total[15m]) > 0
        labels: {severity: warning}
        annotations:
          summary: Messages have stopped being retried and need attention.
          description: >
            Inspect them at GET /api/v1/messages/failed, then requeue with
            POST /api/v1/messages/requeue once the cause is fixed.

      - alert: OutboxPermanentFailures
        expr: increase(outbox_messages_failed_total{reason="permanent"}[15m]) > 0
        labels: {severity: warning}
        annotations:
          summary: Messages are being rejected outright, which points at configuration.

      - alert: OutboxPublishErrorsHigh
        expr: sum(rate(outbox_broker_errors_total{kind="retryable"}[5m])) > 1
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: Sustained publish failures against a broker.

      - alert: OutboxBrokerUnreachable
        # Two expressions, because the first one goes quiet on purpose. Once the
        # dispatcher stops claiming for an unreachable broker it publishes
        # nothing, so it defers nothing either, and the rate falls to zero
        # exactly when the outage is most established. outbox_stream_paused is
        # what stays true; the rate covers the interval before claims stop and
        # streams too quiet for the pause to hold.
        expr: >
          max by (stream) (outbox_stream_paused) == 1
          or sum by (stream) (rate(outbox_messages_deferred_total[5m])) > 0
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: A broker has been unreachable for ten minutes and its stream is not moving.
          description: >
            The messages are intact and their attempt counters untouched — they
            will go out on their own when the broker returns. Nothing here needs
            a requeue. Check outbox_messages_deferred for how many are waiting.

      - alert: OutboxReclaimingLeases
        expr: increase(outbox_messages_reclaimed_total[15m]) > 0
        labels: {severity: warning}
        annotations:
          summary: Leases are expiring, so replicas are dying mid-batch or the lease is too short.

      - alert: OutboxDatabaseErrors
        expr: increase(outbox_db_errors_total[5m]) > 0
        labels: {severity: warning}
        annotations:
          summary: The dispatcher cannot reach or use its database.

      - alert: OutboxNotDispatching
        expr: >
          outbox_messages_by_status{status="pending"} > 0
          and rate(outbox_messages_dispatched_total[10m]) == 0
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: There is a backlog and nothing is being delivered.
```
