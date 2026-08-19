# Use cases

English | [Русский](UseCases.ru.md)

Six deployments, from a laptop to a cluster. Each is complete enough to copy, and
each ends with the mistakes that environment invites — which is the part worth
reading twice.

One rule runs through all of them: **the message is written in the same
transaction as the business change**. Everything else is plumbing.

| | Recipe | For |
|---|---|---|
| 1 | [Laravel, PostgreSQL 18 and RabbitMQ under Docker Compose](usecases/1-laravel-docker.md) | A PHP application on a laptop or one host |
| 2 | [One VDS, supervisord](usecases/2-vds-supervisord.md) | One machine, the dispatcher as a supervised process |
| 3 | [Kubernetes, Kafka, autoscaled on backlog age](usecases/3-kubernetes-kafka.md) | A cluster, with the replica count following the backlog |
| 4 | [Bare metal, systemd, several instances on one host](usecases/4-systemd-baremetal.md) | Several dispatchers on one machine |
| 5 | [Four streams across three RabbitMQ instances](usecases/5-multi-rabbitmq.md) | One producer writing to several brokers |
| 6 | [A Go producer on database/sql](usecases/6-database-sql.md) | A service on `sqlx`, `gorm` or the standard library |

They used to be one document. It grew past the point where anybody read it end
to end, so each is now its own page and this is the index.

## Where to go next

- [Public contract](PublicContract.md) — the columns a producer writes and what
  is explicitly not guaranteed.
- [Configuration](Config.md) — every environment variable.
- [Operations](Operations.md) — scaling, and what to do when something is wrong.
- [Metrics and alerts](MetricsAndAlerts.md).
