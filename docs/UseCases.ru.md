# Сценарии использования

[English](UseCases.md) | Русский

Четыре варианта развёртывания — от ноутбука до кластера. Каждый достаточно
полон, чтобы его скопировать, и каждый заканчивается разбором ошибок, к которым
располагает именно эта среда, — ради этой части документ и стоит читать.

Через все четыре проходит одно правило: **сообщение пишется в той же транзакции,
что и бизнес-изменение**. Всё остальное здесь — обвязка.

- [1. Laravel, PostgreSQL 18 и RabbitMQ под Docker Compose](#1-laravel-postgresql-18-и-rabbitmq-под-docker-compose)
- [2. Один VDS, supervisord](#2-один-vds-supervisord)
- [3. Kubernetes, Kafka, автомасштабирование по возрасту backlog'а](#3-kubernetes-kafka-автомасштабирование-по-возрасту-backlogа)
- [4. Bare metal, systemd, несколько инстансов на одной машине](#4-bare-metal-systemd-несколько-инстансов-на-одной-машине)
- [5. Четыре стрима на трёх инстансах RabbitMQ](#5-четыре-стрима-на-трёх-инстансах-rabbitmq)

---

## 1. Laravel, PostgreSQL 18 и RabbitMQ под Docker Compose

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

## 2. Один VDS, supervisord

Одна машина, приложение и PostgreSQL уже на ней, диспетчер — как процесс под
присмотром.

### Установка

```bash
VERSION=1.0.0
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/outbox_${VERSION}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf "outbox_${VERSION}_linux_amd64.tar.gz"

# Бинарник статический: ни рантайма, ни libc подбирать не нужно.
install -o root -g root -m 0755 "outbox_${VERSION}_linux_amd64/outbox" /usr/local/bin/outbox

useradd --system --no-create-home --shell /usr/sbin/nologin outbox
install -d -o root -g outbox -m 0750 /etc/outbox
```

`/etc/outbox/outbox.env`, режим `0640`, владелец `root:outbox` — внутри пароль:

```dotenv
OUTBOX_DB_DSN=postgres://outbox:secret@127.0.0.1:5432/app?sslmode=disable
OUTBOX_DB_SCHEMA=outbox
OUTBOX_LOG_FORMAT=json

OUTBOX_STREAMS=local
OUTBOX_STREAM_LOCAL_DRIVER=rmq
OUTBOX_DRIVER_RMQ_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_DSN=amqp://app:secret@127.0.0.1:5672/

OUTBOX_HTTP_PORT=8085
OUTBOX_METRICS_PORT=9100
OUTBOX_APP_SHUTDOWN_TIMEOUT=30s
```

Схема применяется один раз, осознанным шагом:

```bash
set -a; . /etc/outbox/outbox.env; set +a
outbox migrate status
outbox migrate up
```

### supervisord

`/etc/supervisor/conf.d/outbox.conf`:

```ini
[program:outbox]
; supervisord не читает env-файлы, поэтому его загружает шелл — а exec этот шелл
; заменяет, иначе сигнал остановит /bin/sh и оставит диспетчер работать.
command=/bin/sh -c 'set -a; . /etc/outbox/outbox.env; set +a; exec /usr/local/bin/outbox run'
user=outbox
directory=/var/lib/outbox

autostart=true
autorestart=true
startsecs=5

; SIGTERM запускает штатное завершение: перестать делать захваты, дописать
; текущий батч, записать его результат, вернуть то, к чему не приступили.
stopsignal=TERM
; Должно превышать OUTBOX_APP_SHUTDOWN_TIMEOUT. Поставите меньше — supervisord
; пришлёт SIGKILL посреди завершения, и захваченное будет ждать истечения lease
; вместо того, чтобы вернуться в очередь сразу.
stopwaitsecs=40
; Убивать группу процессов, а не только шелл выше.
stopasgroup=true
killasgroup=true

stdout_logfile=/var/log/outbox/outbox.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=5
redirect_stderr=true
```

```bash
supervisorctl reread && supervisorctl update
supervisorctl status outbox
curl -s localhost:8085/api/v1/stats | jq .messages
```

### Вместо этого — systemd

Та же машина, без supervisord:

```ini
[Unit]
Description=Transactional Outbox dispatcher
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=exec
User=outbox
EnvironmentFile=/etc/outbox/outbox.env
ExecStart=/usr/local/bin/outbox run
Restart=always
RestartSec=5s
# Больше OUTBOX_APP_SHUTDOWN_TIMEOUT — по той же причине, что и stopwaitsecs.
TimeoutStopSec=40s

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### Что здесь ломается

- **`stopwaitsecs` меньше таймаута остановки.** Самое частое. Ничего не
  теряется — lease истекают, и работу подхватывает следующий запуск, — но при
  каждом рестарте доставка встаёт на `OUTBOX_DISPATCH_LEASE_TTL`.
- **`command=` без `exec`.** Сигнал приходит в `/bin/sh`, тот умирает, а
  диспетчер продолжает работать, и supervisord его теряет.
- **Миграции при старте в проде.** `OUTBOX_DB_AUTO_MIGRATE` превращает рестарт в
  изменение схемы. Оставьте выключенным и запускайте `outbox migrate up`
  намеренно.
- **Env-файл, читаемый всем миром.** В нём пароль от базы.

---

## 3. Kubernetes, Kafka, автомасштабирование по возрасту backlog'а

Диспетчер отдельным Deployment рядом с сервисом-продюсером, публикует в Kafka, а
число реплик следует за backlog'ом, а не за CPU.

### Secret и Deployment

```yaml
apiVersion: v1
kind: Secret
metadata: {name: outbox}
stringData:
  dsn: postgres://outbox:secret@postgres:5432/app?sslmode=require
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox
  labels: {app: outbox}
spec:
  replicas: 2
  selector:
    matchLabels: {app: outbox}
  template:
    metadata:
      labels: {app: outbox}
    spec:
      # Больше OUTBOX_APP_SHUTDOWN_TIMEOUT. Меньше — и kubelet пришлёт SIGKILL
      # посреди завершения, а захваченный батч будет ждать истечения lease.
      terminationGracePeriodSeconds: 45
      containers:
        - name: outbox
          image: ghcr.io/efureev/go-outbox:1.0.0
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
            # Пишется в колонку owner захваченной строки, чтобы было видно,
            # какой под держит lease.
            - name: OUTBOX_APP_INSTANCE
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: OUTBOX_STREAMS
              value: events
            - name: OUTBOX_STREAM_EVENTS_DRIVER
              value: kafka
            - name: OUTBOX_DRIVER_KAFKA_TYPE
              value: kafka
            - name: OUTBOX_DRIVER_KAFKA_BROKERS
              value: my-cluster-kafka-bootstrap:9092
            - name: OUTBOX_DISPATCH_WORKERS
              value: "8"
          ports:
            - {name: http, containerPort: 8085}
            - {name: metrics, containerPort: 9100}
          livenessProbe:
            httpGet: {path: /health, port: http}
          readinessProbe:
            httpGet: {path: /ready, port: http}
          resources:
            requests: {cpu: 100m, memory: 64Mi}
            limits: {memory: 256Mi}
---
apiVersion: v1
kind: Service
metadata:
  name: outbox
  labels: {app: outbox}
spec:
  selector: {app: outbox}
  ports:
    - {name: http, port: 8085}
    - {name: metrics, port: 9100}
```

### Миграции — Job, а не init-контейнер

Init-контейнер выполняется на каждый под, то есть повторяется в каждой реплике и
при каждом рестарте. Job выполняется один раз на релиз. Advisory lock делает
безопасными оба варианта; осмыслен только один.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: outbox-migrate
  annotations:                          # если разворачиваете через Helm
    "helm.sh/hook": pre-install,pre-upgrade
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: ghcr.io/efureev/go-outbox:1.0.0
          args: ["migrate", "up"]
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
```

### Сбор метрик и масштабирование

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata: {name: outbox}
spec:
  selector:
    matchLabels: {app: outbox}
  endpoints:
    - {port: metrics, interval: 30s}
```

Масштабируйтесь по тому, как долго ждёт самое старое сообщение. CPU здесь не
говорит ничего полезного: диспетчер простаивает и при пустом backlog'е, и при
многочасовом, потому что ждёт он брокера.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: {name: outbox}
spec:
  scaleTargetRef: {name: outbox}
  minReplicaCount: 2
  maxReplicaCount: 10
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: max(outbox_oldest_pending_age_seconds)
        threshold: "30"                 # реплика на каждые 30 с возраста backlog'а
```

Реплики делят работу безопасно: захват идёт через `FOR UPDATE SKIP LOCKED` и
несёт lease-токен, который обязана предъявить любая запись результата, а
периодические служебные задачи берут advisory lock и потому остаются на одной
реплике за цикл.

### Что здесь ломается

- **`terminationGracePeriodSeconds` меньше таймаута остановки.** Тогда каждый
  выкат оставляет батч истекать вместо возврата в очередь. Смотрите на
  `outbox_messages_reclaimed_total` после деплоя.
- **Масштабирование по CPU.** Оно никогда не сработает. Масштабируйтесь по
  `outbox_oldest_pending_age_seconds`.
- **Воркеров больше, чем переваривает Kafka или позволяет пул соединений.**
  Каждая реплика к тому же удерживает одно соединение пула под слушатель
  уведомлений, так что закладывайте запас в `OUTBOX_DB_MAX_CONNS` и помните про
  сумму по всем репликам.
- **Оставленный включённым `OUTBOX_DB_AUTO_MIGRATE`.** Две реплики, стартующие
  одновременно, безопасны, но изменение схемы должно быть шагом релиза, а не
  побочным эффектом перезапуска пода.

---

## 4. Bare metal, systemd, несколько инстансов на одной машине

Одна машина с достаточным числом ядер, доступные с неё PostgreSQL и Kafka, и
несколько инстансов диспетчера на общей таблице. Шаблонный юнит systemd даёт
каждому собственную идентичность.

`/etc/systemd/system/outbox@.service`:

```ini
[Unit]
Description=Transactional Outbox dispatcher (instance %i)
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=outbox
EnvironmentFile=/etc/outbox/outbox.env
# %i — имя инстанса, поэтому в захваченной строке видно, кто держит её lease.
Environment=OUTBOX_APP_INSTANCE=%H-%i
# Свой порт на инстанс, чтобы у каждого были свои /metrics и /ready.
Environment=OUTBOX_HTTP_PORT=80%i
Environment=OUTBOX_METRICS_PORT=91%i
ExecStart=/usr/local/bin/outbox run
Restart=always
RestartSec=5s
TimeoutStopSec=45s

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
# Бинарник ставится так же, как в рецепте с VDS выше.
outbox migrate up                       # один раз, до старта инстансов
systemctl enable --now outbox@85 outbox@86 outbox@87
systemctl status 'outbox@*'
```

Получаем три инстанса на портах 8085/8086/8087 и 9185/9186/9187. Они делят одну
таблицу и распределяют работу между собой; перетереть результат друг друга ни
один не может, потому что любая запись результата обязана предъявить
lease-токен, проставленный её собственным захватом.

Prometheus собирает со всех трёх:

```yaml
scrape_configs:
  - job_name: outbox
    static_configs:
      - targets: ["10.0.0.10:9185", "10.0.0.10:9186", "10.0.0.10:9187"]
```

### Настройка на своём железе

Это тот случай, когда дефолты вероятнее всего недобирают пропускную
способность, а узнать это можно только замером на самой машине:

```bash
make up && make bench
```

Прогон отвечает на два главных вопроса. Пропускная способность ограничена
меньшим из `OUTBOX_DISPATCH_WORKERS` и пула каналов драйвера, поэтому поднимать
что-то одно бесполезно. А больший `OUTBOX_DISPATCH_BATCH_SIZE` амортизирует
захват на большее число сообщений — до тех пор, пока батч не перестанет
укладываться в lease.

### Что здесь ломается

- **Первым делом добавлять инстансы.** На одной машине воркеры и размер батча
  дешевле процессов. Инстансы нужны для доступности или когда один процесс
  действительно не справляется.
- **Забытое умножение соединений.** Три инстанса при `OUTBOX_DB_MAX_CONNS=10` —
  это тридцать соединений плюс по одному на слушатель уведомлений. Проверьте
  `max_connections`.
- **Один env-файл, один порт.** Без пер-инстансного `OUTBOX_HTTP_PORT` выше
  второй инстанс не сможет занять порт и будет перезапускаться бесконечно.
- **Инстансы, неразличимые в метриках.** Задайте `OUTBOX_APP_INSTANCE`, иначе
  колонка `owner` не подскажет, какой процесс сидит на зависшем lease.

---

## 5. Четыре стрима на трёх инстансах RabbitMQ

Это не цель развёртывания, а схема маршрутизации: один диспетчер кормит сразу
несколько брокеров. Драйвер — это одно соединение, и их может быть столько, сколько
брокеров нужно достать, так что разделение окружений, арендаторов или радиуса
поражения — вопрос конфигурации.

Здесь `local` и `tetra` делят один инстанс, но каждый своим драйвером, а `test` и
`global` получают по инстансу целиком.

```dotenv
OUTBOX_STREAMS=local,test,global,tetra

OUTBOX_STREAM_LOCAL_DRIVER=rmq_local
OUTBOX_STREAM_TEST_DRIVER=rmq_test
OUTBOX_STREAM_GLOBAL_DRIVER=rmq_global
OUTBOX_STREAM_TETRA_DRIVER=rmq_tetra

OUTBOX_DRIVER_RMQ_LOCAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_LOCAL_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_LOCAL_PREFIX=loc

OUTBOX_DRIVER_RMQ_TEST_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TEST_DSN=amqp://user:pass@rabbit-b:5672/
OUTBOX_DRIVER_RMQ_TEST_PREFIX=tst

OUTBOX_DRIVER_RMQ_GLOBAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_GLOBAL_DSN=amqp://user:pass@rabbit-c:5672/
OUTBOX_DRIVER_RMQ_GLOBAL_PREFIX=glb

# Снова первый инстанс, но отдельным драйвером: своё соединение, свой пул каналов,
# своё именование.
OUTBOX_DRIVER_RMQ_TETRA_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TETRA_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_TETRA_PREFIX=ttr
```

Продюсер выбирает направление одной колонкой:

```sql
INSERT INTO outbox.messages (id, stream, topic, payload, target)
VALUES (gen_random_uuid(), 'tetra', 'orders.placed', convert_to('{}', 'UTF8'), '{}');
```

и сообщение приходит туда, куда указывает префикс:

| Стрим | Драйвер | Инстанс | Очередь |
|---|---|---|---|
| `local` | `rmq_local` | rabbit-a | `loc_orders.placed` |
| `test` | `rmq_test` | rabbit-b | `tst_orders.placed` |
| `global` | `rmq_global` | rabbit-c | `glb_orders.placed` |
| `tetra` | `rmq_tetra` | rabbit-a | `ttr_orders.placed` |

`GET /api/v1/stats` показывает разобранное сопоставление — самый быстрый способ
убедиться, что стрим уходит в тот инстанс, который вы имели в виду.

### Попробовать локально

```yaml
services:
  rabbit-a: {image: rabbitmq:4-alpine, ports: ["55672:5672"]}
  rabbit-b: {image: rabbitmq:4-alpine, ports: ["55673:5672"]}
  rabbit-c: {image: rabbitmq:4-alpine, ports: ["55674:5672"]}
```

Направьте три DSN на `localhost:55672`, `:55673` и `:55674`, вставьте по сообщению
на стрим — и каждый инстанс получит ровно своё.

### Что здесь ломается

- **Счёт соединений разом, а не по драйверам.** `CHANNELS` и параллелизм публикации
  принадлежат драйверу, а не процессу. Четыре драйвера по четыре канала — это
  шестнадцать AMQP-каналов, а `OUTBOX_DISPATCH_WORKERS` применяется к пайплайну
  каждого стрима отдельно.
- **Имя драйвера, являющееся префиксом другого.** `rmq` рядом с `rmq_local`
  обрабатывается корректно — настройки сопоставляются по точному ключу, а не по
  префиксу строки, — но читается плохо. Давайте драйверам невложенные имена.
- **Уверенность, что падение одного брокера локализовано.** Для доставки — да: у
  каждого стрима свой пайплайн, остальные продолжают публиковаться. Для таблицы —
  нет: она растёт, пока сообщения этого стрима переотправляются. Смотрите
  `outbox_oldest_pending_age_seconds`, которая одна на развёртывание, вместе с
  `outbox_messages_retried_total{stream=…}`, которая — нет.
- **Забытое, что префикс — часть контракта с потребителем.** Смените
  `OUTBOX_DRIVER_*_PREFIX`, и все потребители этого стрима окажутся подписаны на
  очередь, в которую больше никто не публикует.

---

## Куда дальше

- [Публичный контракт](PublicContract.ru.md) — какие колонки пишет продюсер и
  что явно не гарантируется.
- [Конфигурация](Config.ru.md) — все переменные окружения.
- [Эксплуатация](Operations.ru.md) — масштабирование и что делать, когда
  что-то пошло не так.
- [Метрики и алерты](MetricsAndAlerts.ru.md).
