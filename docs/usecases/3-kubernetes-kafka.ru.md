# Kubernetes, Kafka, автомасштабирование по возрасту backlog'а

[English](3-kubernetes-kafka.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).


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
