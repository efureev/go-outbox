# Kubernetes, Kafka, autoscaled on backlog age

English | [Русский](3-kubernetes-kafka.ru.md)

Back to the [use case index](../UseCases.md).


The dispatcher as its own Deployment beside the producing service, publishing to
Kafka, with the replica count following the backlog rather than CPU.

### Secret and Deployment

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
      # Above OUTBOX_APP_SHUTDOWN_TIMEOUT. Below it, the kubelet sends SIGKILL
      # mid-drain and the claimed batch waits out its lease.
      terminationGracePeriodSeconds: 45
      containers:
        - name: outbox
          image: ghcr.io/efureev/go-outbox:1.0.0
          env:
            - name: OUTBOX_DB_DSN
              valueFrom: {secretKeyRef: {name: outbox, key: dsn}}
            # Recorded in the owner column of a claimed row, so an operator can
            # tell which pod holds a lease.
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

### Migrations as a Job, not an init container

An init container runs once per pod, so every replica and every restart repeats
it. A Job runs once per release. The advisory lock makes both safe; only one is
sensible.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: outbox-migrate
  annotations:                          # if you deploy with Helm
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

### Scraping and scaling

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

Scale on how long the oldest message has waited. CPU says nothing useful here —
the dispatcher is idle whether the backlog is empty or hours deep, because what
it waits on is the broker.

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
        threshold: "30"                 # a replica per 30s of backlog age
```

Replicas divide the work safely: claims are taken with `FOR UPDATE SKIP LOCKED`
and carry a lease token every write-back must present, and the periodic
housekeeping takes an advisory lock so it stays on one replica per cycle.

### What goes wrong here

- **`terminationGracePeriodSeconds` below the shutdown timeout.** Every rollout
  then leaves a batch to expire instead of being handed back. Watch
  `outbox_messages_reclaimed_total` after a deploy.
- **Scaling on CPU.** It will never trigger. Scale on
  `outbox_oldest_pending_age_seconds`.
- **More workers than Kafka can absorb, or than the database pool allows.** Each
  replica also holds one pooled connection for the notification listener, so
  size `OUTBOX_DB_MAX_CONNS` with room to spare and remember the total across
  replicas.
- **Leaving `OUTBOX_DB_AUTO_MIGRATE` on.** Two replicas starting together are
  safe, but a schema change should be a release step, not a side effect of a
  pod restart.

---
