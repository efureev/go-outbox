# Bare metal, systemd, several instances on one host

English | [Русский](4-systemd-baremetal.ru.md)

Back to the [use case index](../UseCases.md).


One machine with enough cores to be worth using, PostgreSQL and Kafka reachable
from it, and several dispatcher instances sharing the table. A systemd template
unit gives each its own identity.

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
# %i is the instance name, so a claimed row records which one holds its lease.
Environment=OUTBOX_APP_INSTANCE=%H-%i
# One port per instance, so each has its own /metrics and /ready.
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
# Install the binary as in the VDS recipe above.
outbox migrate up                       # once, before the instances start
systemctl enable --now outbox@85 outbox@86 outbox@87
systemctl status 'outbox@*'
```

That gives three instances on ports 8085/8086/8087 and 9185/9186/9187. They
share one table and divide the work between them; none can overwrite another's
result, because every write-back has to present the lease token its claim
stamped on the row.

Prometheus scrapes all three:

```yaml
scrape_configs:
  - job_name: outbox
    static_configs:
      - targets: ["10.0.0.10:9185", "10.0.0.10:9186", "10.0.0.10:9187"]
```

### Tuning on hardware you own

This is the case where the defaults are most likely to be leaving throughput on
the table, and the only way to know is to measure on the machine itself:

```bash
make up && make bench
```

The sweep answers the two questions that matter. Throughput is bounded by the
smaller of `OUTBOX_DISPATCH_WORKERS` and the driver's channel pool, so raising
one alone does nothing. And a larger `OUTBOX_DISPATCH_BATCH_SIZE` amortises the
claim over more messages, until a batch stops fitting inside the lease.

### What goes wrong here

- **Reaching for more instances first.** On one host, workers and batch size are
  cheaper than processes. Instances are for availability, or for when one
  process genuinely cannot keep up.
- **Forgetting that connections multiply.** Three instances at
  `OUTBOX_DB_MAX_CONNS=10` is thirty connections, plus one held by each
  notification listener. Check `max_connections`.
- **One env file, one port.** Without the per-instance `OUTBOX_HTTP_PORT` above,
  the second instance fails to bind and restarts forever.
- **Instances that all look alike in metrics.** Set `OUTBOX_APP_INSTANCE`, or
  the `owner` column cannot tell you which process is sitting on a stuck lease.

---
