# Bare metal, systemd, несколько инстансов на одной машине

[English](4-systemd-baremetal.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).


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
