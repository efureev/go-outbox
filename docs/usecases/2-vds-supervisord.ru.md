# Один VDS, supervisord

[English](2-vds-supervisord.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).


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


`SHA256SUMS` подтверждает, что скачанное не побилось. Чтобы вдобавок убедиться,
что оно пришло из релизного workflow этого репозитория, а не от того, кто
дотянулся до страницы релиза:

```bash
curl -fsSLO "https://github.com/efureev/go-outbox/releases/download/v$VERSION/SHA256SUMS.cosign.bundle"

cosign verify-blob SHA256SUMS \
  --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity-regexp 'https://github.com/efureev/go-outbox/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Подпись keyless: раздавать нечего и красть нечего — нет ни публичного ключа, ни
приватного. Оба флага `--certificate-*` проверяют, что подписал именно этот
workflow в этом репозитории, поэтому их надо указывать: без них команду устроит
любая подпись Sigstore вообще.

Образ подписан по digest — по той же причине, по которой не стоит подписывать
тег: тег это имя, и его можно передвинуть.

```bash
cosign verify ghcr.io/efureev/go-outbox:$VERSION \
  --certificate-identity-regexp 'https://github.com/efureev/go-outbox/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

К каждому релизу прикладывается CycloneDX SBOM на платформу —
`outbox_${VERSION}_linux_amd64.cdx.json` и соседние. Они сгенерированы из
собранных бинарников, а не из дерева исходников, поэтому перечисляют то, что
действительно вошло в сборку.

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
