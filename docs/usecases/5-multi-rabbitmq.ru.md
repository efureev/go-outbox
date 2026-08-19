# Четыре стрима на трёх инстансах RabbitMQ

[English](5-multi-rabbitmq.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).


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
OUTBOX_DRIVER_RMQ_LOCAL_DECLARE=true

OUTBOX_DRIVER_RMQ_TEST_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TEST_DSN=amqp://user:pass@rabbit-b:5672/
OUTBOX_DRIVER_RMQ_TEST_PREFIX=tst
OUTBOX_DRIVER_RMQ_TEST_DECLARE=true

OUTBOX_DRIVER_RMQ_GLOBAL_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_GLOBAL_DSN=amqp://user:pass@rabbit-c:5672/
OUTBOX_DRIVER_RMQ_GLOBAL_PREFIX=glb
OUTBOX_DRIVER_RMQ_GLOBAL_DECLARE=true

# Снова первый инстанс, но отдельным драйвером: своё соединение, свой пул каналов,
# своё именование.
OUTBOX_DRIVER_RMQ_TETRA_TYPE=rabbitmq
OUTBOX_DRIVER_RMQ_TETRA_DSN=amqp://user:pass@rabbit-a:5672/
OUTBOX_DRIVER_RMQ_TETRA_PREFIX=ttr
OUTBOX_DRIVER_RMQ_TETRA_DECLARE=true
```

`DECLARE` здесь включён, чтобы свежему набору брокеров было куда публиковать.
Выключите его, когда топологией начнут владеть потребители: при включённом по
умолчанию `MANDATORY` очередь, которую никто не объявил, заставит брокер вернуть
сообщение, а диспетчер — записать permanent-отказ: видно, но не доставлено.

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

`GET /api/v1/stats` показывает разобранное сопоставление, и у каждого драйвера —
адрес, к которому он подключается, без учётных данных. Это самый быстрый способ
убедиться, что стрим уходит именно в тот инстанс, а не просто разрешился в драйвер
с подходящим именем.

### Попробовать локально

```yaml
# Учётные данные важны: встроенный guest отвергается отовсюду, кроме loopback, а
# подключение через опубликованный порт — это не loopback.
x-rabbit: &rabbit
  image: rabbitmq:4-alpine
  environment:
    RABBITMQ_DEFAULT_USER: outbox
    RABBITMQ_DEFAULT_PASS: outbox

services:
  rabbit-a: {<<: *rabbit, ports: ["55672:5672"]}
  rabbit-b: {<<: *rabbit, ports: ["55673:5672"]}
  rabbit-c: {<<: *rabbit, ports: ["55674:5672"]}
```

Направьте три DSN на `amqp://outbox:outbox@localhost:55672/` и два порта следом,
вставьте по сообщению на стрим — и каждый инстанс получит ровно своё.

### Что здесь ломается

- **Счёт соединений разом, а не по драйверам.** `CHANNELS` и параллелизм публикации
  принадлежат драйверу, а не процессу. Четыре драйвера по четыре канала — это
  шестнадцать AMQP-каналов, а `OUTBOX_DISPATCH_WORKERS` применяется к пайплайну
  каждого стрима отдельно, то есть в этой схеме дефолтные восемь воркеров работают
  против четырёх каналов — и так четыре раза. Половина воркеров каждого пайплайна
  стоит в очереди за каналом; поднимайте `CHANNELS` и `WORKERS` вместе или не
  трогайте ни то ни другое.
- **Имя драйвера, являющееся префиксом другого.** `rmq` рядом с `rmq_local`
  обрабатывается корректно — настройки сопоставляются по точному ключу, а не по
  префиксу строки, — но читается плохо. Давайте драйверам невложенные имена.
- **Уверенность, что падение одного брокера локализовано.** На работающем сервисе —
  да: у каждого стрима свой пайплайн, остальные продолжают публиковаться. На старте
  — нет: все драйверы подключаются до запуска диспетчера, поэтому один недоступный
  брокер означает, что не заработает ни один стрим. А для таблицы не локализовано
  никогда: она растёт, пока сообщения этого стрима переотправляются. Смотрите
  `max(outbox_oldest_pending_age_seconds)` вместе с
  `outbox_messages_retried_total{stream=…}` — именно она называет виновника.
- **Забытое, что префикс — часть контракта с потребителем.** Смените
  `OUTBOX_DRIVER_*_PREFIX`, и все потребители этого стрима окажутся подписаны на
  очередь, в которую больше никто не публикует.
