# Go-продюсер на database/sql

[English](6-database-sql.md) | Русский

Назад к [указателю сценариев](../UseCases.ru.md).

Сервис на `database/sql` — через `lib/pq`, `sqlx`, `gorm` или голую стандартную
библиотеку, — пишущий строки outbox внутри той транзакции, которая у него и так
есть.

`pkg/outboxclient` требует pgx, а это не та зависимость, которую стоит навязывать
коду, выбравшему другое. `pkg/outboxsql` — тот же клиент через `database/sql`, не
импортирующий вообще никакого драйвера.

### Клиент

```go
import "github.com/efureev/go-outbox/pkg/outboxsql"

// Один раз при старте. Схема и таблица должны совпадать с OUTBOX_DB_SCHEMA и
// OUTBOX_DB_TABLE диспетчера.
var outbox = outboxsql.Default() // outbox.messages
```

`Default()` — это `New("outbox", "messages")`, паникующий на идентификаторе,
который нельзя безопасно закавычить; эти два литерала таковыми не являются. Если
имена другие — `New`:

```go
outbox, err := outboxsql.New("events", "queue")
if err != nil {
    return err // Имя, которое нельзя закавычить, — ошибка конфигурации.
}
```

### Запись

Весь паттерн — это вот эта транзакция и больше ничего:

```go
func (s *Accounts) Debit(ctx context.Context, id string, amount int64) error {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer func() { _ = tx.Rollback() }() // После успешного Commit это no-op.

    if _, err := tx.ExecContext(ctx,
        `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, id); err != nil {
        return err
    }

    payload, err := json.Marshal(debited{Account: id, Amount: amount})
    if err != nil {
        return err
    }

    if err := outbox.Enqueue(ctx, tx, outboxsql.Message{
        Stream:  "local",
        Topic:   "account.debited",
        Payload: payload,
        Headers: map[string]string{"traceparent": traceparentOf(ctx)},
        Target:  outboxsql.Target{Key: id},
    }); err != nil {
        return err
    }

    return tx.Commit()
}
```

`Enqueue` принимает что угодно с `ExecContext`, поэтому подходят `*sql.Tx`,
`*sql.DB`, `*sqlx.Tx` и `*sqlx.DB`. **Передавайте транзакцию**, а не пул:
сообщение, записанное мимо неё, может быть опубликовано для изменения, которое
откатили, или потеряно для того, которое нет. Это единственное правило здесь.

С `gorm` — добраться до нижележащей транзакции:

```go
db.Transaction(func(tx *gorm.DB) error {
    if err := tx.Model(&Account{}).Where("id = ?", id).
        UpdateColumn("balance", gorm.Expr("balance - ?", amount)).Error; err != nil {
        return err
    }

    sqlTx, err := tx.DB()
    if err != nil {
        return err
    }

    return outbox.Enqueue(ctx, sqlTx, outboxsql.Message{ /* … */ })
})
```

### Несколько сообщений

```go
err := outbox.EnqueueBatch(ctx, tx, []outboxsql.Message{
    {Stream: "local", Topic: "account.debited", Payload: debit},
    {Stream: "global", Topic: "ledger.entry", Payload: entry},
})
```

`EnqueueBatch` — это по одному запросу на сообщение, а не один round trip на
всех: у pgx есть batch-протокол, у `database/sql` его нет. На нескольких
сообщениях в бизнес-транзакции об этом не стоит и думать. Продюсеру, пишущему
сотни за раз, лучше подойдёт `pkg/outboxclient` и pgx, где батч — один round
trip.

### Отложить сообщение

```go
outbox.Enqueue(ctx, tx, outboxsql.Message{
    Stream:      "local",
    Topic:       "trial.expired",
    Payload:     payload,
    AvailableAt: time.Now().Add(14 * 24 * time.Hour),
})
```

Раньше этого момента диспетчер сообщение не заберёт. Нулевое `AvailableAt`
означает «сейчас».

### Какой драйвер

Клиент не импортирует ни одного и работает через оба драйвера PostgreSQL,
которыми реально пользуются. Оба покрыты интеграционными тестами, включая payload
из произвольных байт и круговой прогон `jsonb`:

```go
import _ "github.com/lib/pq"              // sql.Open("postgres", dsn)
import _ "github.com/jackc/pgx/v5/stdlib" // sql.Open("pgx", dsn)
```

В запросе плейсхолдеры `$1…$n`, то есть это в любом случае PostgreSQL — иначе и
быть не может, потому что диспетчер работает с ним.

### Что здесь ломается

- **Запись через пул вместо транзакции.** `outbox.Enqueue(ctx, s.db, …)`
  компилируется и работает, тихо отдавая ту единственную гарантию, ради которой
  паттерн существует. Если бизнес-изменение откатится, сообщение всё равно
  уедет.
- **`Stream`, которого нет в конфигурации диспетчера.** Строка запишется, будет
  захвачена и упадёт перманентно на первой же попытке с `unknown stream`. Она
  сразу видна в `outbox failed` — так и задумано, — но имя должно совпадать с
  `OUTBOX_STREAMS`.
- **Схема и таблица, не совпадающие с диспетчерскими.** Клиент пишет в таблицу,
  которую никто не читает. Ошибки не будет; сообщения просто никуда не поедут.
  Проверьте `GET /api/v1/stats` — он показывает таблицу, из которой диспетчер
  забирает работу.
- **Ожидание, что `EnqueueBatch` — это один round trip.** Это не так, и на
  горячем пути с большим числом сообщений в транзакции это видно как задержка.
  Такова цена независимости от pgx.
- **Свой `ID` в виде UUIDv4.** Оставьте поле пустым, и клиент сгенерирует v7:
  он сортируется по времени создания и держит индекс первичного ключа
  дописываемым, вместо того чтобы разбрасывать записи по всему дереву.

---
