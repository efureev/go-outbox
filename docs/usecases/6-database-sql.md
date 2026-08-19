# A Go producer on database/sql

English | [Русский](6-database-sql.ru.md)

Back to the [use case index](../UseCases.md).

A service on `database/sql` — through `lib/pq`, `sqlx`, `gorm`, or the standard
library alone — writing outbox rows inside the transaction it already has.

`pkg/outboxclient` requires pgx, which is the wrong dependency to force on a
codebase that chose something else. `pkg/outboxsql` is the same client against
`database/sql`, importing no driver at all.

### The client

```go
import "github.com/efureev/go-outbox/pkg/outboxsql"

// Once, at startup. The schema and table must match the dispatcher's
// OUTBOX_DB_SCHEMA and OUTBOX_DB_TABLE.
var outbox = outboxsql.Default() // outbox.messages
```

`Default()` is `New("outbox", "messages")` and panics on an identifier it cannot
quote, which the two literals are not. Use `New` when the names differ:

```go
outbox, err := outboxsql.New("events", "queue")
if err != nil {
    return err // A name that cannot be quoted safely is a configuration error.
}
```

### The write

The whole pattern is this transaction and nothing else:

```go
func (s *Accounts) Debit(ctx context.Context, id string, amount int64) error {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer func() { _ = tx.Rollback() }() // No-op after a successful commit.

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

`Enqueue` takes anything with `ExecContext`, so `*sql.Tx`, `*sql.DB`, `*sqlx.Tx`
and `*sqlx.DB` all fit. **Pass the transaction**, not the pool: a message written
outside it can be published for a change that was rolled back, or lost for one
that was not. That is the only rule here.

With `gorm`, reach the underlying transaction:

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

### Several messages

```go
err := outbox.EnqueueBatch(ctx, tx, []outboxsql.Message{
    {Stream: "local", Topic: "account.debited", Payload: debit},
    {Stream: "global", Topic: "ledger.entry", Payload: entry},
})
```

`EnqueueBatch` is one statement per message rather than one round trip for all
of them: pgx has a batch protocol and `database/sql` has none. At a handful of
messages per business transaction that is not worth a thought. A producer
writing hundreds at a time should be on `pkg/outboxclient` and pgx, where the
batch is a single round trip.

### Delaying a message

```go
outbox.Enqueue(ctx, tx, outboxsql.Message{
    Stream:      "local",
    Topic:       "trial.expired",
    Payload:     payload,
    AvailableAt: time.Now().Add(14 * 24 * time.Hour),
})
```

The dispatcher will not claim it before then. A zero `AvailableAt` means now.

### Which driver

The client imports none, and works through both PostgreSQL drivers people
actually use. Both are covered by the integration tests, including a payload of
arbitrary bytes and a `jsonb` round trip:

```go
import _ "github.com/lib/pq"            // sql.Open("postgres", dsn)
import _ "github.com/jackc/pgx/v5/stdlib" // sql.Open("pgx", dsn)
```

The statement uses `$1…$n` placeholders, so it is PostgreSQL either way — which
it has to be, because the dispatcher is.

### What goes wrong here

- **Writing through the pool instead of the transaction.** `outbox.Enqueue(ctx,
  s.db, …)` compiles and works, and quietly gives up the one guarantee the
  pattern exists for. If the business change rolls back, the message still ships.
- **A `Stream` the dispatcher has not been configured with.** The row is written,
  claimed, and fails permanently on the first attempt with `unknown stream`. It
  is visible in `outbox failed` immediately, which is the intended behaviour, but
  the name has to match `OUTBOX_STREAMS`.
- **Schema and table that do not match the dispatcher's.** The client writes into
  a table nobody reads. Nothing errors; the messages simply never go anywhere.
  Check `GET /api/v1/stats`, which reports the table the dispatcher is claiming
  from.
- **Expecting `EnqueueBatch` to be one round trip.** It is not, and on a hot path
  writing many messages per transaction that shows up as latency. That is the
  cost of not depending on pgx.
- **Setting `ID` yourself with a UUIDv4.** Leave it empty and the client
  generates a v7, which sorts by creation time and keeps the primary key index
  append-ordered instead of scattering writes across the whole tree.

---
