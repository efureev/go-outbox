-- An inbox: a table the dispatcher delivers into instead of a broker.
--
-- WHO OWNS THIS
--
-- You do. This file is an example, not a migration: it is not embedded in the
-- binary and `outbox migrate up` never applies it. The dispatcher inserts and
-- does nothing else — it does not create this table, does not read it, does not
-- update it and does not clean it up. That boundary is what keeps go-outbox a
-- dispatcher rather than the beginnings of a consumer framework.
--
-- Substitute the schema and table names, then point a driver at them:
--
--   OUTBOX_STREAMS=billing
--   OUTBOX_STREAM_BILLING_DRIVER=inb
--   OUTBOX_DRIVER_INB_TYPE=postgres
--   OUTBOX_DRIVER_INB_SCHEMA=billing
--   OUTBOX_DRIVER_INB_TABLE=inbox
--   OUTBOX_DRIVER_INB_DSN=            # empty: the dispatcher's own database
--
-- WHY THE PRIMARY KEY MATTERS
--
-- Delivery is at-least-once, the same as with any broker: the insert here and
-- the write-back marking the outbox row `sent` are two commits, and a replica
-- can die between them. The primary key is what makes the repeat harmless. The
-- driver inserts with ON CONFLICT (id) DO NOTHING and reports a conflict as a
-- delivery, because the message is in the inbox either way.
--
-- Deduplication therefore stops being an obligation on your code and becomes a
-- property of this schema. Remove the primary key and you have neither.

CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.inbox
(
    -- Written by the dispatcher. INSERT only, never UPDATE and never SELECT.

    -- The outbox row id: the same identifier that travels as MessageId on AMQP
    -- and as the message_id header on Kafka. Deduplication rests on it.
    id      UUID PRIMARY KEY,
    stream  TEXT  NOT NULL,
    -- The effective name — prefix and version applied — which is the string a
    -- consumer would have subscribed to at a broker.
    topic   TEXT  NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,

    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Everything below is yours. The dispatcher does not know these columns
    -- exist, so add, rename and drop them as your consumer needs.
    processed_at TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT
);

COMMENT ON TABLE billing.inbox IS
    'Inbox. go-outbox inserts (id, stream, topic, payload, headers) and nothing else; '
    'every other column, and processing the rows, belongs to the consumer.';

-- CLEANING UP IS YOURS TOO
--
-- go-outbox sweeps its own table on a retention you set. It never touches this
-- one, so this table grows until you clean it — and it grows quietly, because
-- nothing on our side is looking at it. That is the one failure of this driver
-- that shows up six months later rather than immediately.
--
--   DELETE FROM billing.inbox
--    WHERE processed_at IS NOT NULL
--      AND processed_at < now() - interval '7 days';
--
-- In chunks if the table is large, for the same reason the dispatcher's own
-- sweep works in chunks: one unbounded DELETE holds a transaction open for its
-- duration and bloats the table it is trying to shrink. Past roughly ten million
-- rows a day, partition this by received_at and make the cleanup a DROP TABLE.

-- The claim path of whatever reads this. Partial, like the dispatcher's own:
-- it covers the rows still waiting and stays small however large the table
-- grows.
CREATE INDEX IF NOT EXISTS inbox_unprocessed_idx
    ON billing.inbox (received_at)
    WHERE processed_at IS NULL;

-- Optional, and the mirror of the dispatcher's own 0003: wake the consumer on
-- arrival instead of making it poll. Drop it if the consumer polls happily.
CREATE OR REPLACE FUNCTION billing.notify_inbox()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('billing_inbox', NEW.stream);

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS inbox_notify_trg ON billing.inbox;

CREATE TRIGGER inbox_notify_trg
    AFTER INSERT
    ON billing.inbox
    FOR EACH ROW
EXECUTE FUNCTION billing.notify_inbox();
