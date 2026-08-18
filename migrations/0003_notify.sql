-- Wake the dispatcher the moment a message is written, instead of leaving it
-- to the next poll tick.
--
-- The trigger is a latency optimisation, not a correctness mechanism:
-- NOTIFY is delivered on a best-effort basis and is lost when the listening
-- connection drops. The poll loop stays on as reconciliation, and the whole
-- mechanism can be disabled with OUTBOX_DISPATCH_NOTIFY_ENABLED=false for a
-- deployment whose database role cannot create triggers.
--
-- The payload is the stream name, so an instance only wakes the pipeline that
-- has work. A burst of inserts produces a burst of notifications; the listener
-- coalesces them within a debounce window rather than claiming once per row.

CREATE OR REPLACE FUNCTION @schema@.notify_new()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('@channel@', NEW.stream);

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS @table@_notify_trg ON @schema@.@table@;

CREATE TRIGGER @table@_notify_trg
    AFTER INSERT
    ON @schema@.@table@
    FOR EACH ROW
EXECUTE FUNCTION @schema@.notify_new();
