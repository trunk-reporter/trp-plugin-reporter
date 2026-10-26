-- Applied on every open, after the schema: additions to schema version 1 that
-- an existing store needs too. Each statement must be safe to run again.

-- Talkgroup statistics count a talkgroup's units from its events.
CREATE INDEX IF NOT EXISTS unit_events_tg_time ON unit_events(system_id, tgid, time_ms);

-- `meta.total_length_ms` is the running SUM(calls.length_ms) that /stats shows,
-- kept by these triggers on every insert, update and delete of a call.
CREATE TRIGGER IF NOT EXISTS calls_length_insert AFTER INSERT ON calls WHEN new.length_ms IS NOT NULL
BEGIN
    UPDATE meta SET value = CAST(value AS INTEGER) + new.length_ms WHERE key = 'total_length_ms';
END;
CREATE TRIGGER IF NOT EXISTS calls_length_update AFTER UPDATE OF length_ms ON calls
    WHEN COALESCE(old.length_ms, 0) <> COALESCE(new.length_ms, 0)
BEGIN
    UPDATE meta SET value = CAST(value AS INTEGER) + COALESCE(new.length_ms, 0) - COALESCE(old.length_ms, 0) WHERE key = 'total_length_ms';
END;
CREATE TRIGGER IF NOT EXISTS calls_length_delete AFTER DELETE ON calls WHEN old.length_ms IS NOT NULL
BEGIN
    UPDATE meta SET value = CAST(value AS INTEGER) - old.length_ms WHERE key = 'total_length_ms';
END;
-- Computed once, when the key is missing (a new store, or one from before the triggers).
INSERT OR IGNORE INTO meta (key, value) SELECT 'total_length_ms', COALESCE(SUM(length_ms), 0) FROM calls;
CREATE TABLE IF NOT EXISTS preferences (key TEXT PRIMARY KEY, json TEXT NOT NULL, updated_ms INTEGER NOT NULL);
