-- Applied on every open, after the schema: additions to schema version 1 that
-- an existing store needs too. Each statement must be safe to run again.

-- Talkgroup statistics count a talkgroup's units from its events.
CREATE INDEX IF NOT EXISTS unit_events_tg_time ON unit_events(system_id, tgid, time_ms);
-- A unit's first event on a talkgroup (/talkgroups/{id}/units) is a seek here.
CREATE INDEX IF NOT EXISTS unit_events_unit_tg_time ON unit_events(system_id, unit, tgid, time_ms);

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
-- The tone patterns calls carried, one row each (store::tones), indexed from `metadata_json`.
CREATE TABLE IF NOT EXISTS tone_patterns (
    id INTEGER PRIMARY KEY,
    call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    system_id INTEGER NOT NULL,
    tgid INTEGER NOT NULL,
    unit INTEGER,
    time_ms INTEGER NOT NULL,
    pattern TEXT NOT NULL,
    text TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tone_patterns_call ON tone_patterns(call_id, seq);
CREATE INDEX IF NOT EXISTS tone_patterns_time ON tone_patterns(time_ms);
CREATE INDEX IF NOT EXISTS tone_patterns_tg_time ON tone_patterns(system_id, tgid, time_ms);
