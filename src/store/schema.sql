CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE schema_version (version INTEGER NOT NULL);

CREATE TABLE systems (
    id INTEGER PRIMARY KEY,
    short_name TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    sysid INTEGER,
    wacn INTEGER,
    nac INTEGER,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL
);

CREATE TABLE sites (
    id INTEGER PRIMARY KEY,
    system_id INTEGER NOT NULL REFERENCES systems(id),
    name TEXT NOT NULL,
    nac INTEGER,
    rfss INTEGER,
    site_number INTEGER,
    control_channel_hz INTEGER,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    UNIQUE(system_id, name)
);

CREATE TABLE talkgroups (
    system_id INTEGER NOT NULL,
    tgid INTEGER NOT NULL,
    alpha_tag TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    "group" TEXT NOT NULL DEFAULT '',
    tag TEXT NOT NULL DEFAULT '',
    first_heard_ms INTEGER NOT NULL,
    last_heard_ms INTEGER NOT NULL,
    call_count INTEGER NOT NULL DEFAULT 0,
    encrypted_count INTEGER NOT NULL DEFAULT 0,
    last_algid INTEGER,
    PRIMARY KEY(system_id, tgid)
);

CREATE TABLE units (
    system_id INTEGER NOT NULL,
    unit_id INTEGER NOT NULL,
    alpha_tag TEXT NOT NULL DEFAULT '',
    alpha_tag_source TEXT NOT NULL DEFAULT '',
    ota_alpha_tag TEXT NOT NULL DEFAULT '',
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    last_event_type TEXT,
    last_event_ms INTEGER,
    last_event_tgid INTEGER,
    registered INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(system_id, unit_id)
);

CREATE TABLE calls (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    system_id INTEGER NOT NULL,
    site_id INTEGER,
    call_num INTEGER,
    tgid INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER,
    length_ms INTEGER,
    freq_hz INTEGER,
    tdma_slot INTEGER,
    phase2_tdma INTEGER NOT NULL DEFAULT 0,
    analog INTEGER NOT NULL DEFAULT 0,
    encrypted INTEGER NOT NULL DEFAULT 0,
    algid INTEGER,
    key_id INTEGER,
    decrypted INTEGER NOT NULL DEFAULT 0,
    emergency INTEGER NOT NULL DEFAULT 0,
    duplex INTEGER NOT NULL DEFAULT 0,
    packet INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    recorded INTEGER NOT NULL DEFAULT 1,
    not_recorded_reason TEXT,
    audio_path TEXT,
    json_path TEXT,
    audio_type TEXT NOT NULL DEFAULT 'digital',
    signal REAL,
    noise REAL,
    snr REAL,
    clean_voice_pct REAL,
    error_count INTEGER NOT NULL DEFAULT 0,
    spike_count INTEGER NOT NULL DEFAULT 0,
    freq_error INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'recording',
    tg_alpha_tag TEXT NOT NULL DEFAULT '',
    tg_description TEXT NOT NULL DEFAULT '',
    tg_tag TEXT NOT NULL DEFAULT '',
    tg_group TEXT NOT NULL DEFAULT '',
    patched_tgids TEXT,
    metadata_json TEXT
);
CREATE INDEX calls_tg_start ON calls(system_id, tgid, start_ms);
CREATE INDEX calls_start ON calls(start_ms);
CREATE INDEX calls_state ON calls(state);

CREATE TABLE call_transmissions (
    call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    unit INTEGER NOT NULL,
    time_ms INTEGER NOT NULL,
    pos_s REAL NOT NULL,
    len_s REAL NOT NULL,
    emergency INTEGER NOT NULL DEFAULT 0,
    tag TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(call_id, position)
);
CREATE INDEX call_transmissions_unit ON call_transmissions(unit, time_ms);

CREATE TABLE call_frequencies (
    call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    freq_hz INTEGER NOT NULL,
    time_ms INTEGER NOT NULL,
    pos_s REAL NOT NULL,
    len_s REAL NOT NULL,
    error_count INTEGER NOT NULL DEFAULT 0,
    spike_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(call_id, position)
);

CREATE TABLE unit_events (
    id INTEGER PRIMARY KEY,
    system_id INTEGER NOT NULL,
    site_id INTEGER,
    unit INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    tgid INTEGER,
    emergency INTEGER NOT NULL DEFAULT 0,
    call_id INTEGER,
    metadata_json TEXT
);
CREATE INDEX unit_events_unit_time ON unit_events(system_id, unit, time_ms);
CREATE INDEX unit_events_time ON unit_events(time_ms);
CREATE INDEX IF NOT EXISTS unit_events_tg_time ON unit_events(system_id, tgid, time_ms);
CREATE INDEX IF NOT EXISTS unit_events_unit_tg_time ON unit_events(system_id, unit, tgid, time_ms);
CREATE UNIQUE INDEX unit_events_call ON unit_events(call_id, unit, event_type) WHERE call_id IS NOT NULL;

CREATE TABLE recorder_state (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL,
    time_ms INTEGER NOT NULL,
    control_channel_hz INTEGER,
    decode_rate REAL NOT NULL,
    active_calls INTEGER NOT NULL,
    recording INTEGER NOT NULL
);
CREATE INDEX recorder_state_site_time ON recorder_state(site_id, time_ms);
