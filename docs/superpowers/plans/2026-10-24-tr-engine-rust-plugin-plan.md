# tr-engine Rust plugin (phase 1) — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Trunk Recorder Pro (TRP) plugin, in Rust, that stores every call, transmission, unit event and recorder status in SQLite and serves tr-engine's REST API, SSE stream and phase-1 pages on its own port, with no other software to run.

**Architecture:** One binary with three entry points: `--describe`, plugin mode (TRP drives it over stdin/stdout through the `trunk-recorder-plugin` SDK) and `serve <data-dir>` (HTTP only, for when TRP isn't recording). Plugin callbacks map SDK events to rows and hand them to a single SQLite writer thread over a channel; an axum server on a tokio runtime thread answers the API from a small pool of read connections and fans events out over SSE from an in-memory ring buffer. Pages are tr-engine's own HTML, embedded.

**Tech Stack:** Rust 2024 (rust-version 1.85); `trunk-recorder-plugin = "0.5"` (the published SDK, what the released TRP 0.2.9 speaks; TRP's main carries an unpublished 0.7 with a plugin page, live settings, action buttons and names on live events — Task 21 picks those up once it's published, decided 2026-10-25); `rusqlite` (`bundled`); `axum` 0.8 + `tokio` (`rt-multi-thread`, `macros`, `sync`, `time`) + `tower-http` (`fs`); `serde`, `serde_json`, `serde_yaml`; `schemars` 1; `rust-embed`; `time` (`formatting`, `parsing`); `walkdir`; `futures-util`. No `clap` (two fixed arguments, parsed by hand).

**Spec:** `docs/superpowers/specs/2026-10-24-tr-engine-rust-plugin-design.md` (copied into the repo in Task 1 from `D:\trunk-pro-airspy-fix\2026-10-24-tr-engine-rust-plugin-design.md`). tr-engine's contract: `openapi/openapi.yaml` (copied in Task 1, extended in Task 2).

## Global Constraints

- Every served path and response shape is `openapi.yaml`'s (spec anti-goal: no second, diverging API). Spec paths out of scope answer **501** with an `Error` body, never 404.
- All times stored as INTEGER Unix **milliseconds**; all times served as RFC 3339 UTC (`2024-01-15T10:30:00.000Z`); `start_time`/`end_time` query parameters parsed as RFC 3339.
- Systems are known by `short_name` across runs; the SDK's `system` index is never stored.
- Audio is never copied or stored in the database; `calls.audio_path` is relative to TRP's capture folder.
- The plugin sends `call.result` for every `call.concluded` it receives, immediately after the write is queued.
- Plugin callbacks return quickly: no disk or network work on the SDK's reader thread beyond `Sender::send`.
- Never `println!` in plugin mode (stdout is the protocol). Logs go through `Host::info/warn/error`; `eprintln!` only in `serve` mode.
- Defaults start without settings: port **8092**, bind **127.0.0.1**, no token (auth mode `open`), retention **0** (keep everything), backfill **on**. Port **0** means "don't listen" (tests).
- Unrecorded calls are stored and **included by default** in lists, counts and stats; `recorded=true|false` filters them (spec D6).
- Keep the template's `rustfmt.toml`, CI (`cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked`, `check_manifest.py`) and release workflow unchanged apart from the crate name.
- Working plugin id/crate name: `reporter` (spec C10; the owner renames before the first release, one line in `Cargo.toml`).

## Review Focus

1. **A talkgroup or unit number present in two systems**, asked for by plain id (`/talkgroups/9178`), must answer 409 `ambiguous_id` with the matching systems, not the first match. Test in Task 12 (and 13 for units).
2. **A stored `audio_path` that escapes the capture folder** (`..`, absolute path) must answer 404 and never serve a file outside it. Test in Task 14.
3. **A malformed or truncated call JSON on disk** during backfill must be skipped with one warning while the walk continues. Test in Task 18.
4. **The HTTP port already in use at start** must make `start` return an `Err` naming the port and the setting to change, so TRP shows it and doesn't restart the plugin in a loop. Test in Task 19.
5. **A Trunk Recorder file with `start_time_ms = 0`** (older files carry only seconds) must be stored at `start_time * 1000`, not at 0. Test in Task 5.

---

## File structure

```
reporter/
  Cargo.toml                 crate `reporter`; deps above
  CHANGELOG.md               ## [0.1.0] section (release notes come from here)
  src/main.rs                argument dispatch: --describe | --version | serve <data-dir> | plugin mode
  src/config.rs              Config (settings form), SystemConfig = NoConfig
  src/plugin.rs              `Reporter: Plugin` — callbacks → Ingest; start/shutdown wiring
  src/ingest.rs              event mapping: SDK types → store writes + bus events
  src/store/mod.rs           Store (writer thread, read pool, migrations), newtypes
  src/store/schema.sql       DDL, applied once (schema_version 1)
  src/store/systems.rs       systems, sites, meta
  src/store/calls.rs         calls, call_transmissions, call_frequencies
  src/store/talkgroups.rs    talkgroups
  src/store/units.rs         units, unit_events
  src/store/recorder.rs      recorder_state, decimation, retention
  src/bus.rs                 EventBus: ring buffer, filters, Last-Event-ID replay
  src/affiliations.rs        in-memory affiliation map (rebuilt from unit_events at start)
  src/backfill.rs            capture-folder walk → ingest as call.concluded
  src/server.rs              HTTP server thread and handle
  src/api/mod.rs             router, AppState, ApiError, auth layer, param parsing, 501 list
  src/api/types.rs           response structs mirroring openapi.yaml schemas
  src/api/health.rs          /health, /auth-init, /openapi.yaml, /pages
  src/api/systems.rs         /systems, /systems/{id}, /sites/{id}, /p25-systems
  src/api/talkgroups.rs      /talkgroups…, /talkgroups/encryption-stats
  src/api/units.rs           /units…, /unit-events, /unit-affiliations
  src/api/calls.rs           /calls…, audio, frequencies, transmissions
  src/api/stats.rs           /stats, /stats/rates, /recorders
  src/api/sse.rs             /events/stream
  src/web.rs                 rust-embed of web/ and the index route
  web/                       tr-engine pages for phase 1 + auth.js, theme-*.js
  openapi/openapi.yaml       the contract served and tested against
  tests/common/mod.rs        test app builder, seeding helpers, `assert_schema(value, "Call")`
  tests/api_*.rs             one file per API task
  docs/superpowers/specs/…   the design
  docs/superpowers/plans/…   this plan
```

Names used throughout: `SystemId(i64)`, `SiteId(i64)`, `CallId(i64)` newtypes in `store/mod.rs`; `*_ms: i64` for times; `now_ms()` in `store/mod.rs`.

---

### Task 1: Repository from the template

**Files:**
- Create: `D:\reporter\` from `https://github.com/TrunkRecorder/trunk-plugin-template` (clone, delete `.git`, `git init`)
- Modify: `Cargo.toml`, `src/main.rs`, `README.md`, `CHANGELOG.md`
- Create: `src/config.rs`, `src/plugin.rs`, `docs/superpowers/specs/2026-10-24-tr-engine-rust-plugin-design.md`, `docs/superpowers/plans/2026-10-24-tr-engine-rust-plugin-plan.md`, `openapi/openapi.yaml` (copy of tr-engine's current `openapi.yaml`)
- Test: `src/config.rs`, `src/main.rs`

**Interfaces:**
- Produces: `config::Config { port: u16, bind: String, token: Option<String>, retention_days: u32, backfill: bool }` with `Default` (8092, "127.0.0.1", None, 0, true), `#[serde(rename_all = "camelCase", default)]`, `token` marked `#[schemars(extend("x-secret" = true))]`; `config::SystemConfig = trunk_recorder_plugin::NoConfig`.
- Produces: `main::Mode { Describe, Version, Serve { data_dir: PathBuf }, Plugin }` and `fn mode(args: &[String]) -> Result<Mode, String>`.
- Produces: `plugin::Reporter::manifest()` → `Manifest { name: "Reporter", subscribe: [call.start, call.end, call.concluded, unit, status], homepage: "http://127.0.0.1:8092/", ..manifest!() }` (no `audio` in phase 1, spec C5). The homepage is the default port's URL: `--describe` has no settings, so a changed port is only in the log and status line (spec C2, adjusted).

- [ ] **Step 1: Scaffold.** Clone the template into `D:\reporter`, remove `.git`, `git init`, set `[package] name = "reporter"`, `description = "Call, unit and talkgroup history with tr-engine's API and pages, stored in SQLite."`, `repository = "https://github.com/trunk-reporter/reporter"`, `homepage = "http://127.0.0.1:8092/"`, `license = "MIT"`, `rust-version = "1.85"`. Add the Tech Stack dependencies. Replace the template's call-log code in `src/main.rs` with `fn main()` dispatching on `mode()`: `Plugin` → `trunk_recorder_plugin::run::<plugin::Reporter>()`, `Describe`/`Version` → print as the SDK's `run` does, `Serve` → `eprintln!("not built yet")` and exit 2 until Task 19. Copy the design and this plan into `docs/superpowers/…`, and tr-engine's `openapi.yaml` into `openapi/`.
- [ ] **Step 2: Write the failing tests** in `src/config.rs` and `src/main.rs`:

```rust
#[test] fn defaults_start_without_settings() {
    let c: Config = serde_json::from_value(json!({})).unwrap();
    assert_eq!((c.port, c.bind.as_str(), c.token, c.retention_days, c.backfill), (8092, "127.0.0.1", None, 0, true));
}
#[test] fn the_settings_form_hides_the_token() {
    let s = trunk_recorder_plugin::schema::schema_for::<Config>();
    assert_eq!(s["properties"]["token"]["x-secret"], true);
    assert_eq!(s["properties"]["port"]["default"], 8092);
}
#[test] fn arguments_pick_a_mode() {
    assert!(matches!(mode(&[]).unwrap(), Mode::Plugin));
    assert!(matches!(mode(&["--describe".into()]).unwrap(), Mode::Describe));
    assert!(matches!(mode(&["serve".into(), "d".into()]).unwrap(), Mode::Serve { .. }));
    assert!(mode(&["serve".into()]).is_err());
    assert!(mode(&["bogus".into()]).is_err());
}
#[test] fn the_manifest_subscribes_to_what_phase_1_needs() {
    let m = trunk_recorder_plugin::describe::<plugin::Reporter>();
    assert_eq!(m.id, "reporter");
    assert!(m.subscribes("call.concluded") && m.subscribes("unit") && m.subscribes("status") && !m.subscribes("audio"));
    assert!(m.config.is_some() && m.system_config.is_none());
}
```

- [ ] **Step 3: Run `cargo test`** — expect compile failures (no `Config`, `mode`, `Reporter`).
- [ ] **Step 4: Implement** `config.rs` (doc comments become the form: "Port"; "Bind address"; "Access token" with "Leave empty for no login. When set, clients send it as a bearer token."; "Keep history for (days)" with "0 keeps everything."; "Backfill from recorded calls on start"), `mode()`, and `plugin.rs` holding `pub struct Reporter;` with `manifest()` and a `start` returning `Err("not built yet".into())`.
- [ ] **Step 5: Run `cargo test` and `cargo run -- --describe > m.json && python .github/check_manifest.py m.json`** — expect all pass and `reporter 0.1.0: ok`.
- [ ] **Step 6: Commit** `chore: reporter from the plugin template; settings and modes`.

---

### Task 2: Extend the API contract (spec D6)

**Files:**
- Modify (tr-engine repo, the owner's): `openapi.yaml`
- Modify (this repo): `openapi/openapi.yaml` (the same edit, so tests here run before the PR lands)

**Interfaces:**
- Produces: `Call.recorded: boolean`, `Call.not_recorded_reason: string|null`, `Call.decrypted: boolean`, `Call.algid: integer|null`, `Call.key_id: integer|null`, `Call.priority: integer`, `Call.duplex: boolean`, `Call.packet: boolean`; `SystemType` gains `dmr`, `nxdn`, `edacs`; `components/parameters/recorded` (boolean, optional) referenced from `/calls`, `/talkgroups/{id}/calls`, `/units/{id}/calls`; `Error.code` gains `not_implemented`.

- [ ] **Step 1: Edit `openapi.yaml`** in both places: the eight `Call` properties with one-line descriptions (`not_recorded_reason` lists TRP's reasons in prose: `encrypted`, `unknown_tg`, `no_recorder`, `no_source`, …), the three `SystemType` values, the `recorded` parameter, `not_implemented`.
- [ ] **Step 2: Validate** with `npx @redocly/cli lint openapi.yaml` — expect no errors.
- [ ] **Step 3: Open the PR on tr-engine** titled "openapi: fields TRP produces (recorded, cipher, service options), trunked DMR/NXDN/EDACS, not_implemented", citing D6. Phase 1's registry submission (Task 20) waits for its merge; nothing else does.
- [ ] **Step 4: Commit (this repo)** `spec: openapi.yaml with the D6 additions`.

---

### Task 3: Store: open, migrate, writer thread, read pool, meta

**Files:**
- Create: `src/store/mod.rs`, `src/store/schema.sql`, `src/store/systems.rs` (meta functions only, for now)
- Test: `src/store/mod.rs`

**Interfaces:**
- Produces: `pub struct Store` (Clone, cheap); `Store::open(path: &Path) -> Result<Store, StoreError>` (file, `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, 4 read connections); `Store::in_memory() -> Store` (shared-cache URI so writer and readers see one database); `Store::write(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<()> + Send + 'static)` (queued to the writer thread, returns at once); `Store::write_sync<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static) -> Result<T, StoreError>`; `Store::read<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, StoreError>`; `Store::flush(&self)` (waits for the queue to drain); `Store::queue_len(&self) -> usize`; `Store::set_logger(&self, f: Arc<dyn Fn(String) + Send + Sync>)` (write errors go there; Task 9 passes `host.error`); `pub struct StoreError(pub String)` with `Display` and `From<rusqlite::Error>`.
- Produces (`systems.rs`): `meta_get(conn, key: &str) -> rusqlite::Result<Option<String>>`, `meta_set(conn, key, value) -> rusqlite::Result<()>`.
- Produces: `schema.sql` creating every table of spec C4. Columns, as later tasks use them:
  - `meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`; `schema_version(version INTEGER NOT NULL)`.
  - `systems(id INTEGER PRIMARY KEY, short_name TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', sysid INTEGER, wacn INTEGER, nac INTEGER, first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL)`.
  - `sites(id INTEGER PRIMARY KEY, system_id INTEGER NOT NULL REFERENCES systems(id), name TEXT NOT NULL, nac INTEGER, rfss INTEGER, site_number INTEGER, control_channel_hz INTEGER, first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL, UNIQUE(system_id, name))`.
  - `talkgroups(system_id INTEGER NOT NULL, tgid INTEGER NOT NULL, alpha_tag TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', "group" TEXT NOT NULL DEFAULT '', tag TEXT NOT NULL DEFAULT '', first_heard_ms INTEGER NOT NULL, last_heard_ms INTEGER NOT NULL, call_count INTEGER NOT NULL DEFAULT 0, encrypted_count INTEGER NOT NULL DEFAULT 0, last_algid INTEGER, PRIMARY KEY(system_id, tgid))`.
  - `units(system_id INTEGER NOT NULL, unit_id INTEGER NOT NULL, alpha_tag TEXT NOT NULL DEFAULT '', alpha_tag_source TEXT NOT NULL DEFAULT '', ota_alpha_tag TEXT NOT NULL DEFAULT '', first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL, last_event_type TEXT, last_event_ms INTEGER, last_event_tgid INTEGER, registered INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(system_id, unit_id))`.
  - `calls(id INTEGER PRIMARY KEY, path TEXT UNIQUE, system_id INTEGER NOT NULL, site_id INTEGER, call_num INTEGER, tgid INTEGER NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER, length_ms INTEGER, freq_hz INTEGER, tdma_slot INTEGER, phase2_tdma INTEGER NOT NULL DEFAULT 0, analog INTEGER NOT NULL DEFAULT 0, encrypted INTEGER NOT NULL DEFAULT 0, algid INTEGER, key_id INTEGER, decrypted INTEGER NOT NULL DEFAULT 0, emergency INTEGER NOT NULL DEFAULT 0, duplex INTEGER NOT NULL DEFAULT 0, packet INTEGER NOT NULL DEFAULT 0, priority INTEGER NOT NULL DEFAULT 0, recorded INTEGER NOT NULL DEFAULT 1, not_recorded_reason TEXT, audio_path TEXT, json_path TEXT, audio_type TEXT NOT NULL DEFAULT 'digital', signal REAL, noise REAL, snr REAL, clean_voice_pct REAL, error_count INTEGER NOT NULL DEFAULT 0, spike_count INTEGER NOT NULL DEFAULT 0, freq_error INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'recording', tg_alpha_tag TEXT NOT NULL DEFAULT '', tg_description TEXT NOT NULL DEFAULT '', tg_tag TEXT NOT NULL DEFAULT '', tg_group TEXT NOT NULL DEFAULT '', patched_tgids TEXT, metadata_json TEXT)`; indexes on `(system_id, tgid, start_ms)`, `(start_ms)`, `(state)`.
  - `call_transmissions(call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE, position INTEGER NOT NULL, unit INTEGER NOT NULL, time_ms INTEGER NOT NULL, pos_s REAL NOT NULL, len_s REAL NOT NULL, emergency INTEGER NOT NULL DEFAULT 0, tag TEXT NOT NULL DEFAULT '', PRIMARY KEY(call_id, position))`; index `(unit, time_ms)`.
  - `call_frequencies(call_id … ON DELETE CASCADE, position INTEGER NOT NULL, freq_hz INTEGER NOT NULL, time_ms INTEGER NOT NULL, pos_s REAL NOT NULL, len_s REAL NOT NULL, error_count INTEGER NOT NULL DEFAULT 0, spike_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(call_id, position))`.
  - `unit_events(id INTEGER PRIMARY KEY, system_id INTEGER NOT NULL, site_id INTEGER, unit INTEGER NOT NULL, event_type TEXT NOT NULL, time_ms INTEGER NOT NULL, tgid INTEGER, emergency INTEGER NOT NULL DEFAULT 0, call_id INTEGER, metadata_json TEXT)`; indexes `(system_id, unit, time_ms)`, `(time_ms)`, and `CREATE UNIQUE INDEX unit_events_call ON unit_events(call_id, unit, event_type) WHERE call_id IS NOT NULL`.
  - `recorder_state(id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL, time_ms INTEGER NOT NULL, control_channel_hz INTEGER, decode_rate REAL NOT NULL, active_calls INTEGER NOT NULL, recording INTEGER NOT NULL)`; index `(site_id, time_ms)`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn opens_a_file_store_in_wal_mode_with_the_schema() {
    let dir = trunk_recorder_plugin::testing::temp_dir("store");
    let s = Store::open(&dir.join("engine.sqlite")).unwrap();
    let mode: String = s.read(|c| c.query_row("PRAGMA journal_mode", [], |r| r.get(0))).unwrap();
    assert_eq!(mode, "wal");
    let n: i64 = s.read(|c| c.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('systems','sites','talkgroups','units','calls','call_transmissions','call_frequencies','unit_events','recorder_state','meta')", [], |r| r.get(0))).unwrap();
    assert_eq!(n, 10);
    assert_eq!(s.read(|c| c.query_row("SELECT version FROM schema_version", [], |r| r.get::<_, i64>(0))).unwrap(), 1);
}
#[test] fn writes_are_applied_in_order_and_flush_waits() {
    let s = Store::in_memory();
    for i in 0..100 { s.write(move |c| c.execute("INSERT INTO meta VALUES (?1, ?2)", (format!("k{i}"), i.to_string())).map(|_| ())); }
    s.flush();
    assert_eq!(s.read(|c| c.query_row("SELECT count(*) FROM meta", [], |r| r.get::<_, i64>(0))).unwrap(), 100);
    assert_eq!(s.read(|c| meta_get(c, "k7")).unwrap(), Some("7".into()));
}
#[test] fn opening_again_keeps_data() { /* open; write_sync meta_set("a","b"); drop; open; meta_get("a") == Some("b") */ }
```

- [ ] **Step 2: Run `cargo test store::`** — expect compile failure.
- [ ] **Step 3: Implement.** `include_str!("schema.sql")` applied in one `execute_batch` when `schema_version` is missing; the writer thread owns one connection and loops on an `mpsc::Receiver<Box<dyn FnOnce(&Connection) -> rusqlite::Result<()> + Send>>`, passing errors to the logger; `flush` sends a job carrying a `SyncSender<()>`; `in_memory` uses `file:memdb{n}?mode=memory&cache=shared` with `OpenFlags::SQLITE_OPEN_URI`.
- [ ] **Step 4: Run `cargo test store::`** — expect PASS.
- [ ] **Step 5: Commit** `store: SQLite file with WAL, schema v1, writer thread and read pool`.

---

### Task 4: Store: systems and sites

**Files:**
- Modify: `src/store/systems.rs`
- Test: `src/store/systems.rs`

**Interfaces:**
- Produces: `ensure_system(conn, short_name: &str, kind: &str, name: &str, now_ms: i64) -> rusqlite::Result<SystemId>` (insert or touch `last_seen_ms`; `name` updates only when non-empty); `ensure_site(conn, system: SystemId, name: &str, now_ms) -> rusqlite::Result<SiteId>` (callers pass the system's `short_name` when an event's `site` is empty: a conventional system's one site); `set_identity(conn, site: SiteId, id: &Identity, now_ms) -> rusqlite::Result<()>` with `pub struct Identity { nac: Option<u32>, wacn: Option<u32>, sys_id: Option<u32>, rfss: Option<u32>, site_number: Option<u32>, control_channel_hz: Option<u64> }` (fills the site's columns and the system's `sysid/wacn/nac` where `Some`; `None` clears nothing); `system_by_short_name(conn, &str) -> rusqlite::Result<Option<SystemRow>>`; `system_by_id(conn, SystemId) -> rusqlite::Result<Option<SystemRow>>`; `list_systems(conn) -> rusqlite::Result<Vec<SystemRow>>`; `list_sites(conn, SystemId) -> rusqlite::Result<Vec<SiteRow>>`; `site_by_id(conn, SiteId) -> rusqlite::Result<Option<SiteRow>>`.
- Produces: `pub struct SystemRow { id: SystemId, short_name: String, kind: String, name: String, sysid: Option<u32>, wacn: Option<u32>, nac: Option<u32>, first_seen_ms: i64, last_seen_ms: i64 }`, `pub struct SiteRow { id: SiteId, system_id: SystemId, name: String, nac: Option<u32>, rfss: Option<u32>, site_number: Option<u32>, control_channel_hz: Option<u64>, first_seen_ms: i64, last_seen_ms: i64 }`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn a_system_is_one_row_however_often_it_is_seen() {
    let s = Store::in_memory();
    let (a, b) = s.write_sync(|c| Ok((ensure_system(c, "ohmarcs", "p25", "", 1_000)?, ensure_system(c, "ohmarcs", "p25", "Ohio MARCS", 2_000)?))).unwrap();
    assert_eq!(a, b);
    let row = s.read(|c| system_by_short_name(c, "ohmarcs")).unwrap().unwrap();
    assert_eq!((row.name.as_str(), row.first_seen_ms, row.last_seen_ms, row.sysid), ("Ohio MARCS", 1_000, 2_000, None));
}
#[test] fn identity_fills_in_when_the_control_channel_states_it() {
    // ensure_system + ensure_site("ohmarcs-dayton"); set_identity(Identity{nac 0x3A0, wacn 0xBEE00, sys_id 0x3AB, rfss 1, site_number 5, cc 851_000_000})
    // system → sysid Some(0x3AB), wacn Some(0xBEE00); site → rfss Some(1), site_number Some(5), control_channel_hz Some(851_000_000)
    // set_identity(Identity::default()) → nothing cleared
}
#[test] fn sites_are_unique_per_system_and_name() { /* ensure_site twice → same id; list_sites len 1 */ }
```

- [ ] **Step 2: Run `cargo test store::systems`** — expect compile failure.
- [ ] **Step 3: Implement** with `INSERT … ON CONFLICT(short_name) DO UPDATE SET last_seen_ms = excluded.last_seen_ms, name = CASE WHEN excluded.name = '' THEN name ELSE excluded.name END RETURNING id`, and `COALESCE(?, column)` in `set_identity`.
- [ ] **Step 4: Run `cargo test store::systems`** — expect PASS.
- [ ] **Step 5: Commit** `store: systems and sites, identity from status`.

---

### Task 5: Store: calls, transmissions, frequencies

**Files:**
- Create: `src/store/calls.rs`
- Test: `src/store/calls.rs`

**Interfaces:**
- Consumes: `SystemId`, `SiteId` (Task 3), `ensure_system`, `ensure_site` (Task 4).
- Produces: `pub struct NewCall { system_id: SystemId, site_id: Option<SiteId>, call_num: Option<u32>, tgid: u32, start_ms: i64, freq_hz: u64, tdma_slot: Option<u8>, analog: bool, encrypted: bool, algid: Option<u8>, key_id: Option<u16>, decrypted: bool, emergency: bool, duplex: bool, packet: bool, priority: u8, recorded: bool, not_recorded_reason: Option<String>, tg_alpha_tag: String, patched_tgids: Vec<u32> }` and `NewCall::from_info(&CallInfo, SystemId, Option<SiteId>) -> NewCall` (`recorded = info.recording`, `start_ms = (info.start_time * 1000.0) as i64`).
- Produces: `call_started(conn, &NewCall) -> rusqlite::Result<CallId>` (state `recording`); `call_ended(conn, CallId, end_ms: i64, info: &CallInfo) -> rusqlite::Result<()>` (state `ended`, `length_ms = end_ms - start_ms`, refreshes `recorded`, `not_recorded_reason`, `encrypted`, `algid`, `key_id`, `decrypted`, `emergency`, `patched_tgids` from `info`); `call_concluded(conn, system_id: SystemId, site_id: Option<SiteId>, path: &str, record: &CallRecord, audio_path: Option<&str>, json_path: &str, existing: Option<CallId>) -> rusqlite::Result<CallId>` — updates `existing` when `Some`, else `INSERT … ON CONFLICT(path) DO UPDATE` (backfill idempotence); sets `path`, every quality field, `state = 'concluded'`, `recorded = 1`, the `tg_*` columns, `audio_type`, `phase2_tdma`, `tdma_slot`, `freq_error`, `metadata_json` = the record's `extra` as JSON, and **replaces** `call_transmissions` and `call_frequencies` from `srcList`/`freqList`; `start_ms` = `record.start_time_ms` when non-zero else `record.start_time * 1000` (Review Focus 5), the same for `end_ms`; `length_ms` = `record.call_length_ms` when non-zero else `(call_length * 1000) as i64`.
- Produces: `pub fn transmission_len_s(src_list: &[SrcEntry], i: usize, call_length_s: f64) -> f64` = next entry's `pos` minus this one's, or `call_length_s - pos` for the last (tr-engine's `duration` rule).
- Produces: `pub struct CallRow` (every `calls` column, typed; `patched_tgids: Vec<u32>`); `pub struct CallFilter { system_ids: Vec<i64>, site_ids: Vec<i64>, tgids: Vec<u32>, unit_ids: Vec<u32>, emergency: Option<bool>, encrypted: Option<bool>, recorded: Option<bool>, start_ms: Option<i64>, end_ms: Option<i64>, sort: CallSort, desc: bool, limit: u32, offset: u32 }` with `Default` (sort `StartTime`, desc `true`, limit 50) and `pub enum CallSort { StartTime, StopTime, Duration, Tgid, Freq }`; `list_calls(conn, &CallFilter) -> rusqlite::Result<(Vec<CallRow>, i64)>`; `call_by_id(conn, CallId) -> rusqlite::Result<Option<CallRow>>`; `active_calls(conn) -> rusqlite::Result<Vec<CallRow>>` (state `recording`); `transmissions(conn, CallId) -> rusqlite::Result<Vec<TransmissionRow { position, unit, time_ms, pos_s, len_s, emergency, tag }>>`; `frequencies(conn, CallId) -> rusqlite::Result<Vec<FrequencyRow>>`; `units_of(conn, CallId) -> rusqlite::Result<Vec<u32>>` (distinct, first-heard order); `newest_start_ms(conn, SystemId) -> rusqlite::Result<Option<i64>>` (for Task 18).

- [ ] **Step 1: Write the failing tests** (a `CallRecord` from `trunk_recorder_plugin::testing::call(&dir, "sys1", 101).call`, fields edited as needed):

```rust
#[test] fn a_call_goes_start_end_concluded_and_keeps_one_row() { /* call_started → id; call_ended; call_concluded(existing Some(id)) → same id; state "concluded", recorded 1, audio_path Some, 1 transmission, 1 frequency, length_ms 3000 */ }
#[test] fn a_concluded_call_without_a_start_is_inserted_from_the_record() { /* record.start_time_ms = 0 → start_ms == record.start_time * 1000; tg_alpha_tag "TG 101"; units_of == [1234] */ }
#[test] fn the_same_path_concluded_twice_is_one_row() { /* two call_concluded(None) same path → same id; count 1; transmissions not doubled */ }
#[test] fn an_unrecorded_call_keeps_its_row_and_reason() { /* NewCall{recorded false, reason Some("encrypted")} → call_started, call_ended → state "ended", recorded 0, audio_path None, not_recorded_reason "encrypted" */ }
#[test] fn transmission_lengths_run_to_the_next_key_up() {
    let l = [src_at(0.0), src_at(2.5), src_at(4.0)];
    assert_eq!(transmission_len_s(&l, 0, 10.0), 2.5);
    assert_eq!(transmission_len_s(&l, 2, 10.0), 6.0);
}
#[test] fn list_calls_filters_sorts_and_counts() { /* 5 calls over tgids 101/202, 1 emergency, 1 unrecorded, units via transmissions: tgids=[101] total 3; recorded Some(false) total 1; unit_ids=[1234] matches; limit 2 offset 2 → 1 of 3; sort Tgid asc order */ }
```

- [ ] **Step 2: Run `cargo test store::calls`** — expect compile failure.
- [ ] **Step 3: Implement.** `list_calls` builds one `WHERE` with positional parameters; `unit_ids` as `EXISTS (SELECT 1 FROM call_transmissions t WHERE t.call_id = calls.id AND t.unit IN (…))`; `total` from `COUNT(*)` with the same `WHERE`.
- [ ] **Step 4: Run `cargo test store::calls`** — expect PASS.
- [ ] **Step 5: Commit** `store: calls with transmissions and frequencies; list filters`.

---

### Task 6: Store: talkgroups, units, unit events (incl. synthesized call/end)

**Files:**
- Create: `src/store/talkgroups.rs`, `src/store/units.rs`
- Test: both files

**Interfaces:**
- Produces (`talkgroups.rs`): `pub struct TgTags { alpha_tag: String, description: String, group: String, tag: String }` with `TgTags::from_record(&CallRecord)` and `TgTags::from_info(&CallInfo)` (only `alpha_tag`); `touch_talkgroup(conn, system_id, tgid, tags: &TgTags, heard_ms, encrypted: bool, algid: Option<u8>) -> rusqlite::Result<()>` (upsert: `call_count + 1`, `encrypted_count + encrypted`, `last_algid` when `Some`, each tag field updated only when non-empty); `talkgroup(conn, system_id, tgid) -> rusqlite::Result<Option<TalkgroupRow>>`; `talkgroups_by_tgid(conn, tgid) -> rusqlite::Result<Vec<TalkgroupRow>>` (Review Focus 1); `pub struct TalkgroupFilter { system_ids: Vec<i64>, group: Option<String>, search: Option<String>, sort: TgSort, desc: bool, limit: u32, offset: u32 }`, `pub enum TgSort { AlphaTag, Tgid, Group, LastSeen, CallCount, Calls1h, Calls24h, UnitCount }`; `list_talkgroups(conn, &TalkgroupFilter, now_ms) -> rusqlite::Result<(Vec<TalkgroupRow>, i64)>`; `TalkgroupRow` = the table's columns plus `call_count_30d, calls_1h, calls_24h, unit_count_30d: i64` and `relevance_score: Option<i64>`, computed live (plan call: no 5-minute cache in phase 1); `units_on_talkgroup(conn, system_id, tgid, since_ms, limit, offset) -> rusqlite::Result<(Vec<(UnitRow, i64 /*call_count*/)>, i64)>`; `encryption_stats(conn, since_ms, system_ids: &[i64]) -> rusqlite::Result<Vec<EncryptionStatRow { system_id, tgid, alpha_tag, description, encrypted_calls, clear_calls }>>`.
- Produces (`units.rs`): `touch_unit(conn, system_id, unit: u32, tag: &str, tag_ota: &str, seen_ms, last_event: Option<(&str, Option<u32>)>) -> rusqlite::Result<()>` (first/last seen; `alpha_tag` from `tag` when non-empty with `alpha_tag_source = 'recorder'`; `ota_alpha_tag` from `tag_ota` when non-empty; `last_event_*` when given; `registered` 1 on `on`, 0 on `off`); `unit(conn, system_id, unit_id) -> rusqlite::Result<Option<UnitRow>>`; `units_by_id(conn, unit_id) -> rusqlite::Result<Vec<UnitRow>>`; `pub struct UnitFilter { system_ids, search: Option<String>, active_within_ms: Option<i64>, tgids: Vec<u32>, sort: UnitSort, desc, limit, offset }`, `pub enum UnitSort { AlphaTag, UnitId, LastSeen, LastEventTime }`; `list_units(conn, &UnitFilter, now_ms) -> rusqlite::Result<(Vec<UnitRow>, i64)>`.
- Produces (`units.rs`): `pub fn event_type_of(kind: &str) -> Option<(&'static str, Option<serde_json::Value>)>`: registration→`on`, deregistration→`off`, affiliation→`join`, acknowledge→`ackresp`, location→`location`, data_grant→`data`, answer_request→`ans_req`, call_alert→`call_alert`, emergency_alarm→`signal` with `{"signaling_type":"P25","signal_type":"emergency"}` (plan call: tr-engine has no alarm type; `signal`/`emergency` is how its pages show one); anything else → `None`.
- Produces: `pub struct NewUnitEvent { system_id: SystemId, site_id: Option<SiteId>, unit: u32, event_type: &'static str, time_ms: i64, tgid: Option<u32>, emergency: bool, call_id: Option<CallId>, metadata_json: Option<Value> }`; `insert_unit_event(conn, &NewUnitEvent) -> rusqlite::Result<Option<i64>>` (`INSERT OR IGNORE … RETURNING id`; `None` when the partial unique index rejected a duplicate); `pub fn transmission_events(call_id: CallId, system_id: SystemId, site_id: Option<SiteId>, tgid: u32, sources: &[(u32, i64, bool)] /* unit, time_ms, emergency */, end_ms: Option<i64>) -> Vec<NewUnitEvent>` — one `call` per source at its time and, when `end_ms` is `Some`, one `end` per source at the next source's time (the last at `end_ms`); `pub struct UnitEventFilter { system_ids: Vec<i64>, unit_ids: Vec<u32>, tgids: Vec<u32>, types: Vec<String>, emergency: Option<bool>, start_ms: Option<i64>, end_ms: Option<i64>, sort: EventSort, desc: bool, limit: u32, offset: u32 }`, `pub enum EventSort { Time, Unit, Tgid, EventType }`; `list_unit_events(conn, &UnitEventFilter) -> rusqlite::Result<(Vec<UnitEventRow>, i64)>` where `UnitEventRow` joins `units.alpha_tag`, `talkgroups.alpha_tag/description`, `systems.name/short_name`; `affiliation_events_since(conn, since_ms) -> rusqlite::Result<Vec<UnitEventRow>>` (types join/off/call/end/location, oldest first, for Task 8).

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn talkgroup_counters_and_tags_follow_calls() { /* 3 touches, one encrypted algid 0x84 → call_count 3, encrypted_count 1, last_algid 0x84; tags from the 2nd touch survive an empty 3rd */ }
#[test] fn live_stats_count_the_right_windows() { /* calls at now-30min, now-2h, now-40d → calls_1h 1, calls_24h 2, call_count_30d 2 */ }
#[test] fn search_matches_tag_number_and_description_with_relevance() { /* "fire" matches alpha "Fire Disp" (50) and a description (10); "101" matches tgid 101 (100) */ }
#[test] fn kinds_map_to_tr_engine_event_types() {
    assert_eq!(event_type_of("registration").unwrap().0, "on");
    assert_eq!(event_type_of("affiliation").unwrap().0, "join");
    assert_eq!(event_type_of("data_grant").unwrap().0, "data");
    let (t, m) = event_type_of("emergency_alarm").unwrap();
    assert_eq!((t, m.unwrap()["signal_type"].as_str()), ("signal", Some("emergency")));
    assert!(event_type_of("something_new").is_none());
}
#[test] fn transmissions_make_one_call_and_one_end_per_unit() {
    let ev = transmission_events(CallId(1), SystemId(1), None, 101, &[(10, 1_000, false), (20, 3_500, false)], Some(6_000));
    let got: Vec<_> = ev.iter().map(|e| (e.unit, e.event_type, e.time_ms)).collect();
    assert_eq!(got, [(10, "call", 1_000), (20, "call", 3_500), (10, "end", 3_500), (20, "end", 6_000)]);
    assert_eq!(transmission_events(CallId(1), SystemId(1), None, 101, &[(10, 1_000, false)], None).len(), 1);
}
#[test] fn a_synthesized_event_inserted_twice_is_stored_once() { /* same NewUnitEvent{call_id Some} twice → second returns None, count 1; a control-channel event (call_id None) twice → 2 rows */ }
#[test] fn unit_last_event_and_registration_follow_events() { /* ("on", None) → registered 1; ("join", Some(101)) → last_event_tgid 101; ("off", None) → registered 0 */ }
```

- [ ] **Step 2: Run `cargo test store::talkgroups store::units`** — expect compile failure.
- [ ] **Step 3: Implement.** Search: `alpha_tag LIKE '%q%' OR description LIKE … OR "group" LIKE … OR tag LIKE … OR CAST(tgid AS TEXT) LIKE 'q%'`; relevance 100 on `alpha_tag = q OR tgid = q`, 50 on `alpha_tag LIKE 'q%'`, else 10. Window counts as correlated subqueries on `calls(system_id, tgid, start_ms)`; `unit_count_30d` as `COUNT(DISTINCT unit)` over `call_transmissions` joined to those calls plus `unit_events` on the tgid.
- [ ] **Step 4: Run `cargo test store::`** — expect PASS.
- [ ] **Step 5: Commit** `store: talkgroups, units and unit events; synthesized call/end events`.

---

### Task 7: Store: recorder state, decimation, retention

**Files:**
- Create: `src/store/recorder.rs`
- Test: `src/store/recorder.rs`

**Interfaces:**
- Produces: `insert_recorder_state(conn, site_id: SiteId, time_ms, control_channel_hz: Option<u64>, decode_rate: f64, active_calls: u32, recording: u32) -> rusqlite::Result<()>`; `pub struct RecorderStateRow { site_id, time_ms, control_channel_hz, decode_rate, active_calls, recording }`; `latest_recorder_state(conn) -> rusqlite::Result<Vec<RecorderStateRow>>` (newest per site); `first_state_ms(conn, SiteId) -> rusqlite::Result<Option<i64>>`; `decode_rates(conn, system_ids: &[i64], start_ms: Option<i64>, end_ms: Option<i64>, limit: u32) -> rusqlite::Result<Vec<RecorderStateRow>>` (newest first); `decimate(conn, now_ms) -> rusqlite::Result<u64>` (older than 7 days: keep the first row per site per minute; older than 30 days: per hour; returns rows deleted); `retain(conn, keep_days: u32, now_ms) -> rusqlite::Result<u64>` (0: no-op; else deletes `calls` older than the cut-off, cascading, plus `unit_events` and `recorder_state` older than it; returns calls deleted).

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn decimation_keeps_one_row_a_minute_after_a_week() { /* 120 rows 1 s apart at now-8d → 2 remain for that site; rows at now-1h untouched */ }
#[test] fn decimation_keeps_one_row_an_hour_after_a_month() { /* 180 rows 1 min apart at now-31d → 3 remain */ }
#[test] fn retention_zero_keeps_everything() { assert_eq!(retain(c, 0, now).unwrap(), 0); }
#[test] fn retention_deletes_old_calls_and_their_children() { /* calls at now-40d (2 transmissions) and now-1d; retain(30) → 1; its call_transmissions gone (cascade); unit_events older than 30 d gone */ }
```

- [ ] **Step 2: Run** — expect compile failure.
- [ ] **Step 3: Implement.** Decimation deletes rows whose `id` isn't the `MIN(id)` of their `(site_id, time_ms / 60000)` (or `/ 3600000`) bucket within the age band. Cascade needs `PRAGMA foreign_keys = ON` (Task 3).
- [ ] **Step 4: Run `cargo test store::recorder`** — expect PASS.
- [ ] **Step 5: Commit** `store: recorder state with decimation; retention`.

---

### Task 8: Event bus (SSE core) and affiliation map

**Files:**
- Create: `src/bus.rs`, `src/affiliations.rs`
- Test: both

**Interfaces:**
- Produces (`bus.rs`): `pub struct Event { id: String /* "{unix_ms}-{seq}" */, kind: EventKind, sub_type: Option<String>, system_id: Option<i64>, site_id: Option<i64>, tgid: Option<u32>, unit_id: Option<u32>, emergency: bool, data: String /* JSON */ }`; `pub enum EventKind { CallStart, CallUpdate, CallEnd, UnitEvent, RecorderUpdate, RateUpdate }` with `as_str()` giving the spec names (`call_start`, `call_update`, `call_end`, `unit_event`, `recorder_update`, `rate_update`); `pub struct Bus` (Clone) with `Bus::new(ring: usize)` (4096 in production); `publish(&self, NewEvent) -> Event` (assigns the id); `subscribe(&self, filter: Filter) -> Subscription { rx: tokio::sync::mpsc::Receiver<Event>, .. }` (channel of 64; a full channel drops the event for that subscriber; dropping the `Subscription` unsubscribes); `replay_since(&self, last_id: &str, &Filter) -> Vec<Event>` (events after `last_id`; every buffered event when the id is gone); `pub struct Filter { systems: Vec<i64>, sites: Vec<i64>, tgids: Vec<u32>, units: Vec<u32>, types: Vec<String> /* "unit_event" or "unit_event:call" */, emergency_only: bool }` with `Filter::parse(query: &HashMap<String, String>) -> Filter` and `matches(&self, &Event) -> bool` (AND-ed; an event's `None` passes that dimension; compound types match `kind:sub_type`).
- Produces (`affiliations.rs`): `pub struct Affiliations` (Clone over `Arc<Mutex<…>>`); `pub struct Names { system_name: String, sysid: String, unit_alpha_tag: String, tg_alpha_tag: String, tg_description: String, tg_tag: String, tg_group: String }`; `apply(&self, system_id: i64, unit: u32, event_type: &str, tgid: Option<u32>, time_ms: i64, names: Names)` with tr-engine's rules: `join` with a tgid → affiliated (sets `previous_tgid` and `affiliated_since` when the tgid changed); `off` → status `off`; `call`/`end`/`location` with a different tgid → implicit re-affiliation, same tgid → `last_event_time` only; `pub struct AffiliationFilter { system_ids, tgids, unit_ids, status: Option<String>, stale_threshold_ms: Option<i64>, active_within_ms: Option<i64>, limit, offset }`; `snapshot(&self, &AffiliationFilter, now_ms) -> (Vec<AffiliationEntry>, i64, BTreeMap<u32, i64>)` (page, total, `talkgroup_counts` of affiliated units over the filtered set); `rebuild(&self, rows: &[UnitEventRow])` (Task 6's `affiliation_events_since`).

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn ids_are_unique_and_increasing() { /* 3 publishes → ids differ; seq part increases */ }
#[test] fn the_ring_replays_after_an_id_and_everything_when_the_id_is_gone() { /* ring 4: publish 6; replay_since(4th id) → [5th, 6th]; replay_since("0-0") → the last 4 */ }
#[test] fn filters_and_together_and_let_blank_dimensions_through() {
    let f = Filter::parse(&q(&[("systems", "1"), ("types", "unit_event:call,call_start")]));
    assert!(f.matches(&ev(EventKind::UnitEvent, Some("call"), Some(1))));
    assert!(!f.matches(&ev(EventKind::UnitEvent, Some("end"), Some(1))));
    assert!(f.matches(&ev(EventKind::CallStart, None, Some(1))));
    assert!(!f.matches(&ev(EventKind::CallStart, None, Some(2))));
    assert!(Filter::parse(&q(&[("systems", "1")])).matches(&ev(EventKind::RecorderUpdate, None, None)));
}
#[tokio::test] async fn subscribers_get_only_what_their_filter_passes() { /* subscribe tgids=[101]; publish tg 101 and 202; try_recv → one event, then empty */ }
#[test] fn affiliations_follow_join_off_and_implicit_moves() { /* join 101 → affiliated 101; call on 202 → 202 with previous_tgid 101; off → "off"; snapshot talkgroup_counts counts only affiliated */ }
```

- [ ] **Step 2: Run** — expect compile failure.
- [ ] **Step 3: Implement** (`std::sync::Mutex` around a `VecDeque<Event>` and a `HashMap<u64, (Filter, mpsc::Sender<Event>)>`; `AtomicU64` for seq).
- [ ] **Step 4: Run `cargo test bus:: affiliations::`** — expect PASS.
- [ ] **Step 5: Commit** `bus: SSE ring buffer with filters and replay; live affiliation map`.

---

### Task 9: Ingest mapping and the Plugin implementation

**Files:**
- Create: `src/ingest.rs`
- Modify: `src/plugin.rs`
- Test: `src/ingest.rs`, `src/plugin.rs`

**Interfaces:**
- Consumes: Tasks 3–8.
- Produces: `pub struct Ingest { … }` with `Ingest::new(store: Store, bus: Bus, affiliations: Affiliations, host: Option<Host>, systems: &[System<Config, NoConfig>]) -> Result<Ingest, StoreError>` (ensures a row per system and one site per declared site name via `write_sync`), `handle(&mut self, msg: HostMessage)` and `backfill_record(&mut self, path: &str, record: &CallRecord, audio_path: Option<&str>, json_path: &str)` (the `call.concluded` path with no bus event and no `call.result`), `dropped(&self) -> u64` (events of unknown kind). Payload JSON for the bus comes from `fn call_json(conn, CallId) -> Value` — **until Task 14** a minimal object `{call_id, system_id, tgid, start_time, call_state}`; Task 14 swaps in `api::types::Call::from_row`.
- `handle` does spec C5. Keyed by `(system index, CallInfo.id)` in a `live: HashMap<(u16, u32), Live { id: CallId, units_told: HashSet<u32> }>` map:
  - `call.start` → `call_started`; `touch_talkgroup(TgTags::from_info)`; per `sources` entry: `touch_unit`, `insert_unit_event(call)`, bus `unit_event` (`sub_type "call"`); bus `call_start`.
  - `call.end` → `call_ended`; `call` events for sources not yet told; when `recording == false` also `end` events via `transmission_events(.., Some(end_ms))` (no `call.concluded` will come) and the `live` entry is removed; bus `call_update` (plan call: tr-engine's `call_end` means "audio available", so it's sent at `call.concluded`; pages that key on `call_end` then get the `audio_url`).
  - `call.concluded` → `call_concluded(existing = live id or None)`; `touch_talkgroup(TgTags::from_record)`; per `srcList` entry `touch_unit(tag, tag_ota)`; `transmission_events(.., Some(end_ms))` inserted (duplicates ignored); bus `call_end`; `host.call_result(path, Ok, "", "")`; `live` entry removed.
  - `unit` → `event_type_of`; `None` → `dropped += 1`; else `ensure_site` if the site name is new (`write_sync`, once per site), `touch_unit`, `insert_unit_event`, `affiliations.apply`, bus `unit_event` with `sub_type = event_type` and the `UnitEvent` JSON.
  - `status` → per `SystemStatus`: `ensure_site(site_name)`, `set_identity`, `insert_recorder_state`, bus `recorder_update` and `rate_update` (JSON from Task 15's types; until then `{}`).
  - `shutdown` → `store.flush()`.
  Writes go through `store.write(move |c| …)`; `handle` never holds a `Connection` except in the two `write_sync` cases above.
- Produces (`plugin.rs`): `pub struct Reporter { ingest: Ingest, host: Host, store: Store, server: Option<server::ServerHandle> /* Task 19 */, started: Instant }`; `impl Plugin for Reporter` with `start` (for now: `Store::open(data_dir/engine.sqlite)`, `meta_set("capture_dir" | "host_name" | "host_version")`, `Ingest::new`, `affiliations.rebuild(affiliation_events_since(now - 7 d))`, `host.status(Ok, "storing history in engine.sqlite")`), each callback forwarding to `ingest.handle`, `shutdown` → `store.flush()`.

- [ ] **Step 1: Write the failing tests** (drive `Ingest` with an in-memory store and a bus subscription; drive `Reporter` with `testing::run`):

```rust
#[test] fn a_recorded_call_is_one_row_with_its_units_and_events() {
    // CallStart{id 7, tg 101, sources [u10@t0]} → CallEnd{sources [u10, u20@t0+2s]} → CallConcluded(testing::call edited: call_num 7, srcList u10 pos 0, u20 pos 2, stop +5 s)
    // calls: 1 row, state "concluded", recorded 1; call_transmissions 2; unit_events: exactly call×2 and end×2
    // bus order: call_start, unit_event(call), call_update, unit_event(call), call_end; host sent call.result Ok for the path
}
#[test] fn an_unrecorded_call_has_end_events_and_no_result() { /* CallStart{recording false, reason "encrypted"} → CallEnd → recorded 0, reason; unit_events call+end per source; no call.result */ }
#[test] fn a_concluded_call_whose_start_was_lost_is_stored() { /* CallConcluded alone → 1 row from the record; call.result Ok */ }
#[test] fn unit_events_map_and_feed_affiliations() { /* Unit{kind "affiliation", tg 101} → 1 row event_type "join"; affiliations has the unit on 101; bus unit_event sub_type "join"; Unit{kind "weird"} → dropped() == 1 */ }
#[test] fn status_fills_identity_and_recorder_state() { /* Status{sys_id, wacn, cc} → systems.sysid set; recorder_state 1 row; bus recorder_update + rate_update */ }
#[test] fn the_plugin_starts_stores_and_answers() {
    let dir = testing::temp_dir("reporter");
    let out = testing::run::<Reporter>([HostMessage::Hello(testing::hello(&dir, json!({"port": 0}))), HostMessage::CallConcluded(testing::call(&dir, "sys1", 101))]);
    assert!(out.ready());
    assert_eq!(out.results()[0].1, Outcome::Ok);
    assert!(dir.join("data/engine.sqlite").exists());
}
```

- [ ] **Step 2: Run** — expect compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run `cargo test ingest:: plugin::`** — expect PASS.
- [ ] **Step 5: Commit** `ingest: TRP events to rows and bus events; Reporter plugin`.

---

### Task 10: API skeleton: router, errors, auth, parameters, health, auth-init, openapi, schema test helper

**Files:**
- Create: `src/api/mod.rs`, `src/api/types.rs` (`Error`, `AmbiguousError`, time formatting), `src/api/health.rs`, `tests/common/mod.rs`, `tests/api_health.rs`
- Test: `tests/api_health.rs`, `src/api/mod.rs`

**Interfaces:**
- Produces: `pub struct AppState { store: Store, bus: Bus, affiliations: Affiliations, capture_dir: PathBuf, token: Option<String>, started: Instant, version: &'static str, host: HostStatus /* Arc<Mutex<HostStatusInner { name: String, connected: bool, last_seen_ms: Option<i64> }>> */, pages: Arc<web::Pages> /* Task 17; empty until then */ }` (Clone); `pub fn router(state: AppState) -> axum::Router` mounting `/api/v1/*` (and `/` from Task 17).
- Produces: `pub enum ApiError { BadRequest(String), InvalidParameter(String), InvalidTimeRange(String), NotFound(String), Ambiguous { what: &'static str, id: u32, matches: Vec<(i64, String, String)> /* system_id, name, sysid */ }, Unauthorized, NotImplemented(String), Internal(String) }` implementing `IntoResponse` with the spec's `Error { code, error, detail }` (`AmbiguousError` for 409; `NotImplemented` → **501**, `code: not_implemented`, `error: "not available in the Trunk Recorder Pro plugin"`, `detail: <path>`); `From<StoreError>` → `Internal`.
- Produces (`mod.rs`, `pub type Query = HashMap<String, String>`): `pagination(&Query) -> Result<(u32, u32), ApiError>` (limit default 50, 1..=1000; offset ≥ 0); `sort(&Query, default: &str, allowed: &[&str]) -> Result<(String, bool), ApiError>` (`-` prefix = desc; `sort_dir=desc` honoured when no prefix); `time_range(&Query) -> Result<(Option<i64>, Option<i64>), ApiError>` (RFC 3339 → ms; start > end → `InvalidTimeRange`); `ids(&Query, names: &[&str]) -> Vec<i64>` (comma lists; the spec's aliases are passed by callers: `["system_id","systems"]`, `["tgid","tgids"]`, `["unit_id","units","unit_ids"]`, `["site_id","sites"]`, `["sysid","sysids"]` (hex → `systems.sysid`)); `flag(&Query, name) -> Result<Option<bool>, ApiError>`; `composite(id: &str) -> Result<(Option<i64>, u32), ApiError>` (`1:9178`, `1-9178`, `9178`); `pub fn rfc3339(ms: i64) -> String`; `pub fn hex(v: Option<u32>) -> String` (`"0"` for `None`, upper-case).
- Produces: an auth middleware: when `token` is `Some`, require `Authorization: Bearer <token>` or `?token=<token>` on `/api/v1/*` except `/api/v1/health`, `/api/v1/auth-init`, `/api/v1/pages`, `/api/v1/openapi.yaml`; everything outside `/api/` is open.
- Produces (`health.rs`): `GET /health` → `HealthResponse { status: "healthy", version, uptime_seconds, checks: { database: "ok" }, trunk_recorders: [{ instance_id: host name, status: "connected"|"disconnected", last_seen }] }`; `GET /auth-init` → `{ mode: "open"|"token", jwt_enabled: false }`; `GET /openapi.yaml` → the embedded `openapi/openapi.yaml` (`include_str!`) as `application/yaml`; `GET /pages` → `state.pages` (filled in Task 17).
- Produces: `pub const OUT_OF_SCOPE: &[&str]` = every `paths:` key of the spec not served by Tasks 10–17 (with `{id}` segments), each registered as a route answering `NotImplemented(path)` for any method; anything else under `/api/v1` → 404 `not_found`.
- Produces (`tests/common/mod.rs`): `pub fn app() -> (Router, Store, Bus, Affiliations)` on an in-memory store with `capture_dir` = a temp dir; `pub async fn get(app: &Router, path: &str) -> (StatusCode, Value)` via `tower::ServiceExt::oneshot`; `pub async fn get_with(app, path, headers) -> (StatusCode, HeaderMap, Bytes)`; `pub fn seed(store: &Store) -> Seed { sys1: SystemId, sys2: SystemId, site1: SiteId, … }` (two systems — p25 with identity `3AB`/`BEE00`/NAC `3A0`, and conventional — three talkgroups, four units, six calls over the last 40 days, unit events, three status rows); `pub fn assert_schema(v: &Value, schema: &str)` — loads `openapi/openapi.yaml` once (`serde_yaml`), asserts every `required` property is present and every present property's JSON type matches the schema's `type` (resolving one level of `$ref` and `allOf`; `nullable` allows null; `format: date-time` must parse as RFC 3339; `enum` must contain the value).

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn health_is_open_and_shaped() { let (s, v) = get(&app().0, "/api/v1/health").await; assert_eq!(s, 200); assert_schema(&v, "HealthResponse"); assert_eq!(v["checks"]["database"], "ok"); }
#[tokio::test] async fn auth_init_says_open_or_token() { /* no token → mode "open"; token → "token", jwt_enabled false */ }
#[tokio::test] async fn a_token_guards_the_api_but_not_health_or_pages() { /* token "s3": /api/v1/systems → 401 code "unauthorized"; Bearer s3 → 200; ?token=s3 → 200; /api/v1/health, /auth-init, /pages, /openapi.yaml → 200 without */ }
#[tokio::test] async fn out_of_scope_paths_say_so_with_501() { let (s, v) = get(&app().0, "/api/v1/transcriptions/search").await; assert_eq!(s, 501); assert_eq!(v["code"], "not_implemented"); assert_schema(&v, "Error"); }
#[tokio::test] async fn unknown_paths_are_404() { assert_eq!(get(&app().0, "/api/v1/nothing").await.0, 404); }
#[tokio::test] async fn every_spec_path_is_served_or_declared_out_of_scope() {
    // parse openapi.yaml paths; for each GET path substitute {id} with "1"; GET it; assert status != 404
}
#[test] fn parameters_parse_as_the_spec_says() {
    assert_eq!(pagination(&q(&[])).unwrap(), (50, 0));
    assert!(pagination(&q(&[("limit", "0")])).is_err() && pagination(&q(&[("limit", "1001")])).is_err());
    assert_eq!(sort(&q(&[("sort", "-last_seen")]), "alpha_tag", &["alpha_tag", "last_seen"]).unwrap(), ("last_seen".into(), true));
    assert!(sort(&q(&[("sort", "bogus")]), "alpha_tag", &["alpha_tag"]).is_err());
    assert_eq!(ids(&q(&[("tgids", "1,2")]), &["tgid", "tgids"]), vec![1, 2]);
    assert_eq!(composite("1:9178").unwrap(), (Some(1), 9178));
    assert_eq!(composite("1-9178").unwrap(), (Some(1), 9178));
    assert_eq!(composite("9178").unwrap(), (None, 9178));
    assert!(matches!(time_range(&q(&[("start_time", "2024-01-02T00:00:00Z"), ("end_time", "2024-01-01T00:00:00Z")])), Err(ApiError::InvalidTimeRange(_))));
    assert_eq!(rfc3339(1_705_314_600_000), "2024-01-15T10:30:00.000Z");
    assert_eq!((hex(Some(0xBEE00)), hex(None)), ("BEE00".into(), "0".into()));
}
```

- [ ] **Step 2: Run `cargo test --test api_health api::`** — expect compile failure.
- [ ] **Step 3: Implement.** Handlers run store reads inside `tokio::task::spawn_blocking`.
- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** `api: router, errors, auth, parameters, health; schema-checked tests`.

---

### Task 11: API: systems and sites

**Files:**
- Create: `src/api/systems.rs`, `tests/api_systems.rs`
- Modify: `src/api/types.rs`, `src/api/mod.rs`

**Interfaces:**
- Produces (`types.rs`): `System { system_id, system_type, name, sysid: String, wacn: String, sites: Vec<Site> }` with `System::from_rows(&SystemRow, Vec<SiteRow>, instance_id: &str)` and `pub fn system_type(kind: &str) -> &'static str` (p25→`p25`, smartnet→`smartnet`, conventional→`conventional`, dmr→`dmr`, nxdn→`nxdn`, edacs→`edacs`); `Site { site_id, system_id, short_name, instance_id, nac: Option<String> /* hex */, rfss, p25_site_id, sys_num: None }`; `name` served = `systems.name` when non-empty else `short_name`.
- Produces: `GET /systems` → `SystemListResponse`; `GET /systems/{id}` (404 when missing); `GET /sites/{id}`; `GET /p25-systems` → `P25SystemListResponse` (kind p25; `talkgroup_count`, `unit_count`, `calls_24h`); `PATCH /systems/{id}`, `PATCH /sites/{id}` → 501.

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn systems_list_with_sites_and_hex_identity() { /* seed; 200; assert_schema SystemListResponse and each System/Site; sysid "3AB", wacn "BEE00", sites[0].nac "3A0"; conventional sysid "0", one site named after the system */ }
#[tokio::test] async fn a_missing_system_is_404_and_patch_is_501() { /* /systems/99 → 404 code "not_found"; PATCH /systems/1 → 501 */ }
#[tokio::test] async fn p25_systems_count_their_things() { /* seeded counts: talkgroup_count, unit_count, calls_24h as seeded */ }
```

- [ ] **Step 2: Run** — fail. **Step 3: Implement.** **Step 4: `cargo test --test api_systems`** — PASS.
- [ ] **Step 5: Commit** `api: systems, sites, p25-systems`.

---

### Task 12: API: talkgroups

**Files:**
- Create: `src/api/talkgroups.rs`, `tests/api_talkgroups.rs`
- Modify: `src/api/types.rs` (`Talkgroup`, `EncryptionStat`), `src/api/mod.rs`

**Interfaces:**
- Consumes: Task 6 talkgroup functions; Task 5 `list_calls`; Task 10 helpers.
- Produces (`types.rs`): `Talkgroup { system_id, system_name, sysid, tgid, alpha_tag, tag, group, description, mode: &'static str /* "E" when encrypted_count > 0 else "D" (plan call) */, first_seen, last_seen, call_count, calls_1h, calls_24h, unit_count, relevance_score: Option<i64> }` from `TalkgroupRow` + `SystemRow`.
- Produces: `pub fn resolve_talkgroup(conn, id: &str) -> Result<TalkgroupRow, ApiError>`: composite → exact row or 404; plain → `talkgroups_by_tgid`: 0 → 404, 1 → it, >1 → `Ambiguous` (Review Focus 1).
- Produces: `GET /talkgroups` (`system_id`, `sysid`, `group`, `search`; `stats_days` present → 400; sort `alpha_tag, tgid, group, last_seen, call_count, calls_1h, calls_24h, unit_count`, default `alpha_tag`); `GET /talkgroups/{id}`; `GET /talkgroups/{id}/units` (`window` 1..=1440 minutes, default 60; `UnitListResponse` with `call_count`); `GET /talkgroups/encryption-stats` (`hours` default 24, `sysid`) → `EncryptionStatsResponse`; `GET /talkgroups/{id}/calls` → **routed in Task 14** (needs `Call`); `PATCH` → 501.

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn talkgroups_list_sorts_and_searches() { /* ?sort=-calls_1h&limit=2 → ordered, total 3; ?search=fire → relevance_score present; ?stats_days=7 → 400; assert_schema TalkgroupListResponse, Talkgroup */ }
#[tokio::test] async fn plain_ids_are_ambiguous_across_systems() { /* tg 9178 seeded in both systems: /talkgroups/9178 → 409 code "ambiguous_id", matches len 2, assert_schema AmbiguousError; /talkgroups/1:9178 and /talkgroups/2-9178 → 200; /talkgroups/5 → 404 */ }
#[tokio::test] async fn units_on_a_talkgroup_within_a_window() { /* calls at now-10m (u10, u20) and now-3h (u30): ?window=60 → 2 with call_count; ?window=0 → 400 */ }
#[tokio::test] async fn encryption_stats_count_clear_and_encrypted() { /* assert_schema EncryptionStatsResponse */ }
```

- [ ] **Step 2–4: fail, implement, `cargo test --test api_talkgroups` PASS.**
- [ ] **Step 5: Commit** `api: talkgroups list, detail, units, encryption stats; ambiguity`.

---

### Task 13: API: units, unit events, unit affiliations

**Files:**
- Create: `src/api/units.rs`, `tests/api_units.rs`
- Modify: `src/api/types.rs` (`Unit`, `UnitEvent`, `Affiliation`), `src/api/mod.rs`, `src/ingest.rs` (use `UnitEvent::from_row` JSON for the `unit_event` bus payload)

**Interfaces:**
- Consumes: Task 6 unit/event functions; `Affiliations::snapshot` (Task 8); Task 10 helpers.
- Produces (`types.rs`): `Unit { system_id, system_name, sysid, unit_id, alpha_tag, alpha_tag_source: Option<&'static str> /* "csv" when the store says 'recorder' and the tag is non-empty, else omitted */, first_seen, last_seen, last_event_type, last_event_time, last_event_tgid, last_event_tg_tag, recorder_alpha_tag, ota_alpha_tag, call_count: Option<i64>, relevance_score: Option<i64> }`; `UnitEvent { id, event_type, time, system_id, system_name, unit_rid, unit_id, unit_alpha_tag, tgid, tg_alpha_tag, tg_description, instance_id, metadata_json, signaling_type, signal_type }` (the last two lifted from `metadata_json` for `signal`) with `UnitEvent::from_row(&UnitEventRow, instance_id)`; `Affiliation { system_id, system_name, sysid, unit_id, unit_alpha_tag, tgid, tg_alpha_tag, tg_description, tg_tag, tg_group, previous_tgid, affiliated_since, last_event_time, status }`.
- Produces: `resolve_unit(conn, id) -> Result<UnitRow, ApiError>` (as `resolve_talkgroup`); `GET /units` (`sysid`, `search`, `active_within` minutes, `talkgroup`; sort `alpha_tag, unit_id, last_seen, last_event_time`, default `unit_id`); `GET /units/{id}`; `GET /units/{id}/events` (`type`, `talkgroup`, time range); `GET /unit-events` (requires `system_id|systems` or `sysid|sysids` else 400 `invalid_parameter`; `unit_id|units|unit_ids`, `type` comma list, `tgid|tgids`, `emergency`, time range default the last hour, **max 24 h** else `invalid_time_range`; sort `time, unit_rid, tgid, event_type`, default `-time`); `GET /unit-affiliations` (`system_id`, `sysid`, `tgid`, `unit_id`, `status`, `stale_threshold` s, `active_within` s, pagination) → `AffiliationListResponse` with `summary.talkgroup_counts`; `GET /units/{id}/calls` → **routed in Task 14**; `PATCH /units/{id}` → 501.

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn units_list_filters_by_activity_and_talkgroup() { /* ?active_within=30 excludes a unit seen 2 h ago; ?talkgroup=101 includes units with events on 101; assert_schema UnitListResponse, Unit */ }
#[tokio::test] async fn a_unit_detail_embeds_its_last_event() { /* last_event_type "join", last_event_tgid 101, last_event_tg_tag "Fire Disp" */ }
#[tokio::test] async fn unit_events_need_a_system_and_cap_the_range() { /* no system → 400 "invalid_parameter"; 25 h range → 400 "invalid_time_range"; ?system_id=1&type=join,on → only those; a signal event carries signaling_type/signal_type; assert_schema UnitEventListResponse, UnitEvent */ }
#[tokio::test] async fn affiliations_come_from_the_live_map() { /* apply join/off on the app's Affiliations; ?status=affiliated → 1; summary.talkgroup_counts {"101": 1}; assert_schema AffiliationListResponse */ }
#[tokio::test] async fn unit_ids_are_ambiguous_too() { /* same unit in both systems → 409 */ }
```

- [ ] **Step 2–4: fail, implement, `cargo test --test api_units ingest::` PASS.**
- [ ] **Step 5: Commit** `api: units, unit events, live affiliations`.

---

### Task 14: API: calls, audio, frequencies, transmissions

**Files:**
- Create: `src/api/calls.rs`, `tests/api_calls.rs`
- Modify: `src/api/types.rs` (`Call`, `CallUnit`, `CallTransmission`, `CallFrequency`), `src/api/mod.rs`, `src/api/talkgroups.rs`, `src/api/units.rs` (their `/calls` routes), `src/ingest.rs` (`call_json` → `Call::from_row`)

**Interfaces:**
- Consumes: Task 5 read functions; Task 10 helpers.
- Produces (`types.rs`): `Call` with every spec field the plugin can fill: `call_id, system_id, system_name, sysid, site_id, site_short_name, tgid, tg_alpha_tag, tg_description, tg_tag, tg_group, start_time, stop_time: Option, duration /* seconds; elapsed so far while recording */, audio_url: Option /* "/api/v1/calls/{id}/audio" only when recorded and state concluded */, audio_type, freq, freq_error, signal_db, noise_db, error_count, spike_count, call_state /* recording→"recording", ended→"stopped", concluded→"completed" */, emergency, encrypted, analog, conventional /* kind == conventional */, phase2_tdma, tdma_slot, patched_tgids, src_list: Vec<CallTransmission>, freq_list: Vec<CallFrequency>, unit_ids, units: Vec<CallUnit>, has_transcription: false, metadata_json`, plus D6's `recorded, not_recorded_reason, decrypted, algid, key_id, priority, duplex, packet`; `Call::from_row(conn, &CallRow) -> rusqlite::Result<Call>` (joins system/site names, transmissions with unit tags, frequencies); `CallTransmission { src, tag, time, pos, duration, emergency: i64 /* 0/1 */, signal_system: "" }`; `CallFrequency { freq, time, pos, len, error_count, spike_count }`; `CallUnit { system_id, unit_id, alpha_tag }`.
- Produces: `GET /calls` (spec filters plus `recorded`; `deduplicate` accepted and ignored — one recorder has no duplicates; sort `start_time, stop_time, duration, tgid, freq`, default `-start_time`); `GET /calls/active` (no pagination, `ActiveCallListResponse`); `GET /calls/{id}`; `GET /calls/{id}/frequencies`; `GET /calls/{id}/transmissions`; `GET /calls/{id}/audio` → `tower_http::services::ServeFile::new(capture_dir.join(audio_path))` (ranges and `Content-Type` `audio/wav` / `audio/mp4` from the extension) after checking that `capture_dir.join(audio_path).canonicalize()` starts with `capture_dir.canonicalize()` (Review Focus 2); 404 when the row has no `audio_path`, the file is gone, or the check fails; `GET /talkgroups/{id}/calls` and `GET /units/{id}/calls` (time range, `recorded`, pagination).

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn calls_list_filters_sorts_and_pages() { /* seeded 6: ?tgid=101&recorded=false; ?unit_id=1234; ?emergency=true; ?sort=duration&limit=2&offset=1; totals; assert_schema CallListResponse, Call; D6 fields present */ }
#[tokio::test] async fn a_call_detail_has_units_transmissions_and_audio_url() { /* concluded recorded → audio_url Some("/api/v1/calls/{id}/audio"), call_state "completed", src_list[0].duration, units[0].alpha_tag */ }
#[tokio::test] async fn active_calls_are_the_ones_still_recording() { /* 1 recording + 1 concluded → total 1; audio_url null; stop_time null; duration > 0 */ }
#[tokio::test] async fn audio_streams_with_ranges_and_stays_inside_the_capture_folder() {
    // a testing::call 3 s WAV → GET audio: 200, content-type audio/wav, body 48044 bytes; Range: bytes=0-99 → 206, 100 bytes
    // a row whose audio_path is "../../outside.wav" (file created outside the dir) → 404; audio_path None → 404; unknown id → 404
}
#[tokio::test] async fn talkgroup_and_unit_call_lists_work() { /* /talkgroups/1:101/calls total; /units/1:1234/calls total; ?recorded=true */ }
```

- [ ] **Step 2–4: fail, implement, `cargo test --test api_calls --test api_talkgroups --test api_units ingest::` PASS.**
- [ ] **Step 5: Commit** `api: calls, active calls, audio with ranges, frequencies, transmissions`.

---

### Task 15: API: stats, decode rates, recorders

**Files:**
- Create: `src/api/stats.rs`, `tests/api_stats.rs`
- Modify: `src/api/types.rs` (`StatsResponse`, `SystemActivity`, `DecodeRate`, `Recorder`), `src/api/mod.rs`, `src/ingest.rs` (`recorder_update` / `rate_update` payloads)

**Interfaces:**
- Consumes: Task 7 functions; counts from Tasks 5–6.
- Produces: `GET /stats` → `StatsResponse { systems: sites count, talkgroups, units, total_calls, calls_24h, calls_1h, total_duration_hours, system_activity: [SystemActivity { system_id, system_name, sysid, calls_1h, calls_24h, active_talkgroups, active_units }] }`; `GET /stats/rates` (time range, `system_id`, `limit` 1..=10000 default 1000) → `DecodeRatesResponse` with `DecodeRate { time, system_id, system_name, sysid, decode_rate /* TRP's messages per second */, decode_rate_interval /* same */, control_channel }`; `GET /recorders` → one `Recorder` per site from `latest_recorder_state`: `id = "{system_id}_{site_id}"`, `src_num = system_id`, `rec_num = site_id`, `type = kind upper-cased`, `rec_state = "recording" if recording > 0 else "available" if control_channel_hz else "idle"`, `freq = control_channel_hz or 0`, `count = recording`, `duration = (now - first_state_ms) / 1000`, `squelched: false`, `tgid/tg_alpha_tag/unit_id/unit_alpha_tag: null` (spec C7); the other `/stats/*` and `/analytics/*` → 501.
- Produces: `Recorder::from_site(&SiteRow, &SystemRow, &RecorderStateRow, first_ms, now_ms)` and `DecodeRate::from_state(&SiteRow, &SystemRow, &RecorderStateRow)`, used by `ingest.rs`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn stats_count_calls_in_windows_and_per_system() { /* assert_schema StatsResponse; seeded calls_1h, calls_24h, total_calls; system_activity len 2 */ }
#[tokio::test] async fn rates_come_from_recorder_state() { /* 3 seeded status rows → total 3; ?limit=1 → 1; assert_schema DecodeRatesResponse */ }
#[tokio::test] async fn recorders_are_one_per_site() { /* 2 sites, one recording → "recording" / "available"; id "1_1"; assert_schema RecorderListResponse */ }
#[tokio::test] async fn other_stats_are_501() { /* /stats/call-heatmap, /analytics/decode-rates → 501 */ }
```

- [ ] **Step 2–4: fail, implement, `cargo test --test api_stats ingest::` PASS.**
- [ ] **Step 5: Commit** `api: stats, decode rates, recorders from site status`.

---

### Task 16: API: SSE stream

**Files:**
- Create: `src/api/sse.rs`, `tests/api_sse.rs`
- Modify: `src/api/mod.rs`

**Interfaces:**
- Consumes: `Bus::subscribe`, `Bus::replay_since`, `Filter::parse` (Task 8).
- Produces: `GET /events/stream` → `text/event-stream` with `Cache-Control: no-cache`, `X-Accel-Buffering: no`; frames exactly `id: {id}\nevent: {kind}\ndata: {json}\n\n`; `: keepalive\n\n` every 15 s; `Last-Event-ID` (header, or `last_event_id` query) replays first, then live; filters from the query; `?token=` accepted (auth.js uses it for `EventSource`).

- [ ] **Step 1: Write the failing tests** (`axum::serve` on `127.0.0.1:0`; read frames with a raw `tokio::net::TcpStream`):

```rust
#[tokio::test] async fn the_stream_delivers_filtered_frames_in_sse_format() { /* connect ?types=call_start; publish call_start then unit_event; read one frame matching "id: …\nevent: call_start\ndata: {…}\n\n" and nothing else within 200 ms */ }
#[tokio::test] async fn last_event_id_replays_what_was_missed() { /* publish 3; connect with Last-Event-ID = 1st id → the 2nd and 3rd arrive first */ }
#[tokio::test] async fn a_token_in_the_query_opens_the_stream() { /* token set: without → 401; ?token= → 200 text/event-stream */ }
```

- [ ] **Step 2–4: fail, implement (`axum::response::Sse` over a stream merging the subscription receiver and a `tokio::time::interval` for keepalives), `cargo test --test api_sse` PASS.**
- [ ] **Step 5: Commit** `api: SSE event stream with filters, replay and keepalive`.

---

### Task 17: Pages: embed tr-engine's phase-1 pages, `/pages`, index

**Files:**
- Create: `src/web.rs`, `web/` (copied from tr-engine's `web/`: `index.html`, `call-history.html`, `talkgroup-research.html`, `units.html`, `unit-tracker.html`, `systems-overview.html`, `events.html`, `timeline.html`, `auth.js`, `theme-config.js`, `theme-engine.js`, plus any `.js` those pages load with `<script src>` — check each head; `audio-engine.js` only if referenced), `tests/api_pages.rs`
- Modify: `src/api/health.rs` (`/pages`), `src/api/mod.rs` (mount `/`), `src/plugin.rs` (`Pages::scan()` into `AppState`)

**Interfaces:**
- Produces: `#[derive(RustEmbed)] #[folder = "web/"] pub struct Assets;`; `pub struct PageInfo { path, title, description, order }`; `pub struct Pages(pub Vec<PageInfo>)` with `Pages::scan() -> Pages` (each embedded `*.html`'s first 2 KB against tr-engine's three regexes `<meta\s+name="card-title"\s+content="([^"]*)"`, `card-description`, `card-order`; sorted by order; pages without `card-title` excluded); `GET /api/v1/pages` → that list; `GET /` → `index.html`; `GET /{file}` → the embedded file with `mime_type()`; 404 otherwise; `POST /api/v1/pages` → 501.
- Rule (spec C8): a page is embedded only if every endpoint it calls is served. The phase-1 set was checked against each page's requests: index (`/health`, `/stats`, `/recorders`, `/talkgroups?sort=-calls_1h`, stream), call-history (`/systems`, `/calls`, `/calls/{id}`), talkgroup-research (`/systems`, `/talkgroups`, `/talkgroups/{id}`, `/talkgroups/{id}/units`, `/calls?tgid=|unit_id=`, `/unit-affiliations`, `/unit-events`), units (`/talkgroups`, `/talkgroups/{id}/units`, `/units/{id}`, `/calls?…deduplicate=true`, stream), unit-tracker (`/systems`, `/unit-affiliations`), systems-overview (`/health`, `/stats`, `/systems`, `/recorders`, stream `types=recorder_update`), events and timeline (stream).

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test] async fn pages_are_listed_from_their_meta_tags_in_order() { /* /api/v1/pages → array containing {"path": "/call-history.html", "title": "Call History"}; sorted by order; no "/storage.html" */ }
#[tokio::test] async fn the_index_and_assets_are_served() { /* GET / → 200 text/html containing "card-title"; GET /auth.js → 200 with a javascript content-type; GET /nope.html → 404 */ }
#[test] fn every_embedded_page_only_calls_served_endpoints() {
    // for each embedded .html, every "/api/v1/<seg>" or "apiFetch('/<seg>" occurrence's first path segment must be in api::SERVED_PREFIXES
    // = ["health","auth-init","systems","sites","p25-systems","talkgroups","units","unit-events","unit-affiliations","calls","stats","recorders","events","pages","openapi.yaml"]
}
```

- [ ] **Step 2–4: fail, implement, `cargo test --test api_pages web::` PASS.** The rendering check (every panel fed) is Task 19 Step 5.
- [ ] **Step 5: Commit** `web: tr-engine's phase-1 pages embedded; /pages`.

---

### Task 18: Backfill from the capture folder

**Files:**
- Create: `src/backfill.rs`
- Test: `src/backfill.rs`

**Interfaces:**
- Consumes: `Ingest::backfill_record` (Task 9); `newest_start_ms` (Task 5).
- Produces: `pub struct BackfillReport { files: u64, ingested: u64, skipped_known: u64, bad: u64 }`; `pub fn backfill(ingest: &mut Ingest, capture_dir: &Path, systems: &[(SystemId, String)], progress: &mut dyn FnMut(u64)) -> BackfillReport`: for each system, walk `capture_dir/<short_name>/<Y>/<M>/<D>/`, skipping day folders earlier than the day of `newest_start_ms` minus one day; read each `*.json` as `CallRecord`; `path` = relative path without extension; audio = sibling `.wav` if present else `.m4a` if present else `None`; `backfill_record`; `skipped_known` counts paths already in `calls` (checked with one `read` per file — fine at startup rates); a file that fails to parse → `bad += 1` and one `host.warn` naming it (Review Focus 3). `progress` is called every 100 files.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn backfill_ingests_new_files_and_skips_known_ones() { /* sys1/2023/11/14 with two testing::call files → ingested 2; again → skipped_known 2, ingested 0; calls count 2 */ }
#[test] fn a_bad_file_is_skipped_and_the_rest_ingested() { /* "{not json" beside a good one → bad 1, ingested 1 */ }
#[test] fn day_folders_older_than_the_store_are_not_walked() { /* store has a call on 2023-11-20; folders 2023/11/14 and 2023/11/21 → files 1 */ }
```

- [ ] **Step 2–4: fail, implement (`walkdir` depth 4, folder names parsed as integers, `.json` only), `cargo test backfill::` PASS.**
- [ ] **Step 5: Commit** `backfill: ingest call files already on disk`.

---

### Task 19: Start-up wiring: HTTP server, serve mode, maintenance, metrics, shutdown

**Files:**
- Create: `src/server.rs`, `tests/serve_mode.rs`
- Modify: `src/plugin.rs`, `src/main.rs`
- Test: `src/server.rs`, `src/plugin.rs`, `tests/serve_mode.rs`

**Interfaces:**
- Produces (`server.rs`): `pub struct ServerHandle { pub addr: SocketAddr, .. }`; `pub fn start(state: AppState, bind: &str, port: u16) -> Result<ServerHandle, String>`: binds a `std::net::TcpListener` first so a taken port is `Err(format!("port {port} is in use: change Port in the plugin's settings"))` (Review Focus 4), then runs `axum::serve` on a new thread's current-thread runtime; `ServerHandle::stop(self)` (graceful, 2 s).
- Produces (`plugin.rs`): `Reporter::start` in order: open store → `meta` → `Ingest::new` → `affiliations.rebuild(affiliation_events_since(now − 7 d))` → `Pages::scan()` → `server::start` unless `port == 0` → backfill thread when `config.backfill` (status `Warning, "backfilling: N calls"` until done) → maintenance thread (hourly `decimate`, daily `retain(retention_days)`, both via `store.write`; every 5 s `host.metrics(Metrics { queued: Some(store.queue_len()), extra: {"calls": total, "store_mb": file size, "serving": url, "dropped_events": ingest.dropped()} })`) → `host.info("serving tr-engine's API and pages at http://127.0.0.1:8092/")` → `host.status(Ok, "http://127.0.0.1:8092/ · N calls stored")`; `shutdown` → stop server, `store.flush()`, within `grace`. `HostStatus.connected = true`, `last_seen_ms` updated on every `status` message.
- Produces (`main.rs`): `Mode::Serve { data_dir }` → `Store::open(data_dir/engine.sqlite)` (missing → stderr "no store at …: run the plugin once from Trunk Recorder Pro first", exit 2); `capture_dir`, `host_name` from `meta`; `HostStatus.connected = false`; settings from `data_dir/serve.json` (same `Config` shape) when present, else defaults; logs to stderr; runs until Ctrl-C (`tokio::signal::ctrl_c`). `Mode::Describe` → `describe::<Reporter>()` pretty-printed; `Mode::Version` → the version.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn a_taken_port_is_a_settings_error_not_a_crash() {
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port();
    let err = server::start(test_state(), "127.0.0.1", port).err().unwrap();
    assert!(err.contains(&port.to_string()) && err.contains("Port"));
}
#[test] fn the_plugin_serves_while_running_and_stops_cleanly() {
    // hello with {"port": <free port>} fed through a pipe (not testing::run, which ends input at once): after `ready`, blocking GET /api/v1/health → 200 and trunk_recorders[0].status "connected"; close the pipe → exit 0 within 3 s
}
#[test] fn serve_mode_opens_an_existing_store() { /* testing::run once → spawn `cargo run -- serve <dir>/data` with serve.json {"port": free} → GET /api/v1/health 200, trunk_recorders[0].status "disconnected"; kill */ }
#[test] fn serve_mode_without_a_store_says_what_to_do() { /* child exits 2; stderr contains "run the plugin once" */ }
```

- [ ] **Step 2–4: fail, implement, `cargo test` (all) PASS.**
- [ ] **Step 5: Manual check (required before Task 20).** `cargo build --release`, then `trunk-pro plugin run target\release\reporter.exe C:\Users\drewm\AppData\Roaming\trunk-pro\captures --data-dir D:\reporter-data` (the test bench with a persistent store), then `reporter serve D:\reporter-data`; open `http://127.0.0.1:8092/` and walk every listed page: every panel shows data or an honest empty state, call playback plays and seeks, Live Events shows events while TRP records (run the plugin from TRP's Plugins page with **Add from a file** for that). Any page with a blank panel is removed from `web/` or its endpoint fixed before continuing.
- [ ] **Step 6: Commit** `start-up: HTTP server, serve mode, maintenance, metrics, shutdown`.

---

### Task 20: Release readiness and the upstream asks

**Files:**
- Modify: `README.md` (what it is, install from the store, settings, serve mode, what's not in phase 1), `CHANGELOG.md` (`## [0.1.0]`), `Cargo.toml` (final id/name — the owner's choice, spec C10)
- Create: three issues on `TrunkRecorder/trunk-recorder-pro` (spec C11): (a) manifest field for a UI URL/port → Open button and Plugins-page listing; (b) keep-running-while-idle flag with `hello` still sent at recording start; (c) talkgroup/unit file paths in `hello`. A fourth note, not an ask: publishing SDK 0.6 to crates.io lets the plugin read `SystemInfo.name` and `sites` from `hello` (today `System.name` falls back to the short name until a status message arrives).

- [ ] **Step 1:** `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked`, `cargo run -- --describe > m.json && python .github/check_manifest.py m.json --release` — all green.
- [ ] **Step 2:** Push to `github.com/trunk-reporter/<id>`; CI green; tag `v0.1.0`; the Release workflow produces four archives, the manifest and `SHA256SUMS`; `gh attestation verify` one archive.
- [ ] **Step 3:** Install the release in TRP (Plugins page, from the GitHub release URL) on the Windows machine: starts with default settings, the Website link opens the pages, the plugin card shows the status line and metrics.
- [ ] **Step 4:** File the three upstream issues; link them from README.
- [ ] **Step 5:** Submit to the registry (`TrunkRecorder/plugins`, `add-release`) once the D6 spec PR (Task 2) is merged.
- [ ] **Step 6: Commit** `docs: README, changelog 0.1.0`.

---

### Task 21: SDK 0.7 features (gated: starts when `trunk-recorder-plugin` 0.7 is on crates.io and a TRP release speaks it)

What TRP's PR #83 (open on 2026-10-25) and the merged PR #82 add, as they stand today; re-read the SDK's docs when this task starts, since the shape may change before publication.

**Files:**
- Modify: `Cargo.toml` (`trunk-recorder-plugin = "0.7"`), `src/plugin.rs`, `src/config.rs`, `src/ingest.rs`, `src/store/units.rs`, `src/store/talkgroups.rs`, `README.md`, `CHANGELOG.md` (`## [0.2.0]`)
- Test: `src/plugin.rs`, `src/ingest.rs`

**Interfaces:**
- Produces: manifest `page: Some(Page { title: "History" })` so the plugin is listed under Plugins in TRP's navigation; `live_settings: true` with `Plugin::settings(&mut self, setup) -> Result<(), String>` applying retention, token, backfill at once and rebinding the server when `bind`/`port` changed (an `Err` names the port as in Task 19).
- Produces: `x-actions` on `Config`: `backfill` ("Backfill now": runs Task 18 and replies with the report), `check` ("Check store": `PRAGMA quick_check`, row counts and file size in the reply); `Plugin::action(&mut self, a: Action, reply: Reply)` answers from the maintenance thread.
- Produces: names from live events: `CallInfo.sources[].tag` → `touch_unit(tag)` at `call.start`/`call.end`; `UnitEvent.tag` and `UnitEvent.alpha_tag` → `touch_unit` and `touch_talkgroup` tags at `unit` events (today both wait for `call.concluded`).
- Produces: `CallRecord.tones` / `tone_patterns` / `signals` (PR #82) stay in `metadata_json`; not subscribed to `tone_pattern` (no tr-engine model for it — a spec question for phase 2).
- Produces: README drops the "Open from the Website link" workaround in favour of the plugin page, and the upstream ask (a) is re-filed as "render the manifest homepage as a link on the plugin page".

- [ ] **Step 1: Write the failing tests:** `settings_change_without_a_restart` (retention 0 → 30 applies; port change rebinds and `/health` answers on the new port), `a_taken_port_in_new_settings_is_an_error_and_the_old_ones_stay`, `backfill_action_replies_with_a_report`, `names_on_live_events_fill_units_and_talkgroups` (a `unit` event with `tag "Engine 1"`, `alpha_tag "Fire Disp"` → `units.alpha_tag`, `talkgroups.alpha_tag` set before any call concludes).
- [ ] **Step 2–4: fail, implement, `cargo test` PASS.** Also `--describe` through `check_manifest.py` (it must learn the new manifest fields first, or they must be ignored by it).
- [ ] **Step 5: Commit** `sdk 0.7: plugin page, live settings, actions, names from live events`.

---

## Self-review notes

- **Spec coverage:** D1–D6 → Tasks 1, 3, 10/15 (D5: server-only paths 501), 6/9 (D4 kept via transmissions; no pairs table), 2 (D6). C1 → 19; C2 → 1/10/19; C3 → 3; C4 → 3–7; C5 → 9; C6 → 18; C7 → 10–16; C8 → 17/19; C9 → 1; C10 → 20; C11 → 20. Design §6 items each have a home: SQL/indexes (3–7), conformance approach (10's `assert_schema` and the served-or-501 test), identity before `status` (4), ring size/replay (8), audio ranges/types (14), concluded-without-start (5/9), decimation (7), logging/health card (19).
- **Ordering:** Task 14 defines `types::Call`, which Task 9's SSE payloads and Tasks 12/13's `/calls` sub-routes need; Task 9 ships a minimal payload and Tasks 12/13 leave those routes to Task 14, as their Interfaces say. Task 13 likewise upgrades the `unit_event` payload; Task 15 the `recorder_update`/`rate_update` payloads.
- **Not in phase 1, by design:** `audio` topic, `unit_pairs`, talkgroup directory import, trunking/console messages, transcription, call groups (`deduplicate` accepted and ignored), the other `/stats/*` and `/analytics/*`, admin and auth-management paths (all 501).
