# Trunk Reporter Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the plugin everything the new web app depends on: a capability flag, CORS for third-party front ends, the activity, selection and baseline endpoints behind the shared timeline and the Live screen, server-side preferences, and live audio over WebSocket.

**Architecture:** Every addition is additive to the OpenAPI contract and lands as one store query plus one axum handler, tested the way phase 1 tests are (handler tests through the router with `assert_schema`, store tests on an in-memory database). Live audio is a new `audio` module: the plugin's `audio` topic handler publishes PCM frames on a tokio broadcast channel; a WebSocket handler frames them for clients exactly as tr-engine does. Nothing changes in ingest, the store schema (one new table via `ensure.sql`), or the pages.

**Tech Stack:** Rust 2024, `trunk-recorder-plugin` 0.5, axum 0.8 (`ws` feature added), tokio, rusqlite (bundled), serde, schemars 1. Dev: `tokio-tungstenite` for the WebSocket tests. No new native dependencies.

**Spec:** `docs/superpowers/specs/2026-10-25-trunk-reporter-ui-design.md` ("API additions" and the constraints that name them) on top of `docs/superpowers/specs/2026-10-24-tr-engine-rust-plugin-design.md`. The OpenAPI document at `openapi/openapi.yaml` is the contract every response is checked against.

## Global Constraints

- SDK `trunk-recorder-plugin = "0.5"` (crates.io); nothing from the unpublished 0.7.
- `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --locked` and `python .github/check_manifest.py <manifest> --release` stay green after every task.
- Every new or extended endpoint is added to `openapi/openapi.yaml` **in the same task**, additively (new paths, new optional parameters, new optional fields; nothing renamed or removed), with an example, and its handler test calls `assert_schema` against the new schema.
- A new path leaves `OUT_OF_SCOPE` (`src/api/mod.rs`) when it is routed, and its prefix joins `SERVED_PREFIXES`.
- Errors are the spec's `Error` shape through `ApiError`; no SQL text in responses.
- Store writes go through the single writer; reads use the read pool (`blocking`).
- Settings keep `camelCase` names and doc-comment labels (the form is drawn from them).
- Version becomes `0.2.0` in `Cargo.toml` with a `## [0.2.0]` section in `CHANGELOG.md` (Task 11).
- The hub is not in this plan. Endpoints take the filters the spec gives them today (`system_id`, `site_id`); no `recorder` filter yet.

## Review Focus

Inputs the spec implies but no task's tests exercise fully, most likely to bite first:

1. A window that spans months with `interval=10`: the bucket cap must refuse it with a 400 naming the cap, not time out. (Task 3's `too_many_buckets_is_a_400` pins it.)
2. A talkgroup with calls in the window but no `talkgroups` row yet (first seen seconds ago): activity and `talkgroups/active` must still list it with empty names, not drop it. (Task 3 and 4 tests seed such a call.)
3. A client subscribing to no talkgroups on the audio socket must hear everything, as tr-engine's empty filter means "all"; an explicit `unsubscribe` means nothing. (Task 10's filter test.)
4. A slow WebSocket client must not stall the plugin's host thread: the broadcast channel drops for that client and the handler reports lag, the host thread never awaits. (Task 10's `a_slow_client_is_dropped_not_waited_for`.)
5. Preferences bodies that are not objects, or over the size cap, are a 400; a key with a slash is a 404 from the router, not a path traversal. (Task 9's tests.)

---

### Task 1: `features` on `/health`

**Files:**
- Modify: `src/api/health.rs` (`HealthResponse`, `health`)
- Modify: `src/api/mod.rs` (`AppState` gains `features`)
- Modify: `src/server.rs` (`serve_data_dir` sets serve-mode features), `src/plugin.rs` (plugin-mode features)
- Modify: `openapi/openapi.yaml` (`HealthResponse.features`, new `Features` schema)
- Test: `tests/api_health.rs`, `tests/serve_mode.rs`

**Interfaces:**
- Produces: `pub struct Features { pub transcriptions: bool, pub live_audio: bool, pub hub: bool, pub recorders: bool, pub call_groups: bool, pub directory: bool, pub users: bool }` in `src/api/types.rs`, `Serialize + Clone + Copy + Default`, with `pub const PLUGIN: Features` (`live_audio: true, recorders: true`, rest false) and `pub const SERVE: Features` (`recorders: true`, rest false).
- Produces: `AppState.features: Features`.

- [ ] **Step 1: Write the failing tests**

In `tests/api_health.rs`:

```rust
#[tokio::test]
async fn health_says_what_this_server_can_do() {
    let (s, v) = get(&app().0, "/api/v1/health").await;
    assert_eq!(s, 200);
    assert_schema(&v, "HealthResponse");
    let f = &v["features"];
    assert_eq!((f["transcriptions"], f["live_audio"], f["hub"], f["recorders"]), (false, true, false, true));
    assert_eq!((f["call_groups"], f["directory"], f["users"]), (false, false, false));
}
```

In `tests/serve_mode.rs`, in the existing serve-mode test that fetches `/api/v1/health`: assert `v["features"]["live_audio"] == false`.

`tests/common/mod.rs` builds `AppState` with `features: Features::PLUGIN`.

- [ ] **Step 2: Run them, expect failure** — `cargo test --locked --test api_health health_says` fails: no `features` field.

- [ ] **Step 3: Implement** — `Features` in `types.rs`; `AppState.features`; `health` copies `state.features` into `HealthResponse.features`; `serve_data_dir` uses `Features::SERVE`, the plugin `Features::PLUGIN`. In the spec, add `features` (optional, `$ref: Features`) to `HealthResponse` and a `Features` schema of seven booleans with descriptions of one line each.

- [ ] **Step 4: Run** — `cargo test --locked` passes.

- [ ] **Step 5: Commit** — `git commit -m "health: a features object says what this server can do"`.

---

### Task 2: CORS allowlist

**Files:**
- Modify: `src/config.rs` (`cors_origins: Vec<String>`)
- Create: `src/api/cors.rs`
- Modify: `src/api/mod.rs` (`AppState.cors_origins`, layer in `router`)
- Modify: `src/plugin.rs`, `src/server.rs` (pass the setting through; serve mode reads it from `serve.json` like the others)
- Test: `tests/api_cors.rs`

**Interfaces:**
- Produces: `Config.cors_origins: Vec<String>` (camelCase `corsOrigins`; doc comment "Allowed origins" / "Web pages served from these origins may call the API. One origin per line, such as http://192.168.1.20:8081."), default empty.
- Produces: `pub async fn cors(State(state): State<AppState>, req: Request, next: Next) -> Response` middleware in `src/api/cors.rs`.

Behaviour: when the request's `Origin` header exactly matches an entry, add `Access-Control-Allow-Origin: <origin>`, `Vary: Origin`, `Access-Control-Allow-Headers: Authorization, Content-Type`, `Access-Control-Allow-Methods: GET, PUT, POST, OPTIONS`, `Access-Control-Expose-Headers: Content-Length, Content-Range`, `Access-Control-Max-Age: 600`; an `OPTIONS` request with a matching origin is answered `204` with those headers before the token check. A non-matching or absent origin gets no CORS headers and proceeds as today. `*` as an entry allows any origin (echoed, never literal `*`, so credentials still work).

- [ ] **Step 1: Write the failing tests** in `tests/api_cors.rs` (helper: `app_with_cors(&["http://127.0.0.1:8081"])`):

```rust
#[tokio::test]
async fn a_listed_origin_gets_cors_headers_and_a_preflight() { /* GET /api/v1/health with Origin → ACAO == origin, Vary: Origin; OPTIONS → 204 with allow headers, even with a token set and no Authorization */ }
#[tokio::test]
async fn an_unlisted_origin_gets_none() { /* Origin http://evil → no ACAO header; status unchanged */ }
#[tokio::test]
async fn star_allows_any_origin_by_echo() { /* entry "*" → ACAO == the request's origin, not "*" */ }
#[tokio::test]
async fn the_setting_is_in_the_schema() { /* schema_for::<Config>() has corsOrigins as array of string */ }
```

- [ ] **Step 2: Run, expect failure.**
- [ ] **Step 3: Implement** `cors.rs` and wire it as the outermost layer in `router` (before `require_token`).
- [ ] **Step 4: Run** — all green; `check_manifest.py` still ok (the schema grows one array field).
- [ ] **Step 5: Commit** — `git commit -m "api: CORS for listed origins"`.

---

### Task 3: Talkgroup activity, summary and buckets

**Files:**
- Modify: `src/store/stats.rs` (`talkgroup_activity`, `activity_buckets`)
- Modify: `src/api/stats.rs` (`talkgroup_activity` handler), `src/api/types.rs` (`TalkgroupActivity`, `ActivityBucket`, `TalkgroupActivityResponse`)
- Modify: `src/api/mod.rs` (route `/stats/talkgroup-activity`, remove from `OUT_OF_SCOPE`)
- Modify: `openapi/openapi.yaml` (`interval` parameter; `buckets` field on `TalkgroupActivity`)
- Test: `tests/api_stats.rs`, store tests in `src/store/stats.rs`

**Interfaces:**
- Consumes: `time_range(q)` for `after`/`before` (already parses RFC 3339 to ms), `ids(q, &["tgid","tgids"])`, `ids(q, &["system_id","systems"])`, `ids(q, &["site_id","sites"])`.
- Produces: `pub struct ActivityFilter { pub system_ids: Vec<i64>, pub site_ids: Vec<i64>, pub tgids: Vec<u32>, pub after_ms: i64, pub before_ms: i64 }`.
- Produces: `pub fn talkgroup_activity(conn: &Connection, f: &ActivityFilter) -> rusqlite::Result<Vec<TalkgroupActivityRow>>` — one row per `(system_id, tgid)` with calls in the window: `call_count`, `total_duration` (seconds), `emergency_count`, `first_call_ms`, `last_call_ms`, `avg_duration`, plus names via `LEFT JOIN talkgroups` (empty strings when no row). Ordered by `call_count DESC, tgid`.
- Produces: `pub fn activity_buckets(conn: &Connection, f: &ActivityFilter, interval_s: u32) -> rusqlite::Result<Vec<(i64, u32, Vec<ActivityBucketRow>)>>` — per `(system_id, tgid)`, buckets `(bucket_start_ms, calls, seconds)` for non-empty buckets only, `bucket_start_ms = (start_ms / (interval_s*1000)) * (interval_s*1000)`.
- Produces: `GET /api/v1/stats/talkgroup-activity` answering the spec's `TalkgroupActivityResponse`; with `interval` (one of 10, 30, 60, 300, 900, 3600) each item also carries `buckets: [{ time: RFC3339, calls, seconds }]`.

Rules: `after` defaults to 24 h ago, `before` to now (spec). Bucket cap: `((before - after) / interval) * talkgroups_in_window > 10_000` → `400 invalid_parameter` "too many buckets: N, at most 10000; widen interval or narrow the window" (count talkgroups first with a cheap query). Any other `interval` value → 400.

- [ ] **Step 1: Write the failing tests** in `tests/api_stats.rs` using `seed(&store)` plus one extra call on a talkgroup with no `talkgroups` row:

```rust
#[tokio::test]
async fn talkgroup_activity_summarises_the_window() { /* 200; assert_schema TalkgroupActivityResponse; the unnamed talkgroup appears with tg_alpha_tag ""; ordered by call_count desc */ }
#[tokio::test]
async fn talkgroup_activity_buckets_by_interval() { /* interval=60 → each item has buckets; sum of bucket calls == call_count; bucket times are minute-aligned RFC 3339 */ }
#[tokio::test]
async fn too_many_buckets_is_a_400() { /* after=2020-01-01, interval=10 → 400 invalid_parameter mentioning 10000 */ }
#[tokio::test]
async fn a_bad_interval_is_a_400() { /* interval=7 → 400 */ }
```

Store test in `src/store/stats.rs`: `buckets_align_to_the_interval_and_skip_empty_ones`.

- [ ] **Step 2: Run, expect failure** (501 today).
- [ ] **Step 3: Implement** the two store functions (SQL on `calls` with `calls_tg_start`; duration is `COALESCE(end_ms, start_ms) - start_ms`), the handler, the types, the route, the spec: `interval` as an optional integer enum parameter on the existing operation; `buckets` as an optional array of `ActivityBucket { time: date-time, calls: integer, seconds: number }` on `TalkgroupActivity`.
- [ ] **Step 4: Run** — green; `out_of_scope_paths_say_so_with_501` still passes with the path removed from the list.
- [ ] **Step 5: Commit** — `git commit -m "stats: talkgroup activity, as a summary and in buckets"`.

---

### Task 4: `GET /talkgroups/active`

**Files:**
- Modify: `src/store/talkgroups.rs` (`active_in_window`), `src/api/talkgroups.rs` (`active` handler), `src/api/types.rs` (`ActiveTalkgroup`, `ActiveTalkgroupListResponse`)
- Modify: `src/api/mod.rs` (route before `/talkgroups/{id}` so `active` is not taken as an id), `openapi/openapi.yaml` (new path)
- Test: `tests/api_talkgroups.rs`

**Interfaces:**
- Produces: `pub fn active_in_window(conn: &Connection, system_ids: &[i64], after_ms: i64, before_ms: i64, limit: u32) -> rusqlite::Result<Vec<ActiveTalkgroupRow>>` — `(system_id, system_name, tgid, tg_alpha_tag, tg_tag, tg_group, calls, seconds, last_call_ms)`, ordered by `calls DESC, last_call_ms DESC`.
- Produces: `GET /api/v1/talkgroups/active?after&before&system_id&limit` → `{ "talkgroups": [...], "after": RFC3339, "before": RFC3339 }`. `after` defaults to 10 minutes ago, `before` to now, `limit` default 50, max 1000 (clamped as `pagination` does).

- [ ] **Step 1: Write the failing tests:** `active_talkgroups_are_those_with_calls_in_the_window` (ordering, counts, the unnamed talkgroup present, `assert_schema` `ActiveTalkgroupListResponse`), `active_talkgroups_default_window_is_ten_minutes` (a call 20 minutes old is absent; `after` in the body is 10 min before `before`).
- [ ] **Step 2: Run, expect failure** (404 "no endpoint" today, since `/talkgroups/{id}` rejects `active` as an id with 400 — assert the test fails either way).
- [ ] **Step 3: Implement** store query, handler, types, route order, spec path with schemas.
- [ ] **Step 4: Run** — green.
- [ ] **Step 5: Commit** — `git commit -m "talkgroups: the ones active in a window"`.

---

### Task 5: `GET /units/{id}/talkgroups`

**Files:**
- Modify: `src/store/units.rs` (`talkgroups_of_unit`), `src/api/units.rs` (`talkgroups` handler), `src/api/types.rs` (`UnitTalkgroup`, `UnitTalkgroupListResponse`)
- Modify: `src/api/mod.rs` (route), `openapi/openapi.yaml`
- Test: `tests/api_units.rs`

**Interfaces:**
- Consumes: `resolve_unit(conn, id)` for the composite or plain id (ambiguity handling as `/units/{id}`).
- Produces: `pub fn talkgroups_of_unit(conn: &Connection, system_id: SystemId, unit: u32, after_ms: i64, before_ms: i64) -> rusqlite::Result<Vec<UnitTalkgroupRow>>` — from `call_transmissions` joined to `calls` (`call_transmissions_unit` index): `(tgid, tg_alpha_tag, calls, transmissions, seconds, first_ms, last_ms)`, ordered by `transmissions DESC`.
- Produces: `GET /api/v1/units/{id}/talkgroups?after&before` → `{ "unit_id", "system_id", "talkgroups": [...] }`. `after` defaults to 24 h ago.

- [ ] **Step 1: Failing tests:** `a_units_talkgroups_in_a_window_come_from_its_transmissions` (counts per tgid; a transmission outside the window excluded; `assert_schema`), `an_unknown_unit_is_a_404`.
- [ ] **Step 2–5:** run, implement (store, handler, route, spec), run, commit `git commit -m "units: the talkgroups a radio talked on"`.

---

### Task 6: `first_seen` on a talkgroup's units

**Files:**
- Modify: `src/store/talkgroups.rs` (`units_on_talkgroup` returns `first_seen_ms` too), `src/api/talkgroups.rs` (`units` adds the field), `src/api/types.rs`, `openapi/openapi.yaml` (optional `first_seen` date-time on the item schema `/talkgroups/{id}/units` returns)
- Test: `tests/api_talkgroups.rs`

**Interfaces:**
- Changes: `units_on_talkgroup(...) -> rusqlite::Result<(Vec<(UnitRow, i64, i64)>, i64)>` — the tuple gains `first_seen_ms` = `MIN(time_ms)` over `unit_events` for `(system_id, tgid, unit)` (index `unit_events_tg_time`), falling back to the earliest `call_transmissions.time_ms` for that unit on that talkgroup's calls when no event exists.

- [ ] **Step 1: Failing test:** `a_talkgroups_units_say_when_each_was_first_heard_there` — seed two units, one with a `join` event earlier than its first call; assert `first_seen` equals the earlier of the two; `assert_schema` on the response.
- [ ] **Step 2–5:** run, implement, run, commit `git commit -m "talkgroups: first_seen per unit"`.

---

### Task 7: `GET /first-heard`

**Files:**
- Modify: `src/store/talkgroups.rs` (`first_heard_talkgroups`), `src/store/units.rs` (`first_heard_units`), `src/api/stats.rs` (`first_heard` handler), `src/api/types.rs` (`FirstHeardResponse`), `src/api/mod.rs` (route), `openapi/openapi.yaml`
- Test: `tests/api_stats.rs`

**Interfaces:**
- Produces: `GET /api/v1/first-heard?since&system_id&limit` → `{ "since": RFC3339, "talkgroups": [Talkgroup-shaped with first_seen], "units": [Unit-shaped with first_seen] }`, each list newest first, `limit` per list (default 50, max 500). `since` defaults to 24 h ago. Uses `talkgroups.first_seen_ms` and `units.first_seen_ms`.

- [ ] **Step 1: Failing tests:** `first_heard_lists_new_talkgroups_and_units_newest_first`, `first_heard_since_bounds_both_lists`.
- [ ] **Step 2–5:** run, implement, run, commit `git commit -m "first-heard: new talkgroups and radios"`.

---

### Task 8: `GET /stats/baseline`

**Files:**
- Modify: `src/store/stats.rs` (`baseline`), `src/api/stats.rs` (`baseline` handler), `src/api/types.rs` (`BaselineEntry`, `BaselineResponse`), `src/api/mod.rs`, `openapi/openapi.yaml`
- Test: `tests/api_stats.rs`, store test

**Interfaces:**
- Produces: `pub fn baseline(conn: &Connection, system_ids: &[i64], now_ms: i64, window_s: u32, days: u32) -> rusqlite::Result<Vec<BaselineRow>>` — for every `(system_id, tgid)` with calls in `[now - window, now]`: `calls_now`, and over `d = 1..=days` the calls in `[now - d*86400s - window, now - d*86400s]`: `baseline_mean` (mean over days), `baseline_max`, `days_with_data` (days where the count was > 0). One SQL statement using a recursive CTE for the day offsets.
- Produces: `GET /api/v1/stats/baseline?window&days&system_id` → `{ "window_seconds", "days", "now": RFC3339, "talkgroups": [{ system_id, tgid, tg_alpha_tag, calls_now, baseline_mean, baseline_max, days_with_data }] }`. `window` default 600 (60..=86400), `days` default 7 (1..=30).

- [ ] **Step 1: Failing tests:** `baseline_compares_equal_windows_on_prior_days` (seed calls now and exactly one and two days earlier; mean and max as computed; a talkgroup with no history has `days_with_data 0` and `baseline_mean 0`), `baseline_bounds_are_400s`.
- [ ] **Step 2–5:** run, implement, run, commit `git commit -m "stats: baseline for what is busy now"`.

---

### Task 9: Preferences

**Files:**
- Modify: `src/store/ensure.sql` (`CREATE TABLE IF NOT EXISTS preferences (key TEXT PRIMARY KEY, json TEXT NOT NULL, updated_ms INTEGER NOT NULL)`)
- Create: `src/store/preferences.rs` (`get_preference`, `put_preference`)
- Create: `src/api/preferences.rs` (`get`, `put`)
- Modify: `src/api/mod.rs` (routes, `SERVED_PREFIXES`), `openapi/openapi.yaml`
- Test: `tests/api_preferences.rs`

**Interfaces:**
- Produces: `GET /api/v1/preferences/{key}` → `{ "key", "value": <object>, "updated_at": RFC3339 }` or 404 `not_found`; `PUT` with a JSON object body (≤ 256 KiB) stores it and answers the same shape with 200. `key` matches `^[a-z0-9][a-z0-9-]{0,63}$`, else 400. A body that is not a JSON object → 400 `bad_request`. Both need the token when one is set (the existing middleware).
- Produces: `pub fn put_preference(conn, key: &str, json: &str, now_ms: i64)` through the writer (`store.write(...)`), `pub fn get_preference(conn, key) -> Option<(String, i64)>`.

- [ ] **Step 1: Failing tests:** `a_preference_round_trips`, `a_missing_preference_is_a_404`, `a_bad_key_or_body_is_a_400` (key `Colours`, key with `/` → 404 from the router, body `[1]`, body over 256 KiB), `preferences_need_the_token_when_set` (401 without, 200 with).
- [ ] **Step 2–5:** run, implement, run, commit `git commit -m "preferences: a JSON document per key, in the store"`.

---

### Task 10: Live audio over WebSocket

**Files:**
- Modify: `Cargo.toml` (axum `features = ["ws"]`; dev-dependency `tokio-tungstenite = "0.26"`)
- Create: `src/audio.rs` (`AudioBus`, `AudioFrame`, `frame_bytes`)
- Create: `src/api/audio.rs` (`live` WebSocket handler)
- Modify: `src/plugin.rs` (manifest subscribes `topic::AUDIO`; `fn audio(&mut self, chunk: AudioChunk)`; `AppState.audio: Option<AudioBus>`), `src/ingest.rs` (expose `pub fn system_id_of(&self, index: u16, short_name: &str) -> Option<SystemId>` or reuse `system`)
- Modify: `src/api/mod.rs` (route `/audio/live`, out of `OUT_OF_SCOPE`), `src/server.rs` (serve mode: `audio: None`), `openapi/openapi.yaml` (the existing `/audio/live` entry gains a description of the PCM-only frames and the plugin's 404 when not streaming)
- Test: `src/audio.rs` unit tests, `tests/api_audio.rs`

**Interfaces:**
- Produces: `pub struct AudioFrame { pub system_id: i64, pub tgid: u32, pub sample_rate: u16, pub pcm: Arc<[u8]> }` (little-endian 16-bit samples as received from the recorder's chunk).
- Produces: `pub struct AudioBus(tokio::sync::broadcast::Sender<AudioFrame>)` with `pub fn new(capacity: usize) -> AudioBus` (capacity 256), `pub fn publish(&self, f: AudioFrame)` (never blocks; a lagging receiver sees `Lagged`), `pub fn subscribe(&self) -> broadcast::Receiver<AudioFrame>`, `pub fn active_streams(&self) -> usize` (receiver count).
- Produces: `pub fn frame_bytes(f: &AudioFrame, ts_ms: u32, seq: u16) -> Vec<u8>` — the 14-byte big-endian header `system_id u16, tgid u32, ts_ms u32, seq u16, sample_rate u16` then the PCM, exactly as tr-engine writes it.
- Produces: `GET /api/v1/audio/live` upgrading to a WebSocket: the client sends `{"type":"subscribe","tgids":[...],"systems":[...]}` (empty lists match everything) or `{"type":"unsubscribe"}` (matches nothing); the server sends binary frames for matching talkgroups with a per-`(system, tgid)` sequence counter and `ts_ms` since the connection opened, and every 15 s a text `{"type":"keepalive","active_streams":N}`. At most 16 clients; the 17th gets `503 service_unavailable`. When `AppState.audio` is `None` (serve mode) the route answers `404 not_found` "live audio streaming is not enabled". The token comes as `?token=` (already accepted by `require_token`).
- The plugin's `audio` handler converts `AudioChunk { call_id, system, short_name, talkgroup, sample_rate, pcm }` to an `AudioFrame` with the store's `system_id` (via `Ingest`'s system lookup, cached) and publishes. It does no I/O and never awaits.
- `Features::PLUGIN.live_audio` stays `true`; serve mode `false` (Task 1).

- [ ] **Step 1: Write the failing tests**

`src/audio.rs`:

```rust
#[test]
fn the_frame_header_is_fourteen_big_endian_bytes() {
    let f = AudioFrame { system_id: 3, tgid: 9178, sample_rate: 8000, pcm: Arc::from([1u8, 0, 255, 127][..]) };
    let b = frame_bytes(&f, 1234, 7);
    assert_eq!(&b[..14], &[0, 3, 0, 0, 0x23, 0xDA, 0, 0, 0x04, 0xD2, 0, 7, 0x1F, 0x40]);
    assert_eq!(&b[14..], &[1, 0, 255, 127]);
}
```

`tests/api_audio.rs` (serve the router on a port as `tests/api_sse.rs` does; connect with `tokio_tungstenite::connect_async`):

```rust
#[tokio::test]
async fn frames_reach_a_subscribed_client_with_sequence_numbers() { /* subscribe tgids [9178]; publish two frames for 9178 and one for 9179; receive two binary messages, seq 1 and 2, tgid 9178, pcm intact */ }
#[tokio::test]
async fn an_empty_subscription_hears_everything_and_unsubscribe_nothing() { /* {"type":"subscribe"} → all frames; then unsubscribe → a frame published is not received within 300 ms */ }
#[tokio::test]
async fn a_keepalive_names_the_active_streams() { /* with the keepalive interval overridden to 100 ms via AppState.audio_keepalive, a text message {"type":"keepalive","active_streams":1} arrives */ }
#[tokio::test]
async fn a_slow_client_is_dropped_not_waited_for() { /* publish 1000 frames without reading; publish returns immediately; the client later receives a close or a lag gap, and the bus's publish timing stays under 50 ms total */ }
#[tokio::test]
async fn without_a_recorder_live_audio_is_a_404_and_the_token_is_checked() { /* app with audio None → 404 not_found; with a token set and none given → 401 before the upgrade */ }
#[tokio::test]
async fn the_seventeenth_client_is_refused() { /* 16 connections ok; the 17th → 503 */ }
```

`src/plugin.rs` test: `an_audio_chunk_becomes_a_frame_for_the_stores_system_id` using `testing::run` with a `HostMessage::Audio(AudioChunk::new(...))` after a `call.start`, asserting the bus received one frame with the right `system_id` and 8000 Hz.

- [ ] **Step 2: Run, expect failure** (no module; 501 on the route).
- [ ] **Step 3: Implement** `src/audio.rs`, the handler with `axum::extract::ws::WebSocketUpgrade`, a `tokio::select!` loop over the client's control messages, the bus receiver and the keepalive ticker, the client cap as an `Arc<AtomicUsize>` on `AppState`, the plugin wiring, and the spec text.
- [ ] **Step 4: Run** — green; clippy clean (no `unwrap` on the socket).
- [ ] **Step 5: Commit** — `git commit -m "audio: live PCM over WebSocket, as tr-engine frames it"`.

---

### Task 11: Version, changelog, docs

**Files:**
- Modify: `Cargo.toml` (`version = "0.2.0"`), `CHANGELOG.md` (`## [0.2.0]` listing Tasks 1–10 in one line each), `README.md` (CORS setting; live audio; the new endpoints in one short list; preferences), `docs/superpowers/plans/2026-10-24-tr-engine-rust-plugin-plan.md` (Task 21 unchanged; note that phase 2 is this plan)

- [ ] **Step 1:** Edit the files.
- [ ] **Step 2:** `cargo build --release && ./target/release/trunk-reporter.exe --describe > manifest.json && python .github/check_manifest.py manifest.json --release` → `trunk-reporter 0.2.0: ok`.
- [ ] **Step 3:** `cargo test --locked` → all pass.
- [ ] **Step 4: Commit** — `git commit -m "v0.2.0"`.

---

## Self-review notes

- Spec coverage: the eight additions in the UI design's table map to Tasks 1–10 (CORS = 2, activity = 3, active = 4, unit talkgroups = 5, first_seen = 6, first-heard = 7, baseline = 8, preferences = 9, live audio = 10, features = 1).
- Names agree across tasks: `Features::{PLUGIN,SERVE}` (1, 10), `ActivityFilter` (3 only), `AudioBus`/`AudioFrame`/`frame_bytes` (10), `time_range` and `ids` as phase 1 defines them.
- Proportion: no code bodies beyond the frame-header test, which pins the wire format.
