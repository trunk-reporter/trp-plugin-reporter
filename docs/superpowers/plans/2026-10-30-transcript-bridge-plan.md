# Transcript Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** trunk-reporter serves the transcribe plugin's transcripts on tr-engine's routes and pushes each new one onto its event stream as it lands.

**Architecture:** One module, `src/transcripts/`, owns everything that knows the transcribe plugin: the Transcript shape, an HTTP client with bounded timeouts, an SSE frame parser, and a relay task that turns the plugin's event stream into bus events. The API layer gets one adapter (`src/api/transcriptions.rs`) from that shape to the OpenAPI `Transcription`, `BatchTranscriptionResponse` and `Call` fields the pages read; call-list routes fill `has_transcription` / `transcription_text` with one batch request per response. Nothing is stored.

**Tech Stack:** Rust 1.85, axum 0.8, tokio, rusqlite 0.41, serde; new: `reqwest` 0.12 (`default-features = false`, features `["json", "stream", "rustls-tls"]`), `futures-util` (already a dependency) for the byte stream.

**Spec:** `docs/superpowers/specs/2026-10-30-transcript-bridge-design.md` (read it first; it carries the Transcript shape, the plugin's routes, and every decision below).

## Global Constraints

- `rust-version = "1.85"`; `trunk-recorder-plugin = "0.5"` stays.
- The transcribe plugin is not changed; its routes are taken as the spec lists them (`/api/v1/transcripts/{path}`, `/api/v1/transcripts?paths=`, `/api/v1/events`, `/api/v1/health`), 200 paths per batch request at most.
- No transcript is stored or cached in trunk-reporter; every answer is read through.
- A plugin that is unreachable costs a rate-limited log line and "no transcript" answers; never a 5xx and never a request that outlives the timeouts: 3 s connect, 10 s total, except the event stream (connect timeout only, 45 s without a frame counts as dead).
- A transcript counts as present only when `status == "ok"` and `text` is non-empty (`is_ok`).
- Join key: the call path. Normalise `\` to `/` on both sides (store lookups and paths sent to the plugin).
- Pages are untouched. Every response must validate against `openapi/openapi.yaml` (`assert_schema` in the tests), so the adapter emits the OpenAPI `Transcription` with its required `id`, `call_id`, `text`, `source` (`id` = the call id, `source` = `"auto"`).
- Commits: subject in plain English, blank line, then exactly:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01JXUSP15Bo9JgvCdvA3xtZW
  ```
  (`git commit -F -` with a heredoc.)
- Run `cargo test` (the whole crate) before each commit; `cargo fmt` and `cargo clippy --all-targets` clean.

## Review Focus

1. The plugin is up but slow (a request that hangs): a call list must still answer within the client's 10 s and without the transcript fields, not 500 and not after a minute. Pinned in Task 2 (`a_hanging_plugin_is_given_up_within_the_timeout`).
2. A path with Windows backslashes (a backfilled call): its transcript is still found by `/calls/{id}/transcription` and in the batch. Pinned in Task 3 (`a_backfilled_path_with_backslashes_still_joins`).
3. An event frame for a call the store does not have yet (the transcript beat `call_concluded`): retried, then published once the call exists; dropped quietly when it never does. Pinned in Task 6 (`a_frame_for_a_call_not_yet_stored_is_retried_then_published`).
4. The transcribe plugin restarts (the stream closes): the relay reconnects with `Last-Event-ID`, the replayed frame is published once, and a reconnect storm does not happen (backoff). Pinned in Task 6 (`a_closed_stream_reconnects_with_last_event_id_and_publishes_a_replay_once`).
5. A `skipped` or `failed` transcript, and an `ok` one with empty text: no transcript anywhere (404, absent from the batch, `has_transcription` false, no event). Pinned in Tasks 3 and 6.

---

## File Structure

- Create `src/transcripts/mod.rs`: the shape (`Transcript`, `Utterance`, `PluginHealth`), `is_ok`, `normalize_path`, `Throttle` (rate-limited logging), `pub use` of the submodules' public items.
- Create `src/transcripts/client.rs`: `TranscribeClient`.
- Create `src/transcripts/sse.rs`: `SseParser`, `Frame`.
- Create `src/transcripts/relay.rs`: `RelayStatus`, `run_relay`, `spawn_relay`.
- Create `src/api/transcriptions.rs`: the adapter and the two routes; `fill_transcripts`.
- Modify `src/api/calls.rs`: remove the two stubs; fill transcripts in `list`, `one`, `talkgroup_calls`, `unit_calls`.
- Modify `src/api/types.rs`: `Call.transcription_text`, `Call.path` (not serialized); `Features` unchanged (set per state).
- Modify `src/api/mod.rs`: `AppState.transcripts: Option<Arc<TranscribeClient>>`, `AppState.relay: Option<RelayStatus>`; routes moved to `transcriptions::`.
- Modify `src/api/health.rs`: `transcribe` block.
- Modify `src/bus.rs`: `EventKind::Transcription`.
- Modify `src/config.rs`: `transcribe_url`.
- Modify `src/store/calls.rs`: `call_by_path`.
- Modify `src/plugin.rs`: build the client, set features, spawn and stop the relay, status line and metric.
- Modify `src/main.rs` (the `serve` subcommand, where it builds `AppState`): the same client wiring from its config.
- Modify `src/lib.rs`: `pub mod transcripts;`.
- Create `tests/common/fake_transcribe.rs` (+ `pub mod fake_transcribe;` in `tests/common/mod.rs`): a scripted transcribe plugin.
- Create `tests/api_transcriptions.rs`, `tests/relay.rs`.
- Modify `openapi/openapi.yaml`, `README.md`, `CHANGELOG.md`, `Cargo.toml` (version 0.3.0).

---

### Task 1: The shape, the path join, and the client

**Files:**
- Create: `src/transcripts/mod.rs`, `src/transcripts/client.rs`
- Modify: `src/lib.rs`, `Cargo.toml`
- Test: `tests/common/fake_transcribe.rs`, `tests/common/mod.rs`, unit tests in the two new files

**Interfaces:**
- Produces (`src/transcripts/mod.rs`):
  - `#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)] #[serde(rename_all = "camelCase")] pub struct Transcript { pub path: String, pub system: String, pub talkgroup: u32, pub start_ms: i64, pub length_ms: i64, pub status: String, pub reason: Option<String>, pub text: Option<String>, pub language: Option<String>, pub model: String, pub transcribed_ms: i64, #[serde(default)] pub segments: Vec<Utterance> }` (unknown fields ignored: no `deny_unknown_fields`).
  - `pub struct Utterance { pub unit: Option<i64>, pub start: f64, pub end: f64, pub text: String }`.
  - `#[serde(rename_all = "camelCase")] pub struct PluginHealth { pub ok: bool, pub queue: u64, pub in_flight: u64, pub transcribed24h: u64 }` (the engine block ignored).
  - `pub fn is_ok(t: &Transcript) -> bool`: `status == "ok"` and `text` is `Some` and non-blank.
  - `pub fn normalize_path(p: &str) -> String`: `\` → `/`.
  - `pub struct Throttle` with `pub fn new(every: Duration) -> Throttle` and `pub fn allow(&self, key: &'static str) -> bool` (true at most once per `every` per key; `Mutex<HashMap<&'static str, Instant>>` inside, `Clone` via `Arc`).
- Produces (`src/transcripts/client.rs`):
  - `#[derive(Clone)] pub struct TranscribeClient` with `pub fn new(base_url: &str) -> Result<TranscribeClient, String>` (trims whitespace and trailing `/`; builds two `reqwest::Client`s: `http` with `connect_timeout(3 s)` and `timeout(10 s)`, `stream` with `connect_timeout(3 s)` only), `pub fn base(&self) -> &str`,
  - `pub async fn one(&self, path: &str) -> Result<Option<Transcript>, String>` (GET `{base}/api/v1/transcripts/{normalize_path(path)}` with each segment percent-encoded as reqwest does for a path; 404 → `Ok(None)`; other non-2xx → `Err`),
  - `pub async fn batch(&self, paths: &[String]) -> Result<Vec<Transcript>, String>` (chunks of 200, `?paths=` comma-joined normalised paths, results concatenated; an empty input makes no request),
  - `pub async fn health(&self) -> Result<PluginHealth, String>`,
  - `pub async fn events(&self, last_event_id: Option<i64>) -> Result<reqwest::Response, String>` (GET `{base}/api/v1/events`, `Accept: text/event-stream`, `Last-Event-ID` header when given; the caller reads the body stream).
- Produces (`tests/common/fake_transcribe.rs`): `pub struct FakeTranscribe { pub url: String, pub events: tokio::sync::mpsc::UnboundedSender<FakeEvent>, pub last_event_ids: Arc<Mutex<Vec<Option<String>>>>, pub batch_requests: Arc<Mutex<Vec<Vec<String>>>> }`, `pub enum FakeEvent { Frame(Value) /* a Transcript JSON */, Close }`, `pub async fn start(transcripts: Vec<Value>) -> FakeTranscribe` (axum on `127.0.0.1:0`: `/api/v1/transcripts/{*path}` 200/404 from the list; `/api/v1/transcripts?paths=` the ones present, in the order asked, recording each request's paths; `/api/v1/health` `{ "ok": true, "engine": {}, "queue": 0, "inFlight": 0, "transcribed24h": 1 }`; `/api/v1/events` records the `Last-Event-ID` header, then streams `id: <transcribedMs>\nevent: transcript\ndata: <json>\n\n` per `Frame` and ends the body on `Close`; `event: ping` every 100 ms), plus `pub fn hang(&self)` making every route sleep 30 s (for the timeout test).

- [ ] **Step 1: Add the dependency** — `reqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls"] }` under `[dependencies]`; `cargo build` passes.
- [ ] **Step 2: Write the failing unit tests** in `src/transcripts/mod.rs`:
  - `the_plugins_transcript_json_round_trips`: the spec's JSON example deserialises to a `Transcript` with `segments[0] == Utterance { unit: Some(8342327), start: 0.64, end: 1.28, text: "234" }` and serialises back with camelCase keys; an object with an extra `"shape": 1` field still parses.
  - `only_ok_with_text_counts`: `is_ok` is false for `status: "skipped"`, for `status: "ok"` with `text: null`, with `text: "  "`; true for the example.
  - `backslashes_become_slashes`: `normalize_path("marcs\\2026\\10\\30\\x") == "marcs/2026/10/30/x"`.
  - `a_throttle_allows_once_per_interval_per_key`.
- [ ] **Step 3: Run them** — `cargo test transcripts::` fails to compile (no module).
- [ ] **Step 4: Implement `src/transcripts/mod.rs`** and `pub mod transcripts;` in `src/lib.rs`; tests pass.
- [ ] **Step 5: Write the fake plugin** (`tests/common/fake_transcribe.rs`) as specified, and the failing client tests in `tests/api_transcriptions.rs` (first tests of that file; `mod common;`):
  - `one_reads_a_transcript_and_a_missing_path_is_none`.
  - `batch_sends_at_most_200_paths_per_request_and_keeps_order`: 450 scripted paths → 3 requests of 200/200/50; the result order follows the input.
  - `a_down_plugin_is_an_error_not_a_panic`: a client at `http://127.0.0.1:1` (nothing listens) → `one` is `Err` within 5 s.
- [ ] **Step 6: Implement `src/transcripts/client.rs`**; tests pass.
- [ ] **Step 7: Commit** — "Transcripts: the shape, the path join and a client for the transcribe plugin".

### Task 2: The setting, the state, and health

**Files:**
- Modify: `src/config.rs`, `src/api/mod.rs:76-131`, `src/api/health.rs`, `src/plugin.rs:162-256`, `src/main.rs` (serve mode's `AppState`)
- Test: `src/config.rs` tests, `tests/api_health.rs`, `tests/api_transcriptions.rs`

**Interfaces:**
- Consumes: `TranscribeClient::new`, `Throttle` (Task 1).
- Produces:
  - `Config.transcribe_url: String` (default `"http://127.0.0.1:8767"`), doc comment: `/// Transcribe plugin URL` then `/// Where the transcribe plugin serves its transcripts. Leave empty when it isn't installed.`
  - `AppState.transcripts: Option<Arc<TranscribeClient>>` (None: bridge off), `AppState.relay: Option<RelayStatus>` (Task 6 fills it; `None` until then — add the field in Task 6, not here), `AppState.throttle: Throttle` (one per minute).
  - `impl AppState { pub fn with_transcribe(self, url: &str) -> Result<AppState, String> }`: empty/blank → unchanged; else sets `transcripts` and `features.transcriptions = true`.
  - `HealthResponse.transcribe: Option<TranscribeHealth>` (serialized only when `Some`), `pub struct TranscribeHealth { pub url: String, pub connected: bool, pub last_event_ms: Option<i64> }`: `connected`/`last_event_ms` come from `state.relay` when present (Task 6), else `false`/`None` for now.
- [ ] **Step 1: Failing tests**
  - `src/config.rs`: `the_transcribe_url_defaults_to_the_local_plugin` (`Config::default().transcribe_url == "http://127.0.0.1:8767"`; the schema has the field with its label).
  - `tests/api_health.rs`: `health_says_transcriptions_when_a_transcribe_url_is_set`: `state_with(None).0.with_transcribe("http://127.0.0.1:8767")` → `/health` has `features.transcriptions == true` and `transcribe.url == "http://127.0.0.1:8767"`, `connected == false`; without it `features.transcriptions == false` and no `transcribe` key.
  - `tests/api_transcriptions.rs`: `a_hanging_plugin_is_given_up_within_the_timeout`: fake with `hang()`, a seeded call, `GET /calls/{id}` answers 200 within 12 s with `has_transcription == false` (Task 4 fills; here the assertion is just 200 within 12 s and no `transcription_text` — write it now, it passes trivially until Task 4 makes it meaningful; keep it).
- [ ] **Step 2: Run** — compile failures.
- [ ] **Step 3: Implement**: the config field; `AppState` fields and `with_transcribe`; `plugin.rs` applies `with_transcribe(&config.transcribe_url)` when building the state (and logs `host.info("reading transcripts from {url}")`); `main.rs` serve mode the same from its config; health block.
- [ ] **Step 4: Tests pass**; `cargo test` whole crate.
- [ ] **Step 5: Commit** — "Transcripts: a Transcribe plugin URL setting, the client on the state, and health".

### Task 3: The adapter and the two routes

**Files:**
- Create: `src/api/transcriptions.rs`
- Modify: `src/api/calls.rs:96-121` (delete the stubs), `src/api/mod.rs:249-250` (route to the new module), `src/store/calls.rs` (`call_by_path`), `src/api/types.rs` (`Call.path`)
- Test: `tests/api_transcriptions.rs`

**Interfaces:**
- Consumes: `TranscribeClient::one/batch`, `is_ok`, `normalize_path` (Task 1); `AppState.transcripts` (Task 2); `call_row`, `CallRow`, `rfc3339` (existing).
- Produces:
  - `src/store/calls.rs`: `pub fn call_by_path(conn: &Connection, path: &str) -> rusqlite::Result<Option<CallRow>>`: tries `normalize_path(path)` then the same with `/` → `\` (a backfilled Windows row).
  - `src/api/types.rs`: `Call.path: Option<String>` with `#[serde(skip)]`, set in `Call::from_row` from `row.path`.
  - `src/api/transcriptions.rs`:
    - `pub fn to_engine(call_id: i64, t: &Transcript) -> serde_json::Value`: exactly
      ```json
      { "id": <call_id>, "call_id": <call_id>, "text": t.text, "source": "auto", "is_primary": true,
        "language": t.language (omit when null), "model": t.model, "provider": "transcribe",
        "word_count": <words in text split on whitespace>, "created_at": rfc3339(t.transcribed_ms),
        "words": { "words": [], "segments": [ { "src": unit (0 when null), "start", "end", "text" } … ] } }
      ```
    - `pub fn to_batch_entry(call_id: i64, t: &Transcript) -> serde_json::Value`: `{ "call_id", "text", "segments": [same segment objects] }`.
    - `pub async fn transcription(State, Path<String>) -> Result<Json<Value>, ApiError>`: 404 `"call N has no transcription"` when the bridge is off, the call has no `path`, the plugin has none or not `is_ok`, or the client errs (then `state.log` once per minute via `throttle.allow("transcription")`).
    - `pub async fn transcriptions_batch(State, QueryParams) -> Result<Json<Value>, ApiError>`: validation as the old stub (1–500 integer ids, 400 otherwise); the ids' rows via one `blocking` read (unknown ids skipped); `client.batch(paths)`; `{ "transcriptions": [entries in the order asked, ok ones only] }`; bridge off or client error → `{ "transcriptions": [] }`.
- [ ] **Step 1: Failing tests** (`tests/api_transcriptions.rs`, fake plugin seeded with the spec example under the seeded call's path, a `skipped` one under another call's path, and an `ok` with `text: ""` under a third):
  - `a_transcription_is_served_in_the_engines_shape`: 200; `assert_schema(&v, "Transcription")`; `v["call_id"] == id`, `v["source"] == "auto"`, `v["words"]["segments"][0]["src"] == 8342327`, `v["words"]["words"] == []`, `v["created_at"]` is RFC 3339.
  - `skipped_empty_and_unknown_read_as_no_transcription`: 404 for the skipped call, the empty-text call, a call whose path the fake lacks, and a call with no path; 404 with the bridge off (`state.transcripts = None`); 404 (not 500) with the fake down.
  - `the_batch_keeps_the_order_asked_and_drops_the_rest`: `call_ids` = empty-text, ok, unknown, skipped → `transcriptions` has one entry, the ok call; `assert_schema(&v, "BatchTranscriptionResponse")`; `call_ids=abc` → 400; 501 ids → 400.
  - `a_backfilled_path_with_backslashes_still_joins`: a call stored with path `marcs\2026\10\30\x-1_1-call_1` and the fake holding `marcs/2026/10/30/x-1_1-call_1` → 200.
- [ ] **Step 2: Run** — fail (routes answer 404 / empty today).
- [ ] **Step 3: Implement**; remove the stubs from `calls.rs`; route `/calls/{id}/transcription` and `/transcriptions/batch` to `transcriptions::`.
- [ ] **Step 4: Tests pass**; whole crate green (the existing 404 tests in `tests/api_calls.rs` for transcription, if any, must still pass with the bridge off).
- [ ] **Step 5: Commit** — "Transcripts: /calls/{id}/transcription and /transcriptions/batch answer from the transcribe plugin".

### Task 4: Call lists say which calls have a transcript

**Files:**
- Modify: `src/api/transcriptions.rs`, `src/api/types.rs:667-720`, `src/api/calls.rs:57-70, 91-94, 172-200`
- Test: `tests/api_transcriptions.rs`

**Interfaces:**
- Consumes: `Call.path` (Task 3), `TranscribeClient::batch`.
- Produces:
  - `Call.transcription_text: Option<String>` with `#[serde(skip_serializing_if = "Option::is_none")]`, `has_transcription` as before (false by default).
  - `pub async fn fill_transcripts(state: &AppState, calls: &mut [Call])`: no-op when the bridge is off or no call has a path; one `batch` over the distinct normalised paths (the client chunks by 200); for each `is_ok` transcript, the matching calls get `has_transcription = true` and `transcription_text = text`; a client error leaves everything as it was and logs through `throttle.allow("batch")`.
- [ ] **Step 1: Failing tests**:
  - `a_call_list_carries_has_transcription_and_the_text`: `GET /calls` with two seeded calls, one transcribed → that one `has_transcription == true` and `transcription_text == "234?"`, the other `false` with no `transcription_text` key; `assert_schema` of each call against `Call`; the fake saw one batch request with both paths.
  - `one_call_and_talkgroup_and_unit_lists_are_filled_too`: `GET /calls/{id}`, `GET /talkgroups/{sys}/{tgid}/calls` (the route `talkgroup_calls` serves; use its real path from `src/api/mod.rs`), `GET /units/{id}/calls` → the transcribed call reads `has_transcription == true`.
  - `a_hanging_plugin_is_given_up_within_the_timeout` (Task 2) now asserts `has_transcription == false` and answers within 12 s.
- [ ] **Step 2: Run** — fail.
- [ ] **Step 3: Implement**: in `list`, `one`, `talkgroup_calls`, `unit_calls`, after the `blocking` read: `fill_transcripts(&state, &mut body.calls).await` (or the single call). `active` is left alone (calls on the air have no transcript yet).
- [ ] **Step 4: Tests pass**; crate green.
- [ ] **Step 5: Commit** — "Transcripts: call lists say which calls have one, with the text for the snippet".

### Task 5: The SSE parser

**Files:**
- Create: `src/transcripts/sse.rs`
- Test: unit tests in the file

**Interfaces:**
- Produces: `#[derive(Debug, PartialEq, Clone)] pub struct Frame { pub id: Option<String>, pub event: Option<String>, pub data: String }`; `#[derive(Default)] pub struct SseParser { … }` with `pub fn feed(&mut self, bytes: &[u8]) -> Vec<Frame>` (frames completed by this input; a frame ends at a blank line; `data:` lines join with `\n`; a leading single space after the colon is stripped; `:` comment lines ignored; `\r\n` accepted; partial lines kept across calls; a frame with no `data` is not emitted).
- [ ] **Step 1: Failing tests**: `a_frame_split_across_chunks_is_one_frame`; `multi_line_data_joins_with_newlines`; `pings_and_comments_are_separate_frames_or_ignored` (`event: ping\ndata: \n\n` → a frame with event `ping` and empty data; `: keepalive\n\n` → nothing); `crlf_line_endings_work`; `a_frame_without_data_is_not_emitted`.
- [ ] **Step 2: Run** — fail. **Step 3: Implement.** **Step 4: Pass.**
- [ ] **Step 5: Commit** — "Transcripts: a parser for the plugin's event stream".

### Task 6: The relay

**Files:**
- Create: `src/transcripts/relay.rs`
- Modify: `src/bus.rs:14-33`, `src/api/mod.rs` (`AppState.relay`), `src/api/health.rs` (connected / last_event_ms from the status), `src/plugin.rs` (spawn in `start`, stop in `shutdown`, status line, metric), `src/main.rs` (serve mode spawns it too)
- Test: `tests/relay.rs`, `tests/api_sse.rs` (one test), `src/plugin.rs` tests (status line)

**Interfaces:**
- Consumes: `TranscribeClient::events`, `SseParser`, `Transcript`, `is_ok` (Tasks 1, 5); `call_by_path` (Task 3); `Bus::publish(NewEvent)`, `rfc3339`.
- Produces:
  - `src/bus.rs`: `EventKind::Transcription` with `as_str() == "transcription"`.
  - `#[derive(Clone, Default)] pub struct RelayStatus(Arc<Mutex<RelayInner>>)` with `pub fn connected(&self) -> bool`, `pub fn last_event_ms(&self) -> Option<i64>`, `pub fn relayed(&self) -> u64`.
  - `pub async fn run_relay(client: Arc<TranscribeClient>, store: Store, bus: Bus, status: RelayStatus, log: Log, mut stop: tokio::sync::watch::Receiver<bool>)`: the loop of the spec: connect with `Last-Event-ID` = last `transcribedMs` seen (none on the first connection); read the body as a byte stream through `SseParser`, each chunk awaited under `tokio::time::timeout(45 s)` (a timeout = dead stream); `event == Some("transcript")` → parse `Transcript`; skip unless `is_ok`; skip when `(path, transcribed_ms)` was already published (keep the last 256 pairs); look up `call_by_path` on a blocking thread; not found → retry after 2 s, then 10 s, then drop with `log("transcript for {path}: no such call, dropped")` at most once a minute; found → `bus.publish(NewEvent { kind: Transcription, sub_type: None, system_id: Some(row.system_id.0), site_id: row.site_id.map(|s| s.0), tgid: Some(row.tgid), unit_id: None, emergency: row.emergency, data: json!({ "call_id": row.id.0, "system_id": …, "tgid": …, "text": t.text, "start_time": rfc3339(row.start_ms) }).to_string() })`, bump `relayed`, set `last_event_ms`; on error or end: `connected = false`, sleep the backoff (1 s, doubling to 30 s, reset to 1 s after any frame), log "transcribe plugin not reachable at {base}: {e}" only when the state changes from connected/first-attempt to failing, and "transcribe plugin event stream connected" on each (re)connect; `stop` changing to `true` ends the loop at the next await.
  - `pub fn spawn_relay(client: Arc<TranscribeClient>, store: Store, bus: Bus, log: Log) -> (RelayStatus, tokio::sync::watch::Sender<bool>, std::thread::JoinHandle<()>)`: a thread named `transcripts` running a current-thread tokio runtime around `run_relay`.
  - `plugin.rs`: `Reporter.relay: Option<(watch::Sender<bool>, JoinHandle<()>)>`; `start` spawns it when the bridge is on (bus and store exist even with `port == 0`), sets `state.relay = Some(status.clone())`; `shutdown` sends `true` and joins (bounded by the grace: don't wait past the deadline); `status_line` gains ` · transcripts: live` / ` · transcripts: unreachable` when a `RelayStatus` is given (signature `status_line(serving: Option<&str>, calls: i64, transcripts: Option<bool>)`), and `metrics` adds `transcripts_relayed` when given.
  - `main.rs` serve mode: spawn the same when its config has a URL.
- [ ] **Step 1: Failing tests** (`tests/relay.rs`: a store with `seed`, a `Bus`, the fake plugin, `run_relay` on a tokio task with a `watch` stop, a `bus.subscribe(Filter::default())` receiver; each test ends by sending `stop = true`):
  - `an_ok_frame_becomes_a_transcription_event`: a `Frame` for the seeded call's path → within 2 s an `Event` with `kind == EventKind::Transcription`, `system_id`, `tgid`, and `data` parsing to `call_id`, `text == "234?"`, `start_time`; `status.relayed() == 1`, `status.connected()`, `status.last_event_ms() == Some(transcribedMs)`.
  - `skipped_and_empty_frames_make_no_event`: two such frames then an ok one → exactly one event.
  - `a_frame_for_a_call_not_yet_stored_is_retried_then_published`: a frame for a path not in the store; after 1 s `call_started` + the path stored for it; an event arrives within 12 s; a frame for a path never stored makes no event within 13 s.
  - `a_closed_stream_reconnects_with_last_event_id_and_publishes_a_replay_once`: one frame, then `Close`; wait for the reconnect (the fake's `last_event_ids` gains `Some("<transcribedMs>")` within 5 s); send the same frame again (a replay) then a new one → exactly one more event.
  - `tests/api_sse.rs`: `transcription_events_reach_the_stream_and_its_type_filter`: `bus.publish` a Transcription event; `/events/stream?types=transcription` carries `event: transcription` with the data; `?types=call_start` does not.
  - `src/plugin.rs`: `the_status_line_mentions_transcripts_when_bridged`.
- [ ] **Step 2: Run** — fail. **Step 3: Implement.** **Step 4: Pass**; whole crate green; `cargo clippy --all-targets` clean.
- [ ] **Step 5: Commit** — "Transcripts: the plugin's event stream relayed as transcription events".

### Task 7: Spec, docs, version

**Files:**
- Modify: `openapi/openapi.yaml:3373-3384` (event table), `README.md`, `CHANGELOG.md`, `Cargo.toml` (`version = "0.3.0"`), `src/api/mod.rs` (`OUT_OF_SCOPE` unchanged: the other transcription routes stay 501; confirm the `every_spec_path_is_served_or_declared_out_of_scope` test still passes)
- Test: existing spec test; `cargo test`

- [ ] **Step 1:** Add to the event table: `| \`transcription\` | A call's transcript landed (from the transcribe plugin) | \`{ call_id, system_id, tgid, text, start_time }\` |`.
- [ ] **Step 2:** README: a "Transcripts" section (what the setting is, what shows where, that the transcribe plugin keeps the store, what stays out: search, verify, word timing). CHANGELOG: an `## 0.3.0` entry with the four user-visible points (setting; transcription routes and `has_transcription`/`transcription_text`; the `transcription` event; health's `transcribe`).
- [ ] **Step 3:** `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets`; `cargo build --release`.
- [ ] **Step 4: Commit** — "Version 0.3.0: transcripts from the transcribe plugin".

---

The live run (building the release, copying it to `D:\trunk-pro-airspy-fix\trunk-reporter.exe`, restarting the recorder, watching a transcript land on the IRC page) is the controller's, after the final review.
