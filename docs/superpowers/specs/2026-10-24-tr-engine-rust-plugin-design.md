# tr-engine as a Trunk Recorder Pro plugin, in Rust — design

Date: 2026-10-24. Status: draft for the trunk-reporter org. Nothing here is built.

## 1. Intent

tr-engine gives a radio system a durable, queryable history: every call, who spoke in it and when,
every unit event, talkgroup and unit directories, live events, and pages to explore all of it.
Today that needs a server stack: Go binary, PostgreSQL 17+, usually an MQTT broker, Docker Compose,
and the know-how to run them. That stack is the main thing stopping people from using it.

Trunk Recorder Pro (TRP) is the opposite shape: one binary, double-click, radios found for you,
plugins installed from a store with one click. Its own history, the "radio registry", is an
in-memory, capped cache persisted as one JSON file per system: counts survive, facts don't, and it
never references the recorded call files that hold the facts.

This design puts tr-engine's core — the durable store, the ingest, the API, the live stream and
the pages — inside a TRP plugin written in Rust, with SQLite as the store. A TRP user installs it
from the plugin store and has the history, with nothing else to run. tr-engine (Go, PostgreSQL)
stays the server product for multi-recorder and public deployments; the two share one API contract.

## 2. Goals, non-goals, anti-goals

Goals

- One-click install from the TRP plugin store; zero required settings; works on Windows, macOS,
  Linux (x86-64 and arm64), the platforms TRP ships for.
- Durable per-call and per-transmission history fed in-process from TRP's plugin events, no broker.
- Serves tr-engine's REST API and SSE event stream (a defined subset of `openapi.yaml`) so
  tr-engine's built-in pages work unmodified, and tr-dashboard can follow later.
- Playback of any recorded call from its page, seeking to a given unit's transmission.
- Backfills from the call files already on disk, so the history isn't empty on first start.
- Scales to a 24/7 county system for years: audio stays on disk; metadata in SQLite (measured on
  TRP: about 1.3 KB of metadata and 106 KB of audio per call; 10,000 calls/day ≈ 5 GB/year of
  metadata).

Non-goals (stay in the Go tr-engine, or come later)

- Transcription, unit-tag suggestion, S3 / tiered storage, user accounts and API keys, multi-recorder
  merging of one P25 network from several instances, Prometheus metrics, debug-report upload,
  the ad-hoc `POST /query` SQL endpoint.
- Replacing TRP's own dashboard pages. (A later upstream change could let TRP's unit and talkgroup
  pages read from the plugin; this design doesn't depend on it.)
- Changing how TRP records, archives or names files.

Anti-goals (would make this a failure even if it worked)

- Any step a user must do by hand beyond "Install" and "turn it on" for the default experience.
- A second, diverging API: every endpoint served must match `openapi.yaml`, so clients written
  for the Go engine work on the plugin and vice versa.
- Storing audio in the database, or copying audio out of TRP's capture folder.
- Unbounded tables fed by per-message firehoses.

## 3. Constraints (from TRP's plugin system, verified against 0.2.9's code and docs)

- A plugin is a separate executable the recorder starts **when recording starts** and stops (with a
  grace period) **when recording stops**. There is no "keep running while idle" today. Events are
  JSON lines on stdin; the plugin answers on stdout (`ready`, `log`, `status`, `metrics`,
  `call.result`). Manifest via `--describe`. Settings come from a JSON Schema the plugin declares;
  TRP draws the form. Data folder: `plugin-data/<id>/`. Per-plugin event queue of 1,024 messages;
  a plugin that falls behind loses events (counted on the dashboard).
- Topics available: `call.start`, `call.end` (`CallInfo`: talkgroup, tag, frequency, TDMA slot,
  analog / encrypted / emergency / duplex / packet / priority, whether recorded and why not, radios
  heard so far), `call.concluded` (`ConcludedCall`: the full Trunk Recorder-format `CallRecord`
  including `srcList` with each unit's time and position into the audio and `freqList`, plus the
  paths of the `.json`, `.wav` and, if requested, `.m4a`), `unit` (`UnitEvent`: kind — register,
  deregister, affiliate, ack, location, data grant, answer request, call alert — unit, talkgroup,
  emergency, time), `audio` (live 16-bit mono 8 kHz PCM chunks, base64), `status` (every few
  seconds, per site: control channel, decode rate, active and recording calls, and the identity the
  control channel stated: NAC, WACN, system ID, RFSS, site).
- Every event carries the system's `short_name` (its identity across runs) and the site name. A
  call heard on several sites of a system arrives once.
- The manifest has no field for a UI, port or routes; the recorder proxies nothing and embeds
  nothing. The plugin's only link from TRP's UI is the manifest `homepage` shown as "Website".
  TRP "interfaces" (static sites it serves on their own ports) talk to TRP's API only.
- `trunk-pro plugin run <exe> <folder>` replays call JSON files from disk into a plugin.
- Plugins subscribing to `call.concluded` must send a `call.result` for every call, else TRP counts
  the call as failed after an hour and may keep files it would otherwise delete.
- The Rust SDK is the `trunk-recorder-plugin` crate (0.5.x for TRP 0.2.x), with a template repo
  carrying tests, CI and a release workflow that builds per platform and publishes to the store.

## 4. Decisions made

Decided in conversation (the owner's):

- D1. Written in Rust, on the `trunk-recorder-plugin` SDK, shipped as a plugin in the TRP store.
- D2. SQLite is the store; PostgreSQL is not needed for a single recorder.
- D3. The plugin serves tr-engine's API contract; the Go engine remains for server deployments.
- D4. Partners (who a unit talks with) keep tr-engine's/TRP's definition: consecutive talkers in a
  call, plus caller–callee of private calls. No attempt at a smarter social graph.
- D5. Scope is one recorder's data. Server-only features (section 2) are out.
- D6. **The API contract is extended first, additively, in the Go engine's `openapi.yaml`**, so the
  plugin serves data TRP produces that the contract couldn't express. Phase 1 is gated on that
  spec change landing (it needs no Go code to accept the new optional fields). The additions:
  - `Call`: `recorded` (boolean) and `not_recorded_reason` (string, null when recorded);
    `decrypted`, `algid`, `key_id`, `priority`, `duplex`, `packet` as optional fields;
  - `SystemType`: `dmr`, `nxdn`, `edacs` (trunked), alongside the existing values;
  - call list endpoints: a `recorded` filter (`true` / `false` / unset);
  - `EventType`: no change — `call` and `end` already exist (see C4).
  My call on the default, change if you disagree: **unrecorded calls are included** in call lists,
  counts and stats unless the filter excludes them, because "this unit talked on an encrypted
  talkgroup 14 times" is exactly what a unit page should say; the `recorded` flag and reason are
  shown rather than the call hidden. The Go engine, fed by Trunk Recorder's MQTT, simply never
  produces unrecorded calls (TRP's `call_ignored` message is the one exception, and the Go engine
  may adopt it later).

Design calls (mine; change freely):

- C1. **Process shape.** One binary, two modes: `--describe` and plugin mode (stdin/stdout to TRP,
  the default), and a standalone `serve` mode that reads nothing from stdin and only serves the
  store — this is what runs when TRP isn't recording, if the user starts it by hand or TRP later
  gains a keep-running flag. Both modes open the same SQLite file.
- C2. **HTTP on its own port**, default 8092, bound to `127.0.0.1` by default; settings: port,
  bind address, optional bearer token (open when unset, as tr-engine's "open" mode). The manifest's
  `homepage` is set at `--describe` time to `http://127.0.0.1:<port>/` so TRP's "Website" link opens
  it. Serving on TRP's own port is impossible today (section 3).
- C3. **Store layout**: `plugin-data/<id>/engine.sqlite`, WAL mode, `synchronous=NORMAL`, a single
  writer thread fed by a channel from the stdin reader; readers use a small connection pool.
  Crate: `rusqlite` with the `bundled` feature (compiles SQLite in; the template's CI runners have a
  C compiler, and TRP's own release builds already cross-compile C). If a build target proves
  painful, the fallback is the same crate without `bundled` on that platform.
- C4. **Schema**, SQLite-native, shaped like tr-engine's core tables so export/import can map 1:1:
  - `systems` (short_name PK across runs, kind, name, sysid, wacn, nac, first/last seen);
    `sites` (system, name, rfss, site number, control channel, last seen).
  - `talkgroups` (system, tgid, alpha tag, description, group, tag, first/last heard, call count,
    encrypted count, last cipher), filled from the tags each call carries (TRP resolves its
    talkgroup files before the plugin sees a call). tr-engine's `talkgroup_directory` (the CSV
    import) is **not in phase 1**: `hello` doesn't name TRP's talkgroup files, and guessing their
    path from the data folder would couple the plugin to TRP's folder layout — see C11(c).
    `units` (system, unit id, alpha tag, source of tag, first/last seen, last affiliation and when,
    registered flag).
  - `calls` (id, system, site, tgid, start_ms, end_ms, length_ms, freq_hz, tdma_slot, analog,
    encrypted, algid, key_id, decrypted, emergency, duplex, packet, priority, recorded (bool) and
    not_recorded_reason, audio_path (relative to TRP's capture dir; NULL when not recorded),
    json_path, signal, noise, snr, clean_voice_pct, error_count, spike_count, freq_error,
    state (recording / ended / concluded), metadata_json TEXT for anything else the record carries).
  - `call_transmissions` (call_id, position (1..n), unit, time_ms, pos_s into the audio, len_s,
    emergency, tag at the time); indexes on (unit, time_ms) and (call_id).
  - `call_frequencies` (call_id, freq_hz, time_ms, pos_s, len_s, error_count, spike_count).
  - `unit_events` (system, unit, kind, time_ms, talkgroup, emergency, site, metadata_json);
    index on (unit, time_ms) and (time_ms). Kinds are TRP's control-channel kinds **plus
    synthesized `call` and `end` rows, one pair per transmission**, made from `call.start`'s
    sources and `call.concluded`'s `srcList` (and published on SSE as `unit_event`), because
    tr-engine's `EventType` has them and its units, timeline and unit-events pages are built on
    them. Cost: two or three extra rows per call, trivial at this scale (10,000 calls/day ≈ 30,000
    rows/day).
  - No `unit_pairs` table in phase 1: nothing in `openapi.yaml` or the phase-1 pages reads
    partners. D4 still holds for whenever it's needed (phase 3, TRP's own pages); it can be computed
    from `call_transmissions` then, or materialized if it proves slow.
  - `recorder_state` (time_ms, per-site decode rate, control channel, active, recording; one row per
    status message, decimated by a maintenance task: full for a week, then one per minute, then one
    per hour, as tr-engine does).
  - Times are INTEGER Unix milliseconds everywhere; JSON is TEXT read with SQLite's JSON functions;
    no arrays — `calls.unit_ids` becomes a query on `call_transmissions`.
  - Trunking (control channel) messages are **not stored**: TRP's plugin protocol doesn't publish
    them, so there is no firehose table.
  - Retention: a setting, default "keep everything"; when set (days), a daily task deletes calls
    older than that and their child rows. Audio retention stays TRP's business (its Archive
    settings); the plugin's call rows outlive the audio and `audio_path` is checked on request.
- C5. **Event mapping.** `call.start` → insert `calls` (state recording) and publish SSE
  `call_start`; `call.end` → update end/length, SSE `call_end`; `call.concluded` → fill the
  recorded fields and paths, insert `call_transmissions` and `call_frequencies` from `srcList` /
  `freqList`, bump `talkgroups`/`units` counters, synthesize the `call`/`end` unit events, SSE `call_concluded`, then
  `call.result ok` immediately (the plugin never holds TRP's files). A call that ends unrecorded
  (encrypted, no source covering, rules) keeps its row with `recorded = false` and the reason.
  `unit` → `unit_events` plus `units` upsert (affiliation/registration state). `status` →
  `systems`/`sites` identity upsert and a `recorder_state` row. `audio` → live-audio fan-out over
  WebSocket in tr-engine's frame format (phase 2; not subscribed in phase 1 so TRP doesn't build it).
- C6. **Backfill.** On start, if the store is empty (or a setting asks), walk TRP's capture folder
  (given in `hello`) for `*.json` call records newer than the newest call in the store and ingest
  them as `call.concluded` with no SSE. (`trunk-pro plugin run` uses a temporary data folder by
  design, so it only reaches the real store with `--data-dir`; it's a test bench, not the backfill
  path.) Calls that weren't recorded have no file and can't be backfilled; that's accepted.
- C7. **API surface, phase 1** (paths and shapes exactly as `openapi.yaml`): health; systems and
  sites; talkgroups (list, detail, stats, units on it); units (list, detail, events, calls);
  calls (list with tr-engine's filters, detail, audio by call id streaming the file from the
  capture dir with range support); unit events; **unit affiliations** (derived from `unit`
  affiliation events; the talkgroup-research and unit-tracker pages call it); **recorders**, served
  as one entry per site synthesized from `status` (control channel, decode rate, active and
  recording counts), since TRP has no per-recorder state and the index and systems-overview pages
  expect the endpoint and the `recorder_update` SSE event; stats endpoints the built-in pages call; `auth-init`
  reporting open/token mode; SSE `/api/v1/events/stream` with the same event types and server-side
  filters. A conformance test suite is generated from `openapi.yaml` and run against both engines
  (the Go one needs a test Postgres; the plugin needs nothing). Endpoints out of scope return 501
  with a JSON body naming the feature, not 404, so clients can tell "not here" from "wrong path".
- C8. **Pages.** tr-engine's `web/*.html` are embedded and served at `/`, index first, with the
  page-registration meta tags honoured. The bar for a shipped page is **works fully**: every
  panel on it is fed; a page with a blank panel isn't listed. Phase 1 ships: call history,
  talkgroup research, both unit pages (`units.html` and `unit-tracker.html`, which share the
  "Unit Tracker" title), systems overview, events, timeline, and the index. Pages needing phase-2
  features (live audio, transcription, storage) are left out of the index.
- C9. **Crates.** `axum` + `tokio` for HTTP/SSE/WebSocket; `rusqlite` (bundled); `serde`/`serde_json`;
  `schemars` for the settings schema; `rust-embed` for pages; the `trunk-recorder-plugin` SDK for
  the protocol. No ORM, no query generator: hand-written SQL in one module per table, with tests
  on an in-memory database.
- C10. **Naming.** Plugin id to be chosen by the owner (store ids can't change); working name in
  this document: `reporter`. Repository under the trunk-reporter org from the TRP plugin template.
- C11. **Upstream asks to TRP, filed as issues with the first release**, neither blocking phase 1:
  (a) a manifest field declaring a UI port/URL so the plugin card gets an "Open" button and the
  Plugins page lists it; (b) a manifest flag asking to keep the plugin running while the recorder
  is idle, with `hello` still sent at recording start; (c) the paths of each system's talkgroup
  and unit files in `hello`, so the plugin can offer tr-engine's talkgroup directory without
  guessing TRP's folder layout.

## 5. Phases

- Phase 1 (the one-click history): C1–C10 as above, phase-1 API and pages, backfill, retention.
- Phase 2: live audio over WebSocket from the `audio` topic; affiliation views; export/import in
  tr-engine's format so a plugin store can be migrated into a Go engine and back.
- Phase 3 (needs upstream): TRP's own unit and talkgroup pages reading from the plugin.

## 6. Left to the builder

- Exact SQL, indexes beyond those named, and the connection-pool size.
- How `openapi.yaml` is turned into conformance tests (generated or hand-picked per endpoint).
- Identity when the control channel hasn't stated sysid/WACN yet: short_name is the key regardless;
  the identity columns fill in when `status` carries them.
- SSE ring-buffer size and `Last-Event-ID` replay depth (tr-engine uses 4,096 events).
- Range requests and content types for audio (WAV always; M4A when TRP made one for another plugin).
- Handling of `call.concluded` arriving for a call whose `call.start` was lost (queue overflow or
  plugin restart): insert from the record alone.
- Decimation schedule details for `recorder_state`.
- Logging through `Host::info/warn/error`, and what the TRP health card shows (queue depth, calls
  ingested, store size, serving address).
