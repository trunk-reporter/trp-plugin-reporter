# Transcripts in trunk-reporter: the bridge to the transcribe plugin

Design, 2026-10-30. For the `trunk-reporter` plugin (this repo, `D:\reporter`), reading the transcripts the `transcribe` plugin (`D:\trunk-plugin-transcribe`, repo trunk-reporter/trunk-plugin-transcribe) makes, and pushing them to tr-engine's pages as they land.

## Intent

The transcribe plugin transcribes every recorded call and keeps the results in its own SQLite store, served on its HTTP side (port 8767 by default). trunk-reporter serves tr-engine's API and pages, and today answers "no transcription" for every call: its transcription routes were stubbed when it was built, waiting on a TRP `annotations` table that is still phases away upstream. Nothing connects the two plugins, so a running transcribe plugin is invisible on the pages.

This bridges them: trunk-reporter reads transcripts from the transcribe plugin's API, serves them in the shapes its pages already read, and relays the plugin's event stream into its own so a page hears a transcript 5 to 15 seconds after the call ends, without reloading.

The user's decisions behind it (2026-10-30):

- **One standard shape, ours, with provider adapters inside the transcribe plugin.** Consumers never see a provider's shape. The transcribe plugin already works this way; nothing in it changes for this project.
- **One line per keyup with the radio named** is the view to serve: a transcript is the call's text plus utterances, each one radio's span with its text. Word times are an optional refinement inside an utterance, never required (today the transcribe plugin keeps none).
- **Pushed, not only fetched:** transcripts reach the pages live.

## The shape (Transcript, version 1)

What the transcribe plugin serves today, and what trunk-reporter reads. The authority is the transcribe plugin's `Transcript` (`src/store.rs`); this is a restatement so a builder here needs no second repo open.

```json
{
  "path": "marcs/2026/10/30/48549-1793358967_851625000-call_2414",
  "system": "marcs",
  "talkgroup": 48549,
  "startMs": 1793358967667,
  "lengthMs": 1620,
  "status": "ok",
  "reason": null,
  "text": "234?",
  "language": "English",
  "model": "/home/…/Qwen3-ASR-1.7B-flat",
  "transcribedMs": 1793358971026,
  "segments": [ { "unit": 8342327, "start": 0.64, "end": 1.28, "text": "234" } ]
}
```

- `path` is the call's recording path relative to the recorder's capture folder, without extension: the SDK's call path, the same one trunk-reporter stores in its `calls.path` column. It is the join key. Checked live 2026-10-30: both plugins take it from the SDK's `ConcludedCall.path` and the live trunk-reporter store holds it in exactly this form (forward slashes). A call stored by the **backfill** derives its path from the file's path on disk, which on Windows may carry backslashes: the join normalises `\` to `/` on both sides (the store lookup and the paths sent to the client), with a test.
- `status` is `ok`, `skipped` or `failed`; `reason` says why for the other two (`silent`, `hallucination`, `too short`, an engine error). `text` and `segments` are only meaningful for `ok`.
- `segments` are utterances: `unit` is the radio (null when the engine gave no word times, then there is one segment for the whole call), `start` and `end` are seconds from the call's start.
- Not there yet, and optional when they come (a later change to the transcribe plugin, not this project): a per-utterance `confidence`, per-utterance `words` `[{ word, start, end }]`, and a `shape: 1` field on the whole. trunk-reporter must ignore fields it does not know.

The transcribe plugin's routes trunk-reporter uses (all under its `http.port`, default 8767, bound to 127.0.0.1 by default):

| Route | Answers |
|---|---|
| `GET /api/v1/transcripts/{path}` | one Transcript; 404 `{ "error": "no transcript" }` |
| `GET /api/v1/transcripts?paths=a,b,c` | `{ "transcripts": [Transcript…] }`, up to 200 paths per request, a path with no row absent |
| `GET /api/v1/events` | server-sent events: `event: transcript`, `data:` a Transcript, `id:` its `transcribedMs`; `event: ping` every 15 s; a request with `Last-Event-ID` replays rows written since that time |
| `GET /api/v1/health` | `{ ok, engine: { url, model, up, lastOkMs, lastError }, queue, inFlight, transcribed24h }` |

## Goals

1. A call that the transcribe plugin transcribed shows its transcript on tr-engine's pages: the call history's detail panel and the IRC-style live page, per radio where the transcript has utterances.
2. A transcript that lands while a page is open appears on it within seconds, with no reload.
3. Call lists say which calls have a transcript, and carry the text for the snippet column and the client-side search the pages do today.
4. The bridge is off by default when no transcribe plugin is configured, and the plugin's absence (not installed, not running, restarting) costs nothing but a log line and "no transcript" answers: no errors on pages, no stuck requests.
5. The pages are untouched: trunk-reporter translates to the shapes they read now, in one place.

## Non-goals

- Changing the transcribe plugin. Its shape and routes are taken as they are.
- Storing transcripts in trunk-reporter's SQLite, or caching them. Every answer is read through from the transcribe plugin (it is local; a list page costs one batch request).
- Word-level timing, confidence, verification and rejection, the transcription queue and backfill routes tr-engine has (`/calls/{id}/transcribe`, `/transcription/verify`, `/transcriptions/queue`, `/admin/transcribe-backfill`…): they stay 501 "not implemented".
- Full-text search over transcripts (`/transcriptions/search`): stays 501 for now. The pages search client-side over the text they already hold; the transcribe plugin's own `/api/v1/search` is there for a later step.
- TRP's `annotations` table. When it exists, the transcribe plugin can write the same shape there and trunk-reporter's reader switches source; that is why reading lives behind one small client.

## Anti-goals

- A page that shows an empty or wrong transcript for a call (a skipped call's reason as if it were text, a transcript on the wrong call).
- A transcript lost on a hiccup: a reconnect to the event stream must not drop what landed meanwhile.
- A transcribe plugin that is down making trunk-reporter slow: requests to it are bounded, and the pages still load.

## Constraints

- This crate: Rust, `rust-version = "1.85"`, SDK `trunk-recorder-plugin = "0.5"` (unchanged), axum 0.8, tokio, rusqlite 0.41, serde. No HTTP client dependency today: add **`reqwest`** 0.12 with `default-features = false` and features `["json", "stream", "rustls-tls"]` (the transcribe plugin uses the same; rustls keeps the Windows build free of OpenSSL). Streaming is needed for the event stream.
- The settings form is drawn from `Config`'s JSON Schema (`schemars` 1); a new field needs a doc comment for its label and help text, and a default in `Default`.
- The plugin binary the recorder runs is `D:\trunk-pro-airspy-fix\trunk-reporter.exe` (a copy of this crate's release build); the live recorder is on 0.2.11 + PR #104 with the transcribe plugin running (`http://127.0.0.1:8767`, engine on Eddie) and busy: a live run can check the bridge end to end.
- Nothing here touches the recorder, its config, or the transcribe plugin.

## The design

### Settings

One new field in `Config` (`src/config.rs`):

- `transcribe_url: String`, default `"http://127.0.0.1:8767"`. Label "Transcribe plugin URL"; help: "Where the transcribe plugin serves its transcripts. Leave empty when it isn't installed." Empty disables the bridge entirely. A trailing `/` is tolerated.

`Features.transcriptions` (`src/api/types.rs`) is `true` whenever the URL is set, so the pages show their transcript UI; a plugin that is down then simply has no transcripts, which is how the pages already handle "none".

### The client: `src/transcripts.rs`

A small module owning everything that knows the transcribe plugin:

- The shape above as serde structs (`Transcript`, `Utterance`), `camelCase`, unknown fields ignored.
- `TranscribeClient { base: String, http: reqwest::Client }` with:
  - `one(path) -> Result<Option<Transcript>>` (404 → `None`)
  - `batch(paths: &[String]) -> Result<Vec<Transcript>>`, splitting into requests of at most 200 paths
  - `health() -> Result<Health>`
  - every request with a 3 s connect and 10 s total timeout; a connection error or timeout is `Err`, which callers turn into "no transcript" plus a rate-limited log line (one per minute per route), never a 5xx on a page. The event stream is the exception: a connect timeout only, and a stream with no frame (ping included) for 45 s is treated as dead and reconnected.
- `is_ok(&Transcript) -> bool`: `status == "ok"` and `text` non-empty. Only such transcripts count as present anywhere below (the user's call: a silent or failed call reads as having no transcript; the reason stays visible through the plugin's own API).

### The adapter: our shape → tr-engine's responses

In `src/api/calls.rs` (replacing the stubs), one translation function used by every route, `to_engine(call_id, &Transcript) -> EngineTranscription`:

```json
{
  "call_id": 2414,
  "text": "234?",
  "language": "English",
  "model": "…",
  "transcribed_at": "2026-10-30T13:02:51.026Z",
  "segments": [ { "start": 0.64, "end": 1.28, "src": 8342327, "text": "234" } ],
  "words": { "segments": [ …the same… ], "words": [] }
}
```

- `segments[].src` is the utterance's `unit` (null when none). The IRC page matches a segment to a keyup by time overlap and falls back to `src`; the call-history page reads `words.words` and, finding it empty, falls back to the call's `transcription_text`. Both work with this without a page change (`web/irc-radio-live.html` `handleTranscription`, `web/call-history.html` `buildDetailPanel`).
- Routes:
  - `GET /calls/{id}/transcription`: the call's `path` from the store → `client.one(path)`; `ok` → 200 with the shape above; none, not `ok`, no `path`, bridge off, or plugin unreachable → 404 "call N has no transcription" (as today).
  - `GET /transcriptions/batch?call_ids=…`: validate as today; look up the ids' paths in the store; `client.batch(paths)`; answer `{ "transcriptions": [ { "call_id", "text", "segments" } … ] }` for the `ok` ones only, in the order asked.
  - Call lists and single calls (`Call` in `src/api/types.rs`): `has_transcription` true for calls with an `ok` transcript, and a new optional `transcription_text: Option<String>` (serialized only when present) carrying the text. Filled per response: after a list's rows are built, one `client.batch` over their paths (chunked by 200), merged by path; a single call's route does the same with `one`. A plugin that is down leaves both as they were (false, absent).
- The call → path mapping is `CallRow.path` (`src/store/calls.rs`). A call stored without a path (one that was never concluded) has no transcript.

### The relay: the transcribe plugin's event stream → the bus

A tokio task started in `Plugin::start` (`src/plugin.rs`) when the bridge is on, on the plugin's existing runtime, stopped at `shutdown`:

1. `GET {base}/api/v1/events` with `Accept: text/event-stream` and, after the first connection, `Last-Event-ID: <last transcribedMs seen>`, so a reconnect replays what landed while it was down (the plugin keeps rows; nothing is lost).
2. Parse SSE frames from the streamed body (`id:`, `event:`, `data:` lines, blank line ends a frame; `event: ping` ignored; `data:` may span several lines). A small parser in `src/transcripts.rs`, unit-tested; no SSE client dependency.
3. For each `transcript` frame that `is_ok`: find the call by `path` in the store (`calls.path`). Found → publish on the bus:

   ```json
   event: transcription
   data: { "call_id": 2414, "system_id": 1, "tgid": 48549, "text": "234?", "start_time": "…" }
   ```

   as a new `EventKind::Transcription` (`"transcription"`, `src/bus.rs`) with `system_id`, `tgid` and `unit_id` null set on the event so the stream's filters (`systems=`, `tgids=`, `types=transcription`) apply. The IRC page listens for exactly this event name and then fetches `/calls/{id}/transcription` for the utterances (`es.addEventListener('transcription', …)`).

   Not found (the transcript arrived before trunk-reporter stored the concluded call, or the call predates the store): retry the lookup after 2 s and again after 10 s, then drop it with a debug log line. (The transcribe plugin works after the recorder has saved the call, and trunk-reporter stores it on `call_concluded`, so this is rare.)
4. On any error or end of stream: reconnect after a backoff of 1 s doubling to 30 s, reset on a successful frame. A connect failure logs once per state change ("transcribe plugin not reachable at …", then "transcribe plugin event stream connected"), not per attempt.

Settled in the builder check (my calls):

- The plugin's replay is exclusive (`transcribed_ms > Last-Event-ID`). The relay still dedupes by `(path, transcribedMs)` across a reconnect, so a replayed frame is published once. Two rows written in the same millisecond with the cut between them could be missed by the stream; the read-through routes cover that.
- The first connection after trunk-reporter starts sends no `Last-Event-ID`: transcripts that landed while it was down become no events. Pages fetch them by read-through when they load, and nothing is stored to remember a last id (the non-goal above).
- `EventKind::Transcription` is added to `openapi/openapi.yaml`'s event list and wherever the `types=` filter values are documented or validated.

Health and status: `/health` gains `"transcribe": { "url", "connected": bool, "last_event_ms": number|null }` (`src/api/health.rs`); the plugin's status line (`status_line` in `src/plugin.rs`) appends "transcripts: live" or "transcripts: unreachable" when the bridge is on; a metric `transcripts_relayed` counts events published.

### `serve` mode

`trunk-reporter serve` (no recorder) has no settings form; it reads the same `Config` from its file/flags today. The bridge works there the same way from `transcribe_url`; nothing special.

## Testing

- `src/transcripts.rs`: the SSE parser (frames split across chunks, multi-line data, ping, bad id); `is_ok`; `batch` chunking; shapes round-trip from the JSON above; unknown fields ignored.
- A **fake transcribe plugin** in tests (an axum server, as the transcribe plugin's own tests do for the engine): scripted `/api/v1/transcripts`, `/api/v1/transcripts/{path}`, `/api/v1/events` (sends N frames then holds, or closes to force a reconnect, and records the `Last-Event-ID` it was given), `/api/v1/health`.
- API tests (`tests/api_calls.rs` and a new `tests/api_transcripts.rs`): `/calls/{id}/transcription` 200 with the adapter's shape for an `ok` transcript, 404 for a `skipped` one, for a call with no transcript, with the bridge off, and with the fake down (and that the down case answers within the timeout, not a 500); `/transcriptions/batch` order and filtering; a call list carrying `has_transcription` and `transcription_text`; `/health` features and `transcribe`.
- The relay (`tests/relay.rs` or in `src/plugin.rs` tests): a frame becomes a `transcription` event on the bus with the right call id, system and tgid, seen by an SSE subscriber of trunk-reporter's own stream; a `skipped` frame does not; a path not in the store is retried then dropped; after the fake closes the stream, the reconnect carries `Last-Event-ID` and the replayed frame is published once.
- A live run against the recorder: build release, copy to `D:\trunk-pro-airspy-fix\trunk-reporter.exe` (the controller does the copy and the recorder restart), open the IRC page and the call history, and see a transcript land on a live call within 15 s of its end and a detail panel show one.

## Left to the builder

- Module layout inside `src/transcripts.rs` (client, parser, relay task) and whether the relay lives in `plugin.rs` or its own file.
- The exact rate limiting of log lines and the reconnect backoff within the bounds above.
- How list responses carry the batch (one call after the rows are built, or inside the row builder), as long as it is one request per 200 rows.
- Test file placement following the repo's current layout.
