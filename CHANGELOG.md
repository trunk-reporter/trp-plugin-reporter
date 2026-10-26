# Changelog

## [0.2.0]

Phase 2: what the web pages need beyond phase 1.

- The plugin is now named `trunk-reporter` (crate, executable and release files).
- Fifteen more of tr-engine's pages are served, 23 in all, and several pages that
  failed against the phase 1 API now work (the stream graph, scanners and daily
  overview among them).
- A `limit` over 1000 on a list is clamped to 1000 instead of answering 400.
- `/query` answers 403 instead of 501.
- `GET /calls/{id}/transcription` answers 404 "has no transcription" for a call that exists, not 501, so the pages treat it as "none".
- `/health` has a `features` object saying what this server can do.
- New setting "Allowed origins" (`corsOrigins`): pages served from the listed
  origins may call the API.
- `GET /stats/talkgroup-activity`, as a summary and in `interval` buckets.
- `GET /talkgroups/active`: the talkgroups active in a time window.
- `GET /units/{id}/talkgroups`: the talkgroups a radio talked on.
- `/talkgroups/{id}/units` items carry `first_seen_on_talkgroup`.
- `GET /first-heard`: talkgroups and radios heard for the first time.
- `GET /stats/baseline`: what is normal for now, to compare with what is busy.
- `GET` and `PUT /preferences/{key}`: a JSON document per key, kept in the store.
- Live audio as PCM over WebSocket at `/api/v1/audio/live`, framed as tr-engine
  frames it. Plugin mode only; serve mode answers 404.

## [0.1.0]

First release (phase 1).

- A SQLite store in the plugin's data folder holds calls, units, talkgroups and
  affiliations, so history survives restarts.
- tr-engine's REST API for the phase 1 subset at `/api/v1/`, with an optional
  bearer access token. Endpoints that are not built yet answer 501
  `not_implemented`.
- A live event stream (SSE) of calls and unit activity.
- tr-engine's web pages at `/`, linked from the plugin's Website link in TRP.
- Backfill: on first start the capture folder's recorded calls are loaded;
  an interrupted backfill resumes on the next start.
- Retention: "Keep history for (days)" prunes old history; 0 keeps everything.
- Serve mode: `trunk-reporter serve <data-dir>` serves the stored history without the
  recorder.
- Targets Trunk Recorder Pro plugin SDK 0.5. A later release moves to SDK 0.7
  for what 0.5 lacks (system names and sites from `hello`).
