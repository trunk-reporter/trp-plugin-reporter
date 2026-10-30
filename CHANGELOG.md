# Changelog

## [Unreleased]

## [0.3.0]

Transcripts from the `transcribe` plugin.

- New setting "Transcribe plugin URL" (`transcribeUrl`, default
  `http://127.0.0.1:8767`); empty turns transcripts off. Upgrades get the
  bridge on by default: without a transcribe plugin, empty the URL to turn it
  off.
- `GET /calls/{id}/transcription` and `GET /transcriptions/batch` answer from the
  transcribe plugin, and calls carry `has_transcription` and
  `transcription_text`. Search, verify, reject, exclude, the queue and backfill
  stay 501; word timing is not carried.
- A `transcription` event on `/events/stream`, with
  `{ call_id, system_id, tgid, text, start_time }`.
- `/health` has `features.transcriptions` and a `transcribe` block
  (`url`, `connected`, `last_event_ms`); the status line says
  `transcripts: live` or `transcripts: unreachable`.
- OmniTrunker plays every talkgroup until you mute it; before, every talkgroup
  started muted and the page was silent until each was picked by hand. Muted
  talkgroups are the ones saved now (the old "selected" list is dropped), the
  speaker shows a red box when muted and a padlock when the call is encrypted,
  and the header counts the muted ones.
- The shared audio engine starts a new talkgroup at full volume with its stereo
  position assigned. Pages that mute on their own (the scanner) are unchanged.

## [0.2.0]

Phase 2: what the web pages need beyond phase 1.

### Upgrading from 0.1.0

The rename makes 0.2.0 a different plugin to TRP: it does not replace the
`reporter` plugin, and it does not find that plugin's store. To keep your
history:

1. Uninstall the `reporter` plugin in TRP (both serve on port 8092 by default,
   so the two cannot run side by side).
2. Before Trunk Reporter starts for the first time, move the folder
   `plugin-data/reporter` to `plugin-data/trunk-reporter` (beside TRP's config
   file; see Serve mode in the README).
3. Install `trunk-reporter`. Settings are not carried over: set them again on
   the plugin's card.

If Trunk Reporter has already started, it has made an empty store; stop it,
delete `plugin-data/trunk-reporter`, move the old folder in, and start it again.

### Changes

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
