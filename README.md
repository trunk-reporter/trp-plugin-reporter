# Trunk Reporter

Trunk Reporter is a [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro)
plugin that brings [tr-engine](https://github.com/trunk-reporter/tr-engine)'s
call history, REST API and web pages to Trunk Recorder Pro (TRP). It keeps
calls, units, talkgroups and affiliations in a SQLite file in the plugin's own
folder and serves them from the plugin process. There is no database, broker or
second service to run.

## Install

Once Trunk Reporter is listed, install it from the Plugins page in TRP. Until then,
use "Add from a file", or give TRP a GitHub release URL for the archive that
matches your system.

It starts with the default settings below and needs no configuration.

Upgrading from 0.1.0, when the plugin was named `reporter`: uninstall `reporter`
in TRP first (both default to port 8092), and move its folder
`plugin-data/reporter` to `plugin-data/trunk-reporter` before Trunk Reporter
first starts, so it opens the history you have. The CHANGELOG has the steps.

## Settings

| Setting | Default | Notes |
|---|---|---|
| Port | 8092 | |
| Bind address | 127.0.0.1 | Use `0.0.0.0` to reach it from other machines. Without an access token, anyone on the network can then read the call history. |
| Access token | none | Leave empty for no login. When set, clients send it as a bearer token. |
| Keep history for (days) | 0 | 0 keeps everything. |
| Backfill from recorded calls on start | on | See Backfill. |
| Allowed origins | none | Web pages served from these origins may call the API. One origin per line, such as `http://192.168.1.20:8081`. |
| Transcribe plugin URL | `http://127.0.0.1:8767` | Where the transcribe plugin's local API is. Empty turns transcripts off. See Transcripts. |

From Trunk Recorder Pro 0.3.0, a change takes effect without restarting the
plugin, except "Backfill from recorded calls on start", which waits for the
next start. A settings change that moves the server, or changes its token or
origins, closes open event streams and live audio, and clients reconnect. If
the new port is taken or the bind address is not an IP address, the change is
refused and the plugin keeps serving as before. Older recorders restart the
plugin instead.

## What you get

- The API at `http://127.0.0.1:8092/api/v1/`, following tr-engine's
  `openapi.yaml` for the endpoints built so far, including the live event
  stream (SSE).
- Added in 0.2.0: `/stats/talkgroup-activity`, `/talkgroups/active`,
  `/units/{id}/talkgroups`, `/first-heard`, `/stats/baseline`,
  `/preferences/{key}` and `/audio/live`.
- The pages at `http://127.0.0.1:8092/`. The Website link on the plugin's card
  in TRP opens them.

## Transcripts

Trunk Reporter shows transcripts from the `transcribe` plugin. Set "Transcribe
plugin URL" to that plugin's local API (the default is
`http://127.0.0.1:8767`); leave it empty to turn transcripts off. Upgrades get
the bridge on by default: without a transcribe plugin, empty the URL to turn it
off. A transcribe plugin on another host must be bound beyond 127.0.0.1 on its
side.

- `GET /calls/{id}/transcription` and `GET /transcriptions/batch` answer from
  the transcribe plugin.
- Calls, in lists and singly, carry `has_transcription` and `transcription_text`
  (not `/calls/active`: calls on the air have no transcript yet).
- A `transcription` event on `/events/stream` announces a transcript as it
  lands, with `{ call_id, system_id, tgid, text, start_time }`.
- `/health` has `features.transcriptions` and a `transcribe` block with the
  `url`, whether it is `connected`, and `last_event_ms` (the `transcribedMs`
  of the last transcript relayed, ms since the epoch); the block is absent
  when the bridge is off. The plugin's status line says `transcripts: live` or
  `transcripts: unreachable`.

The transcribe plugin keeps the transcripts; Trunk Reporter stores none of
them. Calls it skipped or failed read as having no transcript. Search, verify,
reject and exclude, the queue and backfill are not built and answer 501, and
word timing is not carried.

## Tone patterns

From Trunk Recorder Pro 0.3.0, the recorder finds the patterns a call's tones
make: two-tone pages, long and hi-low alert tones, pulsed tones, DTMF and KNOX
digit strings, Morse IDs and tone sequences. Trunk Reporter keeps them, which
tr-engine has no model for:

- `GET /api/v1/tone-patterns` lists them across calls, newest first. Filter by
  `system_id`, `tgid`, `unit_id` (the radio talking when the pattern began),
  `call_id`, `pattern` (`two_tone,digits`), `start_time` and `end_time`. Each
  has the record's fields (`tones`, `lengths`, `format`, `digits`, `text`,
  `signals`, …) with the call, system, talkgroup and radio beside them.
- A `tone_pattern` event on `/events/stream` announces each one as it is heard,
  with the same fields. Its subtype is the pattern, so
  `types=tone_pattern:two_tone` streams pages only.

They are stored from each call's record (the `tone_patterns` its JSON has),
so an unrecorded call's patterns are only streamed live, not kept. Calls
stored before this version are indexed the first time it opens the store.

## Live audio

Live audio is a WebSocket at `/api/v1/audio/live`. It sends PCM frames, framed
as tr-engine frames them. It works in plugin mode only: serve mode has no
recorder, so it answers 404. With an access token set, pass it as `?token=`
because a browser cannot add a header to a WebSocket.

## Preferences

`GET` and `PUT /api/v1/preferences/{key}` keep one JSON document per key in the
store, for the web app's settings. They are shared by everyone who can reach the
server.

## Serve mode

Trunk Reporter can serve the history without the recorder running:

```sh
trunk-reporter serve <data-dir>
```

The data folder is the plugin's folder, `plugin-data/trunk-reporter/` beside TRP's
config file (`%APPDATA%\trunk-pro\` on Windows, `~/.config/trunk-pro/` on
Linux, `~/Library/Application Support/trunk-pro/` on macOS, or beside the file
given with `--config`). It needs a store from at least one run under TRP. Put
the same settings TRP uses in `serve.json` there (`port`, `bind`, `token`,
`retentionDays`, `backfill`); without it the defaults apply. Serve mode uses
`port`, `bind` and `token` only: it ignores `retentionDays` and `backfill`, so
it neither prunes old history nor loads recorded calls. The capture folder is
read from the store.

## Backfill

On first start, Reporter walks TRP's capture folder and loads the calls already
recorded there, so the history begins with what you have. If it is interrupted
it resumes where it stopped on the next start. Turn "Backfill from recorded
calls on start" off to start with an empty history.

From Trunk Recorder Pro 0.3.0, it also asks the recorder's call store for the
calls it indexes, newest first. This finds calls whose JSON file is not kept
(the call log turned off), which the folder walk cannot see. Calls without a
kept audio file are stored without audio. Once it has reached the oldest call,
later starts fetch only the calls added since.

## Not built yet

Transcript search and review, talkgroup directory import, trunking messages, call groups, the
other statistics and analytics endpoints, and user accounts are not
implemented. Their endpoints answer 501 with the error code `not_implemented`.

## Working with Trunk Recorder Pro

Three small additions to TRP would make Reporter a better fit (design spec C11):

1. A manifest field for a UI URL, so TRP shows an Open button for plugins that
   serve pages (today only the Website link does this).
2. A keep-running-while-idle flag, so the plugin stays up between recordings
   and still receives `hello` when recording starts.
3. Talkgroup and unit file paths in `hello`, so names can be loaded without
   asking the user for them. From 0.3.0, calls and unit events carry the
   names of the radios and talkgroups they involve, so only the ones never
   heard are missing.

Reporter is built on plugin SDK 0.9.0, the one Trunk Recorder Pro 0.3.0 ships, taken
from the recorder's repository (tag `v0.3.0`) until it is on crates.io. It speaks
plugin API 1, as the 0.2 releases do, so they run it too (checked on 0.2.11).
They run it without 0.3.0's additions: settings taken while running, the call
store backfill, system names and sites from `hello`, and unit and talkgroup
names from unit events.

## Spec change for tr-engine

Reporter's API adds a `recorded` filter, `recorded`, `not_recorded_reason` and
`decrypted` fields on calls, and the `not_implemented` error code. Its
`/tone-patterns` endpoint and `tone_pattern` event are its own: they are in the
`openapi.yaml` it serves, not proposed to tr-engine. These need
to be added to tr-engine's `openapi.yaml`: see
[docs/tr-engine-openapi-d6.patch](docs/tr-engine-openapi-d6.patch) (D6 in the
design).

The design is in [docs/superpowers/specs](docs/superpowers/specs) and the plan
in [docs/superpowers/plans](docs/superpowers/plans).

## Development

```sh
cargo test
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cargo run --locked --quiet -- --describe > m.json && python .github/check_manifest.py m.json
```

To try the plugin without a live recorder, replay a capture folder through it
with TRP's test bench:

```sh
cargo build --release
trunk-pro plugin run target\release\trunk-reporter.exe <capture dir> --limit N
```

## License

MIT
