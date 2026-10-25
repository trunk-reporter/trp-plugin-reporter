# Reporter

Reporter is a [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro)
plugin that brings [tr-engine](https://github.com/trunk-reporter/tr-engine)'s
call history, REST API and web pages to Trunk Recorder Pro (TRP). It keeps
calls, units, talkgroups and affiliations in a SQLite file in the plugin's own
folder and serves them from the plugin process. There is no database, broker or
second service to run.

"Reporter" is the working name; the final plugin id is the project owner's
choice.

## Install

Once Reporter is listed, install it from the Plugins page in TRP. Until then,
use "Add from a file", or give TRP a GitHub release URL for the archive that
matches your system.

It starts with the default settings below and needs no configuration.

## Settings

| Setting | Default | Notes |
|---|---|---|
| Port | 8092 | |
| Bind address | 127.0.0.1 | Use `0.0.0.0` to reach it from other machines. |
| Access token | none | Leave empty for no login. When set, clients send it as a bearer token. |
| Keep history for (days) | 0 | 0 keeps everything. |
| Backfill from recorded calls on start | on | See Backfill. |

## What you get

- The API at `http://127.0.0.1:8092/api/v1/`, following tr-engine's
  `openapi.yaml` for the endpoints in phase 1, including the live event stream
  (SSE).
- The pages at `http://127.0.0.1:8092/`. The Website link on the plugin's card
  in TRP opens them.

## Serve mode

Reporter can serve the history without the recorder running:

```sh
reporter serve <data-dir>
```

The data folder is the plugin's folder, `plugin-data/reporter/` beside TRP's
config file (`%APPDATA%\trunk-pro\` on Windows, `~/.config/trunk-pro/` on
Linux, `~/Library/Application Support/trunk-pro/` on macOS, or beside the file
given with `--config`). It needs a store from at least one run under TRP. Put
the same settings TRP uses in `serve.json` there (`port`, `bind`, `token`,
`retentionDays`, `backfill`); without it the defaults apply. The capture folder
is read from the store.

## Backfill

On first start, Reporter walks TRP's capture folder and loads the calls already
recorded there, so the history begins with what you have. If it is interrupted
it resumes where it stopped on the next start. Turn "Backfill from recorded
calls on start" off to start with an empty history.

## Not in phase 1

Live audio, transcription, talkgroup directory import, trunking messages, call
groups, the other statistics and analytics endpoints, and user accounts are not
implemented. Their endpoints answer 501 with the error code `not_implemented`.

## Working with Trunk Recorder Pro

Three small additions to TRP would make Reporter a better fit (design spec C11):

1. A manifest field for a UI URL, so TRP shows an Open button for plugins that
   serve pages (today only the Website link does this).
2. A keep-running-while-idle flag, so the plugin stays up between recordings
   and still receives `hello` when recording starts.
3. Talkgroup and unit file paths in `hello`, so names can be loaded without
   asking the user for them.

The SDK 0.5 is what Reporter targets today.

## Spec change for tr-engine

Reporter's API adds a `recorded` filter, `recorded`, `not_recorded_reason` and
`decrypted` fields on calls, and the `not_implemented` error code. These need
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
trunk-pro plugin run target\release\reporter.exe <capture dir> --limit N
```

## License

MIT
