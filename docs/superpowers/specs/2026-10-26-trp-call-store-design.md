# TRP call store: design

A SQLite store inside Trunk Recorder Pro that indexes every call it finishes, answers the
history list and paging from it, and opens new questions (by talkgroup, by radio, by time)
to the built-in UI, other interfaces and plugins. This is step 1 of the proposal in
TrunkRecorder/trunk-recorder-pro issue #100; the radio registry (step 2) and the metrics
import (step 3) are not in it, but the store is shaped so they drop in.

The code lives in the TRP repository (`D:\trunk-recorder-pro`, a fork of
TrunkRecorder/trunk-recorder-pro) on a branch off upstream `main`, delivered as two pull
requests. This document and its plan live in the trunk-reporter repository because the TRP
tree is not ours to put planning files in.

## Why

Measured on the owner's recorder after three days (39,460 files, 19,730 call records):
listing the newest 300 calls walks the whole recordings tree (77 ms warm, linear in the
archive, minutes on an SD card cold); every question that is not "the newest page" reads
every record (1.2 s for one talkgroup over three days, about 150 s a year); and nothing can
ask for calls by radio at all. The same questions against an indexed SQLite store answer in
single-digit milliseconds regardless of archive size.

## Goals

1. Every call TRP finishes is a row in `<data>/trunk-pro.sqlite` before its `concluded`
   message is broadcast, with the full record JSON and the columns the queries need.
2. The `history` and `olderCalls` messages are answered from the store with their shapes
   unchanged. The 24-hour paging window and the in-memory list go.
3. Calls already on disk when the store is first created are indexed once, in the
   background, without blocking startup or recording.
4. Rows older than a retention window are deleted. Default 120 days; 0 keeps everything.
5. A new WebSocket command answers calls by system, talkgroup, radio and time range.
6. Plugins and interfaces can read the store two ways: a local plugin by opening the file
   read-only at the path TRP gives it, and anyone by a typed HTTP API on TRP's `/api/`.
7. The schema and the writer take later tables (annotations, the registry, metrics) without
   restructuring: one migration list, one writer with one message type per table.

## Non-goals

- Replacing the WAV, JSON, M4A or frames files. They are written exactly as now.
- Changing any message that exists. The browser-only mode (`web/src/web/engine.worker.ts`,
  the same protocol over the browser's own storage) must keep working untouched.
- Deleting audio or JSON files by age (issue #25). Retention here removes rows only.
- The radio registry, talkgroup and unit state tables, the metrics import, annotations.
  Their tables are not created in this step; the design leaves them room.
- Calls TRP did not record. The store holds what `finish_one` finishes. Monitored,
  skipped and encrypted-but-not-recorded calls are the registry's, in step 2.
- A general SQL endpoint. Reads are typed queries.
- Authentication beyond what TRP's ports have today.

## Anti-goals

- A store whose absence or corruption stops recording. Every store error is logged and
  recording continues; a corrupt file is moved aside and a fresh one started.
- Writes that fight the recorder for the disk: no per-call transaction, no checkpoint
  storms, no vacuum, no rewrite of the whole file.
- A second copy of the truth. The record JSON in the row is the bytes TRP wrote to the
  `.json` file, serialised once.

## Constraints

- Rust edition 2024, rust-version 1.95, as the workspace has them. `rusqlite` with the
  `bundled` feature (bundled SQLite, no system library); every CI target already compiles C.
- The protocol schema `docs/api/protocol.schema.json` is generated from
  `web/src/protocol.ts` by `npm run schema` in `web/` and checked by
  `crates/trunk-pro/src/protocol_tests.rs`. New messages are added in TypeScript first.
- The plugin SDK crate (`crates/trunk-recorder-plugin`, 0.6.0 in the workspace) gains one
  optional field with a serde default. Older plugins ignore it; the API version does not
  change.
- In `trunk-app`, only `protocol.rs` (the new command and answer, `listen_may_send`, one
  capability) and `config.rs` (the `store` section) change. Nothing touches `trunk-core`,
  the session or the DSP path.
- The database module is `crates/trunk-pro/src/db.rs` and the `Ctx` field is `db`. The
  existing `store.rs` (registry and interface stores) and `Ctx.store` (the stats writer)
  keep their names; nothing is renamed.

## The store

### File and connection

`<data>/trunk-pro.sqlite`, `<data>` being `paths::data_dir()`. Opened once in `main.rs`
before `Ctx` is built, and held by `Ctx` as `db: db::Db`. Pragmas on open:
`journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `wal_autocheckpoint=0`
(checkpoints are the writer's, by WAL size: when the WAL passes 64 MB, a `PASSIVE`
checkpoint after the next batch). `temp_store=MEMORY`.

If the file cannot be opened or fails `PRAGMA quick_check` at startup, it is renamed to
`trunk-pro.sqlite.corrupt-<unix s>` with a warning and a new one is created. The backfill
then runs again, so nothing is lost but the time to re-index.

### Writer

One thread, `store-writer`, owning the only writing connection. Everything that writes
sends a `Write` enum variant on a channel; the thread drains the channel, applies the batch
in one transaction, commits, and sleeps until the next item or one second, whichever is
later since the last commit. So at most one commit per second under load, and a quiet
recorder commits each call as it arrives. The `concluded` broadcast for a call happens
after its write is enqueued, not after it is committed: the UI never waits for the disk.
A call is visible to `history` within a second of finishing, which is the batching window.

Variants in this step: `Call { row, record_json, live: bool }`, `CallFiles { path, audio,
json }`, `Retention { before_ms }`, `Checkpoint`. A later table is a new variant and a new
match arm.

The writer is the one place that knows a call's current flags, committed or still in the
batch it is applying, so it also owns the `CallFiles` broadcast: it holds a hub handle,
applies `CallFiles`, and publishes `ToClient::CallFiles` only when the flags changed. That
keeps today's behaviour when a plugin settles a call within the batching window, before
the row is committed, which the archive's settle callback can do.

Live rows upsert on `path` (`ON CONFLICT(path) DO UPDATE`, every column); backfill rows
are `INSERT OR IGNORE`. A live call's `.json` is on disk before its row is committed, so
the backfill can meet it; the live row wins because its flags are the archive's.

Readers use a small pool of read-only connections (`SQLITE_OPEN_READ_ONLY`), one per
concurrent query, borrowed under `spawn_blocking` as the commands already are.

### Schema

`schema_version (version INTEGER)` with one row. Migrations are a list of SQL scripts
applied in order inside one transaction at open; this step is version 1. Adding a table or
a column later is version 2, never an edit of version 1.

`meta (key TEXT PRIMARY KEY, value TEXT)`: `backfill_done_at`, `capture_dir` (the folder
the backfill indexed, so a changed capture folder triggers a new backfill), `recorder_id`
(the install's anonymous ID from directory sharing, when one exists).

`calls`:

| column | type | from |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | rowid; arrival order, not time order (three finish threads, and the backfill) |
| `path` | TEXT NOT NULL UNIQUE | the entry's `path`: relative to the capture folder, no extension |
| `short_name` | TEXT NOT NULL | `record.short_name` |
| `nac` | TEXT | `record.nac` (hex, as the record has it) |
| `site` | TEXT | `record.site`, the site's short name; the record carries no numeric site or system id, so the P25 identity joins arrive with step 2's `systems` and `sites` tables, keyed on `short_name` and `nac` |
| `talkgroup` | INTEGER NOT NULL | `record.talkgroup` |
| `start_ms` | INTEGER NOT NULL | `record.start_time_ms` |
| `stop_ms` | INTEGER NOT NULL | `record.stop_time_ms` |
| `length_ms` | INTEGER NOT NULL | `record.call_length_ms` |
| `freq_hz` | INTEGER NOT NULL | `record.freq` |
| `emergency` | INTEGER NOT NULL | 0/1 |
| `encrypted` | INTEGER NOT NULL | 0/1 |
| `decrypted` | INTEGER NOT NULL | 0/1 |
| `duplex` | INTEGER NOT NULL | 0/1, unit-to-unit |
| `priority` | INTEGER | |
| `audio_type` | TEXT | `record.audio_type` |
| `signal`, `noise`, `snr` | REAL | nullable |
| `clean_voice_pct` | REAL | nullable |
| `audio` | INTEGER NOT NULL | 1 while an audio file is kept (settled by the archive) |
| `json` | INTEGER NOT NULL | 1 while the .json file is kept |
| `recorder` | TEXT | `meta.recorder_id` at write time, nullable |
| `record` | TEXT NOT NULL | the full record JSON, the bytes of the .json file |
| `indexed_ms` | INTEGER NOT NULL | when the row was written |

Indexes: `(start_ms)`, `(short_name, talkgroup, start_ms)`, `(talkgroup, start_ms)`.
Nothing on `path` beyond the unique constraint. The `record` column makes every
`CallEntry` reproducible as `{path, record, audio, json}` without touching the disk.

`call_transmissions` (`record.srcList`): `call_id` (REFERENCES calls ON DELETE CASCADE),
`seq` (order within the call), `unit` (`src`), `start_ms` (the call's `start_time_ms` plus
`pos` seconds), `length_ms` (the next entry's `pos` minus this one's; the last runs to the
call's length), `emergency`, `signal_system`, `tag`. Index `(unit, start_ms)`. This is
what "calls by radio" and, later, talk pairs and the registry's per-radio history read.

`call_frequencies` (`record.freqList`): `call_id` (CASCADE), `seq`, `freq_hz`, `start_ms`
(from `pos` as above), `length_ms` (`len` seconds), `error_count`, `spike_count`. No index
beyond the foreign key's.

Every table that will exist later keys on `calls.id` or `calls.path` and gets its own
migration. Annotations in particular are `(call_id, plugin, kind, written_ms, body)` with
`kind` naming the body's shape; nothing in version 1 has to change for them.

### Writing a call

In `runtime::finish_one`, after the files are written and the entry is built, and before
`publish(Concluded)`: build the row from `f.record` and the already-serialised `json`
string, and enqueue `Write::Call { live: true }`. The `audio` and `json` flags are the
entry's. `runtime::call_files` enqueues `Write::CallFiles` and nothing else; the writer
broadcasts `CallFiles` when the flags changed (see Writer). `recorder` is read from the
directory module's ID file directly, null when the file does not exist, never creating it.

`Ctx.history` (the `VecDeque`), `HISTORY_KEPT` and the `historyLimit` documentation line
go. `HISTORY` (300) stays as the size of the `history` message.

### Backfill and reconcile

At startup, the `history` thread that walks the tree today becomes the backfill thread:

- If `meta.backfill_done_at` is unset or `meta.capture_dir` differs from the config's:
  walk the capture folder (as `scan_history` does), oldest file first, and for each `.json`
  not already in `calls` (by path) and not older than the retention window, parse it and
  enqueue `Write::Call`, 500 at a time with a short yield between batches so the writer's
  live calls are never queued behind the backfill. Then set both meta keys. Progress is
  logged every 10,000 files and at the end.
- Otherwise: reconcile only the day folders of the last two days for every system (the
  folder set `older_calls` computes, old names included), the same way. This catches a
  call whose files were written but whose row was lost to a crash between the write and
  the commit.

The same check runs when recording starts (`Ctx::begin_recording`), not only at startup,
because the capture folder can be changed from Setup while TRP runs.

**A changed capture folder keeps the old rows.** The usual reason to change it is moving
the archive to a bigger disk, and a moved archive keeps every relative path, so the rows
stay right. When the new folder really is a different archive, old rows point at files
that are not there: `/calls/{path}` answers 404 for them, exactly as it does today for a
file deleted by hand, and the flags are left alone. No capture-folder column in v1.

A record on disk without a `.json` (a WAV alone) is not indexed; the index is of records.
The `audio` flag on backfill is whether a `.wav` or `.m4a` exists beside the record, as
`scan_history` decides today; `json` is 1.

### Retention

`store.retentionDays` in the config (new `store` section, `#[serde(default)]`, default
120, 0 = keep everything, documented in `docs/configuration/README.md`). Once an hour, and
once at startup, the writer deletes rows with `start_ms` before the window in batches of
1,000 per transaction until none remain, so a long-stopped recorder catching up never holds
one long transaction. Deleting a call cascades to its transmissions and frequencies. Files
are not touched.

### Reading

`history` on connect: the newest 300 by `start_ms`, as entries.

`olderCalls { before, limit }`: rows with `start_ms < before`, newest first, `limit`
(clamped 1..500); `more` is whether one more exists. `before` as the client sends it (ms).
No window. **Paging takes equal starts whole:** when the page's oldest row shares its
`start_ms` with rows beyond the limit, they are included too, so the next page, asked
before that millisecond, misses none. That is today's rule, kept at millisecond rather
than second resolution.

New command and answer, additive, in `protocol.ts` then `protocol.rs`:

```
callsQuery { id, system?: string, talkgroup?: number, unit?: number,
             from?: number, to?: number, before?: number, limit?: number }
callsResult { id, entries: CallEntry[], more: boolean, before: number }
```

`from`/`to`/`before` are ms. `unit` joins `call_transmissions`. `limit` clamps 1..500.
Newest first; `before` for paging exactly as `olderCalls`, equal starts taken whole. A
listen-only connection may send it. Entries are the same `CallEntry` the other messages
carry. `system` matches the record's `short_name` and, as `older_calls` does, any old name
of that system from `migrate::old_names`, so calls filed under a site's own name before
the config change are found.

`hello.capabilities` gains `calls` (the `Capability` enum in `trunk-app` and
`protocol.ts`), present when the store opened, so an interface can test for the command
before sending it. The browser-only mode does not list it and answers the command the way
it answers every desktop-only command. No built-in UI page uses `callsQuery` in this step;
the command exists for interfaces and for the pages that come with the registry work.

### The HTTP API (second pull request)

Mounted on every door TRP serves the API on (the built-in port and each interface's port,
subject to that door's `ApiAccess`: listen or control both read). Routes are nested at
`/api/v1/` ahead of the existing `/api/{*path}` docs catch-all:

- `GET /api/v1/calls` with the query parameters of `callsQuery` (`system`, `talkgroup`,
  `unit`, `from`, `to`, `before`, `limit`), answering `{ entries, more, before }`.
- `GET /api/v1/calls/{*path}` for one entry by its path (a wildcard, since paths carry
  slashes), answering the `CallEntry` `{ path, record, audio, json }`, 404 when unknown.
- `GET /api/v1/store` answering `{ path, schemaVersion, calls, oldestMs, newestMs,
  retentionDays }`, which is also what a plugin uses to learn the file's location over
  HTTP.

Parameter names match the WebSocket command so the API doc can describe both at once.
Errors are `{ "error": "..." }` with 400 for a bad parameter. The OpenAPI description of
these three routes is added to `docs/api/` beside the protocol docs. The route shapes are
chosen to match trunk-reporter's `/api/v1/calls` closely enough that its pages can later
read from TRP with a base-URL change; exact field names follow TRP's `CallEntry`, not
trunk-reporter's, because the entry is the contract that already exists.

### The hello message (second pull request)

The plugin SDK's `Hello` (`crates/trunk-recorder-plugin/src/protocol.rs`, not the
WebSocket `hello`) gains `store: Option<PathBuf>` (serde default, skipped when none): the
store's path, present when the store opened. A local plugin may open it read-only. The SDK's
documentation says: open with `SQLITE_OPEN_READ_ONLY`, expect WAL mode, read
`schema_version` first and refuse versions you don't know, and never write.

## Pull requests

1. **The store and the call index.** `store.rs` (open, migrate, writer, readers, backfill,
   retention), `runtime.rs` changes, `history`/`olderCalls` from the store, `callsQuery`,
   config `store.retentionDays`, docs for the protocol and the setting, tests.
2. **Reading from outside.** `/api/v1/` routes, `Hello.store`, API docs, SDK doc.

Each is reviewable alone and the first is useful alone.

## Tests the design requires

- A record written through `finish_one` is in `history` within the batching window, and
  its `CallEntry` is byte-identical to today's (same `record` JSON, same flags).
- `olderCalls` paging over a seeded store returns every call exactly once across pages.
- `callsQuery` by talkgroup, by unit (via transmissions), by range, and combined; `more`
  correct at the boundary; limits clamped.
- Backfill indexes a seeded capture folder once, skips what is indexed, respects
  retention, and reconciles the last two days on a later start.
- Retention deletes in batches and cascades; `0` deletes nothing.
- A corrupt file is moved aside and recording continues.
- The writer batches: 1,000 enqueued calls commit in a handful of transactions, not 1,000.
- Protocol schema check passes with the new messages; `listen_may_send` admits `callsQuery`.
- The browser-only mode's tests still pass, untouched.

## Left to the builder

- Whether `db.rs` is one file or a `db/` folder.
- Pool size for readers (four is plenty), channel capacity for the writer (bounded, with
  the backfill yielding when it is more than half full).
- Exact batch sizes and the yield between backfill batches.
- Logging text.

## Owner's decisions

- File at `<data>/trunk-pro.sqlite`.
- Retention 120 days by default, configurable, 0 keeps everything.
- Plugins read both ways: the path in hello, and the HTTP API.
- Store the full record JSON in the row.
- Annotations wait; the schema leaves them a clean migration.

## My calls

- Two pull requests rather than one, split at the process boundary.
- `concluded` is broadcast after enqueue, not after commit; the writer owns the
  `CallFiles` broadcast so a fast plugin settle is never lost.
- Reconcile the last two days' folders on every start and every recording start instead
  of a full walk; a changed capture folder keeps the old rows.
- No system id or numeric site in v1: the record does not carry them. `nac` and the
  site's short name are stored, and step 2's tables bring the identity.
- Paging takes equal milliseconds whole, as today's code takes equal seconds whole.
- `callsQuery.system` honours old system names, as `older_calls` does.
- A `calls` capability in the WebSocket hello, so interfaces can test for the command.
- The `store` config section rather than a field under `recording`.
- Indexes limited to the three the queries use.
- The module is `db.rs` and the field `db`; nothing existing is renamed.
