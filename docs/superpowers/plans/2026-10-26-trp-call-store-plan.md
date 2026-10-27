# TRP Call Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A SQLite index of every call Trunk Recorder Pro finishes, answering the history list, paging and a new calls query from it, readable by plugins and interfaces.

**Architecture:** A `db` module in `trunk-pro` owns one SQLite file: one writer thread batching every write into one transaction per second, a small pool of read-only connections for queries. `finish_one` enqueues a row per call; `history`, `olderCalls` and the new `callsQuery` read rows back as the same `CallEntry` JSON the protocol already carries. A backfill thread indexes the existing capture folder once. The second pull request adds `/api/v1/` routes and the store's path in the plugin hello.

**Tech Stack:** Rust 2024 (workspace rust-version 1.95), `rusqlite = { version = "0.41", features = ["bundled"] }`, axum 0.8, serde_json, the existing `web/` TypeScript protocol and `npm run schema`.

**Spec:** `D:\reporter\docs\superpowers\specs\2026-10-26-trp-call-store-design.md` (in the trunk-reporter repo). The code is in `D:\trunk-recorder-pro`, branch `call-store` off `origin/main`.

## Global Constraints

- No existing protocol message changes shape. `history`, `concluded`, `callFiles`, `olderCalls` are byte-for-byte what they are today (checked by `protocol_tests.rs`).
- The browser-only mode (`web/src/web/engine.worker.ts`, `crates/trunk-web`) is not edited.
- `trunk-core`, `trunk-app/src/session.rs` and the DSP path are not edited. In `trunk-app`, only `protocol.rs` and `config.rs` change.
- The database module is `crates/trunk-pro/src/db/` and the `Ctx` field is `db`. `store.rs`, `statstore.rs` and `Ctx.store` are not renamed.
- The file is `<data>/trunk-pro.sqlite` (`paths::data_dir()`); pragmas `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `wal_autocheckpoint=0`, `temp_store=MEMORY`.
- One writer thread, one transaction per batch, at most one commit per second under load. Readers never block on the writer.
- Every db error is logged and recording continues. Nothing in `finish_one` can fail because of the db.
- Retention default 120 days, `store.retentionDays`, 0 keeps everything.
- Paths stored with `/` separators regardless of platform.
- Commits are small and per task; every task ends green on `cargo test -p trunk-pro` and `cargo clippy -p trunk-pro --all-targets -- -D warnings`.

## Review Focus

1. **A live call's `rel` on Windows carries backslashes while a backfilled path carries slashes**, so the same call could be indexed twice. The row normalises to `/` (Task 2 test `paths_are_stored_with_forward_slashes`).
2. **A call record on disk that the current `CallRecord` cannot parse** (an old Trunk Recorder file, a hand-edited one) must be skipped with one log line, never abort the backfill (Task 5 test `an_unreadable_record_is_skipped`).
3. **A record stamped while the clock was off** (`clock_off`, `start_time_ms` near 0) is still indexed and pages last, never breaks paging (Task 3 test `a_zero_start_pages_last`).
4. **A burst of backfill writes must never delay a live call's row**: the backfill yields when the channel is more than half full, and a live `Write::Call` is always accepted (Task 5 test `backfill_yields_to_live_writes`).
5. **The same path arriving twice** (a plugin hold replayed after a restart, a backfill meeting a live call) must leave one row with the live flags (Task 2 test `a_live_row_wins_over_a_backfilled_one`).

---

### Task 0: Branch and dependency

**Files:**
- Modify: `crates/trunk-pro/Cargo.toml`
- Modify: `Cargo.lock`

- [ ] **Step 1: Branch off upstream main**

```bash
cd D:\trunk-recorder-pro
git fetch origin main
git checkout -b call-store origin/main
```

- [ ] **Step 2: Add the dependency**

In `[dependencies]` of `crates/trunk-pro/Cargo.toml`, after `flate2 = "1"`:

```toml
# The call store (db/): bundled SQLite, no system library.
rusqlite = { version = "0.41", features = ["bundled"] }
```

- [ ] **Step 3: Build**

Run: `cargo build -p trunk-pro`
Expected: compiles (the bundled SQLite C build takes a minute the first time).

- [ ] **Step 4: Commit**

```bash
git add crates/trunk-pro/Cargo.toml Cargo.lock
git commit -m "Call store: rusqlite (bundled)"
```

---

### Task 1: The db module: open, migrate, writer, readers

**Files:**
- Create: `crates/trunk-pro/src/db/mod.rs`
- Create: `crates/trunk-pro/src/db/schema.rs`
- Create: `crates/trunk-pro/src/db/writer.rs`
- Modify: `crates/trunk-pro/src/main.rs` (add `mod db;` beside `mod statstore;`)
- Test: inline `#[cfg(test)] mod tests` in `db/mod.rs`

**Interfaces:**
- Produces:
  - `pub struct Db` (`Clone`; its state behind an `Arc`) with `pub fn open(path: &Path, hub: crate::runtime::Hub) -> Result<Db, String>`, `pub fn path(&self) -> &Path`, `pub fn recorder(&self) -> Option<&str>` (the content of `<data>/directory-recorder-id`, read once at open, trimmed, `None` when the file does not exist; never created here), `pub fn send(&self, w: Write)` (blocks only while the bounded channel is full), `pub fn queued(&self) -> usize`, `pub fn with_reader<T>(&self, f: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>) -> Result<T, String>`, `pub fn flush(&self)` (returns once every write sent before it is committed), `pub fn commits(&self) -> u64`.
  - `pub enum Write { Call { row: CallRow, live: bool }, CallFiles { path: String, audio: bool, json: bool }, Meta { key: String, value: String }, Retention { before_ms: i64 }, Checkpoint, Flush(std::sync::mpsc::Sender<()>) }` — `Call` and `CallFiles` are applied in Task 2; this task gives them `todo!()`-free no-op arms that log.
  - `pub const SCHEMA_VERSION: u32 = 1;`
  - `#[cfg(test)] pub fn for_test(name: &str) -> (Db, PathBuf)` opening a fresh file under `std::env::temp_dir()` named `trunk-pro-db-{name}-{pid}.sqlite` with a hub from `tokio::sync::broadcast::channel(16)`.
- Consumes: `crate::runtime::Hub` (`tokio::sync::broadcast::Sender<Arc<Out>>`), `crate::runtime::publish`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn opening_creates_the_file_and_schema_version_1() {
    let (db, path) = Db::for_test("open");
    assert!(path.exists());
    let v: u32 = db.with_reader(|c| c.query_row("select version from schema_version", [], |r| r.get(0))).unwrap();
    assert_eq!(v, SCHEMA_VERSION);
    let mode: String = db.with_reader(|c| c.query_row("pragma journal_mode", [], |r| r.get(0))).unwrap();
    assert_eq!(mode, "wal");
    let tables: Vec<String> = db.with_reader(|c| c.prepare("select name from sqlite_master where type='table' order by name")?.query_map([], |r| r.get(0))?.collect()).unwrap();
    assert_eq!(tables, ["call_frequencies", "call_transmissions", "calls", "meta", "schema_version"]);
}

#[test]
fn a_corrupt_file_is_moved_aside_and_a_fresh_one_opened() {
    let dir = std::env::temp_dir().join(format!("trunk-pro-db-corrupt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("trunk-pro.sqlite");
    std::fs::write(&path, b"this is not a database").unwrap();
    let (hub, _) = tokio::sync::broadcast::channel(16);
    let db = Db::open(&path, hub).unwrap();
    let v: u32 = db.with_reader(|c| c.query_row("select version from schema_version", [], |r| r.get(0))).unwrap();
    assert_eq!(v, 1);
    let aside = std::fs::read_dir(&dir).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().contains(".corrupt-")).count();
    assert_eq!(aside, 1);
}

#[test]
fn writes_are_batched_into_few_transactions() {
    let (db, _) = Db::for_test("batch");
    for i in 0..1000 {
        db.send(Write::Meta { key: format!("k{i}"), value: "v".into() });
    }
    db.flush();
    let n: u32 = db.with_reader(|c| c.query_row("select count(*) from meta", [], |r| r.get(0))).unwrap();
    assert_eq!(n, 1000);
    assert!(db.commits() <= 10, "{} commits for 1000 writes", db.commits());
}

#[test]
fn a_quiet_write_commits_promptly() {
    let (db, _) = Db::for_test("prompt");
    let t = std::time::Instant::now();
    db.send(Write::Meta { key: "a".into(), value: "1".into() });
    db.flush();
    assert!(t.elapsed() < std::time::Duration::from_millis(1500));
    let v: String = db.with_reader(|c| c.query_row("select value from meta where key='a'", [], |r| r.get(0))).unwrap();
    assert_eq!(v, "1");
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p trunk-pro db::tests`
Expected: compile error, `Db` not defined.

- [ ] **Step 3: Implement `db/schema.rs`**

`pub const MIGRATIONS: &[&str]` with one script, version 1, creating in this order: `schema_version(version INTEGER NOT NULL)`, `meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`, `calls`, `call_transmissions`, `call_frequencies`, the three indexes. Columns exactly as the spec's table: `calls(id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE, short_name TEXT NOT NULL, nac TEXT, site TEXT, talkgroup INTEGER NOT NULL, start_ms INTEGER NOT NULL, stop_ms INTEGER NOT NULL, length_ms INTEGER NOT NULL, freq_hz INTEGER NOT NULL, emergency INTEGER NOT NULL, encrypted INTEGER NOT NULL, decrypted INTEGER NOT NULL, duplex INTEGER NOT NULL, priority INTEGER, audio_type TEXT, signal REAL, noise REAL, snr REAL, clean_voice_pct REAL, audio INTEGER NOT NULL, json INTEGER NOT NULL, recorder TEXT, record TEXT NOT NULL, indexed_ms INTEGER NOT NULL)`; `call_transmissions(call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE, seq INTEGER NOT NULL, unit INTEGER NOT NULL, start_ms INTEGER NOT NULL, length_ms INTEGER NOT NULL, emergency INTEGER NOT NULL, signal_system TEXT, tag TEXT, PRIMARY KEY(call_id, seq))`; `call_frequencies(call_id INTEGER NOT NULL REFERENCES calls(id) ON DELETE CASCADE, seq INTEGER NOT NULL, freq_hz INTEGER NOT NULL, start_ms INTEGER NOT NULL, length_ms INTEGER NOT NULL, error_count INTEGER NOT NULL, spike_count INTEGER NOT NULL, PRIMARY KEY(call_id, seq))`; indexes `calls_start(start_ms)`, `calls_system_tg_start(short_name, talkgroup, start_ms)`, `calls_tg_start(talkgroup, start_ms)`, `tx_unit_start(unit, start_ms)` on `call_transmissions`.

`pub fn migrate(conn: &rusqlite::Connection) -> rusqlite::Result<()>`: read the version (0 when the table is missing), apply each script past it inside one transaction, set the version.

- [ ] **Step 4: Implement `db/writer.rs`**

`pub fn spawn(conn: rusqlite::Connection, hub: Hub, rx: std::sync::mpsc::Receiver<Write>, commits: Arc<AtomicU64>)` on a thread named `db-writer`. Loop: block on `recv` (exit when the channel closes); start a transaction; apply that write and every write `try_recv` yields; commit; bump `commits`; answer every `Flush` sender collected in the batch after the commit; if less than a second has passed since the previous commit, sleep the remainder before blocking on `recv` again (so a stream of writes commits once a second, and a lone write commits at once). After each commit, if the WAL file beside the database is larger than 64 MiB, run `pragma wal_checkpoint(PASSIVE)`. Every apply error is `log::warn!`ed and the batch continues. `Meta` upserts.

- [ ] **Step 5: Implement `db/mod.rs`**

`open`: create the parent folder; if the file exists and either `Connection::open` fails or `pragma quick_check` is not `ok`, rename it to `trunk-pro.sqlite.corrupt-<unix s>` (and its `-wal`/`-shm` siblings) with `log::warn!`, then open fresh. Set the pragmas, `migrate`, open the reader pool (four `Connection::open_with_flags(.., SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX)` behind a `Mutex<Vec<Connection>>`; `with_reader` pops one, or opens another if empty, and pushes it back). Channel: `std::sync::mpsc::sync_channel(4096)`. `queued` from an `Arc<AtomicUsize>` the writer decrements. `flush` sends `Write::Flush` and waits.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p trunk-pro db::tests`
Expected: 4 passed.

- [ ] **Step 7: Commit**

```bash
git add crates/trunk-pro/src/db crates/trunk-pro/src/main.rs
git commit -m "Call store: the db module (open, migrate, one writer, readers)"
```

---

### Task 2: Rows: a call record in, a call entry out

**Files:**
- Create: `crates/trunk-pro/src/db/row.rs`
- Modify: `crates/trunk-pro/src/db/writer.rs` (apply `Call` and `CallFiles`)
- Modify: `crates/trunk-pro/src/db/mod.rs` (re-exports)
- Test: inline in `db/row.rs` and `db/writer.rs`

**Interfaces:**
- Produces:
  - `pub struct CallRow { pub path: String, pub short_name, nac, site, talkgroup, start_ms, stop_ms, length_ms, freq_hz, emergency, encrypted, decrypted, duplex, priority, audio_type, signal, noise, snr, clean_voice_pct, audio: bool, json: bool, recorder: Option<String>, record: String, indexed_ms: i64, pub transmissions: Vec<TxRow>, pub frequencies: Vec<FreqRow> }`.
  - `pub fn call_row(rel: &str, record: &trunk_recorder_plugin::CallRecord, record_json: &str, audio: bool, json: bool, recorder: Option<&str>, now_ms: i64) -> CallRow` — `path` is `rel` with every `\` replaced by `/`; transmissions and frequencies derived as the spec says (`start_ms = start_time_ms + pos*1000`, a transmission's `length_ms` = next `pos` minus this one, the last to `call_length_ms`; a frequency's from `len`).
  - `pub fn entry(path: &str, record: &str, audio: bool, json: bool) -> serde_json::Value` building `{"path", "record", "audio", "json"}` with `record` parsed from the stored JSON.
  - Writer: `Call { live: true }` upserts every column on `path` conflict and replaces the call's transmissions and frequencies; `Call { live: false }` is `INSERT OR IGNORE` (children only when the row was inserted). `CallFiles` updates the two flags where `path` matches and, when either changed, calls `crate::runtime::publish(&hub, ToClient::CallFiles { path, audio, json })`.
- Consumes: Task 1's `Db`, `Write`.

- [ ] **Step 1: Write the failing tests**

```rust
fn record(tg: u32, start_ms: i64) -> trunk_recorder_plugin::CallRecord {
    let mut r = trunk_recorder_plugin::CallRecord::default();
    r.short_name = "ohmarcs".into();
    r.talkgroup = tg;
    r.start_time_ms = start_ms;
    r.start_time = start_ms / 1000;
    r.stop_time_ms = start_ms + 4200;
    r.call_length_ms = 4200;
    r.call_length = 4.2;
    r.freq = 851_350_000;
    r.nac = "35C".into();
    r.site = "ohmarcs-1-13".into();
    r.src_list = vec![
        trunk_recorder_plugin::SrcEntry { src: 9001644, time: start_ms / 1000, pos: 0.0, emergency: false, ..Default::default() },
        trunk_recorder_plugin::SrcEntry { src: 8344139, time: start_ms / 1000 + 2, pos: 2.5, emergency: true, ..Default::default() },
    ];
    r.freq_list = vec![trunk_recorder_plugin::FreqEntry { freq: 851_350_000, time: start_ms / 1000, pos: 0.0, len: 4.2, error_count: 3, spike_count: 1 }];
    r
}

#[test]
fn a_row_carries_the_record_and_its_lists() {
    let r = record(9179, 1_700_000_000_000);
    let json = r.to_json();
    let row = call_row("ohmarcs/2026/10/26/9179-1700000000_851350000", &r, &json, true, true, Some("abc"), 5);
    assert_eq!(row.talkgroup, 9179);
    assert_eq!((row.start_ms, row.stop_ms, row.length_ms), (1_700_000_000_000, 1_700_000_004_200, 4200));
    assert_eq!(row.nac.as_deref(), Some("35C"));
    assert_eq!(row.site.as_deref(), Some("ohmarcs-1-13"));
    assert_eq!(row.record, json);
    assert_eq!(row.recorder.as_deref(), Some("abc"));
    let tx: Vec<(i64, i64, i64, bool)> = row.transmissions.iter().map(|t| (t.unit, t.start_ms, t.length_ms, t.emergency)).collect();
    assert_eq!(tx, [(9001644, 1_700_000_000_000, 2500, false), (8344139, 1_700_000_002_500, 1700, true)]);
    assert_eq!((row.frequencies[0].freq_hz, row.frequencies[0].length_ms, row.frequencies[0].error_count), (851_350_000, 4200, 3));
}

#[test]
fn paths_are_stored_with_forward_slashes() {
    let r = record(1, 1000);
    let row = call_row("ohmarcs\\2026\\10\\26\\1-1_851350000", &r, &r.to_json(), true, true, None, 0);
    assert_eq!(row.path, "ohmarcs/2026/10/26/1-1_851350000");
}

#[test]
fn an_entry_is_the_call_entry_the_protocol_carries() {
    let r = record(9179, 1_700_000_000_000);
    let json = r.to_json();
    let e = entry("ohmarcs/x", &json, true, false);
    let mut want = trunk_app::session::call_entry("ohmarcs/x", &r);
    want["audio"] = json!(true);
    want["json"] = json!(false);
    assert_eq!(e, want);
}

// in writer.rs tests
#[test]
fn a_live_row_wins_over_a_backfilled_one() {
    let (db, _) = Db::for_test("upsert");
    let r = record(9179, 1_700_000_000_000);
    let json = r.to_json();
    db.send(Write::Call { row: call_row("ohmarcs/a", &r, &json, true, true, None, 1), live: true });
    db.send(Write::Call { row: call_row("ohmarcs/a", &r, &json, false, false, None, 2), live: false });
    db.flush();
    let (n, audio): (u32, bool) = db.with_reader(|c| c.query_row("select count(*), max(audio) from calls where path='ohmarcs/a'", [], |r| Ok((r.get(0)?, r.get(1)?)))).unwrap();
    assert_eq!((n, audio), (1, true), "the backfilled copy is ignored");
    db.send(Write::Call { row: call_row("ohmarcs/a", &r, &json, false, true, None, 3), live: true });
    db.flush();
    let audio: bool = db.with_reader(|c| c.query_row("select audio from calls where path='ohmarcs/a'", [], |r| r.get(0))).unwrap();
    assert!(!audio, "a live copy replaces");
    let tx: u32 = db.with_reader(|c| c.query_row("select count(*) from call_transmissions", [], |r| r.get(0))).unwrap();
    assert_eq!(tx, 2, "children replaced, not duplicated");
}

#[test]
fn call_files_updates_the_flags_and_says_so_once() {
    let (hub, mut rx) = tokio::sync::broadcast::channel(16);
    let dir = std::env::temp_dir().join(format!("trunk-pro-db-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Db::open(&dir.join("trunk-pro.sqlite"), hub).unwrap();
    let r = record(1, 1000);
    db.send(Write::Call { row: call_row("ohmarcs/b", &r, &r.to_json(), true, true, None, 1), live: true });
    db.send(Write::CallFiles { path: "ohmarcs/b".into(), audio: true, json: true });  // same batch, unchanged
    db.send(Write::CallFiles { path: "ohmarcs/b".into(), audio: false, json: true }); // same batch, changed
    db.flush();
    let m = rx.try_recv().expect("one callFiles");
    assert!(m.text_for_listen().contains("\"callFiles\"") && m.text_for_listen().contains("\"audio\":false"));
    assert!(rx.try_recv().is_err(), "no message for the unchanged update");
}
```

(`text_for_listen` names whatever accessor `Out` has for its listen text; use the existing one.)

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p trunk-pro db::`
Expected: compile errors for `call_row`, `entry`.

- [ ] **Step 3: Implement `row.rs` and the writer arms**

The writer's `Call` arm: `INSERT INTO calls (...) VALUES (...) ON CONFLICT(path) DO UPDATE SET <every column but id and path> = excluded.<column>` for live, `INSERT OR IGNORE` for backfill; take `changes()` to know whether the row landed; `DELETE FROM call_transmissions WHERE call_id = ?` and the same for frequencies when live, then insert the children with the call's id (`SELECT id FROM calls WHERE path = ?`). The `CallFiles` arm: `SELECT audio, json FROM calls WHERE path = ?` (inside the same transaction, so a row from this batch is visible), update when different, publish after the batch commits (collect the messages, send them after `commit`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p trunk-pro db::`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/trunk-pro/src/db
git commit -m "Call store: rows from call records, entries back, flags settled by the writer"
```

---

### Task 3: Reads: newest, older, query

**Files:**
- Create: `crates/trunk-pro/src/db/read.rs`
- Modify: `crates/trunk-pro/src/db/mod.rs` (re-exports)
- Test: inline in `db/read.rs`

**Interfaces:**
- Produces:
  - `pub fn newest(conn: &Connection, limit: usize) -> rusqlite::Result<Vec<Value>>` — entries newest first by `start_ms`, then `id` desc.
  - `pub struct Page { pub entries: Vec<Value>, pub more: bool, pub before: i64 }` where `before` is the `start_ms` of the oldest entry returned (or the asked `before` when empty).
  - `pub fn older(conn: &Connection, before_ms: i64, limit: usize) -> rusqlite::Result<Page>` — `start_ms < before_ms`, newest first, `limit` rows then every further row sharing the oldest row's `start_ms`; `more` is whether any row has `start_ms <` that oldest value.
  - `pub struct CallsFilter { pub systems: Vec<String>, pub talkgroup: Option<u32>, pub unit: Option<i64>, pub from_ms: Option<i64>, pub to_ms: Option<i64>, pub before_ms: Option<i64>, pub limit: usize }`.
  - `pub fn query(conn: &Connection, f: &CallsFilter) -> rusqlite::Result<Page>` — same paging rule; `systems` is an `IN` list on `short_name` (empty = any); `unit` is `EXISTS (select 1 from call_transmissions t where t.call_id = calls.id and t.unit = ?)`; `from_ms <= start_ms < to_ms`.
  - `pub fn one(conn: &Connection, path: &str) -> rusqlite::Result<Option<Value>>`.
- Consumes: Task 2's `entry`, `call_row`.

- [ ] **Step 1: Write the failing tests**

```rust
fn seeded(name: &str) -> Db {
    let (db, _) = Db::for_test(name);
    // 12 calls: talkgroup 2 (odd i) and 1 (even i), starts i*1000, except call 6 shares call 5's start 5000 and call 12 has start 0 (clock off); unit 77 talks on calls 3 (talkgroup 2) and 8 (talkgroup 1).
    for i in 1..=12u32 {
        let start = match i { 6 => 5000, 12 => 0, _ => i as i64 * 1000 };
        let mut r = super::row::tests::record(1 + i % 2, start);
        if i == 3 || i == 8 { r.src_list[0].src = 77; }
        db.send(Write::Call { row: call_row(&format!("s/{i}"), &r, &r.to_json(), true, true, None, 0), live: true });
    }
    db.flush();
    db
}

#[test]
fn newest_is_newest_first() {
    let db = seeded("newest");
    let v = db.with_reader(|c| newest(c, 3)).unwrap();
    let paths: Vec<&str> = v.iter().map(|e| e["path"].as_str().unwrap()).collect();
    assert_eq!(paths, ["s/11", "s/10", "s/9"]);
}

#[test]
fn paging_returns_every_call_exactly_once() {
    let db = seeded("paging");
    let mut seen = Vec::new();
    let mut before = i64::MAX;
    loop {
        let p = db.with_reader(|c| older(c, before, 4)).unwrap();
        seen.extend(p.entries.iter().map(|e| e["path"].as_str().unwrap().to_string()));
        if !p.more { break; }
        before = p.before;
    }
    let mut want: Vec<String> = (1..=12).map(|i| format!("s/{i}")).collect();
    seen.sort(); want.sort();
    assert_eq!(seen, want);
}

#[test]
fn equal_starts_are_taken_whole() {
    let db = seeded("whole");
    let p = db.with_reader(|c| older(c, 7001, 1)).unwrap();  // limit 1: 7000 is s/7 alone
    let paths: Vec<&str> = p.entries.iter().map(|e| e["path"].as_str().unwrap()).collect();
    assert_eq!(paths, ["s/7"], "7000 is alone");
    let p = db.with_reader(|c| older(c, 5001, 1)).unwrap();
    assert_eq!(p.entries.len(), 2, "5000 is shared, both come");
    assert_eq!(p.before, 5000);
    assert!(p.more);
}

#[test]
fn a_zero_start_pages_last() {
    let db = seeded("zero");
    let p = db.with_reader(|c| older(c, 1, 10)).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0]["path"], "s/12");
    assert!(!p.more);
}

#[test]
fn query_filters_by_talkgroup_unit_range_and_system() {
    let db = seeded("query");
    let f = |tg, unit, from, to, systems: &[&str]| CallsFilter { systems: systems.iter().map(|s| s.to_string()).collect(), talkgroup: tg, unit, from_ms: from, to_ms: to, before_ms: None, limit: 100 };
    let n = |f: &CallsFilter| db.with_reader(|c| query(c, f)).unwrap().entries.len();
    assert_eq!(n(&f(Some(1), None, None, None, &[])), 6);
    assert_eq!(n(&f(None, Some(77), None, None, &[])), 2);
    assert_eq!(n(&f(Some(2), Some(77), None, None, &[])), 1);
    assert_eq!(n(&f(None, None, Some(3000), Some(6000), &[])), 4, "3000, 4000, 5000, 5000");
    assert_eq!(n(&f(None, None, None, None, &["ohmarcs"])), 12);
    assert_eq!(n(&f(None, None, None, None, &["other"])), 0);
    let p = db.with_reader(|c| query(c, &CallsFilter { limit: 5, ..f(None, None, None, None, &[]) })).unwrap();
    assert!(p.more);
    assert_eq!(p.entries.len(), 5);
}

#[test]
fn one_by_path() {
    let db = seeded("one");
    assert_eq!(db.with_reader(|c| one(c, "s/4")).unwrap().unwrap()["record"]["talkgroup"], 1);
    assert!(db.with_reader(|c| one(c, "s/99")).unwrap().is_none());
}
```

(`row::tests::record` becomes `pub(crate)` under `#[cfg(test)]` so the read tests can use it.)

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p trunk-pro db::read`
Expected: compile errors.

- [ ] **Step 3: Implement `read.rs`**

One SQL builder for `older` and `query` (the `older` case is a filter with only `before_ms`). Fetch `limit + 1` rows ordered by `start_ms DESC, id DESC`; if more than `limit` came, note the oldest `start_ms` among the first `limit`, keep every fetched row at or above it, then one more query for rows at exactly that `start_ms` not yet taken (the shared-millisecond tail) and `more = exists(start_ms < oldest)`. When `limit + 1` did not come, `more = false`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p trunk-pro db::`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/trunk-pro/src/db
git commit -m "Call store: newest, older and filtered reads with whole-millisecond paging"
```

---

### Task 4: Wire the store into the recorder

**Files:**
- Modify: `crates/trunk-pro/src/runtime.rs` (`Ctx`, `finish_one`, `call_files`, `older_calls`, remove `HISTORY_KEPT` and the `VecDeque`)
- Modify: `crates/trunk-pro/src/main.rs` (open the db before `Ctx`)
- Modify: `crates/trunk-pro/src/server.rs` (the `history` message on connect)
- Modify: `crates/trunk-pro/src/protocol_tests.rs` (the `Ctx` the tests build)
- Modify: `docs/api/README.md` (drop the `historyLimit` row, line 214)
- Test: `crates/trunk-pro/src/runtime.rs` inline, `protocol_tests.rs`

**Interfaces:**
- Consumes: `Db`, `Write`, `read::newest`, `read::older`, `row::call_row`, `row::recorder_id`.
- Produces: `Ctx.db: db::Db`; `runtime::older_calls(ctx, before_ms, limit) -> ToClient` unchanged in signature, now reading the store; `runtime::history(ctx) -> ToClient` (the `history` message, newest 300).

- [ ] **Step 1: Write the failing test**

In `runtime.rs` tests, with whatever helper the existing tests use to build a `Ctx` (if none, a `#[cfg(test)] pub fn test_ctx(capture_dir: &Path) -> Arc<Ctx>` added here and reused by `protocol_tests.rs`). Its config has one system `ohmarcs` with one site whose `site_name` renders as `ohmarcs-butler` (Task 7's old-names test relies on it; name the site whatever `System::site_name` needs for that):

```rust
#[test]
fn a_finished_call_is_in_history_and_pages() {
    let dir = std::env::temp_dir().join(format!("trunk-pro-finish-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ctx = test_ctx(&dir);
    let r = crate::db::row::tests::record(9179, 1_700_000_000_000);
    let entry = index_call(&ctx, "ohmarcs/2026/10/26/9179-1700000000_851350000", &r, true, true);
    ctx.db.flush();
    let ToClient::History { entries } = history(&ctx) else { panic!() };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], entry, "the entry on the wire is the entry in the store");
    let ToClient::OlderCalls { entries, more, .. } = older_calls(&ctx, i64::MAX, 100) else { panic!() };
    assert_eq!((entries.len(), more), (1, false));
}
```

`index_call(ctx, rel, record, audio, json) -> Value` is the piece of `finish_one` that builds the entry and enqueues the row, extracted so it can be tested without a WAV; `finish_one` calls it.

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p trunk-pro runtime::tests::a_finished_call`
Expected: compile error.

- [ ] **Step 3: Implement**

- `Ctx`: replace `pub history: Mutex<VecDeque<Value>>` with `pub db: crate::db::Db`. Remove `HISTORY_KEPT`. Keep `pub const HISTORY: usize = 300`.
- `main.rs`: `let db = db::Db::open(&paths::data_dir().join("trunk-pro.sqlite"), hub.clone()).unwrap_or_else(|e| fatal(&e))` before the `Ctx` literal, `history: Mutex::new(VecDeque::new())` replaced by `db`. Log the path with the other startup lines.
- `finish_one`: after `entry["json"] = json!(keep_json);` call `index_call`'s body: `ctx.db.send(Write::Call { row: call_row(&f.rel, &f.record, &json, keep_audio, keep_json, ctx.db.recorder(), unix_ms), live: true })`; the `publish(Concluded)` stays after it; the block that pushed onto `ctx.history` goes.
- `call_files`: body becomes `ctx.db.send(Write::CallFiles { path: rel.replace('\\', "/"), audio, json })`. The publish moves to the writer (Task 2).
- `older_calls`: `ctx.db.with_reader(|c| read::older(c, before_ms, limit))` into `ToClient::OlderCalls { before: before_ms, entries, more }`; on error log and answer empty with `more: false`. Delete the folder walk, `OLDER_WINDOW_MS`, `start_of`.
- `history(ctx)`: `read::newest(c, HISTORY)` into `ToClient::History { entries }`.
- `server.rs` line 562: `m.push(runtime::history(&ctx))` (it runs inside the existing blocking context or gets `spawn_blocking` like the commands; it must not run a query on the async thread).
- `load_history` and `scan_history` stay for now (Task 5 replaces them).
- `docs/api/README.md`: remove the `historyLimit` row.

- [ ] **Step 4: Run the whole crate's tests**

Run: `cargo test -p trunk-pro`
Expected: pass; `protocol_tests::the_servers_messages` still sees `history` with the same shape.

- [ ] **Step 5: Commit**

```bash
git add -A crates/trunk-pro/src docs/api/README.md
git commit -m "Call store: finished calls indexed; history and older calls answered from the store"
```

---

### Task 5: Backfill and reconcile

**Files:**
- Create: `crates/trunk-pro/src/db/backfill.rs`
- Modify: `crates/trunk-pro/src/runtime.rs` (`load_history` → `db::backfill::start`, remove `scan_history`; call `start` from `begin_recording` too)
- Modify: `crates/trunk-pro/src/main.rs` (the call site)
- Test: inline in `db/backfill.rs`

**Interfaces:**
- Produces:
  - `pub fn start(ctx: Arc<Ctx>)`: spawns the `db-backfill` thread running `run`.
  - `pub fn run(db: &Db, capture_dir: &Path, recent_folders: &[PathBuf], retention_days: u32, now_ms: i64) -> Stats` where `Stats { walked: usize, indexed: usize, skipped: usize }`. Full walk when `meta.backfill_done_at` is unset or `meta.capture_dir` differs from `capture_dir`; else only `recent_folders`.
  - `pub fn recent_folders(cfg: &Config, now_s: i64) -> Vec<PathBuf>`: the day folders of the last two days for every system name and old name, as `older_calls` computed them (lift that loop here).
- Consumes: `Db`, `Write`, `call_row`; `trunk_app::filename::default_path`, `migrate::old_names`.

- [ ] **Step 1: Write the failing tests**

```rust
fn capture(name: &str, calls: &[(&str, u32, i64)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trunk-pro-backfill-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for (rel, tg, start) in calls {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let r = crate::db::row::tests::record(*tg, *start);
        std::fs::write(p.with_extension("json"), r.to_json()).unwrap();
        std::fs::write(p.with_extension("wav"), b"RIFF").unwrap();
    }
    dir
}

#[test]
fn a_first_run_indexes_the_folder_once() {
    let dir = capture("first", &[("s/2026/10/25/1-1_1", 1, 1000), ("s/2026/10/26/2-2_1", 2, 2000)]);
    let (db, _) = Db::for_test("backfill-first");
    let s = run(&db, &dir, &[], 0, 10_000);
    db.flush();
    assert_eq!((s.walked, s.indexed), (2, 2));
    let n: u32 = db.with_reader(|c| c.query_row("select count(*) from calls where audio = 1", [], |r| r.get(0))).unwrap();
    assert_eq!(n, 2);
    let s = run(&db, &dir, &[], 0, 10_000);
    assert_eq!(s.walked, 0, "the second run only reconciles the recent folders, and none were given");
}

#[test]
fn a_changed_capture_folder_is_walked_again() {
    let a = capture("change-a", &[("s/2026/10/25/1-1_1", 1, 1000)]);
    let b = capture("change-b", &[("s/2026/10/25/3-3_1", 3, 3000)]);
    let (db, _) = Db::for_test("backfill-change");
    run(&db, &a, &[], 0, 10_000);
    let s = run(&db, &b, &[], 0, 10_000);
    db.flush();
    assert_eq!(s.indexed, 1);
    let n: u32 = db.with_reader(|c| c.query_row("select count(*) from calls", [], |r| r.get(0))).unwrap();
    assert_eq!(n, 2, "the old rows stay");
}

#[test]
fn retention_bounds_the_backfill() {
    let now = 100 * 86_400_000;
    let dir = capture("retention", &[("s/old", 1, now - 200 * 86_400_000), ("s/new", 1, now - 10 * 86_400_000)]);
    let (db, _) = Db::for_test("backfill-retention");
    let s = run(&db, &dir, &[], 120, now);
    assert_eq!((s.indexed, s.skipped), (1, 1));
}

#[test]
fn an_unreadable_record_is_skipped() {
    let dir = capture("bad", &[("s/good", 1, 1000)]);
    std::fs::write(dir.join("s/bad.json"), b"{ not json").unwrap();
    let (db, _) = Db::for_test("backfill-bad");
    let s = run(&db, &dir, &[], 0, 10_000);
    assert_eq!((s.indexed, s.skipped), (1, 1));
}

#[test]
fn reconcile_indexes_only_the_recent_folders() {
    let dir = capture("recent", &[("s/2026/10/20/1-1_1", 1, 1000), ("s/2026/10/26/2-2_1", 2, 2000)]);
    let (db, _) = Db::for_test("backfill-recent");
    run(&db, &dir, &[], 0, 10_000);                       // full walk
    std::fs::write(dir.join("s/2026/10/26/3-3_1.json"), crate::db::row::tests::record(3, 3000).to_json()).unwrap();
    std::fs::write(dir.join("s/2026/10/20/4-4_1.json"), crate::db::row::tests::record(4, 4000).to_json()).unwrap();
    let s = run(&db, &dir, &[dir.join("s/2026/10/26")], 0, 10_000);
    db.flush();
    assert_eq!(s.indexed, 1, "only the recent folder's new call");
}

#[test]
fn backfill_yields_to_live_writes() {
    let calls: Vec<(String, u32, i64)> = (0..3000).map(|i| (format!("s/{i}"), 1, i as i64)).collect();
    let refs: Vec<(&str, u32, i64)> = calls.iter().map(|(p, t, s)| (p.as_str(), *t, *s)).collect();
    let dir = capture("yield", &refs);
    let (db, _) = Db::for_test("backfill-yield");
    let t = std::thread::spawn({ let db = db.clone(); let dir = dir.clone(); move || run(&db, &dir, &[], 0, 10_000) });
    std::thread::sleep(std::time::Duration::from_millis(50));
    let started = std::time::Instant::now();
    let r = crate::db::row::tests::record(9, 99);
    db.send(Write::Call { row: call_row("live/1", &r, &r.to_json(), true, true, None, 0), live: true });
    assert!(started.elapsed() < std::time::Duration::from_millis(500), "a live write is never queued behind the backfill");
    t.join().unwrap();
    db.flush();
    assert!(db.with_reader(|c| one(c, "live/1")).unwrap().is_some());
}
```

(`Db` is `Clone` from Task 1.)

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p trunk-pro db::backfill`
Expected: compile errors.

- [ ] **Step 3: Implement `backfill.rs`**

`run`: decide full walk vs recent from `meta` (read with a reader). Collect `.json` files (the full walk as `scan_history` walked; the recent case lists each folder), sort by path, and for each: skip when `path` is in `calls` (one `SELECT 1 FROM calls WHERE path = ?` per file, through a reader held for the loop), skip when the record's `start_time_ms` is older than `now_ms - retention_days * 86_400_000` (when `retention_days > 0`), skip with `log::debug!` when it fails to parse as `CallRecord`; else `Write::Call { live: false }` with `audio` = a `.wav` or `.m4a` beside it, `json: true`. Every 200 files, while `db.queued() > 2048`, sleep 20 ms. Log every 10,000 files and a summary at the end. After a full walk, `Write::Meta` for `backfill_done_at` and `capture_dir`.

`start`: reads the config for the capture folder and `recent_folders`, spawns the thread. Called from `main.rs` where `load_history` was, and at the end of `Ctx::begin_recording`.

Remove `scan_history`, `load_history`, and `older_calls`'s folder arithmetic (moved into `recent_folders`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p trunk-pro`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add -A crates/trunk-pro/src
git commit -m "Call store: backfill the capture folder once, reconcile the last two days on every start"
```

---

### Task 6: Retention and the `store` config section

**Files:**
- Modify: `crates/trunk-app/src/config.rs` (`StoreSettings`, `Config.store`)
- Modify: `web/src/protocol.ts` (`Config.store?`, `StoreSettings`)
- Modify: `crates/trunk-pro/src/db/writer.rs` (`Retention` arm in batches)
- Modify: `crates/trunk-pro/src/db/mod.rs` (`pub fn start_retention(&self, days: u32)` spawning the hourly tick)
- Modify: `crates/trunk-pro/src/main.rs` (call it after open)
- Modify: `docs/configuration/README.md` (a `store` entry under Top level)
- Test: inline in `db/writer.rs`, `config.rs`

**Interfaces:**
- Produces: `pub struct StoreSettings { pub retention_days: u32 }` with `Default` of 120, `#[serde(rename_all = "camelCase", default)]`, field `pub store: StoreSettings` on `Config` with the same serde attributes as `monitor`. TS: `store?: StoreSettings` and `export interface StoreSettings { /** Days of call records kept in the call store; 0 keeps everything. */ retentionDays: number }`.
- Consumes: `Write::Retention`.

- [ ] **Step 1: Write the failing tests**

```rust
// config.rs
#[test]
fn the_store_section_defaults_to_120_days() {
    let c = Config::from_value(serde_json::json!({})).unwrap();
    assert_eq!(c.store.retention_days, 120);
    let c = Config::from_value(serde_json::json!({ "store": { "retentionDays": 0 } })).unwrap();
    assert_eq!(c.store.retention_days, 0);
}

// writer.rs
#[test]
fn retention_deletes_old_rows_in_batches_and_cascades() {
    let (db, _) = Db::for_test("retention");
    for i in 0..2500u32 {
        let r = crate::db::row::tests::record(1, i as i64);
        db.send(Write::Call { row: call_row(&format!("s/{i}"), &r, &r.to_json(), true, true, None, 0), live: true });
    }
    db.flush();
    let before = db.commits();
    db.send(Write::Retention { before_ms: 2200 });
    db.flush();
    let (n, tx): (u32, u32) = db.with_reader(|c| Ok((c.query_row("select count(*) from calls", [], |r| r.get(0))?, c.query_row("select count(*) from call_transmissions", [], |r| r.get(0))?))).unwrap();
    assert_eq!((n, tx), (300, 600));
    assert!(db.commits() - before >= 3, "2200 rows in batches of 1000: at least three commits");
    db.send(Write::Retention { before_ms: 0 });
    db.flush();
    let n: u32 = db.with_reader(|c| c.query_row("select count(*) from calls", [], |r| r.get(0))).unwrap();
    assert_eq!(n, 300, "nothing before 0");
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p trunk-app the_store_section && cargo test -p trunk-pro retention_deletes`
Expected: compile errors.

- [ ] **Step 3: Implement**

`Retention` arm: loop `DELETE FROM calls WHERE id IN (SELECT id FROM calls WHERE start_ms < ? ORDER BY start_ms LIMIT 1000)` in its own transaction until `changes() == 0`. `start_retention(days)`: when `days > 0`, a thread `db-retention` sending `Retention { before_ms: now - days * 86_400_000 }` once at start and every hour. `main.rs`: `db.start_retention(cfg.store.retention_days)` after open. Docs: one row in the Top level table and a short paragraph: what is kept, what 0 means, that files are not deleted (pointing at issue #25).

- [ ] **Step 4: Regenerate the schema and run everything**

Run: `cd web && npm run schema && cd .. && cargo test -p trunk-app -p trunk-pro`
Expected: pass; `protocol_tests` accepts a config with `store`.

- [ ] **Step 5: Commit**

```bash
git add -A crates docs web/src/protocol.ts docs/api/protocol.schema.json
git commit -m "Call store: retention (store.retentionDays, 120 by default, 0 keeps everything)"
```

---

### Task 7: The `callsQuery` command and the `calls` capability

**Files:**
- Modify: `web/src/protocol.ts` (`ToRecorder` gains `callsQuery`, `FromRecorder` gains `callsResult`, `Capability` gains `"calls"`)
- Modify: `crates/trunk-app/src/protocol.rs` (`Command::CallsQuery`, `ToClient::CallsResult`, `Capability::Calls`, `listen_may_send`)
- Modify: `crates/trunk-pro/src/server.rs` (dispatch; the capability in `hello` is listed when `ctx.db` opened, which it always has in this design, so `Capability::ALL` stays)
- Modify: `crates/trunk-pro/src/runtime.rs` (`calls_query(ctx, q) -> ToClient`)
- Modify: `docs/api/README.md` (Commands table, Messages table, a paragraph under Recorded calls)
- Modify: `docs/api/protocol.schema.json` (generated)
- Test: `protocol_tests.rs`, `runtime.rs`

**Interfaces:**
- Produces:
  - TS: `| { type: "callsQuery"; id: number; system?: string; talkgroup?: number; unit?: number; from?: number; to?: number; before?: number; limit?: number }` with the doc comment "Recorded calls from the call store, newest first: by system (its short name), talkgroup, radio, and start time (Unix ms, `from` inclusive to `to` exclusive); `before` pages like olderCalls; `limit` 1..500 (100)."
  - TS: `| { type: "callsResult"; id: number; entries: CallEntry[]; more: boolean; before: number }`.
  - Rust: `Command::CallsQuery(CallsQuery)` with `pub struct CallsQuery { pub id: Value, pub system: Option<String>, pub talkgroup: Option<u32>, pub unit: Option<i64>, pub from: Option<i64>, pub to: Option<i64>, pub before: Option<i64>, pub limit: Option<usize> }` (`#[serde(default)]`); `ToClient::CallsResult { id: Value, entries: Vec<Value>, more: bool, before: i64 }`; `Capability::Calls` (doc: "The call store: `callsQuery`.").
  - `runtime::calls_query(ctx: &Ctx, q: &CallsQuery) -> ToClient`: `systems` = the short name plus `migrate::old_names` of the matching configured system (empty when `system` is absent); limit clamped `1..=500`, default 100.
- Consumes: `read::query`, `CallsFilter`.

- [ ] **Step 1: Write the failing tests**

In `protocol_tests.rs`, extend `messages_to_the_recorder` with a `callsQuery` example (every field set) and `the_servers_messages` with a `callsResult` answered from a seeded store (one call indexed through `index_call`, then `Command::parse(r#"{"type":"callsQuery","id":3,"talkgroup":9179}"#)` dispatched the way the test dispatches `olderCalls`). `every_message_type_is_declared` then requires both to be in the schema. In `runtime.rs`:

```rust
#[test]
fn calls_query_honours_old_system_names() {
    let dir = std::env::temp_dir().join(format!("trunk-pro-cq-{}", std::process::id()));
    let ctx = test_ctx(&dir);   // its config has a system "ohmarcs" with a site once named "ohmarcs-butler"
    let mut r = crate::db::row::tests::record(1, 1000);
    r.short_name = "ohmarcs-butler".into();
    index_call(&ctx, "ohmarcs-butler/x", &r, true, true);
    ctx.db.flush();
    let q = CallsQuery { system: Some("ohmarcs".into()), ..Default::default() };
    let ToClient::CallsResult { entries, .. } = calls_query(&ctx, &q) else { panic!() };
    assert_eq!(entries.len(), 1);
}

#[test]
fn calls_query_clamps_the_limit() {
    let ctx = test_ctx(&std::env::temp_dir().join(format!("trunk-pro-cq2-{}", std::process::id())));
    let q = CallsQuery { limit: Some(9999), ..Default::default() };
    let ToClient::CallsResult { entries, more, .. } = calls_query(&ctx, &q) else { panic!() };
    assert!(entries.len() <= 500 && !more);
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd web && npm run schema; cd .. && cargo test -p trunk-pro protocol_tests`
Expected: `every_message_type_is_declared` or compile fails for the missing variants.

- [ ] **Step 3: Implement**

TypeScript first, `npm run schema`, then the Rust variants, `listen_may_send` adds `Command::CallsQuery(_)`, the server dispatch mirrors `OlderCalls` with `spawn_blocking`, `calls_query` builds the `CallsFilter`. Docs: a row in each table and three sentences under Recorded calls saying what the store is, that `history`/`olderCalls` come from it, and when `callsQuery` is available (the `calls` capability).

- [ ] **Step 4: Run everything**

Run: `cargo test -p trunk-app -p trunk-pro && cargo clippy -p trunk-pro --all-targets -- -D warnings && cd web && npm test`
Expected: pass. (`npm test` covers the TypeScript side; nothing there changed behaviour.)

- [ ] **Step 5: Commit**

```bash
git add -A crates docs web/src/protocol.ts
git commit -m "Call store: callsQuery (by system, talkgroup, radio, time), the calls capability"
```

---

### Task 8: PR 1 wrap-up

**Files:**
- Modify: `CHANGELOG.md` (Unreleased entry)
- Modify: `docs/architecture.md` (the persistence paragraph under section 7 and the Measurements note about `history`)

- [ ] **Step 1: Changelog**

Under the unreleased heading, in the repository's voice: the call store, what it answers, the setting, that files are unchanged, that a first start indexes the capture folder in the background.

- [ ] **Step 2: Architecture doc**

Replace "history thread: fills the call list from disk in the background" with the db writer, readers and backfill threads; add the file to the list of what is kept beside the config.

- [ ] **Step 3: Full check and commit**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: pass.

```bash
git add CHANGELOG.md docs/architecture.md
git commit -m "Call store: changelog and architecture notes"
```

The branch `call-store` is PR 1. Push and open it against `TrunkRecorder/trunk-recorder-pro` `main` referencing issue #100 only when the owner says so (it is a shared repository).

---

### Task 9: HTTP routes `/api/v1/` (PR 2)

**Files:**
- Create: `crates/trunk-pro/src/api_v1.rs`
- Modify: `crates/trunk-pro/src/server.rs` (nest the router ahead of `/api/{*path}`)
- Create: `docs/api/calls.openapi.yaml`
- Modify: `docs/api/README.md` (a section "The call store over HTTP")
- Test: inline in `api_v1.rs` using the pattern of `server.rs`'s listener tests

**Interfaces:**
- Produces: `pub fn router() -> axum::Router<Arc<Ctx>>` with `GET /api/v1/calls`, `GET /api/v1/calls/{*path}`, `GET /api/v1/store`. Query parameters: `system`, `talkgroup`, `unit`, `from`, `to`, `before`, `limit` (all optional; a bad value → 400 `{"error": "..."}`). Access: the `Door` extension, both `ApiAccess::Listen` and `Control` allowed; an interface that is off → 404 as the WebSocket does.
- Consumes: `read::query`, `read::one`, `CallsFilter`, `runtime::calls_query`'s system-name resolution (lift it to `runtime::systems_for(cfg, name) -> Vec<String>`).

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn calls_routes_answer_from_the_store() {
    let (ctx, base) = serve_for_test().await;   // the helper server.rs tests use, returning the bound URL
    let r = crate::db::row::tests::record(9179, 1_700_000_000_000);
    crate::runtime::index_call(&ctx, "ohmarcs/a", &r, true, true);
    ctx.db.flush();
    let (code, body) = get(&base, "/api/v1/calls?talkgroup=9179").await;
    assert_eq!(code, 200);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["entries"].as_array().unwrap().len(), 1);
    assert_eq!(v["more"], false);
    let (code, body) = get(&base, "/api/v1/calls/ohmarcs/a").await;
    assert_eq!(code, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["record"]["talkgroup"], 9179);
    assert_eq!(get(&base, "/api/v1/calls/ohmarcs/none").await.0, 404);
    assert_eq!(get(&base, "/api/v1/calls?limit=x").await.0, 400);
    let (code, body) = get(&base, "/api/v1/store").await;
    assert_eq!(code, 200);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!((v["schemaVersion"].as_u64(), v["calls"].as_u64()), (Some(1), Some(1)));
    assert!(v["path"].as_str().unwrap().ends_with("trunk-pro.sqlite"));
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p trunk-pro api_v1`
Expected: 404s / compile errors.

- [ ] **Step 3: Implement**

Handlers under `spawn_blocking`, JSON answers, `Query<HashMap<String, String>>` parsed by hand so a bad number is a 400 with the parameter's name. `store` answers `{ path, schemaVersion, calls, oldestMs, newestMs, retentionDays }`. The OpenAPI file documents the three routes with the `CallEntry` schema referenced from the protocol schema.

- [ ] **Step 4: Run and commit**

Run: `cargo test -p trunk-pro && cargo clippy -p trunk-pro --all-targets -- -D warnings`

```bash
git add -A crates/trunk-pro/src docs/api
git commit -m "Call store: /api/v1/calls, /api/v1/calls/{path}, /api/v1/store"
```

---

### Task 10: The store's path in the plugin hello (PR 2)

**Files:**
- Modify: `crates/trunk-recorder-plugin/src/protocol.rs` (`Hello.store: Option<PathBuf>`, `#[serde(skip_serializing_if = "Option::is_none")]`)
- Modify: `crates/trunk-recorder-plugin/README.md` or its lib docs (the read-only rule)
- Modify: `crates/trunk-pro/src/plugins/host.rs` (fill it where `Hello` is built, line ~250)
- Modify: `crates/trunk-recorder-plugin/Cargo.toml` (0.6.0 → 0.7.0) and the workspace reference
- Test: `crates/trunk-recorder-plugin` inline, `plugins/host.rs` inline

**Interfaces:**
- Produces: `Hello.store`; the host passes `ctx.db.path()`.

- [ ] **Step 1: Write the failing tests**

```rust
// trunk-recorder-plugin
#[test]
fn hello_without_a_store_has_no_store_key() {
    let h = Hello::default();
    assert!(!serde_json::to_string(&h).unwrap().contains("\"store\""));
    let h: Hello = serde_json::from_str(r#"{"api":1}"#).unwrap();
    assert!(h.store.is_none());
    let h: Hello = serde_json::from_str(r#"{"api":1,"store":"/x/trunk-pro.sqlite"}"#).unwrap();
    assert_eq!(h.store.as_deref(), Some(std::path::Path::new("/x/trunk-pro.sqlite")));
}
```

In `host.rs`, extend the existing hello-building test (or add one using the module's existing fixtures) asserting the hello JSON a plugin receives carries `store` ending in `trunk-pro.sqlite`.

- [ ] **Step 2: Run, implement, run**

Run: `cargo test -p trunk-recorder-plugin -p trunk-pro`
Expected: pass after the field and the host change. The SDK's docs gain: open with `SQLITE_OPEN_READ_ONLY`, expect WAL, read `schema_version` first and refuse unknown versions, never write.

- [ ] **Step 3: Commit**

```bash
git add -A crates
git commit -m "Plugins: the call store's path in hello (read-only)"
```

PR 2 is these two tasks on a branch `call-store-api` off `call-store`, opened after PR 1 lands.
