//! The SQLite store: one writer thread applies queued closures in order on the
//! single writing connection; a small pool of other connections serves reads.
// Later tasks use these items; until then the binary does not.
#![allow(dead_code, unused_imports)]

pub mod systems;

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags};

pub use systems::{meta_get, meta_set};

const SCHEMA: &str = include_str!("schema.sql");
const SCHEMA_VERSION: i64 = 1;
const READERS: usize = 4;

/// Current time as Unix milliseconds.
pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// A store failure, as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError(e.to_string())
    }
}

type Job = Box<dyn FnOnce(&Connection) -> rusqlite::Result<()> + Send>;
type Logger = Arc<dyn Fn(String) + Send + Sync>;

struct Inner {
    tx: Mutex<Option<Sender<Job>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    readers: Vec<Mutex<Connection>>,
    next_reader: AtomicUsize,
    queued: Arc<AtomicUsize>,
    logger: Arc<Mutex<Logger>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Closing the channel ends the writer loop once it has drained the queue.
        lock(&self.tx).take();
        if let Some(h) = lock(&self.writer).take() {
            let _ = h.join();
        }
    }
}

/// Handle to the database. Cloning shares the same writer and readers.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn configure(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", true)
}

fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    let has: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version')", [], |r| r.get(0))?;
    if has {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(SCHEMA)?;
    tx.execute("INSERT INTO schema_version VALUES (?1)", [SCHEMA_VERSION])?;
    tx.commit()
}

impl Store {
    /// Open (creating if needed) the database file: WAL, `synchronous=NORMAL`,
    /// foreign keys on, schema applied.
    pub fn open(path: &Path) -> Result<Store, StoreError> {
        let writer = Connection::open(path)?;
        configure(&writer)?;
        writer.pragma_update(None, "journal_mode", "WAL")?;
        writer.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&writer)?;
        let mut readers = Vec::new();
        for _ in 0..READERS {
            let r = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
            configure(&r)?;
            readers.push(Mutex::new(r));
        }
        Ok(Store::start(writer, readers))
    }

    /// A private in-memory database that the writer and readers share.
    pub fn in_memory() -> Store {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let uri = format!("file:memdb{}?mode=memory&cache=shared", COUNTER.fetch_add(1, Ordering::Relaxed));
        let flags = OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_SHARED_CACHE;
        let open = || -> rusqlite::Result<Connection> {
            let c = Connection::open_with_flags(&uri, flags)?;
            configure(&c)?;
            Ok(c)
        };
        let writer = open().expect("open in-memory database");
        migrate(&writer).expect("apply schema");
        let readers = (0..READERS)
            .map(|_| {
                let r = open().expect("open in-memory reader");
                // Shared-cache readers would otherwise get SQLITE_LOCKED while the writer is mid-transaction.
                r.pragma_update(None, "read_uncommitted", true).expect("read_uncommitted");
                Mutex::new(r)
            })
            .collect();
        Store::start(writer, readers)
    }

    fn start(writer: Connection, readers: Vec<Mutex<Connection>>) -> Store {
        let (tx, rx) = mpsc::channel::<Job>();
        let queued = Arc::new(AtomicUsize::new(0));
        let logger: Arc<Mutex<Logger>> = Arc::new(Mutex::new(Arc::new(|_| {})));
        let handle = {
            let (queued, logger) = (queued.clone(), logger.clone());
            std::thread::Builder::new().name("store-writer".into()).spawn(move || writer_loop(writer, rx, queued, logger)).expect("spawn store writer")
        };
        Store {
            inner: Arc::new(Inner { tx: Mutex::new(Some(tx)), writer: Mutex::new(Some(handle)), readers, next_reader: AtomicUsize::new(0), queued, logger }),
        }
    }

    /// Where write errors and writer panics are reported (default: nowhere).
    pub fn set_logger(&self, f: Arc<dyn Fn(String) + Send + Sync>) {
        *lock(&self.inner.logger) = f;
    }

    fn send(&self, job: Job) -> Result<(), StoreError> {
        let guard = lock(&self.inner.tx);
        let tx = guard.as_ref().ok_or_else(|| StoreError("store is closed".into()))?;
        self.inner.queued.fetch_add(1, Ordering::SeqCst);
        tx.send(job).map_err(|_| {
            self.inner.queued.fetch_sub(1, Ordering::SeqCst);
            StoreError("store writer is gone".into())
        })
    }

    /// Queue a write and return at once. Errors go to the logger.
    pub fn write(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<()> + Send + 'static) {
        if let Err(e) = self.send(Box::new(f)) {
            let log = lock(&self.inner.logger).clone();
            log(e.0);
        }
    }

    /// Queue a write and wait for its result.
    pub fn write_sync<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static) -> Result<T, StoreError> {
        let (rtx, rrx) = mpsc::sync_channel::<Result<T, StoreError>>(1);
        self.send(Box::new(move |c| {
            let _ = rtx.send(f(c).map_err(StoreError::from));
            Ok(())
        }))?;
        // A panicking closure drops the sender without a reply.
        rrx.recv().map_err(|_| StoreError("write failed before it returned".into()))?
    }

    /// Run a read on one of the pooled connections.
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, StoreError> {
        let i = self.inner.next_reader.fetch_add(1, Ordering::Relaxed) % self.inner.readers.len();
        let conn = lock(&self.inner.readers[i]);
        f(&conn).map_err(StoreError::from)
    }

    /// Wait until every write queued before this call has been applied.
    pub fn flush(&self) {
        let (rtx, rrx) = mpsc::sync_channel::<()>(1);
        let sent = self.send(Box::new(move |_| {
            let _ = rtx.send(());
            Ok(())
        }));
        if sent.is_ok() {
            let _ = rrx.recv();
        }
    }

    /// Writes queued and not yet applied.
    pub fn queue_len(&self) -> usize {
        self.inner.queued.load(Ordering::SeqCst)
    }
}

fn writer_loop(conn: Connection, rx: Receiver<Job>, queued: Arc<AtomicUsize>, logger: Arc<Mutex<Logger>>) {
    while let Ok(job) = rx.recv() {
        let result = catch_unwind(AssertUnwindSafe(|| job(&conn)));
        queued.fetch_sub(1, Ordering::SeqCst);
        let msg = match result {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => format!("store write failed: {e}"),
            Err(_) => "store write panicked".to_string(),
        };
        let log = lock(&logger).clone();
        log(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_a_file_store_in_wal_mode_with_the_schema() {
        let dir = trunk_recorder_plugin::testing::temp_dir("store");
        let s = Store::open(&dir.join("engine.sqlite")).unwrap();
        let mode: String = s.read(|c| c.query_row("PRAGMA journal_mode", [], |r| r.get(0))).unwrap();
        assert_eq!(mode, "wal");
        let n: i64 = s
            .read(|c| {
                c.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('systems','sites','talkgroups','units','calls','call_transmissions','call_frequencies','unit_events','recorder_state','meta')",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(n, 10);
        assert_eq!(s.read(|c| c.query_row("SELECT version FROM schema_version", [], |r| r.get::<_, i64>(0))).unwrap(), 1);
    }

    #[test]
    fn writes_are_applied_in_order_and_flush_waits() {
        let s = Store::in_memory();
        for i in 0..100 {
            s.write(move |c| c.execute("INSERT INTO meta VALUES (?1, ?2)", (format!("k{i}"), i.to_string())).map(|_| ()));
        }
        s.flush();
        assert_eq!(s.queue_len(), 0);
        assert_eq!(s.read(|c| c.query_row("SELECT count(*) FROM meta", [], |r| r.get::<_, i64>(0))).unwrap(), 100);
        assert_eq!(s.read(|c| meta_get(c, "k7")).unwrap(), Some("7".into()));
    }

    #[test]
    fn opening_again_keeps_data() {
        let dir = trunk_recorder_plugin::testing::temp_dir("store");
        let path = dir.join("engine.sqlite");
        let s = Store::open(&path).unwrap();
        s.write_sync(|c| meta_set(c, "a", "b")).unwrap();
        drop(s);
        let s = Store::open(&path).unwrap();
        assert_eq!(s.read(|c| meta_get(c, "a")).unwrap(), Some("b".into()));
        assert_eq!(s.read(|c| meta_get(c, "missing")).unwrap(), None);
    }

    #[test]
    fn a_panicking_write_does_not_stop_the_writer() {
        let s = Store::in_memory();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        s.set_logger(Arc::new(move |m| sink.lock().unwrap().push(m)));
        s.write(|_| panic!("boom"));
        s.write(|c| c.execute("INSERT INTO nope VALUES (1)", []).map(|_| ()));
        assert_eq!(s.write_sync(|c| meta_set(c, "x", "y")), Ok(()));
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(s.read(|c| meta_get(c, "x")).unwrap(), Some("y".into()));
    }
}
