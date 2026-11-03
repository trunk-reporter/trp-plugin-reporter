//! Backfill from the recorder's call store (`calls.query`, recorders from
//! 0.3.0): the calls it indexes that the store lacks, those whose JSON file
//! isn't kept (the call log off) among them, which the capture-folder walk
//! cannot see. An older recorder never answers, and nothing is walked.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use trunk_recorder_plugin::{CallsPage, CallsQuery, Host};

use crate::ingest::{Ingest, stored_path};
use crate::store::calls::path_exists;
use crate::store::now_ms;
use crate::store::systems::{meta_get, meta_set};

/// `meta` key written once a walk has reached the oldest call the recorder
/// has: until then, every page is walked (an interrupted walk starts again);
/// after, a walk stops at the first page it already has.
pub const COMPLETE_KEY: &str = "recall_complete";
/// Calls asked for at a time (the most a page holds).
const PAGE: usize = 500;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecallReport {
    pub pages: u64,
    pub added: u64,
    pub known: u64,
}

/// The walk, on a thread of its own: [`Recall::page`] hands it the recorder's answers.
pub struct Recall {
    pages: mpsc::Sender<(u64, CallsPage)>,
    thread: JoinHandle<()>,
}

impl Recall {
    /// Ask for the newest page of calls that started before now, and walk the
    /// answers as they come. A set `stop` ends it, as does [`Recall::stop`].
    pub fn start(host: Host, ingest: Ingest, capture_dir: PathBuf, stop: Arc<AtomicBool>) -> Result<Recall, String> {
        let (pages, answers) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("recall".into())
            .spawn(move || {
                let report = walk(&host, ingest, &capture_dir, &answers, &stop);
                if report.added > 0 {
                    host.info(format!("backfill: {} calls added from the recorder's call store ({} already stored)", report.added, report.known));
                }
            })
            .map_err(|e| format!("cannot start the backfill: {e}"))?;
        Ok(Recall { pages, thread })
    }

    /// A page the recorder sent (`calls`), for the request it answers.
    pub fn page(&self, request: u64, page: CallsPage) {
        let _ = self.pages.send((request, page));
    }

    /// Stop it, waiting for its thread until `deadline`.
    pub fn stop(self, deadline: Instant) {
        drop(self.pages);
        while !self.thread.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if self.thread.is_finished() {
            let _ = self.thread.join();
        }
    }
}

/// Page through the recorder's calls, newest first, storing those the store
/// lacks; on to the next page while there is one and (once a walk has
/// completed) this one had any to add. Answers to other requests are ignored.
fn walk(host: &Host, mut ingest: Ingest, capture_dir: &std::path::Path, answers: &mpsc::Receiver<(u64, CallsPage)>, stop: &AtomicBool) -> RecallReport {
    let mut report = RecallReport::default();
    let completed_before = ingest.store().read(|c| meta_get(c, COMPLETE_KEY)).ok().flatten().is_some();
    // Calls since are the live `call.concluded`'s.
    let began = now_ms();
    let ask = |cursor: Option<String>| host.calls(CallsQuery { to: Some(began), cursor, limit: Some(PAGE), ..Default::default() });
    let mut asked = ask(None);
    loop {
        let Ok((request, page)) = answers.recv() else { return report };
        if stop.load(Ordering::Relaxed) {
            return report;
        }
        if request != asked {
            continue;
        }
        report.pages += 1;
        let before = report.added;
        for call in &page.entries {
            if ingest.store().read(|c| path_exists(c, &call.path)).unwrap_or(false) {
                report.known += 1;
                continue;
            }
            let audio = call
                .audio
                .then(|| ["wav", "m4a"].iter().map(|x| capture_dir.join(format!("{}.{x}", call.path))).find(|p| p.is_file()))
                .flatten()
                .map(|p| stored_path(Some(capture_dir), &p));
            let json = if call.json { format!("{}.json", call.path) } else { String::new() };
            let mut record = call.record.clone();
            if record.short_name.is_empty() {
                record.short_name = call.path.split('/').next().unwrap_or_default().to_string();
            }
            ingest.backfill_record(&call.path, &record, audio.as_deref(), &json);
            report.added += 1;
        }
        // The writer's queue stays short, so live writes are not stuck behind the walk.
        ingest.store().flush();
        // Without its call store, the recorder answers from its recent calls alone: not the whole of them.
        if page.error.is_some() {
            return report;
        }
        match page.next {
            Some(cursor) if !completed_before || report.added > before => asked = ask(Some(cursor)),
            Some(_) => return report,
            None => break,
        }
    }
    // Queued after every record above, so it commits only once they have.
    if let Err(e) = ingest.store().write_sync(|c| meta_set(c, COMPLETE_KEY, "1")) {
        host.warn(format!("backfill: could not mark the call store walked: {e}"));
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::affiliations::Affiliations;
    use crate::bus::Bus;
    use crate::config::Config;
    use crate::store::Store;
    use trunk_recorder_plugin::{CallRecord, NoConfig, PluginMessage, StoredCall, System, testing};

    fn ingest(store: &Store, host: Host) -> Ingest {
        let systems: Vec<System<Config, NoConfig>> = vec![System {
            index: 0,
            short_name: "sys1".into(),
            kind: "p25".into(),
            name: String::new(),
            fcc: Default::default(),
            fcc_call_sign: String::new(),
            fcc_name: String::new(),
            sites: Vec::new(),
            config: None,
            settings: Config::default(),
        }];
        Ingest::new(store.clone(), Bus::new(10), Affiliations::new(), Some(host), &systems).unwrap()
    }

    fn stored(n: i64, json: bool) -> StoredCall {
        let start = 1_700_000_000 + n;
        StoredCall {
            path: format!("sys1/2023/11/14/101-{start}_851012500"),
            record: CallRecord { short_name: "sys1".into(), talkgroup: 101, start_time: start, stop_time: start + 2, call_num: n as u64, ..Default::default() },
            audio: false,
            json,
        }
    }

    fn page(calls: Vec<StoredCall>, next: Option<&str>) -> CallsPage {
        CallsPage { entries: calls, next: next.map(String::from), error: None }
    }

    /// The queries sent so far, as (request, cursor).
    fn asked(captured: &testing::Captured) -> Vec<(u64, Option<String>)> {
        captured
            .output()
            .messages
            .iter()
            .filter_map(|m| match m {
                PluginMessage::CallsQuery { request, query } => Some((*request, query.cursor.clone())),
                _ => None,
            })
            .collect()
    }

    /// Wait for the `n`th query (1-based) to have been sent.
    fn wait_for(captured: &testing::Captured, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while asked(captured).len() < n {
            assert!(Instant::now() < deadline, "query {n} was not sent");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn count(store: &Store) -> i64 {
        store.read(|c| c.query_row("SELECT count(*) FROM calls", [], |r| r.get(0))).unwrap()
    }

    #[test]
    fn it_walks_every_page_then_only_what_is_new() {
        let store = Store::in_memory();
        let (host, captured) = testing::capture();
        let dir = testing::temp_dir("recall");
        let stop = Arc::new(AtomicBool::new(false));
        let recall = Recall::start(host.clone(), ingest(&store, host.clone()), dir.clone(), stop.clone()).unwrap();
        wait_for(&captured, 1);
        let q = captured.output().messages.into_iter().find_map(|m| match m {
            PluginMessage::CallsQuery { query, .. } => Some(query),
            _ => None,
        });
        assert!(q.is_some_and(|q| q.to.is_some_and(|t| t <= now_ms()) && q.limit == Some(PAGE)), "calls started before now, a full page");
        // An answer to another request is not this walk's.
        recall.page(99, page(vec![stored(9, true)], None));
        recall.page(1, page(vec![stored(3, true), stored(2, false)], Some("c1")));
        wait_for(&captured, 2);
        assert_eq!(asked(&captured)[1], (2, Some("c1".into())));
        recall.page(2, page(vec![stored(1, false)], None));
        recall.thread.join().unwrap();
        assert_eq!(count(&store), 3);
        let json: String = store.read(|c| c.query_row("SELECT json_path FROM calls WHERE call_num = 3", [], |r| r.get(0))).unwrap();
        assert_eq!(json, format!("{}.json", stored(3, true).path));
        assert_eq!(store.read(|c| meta_get(c, COMPLETE_KEY)).unwrap().as_deref(), Some("1"));
        assert!(captured.output().logs().iter().any(|(_, m)| m.contains("3 calls added from the recorder's call store")));

        // Walked once: a later start stops at the first page it has nothing to add from.
        let (host, captured) = testing::capture();
        let recall = Recall::start(host.clone(), ingest(&store, host), dir, stop).unwrap();
        wait_for(&captured, 1);
        recall.page(1, page(vec![stored(4, false), stored(3, true)], Some("c1")));
        wait_for(&captured, 2);
        recall.page(2, page(vec![stored(2, false), stored(1, false)], Some("c2")));
        recall.thread.join().unwrap();
        assert_eq!(asked(&captured).len(), 2, "no third page");
        assert_eq!(count(&store), 4);
    }

    #[test]
    fn without_its_store_or_an_answer_nothing_is_marked() {
        let store = Store::in_memory();
        let (host, captured) = testing::capture();
        let stop = Arc::new(AtomicBool::new(false));
        let recall = Recall::start(host.clone(), ingest(&store, host.clone()), testing::temp_dir("recall-off"), stop.clone()).unwrap();
        wait_for(&captured, 1);
        let recent = CallsPage { entries: vec![stored(1, true)], next: Some("c1".into()), error: Some("the call store is off".into()) };
        recall.page(1, recent);
        recall.thread.join().unwrap();
        assert_eq!((count(&store), asked(&captured).len()), (1, 1));
        assert_eq!(store.read(|c| meta_get(c, COMPLETE_KEY)).unwrap(), None);

        // A recorder that never answers (before 0.3.0): stopped at shutdown, unmarked.
        let (host, _captured) = testing::capture();
        let recall = Recall::start(host.clone(), ingest(&store, host), testing::temp_dir("recall-old"), stop).unwrap();
        recall.stop(Instant::now() + Duration::from_secs(2));
        assert_eq!(store.read(|c| meta_get(c, COMPLETE_KEY)).unwrap(), None);
    }
}
