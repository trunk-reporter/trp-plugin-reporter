//! Start-up walk over the capture folder: call files already on disk that the
//! store does not have yet.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use time::OffsetDateTime;
use trunk_recorder_plugin::CallRecord;
use walkdir::WalkDir;

use crate::ingest::{Ingest, stored_path};
use crate::store::SystemId;
use crate::store::calls::{newest_start_ms, path_exists};
use crate::store::systems::{meta_get, meta_set};

/// `meta` key written once a walk has covered every system's folders: until
/// then, older day folders are walked too (an interrupted walk resumes).
pub const COMPLETE_KEY: &str = "backfill_complete";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackfillReport {
    /// JSON files seen in the day folders walked.
    pub files: u64,
    pub ingested: u64,
    pub skipped_known: u64,
    /// Files written after the walk began: the live `call.concluded` stores those.
    pub skipped_new: u64,
    pub bad: u64,
}

/// The UTC day of a time in milliseconds, as (year, month, day).
fn day_of_ms(ms: i64) -> Option<(i32, u8, u8)> {
    let d = OffsetDateTime::from_unix_timestamp(ms.div_euclid(1000)).ok()?.date();
    Some((d.year(), u8::from(d.month()), d.day()))
}

/// A `<Y>/<M>/<D>` folder's date, from its path under the system's folder.
fn folder_day(rel: &Path) -> Option<(i32, u8, u8)> {
    let parts: Vec<i32> = rel.components().map(|c| c.as_os_str().to_str().and_then(|s| s.parse().ok())).collect::<Option<_>>()?;
    let [y, m, d] = parts[..] else { return None };
    Some((y, u8::try_from(m).ok()?, u8::try_from(d).ok()?))
}

fn read_record(path: &Path) -> Result<CallRecord, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map_err(|e| e.to_string())
}

/// Ingest the call files under `capture_dir/<short_name>/<Y>/<M>/<D>/` that the
/// store lacks. Once a walk has finished ([`COMPLETE_KEY`]), day folders older
/// than the day before each system's newest stored call are skipped.
/// Files modified after the walk began are left to the live `call.concluded`.
/// Every 100 files the walk waits for the store to catch up, then tells
/// `progress` the file count. A set `stop` ends the walk early, without
/// marking it complete.
pub fn backfill(ingest: &mut Ingest, capture_dir: &Path, systems: &[(SystemId, String)], progress: &mut dyn FnMut(u64), stop: &AtomicBool) -> BackfillReport {
    let mut report = BackfillReport::default();
    let began = SystemTime::now();
    let stopped = || stop.load(Ordering::Relaxed);
    let completed_before = ingest.store().read(|c| meta_get(c, COMPLETE_KEY)).ok().flatten().is_some();
    for (system, short_name) in systems {
        let newest = if completed_before { ingest.store().read(|c| newest_start_ms(c, *system)).ok().flatten() } else { None };
        // One day of slack: a call near midnight may sit in the next folder.
        let cutoff = newest.and_then(|ms| day_of_ms(ms - 86_400_000));
        let root = capture_dir.join(short_name);
        let days: Vec<_> = WalkDir::new(&root)
            .min_depth(3)
            .max_depth(3)
            .sort_by_file_name()
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
            .filter_map(|e| Some((folder_day(e.path().strip_prefix(&root).ok()?)?, e.into_path())))
            .filter(|(day, _)| cutoff.is_none_or(|c| *day >= c))
            .collect();
        for (_, dir) in days {
            if stopped() {
                return report;
            }
            for entry in WalkDir::new(&dir).min_depth(1).max_depth(1).sort_by_file_name().into_iter().filter_map(Result::ok) {
                if stopped() {
                    return report;
                }
                let path = entry.path();
                if !entry.file_type().is_file() || path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                report.files += 1;
                if report.files % 100 == 0 {
                    // The writer's queue stays short, so live writes are not stuck behind the walk.
                    ingest.store().flush();
                    progress(report.files);
                }
                if entry.metadata().ok().and_then(|m| m.modified().ok()).is_some_and(|t| t > began) {
                    report.skipped_new += 1;
                    continue;
                }
                let json_path = stored_path(Some(capture_dir), path);
                let key = json_path.strip_suffix(".json").unwrap_or(&json_path).to_string();
                if ingest.store().read(|c| path_exists(c, &key)).unwrap_or(false) {
                    report.skipped_known += 1;
                    continue;
                }
                let record = match read_record(path) {
                    Ok(r) => r,
                    Err(e) => {
                        report.bad += 1;
                        if let Some(h) = ingest.host() {
                            h.warn(format!("backfill: skipping {json_path}: {e}"));
                        }
                        continue;
                    }
                };
                let audio = ["wav", "m4a"].iter().map(|x| path.with_extension(x)).find(|p| p.is_file()).map(|p| stored_path(Some(capture_dir), &p));
                ingest.backfill_record(&key, &record, audio.as_deref(), &json_path);
                report.ingested += 1;
            }
        }
    }
    // Queued after every record above, so it commits only once they have.
    if let Err(e) = ingest.store().write_sync(|c| meta_set(c, COMPLETE_KEY, "1"))
        && let Some(h) = ingest.host()
    {
        h.warn(format!("backfill: could not mark the walk complete: {e}"));
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
    use trunk_recorder_plugin::{CallRecord, Level, NoConfig, System, testing};

    fn rig(tag: &str) -> (Ingest, Store, std::path::PathBuf, testing::Captured) {
        let store = Store::in_memory();
        let dir = testing::temp_dir(tag);
        let (host, captured) = testing::capture();
        let systems: Vec<System<Config, NoConfig>> =
            vec![System { index: 0, short_name: "sys1".into(), kind: "p25".into(), config: None, settings: Config::default() }];
        let ingest = Ingest::new(store.clone(), Bus::new(100), Affiliations::new(), Some(host), &systems).unwrap().with_capture_dir(dir.clone());
        (ingest, store, dir, captured)
    }

    fn systems_of(store: &Store) -> Vec<(SystemId, String)> {
        vec![(store.read(|c| c.query_row("SELECT id FROM systems WHERE short_name = 'sys1'", [], |r| r.get(0))).unwrap(), "sys1".to_string())]
    }

    fn count(store: &Store) -> i64 {
        store.read(|c| c.query_row("SELECT count(*) FROM calls", [], |r| r.get(0))).unwrap()
    }

    #[test]
    fn backfill_ingests_new_files_and_skips_known_ones() {
        let (mut ingest, store, dir, _) = rig("backfill1");
        testing::call(&dir, "sys1", 101);
        testing::call(&dir, "sys1", 102);
        let systems = systems_of(&store);
        let first = backfill(&mut ingest, &dir, &systems, &mut |_| {}, &AtomicBool::new(false));
        store.flush();
        assert_eq!(first, BackfillReport { files: 2, ingested: 2, ..Default::default() });
        let second = backfill(&mut ingest, &dir, &systems, &mut |_| {}, &AtomicBool::new(false));
        store.flush();
        assert_eq!(second, BackfillReport { files: 2, skipped_known: 2, ..Default::default() });
        assert_eq!(count(&store), 2);
        let (path, audio, json): (String, Option<String>, Option<String>) = store
            .read(|c| c.query_row("SELECT path, audio_path, json_path FROM calls ORDER BY path LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))))
            .unwrap();
        assert!(path.starts_with("sys1/2023/11/14/101-"));
        assert_eq!(audio.as_deref(), Some(format!("{path}.wav").as_str()));
        assert_eq!(json.as_deref(), Some(format!("{path}.json").as_str()));
    }

    #[test]
    fn a_bad_file_is_skipped_and_the_rest_ingested() {
        let (mut ingest, store, dir, captured) = rig("backfill2");
        let cc = testing::call(&dir, "sys1", 101);
        let day = dir.join(&cc.path).parent().unwrap().to_path_buf();
        std::fs::write(day.join("999-1_1.json"), "{not json").unwrap();
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |_| {}, &AtomicBool::new(false));
        store.flush();
        assert_eq!(r, BackfillReport { files: 2, ingested: 1, bad: 1, ..Default::default() });
        assert_eq!(count(&store), 1);
        let warnings: Vec<String> = captured.output().logs().into_iter().filter(|(l, _)| *l == Level::Warn).map(|(_, m)| m).collect();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("999-1_1.json"), "{}", warnings[0]);
    }

    #[test]
    fn a_file_written_after_the_walk_began_is_left_to_the_live_path() {
        let (mut ingest, store, dir, _) = rig("backfill6");
        let cc = testing::call(&dir, "sys1", 101);
        let json = dir.join(format!("{}.json", cc.path));
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(3_600);
        std::fs::File::options().write(true).open(&json).unwrap().set_modified(later).unwrap();
        testing::call(&dir, "sys1", 102);
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |_| {}, &AtomicBool::new(false));
        store.flush();
        assert_eq!(r, BackfillReport { files: 2, ingested: 1, skipped_new: 1, ..Default::default() });
        assert_eq!(count(&store), 1);
    }

    #[test]
    fn the_walk_lets_the_store_catch_up_every_100_files() {
        let (mut ingest, store, dir, _) = rig("backfill7");
        let cc = testing::call(&dir, "sys1", 101);
        let day = dir.join(&cc.path).parent().unwrap().to_path_buf();
        for n in 0..250 {
            let mut rec = cc.call.clone();
            rec.call_num = n;
            std::fs::write(day.join(format!("101-{n}_1.json")), serde_json::to_string(&rec).unwrap()).unwrap();
        }
        let mut queued = Vec::new();
        let s = store.clone();
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |_| queued.push(s.queue_len()), &AtomicBool::new(false));
        assert_eq!((r.files, r.ingested), (251, 251));
        assert_eq!(queued, vec![0, 0]);
    }

    /// A stored call on 2023-11-20 (not on disk); on disk, 2023-11-14 (an
    /// older day folder) and 2023-11-21.
    fn older_folder_rig(tag: &str) -> (Ingest, Store, std::path::PathBuf) {
        let (mut ingest, store, dir, _) = rig(tag);
        let cc = testing::call(&dir, "sys1", 101);
        let mut stored: CallRecord = cc.call.clone();
        stored.start_time = 1_700_481_600;
        stored.stop_time = 1_700_481_603;
        ingest.backfill_record("sys1/2023/11/20/x", &stored, None, "sys1/2023/11/20/x.json");
        store.flush();
        let later = dir.join("sys1").join("2023").join("11").join("21");
        std::fs::create_dir_all(&later).unwrap();
        std::fs::write(later.join("102-1700568000_1.json"), serde_json::to_string(&stored).unwrap()).unwrap();
        (ingest, store, dir)
    }

    fn completed(store: &Store) -> bool {
        store.read(|c| meta_get(c, COMPLETE_KEY)).unwrap().is_some()
    }

    #[test]
    fn day_folders_older_than_the_store_are_not_walked() {
        let (mut ingest, store, dir) = older_folder_rig("backfill3");
        store.write_sync(|c| meta_set(c, COMPLETE_KEY, "1")).unwrap();
        let mut ticks = Vec::new();
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |n| ticks.push(n), &AtomicBool::new(false));
        assert_eq!(r.files, 1);
        assert!(ticks.is_empty());
    }

    #[test]
    fn until_a_walk_completes_older_day_folders_are_walked_too() {
        let (mut ingest, store, dir) = older_folder_rig("backfill4");
        assert!(!completed(&store));
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |_| {}, &AtomicBool::new(false));
        assert_eq!((r.files, r.ingested), (2, 2));
        assert!(completed(&store));
    }

    #[test]
    fn a_stopped_walk_is_not_marked_complete() {
        let (mut ingest, store, dir) = older_folder_rig("backfill5");
        let r = backfill(&mut ingest, &dir, &systems_of(&store), &mut |_| {}, &AtomicBool::new(true));
        assert_eq!(r.files, 0);
        assert!(!completed(&store));
    }
}
