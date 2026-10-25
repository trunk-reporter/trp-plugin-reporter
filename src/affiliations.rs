//! The live map of which unit is affiliated with which talkgroup.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::store::units::UnitEventRow;

/// Display names carried alongside an entry.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Names {
    pub system_name: String,
    pub sysid: String,
    pub unit_alpha_tag: String,
    pub tg_alpha_tag: String,
    pub tg_description: String,
    pub tg_tag: String,
    pub tg_group: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AffiliationEntry {
    pub system_id: i64,
    pub unit_id: u32,
    pub tgid: u32,
    pub previous_tgid: Option<u32>,
    pub affiliated_since_ms: i64,
    pub last_event_time_ms: i64,
    /// `"affiliated"` or `"off"`.
    pub status: &'static str,
    pub names: Names,
}

#[derive(Clone, Debug, Default)]
pub struct AffiliationFilter {
    pub system_ids: Vec<i64>,
    pub tgids: Vec<u32>,
    pub unit_ids: Vec<u32>,
    pub status: Option<String>,
    /// Exclude entries not heard from for longer than this.
    pub stale_threshold_ms: Option<i64>,
    /// Include only entries heard from within this window.
    pub active_within_ms: Option<i64>,
    pub limit: usize,
    pub offset: usize,
}

type EntryMap = HashMap<(i64, u32), AffiliationEntry>;

#[derive(Clone, Default)]
pub struct Affiliations {
    map: Arc<Mutex<EntryMap>>,
}

impl Affiliations {
    pub fn new() -> Affiliations {
        Affiliations::default()
    }

    fn lock(&self) -> MutexGuard<'_, EntryMap> {
        self.map.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn apply(&self, system_id: i64, unit: u32, event_type: &str, tgid: Option<u32>, time_ms: i64, names: Names) {
        apply_to(&mut self.lock(), system_id, unit, event_type, tgid, time_ms, names);
    }

    /// Replace the map with the result of replaying `rows` (oldest first).
    pub fn rebuild(&self, rows: &[UnitEventRow]) {
        let mut map = self.lock();
        map.clear();
        for r in rows {
            let names = Names {
                system_name: r.system_name.clone(),
                unit_alpha_tag: r.unit_alpha_tag.clone(),
                tg_alpha_tag: r.tg_alpha_tag.clone(),
                tg_description: r.tg_description.clone(),
                ..Names::default()
            };
            apply_to(&mut map, r.system_id.0, r.unit, &r.event_type, r.tgid, r.time_ms, names);
        }
    }

    /// The page of entries, the filtered total, and per-talkgroup counts of
    /// affiliated units over the whole filtered set.
    pub fn snapshot(&self, f: &AffiliationFilter, now_ms: i64) -> (Vec<AffiliationEntry>, i64, BTreeMap<u32, i64>) {
        let map = self.lock();
        let mut rows: Vec<&AffiliationEntry> = map
            .values()
            .filter(|e| f.system_ids.is_empty() || f.system_ids.contains(&e.system_id))
            .filter(|e| f.tgids.is_empty() || f.tgids.contains(&e.tgid))
            .filter(|e| f.unit_ids.is_empty() || f.unit_ids.contains(&e.unit_id))
            .filter(|e| f.status.as_deref().is_none_or(|s| s == e.status))
            .filter(|e| f.stale_threshold_ms.is_none_or(|t| now_ms - e.last_event_time_ms <= t))
            .filter(|e| f.active_within_ms.is_none_or(|w| now_ms - e.last_event_time_ms <= w))
            .collect();
        rows.sort_by(|a, b| b.last_event_time_ms.cmp(&a.last_event_time_ms).then(a.unit_id.cmp(&b.unit_id)).then(a.system_id.cmp(&b.system_id)));
        let total = rows.len() as i64;
        let mut counts = BTreeMap::new();
        for e in rows.iter().filter(|e| e.status == "affiliated") {
            *counts.entry(e.tgid).or_insert(0) += 1;
        }
        let page = rows.into_iter().skip(f.offset).take(f.limit).cloned().collect();
        (page, total, counts)
    }
}

fn apply_to(map: &mut EntryMap, system_id: i64, unit: u32, event_type: &str, tgid: Option<u32>, time_ms: i64, names: Names) {
    let key = (system_id, unit);
    let affiliating = match event_type {
        "join" | "call" | "end" | "location" => tgid,
        "off" => {
            if let Some(e) = map.get_mut(&key) {
                e.status = "off";
                e.last_event_time_ms = time_ms;
            }
            return;
        }
        _ => None,
    };
    let Some(tg) = affiliating else {
        if let Some(e) = map.get_mut(&key) {
            e.last_event_time_ms = time_ms;
        }
        return;
    };
    match map.get_mut(&key) {
        Some(e) if e.tgid == tg => {
            e.last_event_time_ms = time_ms;
            if event_type == "join" {
                e.status = "affiliated";
            }
        }
        Some(e) => {
            e.previous_tgid = Some(e.tgid);
            e.tgid = tg;
            e.affiliated_since_ms = time_ms;
            e.last_event_time_ms = time_ms;
            e.status = "affiliated";
            e.names = names;
        }
        None => {
            map.insert(
                key,
                AffiliationEntry {
                    system_id,
                    unit_id: unit,
                    tgid: tg,
                    previous_tgid: None,
                    affiliated_since_ms: time_ms,
                    last_event_time_ms: time_ms,
                    status: "affiliated",
                    names,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> AffiliationFilter {
        AffiliationFilter { limit: 100, ..Default::default() }
    }

    #[test]
    fn affiliations_follow_join_off_and_implicit_moves() {
        let a = Affiliations::new();
        a.apply(1, 7, "join", Some(101), 1000, Names::default());
        let (rows, total, counts) = a.snapshot(&all(), 2000);
        assert_eq!((total, rows[0].tgid, rows[0].previous_tgid, rows[0].status), (1, 101, None, "affiliated"));
        assert_eq!(counts.get(&101), Some(&1));

        a.apply(1, 7, "call", Some(101), 1500, Names::default());
        let (rows, _, _) = a.snapshot(&all(), 2000);
        assert_eq!((rows[0].affiliated_since_ms, rows[0].last_event_time_ms), (1000, 1500));

        a.apply(1, 7, "call", Some(202), 1800, Names::default());
        let (rows, _, counts) = a.snapshot(&all(), 2000);
        assert_eq!((rows[0].tgid, rows[0].previous_tgid, rows[0].affiliated_since_ms), (202, Some(101), 1800));
        assert_eq!(counts.get(&202), Some(&1));

        a.apply(1, 7, "off", None, 1900, Names::default());
        let (rows, total, counts) = a.snapshot(&all(), 2000);
        assert_eq!((rows[0].status, total), ("off", 1));
        assert!(counts.is_empty());

        a.apply(1, 9, "off", None, 1900, Names::default());
        assert_eq!(a.snapshot(&all(), 2000).1, 1);
    }

    #[test]
    fn snapshot_filters_sorts_and_pages() {
        let a = Affiliations::new();
        a.apply(1, 1, "join", Some(10), 1000, Names::default());
        a.apply(1, 2, "join", Some(10), 3000, Names::default());
        a.apply(2, 3, "join", Some(20), 2000, Names::default());
        let (rows, total, _) = a.snapshot(&all(), 3500);
        assert_eq!((total, rows.iter().map(|r| r.unit_id).collect::<Vec<_>>()), (3, vec![2, 3, 1]));
        let f = AffiliationFilter { limit: 1, offset: 1, ..Default::default() };
        let (rows, total, _) = a.snapshot(&f, 3500);
        assert_eq!((total, rows.len(), rows[0].unit_id), (3, 1, 3));
        let f = AffiliationFilter { limit: 10, stale_threshold_ms: Some(2000), ..Default::default() };
        assert_eq!(a.snapshot(&f, 3500).1, 2);
        let f = AffiliationFilter { limit: 10, active_within_ms: Some(600), ..Default::default() };
        assert_eq!(a.snapshot(&f, 3500).1, 1);
        let f = AffiliationFilter { limit: 10, system_ids: vec![2], ..Default::default() };
        assert_eq!(a.snapshot(&f, 3500).1, 1);
    }
}
