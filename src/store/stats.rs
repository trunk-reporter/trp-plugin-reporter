//! Totals and per-system activity for `/stats`.

use rusqlite::Connection;

use super::SystemId;

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;

/// Row counts and call totals across every system.
#[derive(Clone, Debug, PartialEq)]
pub struct StatsRow {
    pub sites: i64,
    pub talkgroups: i64,
    pub units: i64,
    pub total_calls: i64,
    pub calls_24h: i64,
    pub calls_1h: i64,
    pub total_duration_hours: f64,
}

/// One system's recent activity.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityRow {
    pub system_id: SystemId,
    pub calls_1h: i64,
    pub calls_24h: i64,
    pub active_talkgroups: i64,
    pub active_units: i64,
}

/// `total_duration_hours` reads the running total that `ensure.sql`'s triggers keep in `meta`.
pub fn stats(conn: &Connection, now_ms: i64) -> rusqlite::Result<StatsRow> {
    conn.query_row(
        "SELECT (SELECT COUNT(*) FROM sites), (SELECT COUNT(*) FROM talkgroups), (SELECT COUNT(*) FROM units), \
                (SELECT COUNT(*) FROM calls), \
                (SELECT COUNT(*) FROM calls WHERE start_ms >= ?1), \
                (SELECT COUNT(*) FROM calls WHERE start_ms >= ?2), \
                COALESCE((SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'total_length_ms'), 0) / 3600000.0",
        (now_ms - DAY_MS, now_ms - HOUR_MS),
        |r| {
            Ok(StatsRow {
                sites: r.get(0)?,
                talkgroups: r.get(1)?,
                units: r.get(2)?,
                total_calls: r.get(3)?,
                calls_24h: r.get(4)?,
                calls_1h: r.get(5)?,
                total_duration_hours: r.get(6)?,
            })
        },
    )
}

/// Every system's activity, by system id. Active units are those with a
/// transmission on a call started in the last hour, or a unit event in it.
pub fn system_activity(conn: &Connection, now_ms: i64) -> rusqlite::Result<Vec<ActivityRow>> {
    conn.prepare(
        "SELECT s.id, \
                (SELECT COUNT(*) FROM calls WHERE system_id = s.id AND start_ms >= ?2), \
                (SELECT COUNT(*) FROM calls WHERE system_id = s.id AND start_ms >= ?1), \
                (SELECT COUNT(DISTINCT tgid) FROM calls WHERE system_id = s.id AND start_ms >= ?2), \
                (SELECT COUNT(*) FROM (\
                    SELECT t.unit FROM call_transmissions t JOIN calls c ON c.id = t.call_id \
                      WHERE c.system_id = s.id AND c.start_ms >= ?2 AND t.time_ms >= ?2 \
                    UNION SELECT unit FROM unit_events WHERE system_id = s.id AND time_ms >= ?2)) \
         FROM systems s ORDER BY s.id",
    )?
    .query_map((now_ms - DAY_MS, now_ms - HOUR_MS), |r| {
        Ok(ActivityRow { system_id: r.get(0)?, calls_1h: r.get(1)?, calls_24h: r.get(2)?, active_talkgroups: r.get(3)?, active_units: r.get(4)? })
    })?
    .collect()
}

/// Which calls `talkgroup_activity` and `activity_buckets` count: those started in
/// `[after_ms, before_ms)`. An empty list is no filter on that column.
#[derive(Clone, Debug, Default)]
pub struct ActivityFilter {
    pub system_ids: Vec<i64>,
    pub site_ids: Vec<i64>,
    pub tgids: Vec<u32>,
    pub after_ms: i64,
    pub before_ms: i64,
    /// A `calls.state` to match; `None` counts every state.
    pub state: Option<String>,
}

/// One talkgroup's calls in a window. Names are empty when the talkgroup has no row.
#[derive(Clone, Debug, PartialEq)]
pub struct TalkgroupActivityRow {
    pub system_id: SystemId,
    pub tgid: u32,
    pub alpha_tag: String,
    pub description: String,
    pub tag: String,
    pub group: String,
    pub call_count: i64,
    /// Seconds.
    pub total_duration: f64,
    pub emergency_count: i64,
    pub first_call_ms: i64,
    pub last_call_ms: i64,
    /// Seconds.
    pub avg_duration: f64,
}

/// One non-empty bucket of a talkgroup's calls.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityBucketRow {
    pub start_ms: i64,
    pub calls: i64,
    /// Seconds of call time in calls that started in the bucket.
    pub seconds: f64,
}

/// The `WHERE` text for a filter. The ids are numbers, so they are written in; the rest is bound.
fn activity_where(f: &ActivityFilter) -> String {
    fn list<T: std::fmt::Display>(column: &str, v: &[T]) -> String {
        if v.is_empty() { String::new() } else { format!(" AND c.{column} IN ({})", v.iter().map(T::to_string).collect::<Vec<_>>().join(",")) }
    }
    format!(
        "c.start_ms >= :after AND c.start_ms < :before{}{}{}{}",
        list("system_id", &f.system_ids),
        list("site_id", &f.site_ids),
        list("tgid", &f.tgids),
        if f.state.is_some() { " AND c.state = :state" } else { "" }
    )
}

fn activity_params<'a>(f: &'a ActivityFilter, interval_ms: Option<&'a i64>) -> Vec<(&'static str, &'a dyn rusqlite::ToSql)> {
    let mut p: Vec<(&'static str, &dyn rusqlite::ToSql)> = vec![(":after", &f.after_ms), (":before", &f.before_ms)];
    if let Some(s) = &f.state {
        p.push((":state", s));
    }
    if let Some(i) = interval_ms {
        p.push((":interval", i));
    }
    p
}

const DURATION_S: &str = "(COALESCE(c.end_ms, c.start_ms) - c.start_ms) / 1000.0";

/// How many talkgroups have calls in the window.
pub fn activity_talkgroups(conn: &Connection, f: &ActivityFilter) -> rusqlite::Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM (SELECT DISTINCT c.system_id, c.tgid FROM calls c WHERE {})", activity_where(f));
    conn.query_row(&sql, activity_params(f, None).as_slice(), |r| r.get(0))
}

/// One row per `(system, talkgroup)` with calls in the window, busiest first (then by tgid, system).
pub fn talkgroup_activity(conn: &Connection, f: &ActivityFilter) -> rusqlite::Result<Vec<TalkgroupActivityRow>> {
    let sql = format!(
        "SELECT c.system_id, c.tgid, COALESCE(t.alpha_tag, ''), COALESCE(t.description, ''), COALESCE(t.tag, ''), COALESCE(t.\"group\", ''), \
                COUNT(*) AS n, SUM({DURATION_S}), SUM(c.emergency <> 0), MIN(c.start_ms), MAX(c.start_ms) \
         FROM calls c LEFT JOIN talkgroups t ON t.system_id = c.system_id AND t.tgid = c.tgid \
         WHERE {} GROUP BY c.system_id, c.tgid ORDER BY n DESC, c.tgid, c.system_id",
        activity_where(f)
    );
    conn.prepare(&sql)?
        .query_map(activity_params(f, None).as_slice(), |r| {
            let (call_count, total_duration): (i64, f64) = (r.get(6)?, r.get(7)?);
            Ok(TalkgroupActivityRow {
                system_id: r.get(0)?,
                tgid: r.get(1)?,
                alpha_tag: r.get(2)?,
                description: r.get(3)?,
                tag: r.get(4)?,
                group: r.get(5)?,
                call_count,
                total_duration,
                emergency_count: r.get(8)?,
                first_call_ms: r.get(9)?,
                last_call_ms: r.get(10)?,
                avg_duration: if call_count > 0 { total_duration / call_count as f64 } else { 0.0 },
            })
        })?
        .collect()
}

/// Per `(system, tgid)`, the non-empty buckets of `interval_s` seconds, aligned to the epoch:
/// a call is in the bucket its start falls in. Ordered by system, tgid, time.
pub fn activity_buckets(conn: &Connection, f: &ActivityFilter, interval_s: u32) -> rusqlite::Result<Vec<(i64, u32, Vec<ActivityBucketRow>)>> {
    let interval_ms = i64::from(interval_s.max(1)) * 1000;
    let sql = format!(
        "SELECT c.system_id, c.tgid, (c.start_ms / :interval) * :interval AS b, COUNT(*), SUM({DURATION_S}) \
         FROM calls c WHERE {} GROUP BY c.system_id, c.tgid, b ORDER BY c.system_id, c.tgid, b",
        activity_where(f)
    );
    let mut out: Vec<(i64, u32, Vec<ActivityBucketRow>)> = Vec::new();
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(activity_params(f, Some(&interval_ms)).as_slice())?;
    while let Some(r) = rows.next()? {
        let (system, tgid): (i64, u32) = (r.get(0)?, r.get(1)?);
        let bucket = ActivityBucketRow { start_ms: r.get(2)?, calls: r.get(3)?, seconds: r.get(4)? };
        match out.last_mut() {
            Some((s, t, v)) if *s == system && *t == tgid => v.push(bucket),
            _ => out.push((system, tgid, vec![bucket])),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::calls::{NewCall, call_concluded, call_ended, call_started};
    use crate::store::recorder::retain;
    use crate::store::systems::{ensure_system, meta_get};
    use trunk_recorder_plugin::CallInfo;
    use trunk_recorder_plugin::testing::{call, temp_dir};

    fn totals(s: &Store) -> (i64, i64) {
        s.read(|c| {
            let kept: i64 = meta_get(c, "total_length_ms")?.unwrap().parse().unwrap();
            let sum: i64 = c.query_row("SELECT COALESCE(SUM(length_ms), 0) FROM calls", [], |r| r.get(0))?;
            Ok((kept, sum))
        })
        .unwrap()
    }

    #[test]
    fn the_running_length_total_matches_the_calls() {
        let s = Store::in_memory();
        let sys = s.write_sync(|c| ensure_system(c, "sys1", "p25", "", 1_000)).unwrap();
        let rec = call(&temp_dir("stats"), "sys1", 101).call;
        let start = rec.start_time * 1000;
        let info = CallInfo { id: 7, talkgroup: 101, start_time: rec.start_time as f64, recording: true, ..Default::default() };
        let i = info.clone();
        let live = s.write_sync(move |c| call_started(c, &NewCall::from_info(&i, sys, None))).unwrap();
        assert_eq!(totals(&s), (0, 0));
        let i = info.clone();
        s.write_sync(move |c| call_ended(c, live, start + 9_000, &i)).unwrap();
        assert_eq!(totals(&s), (9_000, 9_000));
        // The files say 3 s: the total follows the change.
        let r = rec.clone();
        s.write_sync(move |c| call_concluded(c, sys, None, "sys1/a", &r, None, "sys1/a.json", Some(live)).map(|_| ())).unwrap();
        let (kept, sum) = totals(&s);
        assert_eq!(kept, sum);
        for (n, path) in ["sys1/b", "sys1/c"].into_iter().enumerate() {
            let mut r = rec.clone();
            r.call_num = 100 + n as u64;
            r.start_time_ms += 60_000 * (n as i64 + 1);
            s.write_sync(move |c| call_concluded(c, sys, None, path, &r, None, "x.json", None).map(|_| ())).unwrap();
        }
        let (kept, sum) = totals(&s);
        assert_eq!((kept, s.read(|c| stats(c, 0)).unwrap().total_duration_hours), (sum, sum as f64 / 3_600_000.0));
        // Retention deletes calls; the total drops with them.
        s.write_sync(move |c| c.execute("DELETE FROM calls WHERE path = 'sys1/b'", []).map(|_| ())).unwrap();
        s.write_sync(|c| retain(c, 1, i64::MAX / 2)).unwrap();
        assert_eq!(totals(&s), (0, 0));
    }

    #[test]
    fn buckets_align_to_the_interval_and_skip_empty_ones() {
        let s = Store::in_memory();
        let base = 1_700_000_400_000_i64; // a multiple of 60 000
        s.write_sync(move |c| {
            ensure_system(c, "sys1", "p25", "", 1_000)?;
            // tg 7: 5 s in (4 s long), 59.999 s in (2 s), 180 s in (still open, 0 s); tg 8: at 0 (1 s, emergency);
            // and tg 7 again exactly at the window's end, which is outside it.
            for (tg, start, end, emerg) in [
                (7, base + 5_000, Some(base + 9_000), 0),
                (7, base + 59_999, Some(base + 61_999), 0),
                (7, base + 180_000, None, 0),
                (8, base, Some(base + 1_000), 1),
                (7, base + 600_000, Some(base + 601_000), 0),
            ] {
                c.execute(
                    "INSERT INTO calls (system_id, tgid, start_ms, end_ms, emergency, state) VALUES (1, ?1, ?2, ?3, ?4, 'concluded')",
                    rusqlite::params![tg, start, end, emerg],
                )?;
            }
            Ok(())
        })
        .unwrap();
        let f = ActivityFilter { after_ms: base, before_ms: base + 600_000, ..Default::default() };
        let b = s.read(|c| activity_buckets(c, &f, 60)).unwrap();
        let b7 = vec![ActivityBucketRow { start_ms: base, calls: 2, seconds: 6.0 }, ActivityBucketRow { start_ms: base + 180_000, calls: 1, seconds: 0.0 }];
        assert_eq!(b, vec![(1, 7, b7), (1, 8, vec![ActivityBucketRow { start_ms: base, calls: 1, seconds: 1.0 }])]);
        // 10 s buckets split the first two calls; every start is a multiple of the interval.
        let b = s.read(|c| activity_buckets(c, &f, 10)).unwrap();
        assert_eq!(b[0].2.iter().map(|x| (x.start_ms - base, x.calls)).collect::<Vec<_>>(), vec![(0, 1), (50_000, 1), (180_000, 1)]);
        assert!(b.iter().flat_map(|t| &t.2).all(|x| x.start_ms % 10_000 == 0));
        // The summary agrees with the buckets; the window is half open.
        let rows = s.read(|c| talkgroup_activity(c, &f)).unwrap();
        assert_eq!(rows.iter().map(|r| (r.tgid, r.call_count, r.emergency_count)).collect::<Vec<_>>(), vec![(7, 3, 0), (8, 1, 1)]);
        assert_eq!((rows[0].total_duration, rows[0].avg_duration, rows[0].first_call_ms, rows[0].last_call_ms), (6.0, 2.0, base + 5_000, base + 180_000));
        assert_eq!(rows[0].alpha_tag, "");
        assert_eq!(s.read(|c| activity_talkgroups(c, &f)).unwrap(), 2);
    }

    #[test]
    fn a_store_without_the_total_counts_it_once_at_open() {
        let dir = temp_dir("stats-open");
        let path = dir.join("engine.sqlite");
        let s = Store::open(&path).unwrap();
        s.write_sync(|c| {
            ensure_system(c, "sys1", "p25", "", 1_000)?;
            c.execute("INSERT INTO calls (system_id, tgid, start_ms, length_ms) VALUES (1, 101, 0, 4000), (1, 101, 1, 6000)", [])?;
            c.execute("DELETE FROM meta WHERE key = 'total_length_ms'", []).map(|_| ())
        })
        .unwrap();
        drop(s);
        let s = Store::open(&path).unwrap();
        assert_eq!(totals(&s), (10_000, 10_000));
    }
}
