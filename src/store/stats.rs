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
    /// Which states of call to count.
    pub state: CallStates,
}

/// The `calls.state` values a filter counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CallStates {
    /// Every call.
    #[default]
    Any,
    /// Every call that is no longer recording: `ended` (followed but not recorded, encrypted among them) and `concluded`.
    Finished,
    /// One `calls.state`.
    Is(String),
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
/// The state test is written `+c.state` so that SQLite never picks `calls_state` (every finished call in the
/// store) over the window's `calls_start` / `calls_tg_start`.
fn activity_where(f: &ActivityFilter) -> String {
    fn list<T: std::fmt::Display>(column: &str, v: &[T]) -> String {
        if v.is_empty() { String::new() } else { format!(" AND c.{column} IN ({})", v.iter().map(T::to_string).collect::<Vec<_>>().join(",")) }
    }
    format!(
        "c.start_ms >= :after AND c.start_ms < :before{}{}{}{}",
        list("system_id", &f.system_ids),
        list("site_id", &f.site_ids),
        list("tgid", &f.tgids),
        match f.state {
            CallStates::Any => "",
            CallStates::Finished => " AND +c.state <> 'recording'",
            CallStates::Is(_) => " AND +c.state = :state",
        }
    )
}

fn activity_params<'a>(f: &'a ActivityFilter, interval_ms: Option<&'a i64>) -> Vec<(&'static str, &'a dyn rusqlite::ToSql)> {
    let mut p: Vec<(&'static str, &dyn rusqlite::ToSql)> = vec![(":after", &f.after_ms), (":before", &f.before_ms)];
    if let CallStates::Is(s) = &f.state {
        p.push((":state", s));
    }
    if let Some(i) = interval_ms {
        p.push((":interval", i));
    }
    p
}

const DURATION_S: &str = "(COALESCE(c.end_ms, c.start_ms) - c.start_ms) / 1000.0";

/// One row per `(system, talkgroup)` with calls in the window, busiest first (then by tgid, system).
pub fn talkgroup_activity(conn: &Connection, f: &ActivityFilter) -> rusqlite::Result<Vec<TalkgroupActivityRow>> {
    conn.prepare(&talkgroup_activity_sql(f))?
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

fn talkgroup_activity_sql(f: &ActivityFilter) -> String {
    format!(
        "SELECT c.system_id, c.tgid, COALESCE(t.alpha_tag, ''), COALESCE(t.description, ''), COALESCE(t.tag, ''), COALESCE(t.\"group\", ''), \
                COUNT(*) AS n, SUM({DURATION_S}), SUM(c.emergency <> 0), MIN(c.start_ms), MAX(c.start_ms) \
         FROM calls c LEFT JOIN talkgroups t ON t.system_id = c.system_id AND t.tgid = c.tgid \
         WHERE {} GROUP BY c.system_id, c.tgid ORDER BY n DESC, c.tgid, c.system_id",
        activity_where(f)
    )
}

/// For the talkgroups `talkgroups` names (`(system id, tgid)`; a page of [`talkgroup_activity`]), the non-empty
/// buckets of `interval_s` seconds, aligned to the epoch: a call is in the bucket its start falls in. Ordered by
/// system, tgid, time; talkgroups without a call in the window are left out.
pub fn activity_buckets(
    conn: &Connection,
    f: &ActivityFilter,
    interval_s: u32,
    talkgroups: &[(i64, u32)],
) -> rusqlite::Result<Vec<(i64, u32, Vec<ActivityBucketRow>)>> {
    let mut out: Vec<(i64, u32, Vec<ActivityBucketRow>)> = Vec::new();
    if talkgroups.is_empty() {
        return Ok(out);
    }
    let interval_ms = i64::from(interval_s.max(1)) * 1000;
    let mut stmt = conn.prepare(&activity_buckets_sql(f, talkgroups))?;
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

/// The buckets query, for a non-empty `talkgroups`. The pairs are numbers, so they are written in.
fn activity_buckets_sql(f: &ActivityFilter, talkgroups: &[(i64, u32)]) -> String {
    let pairs = talkgroups.iter().map(|(s, t)| format!("({s},{t})")).collect::<Vec<_>>().join(",");
    format!(
        "SELECT c.system_id, c.tgid, (c.start_ms / :interval) * :interval AS b, COUNT(*), SUM({DURATION_S}) \
         FROM calls c WHERE {} AND (c.system_id, c.tgid) IN (VALUES {pairs}) \
         GROUP BY c.system_id, c.tgid, b ORDER BY c.system_id, c.tgid, b",
        activity_where(f)
    )
}

/// One talkgroup that is active now, against the same clock window on previous days.
#[derive(Clone, Debug, PartialEq)]
pub struct BaselineRow {
    pub system_id: SystemId,
    pub tgid: u32,
    pub alpha_tag: String,
    /// Calls started in `[now - window, now)`.
    pub calls_now: i64,
    /// Mean calls in the same window over the previous `days` days; days without calls count as zero.
    pub baseline_mean: f64,
    pub baseline_max: i64,
    /// How many of those days had any calls.
    pub days_with_data: i64,
}

/// Every talkgroup with calls in `[now - window_s, now)` (empty `system_ids` is every system), busiest first (then by
/// tgid, system), with its calls in each of the equally wide windows ending `d` days before `now`, `d = 1..=days`.
pub fn baseline(conn: &Connection, system_ids: &[i64], now_ms: i64, window_s: u32, days: u32) -> rusqlite::Result<Vec<BaselineRow>> {
    let systems = if system_ids.is_empty() {
        String::new()
    } else {
        format!(" AND system_id IN ({})", system_ids.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
    };
    let sql = format!(
        "WITH RECURSIVE days(d) AS (SELECT 1 UNION ALL SELECT d + 1 FROM days WHERE d < :days), \
              cur AS (SELECT system_id, tgid, COUNT(*) AS n FROM calls \
                       WHERE start_ms >= :now - :window AND start_ms < :now{systems} GROUP BY system_id, tgid), \
              per AS (SELECT cur.system_id, cur.tgid, cur.n AS now_n, \
                             (SELECT COUNT(*) FROM calls c WHERE c.system_id = cur.system_id AND c.tgid = cur.tgid \
                                 AND c.start_ms >= :now - days.d * 86400000 - :window AND c.start_ms < :now - days.d * 86400000) AS day_n \
                        FROM cur, days) \
         SELECT per.system_id, per.tgid, COALESCE(t.alpha_tag, ''), per.now_n, SUM(per.day_n) * 1.0 / :days, MAX(per.day_n), SUM(per.day_n > 0) \
         FROM per LEFT JOIN talkgroups t ON t.system_id = per.system_id AND t.tgid = per.tgid \
         GROUP BY per.system_id, per.tgid ORDER BY per.now_n DESC, per.tgid, per.system_id"
    );
    let window_ms = i64::from(window_s) * 1000;
    conn.prepare_cached(&sql)?
        .query_map(rusqlite::named_params! {":days": i64::from(days), ":now": now_ms, ":window": window_ms}, |r| {
            Ok(BaselineRow {
                system_id: r.get(0)?,
                tgid: r.get(1)?,
                alpha_tag: r.get(2)?,
                calls_now: r.get(3)?,
                baseline_mean: r.get(4)?,
                baseline_max: r.get(5)?,
                days_with_data: r.get(6)?,
            })
        })?
        .collect()
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
        let both = [(1, 7), (1, 8)];
        let b = s.read(|c| activity_buckets(c, &f, 60, &both)).unwrap();
        let b7 = vec![ActivityBucketRow { start_ms: base, calls: 2, seconds: 6.0 }, ActivityBucketRow { start_ms: base + 180_000, calls: 1, seconds: 0.0 }];
        assert_eq!(b, vec![(1, 7, b7), (1, 8, vec![ActivityBucketRow { start_ms: base, calls: 1, seconds: 1.0 }])]);
        // 10 s buckets split the first two calls; every start is a multiple of the interval.
        let b = s.read(|c| activity_buckets(c, &f, 10, &both)).unwrap();
        assert_eq!(b[0].2.iter().map(|x| (x.start_ms - base, x.calls)).collect::<Vec<_>>(), vec![(0, 1), (50_000, 1), (180_000, 1)]);
        assert!(b.iter().flat_map(|t| &t.2).all(|x| x.start_ms % 10_000 == 0));
        // The summary agrees with the buckets; the window is half open.
        let rows = s.read(|c| talkgroup_activity(c, &f)).unwrap();
        assert_eq!(rows.iter().map(|r| (r.tgid, r.call_count, r.emergency_count)).collect::<Vec<_>>(), vec![(7, 3, 0), (8, 1, 1)]);
        assert_eq!((rows[0].total_duration, rows[0].avg_duration, rows[0].first_call_ms, rows[0].last_call_ms), (6.0, 2.0, base + 5_000, base + 180_000));
        assert_eq!(rows[0].alpha_tag, "");
        // Only the talkgroups asked for get buckets; one with no call in the window gets none.
        let b = s.read(|c| activity_buckets(c, &f, 60, &[(1, 8), (1, 9)])).unwrap();
        assert_eq!(b, vec![(1, 8, vec![ActivityBucketRow { start_ms: base, calls: 1, seconds: 1.0 }])]);
        assert!(s.read(|c| activity_buckets(c, &f, 60, &[])).unwrap().is_empty());
    }

    #[test]
    fn finished_calls_are_ended_or_concluded() {
        let s = Store::in_memory();
        s.write_sync(|c| {
            ensure_system(c, "sys1", "p25", "", 1_000)?;
            // tg 7: concluded; tg 8: ended (encrypted, not recorded); tg 9: still recording.
            for (tg, state, enc) in [(7, "concluded", 0), (8, "ended", 1), (9, "recording", 0)] {
                c.execute("INSERT INTO calls (system_id, tgid, start_ms, encrypted, state) VALUES (1, ?1, 5000, ?3, ?2)", rusqlite::params![tg, state, enc])?;
            }
            Ok(())
        })
        .unwrap();
        let tgs = |state: CallStates| {
            let f = ActivityFilter { after_ms: 0, before_ms: 10_000, state, ..Default::default() };
            s.read(|c| talkgroup_activity(c, &f)).unwrap().iter().map(|r| r.tgid).collect::<Vec<_>>()
        };
        assert_eq!(tgs(CallStates::Finished), vec![7, 8]);
        assert_eq!(tgs(CallStates::Any), vec![7, 8, 9]);
        assert_eq!(tgs(CallStates::Is("ended".into())), vec![8]);
    }

    #[test]
    fn activity_is_found_through_the_window_not_the_state() {
        let s = Store::in_memory();
        for state in [CallStates::Finished, CallStates::Is("ended".into())] {
            for f in [
                ActivityFilter { state: state.clone(), ..Default::default() },
                ActivityFilter { system_ids: vec![1], tgids: vec![101], state: state.clone(), ..Default::default() },
            ] {
                let summary = s.read(|c| Ok(plan_named(c, &talkgroup_activity_sql(&f), &activity_params(&f, None)))).unwrap();
                let buckets = s.read(|c| Ok(plan_named(c, &activity_buckets_sql(&f, &[(1, 101), (1, 202)]), &activity_params(&f, Some(&60_000))))).unwrap();
                for plan in [summary, buckets] {
                    assert!(plan.contains("calls_start") || plan.contains("calls_tg_start"), "{plan}");
                    assert!(!plan.contains("calls_state"), "{plan}");
                }
            }
        }
    }

    /// The plan of a query with named parameters, all its lines joined.
    fn plan_named(c: &rusqlite::Connection, sql: &str, params: &[(&str, &dyn rusqlite::ToSql)]) -> String {
        c.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(params, |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n")
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

    #[test]
    fn baseline_counts_equal_windows_on_prior_days() {
        let s = Store::in_memory();
        let now = 1_700_000_000_000_i64;
        let (min, day) = (60_000_i64, 86_400_000_i64);
        s.write_sync(move |c| {
            ensure_system(c, "sys1", "p25", "", 1_000)?;
            c.execute("INSERT INTO talkgroups (system_id, tgid, alpha_tag, first_heard_ms, last_heard_ms) VALUES (1, 7, 'Dispatch', 0, 0)", [])?;
            // tg 7: one call now, two a day ago, one two days ago; tg 8: one now, no history;
            // tg 9: only history (not listed); and tg 7 again exactly at `now`, outside the window.
            for (tg, start) in [
                (7, now - 5 * min),
                (7, now - day - 5 * min),
                (7, now - day - 6 * min),
                (7, now - 2 * day - 5 * min),
                (7, now),
                (8, now - 2 * min),
                (9, now - day - 5 * min),
            ] {
                c.execute("INSERT INTO calls (system_id, tgid, start_ms, state) VALUES (1, ?1, ?2, 'concluded')", rusqlite::params![tg, start])?;
            }
            Ok(())
        })
        .unwrap();
        let rows = s.read(|c| baseline(c, &[], now, 600, 7)).unwrap();
        let got: Vec<_> = rows.iter().map(|r| (r.tgid, r.alpha_tag.as_str(), r.calls_now, r.baseline_mean, r.baseline_max, r.days_with_data)).collect();
        assert_eq!(got, vec![(7, "Dispatch", 1, 3.0 / 7.0, 2, 2), (8, "", 1, 0.0, 0, 0)]);
        // Fewer days, fewer windows: one day back sees only the two calls of day 1.
        let rows = s.read(|c| baseline(c, &[1], now, 600, 1)).unwrap();
        assert_eq!((rows[0].baseline_mean, rows[0].baseline_max, rows[0].days_with_data), (2.0, 2, 1));
        assert!(s.read(|c| baseline(c, &[2], now, 600, 7)).unwrap().is_empty());
    }
}
