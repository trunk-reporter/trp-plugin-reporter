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
