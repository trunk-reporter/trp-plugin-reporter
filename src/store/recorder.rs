//! Periodic recorder-state samples: insert, query, thin out with age, and expire.

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row};

use super::SiteId;
use super::calls::push_in;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

#[derive(Clone, Debug, PartialEq)]
pub struct RecorderStateRow {
    pub site_id: SiteId,
    pub time_ms: i64,
    pub control_channel_hz: Option<u64>,
    pub decode_rate: f64,
    pub active_calls: u32,
    pub recording: u32,
}

const STATE_COLS: &str = "rs.site_id, rs.time_ms, rs.control_channel_hz, rs.decode_rate, rs.active_calls, rs.recording";

fn state_row(r: &Row<'_>) -> rusqlite::Result<RecorderStateRow> {
    Ok(RecorderStateRow {
        site_id: r.get(0)?,
        time_ms: r.get(1)?,
        control_channel_hz: r.get::<_, Option<i64>>(2)?.map(|h| h as u64),
        decode_rate: r.get(3)?,
        active_calls: r.get(4)?,
        recording: r.get(5)?,
    })
}

pub fn insert_recorder_state(
    conn: &Connection,
    site_id: SiteId,
    time_ms: i64,
    control_channel_hz: Option<u64>,
    decode_rate: f64,
    active_calls: u32,
    recording: u32,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO recorder_state (site_id, time_ms, control_channel_hz, decode_rate, active_calls, recording) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        (site_id, time_ms, control_channel_hz.map(|h| h as i64), decode_rate, active_calls, recording),
    )
    .map(|_| ())
}

/// The newest sample of each site, by site id.
pub fn latest_recorder_state(conn: &Connection) -> rusqlite::Result<Vec<RecorderStateRow>> {
    conn.prepare(&format!(
        "SELECT {STATE_COLS} FROM recorder_state rs WHERE rs.id IN (SELECT MAX(id) FROM recorder_state GROUP BY site_id) ORDER BY rs.site_id"
    ))?
    .query_map([], state_row)?
    .collect()
}

pub fn first_state_ms(conn: &Connection, site: SiteId) -> rusqlite::Result<Option<i64>> {
    conn.query_row("SELECT MIN(time_ms) FROM recorder_state WHERE site_id = ?1", [site], |r| r.get(0)).optional().map(Option::flatten)
}

/// Samples, newest first, optionally limited to some systems and a time range.
pub fn decode_rates(conn: &Connection, system_ids: &[i64], start_ms: Option<i64>, end_ms: Option<i64>, limit: u32) -> rusqlite::Result<Vec<RecorderStateRow>> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "s.system_id ", "", system_ids);
    if let Some(t) = start_ms {
        params.push(Value::Integer(t));
        conds.push(format!("rs.time_ms >= ?{}", params.len()));
    }
    if let Some(t) = end_ms {
        params.push(Value::Integer(t));
        conds.push(format!("rs.time_ms <= ?{}", params.len()));
    }
    params.push(Value::Integer(limit as i64));
    let where_sql = if conds.is_empty() { String::new() } else { format!("WHERE {}", conds.join(" AND ")) };
    let sql = format!(
        "SELECT {STATE_COLS} FROM recorder_state rs JOIN sites s ON s.id = rs.site_id {where_sql} ORDER BY rs.time_ms DESC, rs.id DESC LIMIT ?{}",
        params.len()
    );
    conn.prepare(&sql)?.query_map(rusqlite::params_from_iter(params), state_row)?.collect()
}

/// Thin old samples: past a week keep the first per site per minute, past a month the first per site per hour.
/// Returns the rows deleted.
pub fn decimate(conn: &Connection, now_ms: i64) -> rusqlite::Result<u64> {
    let week = now_ms - 7 * DAY_MS;
    let month = now_ms - 30 * DAY_MS;
    let a = conn.execute(
        "DELETE FROM recorder_state WHERE time_ms < ?1 AND time_ms >= ?2 AND id NOT IN \
         (SELECT MIN(id) FROM recorder_state WHERE time_ms < ?1 AND time_ms >= ?2 GROUP BY site_id, time_ms / ?3)",
        (week, month, MINUTE_MS),
    )?;
    let b = conn.execute(
        "DELETE FROM recorder_state WHERE time_ms < ?1 AND id NOT IN \
         (SELECT MIN(id) FROM recorder_state WHERE time_ms < ?1 GROUP BY site_id, time_ms / ?2)",
        (month, HOUR_MS),
    )?;
    Ok((a + b) as u64)
}

/// Delete calls (their children cascade), unit events and samples older than `keep_days`.
/// Zero keeps everything. Returns the calls deleted.
pub fn retain(conn: &Connection, keep_days: u32, now_ms: i64) -> rusqlite::Result<u64> {
    if keep_days == 0 {
        return Ok(0);
    }
    let cutoff = now_ms - keep_days as i64 * DAY_MS;
    let calls = conn.execute("DELETE FROM calls WHERE start_ms < ?1", [cutoff])?;
    conn.execute("DELETE FROM unit_events WHERE time_ms < ?1", [cutoff])?;
    conn.execute("DELETE FROM recorder_state WHERE time_ms < ?1", [cutoff])?;
    Ok(calls as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::systems::{ensure_site, ensure_system};

    const NOW: i64 = 1_800_000_000_000;

    fn seed_site(s: &Store, short: &str) -> SiteId {
        let short = short.to_string();
        s.write_sync(move |c| {
            let sys = ensure_system(c, &short, "p25", "", NOW)?;
            ensure_site(c, sys, &short, NOW)
        })
        .unwrap()
    }

    fn count_for(s: &Store, site: SiteId) -> i64 {
        s.read(|c| c.query_row("SELECT count(*) FROM recorder_state WHERE site_id = ?1", [site], |r| r.get(0))).unwrap()
    }

    #[test]
    fn decimation_keeps_one_row_a_minute_after_a_week() {
        let s = Store::in_memory();
        let (a, b) = (seed_site(&s, "a"), seed_site(&s, "b"));
        // Aligned to a minute so 120 rows 1 s apart span exactly two buckets.
        let base = (NOW - 8 * DAY_MS) / MINUTE_MS * MINUTE_MS;
        s.write_sync(move |c| {
            for i in 0..120 {
                insert_recorder_state(c, a, base + i * 1_000, None, i as f64, 0, 0)?;
            }
            insert_recorder_state(c, b, base, None, 1.0, 0, 0)?;
            for i in 0..5 {
                insert_recorder_state(c, a, NOW - HOUR_MS + i * 1_000, None, 1.0, 0, 0)?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(s.write_sync(|c| decimate(c, NOW)).unwrap(), 118);
        // Two old minutes kept, plus the five recent rows.
        assert_eq!(count_for(&s, a), 7);
        assert_eq!(count_for(&s, b), 1);
        // The first row of each minute is the one kept.
        let kept: Vec<f64> = s
            .read(|c| {
                c.prepare("SELECT decode_rate FROM recorder_state WHERE site_id = ?1 AND time_ms < ?2 ORDER BY time_ms")?
                    .query_map((a, NOW - DAY_MS), |r| r.get(0))?
                    .collect()
            })
            .unwrap();
        assert_eq!(kept, vec![0.0, 60.0]);
    }

    #[test]
    fn decimation_keeps_one_row_an_hour_after_a_month() {
        let s = Store::in_memory();
        let a = seed_site(&s, "a");
        let base = (NOW - 31 * DAY_MS) / HOUR_MS * HOUR_MS;
        s.write_sync(move |c| {
            for i in 0..180 {
                insert_recorder_state(c, a, base + i * MINUTE_MS, None, 1.0, 0, 0)?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(s.write_sync(|c| decimate(c, NOW)).unwrap(), 177);
        assert_eq!(count_for(&s, a), 3);
        // A second pass has nothing left to thin.
        assert_eq!(s.write_sync(|c| decimate(c, NOW)).unwrap(), 0);
    }

    #[test]
    fn latest_state_and_decode_rates_follow_the_filters() {
        let s = Store::in_memory();
        let (a, b, other) = s
            .write_sync(|c| {
                let s1 = ensure_system(c, "one", "p25", "", NOW)?;
                let s2 = ensure_system(c, "two", "p25", "", NOW)?;
                let a = ensure_site(c, s1, "a", NOW)?;
                let b = ensure_site(c, s1, "b", NOW)?;
                let o = ensure_site(c, s2, "o", NOW)?;
                insert_recorder_state(c, a, 1_000, Some(851_000_000), 40.0, 1, 1)?;
                insert_recorder_state(c, a, 2_000, Some(851_000_000), 38.5, 2, 1)?;
                insert_recorder_state(c, b, 1_500, None, 30.0, 0, 0)?;
                insert_recorder_state(c, o, 3_000, None, 20.0, 0, 0)?;
                Ok((a, b, o))
            })
            .unwrap();
        let latest = s.read(latest_recorder_state).unwrap();
        assert_eq!(latest.iter().map(|r| (r.site_id, r.time_ms)).collect::<Vec<_>>(), vec![(a, 2_000), (b, 1_500), (other, 3_000)]);
        assert_eq!((latest[0].control_channel_hz, latest[0].decode_rate, latest[0].active_calls, latest[0].recording), (Some(851_000_000), 38.5, 2, 1));
        assert_eq!(s.read(|c| first_state_ms(c, a)).unwrap(), Some(1_000));
        assert_eq!(s.read(|c| first_state_ms(c, SiteId(999))).unwrap(), None);

        let times = |rows: Vec<RecorderStateRow>| rows.iter().map(|r| r.time_ms).collect::<Vec<_>>();
        let sys_one = s.read(|c| c.query_row("SELECT system_id FROM sites WHERE id = ?1", [a], |r| r.get::<_, i64>(0))).unwrap();
        assert_eq!(times(s.read(|c| decode_rates(c, &[], None, None, 10)).unwrap()), vec![3_000, 2_000, 1_500, 1_000]);
        assert_eq!(times(s.read(|c| decode_rates(c, &[sys_one], None, None, 10)).unwrap()), vec![2_000, 1_500, 1_000]);
        assert_eq!(times(s.read(|c| decode_rates(c, &[], Some(1_500), Some(2_000), 10)).unwrap()), vec![2_000, 1_500]);
        assert_eq!(times(s.read(|c| decode_rates(c, &[], None, None, 2)).unwrap()), vec![3_000, 2_000]);
    }

    #[test]
    fn retention_zero_keeps_everything() {
        let s = Store::in_memory();
        let a = seed_site(&s, "a");
        s.write_sync(move |c| insert_recorder_state(c, a, 1, None, 1.0, 0, 0)).unwrap();
        assert_eq!(s.write_sync(|c| retain(c, 0, NOW)).unwrap(), 0);
        assert_eq!(count_for(&s, a), 1);
    }

    #[test]
    fn retention_deletes_old_calls_and_their_children() {
        let s = Store::in_memory();
        let a = seed_site(&s, "a");
        let (old, young) = s
            .write_sync(move |c| {
                let sys: i64 = c.query_row("SELECT system_id FROM sites WHERE id = ?1", [a], |r| r.get(0))?;
                let mut ids = Vec::new();
                for (age_days, path) in [(40, "old"), (1, "young")] {
                    let start = NOW - age_days * DAY_MS;
                    let id: i64 = c.query_row(
                        "INSERT INTO calls (path, system_id, site_id, tgid, start_ms) VALUES (?1, ?2, ?3, 100, ?4) RETURNING id",
                        (path, sys, a, start),
                        |r| r.get(0),
                    )?;
                    for pos in 0..2 {
                        c.execute(
                            "INSERT INTO call_transmissions (call_id, position, unit, time_ms, pos_s, len_s) VALUES (?1, ?2, 7, ?3, 0, 1)",
                            (id, pos, start),
                        )?;
                    }
                    c.execute(
                        "INSERT INTO call_frequencies (call_id, position, freq_hz, time_ms, pos_s, len_s) VALUES (?1, 0, 851000000, ?2, 0, 1)",
                        (id, start),
                    )?;
                    ids.push(id);
                }
                for (age_days, kind) in [(45, "old_ev"), (2, "young_ev")] {
                    c.execute("INSERT INTO unit_events (system_id, unit, event_type, time_ms) VALUES (?1, 7, ?2, ?3)", (sys, kind, NOW - age_days * DAY_MS))?;
                }
                insert_recorder_state(c, a, NOW - 35 * DAY_MS, None, 1.0, 0, 0)?;
                insert_recorder_state(c, a, NOW - DAY_MS, None, 1.0, 0, 0)?;
                Ok((ids[0], ids[1]))
            })
            .unwrap();

        assert_eq!(s.write_sync(|c| retain(c, 30, NOW)).unwrap(), 1);

        let n = |sql: &'static str, id: i64| s.read(|c| c.query_row(sql, [id], |r| r.get::<_, i64>(0))).unwrap();
        assert_eq!(n("SELECT count(*) FROM calls WHERE id = ?1", old), 0);
        assert_eq!(n("SELECT count(*) FROM call_transmissions WHERE call_id = ?1", old), 0);
        assert_eq!(n("SELECT count(*) FROM call_frequencies WHERE call_id = ?1", old), 0);
        assert_eq!(n("SELECT count(*) FROM calls WHERE id = ?1", young), 1);
        assert_eq!(n("SELECT count(*) FROM call_transmissions WHERE call_id = ?1", young), 2);
        assert_eq!(n("SELECT count(*) FROM call_frequencies WHERE call_id = ?1", young), 1);
        let events: Vec<String> = s.read(|c| c.prepare("SELECT event_type FROM unit_events")?.query_map([], |r| r.get(0))?.collect()).unwrap();
        assert_eq!(events, vec!["young_ev".to_string()]);
        assert_eq!(count_for(&s, a), 1);
    }
}
