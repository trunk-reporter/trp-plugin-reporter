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

pub fn stats(conn: &Connection, now_ms: i64) -> rusqlite::Result<StatsRow> {
    conn.query_row(
        "SELECT (SELECT COUNT(*) FROM sites), (SELECT COUNT(*) FROM talkgroups), (SELECT COUNT(*) FROM units), \
                (SELECT COUNT(*) FROM calls), \
                (SELECT COUNT(*) FROM calls WHERE start_ms >= ?1), \
                (SELECT COUNT(*) FROM calls WHERE start_ms >= ?2), \
                (SELECT COALESCE(SUM(length_ms), 0) FROM calls) / 3600000.0",
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
/// transmission or a unit event in the last hour.
pub fn system_activity(conn: &Connection, now_ms: i64) -> rusqlite::Result<Vec<ActivityRow>> {
    conn.prepare(
        "SELECT s.id, \
                (SELECT COUNT(*) FROM calls WHERE system_id = s.id AND start_ms >= ?2), \
                (SELECT COUNT(*) FROM calls WHERE system_id = s.id AND start_ms >= ?1), \
                (SELECT COUNT(DISTINCT tgid) FROM calls WHERE system_id = s.id AND start_ms >= ?2), \
                (SELECT COUNT(*) FROM (\
                    SELECT t.unit FROM call_transmissions t JOIN calls c ON c.id = t.call_id WHERE c.system_id = s.id AND t.time_ms >= ?2 \
                    UNION SELECT unit FROM unit_events WHERE system_id = s.id AND time_ms >= ?2)) \
         FROM systems s ORDER BY s.id",
    )?
    .query_map((now_ms - DAY_MS, now_ms - HOUR_MS), |r| {
        Ok(ActivityRow { system_id: r.get(0)?, calls_1h: r.get(1)?, calls_24h: r.get(2)?, active_talkgroups: r.get(3)?, active_units: r.get(4)? })
    })?
    .collect()
}
