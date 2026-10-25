//! Talkgroups: counters and tags kept as calls arrive, and the live statistics the API shows.

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, named_params, params_from_iter};
use trunk_recorder_plugin::{CallInfo, CallRecord};

use super::SystemId;
use super::calls::push_in;
use super::units::{UNIT_COLS, UnitRow, like_escape, unit_row};

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

/// What a call says about its talkgroup. Empty fields leave the stored value alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TgTags {
    pub alpha_tag: String,
    pub description: String,
    pub group: String,
    pub tag: String,
}

impl TgTags {
    pub fn from_record(r: &CallRecord) -> TgTags {
        TgTags {
            alpha_tag: r.talkgroup_tag.clone(),
            description: r.talkgroup_description.clone(),
            group: r.talkgroup_group.clone(),
            tag: r.talkgroup_group_tag.clone(),
        }
    }

    /// A call-start knows only the alpha tag.
    pub fn from_info(i: &CallInfo) -> TgTags {
        TgTags { alpha_tag: i.talkgroup_tag.clone(), ..Default::default() }
    }
}

/// A call was heard on the talkgroup: count it and refresh the tags it carries.
pub fn touch_talkgroup(
    conn: &Connection,
    system_id: SystemId,
    tgid: u32,
    tags: &TgTags,
    heard_ms: i64,
    encrypted: bool,
    algid: Option<u8>,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO talkgroups (system_id, tgid, alpha_tag, description, \"group\", tag, first_heard_ms, last_heard_ms, call_count, encrypted_count, last_algid) \
         VALUES (:system_id, :tgid, :alpha, :desc, :group, :tag, :heard, :heard, 1, :enc, :algid) \
         ON CONFLICT(system_id, tgid) DO UPDATE SET \
         alpha_tag = CASE WHEN :alpha <> '' THEN :alpha ELSE alpha_tag END, \
         description = CASE WHEN :desc <> '' THEN :desc ELSE description END, \
         \"group\" = CASE WHEN :group <> '' THEN :group ELSE \"group\" END, \
         tag = CASE WHEN :tag <> '' THEN :tag ELSE tag END, \
         first_heard_ms = MIN(first_heard_ms, :heard), last_heard_ms = MAX(last_heard_ms, :heard), \
         call_count = call_count + 1, encrypted_count = encrypted_count + :enc, last_algid = COALESCE(:algid, last_algid)",
        named_params! {
            ":system_id": system_id, ":tgid": tgid, ":alpha": tags.alpha_tag, ":desc": tags.description, ":group": tags.group, ":tag": tags.tag,
            ":heard": heard_ms, ":enc": encrypted, ":algid": algid,
        },
    )
    .map(|_| ())
}

/// A talkgroup. The statistics and relevance are filled by [`list_talkgroups`] only;
/// elsewhere they are 0 and `None`.
#[derive(Clone, Debug, PartialEq)]
pub struct TalkgroupRow {
    pub system_id: SystemId,
    pub tgid: u32,
    pub alpha_tag: String,
    pub description: String,
    pub group: String,
    pub tag: String,
    pub first_heard_ms: i64,
    pub last_heard_ms: i64,
    pub call_count: i64,
    pub encrypted_count: i64,
    pub last_algid: Option<u8>,
    pub call_count_30d: i64,
    pub calls_1h: i64,
    pub calls_24h: i64,
    pub unit_count_30d: i64,
    pub relevance_score: Option<i64>,
}

const TG_COLS: &str = "system_id, tgid, alpha_tag, description, \"group\", tag, first_heard_ms, last_heard_ms, call_count, encrypted_count, last_algid";

/// Columns 0..11 are the table's; 11..16 are [`stat_columns`] and the relevance.
fn talkgroup_row(r: &Row<'_>) -> rusqlite::Result<TalkgroupRow> {
    let (call_count_30d, calls_1h, calls_24h, unit_count_30d, relevance_score) = (r.get(11)?, r.get(12)?, r.get(13)?, r.get(14)?, r.get(15)?);
    Ok(TalkgroupRow {
        system_id: r.get(0)?,
        tgid: r.get(1)?,
        alpha_tag: r.get(2)?,
        description: r.get(3)?,
        group: r.get(4)?,
        tag: r.get(5)?,
        first_heard_ms: r.get(6)?,
        last_heard_ms: r.get(7)?,
        call_count: r.get(8)?,
        encrypted_count: r.get(9)?,
        last_algid: r.get(10)?,
        call_count_30d,
        calls_1h,
        calls_24h,
        unit_count_30d,
        relevance_score,
    })
}

/// The live-statistics SELECT columns (`call_count_30d, calls_1h, calls_24h, unit_count_30d`)
/// for rows of `talkgroups`. The arguments are the numbers of the parameters holding the
/// window cut-offs (see [`window_cutoffs`]).
fn stat_columns(h1: usize, h24: usize, d30: usize) -> String {
    format!(
        "(SELECT COUNT(*) FROM calls c WHERE c.system_id = talkgroups.system_id AND c.tgid = talkgroups.tgid AND c.start_ms > ?{d30}) AS call_count_30d, \
         (SELECT COUNT(*) FROM calls c WHERE c.system_id = talkgroups.system_id AND c.tgid = talkgroups.tgid AND c.start_ms > ?{h1}) AS calls_1h, \
         (SELECT COUNT(*) FROM calls c WHERE c.system_id = talkgroups.system_id AND c.tgid = talkgroups.tgid AND c.start_ms > ?{h24}) AS calls_24h, \
         (SELECT COUNT(*) FROM (\
            SELECT t.unit FROM call_transmissions t JOIN calls c ON c.id = t.call_id \
              WHERE c.system_id = talkgroups.system_id AND c.tgid = talkgroups.tgid AND c.start_ms > ?{d30} \
            UNION \
            SELECT e.unit FROM unit_events e \
              WHERE e.system_id = talkgroups.system_id AND e.tgid = talkgroups.tgid AND e.time_ms > ?{d30})) AS unit_count_30d"
    )
}

/// The window cut-offs [`stat_columns`] binds, in order: 1 hour, 24 hours, 30 days before `now_ms`.
fn window_cutoffs(now_ms: i64) -> [Value; 3] {
    [Value::Integer(now_ms - HOUR_MS), Value::Integer(now_ms - DAY_MS), Value::Integer(now_ms - 30 * DAY_MS)]
}

/// One talkgroup, with its statistics as of `now_ms`.
pub fn talkgroup(conn: &Connection, system_id: SystemId, tgid: u32, now_ms: i64) -> rusqlite::Result<Option<TalkgroupRow>> {
    // ?1..?3 are the cut-offs, ?4 and ?5 the key.
    let sql = format!("SELECT {TG_COLS}, {}, NULL FROM talkgroups WHERE system_id = ?4 AND tgid = ?5", stat_columns(1, 2, 3));
    let [h1, h24, d30] = window_cutoffs(now_ms);
    conn.query_row(&sql, params_from_iter([h1, h24, d30, Value::Integer(system_id.0), Value::Integer(tgid as i64)]), talkgroup_row).optional()
}

/// The talkgroup number under every system that has it, with statistics as of `now_ms`.
pub fn talkgroups_by_tgid(conn: &Connection, tgid: u32, now_ms: i64) -> rusqlite::Result<Vec<TalkgroupRow>> {
    // ?1..?3 are the cut-offs, ?4 the talkgroup number.
    let sql = format!("SELECT {TG_COLS}, {}, NULL FROM talkgroups WHERE tgid = ?4 ORDER BY system_id", stat_columns(1, 2, 3));
    let [h1, h24, d30] = window_cutoffs(now_ms);
    conn.prepare(&sql)?.query_map(params_from_iter([h1, h24, d30, Value::Integer(tgid as i64)]), talkgroup_row)?.collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TgSort {
    AlphaTag,
    Tgid,
    Group,
    LastSeen,
    CallCount,
    Calls1h,
    Calls24h,
    UnitCount,
}

impl TgSort {
    fn column(self) -> &'static str {
        match self {
            TgSort::AlphaTag => "alpha_tag",
            TgSort::Tgid => "tgid",
            TgSort::Group => "\"group\"",
            TgSort::LastSeen => "last_heard_ms",
            TgSort::CallCount => "call_count_30d",
            TgSort::Calls1h => "calls_1h",
            TgSort::Calls24h => "calls_24h",
            TgSort::UnitCount => "unit_count_30d",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TalkgroupFilter {
    pub system_ids: Vec<i64>,
    pub group: Option<String>,
    pub search: Option<String>,
    pub sort: TgSort,
    pub desc: bool,
    pub limit: u32,
    pub offset: u32,
}

impl Default for TalkgroupFilter {
    fn default() -> Self {
        TalkgroupFilter { system_ids: vec![], group: None, search: None, sort: TgSort::Tgid, desc: false, limit: 50, offset: 0 }
    }
}

/// The page of talkgroups the filter matches, with live statistics, and how many match in all.
pub fn list_talkgroups(conn: &Connection, f: &TalkgroupFilter, now_ms: i64) -> rusqlite::Result<(Vec<TalkgroupRow>, i64)> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "system_id ", "", &f.system_ids);
    if let Some(g) = &f.group {
        params.push(Value::Text(g.clone()));
        conds.push(format!("\"group\" = ?{} COLLATE NOCASE", params.len()));
    }
    let mut relevance = "NULL".to_string();
    if let Some(q) = f.search.as_deref().filter(|q| !q.is_empty()) {
        let esc = like_escape(q);
        params.push(Value::Text(q.to_string()));
        let exact = params.len();
        params.push(Value::Text(format!("%{esc}%")));
        let contains = params.len();
        params.push(Value::Text(format!("{esc}%")));
        let prefix = params.len();
        conds.push(format!(
            "(alpha_tag LIKE ?{contains} ESCAPE '\\' OR description LIKE ?{contains} ESCAPE '\\' OR \"group\" LIKE ?{contains} ESCAPE '\\' \
             OR tag LIKE ?{contains} ESCAPE '\\' OR CAST(tgid AS TEXT) LIKE ?{prefix} ESCAPE '\\')"
        ));
        relevance = format!(
            "CASE WHEN alpha_tag = ?{exact} COLLATE NOCASE OR CAST(tgid AS TEXT) = ?{exact} THEN 100 WHEN alpha_tag LIKE ?{prefix} ESCAPE '\\' THEN 50 ELSE 10 END"
        );
    }
    let wher = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM talkgroups{wher}"), params_from_iter(params.iter()), |r| r.get(0))?;

    let [c1, c24, c30] = window_cutoffs(now_ms);
    let mut bind = |v: Value| {
        params.push(v);
        params.len()
    };
    let (h1, h24, d30) = (bind(c1), bind(c24), bind(c30));
    let (limit, offset) = (bind(Value::Integer(f.limit as i64)), bind(Value::Integer(f.offset as i64)));
    let order_rel = if f.search.as_deref().is_some_and(|q| !q.is_empty()) { "relevance_score DESC, " } else { "" };
    let sql = format!(
        "SELECT {TG_COLS}, {stats}, {relevance} AS relevance_score \
         FROM talkgroups{wher} ORDER BY {order_rel}{col} {dir}, tgid {dir}, system_id LIMIT ?{limit} OFFSET ?{offset}",
        stats = stat_columns(h1, h24, d30),
        col = f.sort.column(),
        dir = if f.desc { "DESC" } else { "ASC" },
    );
    let rows = conn.prepare(&sql)?.query_map(params_from_iter(params.iter()), talkgroup_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

/// Units with a transmission on a call of the talkgroup since `since_ms` (with the number of such
/// calls), and units with an event on it (0 calls when they have no transmission), most active first.
/// Only units the `units` table knows are listed.
pub fn units_on_talkgroup(
    conn: &Connection,
    system_id: SystemId,
    tgid: u32,
    since_ms: i64,
    limit: u32,
    offset: u32,
) -> rusqlite::Result<(Vec<(UnitRow, i64)>, i64)> {
    const ACTIVE: &str = "WITH tx AS (\
            SELECT t.unit AS unit, COUNT(DISTINCT c.id) AS n FROM call_transmissions t JOIN calls c ON c.id = t.call_id \
            WHERE c.system_id = ?1 AND c.tgid = ?2 AND c.start_ms >= ?3 GROUP BY t.unit), \
         active AS (\
            SELECT unit, n FROM tx \
            UNION ALL \
            SELECT DISTINCT unit, 0 FROM unit_events WHERE system_id = ?1 AND tgid = ?2 AND time_ms >= ?3 AND unit NOT IN (SELECT unit FROM tx))";
    let total: i64 = conn.query_row(
        &format!("{ACTIVE} SELECT COUNT(*) FROM active JOIN units u ON u.system_id = ?1 AND u.unit_id = active.unit"),
        (system_id, tgid, since_ms),
        |r| r.get(0),
    )?;
    let cols = UNIT_COLS.split(", ").map(|c| format!("u.{c}")).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "{ACTIVE} SELECT {cols}, active.n FROM active JOIN units u ON u.system_id = ?1 AND u.unit_id = active.unit ORDER BY active.n DESC, u.unit_id LIMIT ?4 OFFSET ?5"
    );
    let rows = conn
        .prepare(&sql)?
        .query_map((system_id, tgid, since_ms, limit, offset), |r| Ok((unit_row(r)?, r.get::<_, i64>(11)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

#[derive(Clone, Debug, PartialEq)]
pub struct EncryptionStatRow {
    pub system_id: SystemId,
    pub tgid: u32,
    pub alpha_tag: String,
    pub description: String,
    pub encrypted_calls: i64,
    pub clear_calls: i64,
}

/// Encrypted and clear calls per talkgroup since `since_ms` (all systems when `system_ids` is empty), most encrypted first.
pub fn encryption_stats(conn: &Connection, since_ms: i64, system_ids: &[i64]) -> rusqlite::Result<Vec<EncryptionStatRow>> {
    let mut conds = vec!["c.start_ms >= ?1".to_string()];
    let mut params = vec![Value::Integer(since_ms)];
    push_in(&mut conds, &mut params, "c.system_id ", "", system_ids);
    let sql = format!(
        "SELECT c.system_id, c.tgid, COALESCE(t.alpha_tag, ''), COALESCE(t.description, ''), SUM(c.encrypted) AS encrypted_calls, SUM(NOT c.encrypted) AS clear_calls \
         FROM calls c LEFT JOIN talkgroups t ON t.system_id = c.system_id AND t.tgid = c.tgid \
         WHERE {} GROUP BY c.system_id, c.tgid ORDER BY encrypted_calls DESC, c.system_id, c.tgid",
        conds.join(" AND ")
    );
    conn.prepare(&sql)?
        .query_map(params_from_iter(params.iter()), |r| {
            Ok(EncryptionStatRow {
                system_id: r.get(0)?,
                tgid: r.get(1)?,
                alpha_tag: r.get(2)?,
                description: r.get(3)?,
                encrypted_calls: r.get(4)?,
                clear_calls: r.get(5)?,
            })
        })?
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::systems::ensure_system;
    use crate::store::units::touch_unit;

    fn sys(s: &Store) -> SystemId {
        s.write_sync(|c| ensure_system(c, "sys1", "p25", "", 1_000)).unwrap()
    }

    fn tags(alpha: &str, desc: &str, group: &str, tag: &str) -> TgTags {
        TgTags { alpha_tag: alpha.into(), description: desc.into(), group: group.into(), tag: tag.into() }
    }

    fn add_call(c: &rusqlite::Connection, sys: SystemId, tgid: u32, start_ms: i64, encrypted: bool) -> i64 {
        c.query_row("INSERT INTO calls (system_id, tgid, start_ms, encrypted) VALUES (?1, ?2, ?3, ?4) RETURNING id", (sys, tgid, start_ms, encrypted), |r| {
            r.get(0)
        })
        .unwrap()
    }

    fn add_tx(c: &rusqlite::Connection, call: i64, pos: i64, unit: u32) {
        c.execute("INSERT INTO call_transmissions VALUES (?1, ?2, ?3, 0, 0, 1, 0, '')", (call, pos, unit)).unwrap();
    }

    #[test]
    fn talkgroup_counters_and_tags_follow_calls() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            touch_talkgroup(c, sys, 101, &tags("", "", "", ""), 1_000, false, None)?;
            touch_talkgroup(c, sys, 101, &tags("Fire Disp", "Fire dispatch", "Fire", "Dispatch"), 2_000, true, Some(0x84))?;
            touch_talkgroup(c, sys, 101, &tags("", "", "", ""), 3_000, false, None)
        })
        .unwrap();
        let tg = s.read(|c| talkgroup(c, sys, 101, 10_000)).unwrap().unwrap();
        assert_eq!((tg.call_count, tg.encrypted_count, tg.last_algid), (3, 1, Some(0x84)));
        assert_eq!((tg.alpha_tag.as_str(), tg.description.as_str(), tg.group.as_str(), tg.tag.as_str()), ("Fire Disp", "Fire dispatch", "Fire", "Dispatch"));
        assert_eq!((tg.first_heard_ms, tg.last_heard_ms), (1_000, 3_000));
        assert_eq!(tg.relevance_score, None);
        assert!(s.read(|c| talkgroup(c, sys, 999, 10_000)).unwrap().is_none());
        assert_eq!(s.read(|c| talkgroups_by_tgid(c, 101, 10_000)).unwrap().len(), 1);
    }

    #[test]
    fn the_detail_row_carries_live_stats() {
        let s = Store::in_memory();
        let sys = sys(&s);
        let now = 100 * 86_400_000_i64;
        s.write_sync(move |c| {
            touch_talkgroup(c, sys, 101, &tags("Fire Disp", "", "", ""), now, false, None)?;
            for ago in [30 * 60_000, 2 * 3_600_000, 40 * 86_400_000] {
                add_call(c, sys, 101, now - ago, false);
            }
            Ok(())
        })
        .unwrap();
        let one = s.read(|c| talkgroup(c, sys, 101, now)).unwrap().unwrap();
        assert_eq!((one.calls_1h, one.calls_24h, one.call_count_30d, one.relevance_score), (1, 2, 2, None));
        let many = s.read(|c| talkgroups_by_tgid(c, 101, now)).unwrap();
        assert_eq!(many.len(), 1);
        assert_eq!((many[0].calls_1h, many[0].calls_24h, many[0].call_count_30d), (1, 2, 2));
    }

    #[test]
    fn tags_come_from_a_record_or_just_the_info() {
        let rec = trunk_recorder_plugin::CallRecord {
            talkgroup_tag: "A".into(),
            talkgroup_description: "D".into(),
            talkgroup_group: "G".into(),
            talkgroup_group_tag: "T".into(),
            ..Default::default()
        };
        assert_eq!(TgTags::from_record(&rec), tags("A", "D", "G", "T"));
        let info = trunk_recorder_plugin::CallInfo { talkgroup_tag: "A".into(), ..Default::default() };
        assert_eq!(TgTags::from_info(&info), tags("A", "", "", ""));
    }

    #[test]
    fn live_stats_count_the_right_windows() {
        let s = Store::in_memory();
        let sys = sys(&s);
        let now = 100 * 86_400_000_i64;
        s.write_sync(move |c| {
            touch_talkgroup(c, sys, 101, &tags("Fire Disp", "", "", ""), now, false, None)?;
            let recent = add_call(c, sys, 101, now - 30 * 60_000, false);
            let older = add_call(c, sys, 101, now - 2 * 3_600_000, false);
            let old = add_call(c, sys, 101, now - 40 * 86_400_000, false);
            // another talkgroup's call does not count
            add_call(c, sys, 202, now - 1_000, false);
            add_tx(c, recent, 0, 10);
            add_tx(c, recent, 1, 11);
            add_tx(c, older, 0, 10);
            add_tx(c, old, 0, 99);
            // an event-only unit counts once, a unit seen both ways counts once, an old event does not count
            for (unit, t) in [(12, now - 1_000), (10, now - 5_000), (98, now - 40 * 86_400_000)] {
                c.execute("INSERT INTO unit_events (system_id, unit, event_type, time_ms, tgid) VALUES (?1, ?2, 'join', ?3, 101)", (sys, unit, t))?;
            }
            Ok(())
        })
        .unwrap();
        let (rows, total) = s.read(|c| list_talkgroups(c, &TalkgroupFilter::default(), now)).unwrap();
        assert_eq!(total, 1);
        let r = &rows[0];
        assert_eq!((r.calls_1h, r.calls_24h, r.call_count_30d, r.unit_count_30d), (1, 2, 2, 3));
    }

    #[test]
    fn search_matches_tag_number_and_description_with_relevance() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            touch_talkgroup(c, sys, 101, &tags("Fire Disp", "", "", ""), 1, false, None)?;
            touch_talkgroup(c, sys, 102, &tags("PD Main", "Backup for fire", "", ""), 1, false, None)?;
            touch_talkgroup(c, sys, 103, &tags("fire", "", "", ""), 1, false, None)?;
            touch_talkgroup(c, sys, 5101, &tags("Other", "", "", ""), 1, false, None)?;
            touch_talkgroup(c, sys, 104, &tags("100%", "", "", ""), 1, false, None)
        })
        .unwrap();
        let search = |q: &str| {
            let f = TalkgroupFilter { search: Some(q.into()), ..Default::default() };
            s.read(|c| list_talkgroups(c, &f, 10)).unwrap().0.iter().map(|t| (t.tgid, t.relevance_score)).collect::<Vec<_>>()
        };
        // exact (case-insensitive) first, then prefix, then description
        assert_eq!(search("fire"), [(103, Some(100)), (101, Some(50)), (102, Some(10))]);
        // a tgid matches by number exactly or by prefix, not by substring
        assert_eq!(search("101"), [(101, Some(100))]);
        assert_eq!(search("10"), [(104, Some(50)), (101, Some(10)), (102, Some(10)), (103, Some(10))]);
        // wildcard characters are literal
        assert_eq!(search("%"), [(104, Some(10))]);
        let none = s.read(|c| list_talkgroups(c, &TalkgroupFilter::default(), 10)).unwrap().0;
        assert!(none.iter().all(|t| t.relevance_score.is_none()));
    }

    #[test]
    fn list_filters_and_sorts_and_pages() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            let other = ensure_system(c, "sys2", "p25", "", 1)?;
            touch_talkgroup(c, sys, 300, &tags("Bravo", "", "G1", ""), 10, false, None)?;
            touch_talkgroup(c, sys, 100, &tags("Alpha", "", "G2", ""), 30, false, None)?;
            touch_talkgroup(c, sys, 200, &tags("Charlie", "", "G1", ""), 20, false, None)?;
            touch_talkgroup(c, other, 100, &tags("Elsewhere", "", "G1", ""), 20, false, None)
        })
        .unwrap();
        let list = |f: TalkgroupFilter| s.read(|c| list_talkgroups(c, &f, 100)).unwrap();
        let tgids = |f: TalkgroupFilter| list(f).0.iter().map(|t| t.tgid).collect::<Vec<_>>();
        assert_eq!(list(TalkgroupFilter::default()).1, 4);
        assert_eq!(tgids(TalkgroupFilter { system_ids: vec![sys.0], ..Default::default() }), [100, 200, 300]);
        assert_eq!(tgids(TalkgroupFilter { system_ids: vec![sys.0], sort: TgSort::AlphaTag, ..Default::default() }), [100, 300, 200]);
        assert_eq!(tgids(TalkgroupFilter { system_ids: vec![sys.0], sort: TgSort::LastSeen, desc: true, ..Default::default() }), [100, 200, 300]);
        assert_eq!(
            tgids(TalkgroupFilter { system_ids: vec![sys.0], group: Some("G1".into()), sort: TgSort::Tgid, desc: true, ..Default::default() }),
            [300, 200]
        );
        let (page, total) = list(TalkgroupFilter { system_ids: vec![sys.0], limit: 1, offset: 1, ..Default::default() });
        assert_eq!((page.len(), total, page[0].tgid), (1, 3, 200));
        // computed-column sorts run
        for sort in [TgSort::Group, TgSort::CallCount, TgSort::Calls1h, TgSort::Calls24h, TgSort::UnitCount] {
            assert_eq!(list(TalkgroupFilter { sort, ..Default::default() }).0.len(), 4);
        }
        assert_eq!(s.read(|c| talkgroups_by_tgid(c, 100, 10_000)).unwrap().len(), 2);
    }

    #[test]
    fn units_on_a_talkgroup_and_encryption_stats() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            let other = ensure_system(c, "sys2", "p25", "", 1)?;
            for u in [10, 11, 12] {
                touch_unit(c, sys, u, &format!("U{u}"), "", 1, None)?;
            }
            touch_talkgroup(c, sys, 101, &tags("Fire Disp", "Fire dispatch", "", ""), 1, false, None)?;
            let a = add_call(c, sys, 101, 5_000, false);
            let b = add_call(c, sys, 101, 6_000, true);
            let old = add_call(c, sys, 101, 100, false);
            add_call(c, sys, 202, 7_000, true);
            add_call(c, other, 101, 8_000, true);
            for (call, pos, unit) in [(a, 0, 10), (a, 1, 11), (b, 0, 10), (old, 0, 12)] {
                add_tx(c, call, pos, unit);
            }
            c.execute("INSERT INTO unit_events (system_id, unit, event_type, time_ms, tgid) VALUES (?1, 12, 'join', 5500, 101)", [sys])?;
            Ok(())
        })
        .unwrap();
        let (rows, total) = s.read(|c| units_on_talkgroup(c, sys, 101, 1_000, 10, 0)).unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.iter().map(|(u, n)| (u.unit_id, *n)).collect::<Vec<_>>(), [(10, 2), (11, 1), (12, 0)]);
        assert_eq!(s.read(|c| units_on_talkgroup(c, sys, 101, 1_000, 1, 1)).unwrap().0[0].0.unit_id, 11);
        let stats = s.read(|c| encryption_stats(c, 1_000, &[sys.0])).unwrap();
        assert_eq!(stats.len(), 2);
        let tg101 = stats.iter().find(|r| r.tgid == 101).unwrap();
        assert_eq!((tg101.encrypted_calls, tg101.clear_calls, tg101.alpha_tag.as_str(), tg101.description.as_str()), (1, 1, "Fire Disp", "Fire dispatch"));
        let tg202 = stats.iter().find(|r| r.tgid == 202).unwrap();
        assert_eq!((tg202.encrypted_calls, tg202.clear_calls, tg202.alpha_tag.as_str()), (1, 0, ""));
        assert_eq!(s.read(|c| encryption_stats(c, 1_000, &[])).unwrap().len(), 3);
    }
}
