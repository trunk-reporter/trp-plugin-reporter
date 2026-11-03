//! The tone patterns calls carried (pages, alert tones, DTMF and KNOX strings,
//! Morse IDs): one row each, indexed from the call's `metadata_json`
//! (`tone_patterns`), so they can be listed across calls.

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params_from_iter};

use super::calls::push_in;
use super::{CallId, SystemId};

/// `meta` key set once the calls stored before tone patterns were indexed have been.
pub const INDEXED_KEY: &str = "tone_patterns_indexed";

/// Index the tone patterns in `metadata_json`: of one call (its rows replaced),
/// or of every call (`None`; rows already there are kept).
const INDEX: &str = "INSERT OR IGNORE INTO tone_patterns (call_id, seq, system_id, tgid, unit, time_ms, pattern, text, json) \
     SELECT c.id, CAST(p.key AS INTEGER), c.system_id, c.tgid, NULLIF(CAST(COALESCE(json_extract(p.value, '$.src'), 0) AS INTEGER), 0), \
     c.start_ms + CAST(ROUND(COALESCE(json_extract(p.value, '$.pos'), 0) * 1000) AS INTEGER), \
     COALESCE(json_extract(p.value, '$.pattern'), ''), COALESCE(json_extract(p.value, '$.text'), ''), p.value \
     FROM calls c, json_each(c.metadata_json, '$.tone_patterns') p \
     WHERE c.metadata_json LIKE '%\"tone_patterns\"%' AND json_valid(c.metadata_json)";

/// The call's tone patterns as its `metadata_json` has them now.
pub fn index_call(conn: &Connection, call: CallId) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM tone_patterns WHERE call_id = ?1", [call])?;
    conn.prepare_cached(&format!("{INDEX} AND c.id = ?1"))?.execute([call])?;
    Ok(())
}

/// The calls stored before tone patterns were indexed, once ([`INDEXED_KEY`]).
pub fn index_stored(conn: &Connection) -> rusqlite::Result<()> {
    let done: Option<String> = conn.query_row("SELECT value FROM meta WHERE key = ?1", [INDEXED_KEY], |r| r.get(0)).optional()?;
    if done.is_none() {
        conn.execute_batch(INDEX)?;
        conn.execute("INSERT INTO meta (key, value) VALUES (?1, '1')", [INDEXED_KEY])?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToneRow {
    pub id: i64,
    pub call_id: CallId,
    pub system_id: SystemId,
    pub tgid: u32,
    pub unit: Option<u32>,
    pub time_ms: i64,
    /// The pattern as the call's record has it.
    pub json: String,
    pub system_name: String,
    pub system_short_name: String,
    pub tg_alpha_tag: String,
    pub unit_alpha_tag: String,
}

#[derive(Clone, Debug, Default)]
pub struct ToneFilter {
    pub system_ids: Vec<i64>,
    pub tgids: Vec<u32>,
    pub units: Vec<u32>,
    pub call_ids: Vec<i64>,
    /// `two_tone`, `digits`, …
    pub patterns: Vec<String>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub desc: bool,
    pub limit: u32,
    pub offset: u32,
}

/// The patterns the filter matches by time (then id), and how many it matches.
pub fn list_tones(conn: &Connection, f: &ToneFilter) -> rusqlite::Result<(Vec<ToneRow>, i64)> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "t.system_id ", "", &f.system_ids);
    push_in(&mut conds, &mut params, "t.tgid ", "", &f.tgids.iter().map(|&t| t as i64).collect::<Vec<_>>());
    push_in(&mut conds, &mut params, "t.unit ", "", &f.units.iter().map(|&u| u as i64).collect::<Vec<_>>());
    push_in(&mut conds, &mut params, "t.call_id ", "", &f.call_ids);
    if !f.patterns.is_empty() {
        let marks: Vec<String> = f
            .patterns
            .iter()
            .map(|p| {
                params.push(Value::Text(p.clone()));
                format!("?{}", params.len())
            })
            .collect();
        conds.push(format!("t.pattern IN ({})", marks.join(", ")));
    }
    if let Some(ms) = f.start_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("t.time_ms >= ?{}", params.len()));
    }
    if let Some(ms) = f.end_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("t.time_ms <= ?{}", params.len()));
    }
    let wher = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM tone_patterns t{wher}"), params_from_iter(params.iter()), |r| r.get(0))?;
    let n = params.len();
    params.push(Value::Integer(f.limit.into()));
    params.push(Value::Integer(f.offset.into()));
    let dir = if f.desc { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT t.id, t.call_id, t.system_id, t.tgid, t.unit, t.time_ms, t.json, s.name, s.short_name, \
         COALESCE(g.alpha_tag, ''), COALESCE(u.alpha_tag, '') \
         FROM tone_patterns t JOIN systems s ON s.id = t.system_id \
         LEFT JOIN talkgroups g ON g.system_id = t.system_id AND g.tgid = t.tgid \
         LEFT JOIN units u ON u.system_id = t.system_id AND u.unit_id = t.unit{wher} \
         ORDER BY t.time_ms {dir}, t.id {dir} LIMIT ?{} OFFSET ?{}",
        n + 1,
        n + 2
    );
    let rows = conn
        .prepare(&sql)?
        .query_map(params_from_iter(params.iter()), |r| {
            Ok(ToneRow {
                id: r.get(0)?,
                call_id: r.get(1)?,
                system_id: r.get(2)?,
                tgid: r.get(3)?,
                unit: r.get(4)?,
                time_ms: r.get(5)?,
                json: r.get(6)?,
                system_name: r.get(7)?,
                system_short_name: r.get(8)?,
                tg_alpha_tag: r.get(9)?,
                unit_alpha_tag: r.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::calls::call_concluded;
    use crate::store::systems::ensure_system;
    use trunk_recorder_plugin::{CallRecord, TonePattern};

    fn page(pattern: &str, pos: f64, src: i64) -> TonePattern {
        TonePattern {
            pattern: pattern.into(),
            tones: vec![849.6, 1292.8],
            lengths: vec![1.0, 3.0],
            pos,
            len: 4.0,
            src,
            text: format!("{pattern} at {pos}"),
            ..Default::default()
        }
    }

    fn conclude(store: &Store, path: &str, patterns: Vec<TonePattern>) -> CallId {
        let path = path.to_string();
        store
            .write_sync(move |c| {
                let sys = ensure_system(c, "sys1", "p25", "", 0)?;
                let r =
                    CallRecord { talkgroup: 101, start_time: 1_700_000_000, start_time_ms: 1_700_000_000_000, tone_patterns: patterns, ..Default::default() };
                Ok(call_concluded(c, sys, None, &path, &r, None, "", None)?.id)
            })
            .unwrap()
    }

    fn all(store: &Store) -> Vec<ToneRow> {
        store.read(|c| list_tones(c, &ToneFilter { limit: 100, ..Default::default() })).unwrap().0
    }

    #[test]
    fn a_calls_patterns_are_indexed_and_replaced_with_it() {
        let store = Store::in_memory();
        let id = conclude(&store, "sys1/a", vec![page("two_tone", 0.5, 1001), page("long", 6.25, 0)]);
        let rows = all(&store);
        assert_eq!(
            rows.iter().map(|r| (r.call_id, r.time_ms, r.unit)).collect::<Vec<_>>(),
            [(id, 1_700_000_000_500, Some(1001)), (id, 1_700_000_006_250, None)]
        );
        let first: serde_json::Value = serde_json::from_str(&rows[0].json).unwrap();
        assert_eq!((first["pattern"].as_str(), first["tones"][1].as_f64()), (Some("two_tone"), Some(1292.8)));
        // Concluded again (its files read back): the same rows, not twice as many.
        conclude(&store, "sys1/a", vec![page("digits", 1.0, 0)]);
        assert_eq!(all(&store).len(), 1);
        // Filtered by pattern.
        conclude(&store, "sys1/b", vec![page("two_tone", 0.0, 0)]);
        let f = ToneFilter { patterns: vec!["two_tone".into()], limit: 10, ..Default::default() };
        assert_eq!(store.read(|c| list_tones(c, &f)).unwrap().1, 1);
        // The call's deletion (retention) takes them with it.
        store.write_sync(move |c| c.execute("DELETE FROM calls WHERE id = ?1", [id])).unwrap();
        assert_eq!(all(&store).len(), 1);
    }

    #[test]
    fn calls_stored_before_the_index_are_indexed_once() {
        let store = Store::in_memory();
        conclude(&store, "sys1/a", vec![page("two_tone", 0.5, 0)]);
        store.write_sync(|c| c.execute_batch("DELETE FROM tone_patterns; DELETE FROM meta WHERE key = 'tone_patterns_indexed';")).unwrap();
        store.write_sync(index_stored).unwrap();
        assert_eq!(all(&store).len(), 1);
        // Marked: not again.
        store.write_sync(|c| c.execute("DELETE FROM tone_patterns", [])).unwrap();
        store.write_sync(index_stored).unwrap();
        assert!(all(&store).is_empty());
    }
}
