//! Units, and the unit events (registrations, affiliations, calls) that go with them.

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, named_params, params_from_iter};
use serde_json::{Value as Json, json};

use super::calls::push_in;
use super::{CallId, SiteId, SystemId};

#[derive(Clone, Debug, PartialEq)]
pub struct UnitRow {
    pub system_id: SystemId,
    pub unit_id: u32,
    pub alpha_tag: String,
    pub alpha_tag_source: String,
    pub ota_alpha_tag: String,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub last_event_type: Option<String>,
    pub last_event_ms: Option<i64>,
    pub last_event_tgid: Option<u32>,
    pub registered: bool,
}

pub(super) const UNIT_COLS: &str =
    "system_id, unit_id, alpha_tag, alpha_tag_source, ota_alpha_tag, first_seen_ms, last_seen_ms, last_event_type, last_event_ms, last_event_tgid, registered";

pub(super) fn unit_row(r: &Row<'_>) -> rusqlite::Result<UnitRow> {
    Ok(UnitRow {
        system_id: r.get(0)?,
        unit_id: r.get(1)?,
        alpha_tag: r.get(2)?,
        alpha_tag_source: r.get(3)?,
        ota_alpha_tag: r.get(4)?,
        first_seen_ms: r.get(5)?,
        last_seen_ms: r.get(6)?,
        last_event_type: r.get(7)?,
        last_event_ms: r.get(8)?,
        last_event_tgid: r.get(9)?,
        registered: r.get(10)?,
    })
}

/// Record that a unit was heard (or seen on the control channel). `last_event` is
/// a tr-engine event type and its talkgroup; `on` registers the unit, `off` deregisters it.
pub fn touch_unit(
    conn: &Connection,
    system_id: SystemId,
    unit: u32,
    tag: &str,
    tag_ota: &str,
    seen_ms: i64,
    last_event: Option<(&str, Option<u32>)>,
) -> rusqlite::Result<()> {
    let (ev_type, ev_tgid) = match last_event {
        Some((t, g)) => (Some(t), g),
        None => (None, None),
    };
    conn.execute(
        "INSERT INTO units (system_id, unit_id, alpha_tag, alpha_tag_source, ota_alpha_tag, first_seen_ms, last_seen_ms, last_event_type, last_event_ms, last_event_tgid, registered) \
         VALUES (:system_id, :unit, :tag, CASE WHEN :tag <> '' THEN 'recorder' ELSE '' END, :ota, :seen, :seen, :ev_type, \
         CASE WHEN :ev_type IS NOT NULL THEN :seen END, CASE WHEN :ev_type IS NOT NULL THEN :ev_tgid END, COALESCE(:ev_type = 'on', 0)) \
         ON CONFLICT(system_id, unit_id) DO UPDATE SET \
         first_seen_ms = MIN(first_seen_ms, :seen), last_seen_ms = MAX(last_seen_ms, :seen), \
         alpha_tag = CASE WHEN :tag <> '' THEN :tag ELSE alpha_tag END, \
         alpha_tag_source = CASE WHEN :tag <> '' THEN 'recorder' ELSE alpha_tag_source END, \
         ota_alpha_tag = CASE WHEN :ota <> '' THEN :ota ELSE ota_alpha_tag END, \
         last_event_type = COALESCE(:ev_type, last_event_type), \
         last_event_ms = CASE WHEN :ev_type IS NOT NULL THEN :seen ELSE last_event_ms END, \
         last_event_tgid = CASE WHEN :ev_type IS NOT NULL THEN :ev_tgid ELSE last_event_tgid END, \
         registered = CASE WHEN :ev_type = 'on' THEN 1 WHEN :ev_type = 'off' THEN 0 ELSE registered END",
        named_params! { ":system_id": system_id, ":unit": unit, ":tag": tag, ":ota": tag_ota, ":seen": seen_ms, ":ev_type": ev_type, ":ev_tgid": ev_tgid },
    )
    .map(|_| ())
}

pub fn unit(conn: &Connection, system_id: SystemId, unit_id: u32) -> rusqlite::Result<Option<UnitRow>> {
    conn.prepare_cached(&format!("SELECT {UNIT_COLS} FROM units WHERE system_id = ?1 AND unit_id = ?2"))?.query_row((system_id, unit_id), unit_row).optional()
}

/// The unit under every system that has it.
pub fn units_by_id(conn: &Connection, unit_id: u32) -> rusqlite::Result<Vec<UnitRow>> {
    conn.prepare(&format!("SELECT {UNIT_COLS} FROM units WHERE unit_id = ?1 ORDER BY system_id"))?.query_map([unit_id], unit_row)?.collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnitSort {
    AlphaTag,
    UnitId,
    LastSeen,
    LastEventTime,
}

impl UnitSort {
    fn column(self) -> &'static str {
        match self {
            UnitSort::AlphaTag => "alpha_tag",
            UnitSort::UnitId => "unit_id",
            UnitSort::LastSeen => "last_seen_ms",
            UnitSort::LastEventTime => "last_event_ms",
        }
    }
}

#[derive(Clone, Debug)]
pub struct UnitFilter {
    pub system_ids: Vec<i64>,
    pub search: Option<String>,
    pub active_within_ms: Option<i64>,
    pub tgids: Vec<u32>,
    pub sort: UnitSort,
    pub desc: bool,
    pub limit: u32,
    pub offset: u32,
}

impl Default for UnitFilter {
    fn default() -> Self {
        UnitFilter { system_ids: vec![], search: None, active_within_ms: None, tgids: vec![], sort: UnitSort::UnitId, desc: false, limit: 50, offset: 0 }
    }
}

/// `%`, `_` and `\` in user text made literal for `LIKE ... ESCAPE '\'`.
pub(super) fn like_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// The page of units the filter matches, and how many match in all.
/// Units first seen at or after `since_ms` (on `system_ids`, or any system when empty), newest first, at most `limit`.
pub fn first_heard_units(conn: &Connection, system_ids: &[i64], since_ms: i64, limit: u32) -> rusqlite::Result<Vec<UnitRow>> {
    let mut conds = vec!["first_seen_ms >= ?1".to_string()];
    let mut params = vec![Value::Integer(since_ms)];
    push_in(&mut conds, &mut params, "system_id ", "", system_ids);
    params.push(Value::Integer(limit.into()));
    let sql =
        format!("SELECT {UNIT_COLS} FROM units WHERE {} ORDER BY first_seen_ms DESC, unit_id DESC, system_id LIMIT ?{}", conds.join(" AND "), params.len());
    conn.prepare_cached(&sql)?.query_map(params_from_iter(params.iter()), unit_row)?.collect()
}

pub fn list_units(conn: &Connection, f: &UnitFilter, now_ms: i64) -> rusqlite::Result<(Vec<UnitRow>, i64)> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "system_id ", "", &f.system_ids);
    let mut relevance = String::new();
    if let Some(q) = f.search.as_deref().filter(|q| !q.is_empty()) {
        let esc = like_escape(q);
        params.push(Value::Text(q.to_string()));
        let exact = params.len();
        params.push(Value::Text(format!("%{esc}%")));
        let contains = params.len();
        params.push(Value::Text(format!("{esc}%")));
        let prefix = params.len();
        conds.push(format!("(alpha_tag LIKE ?{contains} ESCAPE '\\' OR CAST(unit_id AS TEXT) LIKE ?{prefix} ESCAPE '\\')"));
        relevance = format!(
            "CASE WHEN alpha_tag = ?{exact} COLLATE NOCASE OR CAST(unit_id AS TEXT) = ?{exact} THEN 100 WHEN alpha_tag LIKE ?{prefix} ESCAPE '\\' THEN 50 ELSE 10 END"
        );
    }
    if let Some(ms) = f.active_within_ms {
        params.push(Value::Integer(now_ms - ms));
        conds.push(format!("last_seen_ms > ?{}", params.len()));
    }
    if !f.tgids.is_empty() {
        let tgids: Vec<i64> = f.tgids.iter().map(|&t| t as i64).collect();
        let (mut ev, mut tx) = (Vec::new(), Vec::new());
        push_in(
            &mut ev,
            &mut params,
            "EXISTS (SELECT 1 FROM unit_events e WHERE e.system_id = units.system_id AND e.unit = units.unit_id AND e.tgid ",
            ")",
            &tgids,
        );
        push_in(
            &mut tx,
            &mut params,
            "EXISTS (SELECT 1 FROM call_transmissions t JOIN calls c ON c.id = t.call_id WHERE c.system_id = units.system_id AND t.unit = units.unit_id AND c.tgid ",
            ")",
            &tgids,
        );
        conds.push(format!("({} OR {})", ev[0], tx[0]));
    }
    let wher = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM units{wher}"), params_from_iter(params.iter()), |r| r.get(0))?;
    let n = params.len();
    params.push(Value::Integer(f.limit as i64));
    params.push(Value::Integer(f.offset as i64));
    let dir = if f.desc { "DESC" } else { "ASC" };
    let (select_rel, order_rel) =
        if relevance.is_empty() { (String::new(), String::new()) } else { (format!(", {relevance} AS relevance_score"), "relevance_score DESC, ".to_string()) };
    let sql = format!(
        "SELECT {UNIT_COLS}{select_rel} FROM units{wher} ORDER BY {order_rel}{col} {dir}, unit_id {dir}, system_id LIMIT ?{} OFFSET ?{}",
        n + 1,
        n + 2,
        col = f.sort.column()
    );
    let rows = conn.prepare(&sql)?.query_map(params_from_iter(params.iter()), unit_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

/// The tr-engine event type for a control-channel event kind, with any metadata it carries.
pub fn event_type_of(kind: &str) -> Option<(&'static str, Option<Json>)> {
    Some(match kind {
        "registration" => ("on", None),
        "deregistration" => ("off", None),
        "affiliation" => ("join", None),
        "acknowledge" => ("ackresp", None),
        "location" => ("location", None),
        "data_grant" => ("data", None),
        "answer_request" => ("ans_req", None),
        "call_alert" => ("call_alert", None),
        // tr-engine has no alarm type; its pages show an emergency alarm as signal/emergency.
        "emergency_alarm" => ("signal", Some(json!({"signaling_type": "P25", "signal_type": "emergency"}))),
        _ => return None,
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewUnitEvent {
    pub system_id: SystemId,
    pub site_id: Option<SiteId>,
    pub unit: u32,
    pub event_type: &'static str,
    pub time_ms: i64,
    pub tgid: Option<u32>,
    pub emergency: bool,
    pub call_id: Option<CallId>,
    pub metadata_json: Option<Json>,
}

/// Insert an event. `None` when the (call, unit, type) unique index already holds it.
pub fn insert_unit_event(conn: &Connection, e: &NewUnitEvent) -> rusqlite::Result<Option<i64>> {
    let metadata = e.metadata_json.as_ref().map(|m| m.to_string());
    conn.query_row(
        "INSERT OR IGNORE INTO unit_events (system_id, site_id, unit, event_type, time_ms, tgid, emergency, call_id, metadata_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) RETURNING id",
        rusqlite::params![e.system_id, e.site_id, e.unit, e.event_type, e.time_ms, e.tgid, e.emergency, e.call_id, metadata],
        |r| r.get(0),
    )
    .optional()
}

/// The `call` and `end` events a call's transmissions make: a `call` per source at its
/// time and, once the call has ended, an `end` per source at the next source's time
/// (the last at `end_ms`).
pub fn transmission_events(
    call_id: CallId,
    system_id: SystemId,
    site_id: Option<SiteId>,
    tgid: u32,
    sources: &[(u32, i64, bool)],
    end_ms: Option<i64>,
) -> Vec<NewUnitEvent> {
    let make = |event_type: &'static str, &(unit, _, emergency): &(u32, i64, bool), time_ms: i64| NewUnitEvent {
        system_id,
        site_id,
        unit,
        event_type,
        time_ms,
        tgid: Some(tgid),
        emergency,
        call_id: Some(call_id),
        metadata_json: None,
    };
    let mut out: Vec<NewUnitEvent> = sources.iter().map(|s| make("call", s, s.1)).collect();
    if let Some(end) = end_ms {
        for (i, s) in sources.iter().enumerate() {
            out.push(make("end", s, sources.get(i + 1).map_or(end, |next| next.1)));
        }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventSort {
    Time,
    Unit,
    Tgid,
    EventType,
}

impl EventSort {
    fn column(self) -> &'static str {
        match self {
            EventSort::Time => "e.time_ms",
            EventSort::Unit => "e.unit",
            EventSort::Tgid => "e.tgid",
            EventSort::EventType => "e.event_type",
        }
    }
}

#[derive(Clone, Debug)]
pub struct UnitEventFilter {
    pub system_ids: Vec<i64>,
    pub unit_ids: Vec<u32>,
    pub tgids: Vec<u32>,
    pub types: Vec<String>,
    pub emergency: Option<bool>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub sort: EventSort,
    pub desc: bool,
    pub limit: u32,
    pub offset: u32,
}

impl Default for UnitEventFilter {
    fn default() -> Self {
        UnitEventFilter {
            system_ids: vec![],
            unit_ids: vec![],
            tgids: vec![],
            types: vec![],
            emergency: None,
            start_ms: None,
            end_ms: None,
            sort: EventSort::Time,
            desc: true,
            limit: 50,
            offset: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnitEventRow {
    pub id: i64,
    pub system_id: SystemId,
    pub site_id: Option<SiteId>,
    pub unit: u32,
    pub event_type: String,
    pub time_ms: i64,
    pub tgid: Option<u32>,
    pub emergency: bool,
    pub call_id: Option<CallId>,
    pub metadata_json: Option<String>,
    pub system_short_name: String,
    pub system_name: String,
    pub unit_alpha_tag: String,
    pub tg_alpha_tag: String,
    pub tg_description: String,
}

const EVENT_SELECT: &str = "SELECT e.id, e.system_id, e.site_id, e.unit, e.event_type, e.time_ms, e.tgid, e.emergency, e.call_id, e.metadata_json, \
     COALESCE(s.short_name, ''), COALESCE(s.name, ''), COALESCE(u.alpha_tag, ''), COALESCE(t.alpha_tag, ''), COALESCE(t.description, '') \
     FROM unit_events e \
     LEFT JOIN systems s ON s.id = e.system_id \
     LEFT JOIN units u ON u.system_id = e.system_id AND u.unit_id = e.unit \
     LEFT JOIN talkgroups t ON t.system_id = e.system_id AND t.tgid = e.tgid";

fn event_row(r: &Row<'_>) -> rusqlite::Result<UnitEventRow> {
    Ok(UnitEventRow {
        id: r.get(0)?,
        system_id: r.get(1)?,
        site_id: r.get(2)?,
        unit: r.get(3)?,
        event_type: r.get(4)?,
        time_ms: r.get(5)?,
        tgid: r.get(6)?,
        emergency: r.get(7)?,
        call_id: r.get(8)?,
        metadata_json: r.get(9)?,
        system_short_name: r.get(10)?,
        system_name: r.get(11)?,
        unit_alpha_tag: r.get(12)?,
        tg_alpha_tag: r.get(13)?,
        tg_description: r.get(14)?,
    })
}

/// The page of unit events the filter matches, and how many match in all.
pub fn list_unit_events(conn: &Connection, f: &UnitEventFilter) -> rusqlite::Result<(Vec<UnitEventRow>, i64)> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "e.system_id ", "", &f.system_ids);
    push_in(&mut conds, &mut params, "e.unit ", "", &f.unit_ids.iter().map(|&u| u as i64).collect::<Vec<_>>());
    push_in(&mut conds, &mut params, "e.tgid ", "", &f.tgids.iter().map(|&t| t as i64).collect::<Vec<_>>());
    if !f.types.is_empty() {
        let marks: Vec<String> = f
            .types
            .iter()
            .map(|t| {
                params.push(Value::Text(t.clone()));
                format!("?{}", params.len())
            })
            .collect();
        conds.push(format!("e.event_type IN ({})", marks.join(", ")));
    }
    if let Some(b) = f.emergency {
        params.push(Value::Integer(b as i64));
        conds.push(format!("e.emergency = ?{}", params.len()));
    }
    if let Some(ms) = f.start_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("e.time_ms >= ?{}", params.len()));
    }
    if let Some(ms) = f.end_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("e.time_ms <= ?{}", params.len()));
    }
    let wher = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM unit_events e{wher}"), params_from_iter(params.iter()), |r| r.get(0))?;
    let n = params.len();
    params.push(Value::Integer(f.limit as i64));
    params.push(Value::Integer(f.offset as i64));
    let sql = format!(
        "{EVENT_SELECT}{wher} ORDER BY {col} {dir}, e.id {dir} LIMIT ?{} OFFSET ?{}",
        n + 1,
        n + 2,
        col = f.sort.column(),
        dir = if f.desc { "DESC" } else { "ASC" }
    );
    let rows = conn.prepare(&sql)?.query_map(params_from_iter(params.iter()), event_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

/// One unit event with its display names.
pub fn unit_event_by_id(conn: &Connection, id: i64) -> rusqlite::Result<Option<UnitEventRow>> {
    conn.query_row(&format!("{EVENT_SELECT} WHERE e.id = ?1"), [id], event_row).optional()
}

/// Affiliation-type events from `since_ms` on, oldest first.
pub fn affiliation_events_since(conn: &Connection, since_ms: i64) -> rusqlite::Result<Vec<UnitEventRow>> {
    conn.prepare(&format!("{EVENT_SELECT} WHERE e.event_type IN ('join', 'off', 'call', 'end', 'location') AND e.time_ms >= ?1 ORDER BY e.time_ms, e.id"))?
        .query_map([since_ms], event_row)?
        .collect()
}

/// One talkgroup a unit talked on in a window.
#[derive(Clone, Debug, PartialEq)]
pub struct UnitTalkgroupRow {
    pub tgid: u32,
    pub tg_alpha_tag: String,
    pub calls: i64,
    pub transmissions: i64,
    pub seconds: f64,
    pub first_ms: i64,
    pub last_ms: i64,
}

/// The talkgroups `unit` talked on in `[after_ms, before_ms)` (by the transmission's own time), most transmissions first, then by tgid.
pub fn talkgroups_of_unit(conn: &Connection, system_id: SystemId, unit: u32, after_ms: i64, before_ms: i64) -> rusqlite::Result<Vec<UnitTalkgroupRow>> {
    conn.prepare_cached(
        "SELECT c.tgid, COALESCE(t.alpha_tag, ''), COUNT(DISTINCT c.id), COUNT(*) AS n, SUM(x.len_s), MIN(x.time_ms), MAX(x.time_ms)
         FROM call_transmissions x JOIN calls c ON c.id = x.call_id
         LEFT JOIN talkgroups t ON t.system_id = c.system_id AND t.tgid = c.tgid
         WHERE x.unit = :unit AND x.time_ms >= :after AND x.time_ms < :before AND c.system_id = :system
         GROUP BY c.tgid ORDER BY n DESC, c.tgid",
    )?
    .query_map(named_params! { ":unit": unit, ":after": after_ms, ":before": before_ms, ":system": system_id }, |r| {
        Ok(UnitTalkgroupRow {
            tgid: r.get(0)?,
            tg_alpha_tag: r.get(1)?,
            calls: r.get(2)?,
            transmissions: r.get(3)?,
            seconds: r.get(4)?,
            first_ms: r.get(5)?,
            last_ms: r.get(6)?,
        })
    })?
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::systems::ensure_system;

    fn sys(s: &Store) -> SystemId {
        s.write_sync(|c| ensure_system(c, "sys1", "p25", "Sys One", 1_000)).unwrap()
    }

    #[test]
    fn kinds_map_to_tr_engine_event_types() {
        assert_eq!(event_type_of("registration").unwrap().0, "on");
        assert_eq!(event_type_of("deregistration").unwrap().0, "off");
        assert_eq!(event_type_of("affiliation").unwrap().0, "join");
        assert_eq!(event_type_of("acknowledge").unwrap().0, "ackresp");
        assert_eq!(event_type_of("location").unwrap().0, "location");
        assert_eq!(event_type_of("data_grant").unwrap().0, "data");
        assert_eq!(event_type_of("answer_request").unwrap().0, "ans_req");
        assert_eq!(event_type_of("call_alert").unwrap().0, "call_alert");
        let (t, m) = event_type_of("emergency_alarm").unwrap();
        assert_eq!((t, m.unwrap()["signal_type"].as_str()), ("signal", Some("emergency")));
        assert!(event_type_of("something_new").is_none());
    }

    #[test]
    fn transmissions_make_one_call_and_one_end_per_unit() {
        let ev = transmission_events(CallId(1), SystemId(1), None, 101, &[(10, 1_000, false), (20, 3_500, false)], Some(6_000));
        let got: Vec<_> = ev.iter().map(|e| (e.unit, e.event_type, e.time_ms)).collect();
        assert_eq!(got, [(10, "call", 1_000), (20, "call", 3_500), (10, "end", 3_500), (20, "end", 6_000)]);
        assert_eq!(transmission_events(CallId(1), SystemId(1), None, 101, &[(10, 1_000, false)], None).len(), 1);
    }

    #[test]
    fn a_synthesized_event_inserted_twice_is_stored_once() {
        let s = Store::in_memory();
        let sys = sys(&s);
        let ev = transmission_events(CallId(7), sys, None, 101, &[(10, 1_000, true)], None).remove(0);
        let e2 = ev.clone();
        let (a, b) = s.write_sync(move |c| Ok((insert_unit_event(c, &e2)?, insert_unit_event(c, &e2)?))).unwrap();
        assert!(a.is_some());
        assert_eq!(b, None);
        let n = |s: &Store| s.read(|c| c.query_row("SELECT COUNT(*) FROM unit_events", [], |r| r.get::<_, i64>(0))).unwrap();
        assert_eq!(n(&s), 1);
        let ctl = NewUnitEvent { call_id: None, ..ev };
        let c2 = ctl.clone();
        let (a, b) = s.write_sync(move |c| Ok((insert_unit_event(c, &c2)?, insert_unit_event(c, &c2)?))).unwrap();
        assert!(a.is_some() && b.is_some() && a != b);
        assert_eq!(n(&s), 3);
    }

    #[test]
    fn unit_last_event_and_registration_follow_events() {
        let s = Store::in_memory();
        let sys = sys(&s);
        let get = |s: &Store| s.read(|c| unit(c, sys, 10)).unwrap().unwrap();
        s.write_sync(move |c| touch_unit(c, sys, 10, "", "", 1_000, Some(("on", None)))).unwrap();
        let u = get(&s);
        assert_eq!((u.registered, u.last_event_type.as_deref(), u.last_event_tgid, u.alpha_tag_source.as_str()), (true, Some("on"), None, ""));
        s.write_sync(move |c| touch_unit(c, sys, 10, "Medic 1", "OTA 1", 2_000, Some(("join", Some(101))))).unwrap();
        let u = get(&s);
        assert_eq!((u.registered, u.last_event_tgid, u.last_event_ms, u.first_seen_ms, u.last_seen_ms), (true, Some(101), Some(2_000), 1_000, 2_000));
        assert_eq!((u.alpha_tag.as_str(), u.alpha_tag_source.as_str(), u.ota_alpha_tag.as_str()), ("Medic 1", "recorder", "OTA 1"));
        // No event and no tag: the tags and the last event stay.
        s.write_sync(move |c| touch_unit(c, sys, 10, "", "", 3_000, None)).unwrap();
        let u = get(&s);
        assert_eq!((u.alpha_tag.as_str(), u.last_event_type.as_deref(), u.last_seen_ms), ("Medic 1", Some("join"), 3_000));
        s.write_sync(move |c| touch_unit(c, sys, 10, "", "", 4_000, Some(("off", None)))).unwrap();
        let u = get(&s);
        assert_eq!((u.registered, u.last_event_type.as_deref(), u.last_event_tgid), (false, Some("off"), None));
        assert_eq!(s.read(|c| units_by_id(c, 10)).unwrap().len(), 1);
        assert!(s.read(|c| unit(c, sys, 99)).unwrap().is_none());
    }

    #[test]
    fn units_list_searches_filters_and_sorts() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            touch_unit(c, sys, 1001, "Medic 1", "", 10_000, None)?;
            touch_unit(c, sys, 2002, "Engine 1", "", 20_000, None)?;
            touch_unit(c, sys, 3003, "Old medic", "", 100, None)?;
            touch_unit(c, sys, 4004, "100%_unit", "", 30_000, None)?;
            c.execute("INSERT INTO unit_events (system_id, unit, event_type, time_ms, tgid) VALUES (?1, 1001, 'join', 10, 101)", [sys])?;
            c.execute("INSERT INTO calls (id, system_id, tgid, start_ms) VALUES (1, ?1, 202, 5)", [sys])?;
            c.execute("INSERT INTO call_transmissions VALUES (1, 0, 2002, 5, 0, 1, 0, '')", []).map(|_| ())
        })
        .unwrap();
        let list = |f: UnitFilter| s.read(|c| list_units(c, &f, 30_000)).unwrap();
        let ids = |f: UnitFilter| list(f).0.iter().map(|u| u.unit_id).collect::<Vec<_>>();
        assert_eq!(list(UnitFilter::default()).1, 4);
        // "Medic 1" is a prefix match (50), "Old medic" only contains it (10): relevance beats the requested order.
        assert_eq!(ids(UnitFilter { search: Some("medic".into()), ..Default::default() }), [1001, 3003]);
        assert_eq!(ids(UnitFilter { search: Some("medic".into()), desc: true, ..Default::default() }), [1001, 3003]);
        assert_eq!(ids(UnitFilter { search: Some("old".into()), ..Default::default() }), [3003]);
        assert_eq!(ids(UnitFilter { search: Some("2002".into()), ..Default::default() }), [2002]);
        // wildcard characters are literal
        assert_eq!(ids(UnitFilter { search: Some("%".into()), ..Default::default() }), [4004]);
        assert_eq!(ids(UnitFilter { search: Some("_".into()), ..Default::default() }), [4004]);
        assert_eq!(ids(UnitFilter { active_within_ms: Some(15_000), ..Default::default() }), [2002, 4004]);
        assert_eq!(ids(UnitFilter { tgids: vec![101], ..Default::default() }), [1001]);
        assert_eq!(ids(UnitFilter { tgids: vec![202], ..Default::default() }), [2002]);
        assert_eq!(ids(UnitFilter { tgids: vec![101, 202], ..Default::default() }), [1001, 2002]);
        assert_eq!(ids(UnitFilter { sort: UnitSort::LastSeen, desc: true, ..Default::default() }), [4004, 2002, 1001, 3003]);
        assert_eq!(ids(UnitFilter { sort: UnitSort::AlphaTag, ..Default::default() }), [4004, 2002, 1001, 3003]);
        let (page, total) = list(UnitFilter { limit: 2, offset: 1, ..Default::default() });
        assert_eq!((page.len(), total), (2, 4));
    }

    #[test]
    fn events_list_with_joins_filters_and_since() {
        let s = Store::in_memory();
        let sys = sys(&s);
        s.write_sync(move |c| {
            touch_unit(c, sys, 10, "Medic 1", "", 1, None)?;
            c.execute("INSERT INTO talkgroups (system_id, tgid, alpha_tag, description, first_heard_ms, last_heard_ms) VALUES (?1, 101, 'Fire Disp', 'Fire dispatch', 0, 0)", [sys])?;
            for (unit, ty, t, tg, em) in [(10, "join", 100, Some(101), false), (10, "on", 200, None, false), (20, "call", 300, Some(101), true), (20, "data", 400, Some(55), false)] {
                insert_unit_event(c, &NewUnitEvent { system_id: sys, site_id: None, unit, event_type: ty, time_ms: t, tgid: tg, emergency: em, call_id: None, metadata_json: None })?;
            }
            Ok(())
        })
        .unwrap();
        let (rows, total) = s.read(|c| list_unit_events(c, &UnitEventFilter::default())).unwrap();
        assert_eq!((total, rows.iter().map(|e| e.time_ms).collect::<Vec<_>>()), (4, vec![400, 300, 200, 100]));
        let join = rows.iter().find(|e| e.event_type == "join").unwrap();
        assert_eq!(
            (
                join.system_short_name.as_str(),
                join.system_name.as_str(),
                join.unit_alpha_tag.as_str(),
                join.tg_alpha_tag.as_str(),
                join.tg_description.as_str()
            ),
            ("sys1", "Sys One", "Medic 1", "Fire Disp", "Fire dispatch")
        );
        let data = rows.iter().find(|e| e.event_type == "data").unwrap();
        assert_eq!((data.unit_alpha_tag.as_str(), data.tg_alpha_tag.as_str()), ("", ""));
        let f = |f: UnitEventFilter| s.read(|c| list_unit_events(c, &f)).unwrap();
        assert_eq!(f(UnitEventFilter { unit_ids: vec![20], ..Default::default() }).1, 2);
        assert_eq!(f(UnitEventFilter { tgids: vec![101], types: vec!["call".into(), "on".into()], ..Default::default() }).1, 1);
        assert_eq!(f(UnitEventFilter { emergency: Some(true), ..Default::default() }).1, 1);
        assert_eq!(f(UnitEventFilter { start_ms: Some(200), end_ms: Some(300), desc: false, ..Default::default() }).0.len(), 2);
        assert_eq!(f(UnitEventFilter { sort: EventSort::Unit, desc: false, limit: 1, ..Default::default() }).0[0].unit, 10);
        let since = s.read(|c| affiliation_events_since(c, 200)).unwrap();
        assert_eq!(since.iter().map(|e| (e.event_type.as_str(), e.time_ms)).collect::<Vec<_>>(), [("call", 300)]);
        let since = s.read(|c| affiliation_events_since(c, 0)).unwrap();
        assert_eq!(since.iter().map(|e| e.time_ms).collect::<Vec<_>>(), [100, 300]);
    }
}
