//! Calls with their transmissions and frequencies.

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, named_params, params_from_iter};
use trunk_recorder_plugin::{CallInfo, CallRecord, SrcEntry};

use super::{CallId, SiteId, SystemId};

/// A call as it starts (and the fields a call-end refreshes).
#[derive(Clone, Debug)]
pub struct NewCall {
    pub system_id: SystemId,
    pub site_id: Option<SiteId>,
    pub call_num: Option<u32>,
    pub tgid: u32,
    pub start_ms: i64,
    pub freq_hz: u64,
    pub tdma_slot: Option<u8>,
    pub analog: bool,
    pub encrypted: bool,
    pub algid: Option<u8>,
    pub key_id: Option<u16>,
    pub decrypted: bool,
    pub emergency: bool,
    pub duplex: bool,
    pub packet: bool,
    pub priority: u8,
    pub recorded: bool,
    pub not_recorded_reason: Option<String>,
    pub tg_alpha_tag: String,
    pub patched_tgids: Vec<u32>,
}

impl NewCall {
    pub fn from_info(info: &CallInfo, system_id: SystemId, site_id: Option<SiteId>) -> NewCall {
        NewCall {
            system_id,
            site_id,
            call_num: Some(info.id),
            tgid: info.talkgroup,
            start_ms: (info.start_time * 1000.0) as i64,
            freq_hz: info.freq_hz,
            tdma_slot: info.tdma_slot,
            analog: info.analog,
            encrypted: info.encrypted,
            algid: info.algid,
            key_id: info.key_id,
            decrypted: info.decrypted,
            emergency: info.emergency,
            duplex: info.duplex,
            packet: info.packet,
            priority: info.priority,
            recorded: info.recording,
            not_recorded_reason: info.reason.clone(),
            tg_alpha_tag: info.talkgroup_tag.clone(),
            patched_tgids: info.patched_talkgroups.clone(),
        }
    }
}

/// `[101,65001]`, or NULL when there are none.
fn patched_json(tgids: &[u32]) -> Option<String> {
    if tgids.is_empty() {
        return None;
    }
    Some(format!("[{}]", tgids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")))
}

fn parse_patched(text: Option<String>) -> Vec<u32> {
    text.and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default()
}

pub fn call_started(conn: &Connection, c: &NewCall) -> rusqlite::Result<CallId> {
    conn.query_row(
        "INSERT INTO calls (system_id, site_id, call_num, tgid, start_ms, freq_hz, tdma_slot, analog, encrypted, algid, key_id, decrypted, emergency, duplex, packet, priority, \
         recorded, not_recorded_reason, tg_alpha_tag, patched_tgids, state) \
         VALUES (:system_id, :site_id, :call_num, :tgid, :start_ms, :freq_hz, :tdma_slot, :analog, :encrypted, :algid, :key_id, :decrypted, :emergency, :duplex, :packet, :priority, \
         :recorded, :reason, :tg_alpha_tag, :patched, 'recording') RETURNING id",
        named_params! {
            ":system_id": c.system_id, ":site_id": c.site_id, ":call_num": c.call_num, ":tgid": c.tgid, ":start_ms": c.start_ms,
            ":freq_hz": c.freq_hz as i64, ":tdma_slot": c.tdma_slot, ":analog": c.analog, ":encrypted": c.encrypted,
            ":algid": c.algid, ":key_id": c.key_id, ":decrypted": c.decrypted, ":emergency": c.emergency, ":duplex": c.duplex,
            ":packet": c.packet, ":priority": c.priority, ":recorded": c.recorded, ":reason": c.not_recorded_reason,
            ":tg_alpha_tag": c.tg_alpha_tag, ":patched": patched_json(&c.patched_tgids),
        },
        |r| r.get(0),
    )
}

/// The call stopped: it is `ended` (a call already `concluded` stays so).
pub fn call_ended(conn: &Connection, id: CallId, end_ms: i64, info: &CallInfo) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE calls SET end_ms = :end_ms, length_ms = :end_ms - start_ms, state = 'ended', recorded = :recorded, not_recorded_reason = :reason, \
         encrypted = :encrypted, algid = :algid, key_id = :key_id, decrypted = :decrypted, emergency = :emergency, patched_tgids = :patched \
         WHERE id = :id AND state <> 'concluded'",
        named_params! {
            ":id": id, ":end_ms": end_ms, ":recorded": info.recording, ":reason": info.reason, ":encrypted": info.encrypted,
            ":algid": info.algid, ":key_id": info.key_id, ":decrypted": info.decrypted, ":emergency": info.emergency,
            ":patched": patched_json(&info.patched_talkgroups),
        },
    )
    .map(|_| ())
}

/// A transmission's length: to the next key-up, or to the call's end for the last one.
pub fn transmission_len_s(src_list: &[SrcEntry], i: usize, call_length_s: f64) -> f64 {
    match src_list.get(i + 1) {
        Some(next) => next.pos - src_list[i].pos,
        None => call_length_s - src_list[i].pos,
    }
}

/// The call's files are on disk. Updates `existing`, else inserts (or, for a path seen before, updates that row).
#[allow(clippy::too_many_arguments)]
pub fn call_concluded(
    conn: &Connection,
    system_id: SystemId,
    site_id: Option<SiteId>,
    path: &str,
    record: &CallRecord,
    audio_path: Option<&str>,
    json_path: &str,
    existing: Option<CallId>,
) -> rusqlite::Result<CallId> {
    let start_ms = if record.start_time_ms != 0 { record.start_time_ms } else { record.start_time * 1000 };
    let end_ms = if record.stop_time_ms != 0 { record.stop_time_ms } else { record.stop_time * 1000 };
    let length_ms = if record.call_length_ms != 0 { record.call_length_ms } else { (record.call_length * 1000.0) as i64 };
    let analog = record.audio_type == "analog";
    let metadata = (!record.extra.is_empty()).then(|| serde_json::to_string(&record.extra).unwrap_or_default());
    let slot = (record.tdma_slot >= 0).then_some(record.tdma_slot);
    let opt = |v: i64| (v >= 0).then_some(v);
    let conflict = if existing.is_some() { "id" } else { "path" };
    let sql = format!(
        "INSERT INTO calls (id, path, system_id, site_id, call_num, tgid, start_ms, end_ms, length_ms, freq_hz, tdma_slot, phase2_tdma, analog, encrypted, algid, key_id, decrypted, \
         emergency, duplex, packet, priority, recorded, not_recorded_reason, audio_path, json_path, audio_type, signal, noise, snr, clean_voice_pct, error_count, spike_count, \
         freq_error, state, tg_alpha_tag, tg_description, tg_tag, tg_group, patched_tgids, metadata_json) \
         VALUES (:id, :path, :system_id, :site_id, :call_num, :tgid, :start_ms, :end_ms, :length_ms, :freq_hz, :tdma_slot, :phase2, :analog, :encrypted, :algid, :key_id, :decrypted, \
         :emergency, :duplex, :packet, :priority, 1, NULL, :audio_path, :json_path, :audio_type, :signal, :noise, :snr, :clean, :errors, :spikes, \
         :freq_error, 'concluded', :tg_alpha_tag, :tg_description, :tg_tag, :tg_group, :patched, :metadata) \
         ON CONFLICT({conflict}) DO UPDATE SET path = excluded.path, system_id = excluded.system_id, site_id = COALESCE(excluded.site_id, site_id), \
         call_num = excluded.call_num, tgid = excluded.tgid, start_ms = excluded.start_ms, end_ms = excluded.end_ms, length_ms = excluded.length_ms, \
         freq_hz = excluded.freq_hz, tdma_slot = excluded.tdma_slot, phase2_tdma = excluded.phase2_tdma, analog = excluded.analog, encrypted = excluded.encrypted, \
         algid = excluded.algid, key_id = excluded.key_id, decrypted = excluded.decrypted, emergency = excluded.emergency, duplex = excluded.duplex, \
         packet = excluded.packet, priority = excluded.priority, recorded = 1, not_recorded_reason = NULL, audio_path = excluded.audio_path, json_path = excluded.json_path, \
         audio_type = excluded.audio_type, signal = excluded.signal, noise = excluded.noise, snr = excluded.snr, clean_voice_pct = excluded.clean_voice_pct, \
         error_count = excluded.error_count, spike_count = excluded.spike_count, freq_error = excluded.freq_error, state = 'concluded', \
         tg_alpha_tag = excluded.tg_alpha_tag, tg_description = excluded.tg_description, tg_tag = excluded.tg_tag, tg_group = excluded.tg_group, \
         patched_tgids = excluded.patched_tgids, metadata_json = excluded.metadata_json RETURNING id"
    );
    let id: CallId = conn.query_row(
        &sql,
        named_params! {
            ":id": existing, ":path": path, ":system_id": system_id, ":site_id": site_id, ":call_num": record.call_num as i64, ":tgid": record.talkgroup,
            ":start_ms": start_ms, ":end_ms": end_ms, ":length_ms": length_ms, ":freq_hz": record.freq as i64, ":tdma_slot": slot,
            ":phase2": record.phase2_tdma, ":analog": analog, ":encrypted": record.encrypted, ":algid": opt(record.algid), ":key_id": opt(record.key_id),
            ":decrypted": record.decrypted, ":emergency": record.emergency, ":duplex": record.duplex, ":packet": record.mode,
            ":priority": record.priority, ":audio_path": audio_path, ":json_path": json_path, ":audio_type": record.audio_type,
            ":signal": record.signal, ":noise": record.noise, ":snr": record.snr, ":clean": record.clean_voice_pct,
            ":errors": record.error_count() as i64, ":spikes": record.spike_count() as i64, ":freq_error": record.freq_error,
            ":tg_alpha_tag": record.talkgroup_tag, ":tg_description": record.talkgroup_description, ":tg_tag": record.talkgroup_group_tag,
            ":tg_group": record.talkgroup_group, ":patched": patched_json(&record.patched_talkgroups), ":metadata": metadata,
        },
        |r| r.get(0),
    )?;
    conn.execute("DELETE FROM call_transmissions WHERE call_id = ?1", [id])?;
    conn.execute("DELETE FROM call_frequencies WHERE call_id = ?1", [id])?;
    let mut tx = conn
        .prepare("INSERT INTO call_transmissions (call_id, position, unit, time_ms, pos_s, len_s, emergency, tag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")?;
    for (i, s) in record.src_list.iter().enumerate() {
        tx.execute((id, i as i64, s.src, s.time * 1000, s.pos, transmission_len_s(&record.src_list, i, record.call_length), s.emergency, &s.tag))?;
    }
    let mut fq = conn.prepare(
        "INSERT INTO call_frequencies (call_id, position, freq_hz, time_ms, pos_s, len_s, error_count, spike_count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?;
    for (i, f) in record.freq_list.iter().enumerate() {
        fq.execute((id, i as i64, f.freq as i64, f.time * 1000, f.pos, f.len, f.error_count as i64, f.spike_count as i64))?;
    }
    Ok(id)
}

#[derive(Clone, Debug, PartialEq)]
pub struct CallRow {
    pub id: CallId,
    pub path: Option<String>,
    pub system_id: SystemId,
    pub site_id: Option<SiteId>,
    pub call_num: Option<u32>,
    pub tgid: u32,
    pub start_ms: i64,
    pub end_ms: Option<i64>,
    pub length_ms: Option<i64>,
    pub freq_hz: Option<u64>,
    pub tdma_slot: Option<u8>,
    pub phase2_tdma: bool,
    pub analog: bool,
    pub encrypted: bool,
    pub algid: Option<u8>,
    pub key_id: Option<u16>,
    pub decrypted: bool,
    pub emergency: bool,
    pub duplex: bool,
    pub packet: bool,
    pub priority: u8,
    pub recorded: bool,
    pub not_recorded_reason: Option<String>,
    pub audio_path: Option<String>,
    pub json_path: Option<String>,
    pub audio_type: String,
    pub signal: Option<f64>,
    pub noise: Option<f64>,
    pub snr: Option<f64>,
    pub clean_voice_pct: Option<f64>,
    pub error_count: u64,
    pub spike_count: u64,
    pub freq_error: i64,
    pub state: String,
    pub tg_alpha_tag: String,
    pub tg_description: String,
    pub tg_tag: String,
    pub tg_group: String,
    pub patched_tgids: Vec<u32>,
    pub metadata_json: Option<String>,
}

const CALL_COLS: &str = "id, path, system_id, site_id, call_num, tgid, start_ms, end_ms, length_ms, freq_hz, tdma_slot, phase2_tdma, analog, encrypted, algid, key_id, decrypted, \
     emergency, duplex, packet, priority, recorded, not_recorded_reason, audio_path, json_path, audio_type, signal, noise, snr, clean_voice_pct, error_count, spike_count, \
     freq_error, state, tg_alpha_tag, tg_description, tg_tag, tg_group, patched_tgids, metadata_json";

fn call_row(r: &Row<'_>) -> rusqlite::Result<CallRow> {
    Ok(CallRow {
        id: r.get(0)?,
        path: r.get(1)?,
        system_id: r.get(2)?,
        site_id: r.get(3)?,
        call_num: r.get(4)?,
        tgid: r.get(5)?,
        start_ms: r.get(6)?,
        end_ms: r.get(7)?,
        length_ms: r.get(8)?,
        freq_hz: r.get::<_, Option<i64>>(9)?.map(|h| h as u64),
        tdma_slot: r.get(10)?,
        phase2_tdma: r.get(11)?,
        analog: r.get(12)?,
        encrypted: r.get(13)?,
        algid: r.get(14)?,
        key_id: r.get(15)?,
        decrypted: r.get(16)?,
        emergency: r.get(17)?,
        duplex: r.get(18)?,
        packet: r.get(19)?,
        priority: r.get(20)?,
        recorded: r.get(21)?,
        not_recorded_reason: r.get(22)?,
        audio_path: r.get(23)?,
        json_path: r.get(24)?,
        audio_type: r.get(25)?,
        signal: r.get(26)?,
        noise: r.get(27)?,
        snr: r.get(28)?,
        clean_voice_pct: r.get(29)?,
        error_count: r.get::<_, i64>(30)? as u64,
        spike_count: r.get::<_, i64>(31)? as u64,
        freq_error: r.get(32)?,
        state: r.get(33)?,
        tg_alpha_tag: r.get(34)?,
        tg_description: r.get(35)?,
        tg_tag: r.get(36)?,
        tg_group: r.get(37)?,
        patched_tgids: parse_patched(r.get(38)?),
        metadata_json: r.get(39)?,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CallSort {
    #[default]
    StartTime,
    StopTime,
    Duration,
    Tgid,
    Freq,
}

impl CallSort {
    fn column(self) -> &'static str {
        match self {
            CallSort::StartTime => "start_ms",
            CallSort::StopTime => "end_ms",
            CallSort::Duration => "length_ms",
            CallSort::Tgid => "tgid",
            CallSort::Freq => "freq_hz",
        }
    }
}

/// Filters for [`list_calls`]. An empty list or `None` does not filter; `start_ms`/`end_ms` bound the call's start time (inclusive).
#[derive(Clone, Debug)]
pub struct CallFilter {
    pub system_ids: Vec<i64>,
    pub site_ids: Vec<i64>,
    pub tgids: Vec<u32>,
    pub unit_ids: Vec<u32>,
    pub emergency: Option<bool>,
    pub encrypted: Option<bool>,
    pub recorded: Option<bool>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub sort: CallSort,
    pub desc: bool,
    pub limit: u32,
    pub offset: u32,
}

impl Default for CallFilter {
    fn default() -> Self {
        CallFilter {
            system_ids: Vec::new(),
            site_ids: Vec::new(),
            tgids: Vec::new(),
            unit_ids: Vec::new(),
            emergency: None,
            encrypted: None,
            recorded: None,
            start_ms: None,
            end_ms: None,
            sort: CallSort::StartTime,
            desc: true,
            limit: 50,
            offset: 0,
        }
    }
}

pub(super) fn push_in(conds: &mut Vec<String>, params: &mut Vec<Value>, prefix: &str, suffix: &str, values: &[i64]) {
    if values.is_empty() {
        return;
    }
    let marks: Vec<String> = values
        .iter()
        .map(|v| {
            params.push(Value::Integer(*v));
            format!("?{}", params.len())
        })
        .collect();
    conds.push(format!("{prefix}IN ({}){suffix}", marks.join(", ")));
}

/// The page of calls the filter matches, and how many match in all.
pub fn list_calls(conn: &Connection, f: &CallFilter) -> rusqlite::Result<(Vec<CallRow>, i64)> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    push_in(&mut conds, &mut params, "system_id ", "", &f.system_ids);
    push_in(&mut conds, &mut params, "site_id ", "", &f.site_ids);
    push_in(&mut conds, &mut params, "tgid ", "", &f.tgids.iter().map(|&t| t as i64).collect::<Vec<_>>());
    push_in(
        &mut conds,
        &mut params,
        "EXISTS (SELECT 1 FROM call_transmissions t WHERE t.call_id = calls.id AND t.unit ",
        ")",
        &f.unit_ids.iter().map(|&u| u as i64).collect::<Vec<_>>(),
    );
    for (col, v) in [("emergency", f.emergency), ("encrypted", f.encrypted), ("recorded", f.recorded)] {
        if let Some(b) = v {
            params.push(Value::Integer(b as i64));
            conds.push(format!("{col} = ?{}", params.len()));
        }
    }
    if let Some(ms) = f.start_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("start_ms >= ?{}", params.len()));
    }
    if let Some(ms) = f.end_ms {
        params.push(Value::Integer(ms));
        conds.push(format!("start_ms <= ?{}", params.len()));
    }
    let wher = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    let total: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM calls{wher}"), params_from_iter(params.iter()), |r| r.get(0))?;
    let n = params.len();
    params.push(Value::Integer(f.limit as i64));
    params.push(Value::Integer(f.offset as i64));
    let sql = format!(
        "SELECT {CALL_COLS} FROM calls{wher} ORDER BY {col} {dir}, id {dir} LIMIT ?{} OFFSET ?{}",
        n + 1,
        n + 2,
        col = f.sort.column(),
        dir = if f.desc { "DESC" } else { "ASC" },
    );
    let rows = conn.prepare(&sql)?.query_map(params_from_iter(params.iter()), call_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

pub fn call_by_id(conn: &Connection, id: CallId) -> rusqlite::Result<Option<CallRow>> {
    conn.query_row(&format!("SELECT {CALL_COLS} FROM calls WHERE id = ?1"), [id], call_row).optional()
}

/// Calls still being recorded, oldest first.
pub fn active_calls(conn: &Connection) -> rusqlite::Result<Vec<CallRow>> {
    conn.prepare(&format!("SELECT {CALL_COLS} FROM calls WHERE state = 'recording' ORDER BY start_ms, id"))?.query_map([], call_row)?.collect()
}

#[derive(Clone, Debug, PartialEq)]
pub struct TransmissionRow {
    pub position: i64,
    pub unit: u32,
    pub time_ms: i64,
    pub pos_s: f64,
    pub len_s: f64,
    pub emergency: bool,
    pub tag: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrequencyRow {
    pub position: i64,
    pub freq_hz: u64,
    pub time_ms: i64,
    pub pos_s: f64,
    pub len_s: f64,
    pub error_count: u64,
    pub spike_count: u64,
}

pub fn transmissions(conn: &Connection, id: CallId) -> rusqlite::Result<Vec<TransmissionRow>> {
    conn.prepare("SELECT position, unit, time_ms, pos_s, len_s, emergency, tag FROM call_transmissions WHERE call_id = ?1 ORDER BY position")?
        .query_map([id], |r| {
            Ok(TransmissionRow {
                position: r.get(0)?,
                unit: r.get(1)?,
                time_ms: r.get(2)?,
                pos_s: r.get(3)?,
                len_s: r.get(4)?,
                emergency: r.get(5)?,
                tag: r.get(6)?,
            })
        })?
        .collect()
}

pub fn frequencies(conn: &Connection, id: CallId) -> rusqlite::Result<Vec<FrequencyRow>> {
    conn.prepare("SELECT position, freq_hz, time_ms, pos_s, len_s, error_count, spike_count FROM call_frequencies WHERE call_id = ?1 ORDER BY position")?
        .query_map([id], |r| {
            Ok(FrequencyRow {
                position: r.get(0)?,
                freq_hz: r.get::<_, i64>(1)? as u64,
                time_ms: r.get(2)?,
                pos_s: r.get(3)?,
                len_s: r.get(4)?,
                error_count: r.get::<_, i64>(5)? as u64,
                spike_count: r.get::<_, i64>(6)? as u64,
            })
        })?
        .collect()
}

/// The radios heard on the call, each once, in the order first heard.
pub fn units_of(conn: &Connection, id: CallId) -> rusqlite::Result<Vec<u32>> {
    conn.prepare("SELECT unit FROM call_transmissions WHERE call_id = ?1 GROUP BY unit ORDER BY MIN(position)")?.query_map([id], |r| r.get(0))?.collect()
}

/// When the system's newest call started.
pub fn newest_start_ms(conn: &Connection, system: SystemId) -> rusqlite::Result<Option<i64>> {
    conn.query_row("SELECT MAX(start_ms) FROM calls WHERE system_id = ?1", [system], |r| r.get(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::systems::{ensure_site, ensure_system};
    use trunk_recorder_plugin::testing::{call, temp_dir};

    fn src_at(pos: f64) -> SrcEntry {
        SrcEntry { pos, ..Default::default() }
    }

    fn setup(s: &Store) -> (SystemId, SiteId) {
        s.write_sync(|c| {
            let sys = ensure_system(c, "sys1", "p25", "", 1_000)?;
            Ok((sys, ensure_site(c, sys, "sys1", 1_000)?))
        })
        .unwrap()
    }

    fn info(tg: u32, start_s: f64) -> CallInfo {
        CallInfo { id: tg, talkgroup: tg, talkgroup_tag: format!("TG {tg}"), freq_hz: 851_000_000, recording: true, start_time: start_s, ..Default::default() }
    }

    fn record(tg: u32) -> CallRecord {
        call(&temp_dir("calls"), "sys1", tg).call
    }

    #[test]
    fn a_call_goes_start_end_concluded_and_keeps_one_row() {
        let s = Store::in_memory();
        let (sys, site) = setup(&s);
        let rec = record(101);
        let i = info(101, rec.start_time as f64);
        let id = s.write_sync(move |c| call_started(c, &NewCall::from_info(&i, sys, Some(site)))).unwrap();
        assert_eq!(s.read(|c| call_by_id(c, id)).unwrap().unwrap().state, "recording");
        assert_eq!(s.read(active_calls).unwrap().len(), 1);
        let i = info(101, rec.start_time as f64);
        let end = rec.start_time * 1000 + 3000;
        s.write_sync(move |c| call_ended(c, id, end, &i)).unwrap();
        let row = s.read(|c| call_by_id(c, id)).unwrap().unwrap();
        assert_eq!((row.state.as_str(), row.length_ms, row.end_ms), ("ended", Some(3000), Some(end)));
        let r2 = rec.clone();
        let id2 = s.write_sync(move |c| call_concluded(c, sys, Some(site), "sys1/a", &r2, Some("sys1/a.m4a"), "sys1/a.json", Some(id))).unwrap();
        assert_eq!(id2, id);
        let row = s.read(|c| call_by_id(c, id)).unwrap().unwrap();
        assert_eq!((row.state.as_str(), row.recorded, row.audio_path.as_deref(), row.length_ms), ("concluded", true, Some("sys1/a.m4a"), Some(3000)));
        assert_eq!((row.path.as_deref(), row.json_path.as_deref(), row.tgid, row.site_id), (Some("sys1/a"), Some("sys1/a.json"), 101, Some(site)));
        assert_eq!(s.read(|c| transmissions(c, id)).unwrap().len(), 1);
        assert_eq!(s.read(|c| frequencies(c, id)).unwrap().len(), 1);
        assert_eq!(s.read(|c| list_calls(c, &CallFilter::default())).unwrap().1, 1);
        assert_eq!(s.read(active_calls).unwrap().len(), 0);
    }

    #[test]
    fn a_concluded_call_without_a_start_is_inserted_from_the_record() {
        let s = Store::in_memory();
        let (sys, _) = setup(&s);
        let mut rec = record(101);
        rec.start_time_ms = 0;
        rec.stop_time_ms = 0;
        rec.algid = -1;
        rec.key_id = -1;
        rec.patched_talkgroups = vec![101, 65001];
        let expect = rec.start_time * 1000;
        let r = rec.clone();
        let id = s.write_sync(move |c| call_concluded(c, sys, None, "sys1/b", &r, None, "sys1/b.json", None)).unwrap();
        let row = s.read(|c| call_by_id(c, id)).unwrap().unwrap();
        assert_eq!(row.start_ms, expect);
        assert_eq!(row.end_ms, Some(rec.stop_time * 1000));
        assert_eq!(row.tg_alpha_tag, "TG 101");
        assert_eq!((row.algid, row.key_id, row.site_id, row.audio_path.clone()), (None, None, None, None));
        assert_eq!(row.patched_tgids, vec![101, 65001]);
        let raw: Option<String> = s.read(|c| c.query_row("SELECT patched_tgids FROM calls WHERE id = ?1", [id], |r| r.get(0))).unwrap();
        assert_eq!(raw.as_deref(), Some("[101,65001]"));
        assert_eq!(s.read(|c| units_of(c, id)).unwrap(), vec![1234]);
        assert_eq!(s.read(|c| newest_start_ms(c, sys)).unwrap(), Some(expect));
    }

    #[test]
    fn the_same_path_concluded_twice_is_one_row() {
        let s = Store::in_memory();
        let (sys, _) = setup(&s);
        let rec = record(101);
        let mut ids = Vec::new();
        for _ in 0..2 {
            let r = rec.clone();
            ids.push(s.write_sync(move |c| call_concluded(c, sys, None, "sys1/c", &r, Some("sys1/c.m4a"), "sys1/c.json", None)).unwrap());
        }
        assert_eq!(ids[0], ids[1]);
        assert_eq!(s.read(|c| list_calls(c, &CallFilter::default())).unwrap().1, 1);
        assert_eq!(s.read(|c| transmissions(c, ids[0])).unwrap().len(), 1);
        assert_eq!(s.read(|c| frequencies(c, ids[0])).unwrap().len(), 1);
    }

    #[test]
    fn an_unrecorded_call_keeps_its_row_and_reason() {
        let s = Store::in_memory();
        let (sys, site) = setup(&s);
        let mut i = info(202, 1_700_000_000.0);
        i.recording = false;
        i.reason = Some("encrypted".into());
        let i2 = i.clone();
        let id = s.write_sync(move |c| call_started(c, &NewCall::from_info(&i2, sys, Some(site)))).unwrap();
        s.write_sync(move |c| call_ended(c, id, 1_700_000_002_000, &i)).unwrap();
        let row = s.read(|c| call_by_id(c, id)).unwrap().unwrap();
        assert_eq!((row.state.as_str(), row.recorded, row.audio_path, row.not_recorded_reason.as_deref()), ("ended", false, None, Some("encrypted")));
    }

    #[test]
    fn transmission_lengths_run_to_the_next_key_up() {
        let l = [src_at(0.0), src_at(2.5), src_at(4.0)];
        assert_eq!(transmission_len_s(&l, 0, 10.0), 2.5);
        assert_eq!(transmission_len_s(&l, 1, 10.0), 1.5);
        assert_eq!(transmission_len_s(&l, 2, 10.0), 6.0);
    }

    #[test]
    fn list_calls_filters_sorts_and_counts() {
        let s = Store::in_memory();
        let (sys, site) = setup(&s);
        for (n, (tg, emergency, unit)) in [(101u32, false, 1234i64), (101, true, 1234), (202, false, 1234), (202, false, 999)].into_iter().enumerate() {
            let mut r = record(tg);
            r.start_time_ms = (1_700_000_000 + n as i64) * 1000;
            r.stop_time_ms = r.start_time_ms + 3000;
            r.emergency = emergency;
            r.src_list[0].src = unit;
            let path = format!("sys1/{n}");
            s.write_sync(move |c| call_concluded(c, sys, Some(site), &path, &r, Some("a.m4a"), "a.json", None)).unwrap();
        }
        let mut i = info(101, 1_700_000_010.0);
        i.recording = false;
        i.reason = Some("no_recorder".into());
        let unrecorded = s.write_sync(move |c| call_started(c, &NewCall::from_info(&i, sys, Some(site)))).unwrap();

        let list = |f: CallFilter| s.read(|c| list_calls(c, &f)).unwrap();
        assert_eq!(list(CallFilter::default()).1, 5);
        let (rows, total) = list(CallFilter { tgids: vec![101], ..Default::default() });
        assert_eq!((rows.len(), total), (3, 3));
        assert_eq!(rows[0].id, unrecorded, "newest first by default");
        assert_eq!(list(CallFilter { recorded: Some(false), ..Default::default() }).1, 1);
        assert_eq!(list(CallFilter { emergency: Some(true), ..Default::default() }).1, 1);
        assert_eq!(list(CallFilter { encrypted: Some(true), ..Default::default() }).1, 0);
        assert_eq!(list(CallFilter { unit_ids: vec![1234], ..Default::default() }).1, 3);
        assert_eq!(list(CallFilter { unit_ids: vec![999, 5], ..Default::default() }).1, 1);
        assert_eq!(list(CallFilter { system_ids: vec![sys.0], site_ids: vec![site.0], ..Default::default() }).1, 5);
        assert_eq!(list(CallFilter { system_ids: vec![sys.0 + 1], ..Default::default() }).1, 0);
        let (rows, total) = list(CallFilter { start_ms: Some(1_700_000_001_000), end_ms: Some(1_700_000_002_000), ..Default::default() });
        assert_eq!((rows.len(), total), (2, 2));
        let (rows, total) = list(CallFilter { tgids: vec![101], limit: 2, offset: 2, ..Default::default() });
        assert_eq!((rows.len(), total), (1, 3));
        let (rows, _) = list(CallFilter { sort: CallSort::Tgid, desc: false, ..Default::default() });
        assert_eq!(rows.iter().map(|r| r.tgid).collect::<Vec<_>>(), vec![101, 101, 101, 202, 202]);
        let (rows, _) = list(CallFilter { sort: CallSort::Tgid, desc: true, ..Default::default() });
        assert_eq!(rows[0].tgid, 202);
    }
}
