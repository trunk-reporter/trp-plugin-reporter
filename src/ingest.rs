//! TRP's plugin events → store rows and bus events.
//!
//! [`Ingest::handle`] runs on the plugin's message thread and never waits on
//! the database except the first time it sees a system or site (to learn its
//! row id). Everything else is a queued write that owns its data; its side
//! effects (bus events, the affiliation map, a new call's row id) happen in
//! the write's after-commit hook, only if it committed, so bus events come
//! out in the order the recorder sent the messages and never for rolled-back rows.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use trunk_recorder_plugin::{CallInfo, CallRecord, ConcludedCall, Host, HostMessage, NoConfig, Outcome, Status, System, UnitEvent};

use crate::affiliations::{Affiliations, Names};
use crate::api::types::{Call as ApiCall, DecodeRate, Recorder, UnitEvent as ApiUnitEvent};
use crate::bus::{Bus, EventKind, NewEvent};
use crate::config::Config;
use crate::store::calls::{NewCall, call_by_id, call_concluded, call_ended, call_started};
use crate::store::recorder::{RecorderStateRow, first_state_ms, insert_recorder_state};
use crate::store::systems::{Identity, ensure_site, ensure_system, set_identity, site_by_id, system_by_id};
use crate::store::talkgroups::{TgTags, tag_talkgroup, touch_talkgroup};
use crate::store::units::{NewUnitEvent, event_type_of, insert_unit_event, touch_unit, transmission_events, unit_event_by_id};
use crate::store::{CallId, SiteId, Store, StoreError, SystemId, now_ms};

/// A call between its `call.start` and its `call.concluded` (or its unrecorded `call.end`).
struct Live {
    /// Set once the `call.start` write has committed; later writes for the call
    /// run after it on the writer, so they find it set (unless it failed).
    id: Arc<OnceLock<CallId>>,
    system_id: SystemId,
    site_id: SiteId,
    /// Units already published as a `unit_event` (`call`) for this call.
    units_told: HashSet<u32>,
}

pub struct Ingest {
    store: Store,
    bus: Bus,
    affiliations: Affiliations,
    host: Option<Host>,
    capture_dir: Option<PathBuf>,
    /// The recorder's name: the `instance_id` of the events published.
    host_name: String,
    /// System row by short name (a system's identity) and by this run's index.
    by_name: HashMap<String, SystemId>,
    by_index: HashMap<u16, SystemId>,
    sites: HashMap<(SystemId, String), SiteId>,
    live: HashMap<(u16, u32), Live>,
    dropped: u64,
}

/// A conventional system's events carry no site: its one site is named after the system.
fn site_name<'a>(site: &'a str, short_name: &'a str) -> &'a str {
    if site.is_empty() { short_name } else { site }
}

/// The record's stop time in ms (as the store reads it).
fn record_end_ms(r: &CallRecord) -> i64 {
    if r.stop_time_ms != 0 { r.stop_time_ms } else { r.stop_time * 1000 }
}

fn record_start_ms(r: &CallRecord) -> i64 {
    if r.start_time_ms != 0 { r.start_time_ms } else { r.start_time * 1000 }
}

/// A `call_*` bus event for the call's row as it is now.
fn call_event(conn: &Connection, id: CallId, kind: EventKind) -> rusqlite::Result<Option<NewEvent>> {
    let Some(row) = call_by_id(conn, id)? else { return Ok(None) };
    Ok(Some(NewEvent {
        kind,
        sub_type: None,
        system_id: Some(row.system_id.0),
        site_id: row.site_id.map(|s| s.0),
        tgid: Some(row.tgid),
        unit_id: None,
        emergency: row.emergency,
        data: serde_json::to_string(&ApiCall::from_row(conn, &row)?).unwrap_or_default(),
    }))
}

/// The bus payload for `unit_event`: the API's `UnitEvent`, as stored (names and all).
fn unit_event_json(conn: &Connection, id: i64, instance: &str) -> rusqlite::Result<Value> {
    let row = unit_event_by_id(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    Ok(serde_json::to_value(ApiUnitEvent::from_row(&row, instance)).unwrap_or_default())
}

fn unit_event(conn: &Connection, id: i64, e: &NewUnitEvent, instance: &str) -> rusqlite::Result<NewEvent> {
    Ok(NewEvent {
        kind: EventKind::UnitEvent,
        sub_type: Some(e.event_type.to_string()),
        system_id: Some(e.system_id.0),
        site_id: e.site_id.map(|s| s.0),
        tgid: e.tgid,
        unit_id: Some(e.unit),
        emergency: e.emergency,
        data: unit_event_json(conn, id, instance)?.to_string(),
    })
}

/// The bus payloads for `recorder_update` and `rate_update`: the site's newest sample.
fn status_payloads(conn: &Connection, site_id: SiteId, state: &RecorderStateRow) -> rusqlite::Result<(String, String)> {
    let empty = || ("{}".to_string(), "{}".to_string());
    let Some(site) = site_by_id(conn, site_id)? else { return Ok(empty()) };
    let Some(system) = system_by_id(conn, site.system_id)? else { return Ok(empty()) };
    let first = first_state_ms(conn, site_id)?.unwrap_or(state.time_ms);
    let recorder = Recorder::from_site(&site, &system, state, first, state.time_ms);
    let rate = DecodeRate::from_state(&site, &system, state);
    Ok((json!(recorder).to_string(), json!(rate).to_string()))
}

/// The names the affiliation map shows beside an entry.
fn names(conn: &Connection, system_id: SystemId, unit: u32, tgid: Option<u32>) -> rusqlite::Result<Names> {
    let (system_name, sysid): (String, Option<u32>) =
        conn.query_row("SELECT name, sysid FROM systems WHERE id = ?1", [system_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let unit_alpha_tag: String =
        conn.query_row("SELECT alpha_tag FROM units WHERE system_id = ?1 AND unit_id = ?2", (system_id, unit), |r| r.get(0)).optional()?.unwrap_or_default();
    let (tg_alpha_tag, tg_description, tg_tag, tg_group) = match tgid {
        Some(tg) => conn
            .query_row("SELECT alpha_tag, description, tag, \"group\" FROM talkgroups WHERE system_id = ?1 AND tgid = ?2", (system_id, tg), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .optional()?
            .unwrap_or_default(),
        None => Default::default(),
    };
    Ok(Names { system_name, sysid: sysid.map(|s| format!("{s:X}")).unwrap_or_default(), unit_alpha_tag, tg_alpha_tag, tg_description, tg_tag, tg_group })
}

/// What a write's after-commit hook does, and only once the write committed:
/// record a new call's row id, update the affiliation map, publish on the bus.
#[derive(Default)]
struct After {
    call: Option<(Arc<OnceLock<CallId>>, CallId)>,
    affiliations: Vec<(NewUnitEvent, Names)>,
    events: Vec<NewEvent>,
    /// The recorder's name, for the `instance_id` of published events.
    instance: String,
}

impl After {
    /// Feed the affiliation map with `e` (display names are best-effort).
    fn affiliate(&mut self, conn: &Connection, e: &NewUnitEvent) {
        self.affiliations.push((e.clone(), names(conn, e.system_id, e.unit, e.tgid).unwrap_or_default()));
    }
}

/// Insert synthesized `call`/`end` events. When `live`, those that are new feed
/// the affiliation map and each `call` event of a unit in `tell` is published.
/// (Backfilled history leaves the live map alone: the start-up rebuild covers it.)
fn insert_transmission_events(conn: &Connection, after: &mut After, live: bool, events: &[NewUnitEvent], tell: &mut HashSet<u32>) -> rusqlite::Result<()> {
    for e in events {
        let Some(id) = insert_unit_event(conn, e)? else { continue };
        if !live {
            continue;
        }
        after.affiliate(conn, e);
        if e.event_type == "call" && tell.remove(&e.unit) {
            let event = unit_event(conn, id, e, &after.instance)?;
            after.events.push(event);
        }
    }
    Ok(())
}

/// A file path as stored: relative to the capture folder when it is under it.
pub(crate) fn stored_path(capture_dir: Option<&Path>, p: &Path) -> String {
    match capture_dir.and_then(|d| p.strip_prefix(d).ok()) {
        Some(rel) => rel.to_string_lossy().replace('\\', "/"),
        None => p.to_string_lossy().into_owned(),
    }
}

/// A concluded call's audio: the recorder's files (chosen on the writer, which
/// may look at the disk), or a path already worked out (backfill).
enum Audio {
    Files { wav: PathBuf, m4a: Option<PathBuf>, capture_dir: Option<PathBuf> },
    Given(Option<String>),
}

impl Audio {
    /// The WAV, or the M4A when there is no WAV file.
    fn stored(self) -> Option<String> {
        match self {
            Audio::Given(p) => p,
            Audio::Files { wav, m4a, capture_dir } => {
                let file = match m4a {
                    Some(m4a) if !wav.is_file() => m4a,
                    _ => wav,
                };
                (!file.as_os_str().is_empty()).then(|| stored_path(capture_dir.as_deref(), &file))
            }
        }
    }
}

/// What a `call.concluded` (or a backfilled record) writes.
struct Conclusion {
    system_id: SystemId,
    site_id: Option<SiteId>,
    path: String,
    record: CallRecord,
    audio: Audio,
    json_path: String,
    /// The live call's id, when its start was seen.
    existing: Option<Arc<OnceLock<CallId>>>,
    /// Units to publish a `call` unit event for; `None`: publish nothing (backfill).
    tell: Option<HashSet<u32>>,
}

impl Ingest {
    /// Ensure a `systems` row for each of the recorder's systems.
    pub fn new(store: Store, bus: Bus, affiliations: Affiliations, host: Option<Host>, systems: &[System<Config, NoConfig>]) -> Result<Ingest, StoreError> {
        let declared: Vec<(u16, String, String)> = systems.iter().map(|s| (s.index, s.short_name.clone(), s.kind.clone())).collect();
        let now = now_ms();
        let ids = store.write_sync(move |c| {
            declared
                .into_iter()
                .map(|(index, short_name, kind)| Ok((index, ensure_system(c, &short_name, &kind, "", now)?, short_name)))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let mut ingest = Ingest {
            store,
            bus,
            affiliations,
            host,
            capture_dir: None,
            host_name: String::new(),
            by_name: HashMap::new(),
            by_index: HashMap::new(),
            sites: HashMap::new(),
            live: HashMap::new(),
            dropped: 0,
        };
        for (index, id, short_name) in ids {
            ingest.by_index.insert(index, id);
            ingest.by_name.insert(short_name, id);
        }
        Ok(ingest)
    }

    /// Where the recorder keeps calls: a concluded call's file paths are stored relative to it.
    pub fn with_capture_dir(mut self, dir: PathBuf) -> Ingest {
        self.capture_dir = Some(dir);
        self
    }

    /// The recorder's name, published as the `instance_id` of unit events.
    pub fn with_host_name(mut self, name: &str) -> Ingest {
        self.host_name = name.to_string();
        self
    }

    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    pub(crate) fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    /// Unit events of a kind tr-engine has no type for.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    fn error(&self, msg: String) {
        if let Some(h) = &self.host {
            h.error(msg);
        }
    }

    /// The system's row id; a system the hello didn't name is added (waiting on the store).
    fn system(&mut self, index: Option<u16>, short_name: &str) -> Option<SystemId> {
        if let Some(id) = self.by_name.get(short_name) {
            return Some(*id);
        }
        if short_name.is_empty() {
            if let Some(id) = index.and_then(|i| self.by_index.get(&i)) {
                return Some(*id);
            }
            self.error("an event with no system, ignored".into());
            return None;
        }
        let name = short_name.to_string();
        match self.store.write_sync(move |c| ensure_system(c, &name, "", "", now_ms())) {
            Ok(id) => {
                self.by_name.insert(short_name.to_string(), id);
                Some(id)
            }
            Err(e) => {
                self.error(format!("could not add system {short_name}: {e}"));
                None
            }
        }
    }

    /// The site's row id, added the first time it is seen (waiting on the store).
    fn site(&mut self, system_id: SystemId, name: &str) -> Option<SiteId> {
        let key = (system_id, name.to_string());
        if let Some(id) = self.sites.get(&key) {
            return Some(*id);
        }
        let owned = name.to_string();
        match self.store.write_sync(move |c| ensure_site(c, system_id, &owned, now_ms())) {
            Ok(id) => {
                self.sites.insert(key, id);
                Some(id)
            }
            Err(e) => {
                self.error(format!("could not add site {name}: {e}"));
                None
            }
        }
    }

    /// Queue a write whose side effects (`After`) happen only once it has committed.
    fn commit(&self, f: impl FnOnce(&Connection, &mut After) -> rusqlite::Result<()> + Send + 'static) {
        let (bus, affiliations, instance) = (self.bus.clone(), self.affiliations.clone(), self.host_name.clone());
        self.store.write_then(
            move |c| {
                let mut after = After { instance, ..Default::default() };
                f(c, &mut after)?;
                Ok(after)
            },
            move |done| {
                let Some(after) = done else { return };
                if let Some((cell, id)) = after.call {
                    let _ = cell.set(id);
                }
                for (e, names) in after.affiliations {
                    affiliations.apply(e.system_id.0, e.unit, e.event_type, e.tgid, e.time_ms, names);
                }
                for event in after.events {
                    bus.publish(event);
                }
            },
        );
    }

    pub fn handle(&mut self, msg: HostMessage) {
        match msg {
            HostMessage::CallStart(info) => self.call_start(info),
            HostMessage::CallEnd(info) => self.call_end(info),
            HostMessage::CallConcluded(call) => self.call_concluded(call),
            HostMessage::Unit(event) => self.unit(event),
            HostMessage::Status(status) => self.status(status),
            HostMessage::Shutdown(_) => self.store.flush(),
            HostMessage::Hello(_) | HostMessage::Audio(_) | HostMessage::Unknown => {}
        }
    }

    fn call_start(&mut self, info: CallInfo) {
        let Some(system_id) = self.system(Some(info.system), &info.short_name) else { return };
        let site = site_name(&info.site, &info.short_name).to_string();
        let Some(site_id) = self.site(system_id, &site) else { return };
        let cell = Arc::new(OnceLock::new());
        let mut tell: HashSet<u32> = info.sources.iter().map(|s| s.unit).collect();
        self.live.insert((info.system, info.id), Live { id: cell.clone(), system_id, site_id, units_told: tell.clone() });
        self.commit(move |c, after| {
            let new = NewCall::from_info(&info, system_id, Some(site_id));
            let id = call_started(c, &new)?;
            // Known to later writes only once this one commits: SQLite reuses a rolled-back id.
            after.call = Some((cell, id));
            touch_talkgroup(c, system_id, info.talkgroup, &TgTags::from_info(&info), new.start_ms, info.encrypted, info.algid)?;
            let sources = sources_of(&info);
            for &(unit, time_ms, _) in &sources {
                touch_unit(c, system_id, unit, "", "", time_ms, Some(("call", Some(info.talkgroup))))?;
            }
            after.events.extend(call_event(c, id, EventKind::CallStart)?);
            let events = transmission_events(id, system_id, Some(site_id), info.talkgroup, &sources, None);
            insert_transmission_events(c, after, true, &events, &mut tell)
        });
    }

    fn call_end(&mut self, info: CallInfo) {
        let key = (info.system, info.id);
        if !self.live.contains_key(&key) {
            // Its start was lost (the plugin started mid-call): store it from here.
            self.call_start(info.clone());
        }
        let Some(live) = self.live.get_mut(&key) else { return };
        let mut tell: HashSet<u32> = info.sources.iter().map(|s| s.unit).filter(|u| !live.units_told.contains(u)).collect();
        live.units_told.extend(tell.iter().copied());
        let (cell, system_id, site_id) = (live.id.clone(), live.system_id, live.site_id);
        let end_ms = now_ms();
        // An unrecorded call has no `call.concluded` to come: it is over now.
        let ended = (!info.recording).then_some(end_ms);
        if ended.is_some() {
            self.live.remove(&key);
        }
        self.commit(move |c, after| {
            let Some(&id) = cell.get() else { return Ok(()) };
            call_ended(c, id, end_ms, &info)?;
            let sources = sources_of(&info);
            for &(unit, time_ms, _) in sources.iter().filter(|s| tell.contains(&s.0)) {
                touch_unit(c, system_id, unit, "", "", time_ms, Some(("call", Some(info.talkgroup))))?;
            }
            after.events.extend(call_event(c, id, EventKind::CallUpdate)?);
            let events = transmission_events(id, system_id, Some(site_id), info.talkgroup, &sources, ended);
            insert_transmission_events(c, after, true, &events, &mut tell)
        });
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        let live = self.live.remove(&(call.system, call.call.call_num as u32));
        let Some(system_id) = live.as_ref().map(|l| l.system_id).or_else(|| self.system(Some(call.system), &call.call.short_name)) else {
            if let Some(h) = &self.host {
                h.call_result(&call.path, Outcome::Failed, "its system could not be stored", "");
            }
            return;
        };
        let site_id = match &live {
            Some(l) => Some(l.site_id),
            None => {
                let name = site_name(&call.call.site, &call.call.short_name).to_string();
                self.site(system_id, &name)
            }
        };
        let tell = match &live {
            Some(l) => call.call.src_list.iter().map(|s| s.src as u32).filter(|u| !l.units_told.contains(u)).collect(),
            None => call.call.src_list.iter().map(|s| s.src as u32).collect(),
        };
        let conclusion = Conclusion {
            system_id,
            site_id,
            audio: Audio::Files { wav: call.files.wav, m4a: call.files.m4a, capture_dir: self.capture_dir.clone() },
            json_path: stored_path(self.capture_dir.as_deref(), &call.files.json),
            path: call.path.clone(),
            record: call.call,
            existing: live.map(|l| l.id),
            tell: Some(tell),
        };
        self.conclude(conclusion);
        // The plugin never holds the recorder's files: done as soon as it is queued.
        if let Some(h) = &self.host {
            h.call_result(&call.path, Outcome::Ok, "", "");
        }
    }

    /// Store a call record found on disk: as `call.concluded`, with no bus
    /// events, no result, and no change to the live affiliation map.
    pub fn backfill_record(&mut self, path: &str, record: &CallRecord, audio_path: Option<&str>, json_path: &str) {
        let Some(system_id) = self.system(None, &record.short_name) else { return };
        let site_id = self.site(system_id, site_name(&record.site, &record.short_name));
        self.conclude(Conclusion {
            system_id,
            site_id,
            path: path.to_string(),
            record: record.clone(),
            audio: Audio::Given(audio_path.map(String::from)),
            json_path: json_path.to_string(),
            existing: None,
            tell: None,
        });
    }

    fn conclude(&mut self, k: Conclusion) {
        self.commit(move |c, after| {
            let live = k.tell.is_some();
            let r = &k.record;
            let existing = k.existing.as_ref().and_then(|cell| cell.get().copied());
            let audio_path = k.audio.stored();
            let id = call_concluded(c, k.system_id, k.site_id, &k.path, r, audio_path.as_deref(), &k.json_path, existing)?;
            let tags = TgTags::from_record(r);
            if existing.is_some() {
                // `call.start` counted it already.
                tag_talkgroup(c, k.system_id, r.talkgroup, &tags)?;
            } else {
                let algid = u8::try_from(r.algid).ok();
                touch_talkgroup(c, k.system_id, r.talkgroup, &tags, record_start_ms(r), r.encrypted, algid)?;
            }
            for s in &r.src_list {
                touch_unit(c, k.system_id, s.src as u32, &s.tag, &s.tag_ota, s.time * 1000, Some(("call", Some(r.talkgroup))))?;
            }
            let sources: Vec<(u32, i64, bool)> = r.src_list.iter().map(|s| (s.src as u32, s.time * 1000, s.emergency)).collect();
            let events = transmission_events(id, k.system_id, k.site_id, r.talkgroup, &sources, Some(record_end_ms(r)));
            let mut tell = k.tell.unwrap_or_default();
            insert_transmission_events(c, after, live, &events, &mut tell)?;
            if live {
                after.events.extend(call_event(c, id, EventKind::CallEnd)?);
            }
            Ok(())
        });
    }

    fn unit(&mut self, event: UnitEvent) {
        let Some((event_type, metadata)) = event_type_of(&event.kind) else {
            self.dropped += 1;
            return;
        };
        let Some(system_id) = self.system(Some(event.system), &event.short_name) else { return };
        let site_id = self.site(system_id, site_name(&event.site, &event.short_name));
        let e = NewUnitEvent {
            system_id,
            site_id,
            unit: event.unit,
            event_type,
            time_ms: (event.time * 1000.0) as i64,
            tgid: event.talkgroup,
            emergency: event.emergency,
            call_id: None,
            metadata_json: metadata,
        };
        self.commit(move |c, after| {
            touch_unit(c, e.system_id, e.unit, "", "", e.time_ms, Some((e.event_type, e.tgid)))?;
            if let Some(id) = insert_unit_event(c, &e)? {
                after.affiliate(c, &e);
                let event = unit_event(c, id, &e, &after.instance)?;
                after.events.push(event);
            }
            Ok(())
        });
    }

    fn status(&mut self, status: Status) {
        let time_ms = (status.time * 1000.0) as i64;
        for s in status.systems {
            let Some(system_id) = self.system(Some(s.index), &s.short_name) else { continue };
            let Some(site_id) = self.site(system_id, site_name(&s.site_name, &s.short_name)) else { continue };
            self.commit(move |c, after| {
                let identity =
                    Identity { nac: s.nac, wacn: s.wacn, sys_id: s.sys_id, rfss: s.rfss, site_number: s.site, control_channel_hz: s.control_channel_hz };
                set_identity(c, site_id, &identity, time_ms)?;
                insert_recorder_state(c, site_id, time_ms, s.control_channel_hz, s.decode_rate, s.active_calls, s.recording)?;
                let state = RecorderStateRow {
                    site_id,
                    time_ms,
                    control_channel_hz: s.control_channel_hz,
                    decode_rate: s.decode_rate,
                    active_calls: s.active_calls,
                    recording: s.recording,
                };
                let (recorder, rate) = status_payloads(c, site_id, &state)?;
                for (kind, data) in [(EventKind::RecorderUpdate, recorder), (EventKind::RateUpdate, rate)] {
                    after.events.push(NewEvent {
                        kind,
                        sub_type: None,
                        system_id: Some(system_id.0),
                        site_id: Some(site_id.0),
                        tgid: None,
                        unit_id: None,
                        emergency: false,
                        data,
                    });
                }
                Ok(())
            });
        }
    }
}

/// A call's sources as (unit, time ms, emergency).
fn sources_of(info: &CallInfo) -> Vec<(u32, i64, bool)> {
    info.sources.iter().map(|s| (s.unit, (s.time * 1000.0) as i64, s.emergency)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::affiliations::AffiliationFilter;
    use crate::bus::{Event, EventKind, Filter};
    use crate::config::Config;
    use crate::store::systems::system_by_short_name;
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    use trunk_recorder_plugin::{CallSource, Outcome, SrcEntry, Status, SystemStatus, UnitEvent, testing};

    const T0: i64 = 1_700_000_101;

    fn systems() -> Vec<System<Config, NoConfig>> {
        vec![System { index: 0, short_name: "sys1".into(), kind: "p25".into(), config: None, settings: Config::default() }]
    }

    struct Rig {
        ingest: Ingest,
        store: Store,
        affiliations: Affiliations,
        sub: crate::bus::Subscription,
        captured: testing::Captured,
        dir: std::path::PathBuf,
    }

    fn rig() -> Rig {
        let store = Store::in_memory();
        let bus = Bus::new(100);
        let affiliations = Affiliations::new();
        let sub = bus.subscribe(Filter::default());
        let (host, captured) = testing::capture();
        let dir = testing::temp_dir("ingest");
        let ingest =
            Ingest::new(store.clone(), bus, affiliations.clone(), Some(host), &systems()).unwrap().with_capture_dir(dir.clone()).with_host_name("rig-host");
        Rig { ingest, store, affiliations, sub, captured, dir }
    }

    impl Rig {
        fn events(&mut self) -> Vec<Event> {
            self.store.flush();
            let mut out = Vec::new();
            while let Ok(e) = self.sub.rx.try_recv() {
                out.push(e);
            }
            out
        }

        fn count(&self, sql: &str) -> i64 {
            self.store.read(|c| c.query_row(sql, [], |r| r.get(0))).unwrap()
        }

        fn unit_events(&self) -> Vec<(String, u32)> {
            self.store
                .read(|c| {
                    c.prepare("SELECT event_type, unit FROM unit_events ORDER BY event_type, unit")?.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect()
                })
                .unwrap()
        }
    }

    fn info(id: u32, sources: &[(u32, i64)], recording: bool) -> CallInfo {
        CallInfo {
            id,
            system: 0,
            short_name: "sys1".into(),
            talkgroup: 101,
            talkgroup_tag: "TG 101".into(),
            freq_hz: 851_012_500,
            recording,
            reason: (!recording).then(|| "encrypted".to_string()),
            encrypted: !recording,
            start_time: T0 as f64,
            units: sources.iter().map(|s| s.0).collect(),
            sources: sources.iter().map(|&(unit, t)| CallSource { unit, time: t as f64, ..Default::default() }).collect(),
            ..Default::default()
        }
    }

    fn kinds(events: &[Event]) -> Vec<(&'static str, Option<String>)> {
        events.iter().map(|e| (e.kind.as_str(), e.sub_type.clone())).collect()
    }

    #[test]
    fn a_recorded_call_is_one_row_with_its_units_and_events() {
        let mut r = rig();
        r.ingest.handle(HostMessage::CallStart(info(7, &[(10, T0)], true)));
        r.ingest.handle(HostMessage::CallEnd(info(7, &[(10, T0), (20, T0 + 2)], true)));
        let mut cc = testing::call(&r.dir, "sys1", 101);
        cc.call.call_num = 7;
        cc.call.stop_time = T0 + 5;
        cc.call.call_length = 5.0;
        cc.call.src_list =
            vec![SrcEntry { src: 10, time: T0, pos: 0.0, ..Default::default() }, SrcEntry { src: 20, time: T0 + 2, pos: 2.0, ..Default::default() }];
        let path = cc.path.clone();
        r.ingest.handle(HostMessage::CallConcluded(cc));
        let events = r.events();

        assert_eq!(r.count("SELECT count(*) FROM calls"), 1);
        let row = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!((row.state.as_str(), row.recorded, row.call_num), ("concluded", true, Some(7)));
        assert_eq!(row.audio_path.as_deref(), Some(format!("{path}.wav").as_str()));
        assert_eq!(row.json_path.as_deref(), Some(format!("{path}.json").as_str()));
        assert_eq!(r.count("SELECT count(*) FROM call_transmissions"), 2);
        assert_eq!(r.unit_events(), vec![("call".into(), 10), ("call".into(), 20), ("end".into(), 10), ("end".into(), 20)]);
        let end20: i64 = r.store.read(|c| c.query_row("SELECT time_ms FROM unit_events WHERE event_type = 'end' AND unit = 20", [], |x| x.get(0))).unwrap();
        assert_eq!(end20, (T0 + 5) * 1000);
        // The talkgroup counts the call once, though both its start and its files were seen.
        assert_eq!(r.count("SELECT call_count FROM talkgroups WHERE tgid = 101"), 1);

        assert_eq!(
            kinds(&events),
            vec![("call_start", None), ("unit_event", Some("call".into())), ("call_update", None), ("unit_event", Some("call".into())), ("call_end", None),]
        );
        assert_eq!(events[1].unit_id, Some(10));
        assert_eq!(events[3].unit_id, Some(20));
        let data: Value = serde_json::from_str(&events[4].data).unwrap();
        assert_eq!((data["call_id"].as_i64(), data["call_state"].as_str(), data["tgid"].as_u64()), (Some(1), Some("completed"), Some(101)));
        // The call events carry the API's `Call`.
        let start: Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!(
            (start["call_id"].as_i64(), start["tgid"].as_u64(), start["call_state"].as_str(), start["recorded"].as_bool()),
            (Some(1), Some(101), Some("recording"), Some(true))
        );
        assert!(time::OffsetDateTime::parse(start["start_time"].as_str().unwrap(), &time::format_description::well_known::Rfc3339).is_ok());
        assert_eq!(data["audio_url"].as_str(), Some("/api/v1/calls/1/audio"));
        assert_eq!(data["units"].as_array().map(Vec::len), Some(2));
        assert_eq!(r.captured.output().results(), vec![(path, Outcome::Ok, String::new(), String::new())]);
        assert!(r.ingest.live.is_empty());
    }

    #[test]
    fn an_unrecorded_call_has_end_events_and_no_result() {
        let mut r = rig();
        r.ingest.handle(HostMessage::CallStart(info(8, &[(10, T0)], false)));
        r.ingest.handle(HostMessage::CallEnd(info(8, &[(10, T0)], false)));
        let events = r.events();
        let row = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!((row.recorded, row.not_recorded_reason.as_deref(), row.state.as_str()), (false, Some("encrypted"), "ended"));
        assert_eq!(r.unit_events(), vec![("call".into(), 10), ("end".into(), 10)]);
        assert_eq!(kinds(&events), vec![("call_start", None), ("unit_event", Some("call".into())), ("call_update", None)]);
        assert!(r.captured.output().results().is_empty());
        assert!(r.ingest.live.is_empty());
    }

    #[test]
    fn a_concluded_call_whose_start_was_lost_is_stored() {
        let mut r = rig();
        let cc = testing::call(&r.dir, "sys1", 101);
        let path = cc.path.clone();
        r.ingest.handle(HostMessage::CallConcluded(cc));
        let events = r.events();
        assert_eq!(r.count("SELECT count(*) FROM calls"), 1);
        let row = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!((row.path.as_deref(), row.tgid, row.start_ms, row.state.as_str()), (Some(path.as_str()), 101, T0 * 1000, "concluded"));
        assert_eq!(r.unit_events(), vec![("call".into(), 1234), ("end".into(), 1234)]);
        assert_eq!(r.count("SELECT call_count FROM talkgroups WHERE tgid = 101"), 1);
        assert_eq!(kinds(&events), vec![("unit_event", Some("call".into())), ("call_end", None)]);
        assert_eq!(r.captured.output().results(), vec![(path, Outcome::Ok, String::new(), String::new())]);
    }

    #[test]
    fn a_backfilled_record_is_stored_without_bus_events_or_results() {
        let mut r = rig();
        // Unit 1234 is on 202 now; the backfilled call has it on 101 earlier.
        r.ingest.handle(HostMessage::Unit(UnitEvent {
            system: 0,
            short_name: "sys1".into(),
            kind: "affiliation".into(),
            unit: 1234,
            talkgroup: Some(202),
            time: (T0 + 3600) as f64,
            ..Default::default()
        }));
        r.events();
        let snapshot = |r: &Rig| r.affiliations.snapshot(&AffiliationFilter { limit: 100, ..Default::default() }, (T0 + 3600) * 1000);
        let before = snapshot(&r);
        assert_eq!(before.0.iter().map(|e| (e.unit_id, e.tgid)).collect::<Vec<_>>(), vec![(1234, 202)]);

        let cc = testing::call(&r.dir, "sys1", 101);
        r.ingest.backfill_record(&cc.path, &cc.call, Some("a.wav"), "a.json");
        let events = r.events();
        let row = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!((row.audio_path.as_deref(), row.json_path.as_deref()), (Some("a.wav"), Some("a.json")));
        assert_eq!(r.count("SELECT count(*) FROM unit_events WHERE call_id IS NOT NULL"), 2);
        assert!(events.is_empty());
        assert!(r.captured.output().results().is_empty());
        assert_eq!(snapshot(&r), before);
    }

    #[test]
    fn a_failed_call_start_leaves_no_id_and_publishes_nothing() {
        let mut r = rig();
        let fail = "CREATE TRIGGER fail BEFORE INSERT ON talkgroups BEGIN SELECT RAISE(ABORT, 'boom'); END";
        r.store.write_sync(move |c| c.execute_batch(fail)).unwrap();
        r.ingest.handle(HostMessage::CallStart(info(7, &[(10, T0)], true)));
        let events = r.events();
        assert!(events.is_empty());
        assert_eq!(r.count("SELECT count(*) FROM calls"), 0);
        assert!(r.ingest.live[&(0, 7)].id.get().is_none());
        assert!(r.affiliations.snapshot(&AffiliationFilter { limit: 100, ..Default::default() }, T0 * 1000).0.is_empty());

        // SQLite hands the rolled-back id to the next call; call 7's later messages must not touch it.
        r.store.write_sync(|c| c.execute_batch("DROP TRIGGER fail")).unwrap();
        r.ingest.handle(HostMessage::CallStart(info(8, &[(30, T0)], true)));
        r.ingest.handle(HostMessage::CallEnd(info(7, &[(10, T0), (20, T0 + 2)], true)));
        let mut cc = testing::call(&r.dir, "sys1", 101);
        cc.call.call_num = 7;
        r.ingest.handle(HostMessage::CallConcluded(cc));
        r.events();
        let other = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!((other.call_num, other.state.as_str(), other.path.as_deref()), (Some(8), "recording", None));
        assert_eq!(r.count("SELECT count(*) FROM call_transmissions WHERE call_id = 1"), 0);
    }

    #[test]
    fn a_concluded_call_without_a_wav_file_stores_its_m4a() {
        let mut r = rig();
        let cc = testing::call(&r.dir, "sys1", 101);
        std::fs::remove_file(&cc.files.wav).unwrap();
        let path = cc.path.clone();
        r.ingest.handle(HostMessage::CallConcluded(cc));
        r.events();
        let row = r.store.read(|c| call_by_id(c, CallId(1))).unwrap().unwrap();
        assert_eq!(row.audio_path.as_deref(), Some(format!("{path}.m4a").as_str()));
    }

    #[test]
    fn unit_events_map_and_feed_affiliations() {
        let mut r = rig();
        r.ingest.handle(HostMessage::Unit(UnitEvent {
            system: 0,
            short_name: "sys1".into(),
            site: "east".into(),
            kind: "affiliation".into(),
            unit: 10,
            talkgroup: Some(101),
            time: T0 as f64,
            ..Default::default()
        }));
        r.ingest.handle(HostMessage::Unit(UnitEvent { system: 0, short_name: "sys1".into(), kind: "weird".into(), unit: 11, ..Default::default() }));
        let events = r.events();
        assert_eq!(r.unit_events(), vec![("join".into(), 10)]);
        assert_eq!(r.count("SELECT count(*) FROM sites WHERE name = 'east'"), 1);
        let (entries, _, _) = r.affiliations.snapshot(&AffiliationFilter { limit: 100, ..Default::default() }, T0 * 1000);
        assert_eq!(entries.iter().map(|e| (e.unit_id, e.tgid)).collect::<Vec<_>>(), vec![(10, 101)]);
        assert_eq!(kinds(&events), vec![("unit_event", Some("join".into()))]);
        assert_eq!((events[0].unit_id, events[0].tgid), (Some(10), Some(101)));
        let data: Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!((data["event_type"].as_str(), data["unit_id"].as_u64(), data["unit_rid"].as_u64()), (Some("join"), Some(10), Some(10)));
        assert_eq!(data["time"].as_str().map(|t| OffsetDateTime::parse(t, &Rfc3339).unwrap().unix_timestamp()), Some(T0));
        assert_eq!((data["tgid"].as_u64(), data["instance_id"].as_str()), (Some(101), Some("rig-host")));
        assert_eq!(r.ingest.dropped(), 1);
    }

    #[test]
    fn status_fills_identity_and_recorder_state() {
        let mut r = rig();
        r.ingest.handle(HostMessage::Status(Status {
            time: T0 as f64,
            systems: vec![SystemStatus {
                index: 0,
                short_name: "sys1".into(),
                site_name: "sys1".into(),
                control_channel_hz: Some(851_000_000),
                decode_rate: 40.0,
                active_calls: 2,
                recording: 1,
                sys_id: Some(0x3AB),
                wacn: Some(0xBEE00),
                ..Default::default()
            }],
        }));
        let events = r.events();
        let sys = r.store.read(|c| system_by_short_name(c, "sys1")).unwrap().unwrap();
        assert_eq!((sys.sysid, sys.wacn), (Some(0x3AB), Some(0xBEE00)));
        assert_eq!(r.count("SELECT count(*) FROM recorder_state"), 1);
        assert_eq!(r.count("SELECT control_channel_hz FROM sites WHERE name = 'sys1'"), 851_000_000);
        assert_eq!(kinds(&events), vec![("recorder_update", None), ("rate_update", None)]);
        assert!(events.iter().all(|e| e.kind != EventKind::CallStart));

        let rec: Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!((rec["src_num"].as_i64(), rec["rec_num"].as_i64(), rec["rec_state"].as_str()), (Some(1), Some(1), Some("recording")));
        assert_eq!((rec["id"].as_str(), rec["type"].as_str(), rec["freq"].as_u64()), (Some("1_1"), Some("P25"), Some(851_000_000)));
        let rate: Value = serde_json::from_str(&events[1].data).unwrap();
        assert_eq!((rate["decode_rate"].as_f64(), rate["system_id"].as_i64(), rate["sysid"].as_str()), (Some(40.0), Some(1), Some("3AB")));
        assert_eq!(rate["time"].as_str().map(|t| OffsetDateTime::parse(t, &Rfc3339).unwrap().unix_timestamp()), Some(T0));
    }
}
