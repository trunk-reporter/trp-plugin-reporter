//! Shared by the API tests: an app on an in-memory store, request helpers,
//! a seeded history, and a check of a response against the spec's schemas.
// Each test binary uses only some of these.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tower::ServiceExt;
use trunk_recorder_plugin::{CallInfo, CallRecord, FreqEntry, SrcEntry};

use reporter::affiliations::Affiliations;
use reporter::api::{AppState, HostStatus, router};
use reporter::bus::Bus;
use reporter::store::calls::{NewCall, call_concluded, call_ended, call_started};
use reporter::store::recorder::insert_recorder_state;
use reporter::store::systems::{Identity, ensure_site, ensure_system, set_identity};
use reporter::store::talkgroups::{TgTags, touch_talkgroup};
use reporter::store::units::{NewUnitEvent, insert_unit_event, touch_unit, transmission_events};
use reporter::store::{CallId, SiteId, Store, SystemId, now_ms};

/// The host name the test app's health reports.
pub const HOST: &str = "trunk-recorder";

fn temp_capture_dir() -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!("reporter-test-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir_all(&dir).expect("temp capture dir");
    dir
}

/// The app with no token, on an empty in-memory store.
pub fn app() -> (Router, Store, Bus, Affiliations) {
    app_with(None)
}

/// The app guarded by `token` (when `Some`).
pub fn app_with(token: Option<&str>) -> (Router, Store, Bus, Affiliations) {
    let (state, store, bus, affiliations) = state_with(token);
    (router(state), store, bus, affiliations)
}

/// The state the app is built from, for tests that need its capture folder or host status.
/// The host is connected and was last seen now.
pub fn state_with(token: Option<&str>) -> (AppState, Store, Bus, Affiliations) {
    let store = Store::in_memory();
    let bus = Bus::new(100);
    let affiliations = Affiliations::new();
    let host = HostStatus::new(HOST, true);
    host.touch(now_ms());
    let state = AppState::new(store.clone(), bus.clone(), affiliations.clone(), temp_capture_dir(), token.map(String::from), host);
    (state, store, bus, affiliations)
}

/// Send any request.
pub async fn send(app: &Router, req: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let res = app.clone().oneshot(req).await.expect("router is infallible");
    let status = res.status();
    let headers = res.headers().clone();
    let body = res.into_body().collect().await.expect("body").to_bytes();
    (status, headers, body)
}

/// GET with extra headers.
pub async fn get_with(app: &Router, path: &str, headers: &[(&str, &str)]) -> (StatusCode, HeaderMap, Bytes) {
    let mut req = Request::builder().method(Method::GET).uri(path);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    send(app, req.body(Body::empty()).expect("request")).await
}

/// GET; the body as JSON (`Null` when it isn't JSON).
pub async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let (s, _, body) = get_with(app, path, &[]).await;
    (s, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// Any method, no body; the body as JSON (`Null` when it isn't JSON).
pub async fn request(app: &Router, method: Method, path: &str) -> (StatusCode, Value) {
    let (s, _, body) = send(app, Request::builder().method(method).uri(path).body(Body::empty()).expect("request")).await;
    (s, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

// ---------------------------------------------------------------------------
// Seed
// ---------------------------------------------------------------------------

/// What [`seed`] created. All times are relative to `now` (ms), taken at seeding.
///
/// Systems and sites:
/// - `sys1` "ohmarcs", p25, name "" (served as its short name): sysid 0x3AB, wacn 0xBEE00, nac 0x3A0.
///   - `site1` "ohmarcs-dayton": nac 0x3A0, rfss 1, site 5, control channel 851 000 000 Hz.
/// - `sys2` "fire-conv", conventional, no identity.
///   - `site2` "fire-conv" (named like its system).
///
/// Talkgroups (tags from the calls' records):
/// - sys1 101 "Fire Disp" (description "Fire Dispatch", group "Fire", tag "Dispatch"): calls c1, c2, c5 → call_count 3.
/// - sys1 202 "PD Tac" (description "Police Tactical", group "Police", tag "Law Tac"): calls c3, c4, c6 → call_count 3,
///   encrypted_count 1 (c4), last_algid 0x84.
/// - 9178 on **both** systems (sys1 "Interop" heard now-2d, sys2 "Mutual Aid" heard now-1d): call_count 1 each,
///   with no call rows (touched directly) — for the plain-id ambiguity tests.
///
/// Units (alpha tags from the calls' srcList):
/// - 1234 on **both** systems: sys1 "Engine 12" (c2, c3; a `signal` event), sys2 (no tag) a `call` event on 9178 at now-1d.
/// - 10 "Medic 10" (sys1): c1, c3, c6; last event `join` 101 at now-5m.
/// - 20 "Unit 20" (sys1): c1, c5; last event `off` at now-8m (deregistered).
/// - 30 (sys1, no tag): only an `on` event at now-2h (registered, last seen 2 h ago).
///
/// Calls (all sys1 / site1, `calls[i]` = c(i+1); freq 851 012 500 Hz):
/// - c1: tg 101, start now-10m, 12 s, recorded+concluded, srcs 10 @0 s, 20 @6 s.
/// - c2: tg 101, start now-30m, 8 s, recorded+concluded, **emergency**, src 1234 @0 s (emergency).
/// - c3: tg 202, start now-3h, 20 s, recorded+concluded, srcs 1234 @0 s, 10 @10 s.
/// - c4: tg 202, start now-5h, 5 s, **not recorded** (reason "encrypted", encrypted, algid 0x84), state ended, no units.
/// - c5: tg 101, start now-3d, 15 s, recorded+concluded, src 20 @0 s.
/// - c6: tg 202, start now-40d, 30 s, recorded+concluded, src 10 @0 s.
///
/// So: calls_1h 2 (c1, c2), calls_24h 4 (c1–c4), total 6; unit 1234 is on 2 calls, unit 10 on 3, unit 20 on 2.
/// Recorded calls have `path` `ohmarcs/<tg>-<start s>_851012500`, `audio_path` that + `.wav` and `json_path`
/// that + `.json` (relative to the capture folder; the files are not created).
///
/// Unit events: each recorded call's `call`/`end` per source (as ingest writes them), plus
/// - sys1: 30 `on` at now-2h; 1234 `signal` (emergency, tg 101, metadata
///   `{"signaling_type":"P25","signal_type":"emergency"}`) at now-31m; 20 `off` at now-8m; 10 `join` 101 at now-5m;
/// - sys2: 1234 `call` on 9178 at now-1d (no call row).
///
/// Recorder state (site1, cc 851 000 000 Hz): now-2m decode 0.98 active 1 recording 1; now-1m 0.95/0/0; now 0.97/2/1.
#[derive(Clone, Debug)]
pub struct Seed {
    pub now: i64,
    pub sys1: SystemId,
    pub sys2: SystemId,
    pub site1: SiteId,
    pub site2: SiteId,
    /// c1..c6 as documented on [`Seed`].
    pub calls: Vec<CallId>,
}

pub const MIN: i64 = 60_000;
pub const HOUR: i64 = 60 * MIN;
pub const DAY: i64 = 24 * HOUR;

struct Tg {
    tgid: u32,
    alpha: &'static str,
    description: &'static str,
    group: &'static str,
    tag: &'static str,
}

const TG101: Tg = Tg { tgid: 101, alpha: "Fire Disp", description: "Fire Dispatch", group: "Fire", tag: "Dispatch" };
const TG202: Tg = Tg { tgid: 202, alpha: "PD Tac", description: "Police Tactical", group: "Police", tag: "Law Tac" };

/// (unit, tag, seconds into the call)
type Src = (u32, &'static str, i64);

fn record(tg: &Tg, start_ms: i64, len_s: i64, emergency: bool, srcs: &[Src]) -> CallRecord {
    CallRecord {
        call_num: (start_ms / 1000) as u64,
        freq: 851_012_500,
        start_time: start_ms / 1000,
        stop_time: (start_ms + len_s * 1000) / 1000,
        start_time_ms: start_ms,
        stop_time_ms: start_ms + len_s * 1000,
        emergency,
        call_length: len_s as f64,
        call_length_ms: len_s * 1000,
        talkgroup: tg.tgid,
        talkgroup_tag: tg.alpha.into(),
        talkgroup_description: tg.description.into(),
        talkgroup_group: tg.group.into(),
        talkgroup_group_tag: tg.tag.into(),
        short_name: "ohmarcs".into(),
        site: "ohmarcs-dayton".into(),
        freq_list: vec![FreqEntry { freq: 851_012_500, time: start_ms / 1000, pos: 0.0, len: len_s as f64, ..Default::default() }],
        src_list: srcs
            .iter()
            .map(|&(src, tag, at)| SrcEntry { src: src as i64, time: start_ms / 1000 + at, pos: at as f64, emergency, tag: tag.into(), ..Default::default() })
            .collect(),
        ..Default::default()
    }
}

/// Write a recorded call the way ingest's `call.concluded` (with no seen start) does.
fn conclude(c: &rusqlite::Connection, sys: SystemId, site: SiteId, r: &CallRecord) -> rusqlite::Result<CallId> {
    let path = format!("ohmarcs/{}-{}_{}", r.talkgroup, r.start_time, r.freq);
    let id = call_concluded(c, sys, Some(site), &path, r, Some(&format!("{path}.wav")), &format!("{path}.json"), None)?;
    touch_talkgroup(c, sys, r.talkgroup, &TgTags::from_record(r), r.start_time_ms, r.encrypted, None)?;
    for s in &r.src_list {
        touch_unit(c, sys, s.src as u32, &s.tag, "", s.time * 1000, Some(("call", Some(r.talkgroup))))?;
    }
    let sources: Vec<(u32, i64, bool)> = r.src_list.iter().map(|s| (s.src as u32, s.time * 1000, s.emergency)).collect();
    for e in transmission_events(id, sys, Some(site), r.talkgroup, &sources, Some(r.stop_time_ms)) {
        insert_unit_event(c, &e)?;
    }
    Ok(id)
}

/// A control-channel event, written the way ingest writes one.
fn event(c: &rusqlite::Connection, e: NewUnitEvent) -> rusqlite::Result<()> {
    touch_unit(c, e.system_id, e.unit, "", "", e.time_ms, Some((e.event_type, e.tgid)))?;
    insert_unit_event(c, &e).map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn ev(
    system_id: SystemId,
    site_id: SiteId,
    unit: u32,
    event_type: &'static str,
    time_ms: i64,
    tgid: Option<u32>,
    emergency: bool,
    metadata_json: Option<Value>,
) -> NewUnitEvent {
    NewUnitEvent { system_id, site_id: Some(site_id), unit, event_type, time_ms, tgid, emergency, call_id: None, metadata_json }
}

/// Seed the history documented on [`Seed`]. Writes run oldest first, so each
/// unit's `last_event_*` is its newest event.
pub fn seed(store: &Store) -> Seed {
    let now = now_ms();
    store
        .write_sync(move |c| {
            let sys1 = ensure_system(c, "ohmarcs", "p25", "", now - 40 * DAY)?;
            let site1 = ensure_site(c, sys1, "ohmarcs-dayton", now - 40 * DAY)?;
            let identity = Identity {
                nac: Some(0x3A0),
                wacn: Some(0xBEE00),
                sys_id: Some(0x3AB),
                rfss: Some(1),
                site_number: Some(5),
                control_channel_hz: Some(851_000_000),
            };
            set_identity(c, site1, &identity, now)?;
            let sys2 = ensure_system(c, "fire-conv", "conventional", "", now - 2 * DAY)?;
            let site2 = ensure_site(c, sys2, "fire-conv", now - 2 * DAY)?;

            let c6 = conclude(c, sys1, site1, &record(&TG202, now - 40 * DAY, 30, false, &[(10, "Medic 10", 0)]))?;
            let c5 = conclude(c, sys1, site1, &record(&TG101, now - 3 * DAY, 15, false, &[(20, "Unit 20", 0)]))?;
            touch_talkgroup(c, sys1, 9178, &TgTags { alpha_tag: "Interop".into(), ..Default::default() }, now - 2 * DAY, false, None)?;
            touch_talkgroup(c, sys2, 9178, &TgTags { alpha_tag: "Mutual Aid".into(), ..Default::default() }, now - DAY, false, None)?;
            event(c, ev(sys2, site2, 1234, "call", now - DAY, Some(9178), false, None))?;

            // c4: followed but not recorded (encrypted), then ended.
            let c4_start = now - 5 * HOUR;
            let c4 = call_started(
                c,
                &NewCall {
                    system_id: sys1,
                    site_id: Some(site1),
                    call_num: Some(4),
                    tgid: 202,
                    start_ms: c4_start,
                    freq_hz: 851_012_500,
                    tdma_slot: None,
                    analog: false,
                    encrypted: true,
                    algid: Some(0x84),
                    key_id: Some(1),
                    decrypted: false,
                    emergency: false,
                    duplex: false,
                    packet: false,
                    priority: 4,
                    recorded: false,
                    not_recorded_reason: Some("encrypted".into()),
                    tg_alpha_tag: TG202.alpha.into(),
                    patched_tgids: vec![],
                },
            )?;
            touch_talkgroup(c, sys1, 202, &TgTags { alpha_tag: TG202.alpha.into(), ..Default::default() }, c4_start, true, Some(0x84))?;
            let info = CallInfo {
                id: 4,
                talkgroup: 202,
                encrypted: true,
                algid: Some(0x84),
                key_id: Some(1),
                recording: false,
                reason: Some("encrypted".into()),
                ..Default::default()
            };
            call_ended(c, c4, c4_start + 5_000, &info)?;

            let c3 = conclude(c, sys1, site1, &record(&TG202, now - 3 * HOUR, 20, false, &[(1234, "Engine 12", 0), (10, "Medic 10", 10)]))?;
            event(c, ev(sys1, site1, 30, "on", now - 2 * HOUR, None, false, None))?;
            let signal = json!({"signaling_type": "P25", "signal_type": "emergency"});
            event(c, ev(sys1, site1, 1234, "signal", now - 31 * MIN, Some(101), true, Some(signal)))?;
            let c2 = conclude(c, sys1, site1, &record(&TG101, now - 30 * MIN, 8, true, &[(1234, "Engine 12", 0)]))?;
            let c1 = conclude(c, sys1, site1, &record(&TG101, now - 10 * MIN, 12, false, &[(10, "Medic 10", 0), (20, "Unit 20", 6)]))?;
            event(c, ev(sys1, site1, 20, "off", now - 8 * MIN, None, false, None))?;
            event(c, ev(sys1, site1, 10, "join", now - 5 * MIN, Some(101), false, None))?;

            insert_recorder_state(c, site1, now - 2 * MIN, Some(851_000_000), 0.98, 1, 1)?;
            insert_recorder_state(c, site1, now - MIN, Some(851_000_000), 0.95, 0, 0)?;
            insert_recorder_state(c, site1, now, Some(851_000_000), 0.97, 2, 1)?;

            Ok(Seed { now, sys1, sys2, site1, site2, calls: vec![c1, c2, c3, c4, c5, c6] })
        })
        .expect("seed")
}

// ---------------------------------------------------------------------------
// Schema checks
// ---------------------------------------------------------------------------

type Yaml = serde_yaml::Value;

/// The spec, parsed once.
pub fn spec() -> &'static Yaml {
    static SPEC: OnceLock<Yaml> = OnceLock::new();
    SPEC.get_or_init(|| serde_yaml::from_str(include_str!("../../openapi/openapi.yaml")).expect("openapi.yaml parses"))
}

fn named(name: &str) -> &'static Yaml {
    let s = &spec()["components"]["schemas"][name];
    assert!(!s.is_null(), "no schema {name} in openapi.yaml");
    s
}

/// `$ref: "#/components/schemas/X"` → X (one level); anything else as is.
fn deref(node: &'static Yaml) -> &'static Yaml {
    match node.get("$ref").and_then(Yaml::as_str) {
        Some(r) => named(r.strip_prefix("#/components/schemas/").unwrap_or_else(|| panic!("unsupported $ref {r}"))),
        None => node,
    }
}

/// A schema with `$ref` resolved and `allOf` merged (one level each).
#[derive(Default)]
struct Flat {
    ty: Option<String>,
    nullable: bool,
    format: Option<String>,
    values: Option<Vec<Value>>,
    required: Vec<String>,
    properties: Vec<(String, &'static Yaml)>,
    items: Option<&'static Yaml>,
}

fn absorb(flat: &mut Flat, node: &'static Yaml) {
    let node = deref(node);
    if let Some(t) = node.get("type").and_then(Yaml::as_str) {
        flat.ty.get_or_insert_with(|| t.to_string());
    }
    flat.nullable |= node.get("nullable").and_then(Yaml::as_bool).unwrap_or(false);
    if let Some(f) = node.get("format").and_then(Yaml::as_str) {
        flat.format.get_or_insert_with(|| f.to_string());
    }
    if let Some(e) = node.get("enum").and_then(Yaml::as_sequence) {
        flat.values.get_or_insert_with(|| e.iter().map(|v| serde_json::to_value(v).expect("enum value")).collect());
    }
    if let Some(r) = node.get("required").and_then(Yaml::as_sequence) {
        flat.required.extend(r.iter().filter_map(Yaml::as_str).map(String::from));
    }
    if let Some(p) = node.get("properties").and_then(Yaml::as_mapping) {
        for (k, v) in p {
            let k = k.as_str().expect("property name").to_string();
            if !flat.properties.iter().any(|(n, _)| *n == k) {
                flat.properties.push((k, v));
            }
        }
    }
    if let Some(i) = node.get("items") {
        flat.items.get_or_insert(i);
    }
}

fn flatten(node: &'static Yaml) -> Flat {
    // `nullable` beside a `$ref` / `allOf` belongs to the property itself.
    let mut flat = Flat { nullable: node.get("nullable").and_then(Yaml::as_bool).unwrap_or(false), ..Default::default() };
    absorb(&mut flat, node);
    if let Some(all) = deref(node).get("allOf").and_then(Yaml::as_sequence) {
        for part in all {
            absorb(&mut flat, part);
        }
    }
    if flat.ty.is_none() && !flat.properties.is_empty() {
        flat.ty = Some("object".into());
    }
    flat
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// One value against a (flattened) schema: null, type, format, enum.
fn check_value(v: &Value, s: &Flat, at: &str) {
    if v.is_null() {
        assert!(s.nullable, "{at}: is null but the schema is not nullable");
        return;
    }
    if let Some(ty) = &s.ty {
        let got = json_type(v);
        let ok = match ty.as_str() {
            "number" => got == "number" || got == "integer",
            t => got == t,
        };
        assert!(ok, "{at}: expected {ty}, got {got} ({v})");
    }
    if s.format.as_deref() == Some("date-time") {
        let text = v.as_str().unwrap_or_default();
        assert!(OffsetDateTime::parse(text, &Rfc3339).is_ok(), "{at}: {v} is not an RFC 3339 date-time");
    }
    if let Some(values) = &s.values {
        assert!(values.contains(v), "{at}: {v} is not one of {values:?}");
    }
}

/// Assert `v` matches `components.schemas.<schema>`: the value's own type, every
/// `required` property present, and each present property's type, nullability,
/// date-time format and enum (array properties: each element too). Nested
/// objects are not descended into; check them with their own schema.
pub fn assert_schema(v: &Value, schema: &str) {
    let root = flatten(named(schema));
    check_value(v, &root, schema);
    let Some(obj) = v.as_object() else { return };
    for name in &root.required {
        assert!(obj.contains_key(name), "{schema}: required property {name} is missing in {v}");
    }
    for (name, prop) in &root.properties {
        let Some(pv) = obj.get(name) else { continue };
        let at = format!("{schema}.{name}");
        let p = flatten(prop);
        check_value(pv, &p, &at);
        if let (Some(items), Some(elements)) = (p.items, pv.as_array()) {
            let item = flatten(items);
            for (i, e) in elements.iter().enumerate() {
                check_value(e, &item, &format!("{at}[{i}]"));
            }
        }
    }
}
