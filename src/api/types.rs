//! The API's wire types and their formatting: errors, times, hex identities.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::macros::format_description;

use crate::affiliations::AffiliationEntry;
use crate::store::calls::{CallRow, FrequencyRow, TransmissionRow, frequencies, transmissions, units_of};
use crate::store::recorder::RecorderStateRow;
use crate::store::stats::ActivityRow;
use crate::store::systems::{SiteRow, SystemRow};
use crate::store::systems::{site_by_id, system_by_id};
use crate::store::talkgroups::{EncryptionStatRow, TalkgroupRow};
use crate::store::units::unit;
use crate::store::units::{UnitEventRow, UnitRow};
use crate::store::{StoreError, now_ms};
use rusqlite::Connection;

/// The spec's `Error`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Error {
    pub code: &'static str,
    pub error: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

/// One system a plain id matched, in an [`AmbiguousError`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AmbiguousMatch {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
}

/// The spec's `AmbiguousError` (409).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AmbiguousError {
    pub code: &'static str,
    pub error: String,
    pub matches: Vec<AmbiguousMatch>,
}

/// What a handler fails with; its response is the spec's `Error` (or `AmbiguousError`).
#[derive(Clone, Debug, PartialEq)]
pub enum ApiError {
    BadRequest(String),
    InvalidParameter(String),
    InvalidTimeRange(String),
    NotFound(String),
    /// A plain id found in several systems: `what` ("talkgroup", "unit"), the id,
    /// and each match's (system_id, system name, sysid).
    Ambiguous {
        what: &'static str,
        id: u32,
        matches: Vec<(i64, String, String)>,
    },
    Unauthorized,
    /// The request path: tr-engine serves it, this plugin does not.
    NotImplemented(String),
    Internal(String),
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        ApiError::Internal(e.0)
    }
}

impl ApiError {
    fn error(status: StatusCode, code: &'static str, error: impl Into<String>, detail: impl Into<String>) -> Response {
        (status, Json(Error { code, error: error.into(), detail: detail.into() })).into_response()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(m) => ApiError::error(StatusCode::BAD_REQUEST, "bad_request", m, ""),
            ApiError::InvalidParameter(m) => ApiError::error(StatusCode::BAD_REQUEST, "invalid_parameter", m, ""),
            ApiError::InvalidTimeRange(m) => ApiError::error(StatusCode::BAD_REQUEST, "invalid_time_range", m, ""),
            ApiError::NotFound(m) => ApiError::error(StatusCode::NOT_FOUND, "not_found", m, ""),
            ApiError::Ambiguous { what, id, matches } => {
                let body = AmbiguousError {
                    code: "ambiguous_id",
                    error: format!("Ambiguous {what} ID {id}: exists in multiple systems"),
                    matches: matches.into_iter().map(|(system_id, system_name, sysid)| AmbiguousMatch { system_id, system_name, sysid }).collect(),
                };
                (StatusCode::CONFLICT, Json(body)).into_response()
            }
            ApiError::Unauthorized => ApiError::error(StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid token", ""),
            ApiError::NotImplemented(path) => {
                ApiError::error(StatusCode::NOT_IMPLEMENTED, "not_implemented", "not available in the Trunk Recorder Pro plugin", path)
            }
            // The store's message (SQL text) stays out of the response.
            ApiError::Internal(_) => ApiError::error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "internal error", ""),
        }
    }
}

/// Unix ms → `2024-01-15T10:30:00.000Z`.
pub fn rfc3339(ms: i64) -> String {
    let fmt = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");
    OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000).ok().and_then(|t| t.format(&fmt).ok()).unwrap_or_default()
}

/// A P25 identity (sysid, wacn, nac) as the spec writes it: upper-case hex, `"0"` when unknown.
pub fn hex(v: Option<u32>) -> String {
    format!("{:X}", v.unwrap_or(0))
}

/// The spec's `SystemType` for a stored `kind`.
pub fn system_type(kind: &str) -> &'static str {
    match kind {
        "p25" => "p25",
        "smartnet" => "smartnet",
        "dmr" => "dmr",
        "nxdn" => "nxdn",
        "edacs" => "edacs",
        _ => "conventional",
    }
}

/// The spec's `Site`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Site {
    pub site_id: i64,
    pub system_id: i64,
    pub short_name: String,
    pub instance_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nac: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rfss: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p25_site_id: Option<i64>,
    /// Never known to the plugin, so never served.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sys_num: Option<i64>,
}

impl Site {
    pub fn from_row(row: &SiteRow, instance_id: &str) -> Site {
        Site {
            site_id: row.id.0,
            system_id: row.system_id.0,
            short_name: row.name.clone(),
            instance_id: instance_id.to_string(),
            nac: row.nac.map(|n| hex(Some(n))),
            rfss: row.rfss.map(i64::from),
            p25_site_id: row.site_number.map(i64::from),
            sys_num: None,
        }
    }
}

/// The spec's `System`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct System {
    pub system_id: i64,
    pub system_type: &'static str,
    pub name: String,
    pub sysid: String,
    pub wacn: String,
    pub sites: Vec<Site>,
}

/// `systems.name` when set, else the short name.
pub fn display_name(row: &SystemRow) -> String {
    if row.name.is_empty() { row.short_name.clone() } else { row.name.clone() }
}

impl System {
    pub fn from_rows(row: &SystemRow, sites: Vec<SiteRow>, instance_id: &str) -> System {
        System {
            system_id: row.id.0,
            system_type: system_type(&row.kind),
            name: display_name(row),
            sysid: hex(row.sysid),
            wacn: hex(row.wacn),
            sites: sites.iter().map(|s| Site::from_row(s, instance_id)).collect(),
        }
    }
}

/// The spec's `P25System`: a system with its sites and counts.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct P25System {
    pub system_id: i64,
    pub name: String,
    pub sysid: String,
    pub wacn: String,
    pub sites: Vec<Site>,
    pub talkgroup_count: i64,
    pub unit_count: i64,
    pub calls_24h: i64,
}

impl P25System {
    pub fn from_system(system: System, (talkgroup_count, unit_count, calls_24h): (i64, i64, i64)) -> P25System {
        P25System {
            system_id: system.system_id,
            name: system.name,
            sysid: system.sysid,
            wacn: system.wacn,
            sites: system.sites,
            talkgroup_count,
            unit_count,
            calls_24h,
        }
    }
}

#[derive(Serialize)]
pub struct SystemListResponse {
    pub systems: Vec<System>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct P25SystemListResponse {
    pub systems: Vec<P25System>,
    pub total: usize,
}

/// The spec's `Talkgroup`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Talkgroup {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub tgid: u32,
    pub alpha_tag: String,
    pub tag: String,
    pub group: String,
    pub description: String,
    /// `"E"` when any call was encrypted, else `"D"`.
    pub mode: &'static str,
    pub first_seen: String,
    pub last_seen: String,
    pub call_count: i64,
    pub calls_1h: i64,
    pub calls_24h: i64,
    pub unit_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<i64>,
}

impl Talkgroup {
    pub fn from_rows(row: &TalkgroupRow, system: Option<&SystemRow>) -> Talkgroup {
        Talkgroup {
            system_id: row.system_id.0,
            system_name: system.map(display_name).unwrap_or_default(),
            sysid: hex(system.and_then(|s| s.sysid)),
            tgid: row.tgid,
            alpha_tag: row.alpha_tag.clone(),
            tag: row.tag.clone(),
            group: row.group.clone(),
            description: row.description.clone(),
            mode: if row.encrypted_count > 0 { "E" } else { "D" },
            first_seen: rfc3339(row.first_heard_ms),
            last_seen: rfc3339(row.last_heard_ms),
            call_count: row.call_count_30d,
            calls_1h: row.calls_1h,
            calls_24h: row.calls_24h,
            unit_count: row.unit_count_30d,
            relevance_score: row.relevance_score,
        }
    }
}

#[derive(Serialize)]
pub struct TalkgroupListResponse {
    pub talkgroups: Vec<Talkgroup>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

/// The spec's `Unit`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Unit {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub unit_id: u32,
    pub alpha_tag: String,
    /// `"csv"` when the store says `recorder` and the tag is not empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alpha_tag_source: Option<&'static str>,
    pub first_seen: String,
    pub last_seen: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_tgid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_tg_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorder_alpha_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ota_alpha_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<i64>,
}

impl Unit {
    /// `last_event_tg_tag` is the alpha tag of the talkgroup of the last event, when known.
    pub fn from_rows(row: &UnitRow, system: Option<&SystemRow>, last_event_tg_tag: Option<String>, call_count: Option<i64>) -> Unit {
        let tagged = !row.alpha_tag.is_empty();
        Unit {
            system_id: row.system_id.0,
            system_name: system.map(display_name).unwrap_or_default(),
            sysid: hex(system.and_then(|s| s.sysid)),
            unit_id: row.unit_id,
            alpha_tag: row.alpha_tag.clone(),
            alpha_tag_source: (tagged && row.alpha_tag_source == "recorder").then_some("csv"),
            first_seen: rfc3339(row.first_seen_ms),
            last_seen: rfc3339(row.last_seen_ms),
            last_event_type: row.last_event_type.clone(),
            last_event_time: row.last_event_ms.map(rfc3339),
            last_event_tgid: row.last_event_tgid,
            last_event_tg_tag: last_event_tg_tag.filter(|t| !t.is_empty()),
            recorder_alpha_tag: (tagged && row.alpha_tag_source == "recorder").then(|| row.alpha_tag.clone()),
            ota_alpha_tag: Some(row.ota_alpha_tag.clone()).filter(|t| !t.is_empty()),
            call_count,
            relevance_score: None,
        }
    }
}

#[derive(Serialize)]
pub struct UnitListResponse {
    pub units: Vec<Unit>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

/// The spec's `UnitEvent`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnitEvent {
    pub id: i64,
    pub event_type: String,
    pub time: String,
    pub system_id: i64,
    pub system_name: String,
    pub unit_rid: u32,
    pub unit_id: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub unit_alpha_tag: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tgid: Option<u32>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub tg_alpha_tag: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub tg_description: String,
    pub instance_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_json: Option<Value>,
    /// Lifted from `metadata_json` for `signal` events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signaling_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_type: Option<String>,
}

impl UnitEvent {
    pub fn from_row(row: &UnitEventRow, instance_id: &str) -> UnitEvent {
        let metadata: Option<Value> = row.metadata_json.as_deref().and_then(|t| serde_json::from_str(t).ok()).filter(Value::is_object);
        let lift = |key: &str| metadata.as_ref().filter(|_| row.event_type == "signal").and_then(|m| m.get(key)).and_then(Value::as_str).map(String::from);
        UnitEvent {
            id: row.id,
            event_type: row.event_type.clone(),
            time: rfc3339(row.time_ms),
            system_id: row.system_id.0,
            system_name: if row.system_name.is_empty() { row.system_short_name.clone() } else { row.system_name.clone() },
            unit_rid: row.unit,
            unit_id: row.unit,
            unit_alpha_tag: row.unit_alpha_tag.clone(),
            tgid: row.tgid,
            tg_alpha_tag: row.tg_alpha_tag.clone(),
            tg_description: row.tg_description.clone(),
            instance_id: instance_id.to_string(),
            signaling_type: lift("signaling_type"),
            signal_type: lift("signal_type"),
            metadata_json: metadata,
        }
    }
}

#[derive(Serialize)]
pub struct UnitEventListResponse {
    pub events: Vec<UnitEvent>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

/// The spec's `Affiliation`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Affiliation {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub unit_id: u32,
    pub unit_alpha_tag: String,
    pub tgid: u32,
    pub tg_alpha_tag: String,
    pub tg_description: String,
    pub tg_tag: String,
    pub tg_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_tgid: Option<u32>,
    pub affiliated_since: String,
    pub last_event_time: String,
    pub status: &'static str,
}

impl Affiliation {
    /// The system's name and sysid come from the systems table when it is known
    /// (the map's own names may be empty after a rebuild).
    pub fn from_entry(e: &AffiliationEntry, system: Option<&SystemRow>) -> Affiliation {
        let sysid = match system.and_then(|s| s.sysid) {
            Some(s) => hex(Some(s)),
            None if !e.names.sysid.is_empty() => e.names.sysid.clone(),
            None => hex(None),
        };
        Affiliation {
            system_id: e.system_id,
            system_name: system.map(display_name).unwrap_or_else(|| e.names.system_name.clone()),
            sysid,
            unit_id: e.unit_id,
            unit_alpha_tag: e.names.unit_alpha_tag.clone(),
            tgid: e.tgid,
            tg_alpha_tag: e.names.tg_alpha_tag.clone(),
            tg_description: e.names.tg_description.clone(),
            tg_tag: e.names.tg_tag.clone(),
            tg_group: e.names.tg_group.clone(),
            previous_tgid: e.previous_tgid,
            affiliated_since: rfc3339(e.affiliated_since_ms),
            last_event_time: rfc3339(e.last_event_time_ms),
            status: e.status,
        }
    }
}

#[derive(Serialize)]
pub struct AffiliationSummary {
    pub talkgroup_counts: BTreeMap<u32, i64>,
}

#[derive(Serialize)]
pub struct AffiliationListResponse {
    pub affiliations: Vec<Affiliation>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
    pub summary: AffiliationSummary,
}

/// The spec's `TalkgroupEncryptionStat`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EncryptionStat {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub tgid: u32,
    pub tg_alpha_tag: String,
    pub tg_description: String,
    pub encrypted_count: i64,
    pub clear_count: i64,
    pub total_count: i64,
    pub encrypted_pct: f64,
}

impl EncryptionStat {
    pub fn from_rows(row: &EncryptionStatRow, system: Option<&SystemRow>) -> EncryptionStat {
        let total = row.encrypted_calls + row.clear_calls;
        EncryptionStat {
            system_id: row.system_id.0,
            system_name: system.map(display_name).unwrap_or_default(),
            sysid: hex(system.and_then(|s| s.sysid)),
            tgid: row.tgid,
            tg_alpha_tag: row.alpha_tag.clone(),
            tg_description: row.description.clone(),
            encrypted_count: row.encrypted_calls,
            clear_count: row.clear_calls,
            total_count: total,
            encrypted_pct: if total == 0 { 0.0 } else { row.encrypted_calls as f64 * 100.0 / total as f64 },
        }
    }
}

#[derive(Serialize)]
pub struct EncryptionStatsResponse {
    pub stats: Vec<EncryptionStat>,
    pub total: usize,
    pub hours: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};

    async fn parts(e: ApiError) -> (u16, Value) {
        let res = e.into_response();
        let status = res.status().as_u16();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn errors_have_the_spec_codes_and_statuses() {
        assert_eq!(parts(ApiError::BadRequest("b".into())).await, (400, json!({"code": "bad_request", "error": "b"})));
        assert_eq!(parts(ApiError::InvalidParameter("p".into())).await.1["code"], "invalid_parameter");
        assert_eq!(parts(ApiError::InvalidTimeRange("t".into())).await.1["code"], "invalid_time_range");
        assert_eq!(parts(ApiError::NotFound("n".into())).await.0, 404);
        assert_eq!(parts(ApiError::Unauthorized).await, (401, json!({"code": "unauthorized", "error": "missing or invalid token"})));
        let (s, v) = parts(StoreError("disk".into()).into()).await;
        assert_eq!((s, v), (500, json!({"code": "internal_error", "error": "internal error"})));
        let (s, v) = parts(ApiError::NotImplemented("/api/v1/query".into())).await;
        assert_eq!((s, &v["code"], &v["detail"]), (501, &json!("not_implemented"), &json!("/api/v1/query")));
        let matches = vec![(1, "ohmarcs".into(), "3AB".into()), (2, "fire-conv".into(), "0".into())];
        let (s, v) = parts(ApiError::Ambiguous { what: "talkgroup", id: 9178, matches }).await;
        assert_eq!(s, 409);
        assert_eq!(
            v,
            json!({"code": "ambiguous_id", "error": "Ambiguous talkgroup ID 9178: exists in multiple systems", "matches": [
                {"system_id": 1, "system_name": "ohmarcs", "sysid": "3AB"},
                {"system_id": 2, "system_name": "fire-conv", "sysid": "0"},
            ]})
        );
    }
}

/// The spec's `CallTransmission`: one unit key-up in a call.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CallTransmission {
    pub src: u32,
    pub tag: String,
    pub time: String,
    pub pos: f64,
    pub duration: f64,
    /// 0 or 1.
    pub emergency: i64,
    /// Not recorded by the plugin.
    pub signal_system: &'static str,
}

impl CallTransmission {
    pub fn from_row(row: &TransmissionRow) -> CallTransmission {
        CallTransmission {
            src: row.unit,
            tag: row.tag.clone(),
            time: rfc3339(row.time_ms),
            pos: row.pos_s,
            duration: row.len_s,
            emergency: i64::from(row.emergency),
            signal_system: "",
        }
    }
}

/// The spec's `CallFrequency`: a stretch of the call on one frequency.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CallFrequency {
    pub freq: u64,
    pub time: String,
    pub pos: f64,
    pub len: f64,
    pub error_count: u64,
    pub spike_count: u64,
}

impl CallFrequency {
    pub fn from_row(row: &FrequencyRow) -> CallFrequency {
        CallFrequency {
            freq: row.freq_hz,
            time: rfc3339(row.time_ms),
            pos: row.pos_s,
            len: row.len_s,
            error_count: row.error_count,
            spike_count: row.spike_count,
        }
    }
}

/// The spec's `CallUnit`: a radio heard on the call.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CallUnit {
    pub system_id: i64,
    pub unit_id: u32,
    pub alpha_tag: String,
}

/// The spec's `Call`, with every field the plugin can fill. Transcription
/// fields other than `has_transcription` are left out (they are optional).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Call {
    pub call_id: i64,
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site_short_name: Option<String>,
    pub tgid: u32,
    pub tg_alpha_tag: String,
    pub tg_description: String,
    pub tg_tag: String,
    pub tg_group: String,
    pub start_time: String,
    pub stop_time: Option<String>,
    /// Seconds; elapsed so far while the call is recording.
    pub duration: f64,
    pub audio_url: Option<String>,
    pub audio_type: String,
    pub freq: u64,
    pub freq_error: i64,
    pub signal_db: Option<f64>,
    pub noise_db: Option<f64>,
    pub error_count: u64,
    pub spike_count: u64,
    pub call_state: &'static str,
    pub emergency: bool,
    pub encrypted: bool,
    pub analog: bool,
    pub conventional: bool,
    pub phase2_tdma: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tdma_slot: Option<u8>,
    pub recorded: bool,
    pub not_recorded_reason: Option<String>,
    pub decrypted: bool,
    pub algid: Option<u8>,
    pub key_id: Option<u16>,
    pub priority: u8,
    pub duplex: bool,
    pub packet: bool,
    /// The other talkgroups the call was patched to.
    pub patched_tgids: Vec<u32>,
    pub src_list: Vec<CallTransmission>,
    pub freq_list: Vec<CallFrequency>,
    pub unit_ids: Vec<u32>,
    pub units: Vec<CallUnit>,
    pub has_transcription: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_json: Option<Value>,
}

impl Call {
    /// The call with its system and site names, transmissions, frequencies and units.
    pub fn from_row(conn: &Connection, row: &CallRow) -> rusqlite::Result<Call> {
        let system = system_by_id(conn, row.system_id)?;
        let site = match row.site_id {
            Some(s) => site_by_id(conn, s)?,
            None => None,
        };
        let src_list: Vec<CallTransmission> = transmissions(conn, row.id)?.iter().map(CallTransmission::from_row).collect();
        let freq_list = frequencies(conn, row.id)?.iter().map(CallFrequency::from_row).collect();
        let unit_ids = units_of(conn, row.id)?;
        let units = unit_ids
            .iter()
            .map(|&u| {
                Ok(CallUnit { system_id: row.system_id.0, unit_id: u, alpha_tag: unit(conn, row.system_id, u)?.map(|r| r.alpha_tag).unwrap_or_default() })
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let recording = row.state == "recording";
        let duration = match row.length_ms {
            Some(ms) => ms as f64 / 1000.0,
            None if recording => (now_ms() - row.start_ms).max(0) as f64 / 1000.0,
            None => 0.0,
        };
        let concluded = row.state == "concluded";
        let audio_url = (row.recorded && concluded && row.audio_path.is_some()).then(|| format!("/api/v1/calls/{}/audio", row.id.0));
        Ok(Call {
            call_id: row.id.0,
            system_id: row.system_id.0,
            system_name: system.as_ref().map(display_name).unwrap_or_default(),
            sysid: hex(system.as_ref().and_then(|s| s.sysid)),
            site_id: row.site_id.map(|s| s.0),
            site_short_name: site.map(|s| s.name),
            tgid: row.tgid,
            tg_alpha_tag: row.tg_alpha_tag.clone(),
            tg_description: row.tg_description.clone(),
            tg_tag: row.tg_tag.clone(),
            tg_group: row.tg_group.clone(),
            start_time: rfc3339(row.start_ms),
            stop_time: row.end_ms.map(rfc3339),
            duration,
            audio_url,
            audio_type: row.audio_type.clone(),
            freq: row.freq_hz.unwrap_or(0),
            freq_error: row.freq_error,
            signal_db: row.signal,
            noise_db: row.noise,
            error_count: row.error_count,
            spike_count: row.spike_count,
            call_state: match row.state.as_str() {
                "recording" => "recording",
                "ended" => "stopped",
                _ => "completed",
            },
            emergency: row.emergency,
            encrypted: row.encrypted,
            analog: row.analog,
            conventional: system.as_ref().is_some_and(|s| s.kind == "conventional"),
            phase2_tdma: row.phase2_tdma,
            tdma_slot: row.tdma_slot,
            recorded: row.recorded,
            not_recorded_reason: row.not_recorded_reason.clone(),
            decrypted: row.decrypted,
            algid: row.algid,
            key_id: row.key_id,
            priority: row.priority,
            duplex: row.duplex,
            packet: row.packet,
            patched_tgids: row.patched_tgids.iter().copied().filter(|&t| t != row.tgid).collect(),
            src_list,
            freq_list,
            unit_ids,
            units,
            has_transcription: false,
            metadata_json: row.metadata_json.as_deref().and_then(|t| serde_json::from_str(t).ok()).filter(Value::is_object),
        })
    }
}

#[derive(Serialize)]
pub struct CallListResponse {
    pub calls: Vec<Call>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Serialize)]
pub struct ActiveCallListResponse {
    pub calls: Vec<Call>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct CallFrequencyListResponse {
    pub frequencies: Vec<CallFrequency>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct CallTransmissionListResponse {
    pub transmissions: Vec<CallTransmission>,
    pub total: usize,
}

/// The spec's `SystemActivity`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SystemActivity {
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub calls_1h: i64,
    pub calls_24h: i64,
    pub active_talkgroups: i64,
    pub active_units: i64,
}

impl SystemActivity {
    pub fn from_rows(row: &ActivityRow, system: &SystemRow) -> SystemActivity {
        SystemActivity {
            system_id: system.id.0,
            system_name: display_name(system),
            sysid: hex(system.sysid),
            calls_1h: row.calls_1h,
            calls_24h: row.calls_24h,
            active_talkgroups: row.active_talkgroups,
            active_units: row.active_units,
        }
    }
}

/// The spec's `StatsResponse`. `systems` counts sites ("total recording sites").
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatsResponse {
    pub systems: i64,
    pub talkgroups: i64,
    pub units: i64,
    pub total_calls: i64,
    pub calls_24h: i64,
    pub calls_1h: i64,
    pub total_duration_hours: f64,
    pub system_activity: Vec<SystemActivity>,
}

/// The spec's `DecodeRate`. Both rates are trunk-recorder's messages per second.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DecodeRate {
    pub time: String,
    pub system_id: i64,
    pub system_name: String,
    pub sysid: String,
    pub decode_rate: f64,
    pub decode_rate_interval: f64,
    /// Hz; 0 when the site has none.
    pub control_channel: u64,
}

impl DecodeRate {
    pub fn from_state(site: &SiteRow, system: &SystemRow, state: &RecorderStateRow) -> DecodeRate {
        DecodeRate {
            time: rfc3339(state.time_ms),
            system_id: site.system_id.0,
            system_name: display_name(system),
            sysid: hex(system.sysid),
            decode_rate: state.decode_rate,
            decode_rate_interval: state.decode_rate,
            control_channel: state.control_channel_hz.unwrap_or(0),
        }
    }
}

#[derive(Serialize)]
pub struct DecodeRatesResponse {
    pub rates: Vec<DecodeRate>,
    pub total: usize,
}

/// The spec's `Recorder`: one per site, from its newest status sample.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Recorder {
    pub id: String,
    pub src_num: i64,
    pub rec_num: i64,
    #[serde(rename = "type")]
    pub kind: String,
    pub rec_state: &'static str,
    pub freq: u64,
    pub duration: f64,
    pub count: u32,
    pub squelched: bool,
    pub tgid: Option<u32>,
    pub tg_alpha_tag: Option<String>,
    pub unit_id: Option<u32>,
    pub unit_alpha_tag: Option<String>,
}

impl Recorder {
    /// `first_ms` is the site's first sample; `duration` is the time since.
    pub fn from_site(site: &SiteRow, system: &SystemRow, state: &RecorderStateRow, first_ms: i64, now_ms: i64) -> Recorder {
        let rec_state = if state.recording > 0 {
            "recording"
        } else if state.control_channel_hz.is_some() {
            "available"
        } else {
            "idle"
        };
        Recorder {
            id: format!("{}_{}", system.id.0, site.id.0),
            src_num: system.id.0,
            rec_num: site.id.0,
            kind: system.kind.to_uppercase(),
            rec_state,
            freq: state.control_channel_hz.unwrap_or(0),
            duration: (now_ms - first_ms).max(0) as f64 / 1000.0,
            count: state.recording,
            squelched: false,
            tgid: None,
            tg_alpha_tag: None,
            unit_id: None,
            unit_alpha_tag: None,
        }
    }
}

#[derive(Serialize)]
pub struct RecorderListResponse {
    pub recorders: Vec<Recorder>,
    pub total: usize,
}
