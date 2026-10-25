//! The API's wire types and their formatting: errors, times, hex identities.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use time::OffsetDateTime;
use time::macros::format_description;

use crate::store::StoreError;
use crate::store::systems::{SiteRow, SystemRow};

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
fn display_name(row: &SystemRow) -> String {
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
