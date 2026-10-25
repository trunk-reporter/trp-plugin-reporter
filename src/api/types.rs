//! The API's wire types and their formatting: errors, times, hex identities.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use time::OffsetDateTime;
use time::macros::format_description;

use crate::store::StoreError;

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
