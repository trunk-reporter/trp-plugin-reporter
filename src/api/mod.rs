//! tr-engine's REST API under `/api/v1`: shared state, the router, the token
//! guard, and the query-parameter rules every endpoint shares.

pub mod calls;
pub mod health;
pub mod sse;
pub mod stats;
pub mod systems;
pub mod talkgroups;
pub mod types;
pub mod units;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{OriginalUri, Request, State};
use axum::http::Method;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::affiliations::Affiliations;
use crate::bus::Bus;
use crate::store::Store;
use crate::web::Pages;

pub use types::{ApiError, hex, rfc3339};

/// A request's query parameters.
pub type Query = HashMap<String, String>;

/// What the plugin knows of the recorder it runs in, for `/health`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostStatusInner {
    pub name: String,
    pub connected: bool,
    pub last_seen_ms: Option<i64>,
}

/// Shared [`HostStatusInner`]: the plugin updates it, `/health` reads it.
#[derive(Clone, Debug, Default)]
pub struct HostStatus(pub Arc<Mutex<HostStatusInner>>);

impl HostStatus {
    pub fn new(name: &str, connected: bool) -> HostStatus {
        HostStatus(Arc::new(Mutex::new(HostStatusInner { name: name.to_string(), connected, last_seen_ms: None })))
    }

    fn lock(&self) -> MutexGuard<'_, HostStatusInner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The recorder was heard from at `now_ms`.
    pub fn touch(&self, now_ms: i64) {
        self.lock().last_seen_ms = Some(now_ms);
    }

    pub fn snapshot(&self) -> HostStatusInner {
        self.lock().clone()
    }
}

/// Everything a handler needs. Cloning shares it.
#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub bus: Bus,
    pub affiliations: Affiliations,
    /// The recorder's capture folder: call audio paths are relative to it.
    pub capture_dir: PathBuf,
    /// When set, `/api/v1/*` (but the open endpoints) needs it.
    pub token: Option<String>,
    pub started: Instant,
    pub version: &'static str,
    pub host: HostStatus,
    pub pages: Arc<Pages>,
    /// How often an idle event stream gets a keepalive comment.
    pub keepalive: Duration,
}

impl AppState {
    /// Started now, this crate's version, no pages yet. An empty token is no token.
    pub fn new(store: Store, bus: Bus, affiliations: Affiliations, capture_dir: PathBuf, token: Option<String>, host: HostStatus) -> AppState {
        AppState {
            store,
            bus,
            affiliations,
            capture_dir,
            token: token.filter(|t| !t.is_empty()),
            started: Instant::now(),
            version: env!("CARGO_PKG_VERSION"),
            host,
            pages: Arc::new(Pages::empty()),
            keepalive: sse::KEEPALIVE,
        }
    }
}

/// Spec paths (under `/api/v1`) this plugin does not serve: each answers 501
/// for any method. Each API task removes the paths it routes; the
/// `every_spec_path_is_served_or_declared_out_of_scope` test keeps this list
/// and the spec's `paths:` in step.
pub const OUT_OF_SCOPE: &[&str] = &[
    "/auth/setup",
    "/auth/login",
    "/auth/refresh",
    "/auth/logout",
    "/auth/me",
    "/auth/keys",
    "/auth/keys/{id}",
    "/auth/keys/all",
    "/auth/keys/service",
    "/auth/keys/{id}/any",
    "/users",
    "/users/{id}",
    "/talkgroup-directory",
    "/talkgroup-directory/import",
    "/unit-tags/import",
    "/unit-tag-suggestions",
    "/unit-tag-suggestions/{id}",
    "/unit-tag-suggestions/{id}/approve",
    "/unit-tag-suggestions/{id}/dismiss",
    "/calls/{id}/transcription",
    "/calls/{id}/transcriptions",
    "/calls/{id}/transcribe",
    "/calls/{id}/transcription/verify",
    "/calls/{id}/transcription/reject",
    "/calls/{id}/transcription/exclude",
    "/transcriptions/search",
    "/transcriptions/batch",
    "/transcriptions/queue",
    "/call-upload",
    "/call-groups",
    "/call-groups/{id}",
    "/stats/talkgroup-activity",
    "/stats/call-volume",
    "/stats/daily-overview",
    "/stats/category-breakdown",
    "/stats/call-heatmap",
    "/analytics/recorder-utilization",
    "/analytics/decode-rates",
    "/trunking-messages",
    "/console-messages",
    "/audio/live",
    "/audio/jitter",
    "/query",
    "/admin/systems/merge",
    "/admin/maintenance",
    "/admin/transcribe-backfill",
    "/admin/transcribe-backfill/{job_id}",
    "/admin/maintenance/config",
    "/admin/maintenance/config/{key}",
    "/admin/storage/stats",
    "/admin/storage/purge/{table}",
];

/// `/api/v1/*` paths that need no token.
const OPEN: &[&str] = &["/api/v1/health", "/api/v1/auth-init", "/api/v1/pages", "/api/v1/openapi.yaml"];

/// The API (and, from Task 17, the pages at `/`).
pub fn router(state: AppState) -> Router {
    let mut api = Router::new()
        .route("/health", get(health::health))
        .route("/auth-init", get(health::auth_init))
        .route("/openapi.yaml", get(health::openapi))
        .route("/pages", get(health::pages).post(not_implemented))
        .route("/systems", get(systems::list))
        .route("/systems/{id}", get(systems::one).patch(not_implemented))
        .route("/sites/{id}", get(systems::site).patch(not_implemented))
        .route("/p25-systems", get(systems::p25))
        .route("/talkgroups", get(talkgroups::list))
        .route("/talkgroups/encryption-stats", get(talkgroups::encryption_stats))
        .route("/talkgroups/{id}", get(talkgroups::one).patch(not_implemented))
        .route("/talkgroups/{id}/units", get(talkgroups::units))
        .route("/talkgroups/{id}/calls", get(calls::talkgroup_calls))
        .route("/units", get(units::list))
        .route("/units/{id}", get(units::one).patch(not_implemented))
        .route("/units/{id}/events", get(units::events))
        .route("/units/{id}/calls", get(calls::unit_calls))
        .route("/unit-events", get(units::unit_events))
        .route("/unit-affiliations", get(units::affiliations))
        .route("/stats", get(stats::stats))
        .route("/stats/rates", get(stats::rates))
        .route("/recorders", get(stats::recorders))
        .route("/events/stream", get(sse::stream))
        .route("/calls", get(calls::list))
        .route("/calls/active", get(calls::active))
        .route("/calls/{id}", get(calls::one))
        .route("/calls/{id}/audio", get(calls::audio))
        .route("/calls/{id}/frequencies", get(calls::call_frequencies))
        .route("/calls/{id}/transmissions", get(calls::call_transmissions));
    for path in OUT_OF_SCOPE {
        api = api.route(path, any(not_implemented));
    }
    Router::new().nest("/api/v1", api.fallback(not_found)).layer(middleware::from_fn_with_state(state.clone(), require_token)).with_state(state)
}

async fn not_implemented(OriginalUri(uri): OriginalUri) -> ApiError {
    ApiError::NotImplemented(uri.path().to_string())
}

/// Unrouted paths. The `"no endpoint "` prefix tells this 404 from a handler's (a missing row).
async fn not_found(method: Method, OriginalUri(uri): OriginalUri) -> ApiError {
    ApiError::NotFound(format!("no endpoint {method} {}", uri.path()))
}

/// Equal, in time that depends only on the lengths.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// With a token set, every `/api/` path but [`OPEN`] needs `Authorization: Bearer <token>` or `?token=<token>`.
async fn require_token(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let Some(token) = state.token.as_deref() else { return next.run(req).await };
    let path = req.uri().path();
    if !path.starts_with("/api/") || OPEN.contains(&path) {
        return next.run(req).await;
    }
    let bearer = req.headers().get(AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")));
    let query = axum::extract::Query::<Query>::try_from_uri(req.uri()).map(|q| q.0).unwrap_or_default();
    let ok = [bearer, query.get("token").map(String::as_str)].into_iter().flatten().any(|t| same(t.trim().as_bytes(), token.as_bytes()));
    if ok { next.run(req).await } else { ApiError::Unauthorized.into_response() }
}

fn param<'a>(q: &'a Query, name: &str) -> Option<&'a str> {
    q.get(name).map(|v| v.trim()).filter(|v| !v.is_empty())
}

/// `limit` (default 50, 1..=1000) and `offset` (default 0, ≥ 0).
pub fn pagination(q: &Query) -> Result<(u32, u32), ApiError> {
    let limit = match param(q, "limit") {
        None => 50,
        Some(v) => {
            v.parse::<u32>().ok().filter(|l| (1..=1000).contains(l)).ok_or_else(|| ApiError::InvalidParameter(format!("limit must be 1 to 1000, not {v}")))?
        }
    };
    let offset = match param(q, "offset") {
        None => 0,
        Some(v) => v.parse::<u32>().map_err(|_| ApiError::InvalidParameter(format!("offset must be 0 or more, not {v}")))?,
    };
    Ok((limit, offset))
}

/// `sort` (a field from `allowed`, `-` prefix for descending; `default` when absent)
/// and `sort_dir` (`asc`/`desc`, used when the field has no prefix) → (field, descending).
pub fn sort(q: &Query, default: &str, allowed: &[&str]) -> Result<(String, bool), ApiError> {
    let raw = param(q, "sort").unwrap_or(default);
    let (field, prefixed) = match raw.strip_prefix('-') {
        Some(f) => (f, true),
        None => (raw, false),
    };
    if !allowed.contains(&field) {
        return Err(ApiError::InvalidParameter(format!("sort must be one of {}, not {field}", allowed.join(", "))));
    }
    let desc = match param(q, "sort_dir") {
        _ if prefixed => true,
        None | Some("asc") => false,
        Some("desc") => true,
        Some(d) => return Err(ApiError::InvalidParameter(format!("sort_dir must be asc or desc, not {d}"))),
    };
    Ok((field.to_string(), desc))
}

fn time_param(q: &Query, name: &str) -> Result<Option<i64>, ApiError> {
    let Some(v) = param(q, name) else { return Ok(None) };
    let t = OffsetDateTime::parse(v, &Rfc3339).map_err(|_| ApiError::InvalidParameter(format!("{name} must be an RFC 3339 time, not {v}")))?;
    Ok(Some((t.unix_timestamp_nanos() / 1_000_000) as i64))
}

/// `start_time` / `end_time` (RFC 3339, any offset) → Unix ms; start after end is `InvalidTimeRange`.
pub fn time_range(q: &Query) -> Result<(Option<i64>, Option<i64>), ApiError> {
    let (start, end) = (time_param(q, "start_time")?, time_param(q, "end_time")?);
    if let (Some(s), Some(e)) = (start, end)
        && s > e
    {
        return Err(ApiError::InvalidTimeRange("start_time is after end_time".into()));
    }
    Ok((start, end))
}

/// Every id in the comma lists of the parameters `names` (aliases of one filter,
/// e.g. `["tgid", "tgids"]`), decimal; values that aren't numbers are ignored.
pub fn ids(q: &Query, names: &[&str]) -> Vec<i64> {
    names.iter().filter_map(|n| q.get(*n)).flat_map(|v| v.split(',')).filter_map(|s| s.trim().parse().ok()).collect()
}

/// The `sysid` / `sysids` filter: comma lists of hex P25 system ids (as `systems.sysid` holds them).
pub fn sysids(q: &Query) -> Vec<u32> {
    ["sysid", "sysids"].iter().filter_map(|n| q.get(*n)).flat_map(|v| v.split(',')).filter_map(|s| u32::from_str_radix(s.trim(), 16).ok()).collect()
}

/// A boolean parameter: `true`/`1`, `false`/`0`, or absent.
pub fn flag(q: &Query, name: &str) -> Result<Option<bool>, ApiError> {
    match param(q, name) {
        None => Ok(None),
        Some("true" | "1") => Ok(Some(true)),
        Some("false" | "0") => Ok(Some(false)),
        Some(v) => Err(ApiError::InvalidParameter(format!("{name} must be true or false, not {v}"))),
    }
}

/// A talkgroup or unit id: `system_id:id`, `system_id-id`, or a plain `id`.
pub fn composite(id: &str) -> Result<(Option<i64>, u32), ApiError> {
    let bad = || ApiError::InvalidParameter(format!("id must be system_id:id or id, not {id}"));
    match id.split_once([':', '-']) {
        Some((system, n)) => Ok((Some(system.parse().map_err(|_| bad())?), n.parse().map_err(|_| bad())?)),
        None => Ok((None, id.parse().map_err(|_| bad())?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> Query {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn parameters_parse_as_the_spec_says() {
        assert_eq!(pagination(&q(&[])).unwrap(), (50, 0));
        assert!(pagination(&q(&[("limit", "0")])).is_err() && pagination(&q(&[("limit", "1001")])).is_err());
        assert_eq!(sort(&q(&[("sort", "-last_seen")]), "alpha_tag", &["alpha_tag", "last_seen"]).unwrap(), ("last_seen".into(), true));
        assert!(sort(&q(&[("sort", "bogus")]), "alpha_tag", &["alpha_tag"]).is_err());
        assert_eq!(ids(&q(&[("tgids", "1,2")]), &["tgid", "tgids"]), vec![1, 2]);
        assert_eq!(composite("1:9178").unwrap(), (Some(1), 9178));
        assert_eq!(composite("1-9178").unwrap(), (Some(1), 9178));
        assert_eq!(composite("9178").unwrap(), (None, 9178));
        assert!(matches!(time_range(&q(&[("start_time", "2024-01-02T00:00:00Z"), ("end_time", "2024-01-01T00:00:00Z")])), Err(ApiError::InvalidTimeRange(_))));
        assert_eq!(rfc3339(1_705_314_600_000), "2024-01-15T10:30:00.000Z");
        assert_eq!((hex(Some(0xBEE00)), hex(None)), ("BEE00".into(), "0".into()));
    }

    #[test]
    fn parameters_beyond_the_basics() {
        assert_eq!(pagination(&q(&[("limit", "1000"), ("offset", "20")])).unwrap(), (1000, 20));
        assert!(matches!(pagination(&q(&[("offset", "-1")])), Err(ApiError::InvalidParameter(_))));
        assert!(pagination(&q(&[("limit", "ten")])).is_err());
        assert_eq!(sort(&q(&[]), "alpha_tag", &["alpha_tag"]).unwrap(), ("alpha_tag".into(), false));
        assert_eq!(sort(&q(&[("sort", "last_seen"), ("sort_dir", "desc")]), "alpha_tag", &["alpha_tag", "last_seen"]).unwrap(), ("last_seen".into(), true));
        assert_eq!(sort(&q(&[("sort", "-last_seen"), ("sort_dir", "asc")]), "alpha_tag", &["last_seen"]).unwrap(), ("last_seen".into(), true));
        assert!(sort(&q(&[("sort_dir", "up")]), "alpha_tag", &["alpha_tag"]).is_err());
        assert_eq!(time_range(&q(&[("start_time", "2024-01-15T05:30:00-05:00")])).unwrap(), (Some(1_705_314_600_000), None));
        assert!(matches!(time_range(&q(&[("end_time", "yesterday")])), Err(ApiError::InvalidParameter(_))));
        assert_eq!(ids(&q(&[("tgid", "7"), ("tgids", " 8 , x,9")]), &["tgid", "tgids"]), vec![7, 8, 9]);
        assert_eq!(sysids(&q(&[("sysid", "3ab,348")])), vec![0x3AB, 0x348]);
        assert_eq!(
            (flag(&q(&[("e", "true")]), "e").unwrap(), flag(&q(&[("e", "0")]), "e").unwrap(), flag(&q(&[]), "e").unwrap()),
            (Some(true), Some(false), None)
        );
        assert!(flag(&q(&[("e", "maybe")]), "e").is_err());
        assert!(composite("1:x").is_err() && composite("abc").is_err() && composite("").is_err());
    }

    #[test]
    fn tokens_compare_whole() {
        assert!(same(b"s3", b"s3"));
        assert!(!same(b"s3", b"s4") && !same(b"s3", b"s33") && !same(b"", b"s3"));
    }
}
