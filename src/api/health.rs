//! `/health`, `/auth-init`, `/openapi.yaml` and `/pages`: open without a token.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::{AppState, Features, rfc3339};

/// The spec this API serves.
pub const SPEC: &str = include_str!("../../openapi/openapi.yaml");

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub uptime_seconds: u64,
    pub checks: Checks,
    pub trunk_recorders: Vec<TrunkRecorder>,
    pub features: Features,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcribe: Option<TranscribeHealth>,
}

/// How the bridge to the transcribe plugin stands.
#[derive(Serialize)]
pub struct TranscribeHealth {
    pub url: String,
    /// Whether the plugin's event stream is open.
    pub connected: bool,
    /// When the last event arrived from it (ms since the epoch).
    pub last_event_ms: Option<i64>,
}

#[derive(Serialize)]
pub struct Checks {
    pub database: &'static str,
}

#[derive(Serialize)]
pub struct TrunkRecorder {
    pub instance_id: String,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
}

pub async fn health(State(state): State<AppState>) -> Response {
    let store = state.store.clone();
    let db_ok = tokio::task::spawn_blocking(move || store.read(|c| c.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)))).await.is_ok_and(|r| r.is_ok());
    let host = state.host.snapshot();
    let body = HealthResponse {
        status: if db_ok { "healthy" } else { "degraded" },
        version: state.version,
        uptime_seconds: state.started.elapsed().as_secs(),
        checks: Checks { database: if db_ok { "ok" } else { "error" } },
        trunk_recorders: vec![TrunkRecorder {
            instance_id: host.name,
            status: if host.connected { "connected" } else { "disconnected" },
            last_seen: host.last_seen_ms.map(rfc3339),
        }],
        features: state.features,
        transcribe: state.transcripts.as_ref().map(|t| TranscribeHealth { url: t.base().to_string(), connected: false, last_event_ms: None }),
    };
    let status = if db_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(body)).into_response()
}

#[derive(Serialize)]
pub struct AuthInit {
    pub mode: &'static str,
    pub jwt_enabled: bool,
}

pub async fn auth_init(State(state): State<AppState>) -> Json<AuthInit> {
    Json(AuthInit { mode: if state.token.is_some() { "token" } else { "open" }, jwt_enabled: false })
}

pub async fn openapi() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/yaml")], SPEC)
}

pub async fn pages(State(state): State<AppState>) -> Response {
    Json(&*state.pages).into_response()
}
