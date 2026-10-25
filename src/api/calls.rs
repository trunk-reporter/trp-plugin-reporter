//! `/calls`, `/calls/active`, `/calls/{id}` and its audio, frequencies and
//! transmissions, and the call lists of a talkgroup and a unit.

use std::path::{Path as FsPath, PathBuf};

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query as QueryParams, Request, State};
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use rusqlite::Connection;
use tower::ServiceExt;
use tower_http::services::ServeFile;

use super::talkgroups::{blocking, resolve_talkgroup, system_filter, systems};
use super::types::{ActiveCallListResponse, Call, CallFrequency, CallFrequencyListResponse, CallListResponse, CallTransmission, CallTransmissionListResponse};
use super::units::resolve_unit;
use super::{ApiError, AppState, Query, flag, ids, pagination, sort, time_range};
use crate::store::calls::{CallFilter, CallRow, CallSort, active_calls, call_by_id, frequencies, list_calls, transmissions};
use crate::store::{CallId, StoreError, now_ms};

fn id_list(q: &Query, names: &[&str]) -> Vec<u32> {
    ids(q, names).into_iter().filter_map(|i| u32::try_from(i).ok()).collect()
}

fn call_row(conn: &Connection, id: &str) -> Result<CallRow, ApiError> {
    let wanted = id.trim().parse().map(CallId).map_err(|_| ApiError::InvalidParameter(format!("id must be a call id, not {id}")))?;
    call_by_id(conn, wanted).map_err(StoreError::from)?.ok_or_else(|| ApiError::NotFound(format!("call {id} not found")))
}

/// The parameters every call list takes: sort, time range, `recorded`, pagination.
fn base_filter(q: &Query) -> Result<CallFilter, ApiError> {
    let (limit, offset) = pagination(q)?;
    let (field, desc) = sort(q, "-start_time", &["start_time", "stop_time", "duration", "tgid", "freq"])?;
    let sort = match field.as_str() {
        "stop_time" => CallSort::StopTime,
        "duration" => CallSort::Duration,
        "tgid" => CallSort::Tgid,
        "freq" => CallSort::Freq,
        _ => CallSort::StartTime,
    };
    let (start_ms, end_ms) = time_range(q)?;
    // One recorder has no duplicate recordings: `deduplicate` is checked and ignored.
    flag(q, "deduplicate")?;
    Ok(CallFilter { recorded: flag(q, "recorded")?, start_ms, end_ms, sort, desc, limit, offset, ..Default::default() })
}

fn calls_of(c: &Connection, rows: &[CallRow]) -> Result<Vec<Call>, ApiError> {
    Ok(rows.iter().map(|r| Call::from_row(c, r)).collect::<rusqlite::Result<Vec<_>>>().map_err(StoreError::from)?)
}

fn page(c: &Connection, filter: &CallFilter) -> Result<CallListResponse, ApiError> {
    let (rows, total) = list_calls(c, filter).map_err(StoreError::from)?;
    Ok(CallListResponse { calls: calls_of(c, &rows)?, total, limit: filter.limit, offset: filter.offset })
}

pub async fn list(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<CallListResponse>, ApiError> {
    let mut filter = base_filter(&q)?;
    filter.emergency = flag(&q, "emergency")?;
    filter.encrypted = flag(&q, "encrypted")?;
    filter.site_ids = ids(&q, &["site_id", "sites"]);
    filter.tgids = id_list(&q, &["tgid", "tgids"]);
    filter.unit_ids = id_list(&q, &["unit_id", "units", "unit_ids"]);
    let body = blocking(&state, move |c| {
        filter.system_ids = system_filter(&q, &systems(c)?);
        page(c, &filter)
    })
    .await?;
    Ok(Json(body))
}

pub async fn active(State(state): State<AppState>) -> Result<Json<ActiveCallListResponse>, ApiError> {
    let body = blocking(&state, |c| {
        let rows = active_calls(c).map_err(StoreError::from)?;
        let calls = calls_of(c, &rows)?;
        Ok(ActiveCallListResponse { total: calls.len(), calls })
    })
    .await?;
    Ok(Json(body))
}

pub async fn one(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Call>, ApiError> {
    let body = blocking(&state, move |c| Ok(Call::from_row(c, &call_row(c, &id)?).map_err(StoreError::from)?)).await?;
    Ok(Json(body))
}

pub async fn call_frequencies(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<CallFrequencyListResponse>, ApiError> {
    let body = blocking(&state, move |c| {
        let row = call_row(c, &id)?;
        let frequencies: Vec<CallFrequency> = frequencies(c, row.id).map_err(StoreError::from)?.iter().map(CallFrequency::from_row).collect();
        Ok(CallFrequencyListResponse { total: frequencies.len(), frequencies })
    })
    .await?;
    Ok(Json(body))
}

pub async fn call_transmissions(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<CallTransmissionListResponse>, ApiError> {
    let body = blocking(&state, move |c| {
        let row = call_row(c, &id)?;
        let transmissions: Vec<CallTransmission> = transmissions(c, row.id).map_err(StoreError::from)?.iter().map(CallTransmission::from_row).collect();
        Ok(CallTransmissionListResponse { total: transmissions.len(), transmissions })
    })
    .await?;
    Ok(Json(body))
}

/// The call's audio file (`audio_path` is relative to the capture folder). `None`
/// unless the file exists and, with symlinks and `..` resolved, lies inside the folder.
async fn audio_file(capture_dir: &FsPath, audio_path: &str) -> Option<PathBuf> {
    let root = tokio::fs::canonicalize(capture_dir).await.ok()?;
    let file = tokio::fs::canonicalize(capture_dir.join(audio_path)).await.ok()?;
    (file.starts_with(&root) && file.is_file()).then_some(file)
}

/// `GET /calls/{id}/audio`: the file, with range requests.
pub async fn audio(State(state): State<AppState>, Path(id): Path<String>, req: Request) -> Result<Response, ApiError> {
    let row = blocking(&state, move |c| call_row(c, &id)).await?;
    let none = || ApiError::NotFound(format!("call {} has no audio", row.id.0));
    let audio_path = row.audio_path.as_deref().ok_or_else(none)?;
    let file = audio_file(&state.capture_dir, audio_path).await.ok_or_else(none)?;
    let mime = match file.extension().and_then(|e| e.to_str()).map(str::to_ascii_lowercase).as_deref() {
        Some("wav") => Some("audio/wav"),
        Some("m4a") => Some("audio/mp4"),
        _ => None,
    };
    let Ok(res) = ServeFile::new(&file).oneshot(req).await;
    let mut res = res.map(Body::new).into_response();
    if let Some(m) = mime {
        res.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(m));
    }
    Ok(res)
}

/// `/talkgroups/{id}/calls`
pub async fn talkgroup_calls(
    State(state): State<AppState>,
    Path(id): Path<String>,
    QueryParams(q): QueryParams<Query>,
) -> Result<Json<CallListResponse>, ApiError> {
    let mut filter = base_filter(&q)?;
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let tg = resolve_talkgroup(c, &id, now)?;
        filter.system_ids = vec![tg.system_id.0];
        filter.tgids = vec![tg.tgid];
        page(c, &filter)
    })
    .await?;
    Ok(Json(body))
}

/// `/units/{id}/calls`
pub async fn unit_calls(State(state): State<AppState>, Path(id): Path<String>, QueryParams(q): QueryParams<Query>) -> Result<Json<CallListResponse>, ApiError> {
    let mut filter = base_filter(&q)?;
    let body = blocking(&state, move |c| {
        let unit = resolve_unit(c, &id)?;
        filter.system_ids = vec![unit.system_id.0];
        filter.unit_ids = vec![unit.unit_id];
        page(c, &filter)
    })
    .await?;
    Ok(Json(body))
}
