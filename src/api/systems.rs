//! `/systems`, `/systems/{id}`, `/sites/{id}` and `/p25-systems`.

use axum::Json;
use axum::extract::{Path, State};

use super::types::{P25System, P25SystemListResponse, Site, System, SystemListResponse};
use super::{ApiError, AppState};
use crate::store::systems::{list_sites, list_systems, site_by_id, system_by_id, system_counts};
use crate::store::{SiteId, SystemId, now_ms};

fn id(raw: &str) -> Result<i64, ApiError> {
    raw.parse().map_err(|_| ApiError::InvalidParameter(format!("id must be a number, not {raw}")))
}

/// Every system with its sites (`only_p25` limits to P25 networks and counts their things).
async fn systems(state: &AppState, only_p25: bool) -> Result<Vec<(System, (i64, i64, i64))>, ApiError> {
    let store = state.store.clone();
    let instance = state.host.snapshot().name;
    let now = now_ms();
    let rows = tokio::task::spawn_blocking(move || {
        store.read(|c| {
            list_systems(c)?
                .into_iter()
                .filter(|s| !only_p25 || s.kind == "p25")
                .map(|s| {
                    let counts = if only_p25 { system_counts(c, s.id, now)? } else { (0, 0, 0) };
                    Ok((System::from_rows(&s, list_sites(c, s.id)?, &instance), counts))
                })
                .collect()
        })
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(rows)
}

pub async fn list(State(state): State<AppState>) -> Result<Json<SystemListResponse>, ApiError> {
    let systems: Vec<System> = systems(&state, false).await?.into_iter().map(|(s, _)| s).collect();
    Ok(Json(SystemListResponse { total: systems.len(), systems }))
}

pub async fn p25(State(state): State<AppState>) -> Result<Json<P25SystemListResponse>, ApiError> {
    let systems: Vec<P25System> = systems(&state, true).await?.into_iter().map(|(s, counts)| P25System::from_system(s, counts)).collect();
    Ok(Json(P25SystemListResponse { total: systems.len(), systems }))
}

pub async fn one(State(state): State<AppState>, Path(raw): Path<String>) -> Result<Json<System>, ApiError> {
    let sid = SystemId(id(&raw)?);
    let store = state.store.clone();
    let instance = state.host.snapshot().name;
    let found = tokio::task::spawn_blocking(move || {
        store.read(|c| match system_by_id(c, sid)? {
            Some(s) => Ok(Some(System::from_rows(&s, list_sites(c, sid)?, &instance))),
            None => Ok(None),
        })
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    found.map(Json).ok_or_else(|| ApiError::NotFound(format!("system {} not found", sid.0)))
}

pub async fn site(State(state): State<AppState>, Path(raw): Path<String>) -> Result<Json<Site>, ApiError> {
    let sid = SiteId(id(&raw)?);
    let store = state.store.clone();
    let instance = state.host.snapshot().name;
    let found = tokio::task::spawn_blocking(move || store.read(|c| Ok(site_by_id(c, sid)?.map(|s| Site::from_row(&s, &instance)))))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
    found.map(Json).ok_or_else(|| ApiError::NotFound(format!("site {} not found", sid.0)))
}
