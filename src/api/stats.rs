//! `/stats`, `/stats/rates` and `/recorders`.

use axum::Json;
use axum::extract::{Query as QueryParams, State};

use super::talkgroups::{blocking, system_filter, systems};
use super::types::{DecodeRate, DecodeRatesResponse, Recorder, RecorderListResponse, StatsResponse, SystemActivity};
use super::{ApiError, AppState, Query, time_range};
use crate::store::recorder::{decode_rates, first_state_ms, latest_recorder_state};
use crate::store::stats::{stats as stats_row, system_activity};
use crate::store::systems::{site_by_id, system_by_id};
use crate::store::{StoreError, now_ms};

const DEFAULT_RATES: u32 = 1000;
const MAX_RATES: u32 = 10_000;

pub async fn stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let totals = stats_row(c, now).map_err(StoreError::from)?;
        let systems = systems(c)?;
        let system_activity = system_activity(c, now)
            .map_err(StoreError::from)?
            .iter()
            .filter_map(|a| systems.get(&a.system_id.0).map(|s| SystemActivity::from_rows(a, s)))
            .collect();
        Ok(StatsResponse {
            systems: totals.sites,
            talkgroups: totals.talkgroups,
            units: totals.units,
            total_calls: totals.total_calls,
            calls_24h: totals.calls_24h,
            calls_1h: totals.calls_1h,
            total_duration_hours: totals.total_duration_hours,
            system_activity,
        })
    })
    .await?;
    Ok(Json(body))
}

fn rates_limit(q: &Query) -> Result<u32, ApiError> {
    match q.get("limit").map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(DEFAULT_RATES),
        Some(v) => v
            .parse::<u32>()
            .ok()
            .filter(|l| (1..=MAX_RATES).contains(l))
            .ok_or_else(|| ApiError::InvalidParameter(format!("limit must be 1 to {MAX_RATES}, not {v}"))),
    }
}

pub async fn rates(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<DecodeRatesResponse>, ApiError> {
    let (start, end) = time_range(&q)?;
    let limit = rates_limit(&q)?;
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let rows = decode_rates(c, &system_filter(&q, &systems), start, end, limit).map_err(StoreError::from)?;
        let mut rates = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(site) = site_by_id(c, row.site_id).map_err(StoreError::from)? else { continue };
            let Some(system) = systems.get(&site.system_id.0) else { continue };
            rates.push(DecodeRate::from_state(&site, system, row));
        }
        Ok(DecodeRatesResponse { total: rates.len(), rates })
    })
    .await?;
    Ok(Json(body))
}

pub async fn recorders(State(state): State<AppState>) -> Result<Json<RecorderListResponse>, ApiError> {
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let mut recorders = Vec::new();
        for row in latest_recorder_state(c).map_err(StoreError::from)? {
            let Some(site) = site_by_id(c, row.site_id).map_err(StoreError::from)? else { continue };
            let Some(system) = system_by_id(c, site.system_id).map_err(StoreError::from)? else { continue };
            let first = first_state_ms(c, row.site_id).map_err(StoreError::from)?.unwrap_or(row.time_ms);
            recorders.push(Recorder::from_site(&site, &system, &row, first, now));
        }
        Ok(RecorderListResponse { total: recorders.len(), recorders })
    })
    .await?;
    Ok(Json(body))
}
