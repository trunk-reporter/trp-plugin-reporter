//! `/stats`, `/stats/rates` and `/recorders`.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query as QueryParams, State};

use super::talkgroups::{blocking, system_filter, systems};
use super::types::{
    ActivityBucket, DecodeRate, DecodeRatesResponse, Recorder, RecorderListResponse, StatsResponse, SystemActivity, TalkgroupActivity,
    TalkgroupActivityResponse,
};
use super::{ApiError, AppState, Query, ids, pagination, sort, time_range, window};
use crate::store::recorder::{decode_rates, first_state_ms, latest_recorder_state};
use crate::store::stats::{ActivityFilter, activity_buckets, activity_talkgroups, stats as stats_row, system_activity, talkgroup_activity as activity_rows};
use crate::store::systems::{site_by_id, system_by_id};
use crate::store::{StoreError, now_ms};

const DEFAULT_RATES: u32 = 1000;
const MAX_RATES: u32 = 10_000;

/// The bucket sizes `/stats/talkgroup-activity` offers, in seconds.
const INTERVALS: [u32; 6] = [10, 30, 60, 300, 900, 3600];
/// The most buckets one `/stats/talkgroup-activity` answer may hold: talkgroups times the window's length in intervals.
const MAX_BUCKETS: i64 = 10_000;
const DAY_MS: i64 = 86_400_000;

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

fn interval(q: &Query) -> Result<Option<u32>, ApiError> {
    let Some(v) = q.get("interval").map(|v| v.trim()).filter(|v| !v.is_empty()) else { return Ok(None) };
    v.parse::<u32>()
        .ok()
        .filter(|i| INTERVALS.contains(i))
        .map(Some)
        .ok_or_else(|| ApiError::InvalidParameter(format!("interval must be one of 10, 30, 60, 300, 900, 3600 (seconds), not {v}")))
}

/// `call_state` as `calls.state` holds it: `COMPLETED` (the default) is a concluded call, an empty value is any state.
fn call_state(q: &Query) -> Option<String> {
    match q.get("call_state").map(|v| v.trim().to_ascii_lowercase()) {
        None => Some("concluded".into()),
        Some(v) if v.is_empty() => None,
        Some(v) => Some(
            match v.as_str() {
                "completed" => "concluded",
                "stopped" => "ended",
                other => other,
            }
            .to_string(),
        ),
    }
}

pub async fn talkgroup_activity(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<TalkgroupActivityResponse>, ApiError> {
    let now = now_ms();
    let (after_ms, before_ms) = window(&q, now - DAY_MS, now)?;
    let interval = interval(&q)?;
    let (limit, offset) = pagination(&q)?;
    let (field, _) = sort(&q, "calls", &["calls", "duration", "tgid"])?;
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = ActivityFilter {
            system_ids: system_filter(&q, &systems),
            site_ids: ids(&q, &["site_id", "sites"]),
            tgids: ids(&q, &["tgid", "tgids"]).into_iter().filter_map(|t| u32::try_from(t).ok()).collect(),
            after_ms,
            before_ms,
            state: call_state(&q),
        };
        if let Some(interval) = interval {
            let interval_ms = i64::from(interval) * 1000;
            let per_talkgroup = (before_ms - after_ms + interval_ms - 1) / interval_ms;
            let buckets = activity_talkgroups(c, &filter).map_err(StoreError::from)?.saturating_mul(per_talkgroup);
            if buckets > MAX_BUCKETS {
                return Err(ApiError::InvalidParameter(format!("too many buckets: {buckets}, at most {MAX_BUCKETS}; widen interval or narrow the window")));
            }
        }
        let mut rows = activity_rows(c, &filter).map_err(StoreError::from)?;
        match field.as_str() {
            "duration" => rows.sort_by(|a, b| b.total_duration.total_cmp(&a.total_duration).then(a.tgid.cmp(&b.tgid)).then(a.system_id.0.cmp(&b.system_id.0))),
            "tgid" => rows.sort_by_key(|r| (r.tgid, r.system_id.0)),
            _ => {}
        }
        let total = rows.len();
        let mut by_talkgroup: HashMap<(i64, u32), _> = match interval {
            Some(i) => activity_buckets(c, &filter, i).map_err(StoreError::from)?.into_iter().map(|(s, t, b)| ((s, t), b)).collect(),
            None => HashMap::new(),
        };
        let activity = rows
            .iter()
            .skip(offset as usize)
            .take(limit as usize)
            .map(|r| {
                let mut a = TalkgroupActivity::from_rows(r, systems.get(&r.system_id.0));
                if interval.is_some() {
                    let buckets = by_talkgroup.remove(&(r.system_id.0, r.tgid)).unwrap_or_default();
                    a.buckets = Some(buckets.iter().map(ActivityBucket::from_row).collect());
                }
                a
            })
            .collect();
        Ok(TalkgroupActivityResponse { activity, total })
    })
    .await?;
    Ok(Json(body))
}
