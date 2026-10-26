//! `/stats`, `/stats/rates`, `/recorders` and `/first-heard`.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query as QueryParams, State};

use super::talkgroups::{blocking, system_filter, systems};
use super::types::{
    ActivityBucket, BaselineEntry, BaselineResponse, DecodeRate, DecodeRatesResponse, FirstHeardResponse, Recorder, RecorderListResponse, StatsResponse,
    SystemActivity, Talkgroup, TalkgroupActivity, TalkgroupActivityResponse, Unit,
};
use super::{ApiError, AppState, Query, ids, pagination, param, rfc3339, sort, time_param, time_range, window};
use crate::store::recorder::{decode_rates, first_state_ms, latest_recorder_state};
use crate::store::stats::{
    ActivityFilter, CallStates, activity_buckets, baseline as baseline_rows, stats as stats_row, system_activity, talkgroup_activity as activity_rows,
};
use crate::store::systems::{site_by_id, system_by_id};
use crate::store::talkgroups::{first_heard_talkgroups, tg_alpha_tag};
use crate::store::units::first_heard_units;
use crate::store::{StoreError, now_ms};

const DEFAULT_RATES: u32 = 1000;
const MAX_RATES: u32 = 10_000;

/// The bucket sizes `/stats/talkgroup-activity` offers, in seconds.
const INTERVALS: [u32; 6] = [10, 30, 60, 300, 900, 3600];
/// The most buckets one `/stats/talkgroup-activity` answer may hold: the page's talkgroups times the window's length
/// in intervals.
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

/// `call_state` as `calls.state` holds it: `COMPLETED` (the default) is a finished call, whether recorded
/// (`concluded`) or not (`ended`, as an encrypted call stays); `STOPPED` is `ended`; an empty value is any state.
fn call_state(q: &Query) -> CallStates {
    match q.get("call_state").map(|v| v.trim().to_ascii_lowercase()) {
        None => CallStates::Finished,
        Some(v) if v.is_empty() => CallStates::Any,
        Some(v) => match v.as_str() {
            "completed" => CallStates::Finished,
            "stopped" => CallStates::Is("ended".into()),
            other => CallStates::Is(other.to_string()),
        },
    }
}

/// Refuses an answer of more than [`MAX_BUCKETS`] buckets.
fn bucket_cap(buckets: i64) -> Result<(), ApiError> {
    if buckets > MAX_BUCKETS {
        return Err(ApiError::InvalidParameter(format!(
            "too many buckets: {buckets}, at most {MAX_BUCKETS}; widen interval, narrow the window or lower limit"
        )));
    }
    Ok(())
}

pub async fn talkgroup_activity(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<TalkgroupActivityResponse>, ApiError> {
    let now = now_ms();
    let (after_ms, before_ms) = window(&q, now - DAY_MS, now)?;
    let interval = interval(&q)?;
    let (limit, offset) = pagination(&q)?;
    let (field, _) = sort(&q, "calls", &["calls", "duration", "tgid"])?;
    // Buckets per talkgroup; too many for even one talkgroup is refused before the store is asked.
    let per_talkgroup = interval.map(|i| {
        let interval_ms = i64::from(i) * 1000;
        (before_ms - after_ms + interval_ms - 1) / interval_ms
    });
    if let Some(n) = per_talkgroup {
        bucket_cap(n)?;
    }
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
        let mut rows = activity_rows(c, &filter).map_err(StoreError::from)?;
        match field.as_str() {
            "duration" => rows.sort_by(|a, b| b.total_duration.total_cmp(&a.total_duration).then(a.tgid.cmp(&b.tgid)).then(a.system_id.0.cmp(&b.system_id.0))),
            "tgid" => rows.sort_by_key(|r| (r.tgid, r.system_id.0)),
            _ => {}
        }
        let total = rows.len();
        let start = (offset as usize).min(total);
        let page = &rows[start..total.min(start.saturating_add(limit as usize))];
        // The page's talkgroups only, and only as many buckets as the cap allows.
        let mut by_talkgroup: HashMap<(i64, u32), _> = match (interval, per_talkgroup) {
            (Some(i), Some(n)) => {
                bucket_cap((page.len() as i64).saturating_mul(n))?;
                let keys: Vec<(i64, u32)> = page.iter().map(|r| (r.system_id.0, r.tgid)).collect();
                activity_buckets(c, &filter, i, &keys).map_err(StoreError::from)?.into_iter().map(|(s, t, b)| ((s, t), b)).collect()
            }
            _ => HashMap::new(),
        };
        let activity = page
            .iter()
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

/// An integer query parameter within `range`, or `default` when absent.
fn bounded(q: &Query, name: &str, default: u32, range: std::ops::RangeInclusive<u32>) -> Result<u32, ApiError> {
    match param(q, name) {
        None => Ok(default),
        Some(v) => v
            .parse::<u32>()
            .ok()
            .filter(|n| range.contains(n))
            .ok_or_else(|| ApiError::InvalidParameter(format!("{name} must be {} to {}, not {v}", range.start(), range.end()))),
    }
}

/// `GET /stats/baseline`: the talkgroups active in the trailing `window` seconds, each against the mean of the same
/// window on the previous `days` days.
pub async fn baseline(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<BaselineResponse>, ApiError> {
    let now = now_ms();
    let window_seconds = bounded(&q, "window", 600, 60..=86_400)?;
    let days = bounded(&q, "days", 7, 1..=30)?;
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let rows = baseline_rows(c, &system_filter(&q, &systems), now, window_seconds, days).map_err(StoreError::from)?;
        Ok(BaselineResponse { window_seconds, days, now: rfc3339(now), talkgroups: rows.iter().map(BaselineEntry::from).collect() })
    })
    .await?;
    Ok(Json(body))
}

const DEFAULT_FIRST_HEARD: u32 = 50;
const MAX_FIRST_HEARD: u32 = 500;

/// `GET /first-heard`: the talkgroups and radios first heard since `since` (default: 24 hours ago), newest first.
pub async fn first_heard(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<FirstHeardResponse>, ApiError> {
    let now = now_ms();
    let since = time_param(&q, "since")?.unwrap_or(now - DAY_MS);
    let limit = match param(&q, "limit") {
        None => DEFAULT_FIRST_HEARD,
        Some(v) => v
            .parse::<u32>()
            .ok()
            .filter(|l| *l >= 1)
            .map(|l| l.min(MAX_FIRST_HEARD))
            .ok_or_else(|| ApiError::InvalidParameter(format!("limit must be 1 or more, not {v}")))?,
    };
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let system_ids = system_filter(&q, &systems);
        let talkgroups = first_heard_talkgroups(c, &system_ids, since, limit, now)
            .map_err(StoreError::from)?
            .iter()
            .map(|r| Talkgroup::from_rows(r, systems.get(&r.system_id.0)))
            .collect();
        let units = first_heard_units(c, &system_ids, since, limit)
            .map_err(StoreError::from)?
            .iter()
            .map(|r| {
                let tag = match r.last_event_tgid {
                    Some(t) => tg_alpha_tag(c, r.system_id, t).map_err(StoreError::from)?,
                    None => None,
                };
                Ok(Unit::from_rows(r, systems.get(&r.system_id.0), tag, None))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(FirstHeardResponse { since: rfc3339(since), talkgroups, units })
    })
    .await?;
    Ok(Json(body))
}
