//! `/units`, `/units/{id}`, `/units/{id}/events`, `/unit-events` and `/unit-affiliations`.

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use rusqlite::Connection;

use super::talkgroups::{Systems, blocking, system_filter, systems};
use super::types::{
    Affiliation, AffiliationListResponse, AffiliationSummary, Unit, UnitEvent, UnitEventListResponse, UnitListResponse, UnitTalkgroup,
    UnitTalkgroupListResponse,
};
use super::{ApiError, AppState, Query, composite, flag, ids, pagination, sort, time_range, window};
use crate::affiliations::AffiliationFilter;
use crate::store::talkgroups::tg_alpha_tag;
use crate::store::units::{EventSort, UnitEventFilter, UnitFilter, UnitRow, UnitSort, list_unit_events, list_units, talkgroups_of_unit, unit, units_by_id};
use crate::store::{StoreError, SystemId, now_ms};

const SECOND_MS: i64 = 1000;
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;

/// The spec's `EventType`.
const EVENT_TYPES: &[&str] = &["on", "off", "join", "call", "end", "data", "ans_req", "location", "ackresp", "signal", "call_alert"];

/// A unit by `system_id:unit_id` / `system_id-unit_id` or by a plain unit id (409 when several systems have it).
pub fn resolve_unit(conn: &Connection, id: &str) -> Result<UnitRow, ApiError> {
    let not_found = || ApiError::NotFound(format!("unit {id} not found"));
    match composite(id)? {
        (Some(system), unit_id) => unit(conn, SystemId(system), unit_id).map_err(StoreError::from)?.ok_or_else(not_found),
        (None, unit_id) => {
            let mut rows = units_by_id(conn, unit_id).map_err(StoreError::from)?;
            match rows.len() {
                0 => Err(not_found()),
                1 => Ok(rows.remove(0)),
                _ => {
                    let systems = systems(conn)?;
                    let matches = rows
                        .iter()
                        .map(|r| {
                            let u = Unit::from_rows(r, systems.get(&r.system_id.0), None, None);
                            (u.system_id, u.system_name, u.sysid)
                        })
                        .collect();
                    Err(ApiError::Ambiguous { what: "unit", id: unit_id, matches })
                }
            }
        }
    }
}

fn unit_json(c: &Connection, systems: &Systems, row: &UnitRow) -> Result<Unit, ApiError> {
    let tag = match row.last_event_tgid {
        Some(t) => tg_alpha_tag(c, row.system_id, t).map_err(StoreError::from)?,
        None => None,
    };
    Ok(Unit::from_rows(row, systems.get(&row.system_id.0), tag, None))
}

fn id_list(q: &Query, names: &[&str]) -> Vec<u32> {
    ids(q, names).into_iter().filter_map(|i| u32::try_from(i).ok()).collect()
}

/// `name` as a whole number (times `unit_ms`), or `None` when absent.
fn duration_ms(q: &Query, name: &str, unit_ms: i64) -> Result<Option<i64>, ApiError> {
    match q.get(name).map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0)
            .and_then(|n| n.checked_mul(unit_ms))
            .map(Some)
            .ok_or_else(|| ApiError::InvalidParameter(format!("{name} must be 0 or more, not {v}"))),
    }
}

/// The comma list of event types in `type`, each one of the spec's.
fn event_types(q: &Query) -> Result<Vec<String>, ApiError> {
    let mut out = Vec::new();
    for t in q.get("type").into_iter().flat_map(|v| v.split(',')).map(str::trim).filter(|t| !t.is_empty()) {
        if !EVENT_TYPES.contains(&t) {
            return Err(ApiError::InvalidParameter(format!("type must be one of {}, not {t}", EVENT_TYPES.join(", "))));
        }
        out.push(t.to_string());
    }
    Ok(out)
}

pub async fn list(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<UnitListResponse>, ApiError> {
    let (limit, offset) = pagination(&q)?;
    let (field, desc) = sort(&q, "unit_id", &["alpha_tag", "unit_id", "last_seen", "last_event_time"])?;
    let sort = match field.as_str() {
        "alpha_tag" => UnitSort::AlphaTag,
        "last_seen" => UnitSort::LastSeen,
        "last_event_time" => UnitSort::LastEventTime,
        _ => UnitSort::UnitId,
    };
    let active_within_ms = duration_ms(&q, "active_within", MINUTE_MS)?;
    let search = q.get("search").map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let tgids = id_list(&q, &["talkgroup"]);
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = UnitFilter { system_ids: system_filter(&q, &systems), search, active_within_ms, tgids, sort, desc, limit, offset };
        let (rows, total) = list_units(c, &filter, now).map_err(StoreError::from)?;
        let units = rows.iter().map(|r| unit_json(c, &systems, r)).collect::<Result<Vec<_>, _>>()?;
        Ok(UnitListResponse { units, total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

pub async fn one(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Unit>, ApiError> {
    let body = blocking(&state, move |c| {
        let row = resolve_unit(c, &id)?;
        unit_json(c, &systems(c)?, &row)
    })
    .await?;
    Ok(Json(body))
}

/// `/units/{id}/events`: one unit's events, any time range.
pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    QueryParams(q): QueryParams<Query>,
) -> Result<Json<UnitEventListResponse>, ApiError> {
    let (limit, offset) = pagination(&q)?;
    let (start_ms, end_ms) = time_range(&q)?;
    let types = event_types(&q)?;
    let tgids = id_list(&q, &["talkgroup"]);
    let instance = state.host.snapshot().name;
    let body = blocking(&state, move |c| {
        let row = resolve_unit(c, &id)?;
        let filter = UnitEventFilter {
            system_ids: vec![row.system_id.0],
            unit_ids: vec![row.unit_id],
            tgids,
            types,
            start_ms,
            end_ms,
            limit,
            offset,
            ..Default::default()
        };
        let (rows, total) = list_unit_events(c, &filter).map_err(StoreError::from)?;
        Ok(UnitEventListResponse { events: rows.iter().map(|r| UnitEvent::from_row(r, &instance)).collect(), total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

/// `/units/{id}/talkgroups`: the talkgroups a unit talked on in `[after, before)` (default: the last 24 hours).
pub async fn talkgroups(
    State(state): State<AppState>,
    Path(id): Path<String>,
    QueryParams(q): QueryParams<Query>,
) -> Result<Json<UnitTalkgroupListResponse>, ApiError> {
    let now = now_ms();
    let (after, before) = window(&q, now - 24 * HOUR_MS, now)?;
    let body = blocking(&state, move |c| {
        let row = resolve_unit(c, &id)?;
        let rows = talkgroups_of_unit(c, row.system_id, row.unit_id, after, before).map_err(StoreError::from)?;
        Ok(UnitTalkgroupListResponse { unit_id: row.unit_id, system_id: row.system_id.0, talkgroups: rows.iter().map(UnitTalkgroup::from_row).collect() })
    })
    .await?;
    Ok(Json(body))
}

/// `/unit-events`: a system's events; the last hour unless a range is given, at most 24 hours.
pub async fn unit_events(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<UnitEventListResponse>, ApiError> {
    if !["system_id", "systems", "sysid", "sysids"].iter().any(|n| q.contains_key(*n)) {
        return Err(ApiError::InvalidParameter("system_id or sysid is required".into()));
    }
    let (limit, offset) = pagination(&q)?;
    let (field, desc) = sort(&q, "-time", &["time", "unit_rid", "tgid", "event_type"])?;
    let sort = match field.as_str() {
        "unit_rid" => EventSort::Unit,
        "tgid" => EventSort::Tgid,
        "event_type" => EventSort::EventType,
        _ => EventSort::Time,
    };
    let (start, end) = time_range(&q)?;
    let end_ms = end.unwrap_or_else(now_ms);
    let start_ms = start.unwrap_or(end_ms - HOUR_MS);
    if end_ms - start_ms > 24 * HOUR_MS {
        return Err(ApiError::InvalidTimeRange("the time range is at most 24 hours".into()));
    }
    let types = event_types(&q)?;
    let emergency = flag(&q, "emergency")?;
    let unit_ids = id_list(&q, &["unit_id", "units", "unit_ids"]);
    let tgids = id_list(&q, &["tgid", "tgids"]);
    let instance = state.host.snapshot().name;
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = UnitEventFilter {
            system_ids: system_filter(&q, &systems),
            unit_ids,
            tgids,
            types,
            emergency,
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
            sort,
            desc,
            limit,
            offset,
        };
        let (rows, total) = list_unit_events(c, &filter).map_err(StoreError::from)?;
        Ok(UnitEventListResponse { events: rows.iter().map(|r| UnitEvent::from_row(r, &instance)).collect(), total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

/// `/unit-affiliations`: the live map.
pub async fn affiliations(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<AffiliationListResponse>, ApiError> {
    let (limit, offset) = pagination(&q)?;
    let status = match q.get("status").map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => None,
        Some(s @ ("affiliated" | "off")) => Some(s.to_string()),
        Some(s) => return Err(ApiError::InvalidParameter(format!("status must be affiliated or off, not {s}"))),
    };
    let stale_threshold_ms = duration_ms(&q, "stale_threshold", SECOND_MS)?;
    let active_within_ms = duration_ms(&q, "active_within", SECOND_MS)?;
    let tgids = id_list(&q, &["tgid", "tgids"]);
    let unit_ids = id_list(&q, &["unit_id", "units", "unit_ids"]);
    let live = state.affiliations.clone();
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = AffiliationFilter {
            system_ids: system_filter(&q, &systems),
            tgids,
            unit_ids,
            status,
            stale_threshold_ms,
            active_within_ms,
            limit: limit as usize,
            offset: offset as usize,
        };
        let (page, total, talkgroup_counts) = live.snapshot(&filter, now);
        let affiliations = page.iter().map(|e| Affiliation::from_entry(e, systems.get(&e.system_id))).collect();
        Ok(AffiliationListResponse { affiliations, total, limit, offset, summary: AffiliationSummary { talkgroup_counts } })
    })
    .await?;
    Ok(Json(body))
}
