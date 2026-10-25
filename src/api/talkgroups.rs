//! `/talkgroups`, `/talkgroups/{id}`, `/talkgroups/{id}/units` and `/talkgroups/encryption-stats`.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use rusqlite::Connection;

use super::types::{EncryptionStat, EncryptionStatsResponse, Talkgroup, TalkgroupListResponse, Unit, UnitListResponse};
use super::{ApiError, AppState, Query, composite, ids, pagination, sort, sysids};
use crate::store::systems::{SystemRow, list_systems};
use crate::store::talkgroups::{
    TalkgroupFilter, TalkgroupRow, TgSort, encryption_stats as encryption_rows, list_talkgroups, talkgroup, talkgroups_by_tgid, tg_alpha_tag,
    units_on_talkgroup,
};
use crate::store::{StoreError, SystemId, now_ms};

const MINUTE_MS: i64 = 60_000;

type Systems = HashMap<i64, SystemRow>;

fn systems(conn: &Connection) -> Result<Systems, StoreError> {
    Ok(list_systems(conn)?.into_iter().map(|s| (s.id.0, s)).collect())
}

/// The systems the `system_id` and `sysid` (hex) filters name; empty when no filter is given.
/// A filter that names no system yields `[-1]`, which matches nothing.
fn system_filter(q: &Query, systems: &Systems) -> Vec<i64> {
    let by_id = ids(q, &["system_id", "systems"]);
    let by_sysid = sysids(q);
    let given = !by_id.is_empty() || !by_sysid.is_empty() || ["system_id", "systems", "sysid", "sysids"].iter().any(|n| q.contains_key(*n));
    if !given {
        return vec![];
    }
    let mut out = by_id;
    out.extend(systems.values().filter(|s| s.sysid.is_some_and(|x| by_sysid.contains(&x))).map(|s| s.id.0));
    out.sort_unstable();
    out.dedup();
    if out.is_empty() { vec![-1] } else { out }
}

/// A talkgroup by `system_id:tgid` / `system_id-tgid` or by a plain tgid (409 when several systems have it).
pub fn resolve_talkgroup(conn: &Connection, id: &str, now_ms: i64) -> Result<TalkgroupRow, ApiError> {
    let not_found = || ApiError::NotFound(format!("talkgroup {id} not found"));
    match composite(id)? {
        (Some(system), tgid) => talkgroup(conn, SystemId(system), tgid, now_ms).map_err(StoreError::from)?.ok_or_else(not_found),
        (None, tgid) => {
            let mut rows = talkgroups_by_tgid(conn, tgid, now_ms).map_err(StoreError::from)?;
            match rows.len() {
                0 => Err(not_found()),
                1 => Ok(rows.remove(0)),
                _ => {
                    let systems = systems(conn)?;
                    let matches = rows
                        .iter()
                        .map(|r| {
                            let t = Talkgroup::from_rows(r, systems.get(&r.system_id.0));
                            (t.system_id, t.system_name, t.sysid)
                        })
                        .collect();
                    Err(ApiError::Ambiguous { what: "talkgroup", id: tgid, matches })
                }
            }
        }
    }
}

async fn blocking<T: Send + 'static>(state: &AppState, f: impl FnOnce(&Connection) -> Result<T, ApiError> + Send + 'static) -> Result<T, ApiError> {
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || {
        let mut result = None;
        store.read(|c| {
            result = Some(f(c));
            Ok(())
        })?;
        result.unwrap_or_else(|| Err(ApiError::Internal("no result".into())))
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?
}

pub async fn list(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<TalkgroupListResponse>, ApiError> {
    if q.contains_key("stats_days") {
        return Err(ApiError::InvalidParameter("stats_days is not supported".into()));
    }
    let (limit, offset) = pagination(&q)?;
    let (field, desc) = sort(&q, "alpha_tag", &["alpha_tag", "tgid", "group", "last_seen", "call_count", "calls_1h", "calls_24h", "unit_count"])?;
    let sort = match field.as_str() {
        "tgid" => TgSort::Tgid,
        "group" => TgSort::Group,
        "last_seen" => TgSort::LastSeen,
        "call_count" => TgSort::CallCount,
        "calls_1h" => TgSort::Calls1h,
        "calls_24h" => TgSort::Calls24h,
        "unit_count" => TgSort::UnitCount,
        _ => TgSort::AlphaTag,
    };
    let text = |name: &str| q.get(name).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let (group, search) = (text("group"), text("search"));
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = TalkgroupFilter { system_ids: system_filter(&q, &systems), group, search, sort, desc, limit, offset };
        let (rows, total) = list_talkgroups(c, &filter, now).map_err(StoreError::from)?;
        let talkgroups = rows.iter().map(|r| Talkgroup::from_rows(r, systems.get(&r.system_id.0))).collect();
        Ok(TalkgroupListResponse { talkgroups, total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

pub async fn one(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Talkgroup>, ApiError> {
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let row = resolve_talkgroup(c, &id, now)?;
        let systems = systems(c)?;
        Ok(Talkgroup::from_rows(&row, systems.get(&row.system_id.0)))
    })
    .await?;
    Ok(Json(body))
}

pub async fn units(State(state): State<AppState>, Path(id): Path<String>, QueryParams(q): QueryParams<Query>) -> Result<Json<UnitListResponse>, ApiError> {
    let (limit, offset) = pagination(&q)?;
    let window = match q.get("window").map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => 60,
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|w| (1..=1440).contains(w))
            .ok_or_else(|| ApiError::InvalidParameter(format!("window must be 1 to 1440 minutes, not {v}")))?,
    };
    let now = now_ms();
    let body = blocking(&state, move |c| {
        let tg = resolve_talkgroup(c, &id, now)?;
        let (rows, total) = units_on_talkgroup(c, tg.system_id, tg.tgid, now - window * MINUTE_MS, limit, offset).map_err(StoreError::from)?;
        let systems = systems(c)?;
        let units = rows
            .iter()
            .map(|(u, calls)| {
                let tag = match u.last_event_tgid {
                    Some(t) => tg_alpha_tag(c, u.system_id, t).map_err(StoreError::from)?,
                    None => None,
                };
                Ok(Unit::from_rows(u, systems.get(&u.system_id.0), tag, Some(*calls)))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(UnitListResponse { units, total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

pub async fn encryption_stats(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<EncryptionStatsResponse>, ApiError> {
    let hours = match q.get("hours").map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => 24,
        Some(v) => v.parse::<i64>().ok().filter(|h| *h >= 1).ok_or_else(|| ApiError::InvalidParameter(format!("hours must be 1 or more, not {v}")))?,
    };
    let since = now_ms().saturating_sub(hours.saturating_mul(60 * MINUTE_MS));
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let rows = encryption_rows(c, since, &system_filter(&q, &systems)).map_err(StoreError::from)?;
        let stats: Vec<EncryptionStat> = rows.iter().map(|r| EncryptionStat::from_rows(r, systems.get(&r.system_id.0))).collect();
        Ok(EncryptionStatsResponse { total: stats.len(), stats, hours })
    })
    .await?;
    Ok(Json(body))
}
