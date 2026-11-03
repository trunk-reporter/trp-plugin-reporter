//! `/tone-patterns` (Reporter's own, not in tr-engine's spec): the tone patterns
//! calls carried, and the `tone_pattern` event's payload.

use axum::Json;
use axum::extract::{Query as QueryParams, State};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::talkgroups::{blocking, system_filter, systems};
use super::{ApiError, AppState, Query, ids, pagination, rfc3339, sort, time_range};
use crate::store::StoreError;
use crate::store::tones::{ToneFilter, ToneRow, list_tones};

/// What a pattern is said with, beside the pattern itself (as the call's record has it).
#[derive(Clone, Debug, Default)]
pub struct ToneContext {
    /// Its row (`None`: told live, not stored yet).
    pub id: Option<i64>,
    /// The call it was on (`None`: one this plugin hasn't stored).
    pub call_id: Option<i64>,
    pub system_id: i64,
    pub system_name: String,
    pub tgid: u32,
    pub tg_alpha_tag: String,
    /// The radio talking when it began.
    pub unit_id: Option<u32>,
    pub unit_alpha_tag: String,
    pub time_ms: i64,
}

/// The pattern's fields (`pattern`, `tones`, `lengths`, `text`, `signals`, …)
/// with the context's beside them.
pub fn tone_value(pattern: &Map<String, Value>, cx: &ToneContext) -> Value {
    let mut m = pattern.clone();
    let mut put = |k: &str, v: Value| {
        m.insert(k.into(), v);
    };
    if let Some(id) = cx.id {
        put("id", json!(id));
    }
    put("call_id", json!(cx.call_id));
    put("system_id", json!(cx.system_id));
    put("system_name", json!(cx.system_name));
    put("tgid", json!(cx.tgid));
    if !cx.tg_alpha_tag.is_empty() {
        put("tg_alpha_tag", json!(cx.tg_alpha_tag));
    }
    if let Some(u) = cx.unit_id {
        put("unit_id", json!(u));
    }
    if !cx.unit_alpha_tag.is_empty() {
        put("unit_alpha_tag", json!(cx.unit_alpha_tag));
    }
    put("time", json!(rfc3339(cx.time_ms)));
    Value::Object(m)
}

fn from_row(r: &ToneRow) -> Value {
    let pattern: Map<String, Value> = serde_json::from_str(&r.json).unwrap_or_default();
    let cx = ToneContext {
        id: Some(r.id),
        call_id: Some(r.call_id.0),
        system_id: r.system_id.0,
        system_name: if r.system_name.is_empty() { r.system_short_name.clone() } else { r.system_name.clone() },
        tgid: r.tgid,
        tg_alpha_tag: r.tg_alpha_tag.clone(),
        unit_id: r.unit,
        unit_alpha_tag: r.unit_alpha_tag.clone(),
        time_ms: r.time_ms,
    };
    tone_value(&pattern, &cx)
}

#[derive(Serialize)]
pub struct ToneListResponse {
    pub tone_patterns: Vec<Value>,
    pub total: i64,
    pub limit: u32,
    pub offset: u32,
}

/// `/tone-patterns`: newest first unless `sort=time`; filtered by system,
/// talkgroup, unit, call, pattern (`two_tone,digits`) and time.
pub async fn list(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<ToneListResponse>, ApiError> {
    let (limit, offset) = pagination(&q)?;
    let (_, desc) = sort(&q, "-time", &["time"])?;
    let (start_ms, end_ms) = time_range(&q)?;
    let id_list = |names: &[&str]| ids(&q, names).into_iter().filter_map(|i| u32::try_from(i).ok()).collect::<Vec<_>>();
    let (tgids, units) = (id_list(&["tgid", "tgids"]), id_list(&["unit_id", "units", "unit_ids"]));
    let call_ids = ids(&q, &["call_id", "call_ids"]);
    let patterns: Vec<String> = ["pattern", "patterns"]
        .iter()
        .filter_map(|n| q.get(*n))
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(String::from)
        .collect();
    let body = blocking(&state, move |c| {
        let systems = systems(c)?;
        let filter = ToneFilter { system_ids: system_filter(&q, &systems), tgids, units, call_ids, patterns, start_ms, end_ms, desc, limit, offset };
        let (rows, total) = list_tones(c, &filter).map_err(StoreError::from)?;
        Ok(ToneListResponse { tone_patterns: rows.iter().map(from_row).collect(), total, limit, offset })
    })
    .await?;
    Ok(Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pattern_keeps_its_fields_beside_its_context() {
        let pattern: Map<String, Value> =
            serde_json::from_value(json!({"pattern": "two_tone", "tones": [849.6, 1292.8], "src": 0, "text": "two-tone"})).unwrap();
        let cx = ToneContext { call_id: Some(7), system_id: 1, system_name: "County".into(), tgid: 101, time_ms: 1_700_000_000_500, ..Default::default() };
        let v = tone_value(&pattern, &cx);
        assert_eq!(v["pattern"], "two_tone");
        assert_eq!(v["tones"][0], 849.6);
        assert_eq!((v["call_id"].as_i64(), v["tgid"].as_u64(), v["time"].as_str()), (Some(7), Some(101), Some("2023-11-14T22:13:20.500Z")));
        // Nothing unknown is invented.
        assert!(v.get("id").is_none() && v.get("unit_id").is_none() && v.get("tg_alpha_tag").is_none());
    }
}
