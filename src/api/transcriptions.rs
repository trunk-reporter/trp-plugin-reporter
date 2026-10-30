//! `/calls/{id}/transcription` and `/transcriptions/batch`: transcripts read through
//! from the transcribe plugin and given tr-engine's shape. Nothing is stored here; a
//! plugin that is off or unreachable means "no transcript", never an error.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use serde_json::{Value, json};

use super::calls::call_row;
use super::talkgroups::blocking;
use super::types::{Call, rfc3339};
use super::{ApiError, AppState, Query};
use crate::store::calls::call_by_id;
use crate::store::{CallId, StoreError};
use crate::transcripts::{Transcript, Utterance, is_ok, normalize_path};

fn segment(u: &Utterance) -> Value {
    json!({ "src": u.unit.unwrap_or(0), "start": u.start, "end": u.end, "text": u.text })
}

/// The transcript as tr-engine's `Transcription` object for call `call_id`.
pub fn to_engine(call_id: i64, t: &Transcript) -> Value {
    let text = t.text.clone().unwrap_or_default();
    let mut v = json!({
        "id": call_id,
        "call_id": call_id,
        "text": text,
        "source": "auto",
        "is_primary": true,
        "model": t.model,
        "provider": "transcribe",
        "word_count": text.split_whitespace().count(),
        "created_at": rfc3339(t.transcribed_ms),
        "words": { "words": [], "segments": t.segments.iter().map(segment).collect::<Vec<_>>() },
    });
    if let Some(language) = &t.language {
        v["language"] = json!(language);
    }
    v
}

/// The transcript as one entry of the batch answer.
pub fn to_batch_entry(call_id: i64, t: &Transcript) -> Value {
    json!({ "call_id": call_id, "text": t.text.clone().unwrap_or_default(), "segments": t.segments.iter().map(segment).collect::<Vec<_>>() })
}

/// Log an unreachable plugin, once a minute per `key`. Not while the relay reports the
/// plugin's stream down: it said "not reachable" once already, and an install without
/// the plugin would otherwise log an error a minute for as long as pages are browsed.
fn complain(state: &AppState, key: &'static str, err: &str) {
    if state.relay.as_ref().is_some_and(|r| !r.connected()) {
        return;
    }
    if state.throttle.allow(key) {
        (state.log)(format!("transcribe plugin: {err}"));
    }
}

/// `GET /calls/{id}/transcription`: the plugin's transcript of the call, 404 when
/// there is none (or the bridge is off, or the plugin cannot be reached).
pub async fn transcription(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let (call_id, path) = blocking(&state, move |c| {
        let row = call_row(c, &id)?;
        Ok((row.id.0, row.path))
    })
    .await?;
    let none = || ApiError::NotFound(format!("call {call_id} has no transcription"));
    let (Some(client), Some(path)) = (state.transcripts.clone(), path) else { return Err(none()) };
    match client.one(&path).await {
        Ok(Some(t)) if is_ok(&t) => Ok(Json(to_engine(call_id, &t))),
        Ok(_) => Err(none()),
        Err(e) => {
            complain(&state, "transcription", &e);
            Err(none())
        }
    }
}

/// `GET /transcriptions/batch?call_ids=…`: the transcripts of up to 500 calls, in the
/// order asked; calls without one are left out.
pub async fn transcriptions_batch(State(state): State<AppState>, QueryParams(q): QueryParams<Query>) -> Result<Json<Value>, ApiError> {
    let raw = q.get("call_ids").map(|s| s.trim()).filter(|s| !s.is_empty()).ok_or_else(|| ApiError::InvalidParameter("call_ids is required".into()))?;
    let mut ids = Vec::new();
    for part in raw.split(',') {
        ids.push(part.trim().parse::<u64>().map_err(|_| ApiError::InvalidParameter(format!("call_ids must be call ids, not {part}")))?);
    }
    if ids.len() > 500 {
        return Err(ApiError::InvalidParameter(format!("call_ids takes at most 500 ids, not {}", ids.len())));
    }
    let empty = || Json(json!({ "transcriptions": [] }));
    let Some(client) = state.transcripts.clone() else { return Ok(empty()) };
    // (call id, path) of each id that is a stored call with a path, in the order asked.
    let calls: Vec<(i64, String)> = blocking(&state, move |c| {
        let mut found = Vec::new();
        for id in ids {
            let Ok(id) = i64::try_from(id) else { continue };
            if let Some(row) = call_by_id(c, CallId(id)).map_err(StoreError::from)? {
                if let Some(path) = row.path {
                    found.push((id, path));
                }
            }
        }
        Ok(found)
    })
    .await?;
    let paths: Vec<String> = calls.iter().map(|(_, p)| p.clone()).collect();
    let transcripts = match client.batch(&paths).await {
        Ok(t) => t,
        Err(e) => {
            complain(&state, "batch", &e);
            return Ok(empty());
        }
    };
    let by_path: HashMap<String, &Transcript> = transcripts.iter().filter(|t| is_ok(t)).map(|t| (normalize_path(&t.path), t)).collect();
    let entries: Vec<Value> = calls.iter().filter_map(|(id, path)| by_path.get(&normalize_path(path)).map(|t| to_batch_entry(*id, t))).collect();
    Ok(Json(json!({ "transcriptions": entries })))
}

/// Say on each call whether the transcribe plugin has an ok transcript of it, with the
/// text for the snippet. One batch request for the whole list; a bridge that is off, or a
/// plugin that cannot be reached, leaves the calls as they were.
pub async fn fill_transcripts(state: &AppState, calls: &mut [Call]) {
    let Some(client) = state.transcripts.clone() else { return };
    let mut paths: Vec<String> = calls.iter().filter_map(|c| c.path.as_deref()).map(normalize_path).collect();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return;
    }
    let transcripts = match client.batch(&paths).await {
        Ok(t) => t,
        Err(e) => {
            complain(state, "batch", &e);
            return;
        }
    };
    let by_path: HashMap<String, &Transcript> = transcripts.iter().filter(|t| is_ok(t)).map(|t| (normalize_path(&t.path), t)).collect();
    for call in calls {
        if let Some(t) = call.path.as_deref().and_then(|p| by_path.get(&normalize_path(p))) {
            call.has_transcription = true;
            call.transcription_text = t.text.clone();
        }
    }
}
