//! `/preferences/{key}`: a JSON object per key, kept for the web app's settings.

use std::sync::LazyLock;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use regex::Regex;
use serde_json::Value;

use super::talkgroups::blocking;
use super::types::Preference;
use super::{ApiError, AppState};
use crate::store::now_ms;
use crate::store::preferences::{get_preference, put_preference};

/// The largest document a PUT may carry.
const MAX_BYTES: usize = 256 * 1024;

static KEY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z0-9][a-z0-9-]{0,63}$").expect("regex"));

fn check_key(key: &str) -> Result<(), ApiError> {
    if KEY.is_match(key) {
        Ok(())
    } else {
        Err(ApiError::BadRequest("key must be 1 to 64 characters: lowercase letters, digits and hyphens, not starting with a hyphen".into()))
    }
}

fn preference(key: String, json: &str, updated_ms: i64) -> Result<Preference, ApiError> {
    let value = serde_json::from_str(json).map_err(|_| ApiError::Internal(format!("preference {key} holds invalid JSON")))?;
    Ok(Preference { key, value, updated_at: super::rfc3339(updated_ms) })
}

pub async fn get(State(state): State<AppState>, Path(key): Path<String>) -> Result<Json<Preference>, ApiError> {
    check_key(&key)?;
    let body = blocking(&state, move |c| {
        let (json, at) =
            get_preference(c, &key).map_err(crate::store::StoreError::from)?.ok_or_else(|| ApiError::NotFound(format!("preference {key} not found")))?;
        preference(key, &json, at)
    })
    .await?;
    Ok(Json(body))
}

pub async fn put(State(state): State<AppState>, Path(key): Path<String>, body: Bytes) -> Result<Json<Preference>, ApiError> {
    check_key(&key)?;
    if body.len() > MAX_BYTES {
        return Err(ApiError::BadRequest(format!("the body must be at most {MAX_BYTES} bytes")));
    }
    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(v @ Value::Object(_)) => v,
        _ => return Err(ApiError::BadRequest("the body must be a JSON object".into())),
    };
    let json = value.to_string();
    let now = now_ms();
    let stored = json.clone();
    let k = key.clone();
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || store.write_sync(move |c| put_preference(c, &k, &stored, now)))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(preference(key, &json, now)?))
}
