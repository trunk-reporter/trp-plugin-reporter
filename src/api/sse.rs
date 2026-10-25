//! `/events/stream`: the live event feed as Server-Sent Events.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query as Q, State};
use axum::http::header::{CACHE_CONTROL, HeaderName};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::IntoResponse;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use futures_util::StreamExt;
use futures_util::stream;

use super::{AppState, Query, param};
use crate::bus::{Event, Filter};

/// How often an idle stream gets a `: keepalive` comment.
pub const KEEPALIVE: Duration = Duration::from_secs(15);

/// The sequence number of an event id (`"{unix_ms}-{seq}"`).
fn seq(id: &str) -> Option<u64> {
    id.rsplit_once('-').and_then(|(_, s)| s.parse().ok())
}

fn frame(e: &Event) -> SseEvent {
    SseEvent::default().id(&e.id).event(e.kind.as_str()).data(&e.data)
}

pub async fn stream(State(state): State<AppState>, headers: HeaderMap, Q(q): Q<Query>) -> impl IntoResponse {
    let filter = Filter::parse(&q);
    let last_id = headers.get("last-event-id").and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty()).or_else(|| param(&q, "last_event_id"));
    // Subscribe before replaying so nothing published in between is lost; the
    // overlap is dropped below by sequence number.
    let sub = state.bus.subscribe(filter.clone());
    let replayed = last_id.map(|id| state.bus.replay_since(id, &filter)).unwrap_or_default();
    let floor = replayed.last().and_then(|e| seq(&e.id));
    let live = stream::unfold(sub, |mut sub| async move { sub.rx.recv().await.map(|e| (e, sub)) })
        .filter(move |e| std::future::ready(floor.is_none_or(|f| seq(&e.id).is_none_or(|s| s > f))));
    let events = stream::iter(replayed).chain(live).map(|e| Ok::<_, Infallible>(frame(&e)));
    (
        [(CACHE_CONTROL, HeaderValue::from_static("no-cache")), (HeaderName::from_static("x-accel-buffering"), HeaderValue::from_static("no"))],
        Sse::new(events).keep_alive(KeepAlive::new().interval(state.keepalive).text("keepalive")),
    )
}
