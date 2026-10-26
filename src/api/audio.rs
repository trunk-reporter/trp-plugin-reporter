//! `/audio/live`: the recorder's live audio over a WebSocket, as tr-engine sends it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use super::{ApiError, AppState};
use crate::audio::{AudioBus, AudioFrame, frame_bytes};

/// How often a client gets `{"type":"keepalive",...}`.
pub const KEEPALIVE: Duration = Duration::from_secs(15);
/// Clients at once; one more is a 503.
pub const MAX_CLIENTS: usize = 16;
/// The longest one message may take to go out. A client whose socket stays full that long (a peer that
/// stopped reading but never closed) is dropped, so it cannot hold one of the [`MAX_CLIENTS`] places for good.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Which frames a client hears.
#[derive(Clone, Debug, PartialEq)]
enum Filter {
    /// After `unsubscribe`: none.
    Off,
    /// An empty list matches every value; both lists given, both must match.
    On { tgids: Vec<u32>, systems: Vec<i64> },
}

impl Filter {
    /// A client that has said nothing hears everything, as in tr-engine.
    fn all() -> Filter {
        Filter::On { tgids: Vec::new(), systems: Vec::new() }
    }

    /// A client's text message; `None` for anything but `subscribe` / `unsubscribe`.
    fn parse(text: &str) -> Option<Filter> {
        let v: Value = serde_json::from_str(text).ok()?;
        let numbers = |key: &str| -> Vec<i64> { v[key].as_array().map(|a| a.iter().filter_map(Value::as_i64).collect()).unwrap_or_default() };
        match v["type"].as_str()? {
            "subscribe" => {
                Some(Filter::On { tgids: numbers("tgids").into_iter().filter_map(|t| u32::try_from(t).ok()).collect(), systems: numbers("systems") })
            }
            "unsubscribe" => Some(Filter::Off),
            _ => None,
        }
    }

    fn matches(&self, f: &AudioFrame) -> bool {
        match self {
            Filter::Off => false,
            Filter::On { tgids, systems } => (tgids.is_empty() || tgids.contains(&f.tgid)) && (systems.is_empty() || systems.contains(&f.system_id)),
        }
    }
}

/// A connected client's place under [`MAX_CLIENTS`], given back when dropped.
struct Place(Arc<AtomicUsize>);

impl Place {
    fn take(count: &Arc<AtomicUsize>) -> Option<Place> {
        // Taken, then given back when there was none to take.
        let place = Place(count.clone());
        (count.fetch_add(1, Ordering::SeqCst) < MAX_CLIENTS).then_some(place)
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub async fn live(State(state): State<AppState>, ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>) -> Response {
    let Some(bus) = state.audio.clone() else {
        return ApiError::NotFound("live audio streaming is not enabled".into()).into_response();
    };
    let ws = match ws {
        Ok(ws) => ws,
        Err(rejection) => return rejection.into_response(),
    };
    let Some(place) = Place::take(&state.audio_clients) else {
        return ApiError::ServiceUnavailable(format!("{MAX_CLIENTS} live audio clients are connected already")).into_response();
    };
    // Subscribed now, so a frame published once the upgrade is answered reaches the client.
    let rx = bus.subscribe();
    let keepalive = state.audio_keepalive;
    ws.on_upgrade(move |socket| client(socket, rx, bus, keepalive, place))
}

/// One client: its control messages, the bus's frames, and the keepalive, until
/// either side closes or a message cannot go out within [`SEND_TIMEOUT`]. A client
/// too slow for the bus is told what it missed.
async fn client(mut socket: WebSocket, mut rx: Receiver<AudioFrame>, bus: AudioBus, keepalive: Duration, _place: Place) {
    let opened = Instant::now();
    let mut filter = Filter::all();
    let mut seqs: HashMap<(i64, u32), u16> = HashMap::new();
    let mut ticker = interval_at(opened + keepalive, keepalive);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let out = tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(t))) => {
                    if let Some(f) = Filter::parse(t.as_str()) {
                        filter = f;
                    }
                    continue;
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => continue,
            },
            frame = rx.recv() => match frame {
                Ok(f) if filter.matches(&f) => {
                    let seq = seqs.entry((f.system_id, f.tgid)).or_insert(0);
                    *seq = seq.wrapping_add(1);
                    Message::Binary(frame_bytes(&f, opened.elapsed().as_millis() as u32, *seq).into())
                }
                Ok(_) => continue,
                Err(RecvError::Lagged(n)) => Message::Text(json!({"type": "lagged", "dropped": n}).to_string().into()),
                Err(RecvError::Closed) => return,
            },
            _ = ticker.tick() => Message::Text(json!({"type": "keepalive", "active_streams": bus.active_streams()}).to_string().into()),
        };
        // Ending the task drops `_place`, giving the client's place back.
        if !matches!(tokio::time::timeout(SEND_TIMEOUT, socket.send(out)).await, Ok(Ok(()))) {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(system_id: i64, tgid: u32) -> AudioFrame {
        AudioFrame { system_id, tgid, sample_rate: 8000, pcm: Arc::from(&[0u8; 0][..]) }
    }

    #[test]
    fn control_messages_set_the_filter() {
        assert_eq!(Filter::parse(r#"{"type":"subscribe"}"#), Some(Filter::all()));
        assert_eq!(Filter::parse(r#"{"type":"unsubscribe"}"#), Some(Filter::Off));
        assert_eq!(Filter::parse(r#"{"type":"ping"}"#), None);
        assert_eq!(Filter::parse("not json"), None);
        let both = Filter::parse(r#"{"type":"subscribe","tgids":[9178,"x",-1],"systems":[2]}"#).unwrap();
        assert_eq!(both, Filter::On { tgids: vec![9178], systems: vec![2] });
        assert!(both.matches(&f(2, 9178)) && !both.matches(&f(1, 9178)) && !both.matches(&f(2, 1)));
        let systems = Filter::parse(r#"{"type":"subscribe","tgids":[],"systems":[1]}"#).unwrap();
        assert!(systems.matches(&f(1, 5)) && !systems.matches(&f(2, 5)));
        assert!(Filter::all().matches(&f(9, 9)) && !Filter::Off.matches(&f(9, 9)));
    }

    #[test]
    fn places_are_limited_and_given_back() {
        let count = Arc::new(AtomicUsize::new(0));
        let places: Vec<Place> = (0..MAX_CLIENTS).map(|_| Place::take(&count).expect("a place")).collect();
        assert!(Place::take(&count).is_none());
        drop(places);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
