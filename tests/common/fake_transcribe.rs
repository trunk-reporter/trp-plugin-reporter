//! A fake transcribe plugin: the routes the bridge reads, served from a scripted list,
//! with a handle to push event-stream frames, close the stream, and make every route hang.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// What the test pushes onto the event stream.
pub enum FakeEvent {
    /// A transcript (its JSON) sent as an `event: transcript` frame.
    Frame(Value),
    /// End the stream's body.
    Close,
}

/// A running fake plugin.
pub struct FakeTranscribe {
    /// Its base URL, `http://127.0.0.1:<port>`.
    pub url: String,
    /// Push frames onto the event stream.
    pub events: UnboundedSender<FakeEvent>,
    /// The `Last-Event-ID` header of each event-stream request, in order.
    pub last_event_ids: Arc<Mutex<Vec<Option<String>>>>,
    /// The paths of each batch request, in order.
    pub batch_requests: Arc<Mutex<Vec<Vec<String>>>>,
    hanging: Arc<AtomicBool>,
}

impl FakeTranscribe {
    /// From now on every route sleeps 30 s before answering.
    pub fn hang(&self) {
        self.hanging.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Fake {
    transcripts: Arc<Vec<Value>>,
    rx: Arc<tokio::sync::Mutex<UnboundedReceiver<FakeEvent>>>,
    last_event_ids: Arc<Mutex<Vec<Option<String>>>>,
    batch_requests: Arc<Mutex<Vec<Vec<String>>>>,
    hanging: Arc<AtomicBool>,
}

impl Fake {
    async fn maybe_hang(&self) {
        if self.hanging.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    fn find(&self, path: &str) -> Option<&Value> {
        self.transcripts.iter().find(|t| t["path"] == path)
    }
}

async fn one(State(f): State<Fake>, Path(path): Path<String>) -> Response {
    f.maybe_hang().await;
    match f.find(&path) {
        Some(t) => axum::Json(t.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, axum::Json(json!({ "error": "no transcript" }))).into_response(),
    }
}

async fn batch(State(f): State<Fake>, Query(q): Query<HashMap<String, String>>) -> Response {
    f.maybe_hang().await;
    let paths: Vec<String> = q.get("paths").map(|p| p.split(',').filter(|s| !s.is_empty()).map(String::from).collect()).unwrap_or_default();
    f.batch_requests.lock().unwrap().push(paths.clone());
    let found: Vec<Value> = paths.iter().filter_map(|p| f.find(p).cloned()).collect();
    axum::Json(json!({ "transcripts": found })).into_response()
}

async fn health(State(f): State<Fake>) -> Response {
    f.maybe_hang().await;
    axum::Json(json!({ "ok": true, "engine": {}, "queue": 0, "inFlight": 0, "transcribed24h": 1 })).into_response()
}

async fn events(State(f): State<Fake>, headers: HeaderMap) -> Response {
    f.maybe_hang().await;
    let last = headers.get("last-event-id").and_then(|v| v.to_str().ok()).map(String::from);
    f.last_event_ids.lock().unwrap().push(last);
    // One stream at a time: the receiver is held until the stream ends.
    let rx = f.rx.clone().lock_owned().await;
    let stream = futures_util::stream::unfold((rx, tokio::time::interval(Duration::from_millis(100))), |(mut rx, mut tick)| async move {
        let chunk = tokio::select! {
            ev = rx.recv() => match ev {
                Some(FakeEvent::Frame(t)) => format!("id: {}\nevent: transcript\ndata: {}\n\n", t["transcribedMs"], t),
                Some(FakeEvent::Close) | None => return None,
            },
            _ = tick.tick() => "event: ping\ndata: {}\n\n".to_string(),
        };
        Some((Ok::<_, Infallible>(chunk), (rx, tick)))
    });
    ([(header::CONTENT_TYPE, "text/event-stream"), (header::CACHE_CONTROL, "no-cache")], Body::from_stream(stream)).into_response()
}

/// Start a fake plugin on a free local port, serving `transcripts` (Transcript JSON).
pub async fn start(transcripts: Vec<Value>) -> FakeTranscribe {
    let (tx, rx) = unbounded_channel();
    let fake = Fake {
        transcripts: Arc::new(transcripts),
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
        last_event_ids: Arc::default(),
        batch_requests: Arc::default(),
        hanging: Arc::default(),
    };
    let handle = FakeTranscribe {
        url: String::new(),
        events: tx,
        last_event_ids: fake.last_event_ids.clone(),
        batch_requests: fake.batch_requests.clone(),
        hanging: fake.hanging.clone(),
    };
    let app = Router::new()
        .route("/api/v1/transcripts/{*path}", get(one))
        .route("/api/v1/transcripts", get(batch))
        .route("/api/v1/health", get(health))
        .route("/api/v1/events", get(events))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind the fake plugin");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    FakeTranscribe { url, ..handle }
}
