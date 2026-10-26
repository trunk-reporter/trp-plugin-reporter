//! `GET /api/v1/audio/live`: real sockets, a WebSocket client.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use trunk_reporter::api::router;
use trunk_reporter::audio::{AudioBus, AudioFrame};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn serve(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });
    addr
}

async fn connect(addr: SocketAddr, query: &str) -> Result<Client, Box<WsError>> {
    connect_async(format!("ws://{addr}/api/v1/audio/live{query}")).await.map(|(ws, _)| ws).map_err(Box::new)
}

/// The status and JSON body of a refused upgrade.
fn refused(r: Result<Client, Box<WsError>>) -> (u16, Value) {
    match r.map_err(|e| *e) {
        Err(WsError::Http(res)) => {
            let body = res.body().as_deref().and_then(|b| serde_json::from_slice(b).ok()).unwrap_or(Value::Null);
            (res.status().as_u16(), body)
        }
        Err(e) => panic!("expected an HTTP refusal, got {e}"),
        Ok(_) => panic!("expected an HTTP refusal, the upgrade succeeded"),
    }
}

fn frame(system_id: i64, tgid: u32, pcm: &[u8]) -> AudioFrame {
    AudioFrame { system_id, tgid, sample_rate: 8000, pcm: Arc::from(pcm) }
}

/// The next message that isn't a ping or pong, within 2 s.
async fn next(ws: &mut Client) -> Message {
    loop {
        let m = timeout(Duration::from_secs(2), ws.next()).await.expect("a message in time").expect("the stream is open").expect("a message");
        if !matches!(m, Message::Ping(_) | Message::Pong(_)) {
            return m;
        }
    }
}

/// Send a control message, then a ping; once its pong is back, the server has
/// read the control message.
async fn control(ws: &mut Client, msg: Value) {
    ws.send(Message::Text(msg.to_string().into())).await.unwrap();
    ws.send(Message::Ping(b"sync"[..].into())).await.unwrap();
    loop {
        let m = timeout(Duration::from_secs(2), ws.next()).await.expect("a pong in time").expect("the stream is open").expect("a message");
        if matches!(m, Message::Pong(_)) {
            return;
        }
    }
}

/// (system_id, tgid, ts_ms, seq, sample_rate, pcm) of a binary frame.
fn parse(m: Message) -> (u16, u32, u32, u16, u16, Vec<u8>) {
    let Message::Binary(b) = m else { panic!("expected a binary frame, got {m:?}") };
    assert!(b.len() >= 14, "a frame has a 14-byte header: {b:?}");
    (
        u16::from_be_bytes([b[0], b[1]]),
        u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
        u32::from_be_bytes([b[6], b[7], b[8], b[9]]),
        u16::from_be_bytes([b[10], b[11]]),
        u16::from_be_bytes([b[12], b[13]]),
        b[14..].to_vec(),
    )
}

fn text(m: Message) -> Value {
    let Message::Text(t) = m else { panic!("expected a text message, got {m:?}") };
    serde_json::from_str(t.as_str()).expect("JSON text")
}

/// The app with a live-audio bus, served on a port.
async fn audio_app(token: Option<&str>) -> (SocketAddr, AudioBus) {
    let (state, bus) = common::state_with_audio(token);
    (serve(router(state)).await, bus)
}

#[tokio::test]
async fn frames_reach_a_subscribed_client_with_sequence_numbers() {
    let (addr, bus) = audio_app(None).await;
    let mut ws = connect(addr, "").await.unwrap();
    control(&mut ws, json!({"type": "subscribe", "tgids": [9178], "systems": []})).await;
    bus.publish(frame(3, 9178, &[1, 0, 2, 0]));
    bus.publish(frame(3, 9179, &[9, 9]));
    bus.publish(frame(3, 9178, &[3, 0, 255, 127]));
    let (sys, tgid, _, seq, rate, pcm) = parse(next(&mut ws).await);
    assert_eq!((sys, tgid, seq, rate, pcm), (3, 9178, 1, 8000, vec![1, 0, 2, 0]));
    let (sys, tgid, _, seq, rate, pcm) = parse(next(&mut ws).await);
    assert_eq!((sys, tgid, seq, rate, pcm), (3, 9178, 2, 8000, vec![3, 0, 255, 127]));
    let more = timeout(Duration::from_millis(300), next(&mut ws)).await;
    assert!(more.is_err(), "9179 is not subscribed: {more:?}");
}

#[tokio::test]
async fn an_empty_subscription_hears_everything_and_unsubscribe_nothing() {
    let (addr, bus) = audio_app(None).await;
    let mut ws = connect(addr, "").await.unwrap();
    control(&mut ws, json!({"type": "subscribe"})).await;
    bus.publish(frame(1, 101, &[1, 0]));
    bus.publish(frame(2, 202, &[2, 0]));
    let a = parse(next(&mut ws).await);
    let b = parse(next(&mut ws).await);
    assert_eq!((a.0, a.1, a.3), (1, 101, 1));
    assert_eq!((b.0, b.1, b.3), (2, 202, 1), "each (system, talkgroup) counts on its own");

    // Both lists given: both must match.
    control(&mut ws, json!({"type": "subscribe", "tgids": [101, 202], "systems": [2]})).await;
    bus.publish(frame(1, 101, &[1, 0]));
    bus.publish(frame(2, 202, &[2, 0]));
    let c = parse(next(&mut ws).await);
    assert_eq!((c.0, c.1, c.3), (2, 202, 2));

    control(&mut ws, json!({"type": "unsubscribe"})).await;
    bus.publish(frame(1, 101, &[1, 0]));
    let more = timeout(Duration::from_millis(300), next(&mut ws)).await;
    assert!(more.is_err(), "unsubscribed, yet: {more:?}");
}

#[tokio::test]
async fn a_client_that_says_nothing_hears_everything() {
    let (addr, bus) = audio_app(None).await;
    let mut ws = connect(addr, "").await.unwrap();
    // Unknown text and binary are ignored.
    control(&mut ws, json!({"type": "hello"})).await;
    ws.send(Message::Binary(vec![1, 2, 3].into())).await.unwrap();
    bus.publish(frame(4, 77, &[7, 0]));
    let (sys, tgid, ts, seq, rate, pcm) = parse(next(&mut ws).await);
    assert_eq!((sys, tgid, seq, rate, pcm), (4, 77, 1, 8000, vec![7, 0]));
    assert!(ts < 2_000, "ms since the connection opened: {ts}");
}

#[tokio::test]
async fn a_keepalive_names_the_active_streams() {
    let (mut state, _bus) = common::state_with_audio(None);
    state.audio_keepalive = Duration::from_millis(100);
    let addr = serve(router(state)).await;
    let mut ws = connect(addr, "").await.unwrap();
    assert_eq!(text(next(&mut ws).await), json!({"type": "keepalive", "active_streams": 1}));
}

#[tokio::test]
async fn a_slow_client_is_dropped_not_waited_for() {
    let (addr, bus) = audio_app(None).await;
    let mut ws = connect(addr, "").await.unwrap();
    // The test's runtime has one thread: the server reads nothing while this runs.
    let pcm = [0u8; 320];
    let started = Instant::now();
    for _ in 0..1000 {
        bus.publish(frame(1, 101, &pcm));
    }
    let took = started.elapsed();
    assert!(took < Duration::from_millis(50), "1000 publishes took {took:?}");
    // The bus keeps the newest 256; the client is told of the rest.
    assert_eq!(text(next(&mut ws).await), json!({"type": "lagged", "dropped": 744}));
    for want in 1..=256u16 {
        let (_, _, _, seq, _, pcm) = parse(next(&mut ws).await);
        assert_eq!((seq, pcm.len()), (want, 320));
    }
    // Still open after the lag.
    bus.publish(frame(1, 101, &[5, 0]));
    assert_eq!(parse(next(&mut ws).await).3, 257);
}

#[tokio::test]
async fn without_a_recorder_live_audio_is_a_404_and_the_token_is_checked() {
    let (router_none, ..) = common::app();
    let (s, v) = common::get(&router_none, "/api/v1/audio/live").await;
    assert_eq!(s, 404);
    assert_eq!(v, json!({"code": "not_found", "error": "live audio streaming is not enabled"}));
    common::assert_schema(&v, "Error");
    let addr = serve(router_none).await;
    assert_eq!(refused(connect(addr, "").await).0, 404);

    let (addr, _bus) = audio_app(Some("s3")).await;
    let (s, v) = refused(connect(addr, "").await);
    assert_eq!((s, v["code"].as_str()), (401, Some("unauthorized")));
    assert_eq!(refused(connect(addr, "?token=wrong").await).0, 401);
    assert!(connect(addr, "?token=s3").await.is_ok());
}

#[tokio::test]
async fn the_seventeenth_client_is_refused() {
    let (state, _bus) = common::state_with_audio(None);
    let clients = state.audio_clients.clone();
    let addr = serve(router(state)).await;
    let mut open = Vec::new();
    for _ in 0..16 {
        open.push(connect(addr, "").await.unwrap());
    }
    let (s, v) = refused(connect(addr, "").await);
    assert_eq!(s, 503);
    assert_eq!(v["code"], "service_unavailable");
    common::assert_schema(&v, "Error");

    // A client that leaves frees its place.
    let mut gone = open.pop().unwrap();
    gone.close(None).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while clients.load(Ordering::SeqCst) > 15 {
        assert!(Instant::now() < deadline, "the count did not drop");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(connect(addr, "").await.is_ok());
}
