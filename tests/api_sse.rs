//! `GET /api/v1/events/stream`: real sockets, raw HTTP.

mod common;

use std::time::Duration;

use axum::Router;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use trunk_reporter::bus::{Bus, EventKind, NewEvent};

fn ev(kind: EventKind, data: &str) -> NewEvent {
    NewEvent { kind, sub_type: None, system_id: None, site_id: None, tgid: None, unit_id: None, emergency: false, data: data.into() }
}

async fn serve(router: Router) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });
    addr
}

/// Connect and read the response head; returns the stream, the head, and any body bytes already read.
async fn open(addr: std::net::SocketAddr, target: &str, headers: &str) -> (TcpStream, String, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {target} HTTP/1.1\r\nHost: x\r\n{headers}\r\n").as_bytes()).await.unwrap();
    let mut buf = String::new();
    loop {
        let mut chunk = [0u8; 1024];
        let n = timeout(Duration::from_secs(2), s.read(&mut chunk)).await.expect("head in time").unwrap();
        assert!(n > 0, "closed before the head ended: {buf:?}");
        buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
        if let Some(i) = buf.find("\r\n\r\n") {
            let body = buf[i + 4..].to_string();
            buf.truncate(i);
            return (s, buf, body);
        }
    }
}

/// Raw socket bytes not yet decoded, and the decoded body not yet returned as frames.
#[derive(Default)]
struct Pending {
    raw: String,
    body: String,
}

impl Pending {
    fn new(initial: String) -> Pending {
        Pending { raw: initial, body: String::new() }
    }

    /// Move every complete chunk from `raw` to `body`.
    fn decode(&mut self) {
        while let Some(i) = self.raw.find("\r\n") {
            let Ok(n) = usize::from_str_radix(&self.raw[..i], 16) else { return };
            if self.raw.len() < i + 2 + n + 2 {
                return;
            }
            self.body.push_str(&self.raw[i + 2..i + 2 + n]);
            self.raw.drain(..i + 2 + n + 2);
        }
    }
}

/// Read until a complete frame (ending `\n\n`) is decoded; returns it and keeps the rest.
async fn frame(s: &mut TcpStream, pending: &mut Pending) -> String {
    loop {
        pending.decode();
        if let Some(i) = pending.body.find("\n\n") {
            return pending.body.drain(..i + 2).collect();
        }
        let mut chunk = [0u8; 1024];
        let n = timeout(Duration::from_secs(2), s.read(&mut chunk)).await.expect("frame in time").unwrap();
        assert!(n > 0, "stream closed");
        pending.raw.push_str(&String::from_utf8_lossy(&chunk[..n]));
    }
}

fn publish(bus: &Bus, kind: EventKind, data: &str) -> String {
    bus.publish(ev(kind, data)).id
}

#[tokio::test]
async fn the_stream_delivers_filtered_frames_in_sse_format() {
    let (router, _s, bus, _a) = common::app();
    let addr = serve(router).await;
    let (mut s, head, body) = open(addr, "/api/v1/events/stream?types=call_start", "").await;
    let head = head.to_lowercase();
    assert!(head.starts_with("http/1.1 200"), "{head}");
    assert!(head.contains("content-type: text/event-stream"), "{head}");
    assert!(head.contains("cache-control: no-cache"), "{head}");
    assert!(head.contains("x-accel-buffering: no"), "{head}");
    let mut pending = Pending::new(body);
    while bus.subscriber_count() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let id = publish(&bus, EventKind::CallStart, "{\"call_id\":1}");
    publish(&bus, EventKind::UnitEvent, "{\"unit\":2}");
    let f = frame(&mut s, &mut pending).await;
    assert_eq!(f, format!("id: {id}\nevent: call_start\ndata: {{\"call_id\":1}}\n\n"));
    let more = timeout(Duration::from_millis(200), frame(&mut s, &mut pending)).await;
    assert!(more.is_err(), "unexpected extra frame: {more:?}");
}

#[tokio::test]
async fn last_event_id_replays_what_was_missed() {
    let (router, _s, bus, _a) = common::app();
    let addr = serve(router).await;
    let first = publish(&bus, EventKind::CallStart, "{\"n\":1}");
    let second = publish(&bus, EventKind::CallStart, "{\"n\":2}");
    let third = publish(&bus, EventKind::CallEnd, "{\"n\":3}");
    let (mut s, _head, body) = open(addr, "/api/v1/events/stream", &format!("Last-Event-ID: {first}\r\n")).await;
    let mut pending = Pending::new(body);
    assert_eq!(frame(&mut s, &mut pending).await, format!("id: {second}\nevent: call_start\ndata: {{\"n\":2}}\n\n"));
    assert_eq!(frame(&mut s, &mut pending).await, format!("id: {third}\nevent: call_end\ndata: {{\"n\":3}}\n\n"));
    let live = publish(&bus, EventKind::CallStart, "{\"n\":4}");
    assert_eq!(frame(&mut s, &mut pending).await, format!("id: {live}\nevent: call_start\ndata: {{\"n\":4}}\n\n"));

    // The query parameter works the same.
    let (mut s, _head, body) = open(addr, &format!("/api/v1/events/stream?last_event_id={second}"), "").await;
    let mut pending = Pending::new(body);
    assert_eq!(frame(&mut s, &mut pending).await, format!("id: {third}\nevent: call_end\ndata: {{\"n\":3}}\n\n"));
}

#[tokio::test]
async fn a_token_in_the_query_opens_the_stream() {
    let (router, _s, _b, _a) = common::app_with(Some("s3cret"));
    let addr = serve(router).await;
    let (_s1, head, _) = open(addr, "/api/v1/events/stream", "").await;
    assert!(head.starts_with("HTTP/1.1 401"), "{head}");
    let (_s2, head, _) = open(addr, "/api/v1/events/stream?token=s3cret", "").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(head.to_lowercase().contains("content-type: text/event-stream"), "{head}");
}

#[tokio::test]
async fn an_idle_stream_sends_keepalive_comments() {
    let (mut state, _s, _b, _a) = common::state_with(None);
    state.keepalive = Duration::from_millis(50);
    let addr = serve(trunk_reporter::api::router(state)).await;
    let (mut s, _head, body) = open(addr, "/api/v1/events/stream", "").await;
    let mut pending = Pending::new(body);
    assert_eq!(frame(&mut s, &mut pending).await, ": keepalive\n\n");
}
