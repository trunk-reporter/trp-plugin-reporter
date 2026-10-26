//! The binary end to end: the plugin serving over a pipe, and `serve <data dir>`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use trunk_recorder_plugin::{HostMessage, testing};

use trunk_reporter::plugin::Reporter;

const BIN: &str = env!("CARGO_BIN_EXE_trunk-reporter");

/// A port nothing listens on (bound, then let go).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A blocking HTTP/1.1 GET: the status and the body.
fn get(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(s, "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n")?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
    Ok((status, body))
}

/// The health body, polled until the server answers (≤ 5 s).
fn health(port: u16) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match get(port, "/api/v1/health") {
            Ok((200, body)) => return serde_json::from_str(&body).expect("health is JSON"),
            other if Instant::now() > deadline => panic!("no health answer: {other:?}"),
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Every stdout line, from a thread (so the child never blocks on a full pipe).
fn lines(out: ChildStdout) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn wait_exit(child: &mut Child, within: Duration) -> Option<i32> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

#[test]
fn the_plugin_serves_while_running_and_stops_cleanly() {
    let dir = testing::temp_dir("reporter-serve-plugin");
    let port = free_port();
    let hello = serde_json::to_string(&HostMessage::Hello(testing::hello(&dir, json!({"port": port})))).unwrap();
    let mut child = Command::new(BIN).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{hello}").unwrap();
    let out = lines(child.stdout.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let line = out.recv_timeout(deadline.saturating_duration_since(Instant::now())).expect("a ready line");
        if serde_json::from_str::<Value>(&line).is_ok_and(|v| v["type"] == "ready") {
            break;
        }
    }
    let (status, body) = get(port, "/api/v1/health").unwrap();
    assert_eq!(status, 200, "{body}");
    let h: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(h["trunk_recorders"][0]["status"], "connected");
    assert_eq!(h["trunk_recorders"][0]["instance_id"], "test");
    drop(stdin);
    assert_eq!(wait_exit(&mut child, Duration::from_secs(5)), Some(0));
    // Stopped: the port is free again.
    assert!(get(port, "/api/v1/health").is_err());
}

fn serve(data_dir: &Path) -> Child {
    Command::new(BIN).arg("serve").arg(data_dir).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap()
}

#[test]
fn serve_mode_opens_an_existing_store() {
    let dir = testing::temp_dir("reporter-serve-store");
    let out = testing::run::<Reporter>([
        HostMessage::Hello(testing::hello(&dir, json!({"port": 0, "backfill": false}))),
        HostMessage::CallConcluded(testing::call(&dir, "sys1", 101)),
    ]);
    assert!(out.ready());
    let port = free_port();
    std::fs::write(dir.join("data/serve.json"), json!({"port": port}).to_string()).unwrap();
    let mut child = serve(&dir.join("data"));
    let h = health(port);
    assert_eq!(h["trunk_recorders"][0]["status"], "disconnected");
    assert_eq!(h["trunk_recorders"][0]["instance_id"], "test");
    assert_eq!(h["features"]["live_audio"], false);
    let (status, body) = get(port, "/api/v1/calls").unwrap();
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["total"], 1, "{body}");
    child.kill().unwrap();
    let _ = child.wait();
}

#[test]
fn serve_mode_without_a_store_says_what_to_do() {
    let dir = testing::temp_dir("reporter-serve-empty");
    let mut child = serve(&dir);
    assert_eq!(wait_exit(&mut child, Duration::from_secs(5)), Some(2));
    let mut err = String::new();
    child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    assert!(err.contains("run the plugin once"), "{err}");
    assert!(!dir.join("engine.sqlite").exists());
}

#[test]
fn serve_mode_reports_a_bad_settings_file() {
    let dir = testing::temp_dir("reporter-serve-badjson");
    testing::run::<Reporter>([HostMessage::Hello(testing::hello(&dir, json!({"port": 0, "backfill": false})))]);
    std::fs::write(dir.join("data/serve.json"), "{not json").unwrap();
    let mut child = serve(&dir.join("data"));
    assert_eq!(wait_exit(&mut child, Duration::from_secs(5)), Some(2));
    let mut err = String::new();
    child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
    assert!(err.contains("serve.json"), "{err}");
}
