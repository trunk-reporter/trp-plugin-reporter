//! The relay: the transcribe plugin's event stream (the fake) becomes `transcription`
//! events on the bus.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::fake_transcribe::{self, FakeEvent, FakeTranscribe};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::timeout;
use trunk_reporter::api::Log;
use trunk_reporter::bus::{Bus, Event, EventKind, Filter, Subscription};
use trunk_reporter::store::calls::{NewCall, call_started};
use trunk_reporter::store::{CallId, Store};
use trunk_reporter::transcripts::client::TranscribeClient;
use trunk_reporter::transcripts::relay::{RelayStatus, run_relay, run_relay_within};

/// A relay running against a fake plugin, on a seeded store.
struct Rig {
    store: Store,
    seed: common::Seed,
    sub: Subscription,
    status: RelayStatus,
    fake: FakeTranscribe,
    logs: Arc<Mutex<Vec<String>>>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Rig {
    async fn start() -> Rig {
        let store = Store::in_memory();
        let seed = common::seed(&store);
        let bus = Bus::new(100);
        let sub = bus.subscribe(Filter::default());
        let fake = fake_transcribe::start(vec![]).await;
        let client = Arc::new(TranscribeClient::new(&fake.url).unwrap());
        let status = RelayStatus::default();
        let logs: Arc<Mutex<Vec<String>>> = Arc::default();
        let sink = logs.clone();
        let log: Log = Arc::new(move |m| sink.lock().unwrap().push(m));
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run_relay(client, store.clone(), bus, status.clone(), log, stopped));
        Rig { store, seed, sub, status, fake, logs, stop, task }
    }

    /// The stored path of the seed's newest call (c1).
    fn c1_path(&self) -> String {
        let id = self.seed.calls[0];
        self.store.read(|c| c.query_row("SELECT path FROM calls WHERE id = ?1", [id], |r| r.get(0))).unwrap()
    }

    fn send(&self, t: Value) {
        self.fake.events.send(FakeEvent::Frame(t)).unwrap();
    }

    async fn next(&mut self, within: Duration) -> Option<Event> {
        timeout(within, self.sub.rx.recv()).await.ok().flatten()
    }

    /// Waits up to 5 s for a log line equal to `line`.
    async fn logged(&self, line: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.logs.lock().unwrap().iter().any(|m| m == line) {
            assert!(Instant::now() < deadline, "no line {line:?} in {:?}", self.logs.lock().unwrap());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Stop the relay and see that it ends.
    async fn finish(self) {
        self.stop.send(true).unwrap();
        timeout(Duration::from_secs(5), self.task).await.expect("the relay stops").unwrap();
    }
}

fn transcript(path: &str, status: &str, text: Option<&str>, transcribed_ms: i64) -> Value {
    json!({
        "path": path, "system": "ohmarcs", "talkgroup": 101, "startMs": 1, "lengthMs": 1620,
        "status": status, "reason": null, "text": text, "language": "English", "model": "m",
        "transcribedMs": transcribed_ms, "segments": []
    })
}

fn data(e: &Event) -> Value {
    serde_json::from_str(&e.data).unwrap()
}

#[tokio::test]
async fn an_ok_frame_becomes_a_transcription_event() {
    let mut rig = Rig::start().await;
    let path = rig.c1_path();
    rig.send(transcript(&path, "ok", Some("234?"), 1_793_358_971_026));
    let e = rig.next(Duration::from_secs(2)).await.expect("a transcription event");
    assert_eq!(e.kind, EventKind::Transcription);
    assert_eq!(e.kind.as_str(), "transcription");
    assert_eq!(e.system_id, Some(rig.seed.sys1.0));
    assert_eq!(e.site_id, Some(rig.seed.site1.0));
    assert_eq!(e.tgid, Some(101));
    assert_eq!(e.unit_id, None);
    let d = data(&e);
    assert_eq!(d["call_id"], rig.seed.calls[0].0);
    assert_eq!(d["system_id"], rig.seed.sys1.0);
    assert_eq!(d["tgid"], 101);
    assert_eq!(d["text"], "234?");
    assert!(d["start_time"].as_str().is_some_and(|s| s.ends_with('Z') && s.contains('T')), "{d}");
    assert_eq!(rig.status.relayed(), 1);
    assert!(rig.status.connected());
    assert_eq!(rig.status.last_event_ms(), Some(1_793_358_971_026));
    assert!(rig.logs.lock().unwrap().iter().any(|m| m == "transcribe plugin event stream connected"));
    // `/health` reads the same status.
    let mut state = common::state_with(None).0.with_transcribe(&rig.fake.url).unwrap();
    state.relay = Some(rig.status.clone());
    let (_, v) = common::get(&trunk_reporter::api::router(state), "/api/v1/health").await;
    assert_eq!(v["transcribe"]["connected"], true);
    assert_eq!(v["transcribe"]["last_event_ms"], 1_793_358_971_026i64);
    rig.finish().await;
}

#[tokio::test]
async fn skipped_and_empty_frames_make_no_event() {
    let mut rig = Rig::start().await;
    let path = rig.c1_path();
    rig.send(transcript(&path, "skipped", None, 10));
    rig.send(transcript(&path, "ok", Some("  "), 11));
    rig.send(transcript(&path, "ok", Some("go ahead"), 12));
    let e = rig.next(Duration::from_secs(2)).await.expect("the ok frame's event");
    assert_eq!(data(&e)["text"], "go ahead");
    assert!(rig.next(Duration::from_millis(500)).await.is_none(), "only one event");
    assert_eq!(rig.status.relayed(), 1);
    rig.finish().await;
}

#[tokio::test]
async fn a_frame_for_a_call_not_yet_stored_is_retried_then_published() {
    let mut rig = Rig::start().await;
    let late = "ohmarcs/101-1793358967_851012500";
    rig.send(transcript(late, "ok", Some("late"), 20));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (sys1, site1) = (rig.seed.sys1, rig.seed.site1);
    let id: CallId = rig
        .store
        .write_sync(move |c| {
            let id = call_started(
                c,
                &NewCall {
                    system_id: sys1,
                    site_id: Some(site1),
                    call_num: Some(77),
                    tgid: 101,
                    start_ms: 1_793_358_967_000,
                    freq_hz: 851_012_500,
                    tdma_slot: None,
                    analog: false,
                    encrypted: false,
                    algid: None,
                    key_id: None,
                    decrypted: false,
                    emergency: false,
                    duplex: false,
                    packet: false,
                    priority: 4,
                    recorded: true,
                    not_recorded_reason: None,
                    tg_alpha_tag: "Fire Disp".into(),
                    patched_tgids: vec![],
                },
            )?;
            c.execute("UPDATE calls SET path = ?1 WHERE id = ?2", rusqlite::params![late, id])?;
            Ok(id)
        })
        .unwrap();
    let e = rig.next(Duration::from_secs(12)).await.expect("the retried frame's event");
    assert_eq!(data(&e)["call_id"], id.0);
    assert_eq!(data(&e)["text"], "late");

    rig.send(transcript("ohmarcs/101-1_1", "ok", Some("never"), 21));
    assert!(rig.next(Duration::from_secs(15)).await.is_none(), "a call never stored makes no event");
    assert!(rig.logs.lock().unwrap().iter().any(|m| m == "transcript for ohmarcs/101-1_1: no such call, dropped"), "{:?}", rig.logs.lock().unwrap());
    assert_eq!(rig.status.relayed(), 1);
    rig.finish().await;
}

#[tokio::test]
async fn a_closed_stream_reconnects_with_last_event_id_and_publishes_a_replay_once() {
    let mut rig = Rig::start().await;
    let path = rig.c1_path();
    let first = transcript(&path, "ok", Some("one"), 1_000);
    rig.send(first.clone());
    assert_eq!(data(&rig.next(Duration::from_secs(2)).await.expect("the first event"))["text"], "one");
    rig.fake.events.send(FakeEvent::Close).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rig.fake.last_event_ids.lock().unwrap().contains(&Some("1000".into())) {
        assert!(Instant::now() < deadline, "no reconnect with Last-Event-ID: {:?}", rig.fake.last_event_ids.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rig.fake.last_event_ids.lock().unwrap()[0], None, "the first connection asks for no replay");
    rig.send(first);
    rig.send(transcript(&path, "ok", Some("two"), 2_000));
    assert_eq!(data(&rig.next(Duration::from_secs(2)).await.expect("the new event"))["text"], "two");
    assert!(rig.next(Duration::from_millis(500)).await.is_none(), "the replay is published once");
    assert_eq!(rig.status.relayed(), 2);
    rig.finish().await;
}

#[tokio::test]
async fn an_undecodable_frame_makes_no_event_and_is_logged() {
    let mut rig = Rig::start().await;
    let path = rig.c1_path();
    rig.send(json!({ "path": 7, "transcribedMs": 5 }));
    rig.send(transcript(&path, "ok", Some("after"), 6));
    assert_eq!(data(&rig.next(Duration::from_secs(2)).await.expect("the good frame's event"))["text"], "after");
    assert!(rig.next(Duration::from_millis(300)).await.is_none(), "the undecodable frame makes no event");
    assert_eq!(rig.status.relayed(), 1);
    let logs = rig.logs.lock().unwrap().clone();
    assert!(logs.iter().any(|m| m.starts_with("transcribe plugin: undecodable transcript frame: ")), "{logs:?}");
    rig.finish().await;
}

#[tokio::test]
async fn a_closed_stream_logs_why_it_ended() {
    let rig = Rig::start().await;
    rig.logged("transcribe plugin event stream connected").await;
    rig.fake.events.send(FakeEvent::Close).unwrap();
    rig.logged("transcribe plugin: event stream ended: the plugin closed it").await;
    rig.finish().await;
}

#[tokio::test]
async fn a_stream_that_only_pings_and_closes_backs_off() {
    let rig = Rig::start().await;
    // Each connection gets pings (every 100 ms) for 250 ms, then is closed.
    let watch_for = Duration::from_secs(6);
    let started = Instant::now();
    let mut closed = 0;
    while started.elapsed() < watch_for {
        let connections = rig.fake.last_event_ids.lock().unwrap().len();
        if connections > closed {
            tokio::time::sleep(Duration::from_millis(250)).await;
            rig.fake.events.send(FakeEvent::Close).unwrap();
            closed = connections;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Backoffs of 1 s, 2 s, 4 s: connections at about 0, 1.25 and 3.5 s. Were pings to
    // reset the backoff, one every 1.25 s would make five.
    let connections = rig.fake.last_event_ids.lock().unwrap().len();
    assert!((2..=4).contains(&connections), "{connections} connections in {watch_for:?}");
    rig.finish().await;
}

#[tokio::test]
async fn a_plugin_that_never_answers_is_given_up_and_retried() {
    // Takes connections and holds them, never answering.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let accepted: Arc<Mutex<Vec<tokio::net::TcpStream>>> = Arc::default();
    let held = accepted.clone();
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            held.lock().unwrap().push(socket);
        }
    });
    let client = Arc::new(TranscribeClient::new(&url).unwrap());
    let status = RelayStatus::default();
    let logs: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = logs.clone();
    let log: Log = Arc::new(move |m| sink.lock().unwrap().push(m));
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run_relay_within(Duration::from_millis(300), client, Store::in_memory(), Bus::new(10), status.clone(), log, stopped));
    let want = format!("transcribe plugin not reachable at {url}: no answer within 300ms");
    let deadline = Instant::now() + Duration::from_secs(10);
    // The line, then a second attempt after the 1 s backoff: the relay keeps running.
    while !(logs.lock().unwrap().contains(&want) && accepted.lock().unwrap().len() >= 2) {
        assert!(Instant::now() < deadline, "logs {:?}, {} connections", logs.lock().unwrap(), accepted.lock().unwrap().len());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!status.connected());
    assert!(!task.is_finished());
    stop.send(true).unwrap();
    timeout(Duration::from_secs(2), task).await.expect("a stop ends the relay").unwrap();
    server.abort();
}

#[tokio::test]
async fn an_unreachable_plugin_is_logged_once_and_stops_promptly() {
    let store = Store::in_memory();
    let client = Arc::new(TranscribeClient::new("http://127.0.0.1:1").unwrap());
    let status = RelayStatus::default();
    let logs: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = logs.clone();
    let log: Log = Arc::new(move |m| sink.lock().unwrap().push(m));
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run_relay(client, store, Bus::new(10), status.clone(), log, stopped));
    let deadline = Instant::now() + Duration::from_secs(10);
    while !logs.lock().unwrap().iter().any(|m| m.starts_with("transcribe plugin not reachable")) {
        assert!(Instant::now() < deadline, "no line about the unreachable plugin");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Backoffs of 1 s then 2 s: more failed attempts, no more lines.
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let unreachable = logs.lock().unwrap().iter().filter(|m| m.starts_with("transcribe plugin not reachable at http://127.0.0.1:1")).count();
    assert_eq!(unreachable, 1, "{:?}", logs.lock().unwrap());
    assert!(!status.connected());
    stop.send(true).unwrap();
    timeout(Duration::from_secs(2), task).await.expect("a stop ends the backoff sleep").unwrap();
}
