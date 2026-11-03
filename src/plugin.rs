//! The plugin: its manifest, start-up (store, server, backfill, maintenance, the
//! transcripts relay), event handling and shutdown.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::watch;
use trunk_recorder_plugin::{AudioChunk, CallInfo, ConcludedCall, Host, HostMessage, Manifest, Metrics, Plugin, Setup, State, Status, UnitEvent, topic};

use crate::affiliations::Affiliations;
use crate::api::{AppState, HostStatus};
use crate::audio::{AudioBus, AudioFrame, BUS_CAPACITY};
use crate::backfill::backfill;
use crate::bus::Bus;
use crate::config::{Config, SystemConfig};
use crate::ingest::Ingest;
use crate::server::{self, BUS_RING, ServerHandle, WEEK_MS};
use crate::store::calls::end_stale_calls;
use crate::store::recorder::{decimate, retain};
use crate::store::systems::{list_systems, meta_set};
use crate::store::units::affiliation_events_since;
use crate::store::{Store, now_ms};
use crate::transcripts::client::TranscribeClient;
use crate::transcripts::relay::{RelayStatus, spawn_relay};

/// How often the recorder gets metrics.
const TICK: Duration = Duration::from_secs(5);
/// How often the metrics' call count is counted again (it walks the calls table).
const CALLS_EVERY: Duration = Duration::from_secs(60);
/// The first decimate and retain run this long after start.
const FIRST_UPKEEP: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3_600);
const DAY: Duration = Duration::from_secs(86_400);
/// The longest the server's stop may take of the shutdown grace.
const SERVER_STOP: Duration = Duration::from_secs(2);

pub struct Reporter {
    ingest: Ingest,
    store: Store,
    /// What `/health` says of the recorder: touched on every `status`.
    host_status: HostStatus,
    server: Option<ServerHandle>,
    maintenance: Option<(mpsc::Sender<()>, JoinHandle<()>)>,
    /// Set at shutdown: a backfill still walking stops (and stays incomplete).
    stop_backfill: Arc<AtomicBool>,
    /// The recorder's live audio, for `/audio/live`'s clients.
    audio: AudioBus,
    /// The relay of the transcribe plugin's event stream, when the bridge is on.
    relay: Option<(watch::Sender<bool>, JoinHandle<()>)>,
}

#[cfg(test)]
thread_local! {
    /// A receiver on the bus of the last plugin started on this thread, for the
    /// tests that run the plugin through `testing::run` (which keeps the plugin).
    static AUDIO_TAP: std::cell::RefCell<Option<tokio::sync::broadcast::Receiver<AudioFrame>>> = const { std::cell::RefCell::new(None) };
}

fn calls_stored(store: &Store) -> i64 {
    store.read(|c| c.query_row("SELECT count(*) FROM calls", [], |r| r.get(0))).unwrap_or(0)
}

/// The status line once the plugin is up (and after a backfill); `transcripts` is
/// whether the relay's stream is open, when the bridge is on.
fn status_line(serving: Option<&str>, calls: i64, transcripts: Option<bool>) -> String {
    let line = match serving {
        Some(url) => format!("{url} · {calls} calls stored"),
        None => format!("not serving (Port is 0) · {calls} calls stored"),
    };
    match transcripts {
        Some(true) => format!("{line} · transcripts: live"),
        Some(false) => format!("{line} · transcripts: unreachable"),
        None => line,
    }
}

/// The relay's state when the status line should be set again for it: the bridge
/// is on and its stream opened or closed since `reported` (`None`: never reported).
fn relay_change(now: Option<bool>, reported: Option<bool>) -> Option<bool> {
    now.filter(|c| reported != Some(*c))
}

/// The database file and its write-ahead log, in bytes.
fn store_bytes(db: &Path) -> u64 {
    let wal = PathBuf::from(format!("{}-wal", db.display()));
    [db, wal.as_path()].iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len()).sum()
}

/// What the recorder shows on the plugin's card; `calls` is the stored call count,
/// `relay` the transcripts relay when the bridge is on.
fn metrics(store: &Store, db: &Path, serving: Option<&str>, dropped: u64, calls: i64, relay: Option<&RelayStatus>) -> Metrics {
    let mb = (store_bytes(db) as f64 / 1e5).round() / 10.0;
    let mut m = Metrics { queued: Some(store.queue_len() as u64), ..Default::default() };
    m.extra.insert("calls".into(), json!(calls));
    m.extra.insert("store_mb".into(), json!(mb));
    m.extra.insert("serving".into(), json!(serving.unwrap_or("not serving")));
    m.extra.insert("dropped_events".into(), json!(dropped));
    if let Some(r) = relay {
        m.extra.insert("transcripts_relayed".into(), json!(r.relayed()));
    }
    m
}

/// Metrics every [`TICK`] (the call count every [`CALLS_EVERY`]); recorder samples thinned hourly and old history
/// deleted daily (both first a minute after start). The status line is set again
/// whenever the transcripts relay's stream opens or closes, except while a backfill
/// (`backfilling`) owns the status. Stops when the sender is used or dropped.
#[allow(clippy::too_many_arguments)]
fn maintenance(
    host: Host,
    store: Store,
    db: PathBuf,
    serving: Option<String>,
    dropped: Arc<AtomicU64>,
    retention_days: u32,
    relay: Option<RelayStatus>,
    backfilling: Arc<AtomicBool>,
) -> Result<(mpsc::Sender<()>, JoinHandle<()>), String> {
    let (stop, stopped) = mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("maintenance".into())
        .spawn(move || {
            let first = Instant::now() + FIRST_UPKEEP;
            let (mut next_decimate, mut next_retain) = (first, first);
            let mut calls: Option<(Instant, i64)> = None;
            // The relay state the status line last showed.
            let mut reported: Option<bool> = None;
            // A message or a dropped sender ends it.
            while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(TICK) {
                let count = match calls {
                    Some((at, n)) if at.elapsed() < CALLS_EVERY => n,
                    _ => {
                        let n = calls_stored(&store);
                        calls = Some((Instant::now(), n));
                        n
                    }
                };
                host.metrics(&metrics(&store, &db, serving.as_deref(), dropped.load(Ordering::Relaxed), count, relay.as_ref()));
                if !backfilling.load(Ordering::Relaxed)
                    && let Some(connected) = relay_change(relay.as_ref().map(RelayStatus::connected), reported)
                {
                    host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), Some(connected)));
                    reported = Some(connected);
                }
                let now = Instant::now();
                if now >= next_decimate {
                    store.write(|c| decimate(c, now_ms()).map(|_| ()));
                    next_decimate = now + HOUR;
                }
                if now >= next_retain {
                    let h = host.clone();
                    store.write_then(
                        move |c| retain(c, retention_days, now_ms()),
                        move |deleted| {
                            if let Some(n) = deleted.filter(|n| *n > 0) {
                                h.info(format!("removed {n} calls older than {retention_days} days"));
                            }
                        },
                    );
                    next_retain = now + DAY;
                }
            }
        })
        .map_err(|e| format!("cannot start maintenance: {e}"))?;
    Ok((stop, thread))
}

impl Plugin for Reporter {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Trunk Reporter".into(),
            subscribe: vec![
                topic::CALL_START.into(),
                topic::CALL_END.into(),
                topic::CALL_CONCLUDED.into(),
                topic::UNIT.into(),
                topic::STATUS.into(),
                topic::AUDIO.into(),
            ],
            // `--describe` has no settings, so this is the default port's URL;
            // a changed port is only in the log and status line.
            homepage: "http://127.0.0.1:8092/".into(),
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        std::fs::create_dir_all(&setup.data_dir).map_err(|e| format!("cannot create {}: {e}", setup.data_dir.display()))?;
        let path = setup.data_dir.join("engine.sqlite");
        let store = Store::open(&path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        let log = host.clone();
        store.set_logger(Arc::new(move |m| log.error(m)));
        let meta = [
            ("capture_dir", setup.capture_dir.to_string_lossy().into_owned()),
            ("host_name", setup.host.name.clone()),
            ("host_version", setup.host.version.clone()),
        ];
        store.write_sync(move |c| meta.iter().try_for_each(|(k, v)| meta_set(c, k, v))).map_err(|e| e.to_string())?;
        // No call is live before the recorder's first message.
        store.write_sync(end_stale_calls).map_err(|e| e.to_string())?;
        let bus = Bus::new(BUS_RING);
        let affiliations = Affiliations::new();
        let new_ingest = || -> Result<Ingest, String> {
            let mut ingest = Ingest::new(store.clone(), bus.clone(), affiliations.clone(), Some(host.clone()), &setup.systems)
                .map_err(|e| e.to_string())?
                .with_capture_dir(setup.capture_dir.clone())
                .with_host_name(&setup.host.name);
            // Known systems and sites then never wait on the store (behind a backfill).
            ingest.warm_caches().map_err(|e| e.to_string())?;
            Ok(ingest)
        };
        let ingest = new_ingest()?;
        let recent = store.read(|c| affiliation_events_since(c, now_ms() - WEEK_MS)).map_err(|e| e.to_string())?;
        affiliations.rebuild(&recent);

        let audio = AudioBus::new(BUS_CAPACITY);
        #[cfg(test)]
        AUDIO_TAP.with(|t| *t.borrow_mut() = Some(audio.subscribe()));
        let config = &setup.config;
        // The relay runs whether or not the API is served. Its sender, dropped by an
        // early return below, stops it.
        let transcribe = match config.transcribe_url.trim() {
            "" => None,
            url => Some(Arc::new(TranscribeClient::new(url)?)),
        };
        let (relay_status, relay) = match &transcribe {
            Some(client) => {
                host.info(format!("reading transcripts from {}", client.base()));
                let h = host.clone();
                let (status, stop, thread) = spawn_relay(client.clone(), store.clone(), bus.clone(), Arc::new(move |m| h.info(m)));
                (Some(status), Some((stop, thread)))
            }
            None => (None, None),
        };
        let host_status = HostStatus::new(&setup.host.name, true);
        host_status.touch(now_ms());
        let server = if config.port == 0 {
            None
        } else {
            let mut state =
                AppState::new(store.clone(), bus.clone(), affiliations.clone(), setup.capture_dir.clone(), config.token.clone(), host_status.clone());
            if let Some(client) = &transcribe {
                state = state.with_transcribe_client(client.clone());
            }
            state.relay = relay_status.clone();
            let log = host.clone();
            state.cors_origins = config.allowed_origins();
            state.log = Arc::new(move |m| log.error(m));
            state.audio = Some(audio.clone());
            Some(server::start(state, &config.bind, config.port)?)
        };
        let serving = server.as_ref().map(ServerHandle::url);
        if let Some(url) = &serving {
            host.info(format!("serving tr-engine's API and pages at {url}"));
        }

        let stop_backfill = Arc::new(AtomicBool::new(false));
        // While set, the backfill owns the status line and maintenance leaves it alone.
        let backfilling = Arc::new(AtomicBool::new(config.backfill));
        // The status is set before the backfill starts, so its own (the
        // last word once it ends) cannot be overwritten by this one.
        if config.backfill {
            host.status(State::Warning, "backfilling: 0 calls");
            let mut walker = new_ingest()?;
            let systems: Vec<_> = store.read(list_systems).map_err(|e| e.to_string())?.into_iter().map(|s| (s.id, s.short_name)).collect();
            let (host, store, capture_dir, serving, stop) = (host.clone(), store.clone(), setup.capture_dir.clone(), serving.clone(), stop_backfill.clone());
            let (relay_status, backfilling) = (relay_status.clone(), backfilling.clone());
            std::thread::Builder::new()
                .name("backfill".into())
                .spawn(move || {
                    let mut told = Instant::now();
                    let report = backfill(
                        &mut walker,
                        &capture_dir,
                        &systems,
                        &mut |n| {
                            if told.elapsed() >= Duration::from_secs(1) {
                                told = Instant::now();
                                host.status(State::Warning, format!("backfilling: {n} calls"));
                            }
                        },
                        &stop,
                    );
                    store.flush();
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    if report.ingested > 0 || report.bad > 0 {
                        host.info(format!(
                            "backfill: {} calls added from {} files ({} already stored, {} left to the recorder, {} unreadable)",
                            report.ingested, report.files, report.skipped_known, report.skipped_new, report.bad
                        ));
                    }
                    host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), relay_status.as_ref().map(RelayStatus::connected)));
                    backfilling.store(false, Ordering::Relaxed);
                })
                .map_err(|e| format!("cannot start the backfill: {e}"))?;
        } else {
            host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), relay_status.as_ref().map(RelayStatus::connected)));
        }

        let maintenance = maintenance(host.clone(), store.clone(), path, serving, ingest.dropped_counter(), config.retention_days, relay_status, backfilling)?;
        Ok(Reporter { ingest, store, host_status, server, maintenance: Some(maintenance), stop_backfill, audio, relay })
    }

    fn call_start(&mut self, call: CallInfo) {
        self.ingest.handle(HostMessage::CallStart(call));
    }

    fn call_end(&mut self, call: CallInfo) {
        self.ingest.handle(HostMessage::CallEnd(call));
    }

    fn call_concluded(&mut self, call: ConcludedCall) {
        self.ingest.handle(HostMessage::CallConcluded(call));
    }

    fn unit(&mut self, event: UnitEvent) {
        self.ingest.handle(HostMessage::Unit(event));
    }

    /// Published to the live-audio clients; no I/O, no waiting. A chunk of a
    /// system not yet seen is dropped, and so is every chunk while no client
    /// listens (before it is decoded).
    fn audio(&mut self, chunk: AudioChunk) {
        if self.audio.active_streams() == 0 {
            return;
        }
        let Some(system_id) = self.ingest.system_id_of(chunk.system, &chunk.short_name) else { return };
        let pcm: Arc<[u8]> = trunk_recorder_plugin::base64::decode(&chunk.pcm).into();
        let sample_rate = u16::try_from(chunk.sample_rate).unwrap_or(u16::MAX);
        self.audio.publish(AudioFrame { system_id: system_id.0, tgid: chunk.talkgroup, sample_rate, pcm });
    }

    fn status(&mut self, status: Status) {
        self.host_status.touch(now_ms());
        self.ingest.handle(HostMessage::Status(status));
    }

    /// A running backfill is told to stop, then maintenance, then the transcripts relay (waited
    /// for within `grace`), then the server (at most 2 s of `grace`), then every
    /// queued write. The flush runs even when the grace is spent: the
    /// history matters more than the deadline.
    fn shutdown(&mut self, grace: Duration) {
        let deadline = Instant::now() + grace;
        self.stop_backfill.store(true, Ordering::Relaxed);
        if let Some((stop, thread)) = self.maintenance.take() {
            let _ = stop.send(());
            let _ = thread.join();
        }
        if let Some((stop, thread)) = self.relay.take() {
            // It ends at its next await; a thread still running at the deadline is left to the exit.
            let _ = stop.send(true);
            while !thread.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            if thread.is_finished() {
                let _ = thread.join();
            }
        }
        if let Some(server) = self.server.take() {
            server.stop_within(deadline.saturating_duration_since(Instant::now()).min(SERVER_STOP));
        }
        self.store.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trunk_recorder_plugin::{HostInfo, NoConfig, Outcome, System, testing};

    #[test]
    fn the_manifest_subscribes_to_what_the_plugin_needs() {
        let m = trunk_recorder_plugin::describe::<Reporter>();
        assert_eq!((m.id.as_str(), m.name.as_str()), ("trunk-reporter", "Trunk Reporter"));
        assert!(m.subscribes("call.concluded") && m.subscribes("unit") && m.subscribes("status") && m.subscribes("audio"));
        assert!(m.config.is_some() && m.system_config.is_none());
    }

    #[test]
    fn the_plugin_starts_stores_and_answers() {
        let dir = testing::temp_dir("reporter");
        let out = testing::run::<Reporter>([
            HostMessage::Hello(testing::hello(&dir, json!({"transcribeUrl": "", "port": 0, "backfill": false}))),
            HostMessage::CallConcluded(testing::call(&dir, "sys1", 101)),
        ]);
        assert!(out.ready());
        assert_eq!(out.results()[0].1, Outcome::Ok);
        assert!(dir.join("data/engine.sqlite").exists());
        let store = crate::store::Store::open(&dir.join("data/engine.sqlite")).unwrap();
        assert_eq!(store.read(|c| c.query_row("SELECT count(*) FROM calls", [], |r| r.get::<_, i64>(0))).unwrap(), 1);
        assert_eq!(store.read(|c| crate::store::systems::meta_get(c, "host_name")).unwrap(), Some("test".into()));
    }

    #[test]
    fn an_audio_chunk_becomes_a_frame_for_the_stores_system_id() {
        let dir = testing::temp_dir("reporter-audio");
        let call =
            CallInfo { id: 7, system: 0, short_name: "sys1".into(), talkgroup: 9178, recording: true, start_time: 1_700_000_000.0, ..Default::default() };
        let out = testing::run::<Reporter>([
            HostMessage::Hello(testing::hello(&dir, json!({"transcribeUrl": "", "port": 0, "backfill": false}))),
            HostMessage::CallStart(call),
            HostMessage::Audio(trunk_recorder_plugin::AudioChunk::new(7, 0, 9178, 8000, &[1, -1, 300])),
        ]);
        assert!(out.ready());
        let mut rx = AUDIO_TAP.with(|t| t.borrow_mut().take()).expect("the plugin's bus was tapped");
        let frame = rx.try_recv().expect("one frame");
        assert!(rx.try_recv().is_err(), "only one frame");
        let store = Store::open(&dir.join("data/engine.sqlite")).unwrap();
        let sys1: i64 = store.read(|c| c.query_row("SELECT id FROM systems WHERE short_name = 'sys1'", [], |r| r.get(0))).unwrap();
        assert_eq!((frame.system_id, frame.tgid, frame.sample_rate), (sys1, 9178, 8000));
        assert_eq!(&frame.pcm[..], &[1, 0, 255, 255, 44, 1]);
    }

    #[test]
    fn without_a_port_it_stores_but_does_not_serve() {
        let dir = testing::temp_dir("reporter-noport");
        let out = testing::run::<Reporter>([HostMessage::Hello(testing::hello(&dir, json!({"transcribeUrl": "", "port": 0, "backfill": false})))]);
        assert!(out.ready());
        assert_eq!(out.status(), Some((State::Ok, "not serving (Port is 0) · 0 calls stored".into())));
        assert!(!out.logs().iter().any(|(_, m)| m.contains("serving tr-engine")));
    }

    #[test]
    fn a_taken_port_stops_the_start_with_a_settings_error() {
        let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = busy.local_addr().unwrap().port();
        let dir = testing::temp_dir("reporter-busy");
        let out = testing::run::<Reporter>([HostMessage::Hello(testing::hello(&dir, json!({"transcribeUrl": "", "port": port, "backfill": false})))]);
        assert!(!out.ready());
        let (state, msg) = out.status().unwrap();
        assert_eq!(state, State::Error);
        assert!(msg.contains(&format!("port {port} is in use")), "{msg}");
    }

    fn setup(dir: &Path, config: Config) -> Setup<Config, SystemConfig> {
        let system: System<Config, NoConfig> = System { index: 0, short_name: "sys1".into(), kind: "p25".into(), config: None, settings: config.clone() };
        Setup {
            config,
            systems: vec![system],
            host: HostInfo { name: "test".into(), version: "0".into() },
            capture_dir: dir.to_path_buf(),
            data_dir: dir.join("data"),
            audio_formats: vec!["wav".into()],
        }
    }

    #[test]
    fn backfill_runs_beside_the_plugin_and_ends_on_the_ok_line() {
        let dir = testing::temp_dir("reporter-backfill");
        testing::call(&dir, "sys1", 101);
        testing::call(&dir, "sys1", 102);
        let (host, captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, Config { port: 0, transcribe_url: String::new(), ..Config::default() })).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while captured.output().status().is_some_and(|(s, _)| s == State::Warning) {
            assert!(Instant::now() < deadline, "the backfill did not finish");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(captured.output().status(), Some((State::Ok, "not serving (Port is 0) · 2 calls stored".into())));
        assert!(captured.output().logs().iter().any(|(_, m)| m.contains("backfill: 2 calls added from 2 files")));
        let mark = crate::store::systems::meta_get;
        assert_eq!(plugin.store.read(|c| mark(c, crate::backfill::COMPLETE_KEY)).unwrap().as_deref(), Some("1"));
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn a_call_left_recording_by_the_last_run_is_ended_at_start() {
        let dir = testing::temp_dir("reporter-stale");
        let db = dir.join("data").join("engine.sqlite");
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        let store = Store::open(&db).unwrap();
        store
            .write_sync(|c| {
                c.execute("INSERT INTO systems (short_name, kind, first_seen_ms, last_seen_ms) VALUES ('sys1', 'p25', 0, 0)", [])?;
                c.execute("INSERT INTO calls (system_id, call_num, tgid, start_ms, state) VALUES (1, 7, 101, ?1, 'recording')", [now_ms()])
            })
            .unwrap();
        drop(store);
        let (host, _captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, Config { port: 0, transcribe_url: String::new(), backfill: false, ..Config::default() })).unwrap();
        let state: String = plugin.store.read(|c| c.query_row("SELECT state FROM calls", [], |r| r.get(0))).unwrap();
        assert_eq!(state, "ended");
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn a_status_message_marks_the_recorder_seen() {
        let dir = testing::temp_dir("reporter-seen");
        let (host, _captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, Config { port: 0, transcribe_url: String::new(), backfill: false, ..Config::default() })).unwrap();
        plugin.host_status.0.lock().unwrap().last_seen_ms = None;
        plugin.status(Status::default());
        let seen = plugin.host_status.snapshot();
        assert!(seen.connected && seen.last_seen_ms.is_some_and(|t| t > now_ms() - 5_000));
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn the_status_line_mentions_transcripts_when_bridged() {
        assert_eq!(status_line(Some("http://127.0.0.1:8092/"), 3, None), "http://127.0.0.1:8092/ · 3 calls stored");
        assert_eq!(status_line(Some("http://127.0.0.1:8092/"), 3, Some(true)), "http://127.0.0.1:8092/ · 3 calls stored · transcripts: live");
        assert_eq!(status_line(None, 0, Some(false)), "not serving (Port is 0) · 0 calls stored · transcripts: unreachable");
    }

    #[test]
    fn the_status_line_follows_the_relay_once_per_change() {
        // Bridge off: never.
        assert_eq!(relay_change(None, None), None);
        // The first tick reports, whatever the state.
        assert_eq!(relay_change(Some(false), None), Some(false));
        // Unchanged: nothing; changed: the new state.
        assert_eq!(relay_change(Some(false), Some(false)), None);
        assert_eq!(relay_change(Some(true), Some(false)), Some(true));
        assert_eq!(relay_change(Some(true), Some(true)), None);
        assert_eq!(relay_change(Some(false), Some(true)), Some(false));
    }

    #[test]
    fn metrics_say_what_is_stored_and_where_it_is_served() {
        let dir = testing::temp_dir("reporter-metrics");
        let db = dir.join("engine.sqlite");
        let store = Store::open(&db).unwrap();
        let m = metrics(&store, &db, Some("http://127.0.0.1:8092/"), 3, calls_stored(&store), None);
        assert_eq!(m.queued, Some(0));
        assert_eq!(m.extra["calls"], 0);
        assert_eq!(m.extra["serving"], "http://127.0.0.1:8092/");
        assert_eq!(m.extra["dropped_events"], 3);
        assert!(m.extra["store_mb"].as_f64().is_some_and(|mb| mb >= 0.0));
        assert_eq!(metrics(&store, &db, None, 0, 7, None).extra["serving"], "not serving");
        assert_eq!(metrics(&store, &db, None, 0, 7, None).extra["calls"], 7);
        assert!(!m.extra.contains_key("transcripts_relayed"));
        assert_eq!(metrics(&store, &db, None, 0, 7, Some(&RelayStatus::default())).extra["transcripts_relayed"], 0);
    }
}
