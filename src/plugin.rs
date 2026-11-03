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
use trunk_recorder_plugin::{
    AudioChunk, CallInfo, CallsPage, ConcludedCall, Host, HostMessage, Manifest, Metrics, Plugin, Setup, State, Status, TonePatternEvent, UnitEvent, topic,
};

use crate::affiliations::Affiliations;
use crate::api::{AppState, HostStatus};
use crate::audio::{AudioBus, AudioFrame, BUS_CAPACITY};
use crate::backfill::backfill;
use crate::bus::Bus;
use crate::config::{Config, SystemConfig};
use crate::ingest::Ingest;
use crate::recall::Recall;
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
/// How long a server being replaced (new settings) waits for its open requests.
const RESTART_STOP: Duration = Duration::from_millis(500);
/// How long a relay being replaced is waited for.
const RELAY_STOP: Duration = Duration::from_secs(1);

pub struct Reporter {
    host: Host,
    ingest: Ingest,
    store: Store,
    bus: Bus,
    affiliations: Affiliations,
    capture_dir: PathBuf,
    /// The store's file.
    db: PathBuf,
    /// The settings it runs with: the start's, then each change it took.
    config: Config,
    /// What `/health` says of the recorder: touched on every `status`.
    host_status: HostStatus,
    /// When the plugin started (the API's uptime, across server restarts).
    started: Instant,
    server: Option<ServerHandle>,
    maintenance: Option<(mpsc::Sender<()>, JoinHandle<()>)>,
    /// Set at shutdown: a backfill still walking stops (and stays incomplete).
    stop_backfill: Arc<AtomicBool>,
    /// Set while a backfill runs: it owns the status line, and maintenance leaves it alone.
    backfilling: Arc<AtomicBool>,
    /// The recorder's live audio, for `/audio/live`'s clients.
    audio: AudioBus,
    /// The relay of the transcribe plugin's event stream, when the bridge is on.
    relay: Option<Relay>,
    /// The backfill from the recorder's call store, while it walks.
    recall: Option<Recall>,
}

/// The transcripts bridge: the transcribe plugin's client, and the relay of its event stream.
struct Relay {
    client: Arc<TranscribeClient>,
    status: RelayStatus,
    stop: watch::Sender<bool>,
    thread: JoinHandle<()>,
}

impl Relay {
    /// The relay of `url`'s events, or none when the URL is empty (the bridge off).
    fn start(host: &Host, store: &Store, bus: &Bus, url: &str) -> Result<Option<Relay>, String> {
        let url = url.trim();
        if url.is_empty() {
            return Ok(None);
        }
        let client = Arc::new(TranscribeClient::new(url)?);
        host.info(format!("reading transcripts from {}", client.base()));
        let h = host.clone();
        let (status, stop, thread) = spawn_relay(client.clone(), store.clone(), bus.clone(), Arc::new(move |m| h.info(m)));
        Ok(Some(Relay { client, status, stop, thread }))
    }

    /// Stop it, waiting for its thread until `deadline`; one still running then is left to end at its next await.
    fn stop(self, deadline: Instant) {
        let _ = self.stop.send(true);
        while !self.thread.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if self.thread.is_finished() {
            let _ = self.thread.join();
        }
    }
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

/// Whether the status line should be set again: it hasn't been (`reported`
/// is `None`), or the transcripts relay's stream opened or closed since
/// (`relay`: whether it is open, `None` when the bridge is off).
fn status_due(relay: Option<bool>, reported: Option<Option<bool>>) -> bool {
    reported != Some(relay)
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
/// deleted daily (both first a minute after start). The status line is set on the
/// first tick and again whenever the transcripts relay's stream opens or closes,
/// except while a backfill (`backfilling`) owns the status (and once it is over).
/// Stops when the sender is used or dropped.
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
            // The relay state the status line last showed (`None`: not set yet).
            let mut reported: Option<Option<bool>> = None;
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
                let connected = relay.as_ref().map(RelayStatus::connected);
                if backfilling.load(Ordering::Relaxed) {
                    reported = None;
                } else if status_due(connected, reported) {
                    host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), connected));
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
                topic::TONE_PATTERN.into(),
            ],
            // `--describe` has no settings, so this is the default port's URL;
            // a changed port is only in the log and status line.
            homepage: "http://127.0.0.1:8092/".into(),
            // New settings reach `settings` instead of restarting the plugin (recorders from 0.3.0).
            live_settings: true,
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
        let walker = if setup.config.backfill { Some(new_ingest()?) } else { None };
        let recaller = if setup.config.backfill { Some(new_ingest()?) } else { None };
        let recent = store.read(|c| affiliation_events_since(c, now_ms() - WEEK_MS)).map_err(|e| e.to_string())?;
        affiliations.rebuild(&recent);

        let audio = AudioBus::new(BUS_CAPACITY);
        #[cfg(test)]
        AUDIO_TAP.with(|t| *t.borrow_mut() = Some(audio.subscribe()));
        // The relay runs whether or not the API is served. An early return below
        // drops its sender, which stops it.
        let relay = Relay::start(&host, &store, &bus, &setup.config.transcribe_url)?;
        let host_status = HostStatus::new(&setup.host.name, true);
        host_status.touch(now_ms());
        let mut reporter = Reporter {
            host: host.clone(),
            ingest,
            store: store.clone(),
            bus,
            affiliations,
            capture_dir: setup.capture_dir.clone(),
            db: path,
            config: setup.config,
            host_status,
            started: Instant::now(),
            server: None,
            maintenance: None,
            stop_backfill: Arc::new(AtomicBool::new(false)),
            backfilling: Arc::new(AtomicBool::new(walker.is_some())),
            audio,
            relay,
            recall: None,
        };
        let config = reporter.config.clone();
        if config.port != 0 {
            reporter.server = Some(server::start(reporter.app_state(&config), &config.bind, config.port)?);
        }
        let serving = reporter.serving();
        if let Some(url) = &serving {
            host.info(format!("serving tr-engine's API and pages at {url}"));
        }

        let relay_status = reporter.relay.as_ref().map(|r| r.status.clone());
        // The status is set before the backfill starts, so its own (the
        // last word once it ends) cannot be overwritten by this one.
        if let Some(mut walker) = walker {
            host.status(State::Warning, "backfilling: 0 calls");
            let systems: Vec<_> = store.read(list_systems).map_err(|e| e.to_string())?.into_iter().map(|s| (s.id, s.short_name)).collect();
            let (host, capture_dir, stop, backfilling) = (host.clone(), setup.capture_dir, reporter.stop_backfill.clone(), reporter.backfilling.clone());
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
                    // (Where it is served as it started: maintenance says it again, as it is now, at its next tick.)
                    host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), relay_status.as_ref().map(RelayStatus::connected)));
                    backfilling.store(false, Ordering::Relaxed);
                })
                .map_err(|e| format!("cannot start the backfill: {e}"))?;
        } else {
            host.status(State::Ok, status_line(serving.as_deref(), calls_stored(&store), relay_status.as_ref().map(RelayStatus::connected)));
        }

        // Beside the folder walk: the recorder's call store has the calls whose JSON isn't kept.
        if let Some(ingest) = recaller {
            reporter.recall = Some(Recall::start(host.clone(), ingest, reporter.capture_dir.clone(), reporter.stop_backfill.clone())?);
        }
        reporter.start_maintenance()?;
        Ok(reporter)
    }

    /// New settings, taken without a restart. A bad bind address or
    /// transcribe URL, or a port that is in use, leaves the settings it had,
    /// served as they were. A server that moves or changes its token or
    /// origins closes its open event streams and live audio (clients reconnect).
    /// "Backfill on start" waits for the next start.
    fn settings(&mut self, setup: Setup<Config, SystemConfig>) -> Result<(), String> {
        self.ingest.declare(&setup.systems).map_err(|e| e.to_string())?;
        let new = setup.config;
        let old = &self.config;
        let bridge_changes = new.transcribe_url.trim() != old.transcribe_url.trim();
        let server_changes =
            bridge_changes || (new.port, new.bind.trim(), &new.token, new.allowed_origins()) != (old.port, old.bind.trim(), &old.token, old.allowed_origins());
        if server_changes {
            // What can be checked is, before anything stops.
            if bridge_changes && !new.transcribe_url.trim().is_empty() {
                TranscribeClient::new(&new.transcribe_url)?;
            }
            if new.port != 0 {
                server::bind_ip(&new.bind)?;
            }
            let listener = self.rebind(&new)?;
            // Nothing fails from here.
            if bridge_changes {
                if let Some(relay) = self.relay.take() {
                    relay.stop(Instant::now() + RELAY_STOP);
                }
                self.relay = Relay::start(&self.host, &self.store, &self.bus, &new.transcribe_url)?;
            }
            if let Some(listener) = listener {
                self.server = Some(server::serve_on(self.app_state(&new), listener)?);
            }
            match self.serving() {
                Some(url) => self.host.info(format!("serving tr-engine's API and pages at {url}")),
                None => self.host.info("not serving (Port is 0)"),
            }
        }
        self.config = new;
        // Its retention, and where it serves, as they are now; it sets the status line again.
        self.start_maintenance()
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

    fn tone_pattern(&mut self, t: TonePatternEvent) {
        self.ingest.handle(HostMessage::TonePattern(t));
    }

    /// A page of the recorder's calls, for the backfill that asked.
    fn calls(&mut self, request: u64, page: CallsPage) {
        if let Some(recall) = &self.recall {
            recall.page(request, page);
        }
    }

    fn status(&mut self, status: Status) {
        self.host_status.touch(now_ms());
        self.ingest.handle(HostMessage::Status(status));
    }

    /// A running backfill is told to stop (the call store's waited for within `grace`), then maintenance, then the transcripts relay (waited
    /// for within `grace`), then the server (at most 2 s of `grace`), then every
    /// queued write. The flush runs even when the grace is spent: the
    /// history matters more than the deadline.
    fn shutdown(&mut self, grace: Duration) {
        let deadline = Instant::now() + grace;
        self.stop_backfill.store(true, Ordering::Relaxed);
        if let Some(recall) = self.recall.take() {
            recall.stop(deadline);
        }
        self.stop_maintenance();
        if let Some(relay) = self.relay.take() {
            relay.stop(deadline);
        }
        if let Some(server) = self.server.take() {
            server.stop_within(deadline.saturating_duration_since(Instant::now()).min(SERVER_STOP));
        }
        self.store.flush();
    }
}

impl Reporter {
    /// Where the API is served, when it is.
    fn serving(&self) -> Option<String> {
        self.server.as_ref().map(ServerHandle::url)
    }

    /// The API's state for `config`, with the transcripts bridge as it is now.
    fn app_state(&self, config: &Config) -> AppState {
        let mut state = AppState::new(
            self.store.clone(),
            self.bus.clone(),
            self.affiliations.clone(),
            self.capture_dir.clone(),
            config.token.clone(),
            self.host_status.clone(),
        );
        state.started = self.started;
        if let Some(relay) = &self.relay {
            state = state.with_transcribe_client(relay.client.clone());
            state.relay = Some(relay.status.clone());
        }
        let log = self.host.clone();
        state.cors_origins = config.allowed_origins();
        state.log = Arc::new(move |m| log.error(m));
        state.audio = Some(self.audio.clone());
        state
    }

    /// The old server stopped and a socket for `new`'s address bound (none
    /// for port 0). A new port is bound while the old server still serves;
    /// the same port, once it has let go. A failed bind leaves the old server
    /// serving (started again on its address, when it had let go).
    fn rebind(&mut self, new: &Config) -> Result<Option<std::net::TcpListener>, String> {
        if new.port == 0 {
            if let Some(old) = self.server.take() {
                old.stop_within(RESTART_STOP);
            }
            return Ok(None);
        }
        if self.server.as_ref().is_none_or(|s| s.addr.port() != new.port) {
            let listener = server::listen(&new.bind, new.port)?;
            if let Some(old) = self.server.take() {
                old.stop_within(RESTART_STOP);
            }
            return Ok(Some(listener));
        }
        if let Some(old) = self.server.take() {
            old.stop_within(RESTART_STOP);
        }
        server::listen(&new.bind, new.port).map(Some).map_err(|e| {
            let old = self.config.clone();
            match server::start(self.app_state(&old), &old.bind, old.port) {
                Ok(server) => {
                    self.server = Some(server);
                    e
                }
                Err(again) => format!("{e}; and serving as before failed: {again}"),
            }
        })
    }

    fn stop_maintenance(&mut self) {
        if let Some((stop, thread)) = self.maintenance.take() {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }

    /// Maintenance (re)started with the settings and server as they are now.
    fn start_maintenance(&mut self) -> Result<(), String> {
        self.stop_maintenance();
        self.maintenance = Some(maintenance(
            self.host.clone(),
            self.store.clone(),
            self.db.clone(),
            self.serving(),
            self.ingest.dropped_counter(),
            self.config.retention_days,
            self.relay.as_ref().map(|r| r.status.clone()),
            self.backfilling.clone(),
        )?);
        Ok(())
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
        assert!(m.subscribes("call.concluded") && m.subscribes("unit") && m.subscribes("status") && m.subscribes("audio") && m.subscribes("tone_pattern"));
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
        let system: System<Config, NoConfig> = System {
            index: 0,
            short_name: "sys1".into(),
            kind: "p25".into(),
            name: String::new(),
            fcc: Default::default(),
            fcc_call_sign: String::new(),
            fcc_name: String::new(),
            sites: Vec::new(),
            config: None,
            settings: config.clone(),
        };
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
    fn backfill_also_asks_the_recorders_call_store() {
        let dir = testing::temp_dir("reporter-recall");
        let (host, captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, Config { port: 0, transcribe_url: String::new(), ..Config::default() })).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let request = loop {
            let asked = captured.output().messages.iter().find_map(|m| match m {
                trunk_recorder_plugin::PluginMessage::CallsQuery { request, .. } => Some(*request),
                _ => None,
            });
            if let Some(r) = asked {
                break r;
            }
            assert!(Instant::now() < deadline, "the call store was not asked");
            std::thread::sleep(Duration::from_millis(5));
        };
        // A call whose JSON file the recorder didn't keep: only its store has it.
        let record = trunk_recorder_plugin::CallRecord { short_name: "sys1".into(), talkgroup: 101, start_time: 1_700_000_000, ..Default::default() };
        let call = trunk_recorder_plugin::StoredCall { path: "sys1/2023/11/14/101-1700000000_851012500".into(), record, audio: false, json: false };
        plugin.calls(request, CallsPage { entries: vec![call], ..Default::default() });
        while calls_stored(&plugin.store) == 0 {
            assert!(Instant::now() < deadline, "the call was not stored");
            std::thread::sleep(Duration::from_millis(5));
        }
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
    fn the_status_line_is_set_first_then_once_per_relay_change() {
        // The first tick sets it, bridge or not.
        assert!(status_due(None, None));
        assert!(status_due(Some(false), None));
        // Bridge off, once set: never again.
        assert!(!status_due(None, Some(None)));
        // Unchanged: nothing; changed: again.
        assert!(!status_due(Some(false), Some(Some(false))));
        assert!(status_due(Some(true), Some(Some(false))));
        assert!(!status_due(Some(true), Some(Some(true))));
        assert!(status_due(Some(false), Some(Some(true))));
    }

    /// A port nothing listens on (as far as the OS knows just now).
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    /// The status of `GET path` on 127.0.0.1:`port` (`None`: nothing listens).
    fn get(port: u16, path: &str) -> Option<u16> {
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
        write!(s, "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").ok()?;
        let mut head = String::new();
        s.read_to_string(&mut head).ok()?;
        head.get(9..12)?.parse().ok()
    }

    fn quiet(port: u16) -> Config {
        Config { port, transcribe_url: String::new(), backfill: false, ..Config::default() }
    }

    #[test]
    fn the_manifest_takes_settings_live() {
        assert!(trunk_recorder_plugin::describe::<Reporter>().live_settings);
    }

    #[test]
    fn new_settings_move_the_server_and_its_token_without_a_restart() {
        let dir = testing::temp_dir("reporter-live");
        let (a, b) = (free_port(), free_port());
        let (host, captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, quiet(a))).unwrap();
        assert_eq!(get(a, "/api/v1/calls"), Some(200));
        let started = plugin.started;

        // Another port: served there, not here.
        plugin.settings(setup(&dir, quiet(b))).unwrap();
        assert_eq!((get(a, "/api/v1/calls"), get(b, "/api/v1/calls")), (None, Some(200)));
        assert!(captured.output().logs().iter().any(|(_, m)| m.contains(&format!("serving tr-engine's API and pages at http://127.0.0.1:{b}/"))));

        // A token, on the same port.
        plugin.settings(setup(&dir, Config { token: Some("s3cret".into()), ..quiet(b) })).unwrap();
        assert_eq!(get(b, "/api/v1/calls"), Some(401));

        // Retention: the server is left as it is.
        plugin.settings(setup(&dir, Config { token: Some("s3cret".into()), retention_days: 30, ..quiet(b) })).unwrap();
        assert_eq!((plugin.config.retention_days, get(b, "/api/v1/calls")), (30, Some(401)));

        // Port 0: stored, not served.
        plugin.settings(setup(&dir, quiet(0))).unwrap();
        assert_eq!(get(b, "/api/v1/calls"), None);
        assert!(plugin.server.is_none());
        assert_eq!(plugin.started, started);
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn settings_that_cannot_be_served_leave_it_serving_as_before() {
        let dir = testing::temp_dir("reporter-live-bad");
        let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taken = busy.local_addr().unwrap().port();
        let a = free_port();
        let (host, _captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, quiet(a))).unwrap();

        let err = plugin.settings(setup(&dir, quiet(taken))).unwrap_err();
        assert!(err.contains(&format!("port {taken} is in use")), "{err}");
        let err = plugin.settings(setup(&dir, Config { bind: "localhost".into(), ..quiet(a) })).unwrap_err();
        assert!(err.contains("not an IP address"), "{err}");
        assert_eq!(get(a, "/api/v1/calls"), Some(200));
        assert_eq!(plugin.config, quiet(a));

        // The same port, on an address it cannot have: served as before.
        let err = plugin.settings(setup(&dir, Config { bind: "192.0.2.1".into(), ..quiet(a) })).unwrap_err();
        assert!(err.contains("cannot listen on 192.0.2.1"), "{err}");
        assert_eq!(get(a, "/api/v1/calls"), Some(200));
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn new_settings_name_the_systems_again() {
        let dir = testing::temp_dir("reporter-live-names");
        let (host, _captured) = testing::capture();
        let mut plugin = Reporter::start(host, setup(&dir, quiet(0))).unwrap();
        let mut s = setup(&dir, quiet(0));
        s.systems[0].name = "County".into();
        plugin.settings(s).unwrap();
        let name: String = plugin.store.read(|c| c.query_row("SELECT name FROM systems WHERE short_name = 'sys1'", [], |r| r.get(0))).unwrap();
        assert_eq!(name, "County");
        plugin.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn a_settings_message_reaches_the_running_plugin() {
        let dir = testing::temp_dir("reporter-live-msg");
        let mut changed = testing::hello(&dir, json!({"transcribeUrl": "", "port": 0, "backfill": false, "retentionDays": 7}));
        changed.systems[0].name = "County".into();
        let out = testing::run::<Reporter>([
            HostMessage::Hello(testing::hello(&dir, json!({"transcribeUrl": "", "port": 0, "backfill": false}))),
            HostMessage::Settings(changed),
        ]);
        assert!(out.ready());
        assert_eq!(out.status().map(|(s, _)| s), Some(State::Ok));
        let store = Store::open(&dir.join("data/engine.sqlite")).unwrap();
        let name: String = store.read(|c| c.query_row("SELECT name FROM systems WHERE short_name = 'sys1'", [], |r| r.get(0))).unwrap();
        assert_eq!(name, "County");
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
