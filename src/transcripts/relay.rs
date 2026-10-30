//! The relay: the transcribe plugin's event stream read and each `ok` transcript
//! published on the bus as a `transcription` event for its call.
//!
//! It reconnects after any error or end of the stream (1 s, doubling to 30 s, back to
//! 1 s once a frame arrives) and asks for what it missed with `Last-Event-ID`. A
//! transcript whose call is not stored yet is looked up again after 2 s and 10 s,
//! without holding up the frames behind it, then dropped.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::watch;
use tokio::time::Instant;

use super::client::TranscribeClient;
use super::{SseParser, Throttle, Transcript, is_ok, normalize_path};
use crate::api::{Log, rfc3339};
use crate::bus::{Bus, EventKind, NewEvent};
use crate::store::Store;
use crate::store::calls::{CallRow, call_by_path};

/// A stream with no frame (pings included) for this long is dead.
const IDLE: Duration = Duration::from_secs(45);
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// The waits before the second and third lookups of a call not yet stored.
const RETRIES: [Duration; 2] = [Duration::from_secs(2), Duration::from_secs(10)];
/// How many published `(path, transcribedMs)` pairs are remembered, so a replay is not published twice.
const SEEN: usize = 256;
/// The "no such call" line is logged at most this often.
const DROPPED_LOG_EVERY: Duration = Duration::from_secs(60);

#[derive(Default)]
struct RelayInner {
    connected: bool,
    last_event_ms: Option<i64>,
    relayed: u64,
}

/// How the relay stands, for `/health`, the status line and the metrics. Cheap to clone.
#[derive(Clone, Default)]
pub struct RelayStatus(Arc<Mutex<RelayInner>>);

impl RelayStatus {
    fn lock(&self) -> MutexGuard<'_, RelayInner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether the plugin's event stream is open.
    pub fn connected(&self) -> bool {
        self.lock().connected
    }

    /// The `transcribedMs` of the last transcript published.
    pub fn last_event_ms(&self) -> Option<i64> {
        self.lock().last_event_ms
    }

    /// How many transcription events were published.
    pub fn relayed(&self) -> u64 {
        self.lock().relayed
    }

    fn set_connected(&self, connected: bool) {
        self.lock().connected = connected;
    }
}

/// A transcript whose call was not found, waiting for its next lookup.
struct Pending {
    transcript: Transcript,
    due: Instant,
    /// Which of [`RETRIES`] comes after this lookup.
    next: usize,
}

/// What the loop keeps between frames and connections.
struct Relay {
    store: Store,
    bus: Bus,
    status: RelayStatus,
    log: Log,
    seen: VecDeque<(String, i64)>,
    pending: Vec<Pending>,
    throttle: Throttle,
}

/// A wait ended by a stop, not by its future.
struct Stopped;

/// Resolves once `stop` is `true` or its sender is gone.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

impl Relay {
    fn key(t: &Transcript) -> (String, i64) {
        (normalize_path(&t.path), t.transcribed_ms)
    }

    /// Awaits `fut`, running the lookups that fall due meanwhile; [`Stopped`] when told to stop.
    async fn wait<F: Future>(&mut self, fut: F, stop: &mut watch::Receiver<bool>) -> Result<F::Output, Stopped> {
        tokio::pin!(fut);
        loop {
            let due = self.pending.iter().map(|p| p.due).min();
            tokio::select! {
                out = &mut fut => return Ok(out),
                _ = stopped(stop) => return Err(Stopped),
                _ = async { tokio::time::sleep_until(due.unwrap_or_else(Instant::now)).await }, if due.is_some() => self.retry_due().await,
            }
        }
    }

    async fn lookup(&self, path: &str) -> Option<CallRow> {
        let (store, path) = (self.store.clone(), path.to_string());
        tokio::task::spawn_blocking(move || store.read(|c| call_by_path(c, &path))).await.ok()?.ok()?
    }

    /// A transcript from the stream: published when it counts, is new and its call is stored.
    async fn transcript(&mut self, t: Transcript) {
        let key = Relay::key(&t);
        if !is_ok(&t) || self.seen.contains(&key) || self.pending.iter().any(|p| Relay::key(&p.transcript) == key) {
            return;
        }
        match self.lookup(&t.path).await {
            Some(row) => self.publish(t, &row),
            None => self.pending.push(Pending { transcript: t, due: Instant::now() + RETRIES[0], next: 1 }),
        }
    }

    /// Looks up every transcript now due again; one still without its call waits for the next retry or is dropped.
    async fn retry_due(&mut self) {
        let now = Instant::now();
        let (due, waiting): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut self.pending).into_iter().partition(|p| p.due <= now);
        self.pending = waiting;
        for p in due {
            match self.lookup(&p.transcript.path).await {
                Some(row) => self.publish(p.transcript, &row),
                None if p.next < RETRIES.len() => self.pending.push(Pending { due: Instant::now() + RETRIES[p.next], next: p.next + 1, ..p }),
                None => {
                    if self.throttle.allow("dropped") {
                        (self.log)(format!("transcript for {}: no such call, dropped", p.transcript.path));
                    }
                }
            }
        }
    }

    fn publish(&mut self, t: Transcript, row: &CallRow) {
        let data = json!({
            "call_id": row.id.0,
            "system_id": row.system_id.0,
            "tgid": row.tgid,
            "text": t.text,
            "start_time": rfc3339(row.start_ms),
        });
        self.bus.publish(NewEvent {
            kind: EventKind::Transcription,
            sub_type: None,
            system_id: Some(row.system_id.0),
            site_id: row.site_id.map(|s| s.0),
            tgid: Some(row.tgid),
            unit_id: None,
            emergency: row.emergency,
            data: data.to_string(),
        });
        let mut s = self.status.lock();
        s.relayed += 1;
        s.last_event_ms = Some(t.transcribed_ms);
        drop(s);
        self.seen.push_back(Relay::key(&t));
        while self.seen.len() > SEEN {
            self.seen.pop_front();
        }
    }
}

/// Relays the plugin's event stream onto `bus` until `stop` becomes `true` (or its
/// sender is dropped). The first connection asks for no replay; each reconnect sends
/// the last `transcribedMs` seen as `Last-Event-ID`. Logs through `log` only when the
/// connection's state changes, and a dropped transcript at most once a minute.
pub async fn run_relay(client: Arc<TranscribeClient>, store: Store, bus: Bus, status: RelayStatus, log: Log, mut stop: watch::Receiver<bool>) {
    let mut relay = Relay { store, bus, status, log, seen: VecDeque::new(), pending: Vec::new(), throttle: Throttle::new(DROPPED_LOG_EVERY) };
    let mut last_id: Option<i64> = None;
    let mut backoff = FIRST_BACKOFF;
    // Whether "not reachable" was logged since the last connection.
    let mut failing = false;
    loop {
        let Ok(opened) = relay.wait(client.events(last_id), &mut stop).await else { return };
        match opened {
            Ok(resp) => {
                relay.status.set_connected(true);
                failing = false;
                (relay.log)("transcribe plugin event stream connected".into());
                let mut body = resp.bytes_stream();
                let mut parser = SseParser::default();
                // An error, the end of the body or 45 s of silence ends the connection; the
                // reconnect then says "connected" again or "not reachable".
                loop {
                    let Ok(next) = relay.wait(tokio::time::timeout(IDLE, body.next()), &mut stop).await else {
                        relay.status.set_connected(false);
                        return;
                    };
                    let Ok(Some(Ok(chunk))) = next else { break };
                    for frame in parser.feed(&chunk) {
                        backoff = FIRST_BACKOFF;
                        if frame.event.as_deref() != Some("transcript") {
                            continue;
                        }
                        let Ok(t) = serde_json::from_str::<Transcript>(&frame.data) else { continue };
                        last_id = Some(last_id.map_or(t.transcribed_ms, |id| id.max(t.transcribed_ms)));
                        relay.transcript(t).await;
                    }
                }
                relay.status.set_connected(false);
            }
            Err(e) => {
                if !failing {
                    failing = true;
                    (relay.log)(format!("transcribe plugin not reachable at {}: {e}", client.base()));
                }
            }
        }
        if relay.wait(tokio::time::sleep(backoff), &mut stop).await.is_err() {
            return;
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Runs [`run_relay`] on a thread of its own (`transcripts`, with a current-thread
/// runtime), so it needs neither the server nor its runtime. Sending `true` on the
/// returned sender, or dropping it, stops it; the thread then ends.
pub fn spawn_relay(client: Arc<TranscribeClient>, store: Store, bus: Bus, log: Log) -> (RelayStatus, watch::Sender<bool>, std::thread::JoinHandle<()>) {
    let status = RelayStatus::default();
    let (stop, stopped) = watch::channel(false);
    let s = status.clone();
    let thread = std::thread::Builder::new()
        .name("transcripts".into())
        .spawn(move || match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt.block_on(run_relay(client, store, bus, s, log.clone(), stopped)),
            Err(e) => log(format!("cannot start the transcripts relay: {e}")),
        })
        .expect("a thread for the transcripts relay");
    (status, stop, thread)
}
