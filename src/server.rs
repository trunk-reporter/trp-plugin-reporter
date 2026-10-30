//! The HTTP server: the API and pages on their own thread, and the set-up of
//! `serve <data dir>`.

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::affiliations::Affiliations;
use crate::api::{AppState, Features, HostStatus, router};
use crate::bus::Bus;
use crate::config::Config;
use crate::store::systems::meta_get;
use crate::store::units::affiliation_events_since;
use crate::store::{Store, now_ms};

/// Events the SSE stream can replay to a client that reconnects.
pub const BUS_RING: usize = 1_000;
/// How far back the affiliation map is rebuilt from at start.
pub const WEEK_MS: i64 = 7 * 86_400_000;
/// How long [`ServerHandle::stop`] waits for open connections.
const STOP_WAIT: Duration = Duration::from_secs(2);

/// A running server.
pub struct ServerHandle {
    /// Where it listens (the real port when 0 was asked for).
    pub addr: SocketAddr,
    stop: oneshot::Sender<()>,
    done: mpsc::Receiver<()>,
    thread: JoinHandle<()>,
}

impl ServerHandle {
    /// `http://<addr>/`.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// Stop taking connections and let open ones finish, for up to 2 s.
    pub fn stop(self) {
        self.stop_within(STOP_WAIT)
    }

    /// As [`ServerHandle::stop`], waiting at most `wait`. A connection still
    /// open then (an event stream) is left to the process's exit.
    pub fn stop_within(self, wait: Duration) {
        let _ = self.stop.send(());
        if self.done.recv_timeout(wait).is_ok() {
            let _ = self.thread.join();
        }
    }
}

/// Listen on `bind:port` and serve `state` on a thread of its own. The socket
/// is bound before this returns, so a taken port is an error here, worded for
/// the settings form.
pub fn start(state: AppState, bind: &str, port: u16) -> Result<ServerHandle, String> {
    let ip: IpAddr = bind.trim().parse().map_err(|_| format!("bind address {bind} is not an IP address: change Bind address in the plugin's settings"))?;
    let listener = std::net::TcpListener::bind((ip, port)).map_err(|e| match e.kind() {
        std::io::ErrorKind::AddrInUse => format!("port {port} is in use: change Port in the plugin's settings"),
        _ => format!("cannot listen on {ip}:{port} ({e}): change Bind address or Port in the plugin's settings"),
    })?;
    listener.set_nonblocking(true).map_err(|e| format!("cannot listen on {ip}:{port}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| format!("cannot start the server: {e}"))?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (done_tx, done) = mpsc::channel();
    let log = state.log.clone();
    let app = router(state);
    let thread = std::thread::Builder::new()
        .name("http".into())
        .spawn(move || {
            runtime.block_on(async move {
                let served = match tokio::net::TcpListener::from_std(listener) {
                    Ok(l) => {
                        axum::serve(l, app)
                            .with_graceful_shutdown(async move {
                                stopped.await.ok();
                            })
                            .await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = served {
                    log(format!("the HTTP server stopped: {e}"));
                }
            });
            let _ = done_tx.send(());
        })
        .map_err(|e| format!("cannot start the server: {e}"))?;
    Ok(ServerHandle { addr, stop, done, thread })
}

/// `serve <data dir>`: the history a plugin run left in `data_dir`, served with
/// `data_dir/serve.json`'s settings (the plugin's shape; defaults when absent)
/// and no recorder attached. Logs go to stderr.
pub fn serve_data_dir(data_dir: &Path) -> Result<ServerHandle, String> {
    let path = data_dir.join("engine.sqlite");
    let no_store = || format!("no store at {}: run the plugin once from Trunk Recorder Pro first", path.display());
    if !path.is_file() {
        return Err(no_store());
    }
    let settings = data_dir.join("serve.json");
    let config: Config = match std::fs::read_to_string(&settings) {
        Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{}: {e}", settings.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Config::default(),
        Err(e) => return Err(format!("cannot read {}: {e}", settings.display())),
    };
    let store = Store::open(&path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    store.set_logger(Arc::new(|m| eprintln!("{m}")));
    // Calls still `recording` are left alone: the plugin may be running on
    // this same file (only its own start ends them).
    let meta = |key: &str| store.read(|c| meta_get(c, key)).map_err(|e| e.to_string());
    let (Some(capture_dir), Some(host_name)) = (meta("capture_dir")?, meta("host_name")?) else {
        return Err(no_store());
    };
    let affiliations = Affiliations::new();
    affiliations.rebuild(&store.read(|c| affiliation_events_since(c, now_ms() - WEEK_MS)).map_err(|e| e.to_string())?);
    let mut state = AppState::new(store, Bus::new(BUS_RING), affiliations, capture_dir.into(), config.token.clone(), HostStatus::new(&host_name, false))
        .with_transcribe(&config.transcribe_url)?;
    state.features = Features::SERVE;
    state.cors_origins = config.allowed_origins();
    state.log = Arc::new(|m| eprintln!("{m}"));
    start(state, &config.bind, config.port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn test_state() -> AppState {
        AppState::new(Store::in_memory(), Bus::new(10), Affiliations::new(), std::env::temp_dir(), None, HostStatus::new("t", true))
    }

    #[test]
    fn a_taken_port_is_a_settings_error_not_a_crash() {
        let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = busy.local_addr().unwrap().port();
        let err = start(test_state(), "127.0.0.1", port).err().unwrap();
        assert!(err.contains(&port.to_string()) && err.contains("Port"), "{err}");
    }

    #[test]
    fn a_bind_address_must_be_an_ip() {
        let err = start(test_state(), "localhost", 0).err().unwrap();
        assert!(err.contains("not an IP address"), "{err}");
    }

    #[test]
    fn it_serves_until_stopped() {
        let server = start(test_state(), "127.0.0.1", 0).unwrap();
        let addr = server.addr;
        assert_ne!(addr.port(), 0);
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        write!(s, "GET /api/v1/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
        let mut body = String::new();
        s.read_to_string(&mut body).unwrap();
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");
        let t = std::time::Instant::now();
        server.stop();
        assert!(t.elapsed() < Duration::from_secs(3));
        assert!(std::net::TcpStream::connect(addr).is_err());
    }

    #[test]
    fn stop_does_not_wait_on_an_open_event_stream() {
        let server = start(test_state(), "127.0.0.1", 0).unwrap();
        let mut s = std::net::TcpStream::connect(server.addr).unwrap();
        write!(s, "GET /api/v1/events/stream HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let mut first = [0u8; 12];
        s.read_exact(&mut first).unwrap();
        assert_eq!(&first, b"HTTP/1.1 200");
        let t = std::time::Instant::now();
        server.stop_within(Duration::from_millis(300));
        assert!(t.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn serve_mode_needs_a_store_and_readable_settings() {
        let dir = trunk_recorder_plugin::testing::temp_dir("reporter-serve-unit");
        assert!(serve_data_dir(&dir).err().unwrap().contains("run the plugin once"));
        // A store no plugin run wrote (no recorder details) is no store either.
        drop(Store::open(&dir.join("engine.sqlite")).unwrap());
        assert!(serve_data_dir(&dir).err().unwrap().contains("run the plugin once"));
        std::fs::write(dir.join("serve.json"), "{\"port\": \"eighty\"}").unwrap();
        assert!(serve_data_dir(&dir).err().unwrap().contains("serve.json"));
    }

    #[test]
    fn serve_mode_leaves_a_live_call_alone() {
        let dir = trunk_recorder_plugin::testing::temp_dir("reporter-serve-live");
        let store = Store::open(&dir.join("engine.sqlite")).unwrap();
        store
            .write_sync(|c| {
                c.execute("INSERT INTO meta (key, value) VALUES ('capture_dir', '.'), ('host_name', 'test')", [])?;
                c.execute("INSERT INTO systems (short_name, kind, first_seen_ms, last_seen_ms) VALUES ('sys1', 'p25', 0, 0)", [])?;
                c.execute("INSERT INTO calls (system_id, call_num, tgid, start_ms, state) VALUES (1, 7, 101, 0, 'recording')", [])
            })
            .unwrap();
        std::fs::write(dir.join("serve.json"), "{\"port\": 0}").unwrap();
        let server = serve_data_dir(&dir).unwrap();
        let state: String = store.read(|c| c.query_row("SELECT state FROM calls", [], |r| r.get(0))).unwrap();
        assert_eq!(state, "recording");
        server.stop();
    }
}
