//! The plugin: its manifest, startup, and event handling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use trunk_recorder_plugin::{CallInfo, ConcludedCall, Host, HostMessage, Manifest, Plugin, Setup, State, Status, UnitEvent, topic};

use crate::affiliations::Affiliations;
use crate::bus::Bus;
use crate::config::{Config, SystemConfig};
use crate::ingest::Ingest;
use crate::store::systems::meta_set;
use crate::store::units::affiliation_events_since;
use crate::store::{Store, now_ms};

/// Events the SSE stream can replay to a client that reconnects.
const BUS_RING: usize = 1_000;
const WEEK_MS: i64 = 7 * 86_400_000;

pub struct Reporter {
    ingest: Ingest,
    // The server (Task 19) reports through these.
    #[allow(dead_code)]
    host: Host,
    store: Store,
    #[allow(dead_code)]
    started: Instant,
}

impl Plugin for Reporter {
    type Config = Config;
    type SystemConfig = SystemConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Reporter".into(),
            subscribe: vec![topic::CALL_START.into(), topic::CALL_END.into(), topic::CALL_CONCLUDED.into(), topic::UNIT.into(), topic::STATUS.into()],
            // `--describe` has no settings, so this is the default port's URL;
            // a changed port is only in the log and status line.
            homepage: "http://127.0.0.1:8092/".into(),
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        let started = Instant::now();
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
        let affiliations = Affiliations::new();
        let ingest = Ingest::new(store.clone(), Bus::new(BUS_RING), affiliations.clone(), Some(host.clone()), &setup.systems)
            .map_err(|e| e.to_string())?
            .with_capture_dir(setup.capture_dir.clone());
        let recent = store.read(|c| affiliation_events_since(c, now_ms() - WEEK_MS)).map_err(|e| e.to_string())?;
        affiliations.rebuild(&recent);
        host.status(State::Ok, "storing history in engine.sqlite");
        Ok(Reporter { ingest, host, store, started })
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

    fn status(&mut self, status: Status) {
        self.ingest.handle(HostMessage::Status(status));
    }

    fn shutdown(&mut self, _grace: Duration) {
        self.store.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_manifest_subscribes_to_what_phase_1_needs() {
        let m = trunk_recorder_plugin::describe::<Reporter>();
        assert_eq!(m.id, "reporter");
        assert!(m.subscribes("call.concluded") && m.subscribes("unit") && m.subscribes("status") && !m.subscribes("audio"));
        assert!(m.config.is_some() && m.system_config.is_none());
    }

    #[test]
    fn the_plugin_starts_stores_and_answers() {
        use serde_json::json;
        use trunk_recorder_plugin::{HostMessage, Outcome, testing};
        let dir = testing::temp_dir("reporter");
        let out = testing::run::<Reporter>([
            HostMessage::Hello(testing::hello(&dir, json!({"port": 0}))),
            HostMessage::CallConcluded(testing::call(&dir, "sys1", 101)),
        ]);
        assert!(out.ready());
        assert_eq!(out.results()[0].1, Outcome::Ok);
        assert!(dir.join("data/engine.sqlite").exists());
        let store = crate::store::Store::open(&dir.join("data/engine.sqlite")).unwrap();
        assert_eq!(store.read(|c| c.query_row("SELECT count(*) FROM calls", [], |r| r.get::<_, i64>(0))).unwrap(), 1);
        assert_eq!(store.read(|c| crate::store::systems::meta_get(c, "host_name")).unwrap(), Some("test".into()));
    }
}
