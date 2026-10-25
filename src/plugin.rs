//! The plugin: its manifest, and (later) startup and event handling.

use trunk_recorder_plugin::{Host, Manifest, Plugin, Setup, topic};

use crate::config::{Config, SystemConfig};

pub struct Reporter;

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

    fn start(_host: Host, _setup: Setup<Config, SystemConfig>) -> Result<Self, String> {
        Err("not built yet".into())
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
}
