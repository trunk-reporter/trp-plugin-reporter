//! The plugin's settings: the recorder draws a form from [`Config`].

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reporter has no per-system settings.
pub type SystemConfig = trunk_recorder_plugin::NoConfig;

/// The plugin's settings. Doc comments become the form's labels and help text.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct Config {
    /// Port
    pub port: u16,
    /// Bind address
    pub bind: String,
    /// Access token
    ///
    /// Leave empty for no login. When set, clients send it as a bearer token.
    #[schemars(extend("x-secret" = true))]
    pub token: Option<String>,
    /// Keep history for (days)
    ///
    /// 0 keeps everything.
    pub retention_days: u32,
    /// Backfill from recorded calls on start
    pub backfill: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config { port: 8092, bind: "127.0.0.1".into(), token: None, retention_days: 0, backfill: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_start_without_settings() {
        let c: Config = serde_json::from_value(json!({})).unwrap();
        assert_eq!((c.port, c.bind.as_str(), c.token, c.retention_days, c.backfill), (8092, "127.0.0.1", None, 0, true));
    }

    #[test]
    fn the_settings_form_hides_the_token() {
        let s = trunk_recorder_plugin::schema::schema_for::<Config>();
        assert_eq!(s["properties"]["token"]["x-secret"], true);
        assert_eq!(s["properties"]["port"]["default"], 8092);
    }
}
