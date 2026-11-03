//! The plugin's settings: the recorder draws a form from [`Config`].

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reporter has no per-system settings.
pub type SystemConfig = trunk_recorder_plugin::NoConfig;

/// The plugin's settings. Doc comments become the form's labels and help text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// Allowed origins
    ///
    /// Web pages served from these origins may call the API. One origin per line, such as http://192.168.1.20:8081.
    pub cors_origins: Vec<String>,
    /// Transcribe plugin URL
    ///
    /// Where the transcribe plugin serves its transcripts. Leave empty when it isn't installed.
    pub transcribe_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 8092,
            bind: "127.0.0.1".into(),
            token: None,
            retention_days: 0,
            backfill: true,
            cors_origins: Vec::new(),
            transcribe_url: "http://127.0.0.1:8767".into(),
        }
    }
}

impl Config {
    /// The allowed origins as a browser sends them: each entry without surrounding whitespace or a
    /// trailing `/` (as one copied from the address bar has), and blank entries dropped.
    pub fn allowed_origins(&self) -> Vec<String> {
        self.cors_origins.iter().map(|o| o.trim().trim_end_matches('/').to_string()).filter(|o| !o.is_empty()).collect()
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
    fn allowed_origins_are_trimmed() {
        let c: Config = serde_json::from_value(json!({"corsOrigins": ["http://a:1/", " http://b:2 ", "", "*"]})).unwrap();
        assert_eq!(c.allowed_origins(), vec!["http://a:1", "http://b:2", "*"]);
    }

    #[test]
    fn the_settings_form_hides_the_token() {
        let s = trunk_recorder_plugin::schema::schema_for::<Config>();
        assert_eq!(s["properties"]["token"]["x-secret"], true);
        assert_eq!(s["properties"]["port"]["default"], 8092);
    }

    #[test]
    fn the_transcribe_url_defaults_to_the_local_plugin() {
        assert_eq!(Config::default().transcribe_url, "http://127.0.0.1:8767");
        let s = trunk_recorder_plugin::schema::schema_for::<Config>();
        assert_eq!(s["properties"]["transcribeUrl"]["title"], "Transcribe plugin URL");
        assert_eq!(s["properties"]["transcribeUrl"]["default"], "http://127.0.0.1:8767");
    }
}
