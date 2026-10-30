//! The client for the transcribe plugin's local HTTP API. Every request is bounded
//! (3 s to connect, 10 s in all) so a plugin that is down or stuck cannot slow a page;
//! the event stream has a connect timeout only, since it is meant to stay open (the
//! relay bounds the wait for its answer).

use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;

use super::{PluginHealth, Transcript, normalize_path};

/// The most paths one batch request carries (the plugin's limit).
const BATCH_MAX: usize = 200;

/// Talks to one transcribe plugin. Cheap to clone.
#[derive(Clone)]
pub struct TranscribeClient {
    base: String,
    http: reqwest::Client,
    stream: reqwest::Client,
}

#[derive(Deserialize)]
struct Batch {
    transcripts: Vec<Transcript>,
}

impl TranscribeClient {
    /// A client for the plugin at `base_url` (surrounding spaces and trailing `/` dropped).
    pub fn new(base_url: &str) -> Result<TranscribeClient, String> {
        let base = base_url.trim().trim_end_matches('/').to_string();
        if base.is_empty() {
            return Err("the transcribe plugin's URL is empty".into());
        }
        let connect = Duration::from_secs(3);
        let http = reqwest::Client::builder().connect_timeout(connect).timeout(Duration::from_secs(10)).build().map_err(|e| e.to_string())?;
        let stream = reqwest::Client::builder().connect_timeout(connect).build().map_err(|e| e.to_string())?;
        Ok(TranscribeClient { base, http, stream })
    }

    /// The plugin's base URL, as cleaned.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The transcript of the call at `path`; `None` when the plugin has none.
    pub async fn one(&self, path: &str) -> Result<Option<Transcript>, String> {
        let mut url = reqwest::Url::parse(&format!("{}/api/v1/transcripts/", self.base)).map_err(|e| e.to_string())?;
        // Each segment is percent-encoded as a URL path segment.
        url.path_segments_mut()
            .map_err(|_| "the transcribe plugin's URL cannot take a path".to_string())?
            .pop_if_empty()
            .extend(normalize_path(path).split('/'));
        let resp = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status().map_err(|e| e.to_string())?;
        resp.json::<Transcript>().await.map(Some).map_err(|e| e.to_string())
    }

    /// The transcripts of the calls at `paths`, in the order asked; a path with none is
    /// absent. Asks in chunks of at most 200; no paths, no request.
    pub async fn batch(&self, paths: &[String]) -> Result<Vec<Transcript>, String> {
        let mut found = Vec::new();
        for chunk in paths.chunks(BATCH_MAX) {
            let joined = chunk.iter().map(|p| normalize_path(p)).collect::<Vec<_>>().join(",");
            let resp = self
                .http
                .get(format!("{}/api/v1/transcripts", self.base))
                .query(&[("paths", joined)])
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .map_err(|e| e.to_string())?;
            found.extend(resp.json::<Batch>().await.map_err(|e| e.to_string())?.transcripts);
        }
        Ok(found)
    }

    /// The plugin's health.
    pub async fn health(&self) -> Result<PluginHealth, String> {
        let resp = self.http.get(format!("{}/api/v1/health", self.base)).send().await.and_then(|r| r.error_for_status()).map_err(|e| e.to_string())?;
        resp.json::<PluginHealth>().await.map_err(|e| e.to_string())
    }

    /// Opens the plugin's event stream, asking for what was written since
    /// `last_event_id` when given. The caller reads the body, and bounds the wait for the
    /// answer: only connecting is timed out here.
    pub async fn events(&self, last_event_id: Option<i64>) -> Result<reqwest::Response, String> {
        let mut req = self.stream.get(format!("{}/api/v1/events", self.base)).header("Accept", "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("Last-Event-ID", id.to_string());
        }
        req.send().await.and_then(|r| r.error_for_status()).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_base_is_trimmed_and_an_empty_one_refused() {
        assert_eq!(TranscribeClient::new("  http://h:1/// ").unwrap().base(), "http://h:1");
        assert!(TranscribeClient::new("   ").is_err());
    }
}
