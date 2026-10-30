//! Transcripts from the transcribe plugin: the shape it serves, the rule for which
//! ones count, the path that joins them to our calls, and a client for its API.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub mod client;
pub mod sse;

pub use sse::{Frame, SseParser};

/// A transcript as the transcribe plugin serves it. Fields this crate does not know are ignored.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    /// The call's recording path, without extension: the key that joins it to `calls.path`.
    pub path: String,
    pub system: String,
    pub talkgroup: u32,
    pub start_ms: i64,
    pub length_ms: i64,
    /// `ok`, `skipped` or `failed`.
    pub status: String,
    /// Why the call is not `ok`.
    pub reason: Option<String>,
    pub text: Option<String>,
    pub language: Option<String>,
    pub model: String,
    pub transcribed_ms: i64,
    #[serde(default)]
    pub segments: Vec<Utterance>,
}

/// One utterance of a transcript.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Utterance {
    /// The radio that spoke, when the engine gave word times.
    pub unit: Option<i64>,
    /// Seconds from the call's start.
    pub start: f64,
    pub end: f64,
    pub text: String,
}

/// What the plugin's health route says that we read; its engine block is ignored.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PluginHealth {
    pub ok: bool,
    pub queue: u64,
    pub in_flight: u64,
    pub transcribed24h: u64,
}

/// Whether a transcript counts as present: transcribed (`ok`) with some text.
pub fn is_ok(t: &Transcript) -> bool {
    t.status == "ok" && t.text.as_deref().is_some_and(|s| !s.trim().is_empty())
}

/// A call path in the form both plugins use: forward slashes (a backfilled call's path
/// may carry backslashes on Windows).
pub fn normalize_path(p: &str) -> String {
    p.replace('\\', "/")
}

/// Lets a log line through at most once per interval per key.
#[derive(Clone)]
pub struct Throttle {
    every: Duration,
    last: Arc<Mutex<HashMap<&'static str, Instant>>>,
}

impl Throttle {
    pub fn new(every: Duration) -> Throttle {
        Throttle { every, last: Arc::default() }
    }

    /// True at most once per interval for `key`.
    pub fn allow(&self, key: &'static str) -> bool {
        let now = Instant::now();
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        match last.get(key) {
            Some(t) if now.duration_since(*t) < self.every => false,
            _ => {
                last.insert(key, now);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> serde_json::Value {
        serde_json::json!({
            "path": "marcs/2026/10/30/48549-1793358967_851625000-call_2414",
            "system": "marcs",
            "talkgroup": 48549,
            "startMs": 1793358967667i64,
            "lengthMs": 1620,
            "status": "ok",
            "reason": null,
            "text": "234?",
            "language": "English",
            "model": "/home/x/Qwen3-ASR-1.7B-flat",
            "transcribedMs": 1793358971026i64,
            "segments": [ { "unit": 8342327, "start": 0.64, "end": 1.28, "text": "234" } ]
        })
    }

    #[test]
    fn the_plugins_transcript_json_round_trips() {
        let t: Transcript = serde_json::from_value(example()).unwrap();
        assert_eq!(t.segments[0], Utterance { unit: Some(8342327), start: 0.64, end: 1.28, text: "234".into() });
        assert_eq!(t.start_ms, 1793358967667);
        let back = serde_json::to_value(&t).unwrap();
        assert_eq!(back["startMs"], 1793358967667i64);
        assert_eq!(back["transcribedMs"], 1793358971026i64);
        assert_eq!(back["segments"][0]["unit"], 8342327);
        let mut extra = example();
        extra["shape"] = serde_json::json!(1);
        assert!(serde_json::from_value::<Transcript>(extra).is_ok());
    }

    #[test]
    fn only_ok_with_text_counts() {
        let ok: Transcript = serde_json::from_value(example()).unwrap();
        assert!(is_ok(&ok));
        let skipped = Transcript { status: "skipped".into(), ..ok.clone() };
        assert!(!is_ok(&skipped));
        assert!(!is_ok(&Transcript { text: None, ..ok.clone() }));
        assert!(!is_ok(&Transcript { text: Some("  ".into()), ..ok }));
    }

    #[test]
    fn backslashes_become_slashes() {
        assert_eq!(normalize_path("marcs\\2026\\10\\30\\x"), "marcs/2026/10/30/x");
    }

    #[test]
    fn a_throttle_allows_once_per_interval_per_key() {
        let t = Throttle::new(Duration::from_millis(50));
        assert!(t.allow("a"));
        assert!(!t.allow("a"));
        assert!(t.allow("b"));
        std::thread::sleep(Duration::from_millis(60));
        assert!(t.allow("a"));
    }
}
