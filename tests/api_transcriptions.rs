mod common;

use std::time::{Duration, Instant};

use common::fake_transcribe;
use serde_json::{Value, json};
use trunk_reporter::transcripts::client::TranscribeClient;

fn row(path: &str, text: &str) -> Value {
    json!({
        "path": path, "system": "marcs", "talkgroup": 48549, "startMs": 1, "lengthMs": 1620,
        "status": "ok", "reason": null, "text": text, "language": "English", "model": "m",
        "transcribedMs": 2, "segments": [ { "unit": 8342327, "start": 0.64, "end": 1.28, "text": text } ]
    })
}

#[tokio::test]
async fn one_reads_a_transcript_and_a_missing_path_is_none() {
    let fake = fake_transcribe::start(vec![row("marcs/2026/10/30/x", "234?")]).await;
    let client = TranscribeClient::new(&format!("  {}/ ", fake.url)).unwrap();
    assert_eq!(client.base(), fake.url);
    // A backslash path joins to the same transcript.
    let t = client.one("marcs\\2026\\10\\30\\x").await.unwrap().expect("a transcript");
    assert_eq!(t.text.as_deref(), Some("234?"));
    assert_eq!(t.segments[0].unit, Some(8342327));
    assert!(client.one("marcs/2026/10/30/nope").await.unwrap().is_none());
    let h = client.health().await.unwrap();
    assert!(h.ok && h.transcribed24h == 1);
}

#[tokio::test]
async fn batch_sends_at_most_200_paths_per_request_and_keeps_order() {
    let paths: Vec<String> = (0..450).map(|i| format!("marcs/2026/10/30/c{i}")).collect();
    let fake = fake_transcribe::start(paths.iter().map(|p| row(p, "hi")).collect()).await;
    let client = TranscribeClient::new(&fake.url).unwrap();
    // Asked in reverse, the last one a backslash path with no row.
    let mut asked: Vec<String> = paths.iter().rev().cloned().collect();
    asked.push("marcs\\2026\\10\\30\\absent".into());
    let got = client.batch(&asked).await.unwrap();
    let sizes: Vec<usize> = fake.batch_requests.lock().unwrap().iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![200, 200, 51]);
    let got_paths: Vec<&str> = got.iter().map(|t| t.path.as_str()).collect();
    let want: Vec<&str> = asked.iter().take(450).map(String::as_str).collect();
    assert_eq!(got_paths, want);
    assert!(client.batch(&[]).await.unwrap().is_empty());
    assert_eq!(fake.batch_requests.lock().unwrap().len(), 3, "an empty input makes no request");
}

#[tokio::test]
async fn a_down_plugin_is_an_error_not_a_panic() {
    let client = TranscribeClient::new("http://127.0.0.1:1").unwrap();
    let started = Instant::now();
    assert!(client.one("a/b").await.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn a_hanging_plugin_is_given_up_within_the_timeout() {
    let fake = fake_transcribe::start(vec![]).await;
    fake.hang();
    let (state, store, ..) = common::state_with(None);
    let app = trunk_reporter::api::router(state.with_transcribe(&fake.url).unwrap());
    let c1 = common::seed(&store).calls[0].0;
    let started = Instant::now();
    let (status, v) = common::get(&app, &format!("/api/v1/calls/{c1}")).await;
    assert_eq!(status, 200);
    assert!(started.elapsed() < Duration::from_secs(12), "took {:?}", started.elapsed());
    assert!(v.get("transcription_text").is_none(), "{v}");
}
