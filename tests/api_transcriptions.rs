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

// ---------------------------------------------------------------------------
// The routes
// ---------------------------------------------------------------------------

fn path_of(store: &trunk_reporter::store::Store, id: trunk_reporter::store::CallId) -> Option<String> {
    store.write_sync(move |c| c.query_row("SELECT path FROM calls WHERE id = ?1", [id], |r| r.get(0))).unwrap()
}

/// A seeded store, a fake plugin holding the ok transcript of call 0, a skipped one of call 1,
/// and an ok-but-empty one of call 2; the router reading from it. Call 3 has no path.
async fn bridged() -> (axum::Router, common::Seed, common::fake_transcribe::FakeTranscribe) {
    let (state, store, ..) = common::state_with(None);
    let seed = common::seed(&store);
    let p = |i: usize| path_of(&store, seed.calls[i]);
    let mut skipped = row(&p(1).unwrap(), "");
    skipped["status"] = json!("skipped");
    skipped["reason"] = json!("too short");
    skipped["text"] = Value::Null;
    skipped["segments"] = json!([]);
    let fake = fake_transcribe::start(vec![row(&p(0).unwrap(), "234? Medic 10 responding"), skipped, row(&p(2).unwrap(), "")]).await;
    let app = trunk_reporter::api::router(state.with_transcribe(&fake.url).unwrap());
    (app, seed, fake)
}

#[tokio::test]
async fn a_transcription_is_served_in_the_engines_shape() {
    let (app, seed, _fake) = bridged().await;
    let id = seed.calls[0].0;
    let (status, v) = common::get(&app, &format!("/api/v1/calls/{id}/transcription")).await;
    assert_eq!(status, 200, "{v}");
    common::assert_schema(&v, "Transcription");
    assert_eq!(v["call_id"], id);
    assert_eq!(v["id"], id);
    assert_eq!(v["source"], "auto");
    assert_eq!(v["text"], "234? Medic 10 responding");
    assert_eq!(v["word_count"], 4);
    assert_eq!(v["words"]["segments"][0]["src"], 8342327);
    assert_eq!(v["words"]["words"], json!([]));
    assert!(time::OffsetDateTime::parse(v["created_at"].as_str().unwrap(), &time::format_description::well_known::Rfc3339).is_ok(), "{v}");
}

#[tokio::test]
async fn skipped_empty_and_unknown_read_as_no_transcription() {
    let (app, seed, _fake) = bridged().await;
    for i in [1, 2, 3, 4] {
        let (status, v) = common::get(&app, &format!("/api/v1/calls/{}/transcription", seed.calls[i].0)).await;
        assert_eq!(status, 404, "call {i}: {v}");
    }
    // The bridge off.
    let (state, store, ..) = common::state_with(None);
    let s = common::seed(&store);
    let off = trunk_reporter::api::router(state);
    assert_eq!(common::get(&off, &format!("/api/v1/calls/{}/transcription", s.calls[0].0)).await.0, 404);
    // The plugin down.
    let (state, store, ..) = common::state_with(None);
    let s = common::seed(&store);
    let down = trunk_reporter::api::router(state.with_transcribe("http://127.0.0.1:1").unwrap());
    assert_eq!(common::get(&down, &format!("/api/v1/calls/{}/transcription", s.calls[0].0)).await.0, 404);
}

#[tokio::test]
async fn the_batch_keeps_the_order_asked_and_drops_the_rest() {
    let (app, seed, _fake) = bridged().await;
    let ids: Vec<String> = [2, 0, 4, 1].iter().map(|&i| seed.calls[i].0.to_string()).collect();
    let (status, v) = common::get(&app, &format!("/api/v1/transcriptions/batch?call_ids={}", ids.join(","))).await;
    assert_eq!(status, 200, "{v}");
    common::assert_schema(&v, "BatchTranscriptionResponse");
    let list = v["transcriptions"].as_array().unwrap();
    assert_eq!(list.len(), 1, "{v}");
    assert_eq!(list[0]["call_id"], seed.calls[0].0);
    assert_eq!(list[0]["text"], "234? Medic 10 responding");
    assert_eq!(list[0]["segments"][0]["src"], 8342327);
    assert_eq!(common::get(&app, "/api/v1/transcriptions/batch?call_ids=abc").await.0, 400);
    let many: Vec<String> = (1..=501).map(|i| i.to_string()).collect();
    assert_eq!(common::get(&app, &format!("/api/v1/transcriptions/batch?call_ids={}", many.join(","))).await.0, 400);
}

#[tokio::test]
async fn a_backfilled_path_with_backslashes_still_joins() {
    let (state, store, ..) = common::state_with(None);
    let seed = common::seed(&store);
    let id = seed.calls[0];
    store.write_sync(move |c| c.execute("UPDATE calls SET path = ?1 WHERE id = ?2", (r"marcs\2026\10\30\x-1_1-call_1", id))).unwrap();
    let fake = fake_transcribe::start(vec![row("marcs/2026/10/30/x-1_1-call_1", "hello there")]).await;
    let app = trunk_reporter::api::router(state.with_transcribe(&fake.url).unwrap());
    let (status, v) = common::get(&app, &format!("/api/v1/calls/{}/transcription", id.0)).await;
    assert_eq!(status, 200, "{v}");
    assert_eq!(v["text"], "hello there");
}

#[test]
fn call_by_path_finds_a_forward_slash_row_and_a_backslash_one() {
    use trunk_reporter::store::calls::call_by_path;
    let (_, store, ..) = common::state_with(None);
    let seed = common::seed(&store);
    let (a, b) = (seed.calls[0], seed.calls[1]);
    store.write_sync(move |c| c.execute("UPDATE calls SET path = ?1 WHERE id = ?2", (r"marcs\2026\x", b))).unwrap();
    let pa = path_of(&store, a).unwrap();
    store
        .write_sync(move |c| {
            assert_eq!(call_by_path(c, &pa.replace('/', "\\"))?.map(|r| r.id), Some(a));
            assert_eq!(call_by_path(c, "marcs/2026/x")?.map(|r| r.id), Some(b));
            assert!(call_by_path(c, "marcs/2026/none")?.is_none());
            Ok(())
        })
        .unwrap();
}
