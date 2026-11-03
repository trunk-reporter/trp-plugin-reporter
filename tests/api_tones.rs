mod common;

use axum::http::StatusCode;
use common::*;
use trunk_recorder_plugin::{CallRecord, TonePattern};
use trunk_reporter::store::calls::call_concluded;
use trunk_reporter::store::systems::ensure_system;
use trunk_reporter::store::talkgroups::{TgTags, touch_talkgroup};

fn pattern(kind: &str, pos: f64, src: i64) -> TonePattern {
    TonePattern {
        pattern: kind.into(),
        tones: vec![849.63, 1292.85],
        lengths: vec![1.0, 3.04],
        format: "Quick Call II".into(),
        pos,
        len: 4.04,
        src,
        origin: "audio".into(),
        text: "two-tone 849.63 → 1292.85 Hz, 1 + 3.04 s (Quick Call II)".into(),
        signals: vec!["station-5".into()],
        ..Default::default()
    }
}

#[tokio::test]
async fn tone_patterns_are_listed_with_their_call_and_names() {
    let (app, store, _, _) = app();
    store
        .write_sync(|c| {
            let sys = ensure_system(c, "county", "p25", "County", 0)?;
            touch_talkgroup(c, sys, 101, &TgTags { alpha_tag: "Fire Dispatch".into(), ..Default::default() }, 1_700_000_000_000, false, None)?;
            let r = CallRecord {
                talkgroup: 101,
                start_time_ms: 1_700_000_000_000,
                tone_patterns: vec![pattern("two_tone", 0.5, 0), pattern("digits", 9.0, 1001)],
                ..Default::default()
            };
            call_concluded(c, sys, None, "county/a", &r, None, "", None)?;
            let later = CallRecord { talkgroup: 202, start_time_ms: 1_700_000_100_000, tone_patterns: vec![pattern("long", 1.0, 0)], ..Default::default() };
            call_concluded(c, sys, None, "county/b", &later, None, "", None).map(|_| ())
        })
        .unwrap();

    let (st, v) = get(&app, "/api/v1/tone-patterns").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "TonePatternListResponse");
    assert_eq!(v["total"], 3);
    let first = &v["tone_patterns"][0];
    assert_schema(first, "TonePattern");
    // Newest first.
    assert_eq!((first["pattern"].as_str(), first["tgid"].as_u64()), (Some("long"), Some(202)));

    let (_, v) = get(&app, "/api/v1/tone-patterns?pattern=two_tone&sort=time").await;
    let page = &v["tone_patterns"][0];
    assert_eq!((v["total"].as_i64(), page["system_name"].as_str(), page["tg_alpha_tag"].as_str()), (Some(1), Some("County"), Some("Fire Dispatch")));
    assert_eq!(
        (page["time"].as_str(), page["format"].as_str(), page["signals"][0].as_str()),
        (Some("2023-11-14T22:13:20.500Z"), Some("Quick Call II"), Some("station-5"))
    );
    assert!(page["call_id"].as_i64().is_some() && page.get("unit_id").is_none());

    let (_, v) = get(&app, "/api/v1/tone-patterns?unit_id=1001").await;
    assert_eq!((v["total"].as_i64(), v["tone_patterns"][0]["pattern"].as_str()), (Some(1), Some("digits")));
    let (_, v) = get(&app, "/api/v1/tone-patterns?tgid=101&end_time=2023-11-14T22:13:21Z").await;
    assert_eq!(v["total"], 1);

    let (st, v) = get(&app, "/api/v1/tone-patterns?sort=pattern").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
}
