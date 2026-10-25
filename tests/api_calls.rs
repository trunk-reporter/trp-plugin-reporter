mod common;

use axum::http::StatusCode;
use common::*;
use reporter::api::{rfc3339, router};
use reporter::store::calls::{NewCall, call_started};
use reporter::store::now_ms;
use serde_json::Value;
use trunk_recorder_plugin::testing;

fn call_ids(v: &Value) -> Vec<i64> {
    v["calls"].as_array().unwrap().iter().map(|c| c["call_id"].as_i64().unwrap()).collect()
}

#[tokio::test]
async fn calls_list_filters_sorts_and_pages() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let id = |i: usize| s.calls[i].0;

    let (st, v) = get(&app, "/api/v1/calls").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "CallListResponse");
    assert_schema(&v["calls"][0], "Call");
    assert_eq!((v["total"].as_i64(), v["limit"].as_i64(), v["offset"].as_i64()), (Some(6), Some(50), Some(0)));
    assert_eq!(call_ids(&v), vec![id(0), id(1), id(2), id(3), id(4), id(5)]); // -start_time

    // The D6 fields are there, on a recorded call and on the unrecorded c4.
    let c1 = &v["calls"][0];
    assert_eq!((c1["recorded"].as_bool(), c1["decrypted"].as_bool(), c1["priority"].as_i64()), (Some(true), Some(false), Some(0)));
    assert!(c1.get("duplex").is_some() && c1.get("packet").is_some() && c1.get("algid").is_some() && c1.get("not_recorded_reason").is_some());
    let c4 = &v["calls"][3];
    assert_eq!(
        (c4["recorded"].as_bool(), c4["not_recorded_reason"].as_str(), c4["algid"].as_i64(), c4["key_id"].as_i64(), c4["priority"].as_i64()),
        (Some(false), Some("encrypted"), Some(0x84), Some(1), Some(4))
    );
    assert_eq!((c4["call_state"].as_str(), c4["encrypted"].as_bool(), c4["audio_url"].clone()), (Some("stopped"), Some(true), Value::Null));

    let (_, v) = get(&app, "/api/v1/calls?tgid=202&recorded=false").await;
    assert_eq!((v["total"].as_i64(), call_ids(&v)), (Some(1), vec![id(3)]));
    let (_, v) = get(&app, "/api/v1/calls?tgid=101&recorded=false").await;
    assert_eq!(v["total"], 0);
    let (_, v) = get(&app, "/api/v1/calls?unit_id=1234").await;
    assert_eq!(call_ids(&v), vec![id(1), id(2)]);
    let (_, v) = get(&app, "/api/v1/calls?emergency=true").await;
    assert_eq!(call_ids(&v), vec![id(1)]);
    let (_, v) = get(&app, "/api/v1/calls?encrypted=true&deduplicate=true").await;
    assert_eq!(call_ids(&v), vec![id(3)]);
    let (_, v) = get(&app, "/api/v1/calls?sysid=3ab&site_id=1").await;
    assert_eq!(v["total"], 6);
    let (_, v) = get(&app, "/api/v1/calls?sysid=ffff").await;
    assert_eq!(v["total"], 0);
    let (_, v) = get(&app, "/api/v1/calls?system_id=2").await;
    assert_eq!(v["total"], 0);

    // Durations: c4 5, c2 8, c1 12, c5 15, c3 20, c6 30 s.
    let (_, v) = get(&app, "/api/v1/calls?sort=duration&limit=2&offset=1").await;
    assert_eq!((call_ids(&v), v["total"].as_i64(), v["limit"].as_i64(), v["offset"].as_i64()), (vec![id(1), id(0)], Some(6), Some(2), Some(1)));
    let (_, v) = get(&app, "/api/v1/calls?sort=-tgid&limit=1").await;
    assert_eq!(v["calls"][0]["tgid"], 202);
    for sort in ["stop_time", "freq", "-start_time"] {
        let (st, v) = get(&app, &format!("/api/v1/calls?sort={sort}")).await;
        assert_eq!(st, StatusCode::OK);
        assert_schema(&v, "CallListResponse");
    }

    let from = rfc3339(s.now - 4 * HOUR);
    let (_, v) = get(&app, &format!("/api/v1/calls?start_time={from}")).await;
    assert_eq!(v["total"], 3);

    for bad in ["/api/v1/calls?sort=bogus", "/api/v1/calls?recorded=maybe", "/api/v1/calls?limit=0", "/api/v1/calls?start_time=yesterday"] {
        let (st, v) = get(&app, bad).await;
        assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    }
}

#[tokio::test]
async fn a_call_detail_has_units_transmissions_and_audio_url() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let c1 = s.calls[0].0;

    let (st, v) = get(&app, &format!("/api/v1/calls/{c1}")).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "Call");
    assert_eq!(v["audio_url"].as_str(), Some(format!("/api/v1/calls/{c1}/audio").as_str()));
    assert_eq!((v["call_state"].as_str(), v["recorded"].as_bool(), v["duration"].as_f64()), (Some("completed"), Some(true), Some(12.0)));
    assert_eq!((v["system_name"].as_str(), v["sysid"].as_str(), v["site_short_name"].as_str()), (Some("ohmarcs"), Some("3AB"), Some("ohmarcs-dayton")));
    assert_eq!(
        (v["tg_alpha_tag"].as_str(), v["tg_group"].as_str(), v["freq"].as_i64(), v["conventional"].as_bool()),
        (Some("Fire Disp"), Some("Fire"), Some(851_012_500), Some(false))
    );
    assert_eq!(v["has_transcription"], false);
    assert_eq!(
        (v["unit_ids"].clone(), v["units"][0]["alpha_tag"].as_str(), v["units"][1]["unit_id"].as_i64()),
        (serde_json::json!([10, 20]), Some("Medic 10"), Some(20))
    );
    assert_schema(&v["units"][0], "CallUnit");
    assert_eq!((v["src_list"][0]["src"].as_i64(), v["src_list"][0]["duration"].as_f64(), v["src_list"][1]["pos"].as_f64()), (Some(10), Some(6.0), Some(6.0)));
    assert_schema(&v["src_list"][0], "CallTransmission");
    assert_schema(&v["freq_list"][0], "CallFrequency");
    assert_eq!(v["freq_list"][0]["len"].as_f64(), Some(12.0));

    let (st, v) = get(&app, &format!("/api/v1/calls/{c1}/transmissions")).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "CallTransmissionListResponse");
    assert_eq!((v["total"].as_i64(), v["transmissions"][1]["src"].as_i64(), v["transmissions"][1]["emergency"].as_i64()), (Some(2), Some(20), Some(0)));
    let c2 = s.calls[1].0;
    let (_, v) = get(&app, &format!("/api/v1/calls/{c2}/transmissions")).await;
    assert_eq!(v["transmissions"][0]["emergency"], 1);

    let (st, v) = get(&app, &format!("/api/v1/calls/{c1}/frequencies")).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "CallFrequencyListResponse");
    assert_eq!((v["total"].as_i64(), v["frequencies"][0]["freq"].as_i64()), (Some(1), Some(851_012_500)));

    // The unrecorded call has no audio and no lists.
    let c4 = s.calls[3].0;
    let (_, v) = get(&app, &format!("/api/v1/calls/{c4}")).await;
    assert_schema(&v, "Call");
    assert_eq!((v["audio_url"].clone(), v["call_state"].as_str()), (Value::Null, Some("stopped")));
    let (_, v) = get(&app, &format!("/api/v1/calls/{c4}/transmissions")).await;
    assert_eq!(v["total"], 0);

    for path in ["/api/v1/calls/9999", "/api/v1/calls/9999/transmissions", "/api/v1/calls/9999/frequencies"] {
        let (st, v) = get(&app, path).await;
        assert_eq!((st, v["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
    }
    let (st, v) = get(&app, "/api/v1/calls/abc").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
}

#[tokio::test]
async fn active_calls_are_the_ones_still_recording() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let (sys, site) = (s.sys1, s.site1);
    let id = store
        .write_sync(move |c| {
            call_started(
                c,
                &NewCall {
                    system_id: sys,
                    site_id: Some(site),
                    call_num: Some(99),
                    tgid: 101,
                    start_ms: now_ms() - 4_000,
                    freq_hz: 851_012_500,
                    tdma_slot: None,
                    analog: false,
                    encrypted: false,
                    algid: None,
                    key_id: None,
                    decrypted: false,
                    emergency: false,
                    duplex: false,
                    packet: false,
                    priority: 4,
                    recorded: true,
                    not_recorded_reason: None,
                    tg_alpha_tag: "Fire Disp".into(),
                    patched_tgids: vec![101, 202],
                },
            )
        })
        .unwrap();

    // A call that started over an hour ago and never ended is not live.
    store
        .write_sync(move |c| {
            c.execute(
                "INSERT INTO calls (system_id, site_id, call_num, tgid, start_ms, state) VALUES (?1, ?2, 98, 101, ?3, 'recording')",
                (sys, site, now_ms() - 2 * 3_600_000),
            )
        })
        .unwrap();

    let (st, v) = get(&app, "/api/v1/calls/active").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "ActiveCallListResponse");
    assert_eq!((v["total"].as_i64(), call_ids(&v)), (Some(1), vec![id.0]));
    let call = &v["calls"][0];
    assert_schema(call, "Call");
    assert_eq!((call["audio_url"].clone(), call["stop_time"].clone(), call["call_state"].as_str()), (Value::Null, Value::Null, Some("recording")));
    assert!(call["duration"].as_f64().unwrap() > 0.0);
    assert_eq!(call["patched_tgids"], serde_json::json!([202]));

    for (query, total) in
        [("tgid=101", 1), ("tgid=202", 0), ("sysid=3ab", 1), ("sysid=ffff", 0), ("emergency=false", 1), ("emergency=true", 0), ("encrypted=true", 0)]
    {
        let (st, v) = get(&app, &format!("/api/v1/calls/active?{query}")).await;
        assert_eq!((st, v["total"].as_i64()), (StatusCode::OK, Some(total)), "{query}");
        assert_eq!(v["calls"].as_array().unwrap().len() as i64, total);
    }

    // It is also in the list and by id, and /calls/active is not a call id.
    let (_, v) = get(&app, &format!("/api/v1/calls/{}", id.0)).await;
    assert_eq!(v["call_state"], "recording");
    let (_, v) = get(&app, "/api/v1/calls").await;
    assert_eq!(v["total"], 8);
}

#[tokio::test]
async fn audio_streams_with_ranges_and_stays_inside_the_capture_folder() {
    let (state, store, _, _) = state_with(None);
    let capture = state.capture_dir.clone();
    let app = router(state);
    let s = seed(&store);
    let (c1, c2, c3, c4) = (s.calls[0].0, s.calls[1].0, s.calls[2].0, s.calls[3].0);

    // A real 3 s WAV and its M4A, under the capture folder.
    let cc = testing::call(&capture, "ohmarcs", 101);
    let (wav, m4a) = (format!("{}.wav", cc.path), format!("{}.m4a", cc.path));
    // A file outside the folder, named by a row inside it.
    let outside = capture.parent().unwrap().join(format!("outside-{}.wav", std::process::id()));
    std::fs::write(&outside, b"RIFF not yours").unwrap();
    let escape = format!("../{}", outside.file_name().unwrap().to_str().unwrap());
    // The same file by its absolute path.
    let c6 = s.calls[5].0;
    let absolute = outside.to_str().unwrap().to_string();
    assert!(std::path::Path::new(&absolute).is_absolute());
    store
        .write_sync(move |c| {
            c.execute("UPDATE calls SET audio_path = ?1 WHERE id = ?2", (wav, c1))?;
            c.execute("UPDATE calls SET audio_path = ?1 WHERE id = ?2", (m4a, c3))?;
            c.execute("UPDATE calls SET audio_path = ?1 WHERE id = ?2", (escape, c2))?;
            c.execute("UPDATE calls SET audio_path = ?1 WHERE id = ?2", (absolute, c6))?;
            Ok(())
        })
        .unwrap();

    let (st, h, body) = get_with(&app, &format!("/api/v1/calls/{c1}/audio"), &[]).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(h["content-type"], "audio/wav");
    assert_eq!(body.len(), 48_044);
    assert_eq!(h["accept-ranges"], "bytes");

    let (st, h, body) = get_with(&app, &format!("/api/v1/calls/{c1}/audio"), &[("range", "bytes=0-99")]).await;
    assert_eq!(st, StatusCode::PARTIAL_CONTENT);
    assert_eq!((body.len(), h["content-range"].to_str().unwrap()), (100, "bytes 0-99/48044"));
    assert_eq!(&body[..4], b"RIFF");

    let (st, h, _) = get_with(&app, &format!("/api/v1/calls/{c3}/audio"), &[]).await;
    assert_eq!((st, h["content-type"].to_str().unwrap()), (StatusCode::OK, "audio/mp4"));

    // Out of the folder, no audio path, a missing file, an unknown call.
    let (st, _, body) = get_with(&app, &format!("/api/v1/calls/{c2}/audio"), &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["code"], "not_found");
    assert!(!String::from_utf8_lossy(&body).contains("not yours"));
    let (st, _, body) = get_with(&app, &format!("/api/v1/calls/{c6}/audio"), &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "an absolute path outside the folder");
    assert!(!String::from_utf8_lossy(&body).contains("not yours"));
    let (st, _, _) = get_with(&app, &format!("/api/v1/calls/{c4}/audio"), &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let c5 = s.calls[4].0; // audio_path set, file never created
    let (st, _, _) = get_with(&app, &format!("/api/v1/calls/{c5}/audio"), &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _, _) = get_with(&app, "/api/v1/calls/9999/audio", &[]).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let _ = std::fs::remove_file(outside);
}

#[tokio::test]
async fn talkgroup_and_unit_call_lists_work() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let id = |i: usize| s.calls[i].0;
    let sys = s.sys1.0;

    let (st, v) = get(&app, &format!("/api/v1/talkgroups/{sys}:101/calls")).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "CallListResponse");
    assert_eq!((v["total"].as_i64(), call_ids(&v)), (Some(3), vec![id(0), id(1), id(4)]));
    let (_, v) = get(&app, "/api/v1/talkgroups/202/calls?recorded=true").await;
    assert_eq!(call_ids(&v), vec![id(2), id(5)]);
    let (_, v) = get(&app, "/api/v1/talkgroups/202/calls?recorded=false").await;
    assert_eq!(call_ids(&v), vec![id(3)]);
    let end = rfc3339(s.now - DAY);
    let (_, v) = get(&app, &format!("/api/v1/talkgroups/101/calls?end_time={end}&limit=1")).await;
    assert_eq!((v["total"].as_i64(), call_ids(&v)), (Some(1), vec![id(4)]));

    let (_, v) = get(&app, &format!("/api/v1/units/{sys}:1234/calls")).await;
    assert_eq!((v["total"].as_i64(), call_ids(&v)), (Some(2), vec![id(1), id(2)]));
    let (_, v) = get(&app, &format!("/api/v1/units/{sys}-10/calls?recorded=true&sort=start_time")).await;
    assert_eq!(call_ids(&v), vec![id(5), id(2), id(0)]);

    // Unit 1234 is on both systems; talkgroup 9178 too.
    let (st, v) = get(&app, "/api/v1/units/1234/calls").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::CONFLICT, Some("ambiguous_id")));
    let (st, v) = get(&app, "/api/v1/talkgroups/9178/calls").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::CONFLICT, Some("ambiguous_id")));
    let (st, _) = get(&app, "/api/v1/units/77777/calls").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get(&app, "/api/v1/talkgroups/1:5555/calls").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

/// The calendar heatmap and daily overview ask for `limit=50000`; tr-engine
/// answers with its maximum page rather than a 400, and so do we.
#[tokio::test]
async fn an_oversized_limit_is_the_maximum_page() {
    let (app, ..) = app();
    let (st, v) = get(&app, "/api/v1/calls?limit=50000").await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["limit"], 1000, "{v}");
}
