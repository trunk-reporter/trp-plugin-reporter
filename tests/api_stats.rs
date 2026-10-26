mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::Value;
use trunk_reporter::api::rfc3339;
use trunk_reporter::store::recorder::insert_recorder_state;

#[tokio::test]
async fn stats_count_calls_in_windows_and_per_system() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/stats").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "StatsResponse");
    // Sites, not systems: two systems with a site each.
    assert_eq!((v["systems"].as_i64(), v["talkgroups"].as_i64(), v["units"].as_i64()), (Some(2), Some(4), Some(5)));
    assert_eq!((v["total_calls"].as_i64(), v["calls_24h"].as_i64(), v["calls_1h"].as_i64()), (Some(6), Some(4), Some(2)));
    // c1 12 s + c2 8 s + c3 20 s + c4 5 s + c5 15 s + c6 30 s.
    let hours = v["total_duration_hours"].as_f64().unwrap();
    assert!((hours - 90.0 / 3600.0).abs() < 1e-9, "{hours}");

    let activity = v["system_activity"].as_array().unwrap();
    assert_eq!(activity.len(), 2);
    for a in activity {
        assert_schema(a, "SystemActivity");
    }
    assert_eq!(activity[0]["system_id"], s.sys1.0);
    assert_eq!(activity[0]["sysid"], "3AB");
    assert_eq!(activity[0]["system_name"], "ohmarcs");
    // sys1: c1, c2 in the hour (tg 101); units 10, 20 (c1), 1234 (c2).
    let counts = |a: &Value| ["calls_1h", "calls_24h", "active_talkgroups", "active_units"].map(|k| a[k].as_i64().unwrap());
    assert_eq!(counts(&activity[0]), [2, 4, 1, 3]);
    assert_eq!(activity[1]["system_id"], s.sys2.0);
    assert_eq!(counts(&activity[1]), [0, 0, 0, 0]);
}

#[tokio::test]
async fn stats_of_an_empty_store_are_zero() {
    let (app, _, _, _) = app();
    let (st, v) = get(&app, "/api/v1/stats").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "StatsResponse");
    assert_eq!((v["systems"].as_i64(), v["total_calls"].as_i64(), v["total_duration_hours"].as_f64()), (Some(0), Some(0), Some(0.0)));
    assert_eq!(v["system_activity"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn rates_come_from_recorder_state() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/stats/rates").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "DecodeRatesResponse");
    assert_eq!(v["total"], 3);
    let rates = v["rates"].as_array().unwrap();
    assert_eq!(rates.len(), 3);
    for r in rates {
        assert_schema(r, "DecodeRate");
    }
    // Newest first.
    assert_eq!(rates[0]["time"], rfc3339(s.now));
    assert_eq!((rates[0]["decode_rate"].as_f64(), rates[0]["decode_rate_interval"].as_f64()), (Some(0.97), Some(0.97)));
    assert_eq!(rates[2]["decode_rate"], 0.98);
    assert_eq!(
        (rates[0]["system_id"].as_i64(), rates[0]["sysid"].as_str(), rates[0]["control_channel"].as_i64()),
        (Some(s.sys1.0), Some("3AB"), Some(851_000_000))
    );
    assert_eq!(rates[0]["system_name"], "ohmarcs");

    let (_, v) = get(&app, "/api/v1/stats/rates?limit=1").await;
    assert_eq!((v["rates"].as_array().unwrap().len(), v["total"].as_i64()), (1, Some(1)));

    // A time range: only the newest sample is at or after now-30s.
    let since = rfc3339(s.now - 30_000);
    let (_, v) = get(&app, &format!("/api/v1/stats/rates?start_time={since}")).await;
    assert_eq!(v["total"], 1);

    let (_, v) = get(&app, "/api/v1/stats/rates?sysid=3ab").await;
    assert_eq!(v["total"], 3);
    let (_, v) = get(&app, &format!("/api/v1/stats/rates?system_id={}", s.sys2.0)).await;
    assert_eq!(v["total"], 0);

    for bad in ["limit=0", "limit=10001", "limit=x", "start_time=nope"] {
        let (st, _) = get(&app, &format!("/api/v1/stats/rates?{bad}")).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}");
    }
    let (st, _) = get(&app, "/api/v1/stats/rates?limit=10000").await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn recorders_are_one_per_site() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    // The seed has status only for site 1; give site 2 a control channel and nothing recording.
    let site2 = s.site2;
    store.write_sync(move |c| insert_recorder_state(c, site2, s.now - 1000, Some(852_000_000), 0.5, 0, 0)).unwrap();

    let (st, v) = get(&app, "/api/v1/recorders").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "RecorderListResponse");
    assert_eq!(v["total"], 2);
    let recorders = v["recorders"].as_array().unwrap();
    for r in recorders {
        assert_schema(r, "Recorder");
    }
    let r1 = &recorders[0];
    assert_eq!(r1["id"], format!("{}_{}", s.sys1.0, s.site1.0));
    assert_eq!((r1["src_num"].as_i64(), r1["rec_num"].as_i64()), (Some(s.sys1.0), Some(s.site1.0)));
    assert_eq!((r1["type"].as_str(), r1["rec_state"].as_str()), (Some("P25"), Some("recording")));
    assert_eq!((r1["freq"].as_i64(), r1["count"].as_i64(), r1["squelched"].as_bool()), (Some(851_000_000), Some(1), Some(false)));
    // First sample was two minutes ago.
    let d = r1["duration"].as_f64().unwrap();
    assert!((119.0..125.0).contains(&d), "{d}");
    for k in ["tgid", "tg_alpha_tag", "unit_id", "unit_alpha_tag"] {
        assert!(r1[k].is_null(), "{k}");
    }
    let r2 = &recorders[1];
    assert_eq!(r2["id"], format!("{}_{}", s.sys2.0, s.site2.0));
    assert_eq!(
        (r2["type"].as_str(), r2["rec_state"].as_str(), r2["freq"].as_i64(), r2["count"].as_i64()),
        (Some("CONVENTIONAL"), Some("available"), Some(852_000_000), Some(0))
    );
}

#[tokio::test]
async fn a_site_without_a_control_channel_is_idle() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let site2 = s.site2;
    store.write_sync(move |c| insert_recorder_state(c, site2, s.now, None, 0.0, 0, 0)).unwrap();
    let (_, v) = get(&app, "/api/v1/recorders").await;
    assert_eq!((v["recorders"][1]["rec_state"].as_str(), v["recorders"][1]["freq"].as_i64()), (Some("idle"), Some(0)));
    let (_, v) = get(&app, "/api/v1/stats/rates").await;
    let none = v["rates"].as_array().unwrap().iter().find(|r| r["system_id"] == s.sys2.0).unwrap();
    assert_eq!(none["control_channel"], 0);
}

#[tokio::test]
async fn other_stats_are_501() {
    let (app, _, _, _) = app();
    for path in ["/api/v1/stats/call-heatmap", "/api/v1/analytics/decode-rates", "/api/v1/stats/call-volume", "/api/v1/analytics/recorder-utilization"] {
        let (st, _) = get(&app, path).await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED, "{path}");
    }
}
