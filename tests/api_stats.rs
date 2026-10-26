mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::Value;
use trunk_recorder_plugin::CallRecord;
use trunk_reporter::api::rfc3339;
use trunk_reporter::store::calls::call_concluded;
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

/// Two calls on tg 555 of sys1, which has no `talkgroups` row (20 and 21 minutes ago, 4 s each).
fn unnamed_talkgroup_calls(store: &trunk_reporter::store::Store, s: &Seed) {
    let (sys1, site1, now) = (s.sys1, s.site1, s.now);
    store
        .write_sync(move |c| {
            for (n, ago) in [20 * MIN, 21 * MIN].into_iter().enumerate() {
                let start = now - ago;
                let r = CallRecord {
                    call_num: 900 + n as u64,
                    freq: 851_012_500,
                    start_time: start / 1000,
                    stop_time: start / 1000 + 4,
                    start_time_ms: start,
                    stop_time_ms: start + 4_000,
                    call_length: 4.0,
                    call_length_ms: 4_000,
                    talkgroup: 555,
                    short_name: "ohmarcs".into(),
                    ..Default::default()
                };
                call_concluded(c, sys1, Some(site1), &format!("ohmarcs/555-{start}"), &r, None, "x.json", None)?;
            }
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn talkgroup_activity_summarises_the_window() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    unnamed_talkgroup_calls(&store, &s);

    let (st, v) = get(&app, "/api/v1/stats/talkgroup-activity").await;
    assert_eq!(st, StatusCode::OK);
    // The last 24 h, completed calls: c1, c2 (tg 101), c3 (tg 202; c4 is stopped, not completed) and two on 555.
    let items = v["activity"].as_array().unwrap();
    assert_eq!(v["total"], 3);
    for i in items {
        assert_schema(i, "TalkgroupActivity");
        assert!(i.get("buckets").is_none(), "no interval, no buckets");
    }
    let row = |i: &Value| {
        (
            i["tgid"].as_i64().unwrap(),
            i["tg_alpha_tag"].as_str().unwrap().to_string(),
            i["call_count"].as_i64().unwrap(),
            i["total_duration"].as_f64().unwrap(),
            i["emergency_count"].as_i64().unwrap(),
        )
    };
    // By call count, then tgid: 101 and 555 tie on 2.
    assert_eq!(row(&items[0]), (101, "Fire Disp".into(), 2, 20.0, 1));
    assert_eq!(row(&items[1]), (555, "".into(), 2, 8.0, 0));
    assert_eq!(row(&items[2]), (202, "PD Tac".into(), 1, 20.0, 0));
    assert_eq!((items[0]["system_id"].as_i64(), items[0]["system_name"].as_str()), (Some(s.sys1.0), Some("ohmarcs")));
    assert_eq!(
        (items[0]["tg_group"].as_str(), items[0]["tg_tag"].as_str(), items[0]["tg_description"].as_str()),
        (Some("Fire"), Some("Dispatch"), Some("Fire Dispatch"))
    );
    assert_eq!(items[0]["first_call"], rfc3339(s.now - 30 * MIN));
    assert_eq!(items[0]["last_call"], rfc3339(s.now - 10 * MIN));

    // Every state, and a wider window: c4 and c5 join.
    let (_, v) = get(&app, "/api/v1/stats/talkgroup-activity?call_state=&sort=tgid").await;
    let tgs: Vec<(i64, i64)> = v["activity"].as_array().unwrap().iter().map(|i| (i["tgid"].as_i64().unwrap(), i["call_count"].as_i64().unwrap())).collect();
    assert_eq!(tgs, vec![(101, 2), (202, 2), (555, 2)]);
    let after = rfc3339(s.now - 4 * DAY);
    let (_, v) = get(&app, &format!("/api/v1/stats/talkgroup-activity?after={after}&tgids=101&limit=1")).await;
    assert_eq!((v["total"].as_i64(), v["activity"][0]["call_count"].as_i64()), (Some(1), Some(3)));
    let (_, v) = get(&app, &format!("/api/v1/stats/talkgroup-activity?start_time={after}&system_id={}&sort=duration", s.sys2.0)).await;
    assert_eq!((v["total"].as_i64(), v["activity"].as_array().unwrap().len()), (Some(0), 0));
    let (st, v) = get(&app, "/api/v1/stats/talkgroup-activity?after=2024-01-02T00:00:00Z&before=2024-01-01T00:00:00Z").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_time_range")));
}

#[tokio::test]
async fn talkgroup_activity_buckets_by_interval() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    unnamed_talkgroup_calls(&store, &s);

    let (st, v) = get(&app, "/api/v1/stats/talkgroup-activity?interval=60").await;
    assert_eq!(st, StatusCode::OK);
    let items = v["activity"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    for i in items {
        assert_schema(i, "TalkgroupActivity");
        let buckets = i["buckets"].as_array().unwrap();
        let mut calls = 0;
        let mut seconds = 0.0;
        for b in buckets {
            assert_schema(b, "ActivityBucket");
            let t = time::OffsetDateTime::parse(b["time"].as_str().unwrap(), &time::format_description::well_known::Rfc3339).unwrap();
            assert_eq!(t.unix_timestamp() % 60, 0, "{b}");
            calls += b["calls"].as_i64().unwrap();
            seconds += b["seconds"].as_f64().unwrap();
        }
        assert_eq!(calls, i["call_count"].as_i64().unwrap());
        assert!((seconds - i["total_duration"].as_f64().unwrap()).abs() < 1e-9);
    }
    // tg 101: c2 (8 s) 30 minutes ago and c1 (12 s) 10 minutes ago, in minutes of their own.
    let minute = |ago: i64| rfc3339((s.now - ago) / 60_000 * 60_000);
    let b = &items[0]["buckets"];
    assert_eq!(b.as_array().unwrap().len(), 2);
    assert_eq!((b[0]["time"].as_str(), b[0]["calls"].as_i64(), b[0]["seconds"].as_f64()), (Some(minute(30 * MIN).as_str()), Some(1), Some(8.0)));
    assert_eq!((b[1]["time"].as_str(), b[1]["calls"].as_i64(), b[1]["seconds"].as_f64()), (Some(minute(10 * MIN).as_str()), Some(1), Some(12.0)));
}

#[tokio::test]
async fn too_many_buckets_is_a_400() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let (st, v) = get(&app, "/api/v1/stats/talkgroup-activity?after=2020-01-01T00:00:00Z&interval=10").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    assert!(v["error"].as_str().unwrap().contains("at most 10000"), "{v}");
    // The cap is talkgroups x ceil(window / interval), with explicit times so the clock does not matter.
    // One talkgroup (101), 10 s buckets: a window of exactly 10 000 buckets passes, one ms more is 10 001.
    let after = s.now - HOUR;
    let url = |tgids: &str, len_ms: i64| {
        format!("/api/v1/stats/talkgroup-activity?interval=10&tgids={tgids}&after={}&before={}", rfc3339(after), rfc3339(after + len_ms))
    };
    let (st, _) = get(&app, &url("101", 100_000_000)).await;
    assert_eq!(st, StatusCode::OK);
    let (st, v) = get(&app, &url("101", 100_000_001)).await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    assert!(v["error"].as_str().unwrap().contains("too many buckets: 10001,"), "{v}");
    // Two talkgroups (101 and 202, heard in the last 4 h): 5 000 buckets each is the limit.
    let after = s.now - 4 * HOUR;
    let url = |len_ms: i64| format!("/api/v1/stats/talkgroup-activity?interval=10&tgids=101,202&after={}&before={}", rfc3339(after), rfc3339(after + len_ms));
    let (st, _) = get(&app, &url(50_000_000)).await;
    assert_eq!(st, StatusCode::OK);
    let (st, v) = get(&app, &url(50_000_001)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(v["error"].as_str().unwrap().contains("too many buckets: 10002,"), "{v}");
}

#[tokio::test]
async fn a_bad_interval_is_a_400() {
    let (app, store, _, _) = app();
    seed(&store);
    for interval in ["7", "0", "-60", "minute", "61"] {
        let (st, v) = get(&app, &format!("/api/v1/stats/talkgroup-activity?interval={interval}")).await;
        assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")), "{interval}");
    }
}
