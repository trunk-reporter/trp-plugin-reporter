mod common;

use axum::http::{Method, StatusCode};
use common::*;
use serde_json::Value;

fn tgids(v: &Value) -> Vec<(i64, i64)> {
    v["talkgroups"].as_array().unwrap().iter().map(|t| (t["system_id"].as_i64().unwrap(), t["tgid"].as_i64().unwrap())).collect()
}

#[tokio::test]
async fn talkgroups_list_sorts_and_searches() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/talkgroups?sort=-calls_1h&limit=2&system_id=1").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "TalkgroupListResponse");
    assert_eq!((v["total"].as_i64(), v["limit"].as_i64(), v["offset"].as_i64()), (Some(3), Some(2), Some(0)));
    assert_eq!(tgids(&v), vec![(s.sys1.0, 101), (s.sys1.0, 9178)]); // ties on calls_1h fall back to tgid, descending
    let t = &v["talkgroups"][0];
    assert_schema(t, "Talkgroup");
    assert_eq!((t["calls_1h"].as_i64(), t["calls_24h"].as_i64(), t["call_count"].as_i64()), (Some(2), Some(2), Some(3)));
    assert_eq!((t["system_name"].as_str(), t["sysid"].as_str(), t["mode"].as_str()), (Some("ohmarcs"), Some("3AB"), Some("D")));
    assert!(t.get("relevance_score").is_none());

    let (_, v) = get(&app, "/api/v1/talkgroups?sort=tgid").await;
    assert_eq!(v["total"], 4);
    let (_, v) = get(&app, "/api/v1/talkgroups?sysid=3ab").await;
    assert_eq!(v["total"], 3);
    let (_, v) = get(&app, "/api/v1/talkgroups?sysid=ffff").await;
    assert_eq!(v["total"], 0);
    let (_, v) = get(&app, "/api/v1/talkgroups?group=police").await;
    assert_eq!(tgids(&v), vec![(s.sys1.0, 202)]);
    assert_eq!(v["talkgroups"][0]["mode"], "E");

    let (_, v) = get(&app, "/api/v1/talkgroups?search=fire").await;
    assert_eq!(tgids(&v), vec![(s.sys1.0, 101)]);
    assert_schema(&v["talkgroups"][0], "Talkgroup");
    assert!(v["talkgroups"][0]["relevance_score"].as_i64().is_some());

    let (st, v) = get(&app, "/api/v1/talkgroups?stats_days=7").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    let (st, _) = get(&app, "/api/v1/talkgroups?sort=bogus").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn plain_ids_are_ambiguous_across_systems() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/talkgroups/9178").await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(v["code"], "ambiguous_id");
    assert_eq!(v["matches"].as_array().unwrap().len(), 2);
    assert_schema(&v, "AmbiguousError");

    for path in [format!("/api/v1/talkgroups/{}:9178", s.sys1.0), format!("/api/v1/talkgroups/{}-9178", s.sys2.0)] {
        let (st, v) = get(&app, &path).await;
        assert_eq!(st, StatusCode::OK, "{path}");
        assert_schema(&v, "Talkgroup");
        assert_eq!(v["tgid"], 9178);
    }
    let (_, v) = get(&app, &format!("/api/v1/talkgroups/{}-9178", s.sys2.0)).await;
    assert_eq!((v["alpha_tag"].as_str(), v["system_name"].as_str()), (Some("Mutual Aid"), Some("fire-conv")));

    let (st, v) = get(&app, "/api/v1/talkgroups/101").await;
    assert_eq!((st, v["alpha_tag"].as_str(), v["calls_1h"].as_i64()), (StatusCode::OK, Some("Fire Disp"), Some(2)));
    let (st, _) = get(&app, "/api/v1/talkgroups/5").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get(&app, "/api/v1/talkgroups/9:9178").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get(&app, "/api/v1/talkgroups/abc").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, _) = request(&app, Method::PATCH, "/api/v1/talkgroups/101").await;
    assert_eq!(st, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn units_on_a_talkgroup_within_a_window() {
    let (app, store, _, _) = app();
    seed(&store);

    // Talkgroup 101: c1 (now-10m: units 10, 20), c2 (now-30m: unit 1234), the 1234 signal and the 10 join.
    let (st, v) = get(&app, "/api/v1/talkgroups/101/units?window=20").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "UnitListResponse");
    let ids: Vec<(i64, i64)> = v["units"].as_array().unwrap().iter().map(|u| (u["unit_id"].as_i64().unwrap(), u["call_count"].as_i64().unwrap())).collect();
    assert_eq!(ids, vec![(10, 1), (20, 1)]);
    assert_eq!(v["total"], 2);
    let u = &v["units"][0];
    assert_schema(u, "Unit");
    assert_eq!((u["alpha_tag"].as_str(), u["alpha_tag_source"].as_str(), u["last_event_type"].as_str()), (Some("Medic 10"), Some("csv"), Some("join")));
    assert_eq!((u["last_event_tgid"].as_i64(), u["last_event_tg_tag"].as_str()), (Some(101), Some("Fire Disp")));

    let (_, v) = get(&app, "/api/v1/talkgroups/101/units?window=60").await;
    assert_eq!(v["total"], 3);
    let (_, v) = get(&app, "/api/v1/talkgroups/101/units").await;
    assert_eq!(v["total"], 3);

    for w in ["0", "1441", "x"] {
        let (st, v) = get(&app, &format!("/api/v1/talkgroups/101/units?window={w}")).await;
        assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")), "window={w}");
    }
    let (st, _) = get(&app, "/api/v1/talkgroups/9178/units").await;
    assert_eq!(st, StatusCode::CONFLICT);
    let (st, _) = get(&app, "/api/v1/talkgroups/7/units").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn encryption_stats_count_clear_and_encrypted() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/talkgroups/encryption-stats?hours=24").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "EncryptionStatsResponse");
    assert_eq!((v["hours"].as_i64(), v["total"].as_i64()), (Some(24), Some(2)));
    let first = &v["stats"][0];
    assert_schema(first, "TalkgroupEncryptionStat");
    assert_eq!(
        (first["tgid"].as_i64(), first["encrypted_count"].as_i64(), first["clear_count"].as_i64(), first["total_count"].as_i64()),
        (Some(202), Some(1), Some(1), Some(2))
    );
    assert_eq!(first["encrypted_pct"].as_f64(), Some(50.0));
    assert_eq!(first["sysid"], "3AB");
    let second = &v["stats"][1];
    assert_eq!((second["tgid"].as_i64(), second["encrypted_count"].as_i64(), second["clear_count"].as_i64()), (Some(101), Some(0), Some(2)));

    let (_, v) = get(&app, "/api/v1/talkgroups/encryption-stats?hours=1").await;
    assert_eq!(v["total"], 1);
    let (_, v) = get(&app, "/api/v1/talkgroups/encryption-stats?hours=24&sysid=3ab").await;
    assert_eq!(v["total"], 2);
    let (_, v) = get(&app, &format!("/api/v1/talkgroups/encryption-stats?system_id={}", s.sys2.0)).await;
    assert_eq!(v["total"], 0);
    let (st, _) = get(&app, "/api/v1/talkgroups/encryption-stats?hours=0").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}
