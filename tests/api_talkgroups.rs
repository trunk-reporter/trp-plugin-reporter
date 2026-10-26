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

#[tokio::test]
async fn active_talkgroups_are_those_with_calls_in_the_window() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    // Talkgroup 555 has no talkgroups row: two calls, the latest a minute ago.
    store
        .write_sync(move |c| {
            for (ago, len) in [(60_000, 3_000), (120_000, 4_000)] {
                c.execute(
                    "INSERT INTO calls (system_id, tgid, start_ms, end_ms, state) VALUES (?1, 555, ?2, ?3, 'concluded')",
                    rusqlite::params![s.sys1.0, s.now - ago, s.now - ago + len],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let (after, before) = (s.now - 4 * 3_600_000, s.now + 1_000);
    let (after_s, before_s) = (trunk_reporter::api::rfc3339(after), trunk_reporter::api::rfc3339(before));
    let url = format!("/api/v1/talkgroups/active?after={after_s}&before={before_s}");
    let (st, v) = get(&app, &url).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "ActiveTalkgroupListResponse");
    // Busiest first, ties by the later last call; c4 (5 h ago) and older are outside.
    let rows: Vec<_> = v["talkgroups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| (t["tgid"].as_i64().unwrap(), t["calls"].as_i64().unwrap(), t["seconds"].as_f64().unwrap()))
        .collect();
    assert_eq!(rows, vec![(555, 2, 7.0), (101, 2, 20.0), (202, 1, 20.0)]);
    let unnamed = &v["talkgroups"][0];
    assert_schema(unnamed, "ActiveTalkgroup");
    assert_eq!((unnamed["tg_alpha_tag"].as_str(), unnamed["tg_tag"].as_str(), unnamed["tg_group"].as_str()), (Some(""), Some(""), Some("")));
    assert_eq!(unnamed["system_id"].as_i64(), Some(s.sys1.0));
    let named = &v["talkgroups"][1];
    assert_eq!((named["tg_alpha_tag"].as_str(), named["tg_tag"].as_str(), named["tg_group"].as_str()), (Some("Fire Disp"), Some("Dispatch"), Some("Fire")));
    assert_eq!(named["system_name"], "ohmarcs");
    assert!(named["last_call"].as_str().unwrap().ends_with('Z'));
    assert_eq!(
        (v["after"].as_str().map(str::to_owned), v["before"].as_str().map(str::to_owned)),
        (Some(trunk_reporter::api::rfc3339(after)), Some(trunk_reporter::api::rfc3339(before)))
    );

    // The window is half open, the limit cuts, the system filter filters.
    let (_, v) = get(&app, &format!("/api/v1/talkgroups/active?after={after_s}&before={}", trunk_reporter::api::rfc3339(s.now - 10 * 60_000))).await;
    assert_eq!(v["talkgroups"].as_array().unwrap().iter().map(|t| t["tgid"].as_i64().unwrap()).collect::<Vec<_>>(), vec![101, 202]);
    let (_, v) = get(&app, &format!("{url}&limit=1")).await;
    assert_eq!(v["talkgroups"].as_array().unwrap().len(), 1);
    let (_, v) = get(&app, &format!("{url}&system_id={}", s.sys2.0)).await;
    assert_eq!(v["talkgroups"].as_array().unwrap().len(), 0);
    let (st, _) = get(&app, &format!("/api/v1/talkgroups/active?after={before_s}&before={after_s}")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn active_talkgroups_default_window_is_ten_minutes() {
    let (app, store, _, _) = app();
    let now = trunk_reporter::store::now_ms();
    store
        .write_sync(move |c| {
            let sys = trunk_reporter::store::systems::ensure_system(c, "sys1", "p25", "", now - 3_600_000)?;
            for (tgid, ago) in [(11, 5 * 60_000), (22, 20 * 60_000)] {
                c.execute(
                    "INSERT INTO calls (system_id, tgid, start_ms, end_ms, state) VALUES (?1, ?2, ?3, ?3, 'concluded')",
                    rusqlite::params![sys.0, tgid, now - ago],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let (st, v) = get(&app, "/api/v1/talkgroups/active").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "ActiveTalkgroupListResponse");
    assert_eq!(v["talkgroups"].as_array().unwrap().iter().map(|t| t["tgid"].as_i64().unwrap()).collect::<Vec<_>>(), vec![11]);
    let parse = |k: &str| time::OffsetDateTime::parse(v[k].as_str().unwrap(), &time::format_description::well_known::Rfc3339).unwrap();
    assert_eq!((parse("before") - parse("after")).whole_seconds(), 600);
}
