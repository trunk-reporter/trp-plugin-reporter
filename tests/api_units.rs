mod common;

use axum::http::{Method, StatusCode};
use common::*;
use serde_json::Value;
use trunk_reporter::affiliations::Names;
use trunk_reporter::api::rfc3339;

fn unit_keys(v: &Value) -> Vec<(i64, i64)> {
    v["units"].as_array().unwrap().iter().map(|u| (u["system_id"].as_i64().unwrap(), u["unit_id"].as_i64().unwrap())).collect()
}

fn event_types(v: &Value) -> Vec<String> {
    v["events"].as_array().unwrap().iter().map(|e| e["event_type"].as_str().unwrap().to_string()).collect()
}

#[tokio::test]
async fn units_list_filters_by_activity_and_talkgroup() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/units").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "UnitListResponse");
    assert_eq!(v["total"], 5);
    assert_eq!(unit_keys(&v)[0], (s.sys1.0, 10)); // default sort: unit_id
    assert_schema(&v["units"][0], "Unit");

    // 30 was last seen 2 h ago and sys2's 1234 a day ago.
    let (_, v) = get(&app, "/api/v1/units?active_within=45&sort=unit_id").await;
    assert_eq!(unit_keys(&v), vec![(s.sys1.0, 10), (s.sys1.0, 20), (s.sys1.0, 1234)]);

    let (_, v) = get(&app, "/api/v1/units?talkgroup=101").await;
    assert_eq!(unit_keys(&v), vec![(s.sys1.0, 10), (s.sys1.0, 20), (s.sys1.0, 1234)]);
    let (_, v) = get(&app, "/api/v1/units?talkgroup=9178").await;
    assert_eq!(unit_keys(&v), vec![(s.sys2.0, 1234)]);

    let (_, v) = get(&app, "/api/v1/units?sysid=3ab&sort=-unit_id&limit=1").await;
    assert_eq!((v["total"].as_i64(), unit_keys(&v)), (Some(4), vec![(s.sys1.0, 1234)]));
    let (_, v) = get(&app, "/api/v1/units?search=medic").await;
    assert_eq!(unit_keys(&v), vec![(s.sys1.0, 10)]);
    let (_, v) = get(&app, "/api/v1/units?sort=last_event_time").await;
    assert_schema(&v, "UnitListResponse");

    for bad in ["/api/v1/units?active_within=soon", "/api/v1/units?sort=bogus"] {
        let (st, v) = get(&app, bad).await;
        assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    }
}

#[tokio::test]
async fn a_unit_detail_embeds_its_last_event() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/units/10").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "Unit");
    assert_eq!((v["last_event_type"].as_str(), v["last_event_tgid"].as_i64(), v["last_event_tg_tag"].as_str()), (Some("join"), Some(101), Some("Fire Disp")));
    assert_eq!((v["alpha_tag"].as_str(), v["system_name"].as_str(), v["sysid"].as_str()), (Some("Medic 10"), Some("ohmarcs"), Some("3AB")));

    let (st, v) = get(&app, &format!("/api/v1/units/{}:20", s.sys1.0)).await;
    assert_eq!((st, v["last_event_type"].as_str()), (StatusCode::OK, Some("off")));
    let (st, v) = get(&app, "/api/v1/units/999").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
    let (st, _) = get(&app, "/api/v1/units/abc").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, v) = request(&app, Method::PATCH, "/api/v1/units/10").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::NOT_IMPLEMENTED, Some("not_implemented")));
}

#[tokio::test]
async fn unit_events_need_a_system_and_cap_the_range() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/unit-events").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
    let (start, end) = (rfc3339(s.now - 25 * HOUR), rfc3339(s.now));
    let (st, v) = get(&app, &format!("/api/v1/unit-events?system_id=1&start_time={start}&end_time={end}")).await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_time_range")));

    let three_h = rfc3339(s.now - 3 * HOUR);
    let (st, v) = get(&app, &format!("/api/v1/unit-events?system_id={}&type=join,on&start_time={three_h}", s.sys1.0)).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "UnitEventListResponse");
    assert_eq!((v["total"].as_i64(), event_types(&v)), (Some(2), vec!["join".to_string(), "on".to_string()])); // newest first
    let e = &v["events"][0];
    assert_schema(e, "UnitEvent");
    assert_eq!((e["unit_id"].as_i64(), e["unit_rid"].as_i64(), e["tgid"].as_i64()), (Some(10), Some(10), Some(101)));
    assert_eq!((e["tg_alpha_tag"].as_str(), e["unit_alpha_tag"].as_str(), e["instance_id"].as_str()), (Some("Fire Disp"), Some("Medic 10"), Some(HOST)));

    // By sysid, default range (last hour): the signal event carries its metadata and lifted fields.
    let (_, v) = get(&app, "/api/v1/unit-events?sysid=3ab&type=signal").await;
    assert_eq!(v["total"], 1);
    let e = &v["events"][0];
    assert_schema(e, "UnitEvent");
    assert_eq!((e["signaling_type"].as_str(), e["signal_type"].as_str()), (Some("P25"), Some("emergency")));
    assert_eq!(e["metadata_json"]["signal_type"], "emergency");
    let (_, v) = get(&app, "/api/v1/unit-events?sysid=3ab&emergency=true").await;
    assert!(v["total"].as_i64().unwrap() >= 1);
    let (_, v) = get(&app, "/api/v1/unit-events?sysid=ffff").await;
    assert_eq!(v["total"], 0);

    let (_, v) = get(&app, &format!("/api/v1/unit-events?system_id=1&unit_id=10&tgid=101&sort=event_type&start_time={three_h}")).await;
    assert!(v["total"].as_i64().unwrap() >= 2);
    let types = event_types(&v);
    assert!(types.windows(2).all(|w| w[0] <= w[1]));
    let (st, _) = get(&app, "/api/v1/unit-events?system_id=1&type=bogus").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // One unit's events.
    let (st, v) = get(&app, "/api/v1/units/10/events?type=join").await;
    assert_eq!((st, event_types(&v)), (StatusCode::OK, vec!["join".to_string()]));
    assert_schema(&v, "UnitEventListResponse");
    let (_, v) = get(&app, "/api/v1/units/10/events?talkgroup=202").await;
    assert!(v["events"].as_array().unwrap().iter().all(|e| e["tgid"] == 202));
    let (st, _) = get(&app, "/api/v1/units/1234/events").await;
    assert_eq!(st, StatusCode::CONFLICT);
}

#[tokio::test]
async fn affiliations_come_from_the_live_map() {
    let (app, store, _, affiliations) = app();
    let s = seed(&store);
    let names = Names { unit_alpha_tag: "Medic 10".into(), tg_alpha_tag: "Fire Disp".into(), ..Default::default() };
    affiliations.apply(s.sys1.0, 10, "join", Some(101), s.now - 5 * MIN, names);
    affiliations.apply(s.sys1.0, 20, "join", Some(101), s.now - 20 * MIN, Names::default());
    affiliations.apply(s.sys1.0, 20, "off", None, s.now - 8 * MIN, Names::default());

    let (st, v) = get(&app, "/api/v1/unit-affiliations?status=affiliated").await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "AffiliationListResponse");
    assert_eq!(v["total"], 1);
    assert_eq!(v["summary"]["talkgroup_counts"], serde_json::json!({"101": 1}));
    let a = &v["affiliations"][0];
    assert_schema(a, "Affiliation");
    assert_eq!((a["unit_id"].as_i64(), a["tgid"].as_i64(), a["status"].as_str()), (Some(10), Some(101), Some("affiliated")));
    assert_eq!((a["sysid"].as_str(), a["system_name"].as_str(), a["unit_alpha_tag"].as_str()), (Some("3AB"), Some("ohmarcs"), Some("Medic 10")));

    let (_, v) = get(&app, "/api/v1/unit-affiliations?status=off").await;
    assert_eq!((v["total"].as_i64(), v["affiliations"][0]["unit_id"].as_i64()), (Some(1), Some(20)));
    let (_, v) = get(&app, "/api/v1/unit-affiliations").await;
    assert_eq!(v["total"], 2);
    let (_, v) = get(&app, "/api/v1/unit-affiliations?active_within=360").await; // 6 min
    assert_eq!(v["total"], 1);
    let (_, v) = get(&app, "/api/v1/unit-affiliations?stale_threshold=3600&tgid=101&sysid=3ab&unit_id=10&limit=1").await;
    assert_eq!((v["total"].as_i64(), v["limit"].as_i64()), (Some(1), Some(1)));
    let (_, v) = get(&app, "/api/v1/unit-affiliations?sysid=ffff").await;
    assert_eq!(v["total"], 0);
    let (st, v) = get(&app, "/api/v1/unit-affiliations?status=bogus").await;
    assert_eq!((st, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_parameter")));
}

#[tokio::test]
async fn unit_ids_are_ambiguous_too() {
    let (app, store, _, _) = app();
    let s = seed(&store);

    let (st, v) = get(&app, "/api/v1/units/1234").await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(v["code"], "ambiguous_id");
    assert_eq!(v["matches"].as_array().unwrap().len(), 2);
    assert_schema(&v, "AmbiguousError");

    for path in [format!("/api/v1/units/{}:1234", s.sys1.0), format!("/api/v1/units/{}-1234", s.sys2.0)] {
        let (st, v) = get(&app, &path).await;
        assert_eq!(st, StatusCode::OK, "{path}");
        assert_schema(&v, "Unit");
    }
}

#[tokio::test]
async fn a_units_talkgroups_in_a_window_come_from_its_transmissions() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    // Medic 10 keys up a second time on 101 in the call from 10 minutes ago.
    let (extra_ms, c1) = (s.now - 9 * 60_000, s.calls[0].0);
    store
        .write_sync(move |c| {
            c.execute(
                "INSERT INTO call_transmissions (call_id, position, unit, time_ms, pos_s, len_s) VALUES (?1, 9, 10, ?2, 0, 4.0)",
                rusqlite::params![c1, extra_ms],
            )?;
            Ok(())
        })
        .unwrap();
    let url = |after: i64, before: i64| format!("/api/v1/units/10/talkgroups?after={}&before={}", rfc3339(after), rfc3339(before));
    let rows = |v: &Value| -> Vec<(i64, i64, i64)> {
        v["talkgroups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| (t["tgid"].as_i64().unwrap(), t["calls"].as_i64().unwrap(), t["transmissions"].as_i64().unwrap()))
            .collect()
    };

    // The last four hours: the call from 40 days ago is outside; 101 has two transmissions in one call.
    let (st, v) = get(&app, &url(s.now - 4 * 3_600_000, s.now + 1_000)).await;
    assert_eq!(st, StatusCode::OK);
    assert_schema(&v, "UnitTalkgroupListResponse");
    assert_eq!((v["unit_id"].as_i64(), v["system_id"].as_i64()), (Some(10), Some(s.sys1.0)));
    assert_eq!(rows(&v), vec![(101, 1, 2), (202, 1, 1)]);
    let t = &v["talkgroups"][0];
    assert_schema(t, "UnitTalkgroup");
    assert_eq!(t["tg_alpha_tag"], "Fire Disp");
    assert_eq!(t["seconds"].as_f64(), Some(10.0));
    assert_eq!(t["last_time"].as_str(), Some(rfc3339(extra_ms).as_str()));
    assert!(t["first_time"].as_str().unwrap() < t["last_time"].as_str().unwrap());

    // Half open: ending at the later transmission leaves it out.
    let (_, v) = get(&app, &url(s.now - 4 * 3_600_000, extra_ms)).await;
    assert_eq!(rows(&v), vec![(101, 1, 1), (202, 1, 1)]);

    // A wide window takes in the old call; ties on transmissions go by tgid.
    let (_, v) = get(&app, &url(s.now - 60 * 86_400_000, s.now + 1_000)).await;
    assert_eq!(rows(&v), vec![(101, 1, 2), (202, 2, 2)]);

    // The composite id, the default window (24 h), and an inverted window.
    let (_, v) = get(&app, &format!("/api/v1/units/{}:10/talkgroups", s.sys1.0)).await;
    assert_eq!(rows(&v), vec![(101, 1, 2), (202, 1, 1)]);
    let (st, _) = get(&app, &url(s.now, s.now - 1)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_unknown_unit_is_a_404_for_talkgroups() {
    let (app, store, _, _) = app();
    seed(&store);
    let (st, v) = get(&app, "/api/v1/units/999999/talkgroups").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_schema(&v, "Error");
}
