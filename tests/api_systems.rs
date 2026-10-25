mod common;

use axum::http::{Method, StatusCode};
use common::*;
use serde_json::Value;

#[tokio::test]
async fn systems_list_with_sites_and_hex_identity() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let (status, v) = get(&app, "/api/v1/systems").await;
    assert_eq!(status, StatusCode::OK);
    assert_schema(&v, "SystemListResponse");
    assert_eq!(v["total"], 2);
    let systems = v["systems"].as_array().unwrap();
    assert_eq!(systems.len(), 2);
    for sys in systems {
        assert_schema(sys, "System");
        for site in sys["sites"].as_array().unwrap() {
            assert_schema(site, "Site");
        }
    }
    let p25 = &systems[0];
    assert_eq!((p25["system_id"].as_i64(), p25["system_type"].as_str(), p25["name"].as_str()), (Some(s.sys1.0), Some("p25"), Some("ohmarcs")));
    assert_eq!((p25["sysid"].as_str(), p25["wacn"].as_str()), (Some("3AB"), Some("BEE00")));
    let site = &p25["sites"][0];
    assert_eq!(site["site_id"].as_i64(), Some(s.site1.0));
    assert_eq!(site["system_id"].as_i64(), Some(s.sys1.0));
    assert_eq!((site["short_name"].as_str(), site["instance_id"].as_str(), site["nac"].as_str()), (Some("ohmarcs-dayton"), Some(HOST), Some("3A0")));
    assert_eq!((site["rfss"].as_i64(), site["p25_site_id"].as_i64()), (Some(1), Some(5)));
    assert!(site.get("sys_num").is_none_or(Value::is_null));

    let conv = &systems[1];
    assert_eq!((conv["system_type"].as_str(), conv["sysid"].as_str(), conv["wacn"].as_str()), (Some("conventional"), Some("0"), Some("0")));
    assert_eq!(conv["sites"].as_array().unwrap().len(), 1);
    assert_eq!(conv["sites"][0]["short_name"], "fire-conv");
}

#[tokio::test]
async fn one_system_and_one_site_by_id() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let (status, v) = get(&app, &format!("/api/v1/systems/{}", s.sys1.0)).await;
    assert_eq!(status, StatusCode::OK);
    assert_schema(&v, "System");
    assert_eq!(v["sites"].as_array().unwrap().len(), 1);
    let (status, v) = get(&app, &format!("/api/v1/sites/{}", s.site1.0)).await;
    assert_eq!(status, StatusCode::OK);
    assert_schema(&v, "Site");
    assert_eq!((v["short_name"].as_str(), v["nac"].as_str()), (Some("ohmarcs-dayton"), Some("3A0")));
    let (status, v) = get(&app, "/api/v1/sites/999").await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
    assert_eq!(get(&app, "/api/v1/systems/abc").await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_missing_system_is_404_and_patch_is_501() {
    let (app, store, _, _) = app();
    seed(&store);
    let (status, v) = get(&app, "/api/v1/systems/99").await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
    for path in ["/api/v1/systems/1", "/api/v1/sites/1"] {
        let (status, v) = request(&app, Method::PATCH, path).await;
        assert_eq!((status, v["code"].as_str()), (StatusCode::NOT_IMPLEMENTED, Some("not_implemented")), "{path}");
    }
}

#[tokio::test]
async fn p25_systems_count_their_things() {
    let (app, store, _, _) = app();
    let s = seed(&store);
    let (status, v) = get(&app, "/api/v1/p25-systems").await;
    assert_eq!(status, StatusCode::OK);
    assert_schema(&v, "P25SystemListResponse");
    assert_eq!(v["total"], 1);
    let sys = &v["systems"][0];
    assert_schema(sys, "P25System");
    assert_eq!(sys["system_id"].as_i64(), Some(s.sys1.0));
    assert_eq!((sys["sysid"].as_str(), sys["wacn"].as_str()), (Some("3AB"), Some("BEE00")));
    assert_eq!(sys["sites"].as_array().unwrap().len(), 1);
    // Talkgroups 101, 202, 9178; units 1234, 10, 20, 30; calls c1..c4 in the last 24 h.
    assert_eq!((sys["talkgroup_count"].as_i64(), sys["unit_count"].as_i64(), sys["calls_24h"].as_i64()), (Some(3), Some(4), Some(4)));
}
