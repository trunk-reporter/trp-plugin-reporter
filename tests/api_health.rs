mod common;

use axum::http::{Method, StatusCode};
use common::*;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use trunk_reporter::api::OUT_OF_SCOPE;

#[tokio::test]
async fn health_is_open_and_shaped() {
    let (s, v) = get(&app().0, "/api/v1/health").await;
    assert_eq!(s, 200);
    assert_schema(&v, "HealthResponse");
    assert_eq!(v["status"], "healthy");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["checks"]["database"], "ok");
    let recorders = v["trunk_recorders"].as_array().expect("trunk_recorders");
    assert_eq!(recorders.len(), 1);
    assert_eq!((recorders[0]["instance_id"].as_str(), recorders[0]["status"].as_str()), (Some(HOST), Some("connected")));
    assert!(OffsetDateTime::parse(recorders[0]["last_seen"].as_str().unwrap_or_default(), &Rfc3339).is_ok(), "{v}");
}

#[tokio::test]
async fn health_says_transcriptions_when_a_transcribe_url_is_set() {
    let app = trunk_reporter::api::router(state_with(None).0.with_transcribe("http://127.0.0.1:8767").unwrap());
    let (s, v) = get(&app, "/api/v1/health").await;
    assert_eq!(s, 200);
    assert_schema(&v, "HealthResponse");
    assert_eq!(v["features"]["transcriptions"], true);
    assert_eq!(v["transcribe"]["url"], "http://127.0.0.1:8767");
    assert_eq!(v["transcribe"]["connected"], false);
    assert!(v["transcribe"]["last_event_ms"].is_null(), "{v}");
    // A blank URL leaves the bridge off.
    let blank = trunk_reporter::api::router(state_with(None).0.with_transcribe("  ").unwrap());
    assert_eq!(get(&blank, "/api/v1/health").await.1["features"]["transcriptions"], false);
    let (_, v) = get(&app_with(None).0, "/api/v1/health").await;
    assert_eq!(v["features"]["transcriptions"], false);
    assert!(v.get("transcribe").is_none(), "{v}");
}

#[tokio::test]
async fn auth_init_says_open_or_token() {
    let (s, v) = get(&app().0, "/api/v1/auth-init").await;
    assert_eq!(s, 200);
    assert_eq!((v["mode"].as_str(), v["jwt_enabled"].as_bool()), (Some("open"), Some(false)));
    let (s, v) = get(&app_with(Some("s3")).0, "/api/v1/auth-init").await;
    assert_eq!(s, 200);
    assert_eq!((v["mode"].as_str(), v["jwt_enabled"].as_bool()), (Some("token"), Some(false)));
    assert!(v.get("read_token").is_none(), "the token is never handed out: {v}");
    // An empty token is no token.
    assert_eq!(get(&app_with(Some("")).0, "/api/v1/auth-init").await.1["mode"], "open");
}

#[tokio::test]
async fn a_token_guards_the_api_but_not_health_or_pages() {
    let app = app_with(Some("s3")).0;
    // /systems is a guarded path (501 until Task 11 serves it): what matters here is 401 or not.
    let (s, v) = get(&app, "/api/v1/systems").await;
    assert_eq!(s, 401);
    assert_eq!(v["code"], "unauthorized");
    assert_schema(&v, "Error");
    assert_eq!(get_with(&app, "/api/v1/systems", &[("Authorization", "Bearer wrong")]).await.0, 401);
    assert_eq!(get(&app, "/api/v1/systems?token=wrong").await.0, 401);
    assert_eq!(get(&app, "/api/v1/nothing").await.0, 401, "unknown paths are guarded too");
    assert_ne!(get_with(&app, "/api/v1/systems", &[("Authorization", "Bearer s3")]).await.0, 401);
    assert_ne!(get(&app, "/api/v1/systems?token=s3").await.0, 401);
    for open in ["/api/v1/health", "/api/v1/auth-init", "/api/v1/pages", "/api/v1/openapi.yaml"] {
        assert_eq!(get(&app, open).await.0, 200, "{open}");
    }
    // Outside /api/ nothing is guarded: the pages are open.
    assert_eq!(get(&app, "/index.html").await.0, 200);
    assert_eq!(get(&app, "/nope.html").await.0, 404);
}

#[tokio::test]
async fn pages_and_the_spec_are_served() {
    let app = app().0;
    let (s, v) = get(&app, "/api/v1/pages").await;
    assert_eq!(s, StatusCode::OK);
    assert!(v.as_array().is_some_and(|pages| !pages.is_empty()), "the embedded pages are listed: {v}");
    let (s, headers, body) = get_with(&app, "/api/v1/openapi.yaml", &[]).await;
    assert_eq!(s, 200);
    assert_eq!(headers["content-type"], "text/yaml");
    assert_eq!(&body[..], include_str!("../openapi/openapi.yaml").as_bytes());
}

/// tr-engine's pages read a 403 from `/query` as "turned off" and go on
/// without it (the stream graph then shows live traffic only); a 501 stops them.
#[tokio::test]
async fn ad_hoc_sql_is_forbidden_not_unimplemented() {
    let (s, v) = request(&app().0, Method::POST, "/api/v1/query").await;
    assert_eq!(s, 403, "{v}");
    assert_eq!(v["code"], "forbidden");
    assert_schema(&v, "Error");
}

#[tokio::test]
async fn out_of_scope_paths_say_so_with_501() {
    let (s, v) = get(&app().0, "/api/v1/transcriptions/search").await;
    assert_eq!(s, 501);
    assert_eq!(v["code"], "not_implemented");
    assert_eq!(v["error"], "not available in the Trunk Recorder Pro plugin");
    assert_eq!(v["detail"], "/api/v1/transcriptions/search");
    assert_schema(&v, "Error");
    // Any method.
    assert_eq!(request(&app().0, Method::POST, "/api/v1/call-upload").await.0, 501);
}

#[tokio::test]
async fn unknown_paths_are_404() {
    let (s, v) = get(&app().0, "/api/v1/nothing").await;
    assert_eq!(s, 404);
    assert_eq!(v["code"], "not_found");
    assert_eq!(v["error"], "no endpoint GET /api/v1/nothing");
    assert_schema(&v, "Error");
}

#[tokio::test]
async fn every_spec_path_is_served_or_declared_out_of_scope() {
    let app = app().0;
    let paths = spec()["paths"].as_mapping().expect("paths");
    let mut missing = Vec::new();
    for (path, ops) in paths {
        let path = path.as_str().expect("path");
        // Every `{param}` → "1".
        let concrete: String = path.split('/').map(|seg| if seg.starts_with('{') { "1" } else { seg }).collect::<Vec<_>>().join("/");
        for (method, _) in ops.as_mapping().expect("operations") {
            let method = method.as_str().expect("method");
            let Ok(m) = Method::from_bytes(method.to_uppercase().as_bytes()) else { continue };
            if concrete == "/events/stream" {
                // Never ends: take the head only.
                use tower::ServiceExt;
                let req = axum::http::Request::builder().method(m).uri("/api/v1/events/stream").body(axum::body::Body::empty()).expect("request");
                let res = app.clone().oneshot(req).await.expect("router is infallible");
                assert_eq!(res.status(), 200, "the event stream answers at once");
                continue;
            }
            let (s, v) = request(&app, m, &format!("/api/v1{concrete}")).await;
            // A routed handler may 404 a missing row (the store is empty); only the fallback's "no endpoint" 404 means unrouted.
            let unrouted = s == 404 && v["error"].as_str().is_some_and(|e| e.starts_with("no endpoint "));
            if unrouted || s == 405 {
                missing.push(format!("{method} {path} → {s}"));
            }
        }
    }
    assert!(missing.is_empty(), "spec paths neither served nor declared out of scope:\n{}", missing.join("\n"));
    for p in OUT_OF_SCOPE {
        assert!(paths.contains_key(*p), "OUT_OF_SCOPE lists {p}, which is not a spec path");
    }
}

#[test]
fn the_seed_is_what_its_doc_says() {
    use trunk_reporter::store::calls::{CallFilter, list_calls};
    use trunk_reporter::store::systems::{list_sites, system_by_id};
    use trunk_reporter::store::talkgroups::{talkgroup, talkgroups_by_tgid};
    use trunk_reporter::store::units::{unit, units_by_id};

    let (_, store, _, _) = app();
    let seed = seed(&store);
    let sys1 = store.read(|c| system_by_id(c, seed.sys1)).unwrap().unwrap();
    assert_eq!((sys1.short_name.as_str(), sys1.kind.as_str(), sys1.sysid, sys1.wacn, sys1.nac), ("ohmarcs", "p25", Some(0x3AB), Some(0xBEE00), Some(0x3A0)));
    let sys2 = store.read(|c| system_by_id(c, seed.sys2)).unwrap().unwrap();
    assert_eq!((sys2.short_name.as_str(), sys2.kind.as_str(), sys2.sysid), ("fire-conv", "conventional", None));
    let sites = store.read(|c| list_sites(c, seed.sys2)).unwrap();
    assert_eq!((sites.len(), sites[0].name.as_str(), sites[0].id), (1, "fire-conv", seed.site2));

    let (calls, total) = store.read(|c| list_calls(c, &CallFilter { limit: 100, ..Default::default() })).unwrap();
    assert_eq!((total, calls.len(), seed.calls.len()), (6, 6, 6));
    let c4 = calls.iter().find(|c| c.id == seed.calls[3]).unwrap();
    assert_eq!((c4.recorded, c4.not_recorded_reason.as_deref()), (false, Some("encrypted")));
    assert!(calls.iter().find(|c| c.id == seed.calls[1]).unwrap().emergency);
    assert!(calls.iter().all(|c| c.start_ms >= seed.now - 40 * DAY));

    let tg101 = store.read(|c| talkgroup(c, seed.sys1, 101, seed.now)).unwrap().unwrap();
    assert_eq!((tg101.alpha_tag.as_str(), tg101.call_count), ("Fire Disp", 3));
    assert_eq!(store.read(|c| talkgroups_by_tgid(c, 9178, seed.now)).unwrap().len(), 2);
    assert_eq!(store.read(|c| units_by_id(c, 1234)).unwrap().len(), 2);
    let u10 = store.read(|c| unit(c, seed.sys1, 10)).unwrap().unwrap();
    assert_eq!((u10.alpha_tag.as_str(), u10.last_event_type.as_deref(), u10.last_event_tgid), ("Medic 10", Some("join"), Some(101)));
    let u30 = store.read(|c| unit(c, seed.sys1, 30)).unwrap().unwrap();
    assert_eq!(u30.last_seen_ms, seed.now - 2 * HOUR);
}

#[test]
fn the_schema_check_catches_mismatches() {
    use serde_json::json;
    let fails = |v: Value, schema: &'static str| std::panic::catch_unwind(move || assert_schema(&v, schema)).is_err();
    assert!(!fails(json!({"code": "not_found", "error": "x"}), "Error"));
    assert!(fails(json!({"code": "not_found"}), "Error"), "a required property is missing");
    assert!(fails(json!({"code": "nope", "error": "x"}), "Error"), "a value outside the enum");
    assert!(fails(json!({"code": "not_found", "error": 3}), "Error"), "a wrong type");
    assert!(fails(json!({"status": "healthy", "uptime_seconds": 1.5}), "HealthResponse"), "a number is not an integer");
    assert!(fails(json!({"status": "healthy", "checks": []}), "HealthResponse"), "an array is not an object");
    assert!(fails(json!([]), "HealthResponse"), "the value itself has the wrong type");
    assert!(fails(json!({"code": "ambiguous_id", "error": "x", "matches": [1]}), "AmbiguousError"), "array items are checked");
    assert!(fails(json!({"code": null, "error": "x"}), "Error"), "null where not nullable");
}

#[tokio::test]
async fn health_says_what_this_server_can_do() {
    let (s, v) = get(&app().0, "/api/v1/health").await;
    assert_eq!(s, 200);
    assert_schema(&v, "HealthResponse");
    let f = &v["features"];
    let flag = |k: &str| f[k].as_bool().unwrap_or_else(|| panic!("features.{k} is a boolean: {f}"));
    assert_eq!((flag("transcriptions"), flag("live_audio"), flag("hub"), flag("recorders")), (false, true, false, true));
    assert_eq!((flag("call_groups"), flag("directory"), flag("users")), (false, false, false));
}

#[test]
fn the_examples_added_for_phase_2_match_their_schemas() {
    for name in ["Features", "ActiveTalkgroupListResponse"] {
        let example = &spec()["components"]["schemas"][name]["example"];
        assert!(!example.is_null(), "{name} has no example");
        assert_schema(&serde_json::to_value(example).unwrap(), name);
    }
}
