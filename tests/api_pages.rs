//! The embedded pages: `/api/v1/pages`, `/`, the page assets, and a check that
//! no page calls an endpoint this plugin does not serve (spec C8).

mod common;

use axum::http::{Method, StatusCode};
use common::{app, app_with, get, get_with, request};
use regex::Regex;
use reporter::api::SERVED_PREFIXES;
use reporter::web::Assets;

#[tokio::test]
async fn pages_are_listed_from_their_meta_tags_in_order() {
    let (app, ..) = app();
    let (s, v) = get(&app, "/api/v1/pages").await;
    assert_eq!(s, StatusCode::OK);
    let pages = v.as_array().expect("an array");
    assert!(pages.iter().any(|p| p["path"] == "/call-history.html" && p["title"] == "Call History"), "{v}");
    assert!(pages.iter().any(|p| p["path"] == "/index.html" && p["title"] == "Event Horizon"), "{v}");
    assert!(!pages.iter().any(|p| p["path"] == "/storage.html"), "{v}");
    let orders: Vec<i64> = pages.iter().map(|p| p["order"].as_i64().expect("order")).collect();
    assert!(orders.windows(2).all(|w| w[0] <= w[1]), "not sorted: {orders:?}");
    assert_eq!(pages.len(), 8, "{v}");
    for p in pages {
        assert!(p["description"].is_string(), "{p}");
    }
}

#[tokio::test]
async fn the_index_and_assets_are_served() {
    let (app, ..) = app();
    let (s, h, body) = get_with(&app, "/", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert!(h["content-type"].to_str().unwrap().starts_with("text/html"));
    assert!(String::from_utf8_lossy(&body).contains("card-title"));

    let (s, h, body) = get_with(&app, "/auth.js", &[]).await;
    assert_eq!(s, StatusCode::OK);
    let ct = h["content-type"].to_str().unwrap();
    assert!(ct.contains("javascript"), "{ct}");
    assert!(!body.is_empty());

    let (s, ..) = get_with(&app, "/call-history.html?x=1", &[]).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = get_with(&app, "/nope.html", &[]).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, ..) = get_with(&app, "/storage.html", &[]).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pages_and_assets_need_no_token_and_the_api_wins_the_path() {
    let (app, ..) = app_with(Some("s3"));
    let (s, ..) = get_with(&app, "/", &[]).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = get_with(&app, "/theme-engine.js", &[]).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = get_with(&app, "/api/v1/stats", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, ..) = get_with(&app, "/api/v1/nope", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn posting_a_page_is_not_implemented() {
    let (app, ..) = app();
    let (s, _) = request(&app, Method::POST, "/api/v1/pages").await;
    assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
}

#[test]
fn every_embedded_page_only_calls_served_endpoints() {
    let direct = Regex::new(r"/api/v1/([A-Za-z0-9_.-]+)").unwrap();
    let fetched = Regex::new(r#"apiFetch\(\s*['"`]/([A-Za-z0-9_.-]+)"#).unwrap();
    let mut checked = 0;
    for name in Assets::iter().filter(|n| n.ends_with(".html")) {
        let file = Assets::get(&name).expect("embedded");
        let text = String::from_utf8_lossy(&file.data);
        for re in [&direct, &fetched] {
            for c in re.captures_iter(&text) {
                let seg = &c[1];
                assert!(SERVED_PREFIXES.contains(&seg), "{name} calls /{seg}, which is not served");
                checked += 1;
            }
        }
    }
    assert!(checked > 0, "no API calls found; the regexes are wrong");
}
