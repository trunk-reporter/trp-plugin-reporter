//! CORS for the origins an operator lists.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{app_with_cors, get_with, send};
use trunk_reporter::config::Config;

const ORIGIN: &str = "http://127.0.0.1:8081";

#[tokio::test]
async fn a_listed_origin_gets_cors_headers_and_a_preflight() {
    let app = app_with_cors(&[ORIGIN], Some("secret"));

    let (status, h, _) = get_with(&app, "/api/v1/health", &[("origin", ORIGIN)]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h["access-control-allow-origin"], ORIGIN);
    assert_eq!(h["vary"], "Origin");
    assert_eq!(h["access-control-expose-headers"], "Content-Length, Content-Range");

    // A preflight carries no Authorization, and a token is set.
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/v1/calls")
        .header("origin", ORIGIN)
        .header("access-control-request-method", "GET")
        .body(Body::empty())
        .unwrap();
    let (status, h, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(h["access-control-allow-origin"], ORIGIN);
    assert_eq!(h["access-control-allow-headers"], "Authorization, Content-Type");
    assert_eq!(h["access-control-allow-methods"], "GET, PUT, POST, OPTIONS");
    assert_eq!(h["access-control-max-age"], "600");
}

#[tokio::test]
async fn an_unlisted_origin_gets_none() {
    let app = app_with_cors(&[ORIGIN], Some("secret"));
    let (status, h, _) = get_with(&app, "/api/v1/calls", &[("origin", "http://evil")]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(h.get("access-control-allow-origin").is_none());

    let req = Request::builder().method(Method::OPTIONS).uri("/api/v1/calls").header("origin", "http://evil").body(Body::empty()).unwrap();
    let (status, h, _) = send(&app, req).await;
    assert_ne!(status, StatusCode::NO_CONTENT);
    assert!(h.get("access-control-allow-origin").is_none());

    // No Origin at all: as today.
    let (status, h, _) = get_with(&app_with_cors(&[ORIGIN], None), "/api/v1/health", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(h.get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn star_allows_any_origin_by_echo() {
    let app = app_with_cors(&["*"], None);
    let (_, h, _) = get_with(&app, "/api/v1/health", &[("origin", "http://anywhere.example")]).await;
    assert_eq!(h["access-control-allow-origin"], "http://anywhere.example");
}

#[tokio::test]
async fn an_origin_copied_from_the_address_bar_matches() {
    // Pasted with a trailing slash and a space: the browser sends neither.
    let config = Config { cors_origins: vec![format!(" {ORIGIN}/ "), "  ".into()], ..Default::default() };
    let origins = config.allowed_origins();
    assert_eq!(origins, vec![ORIGIN.to_string()]);
    let app = app_with_cors(&origins.iter().map(String::as_str).collect::<Vec<_>>(), None);
    let (_, h, _) = get_with(&app, "/api/v1/health", &[("origin", ORIGIN)]).await;
    assert_eq!(h["access-control-allow-origin"], ORIGIN);
}

#[test]
fn the_setting_is_in_the_schema() {
    let s = trunk_recorder_plugin::schema::schema_for::<Config>();
    assert_eq!(s["properties"]["corsOrigins"]["type"], "array");
    assert_eq!(s["properties"]["corsOrigins"]["items"]["type"], "string");
}
