mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::*;
use serde_json::{Value, json};

async fn put(app: &Router, path: &str, body: Vec<u8>, headers: &[(&str, &str)]) -> (StatusCode, Value) {
    let mut req = Request::builder().method(Method::PUT).uri(path).header("Content-Type", "application/json");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let (s, _, bytes) = send(app, req.body(Body::from(body)).expect("request")).await;
    (s, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_preference_round_trips() {
    let app = app().0;
    let doc = json!({"rules": [{"tgid": 9178, "colour": "#ff0000"}], "n": 1});
    let (s, v) = put(&app, "/api/v1/preferences/colour-rules", doc.to_string().into_bytes(), &[]).await;
    assert_eq!(s, 200, "{v}");
    assert_schema(&v, "Preference");
    assert_eq!((v["key"].as_str(), &v["value"]), (Some("colour-rules"), &doc));
    let (s, got) = get(&app, "/api/v1/preferences/colour-rules").await;
    assert_eq!(s, 200);
    assert_schema(&got, "Preference");
    assert_eq!(got, v);
    // A second PUT replaces the document.
    let (s, v2) = put(&app, "/api/v1/preferences/colour-rules", b"{\"n\":2}".to_vec(), &[]).await;
    assert_eq!((s, &v2["value"]), (StatusCode::OK, &json!({"n": 2})));
    assert_eq!(get(&app, "/api/v1/preferences/colour-rules").await.1["value"], json!({"n": 2}));
}

#[tokio::test]
async fn a_missing_preference_is_a_404() {
    let (s, v) = get(&app().0, "/api/v1/preferences/nothing-here").await;
    assert_eq!(s, 404);
    assert_eq!(v["code"], "not_found");
    assert_schema(&v, "Error");
}

#[tokio::test]
async fn a_bad_key_or_body_is_a_400() {
    let app = app().0;
    for key in ["Colours", "-a", "a_b", &"a".repeat(65)] {
        let (s, v) = get(&app, &format!("/api/v1/preferences/{key}")).await;
        assert_eq!((s.as_u16(), v["code"].as_str()), (400, Some("bad_request")), "GET {key}");
        let (s, v) = put(&app, &format!("/api/v1/preferences/{key}"), b"{}".to_vec(), &[]).await;
        assert_eq!((s.as_u16(), v["code"].as_str()), (400, Some("bad_request")), "PUT {key}");
    }
    // 64 characters is the longest key.
    assert_eq!(put(&app, &format!("/api/v1/preferences/{}", "a".repeat(64)), b"{}".to_vec(), &[]).await.0, 200);
    // A slash never reaches the handler.
    assert_eq!(get(&app, "/api/v1/preferences/a/b").await.0, 404);
    assert_eq!(put(&app, "/api/v1/preferences/a/b", b"{}".to_vec(), &[]).await.0, 404);
    for body in ["[1]", "\"s\"", "3", "null", "{", ""] {
        let (s, v) = put(&app, "/api/v1/preferences/k", body.as_bytes().to_vec(), &[]).await;
        assert_eq!((s.as_u16(), v["code"].as_str()), (400, Some("bad_request")), "body {body:?}");
        assert_schema(&v, "Error");
    }
    let big = format!("{{\"a\":\"{}\"}}", "x".repeat(256 * 1024));
    let (s, v) = put(&app, "/api/v1/preferences/k", big.into_bytes(), &[]).await;
    assert_eq!((s.as_u16(), v["code"].as_str()), (400, Some("bad_request")));
    // Past axum's own 2 MiB default too: still the spec's 400, not a plain-text 413.
    let huge = format!("{{\"a\":\"{}\"}}", "x".repeat(3 * 1024 * 1024));
    let (s, v) = put(&app, "/api/v1/preferences/k", huge.into_bytes(), &[]).await;
    assert_eq!((s.as_u16(), v["code"].as_str()), (400, Some("bad_request")));
    assert_schema(&v, "Error");
    // Exactly the cap is fine.
    let exact = format!("{{\"a\":\"{}\"}}", "x".repeat(256 * 1024 - 8));
    assert_eq!(exact.len(), 256 * 1024);
    assert_eq!(put(&app, "/api/v1/preferences/k", exact.into_bytes(), &[]).await.0, 200);
    assert_eq!(get(&app, "/api/v1/preferences/k").await.0, 200);
}

#[tokio::test]
async fn preferences_need_the_token_when_set() {
    let app = app_with(Some("tok")).0;
    let bearer = [("Authorization", "Bearer tok")];
    assert_eq!(get(&app, "/api/v1/preferences/k").await.0, 401);
    assert_eq!(put(&app, "/api/v1/preferences/k", b"{}".to_vec(), &[]).await.0, 401);
    assert_eq!(put(&app, "/api/v1/preferences/k", b"{\"a\":1}".to_vec(), &bearer).await.0, 200);
    let (s, _, body) = get_with(&app, "/api/v1/preferences/k", &bearer).await;
    assert_eq!(s, 200);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["value"], json!({"a": 1}));
}
