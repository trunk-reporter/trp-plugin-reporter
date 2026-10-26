//! CORS for the origins an operator lists (`corsOrigins`). It runs outside the
//! token guard, so a browser's preflight (which cannot carry a token) is answered.

use axum::extract::{Request, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
    HeaderValue, ORIGIN, VARY,
};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use super::AppState;

fn add_headers(headers: &mut HeaderMap, origin: HeaderValue) {
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.append(VARY, HeaderValue::from_static("Origin"));
    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Authorization, Content-Type"));
    headers.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, PUT, POST, OPTIONS"));
    headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("Content-Length, Content-Range"));
    headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
}

/// Add CORS headers when the request's `Origin` is listed (`*` lists every origin,
/// echoed, never sent as `*`); answer a listed origin's `OPTIONS` with 204.
pub async fn cors(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let origin =
        req.headers().get(ORIGIN).filter(|o| o.to_str().is_ok_and(|o| state.cors_origins.iter().any(|allowed| allowed == "*" || allowed == o))).cloned();
    let Some(origin) = origin else { return next.run(req).await };
    let mut res = if req.method() == Method::OPTIONS {
        let mut res = Response::new(axum::body::Body::empty());
        *res.status_mut() = StatusCode::NO_CONTENT;
        res
    } else {
        next.run(req).await
    };
    add_headers(res.headers_mut(), origin);
    res
}
