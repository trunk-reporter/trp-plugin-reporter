//! tr-engine's phase-1 web pages, embedded in the binary, and the navigation
//! cards `/api/v1/pages` lists from their `card-*` meta tags.

use std::sync::LazyLock;

use axum::http::Uri;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use regex::Regex;
use rust_embed::RustEmbed;
use serde::Serialize;

use crate::api::ApiError;

/// The files under `web/`, compiled in (also in debug builds, so tests see what release serves).
#[derive(RustEmbed)]
#[folder = "web/"]
pub struct Assets;

/// A page's navigation card, from its `card-title` meta tags.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PageInfo {
    pub path: String,
    pub title: String,
    pub description: String,
    pub order: i64,
}

/// The pages `/api/v1/pages` lists, in order.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Pages(pub Vec<PageInfo>);

/// How much of a page is searched for its meta tags (as tr-engine does).
const HEAD_BYTES: usize = 2048;

static TITLE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"<meta\s+name="card-title"\s+content="([^"]*)""#).expect("regex"));
static DESCRIPTION: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"<meta\s+name="card-description"\s+content="([^"]*)""#).expect("regex"));
static ORDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"<meta\s+name="card-order"\s+content="(\d+)""#).expect("regex"));

impl Pages {
    pub fn empty() -> Pages {
        Pages(Vec::new())
    }

    /// The embedded `*.html` pages that have a `card-title`, by `card-order` (then path).
    pub fn scan() -> Pages {
        let mut pages: Vec<PageInfo> = Assets::iter()
            .filter(|name| name.ends_with(".html"))
            .filter_map(|name| {
                let file = Assets::get(&name)?;
                let end = file.data.len().min(HEAD_BYTES);
                // A cut in the middle of a character becomes U+FFFD at the very end; harmless.
                let head = String::from_utf8_lossy(&file.data[..end]);
                let title = TITLE.captures(&head)?[1].to_string();
                let description = DESCRIPTION.captures(&head).map(|c| c[1].to_string()).unwrap_or_default();
                let order = ORDER.captures(&head).and_then(|c| c[1].parse().ok()).unwrap_or(0);
                Some(PageInfo { path: format!("/{name}"), title, description, order })
            })
            .collect();
        pages.sort_by(|a, b| a.order.cmp(&b.order).then_with(|| a.path.cmp(&b.path)));
        Pages(pages)
    }
}

fn embedded(name: &str) -> Result<Response, ApiError> {
    let file = Assets::get(name).ok_or_else(|| ApiError::NotFound(format!("no page {name}")))?;
    Ok(([(CONTENT_TYPE, file.metadata.mimetype().to_string())], file.data.into_owned()).into_response())
}

/// `GET /`: the index page.
pub async fn index() -> Result<Response, ApiError> {
    embedded("index.html")
}

/// `GET /{file}`: an embedded page or asset.
pub async fn file(uri: Uri) -> Result<Response, ApiError> {
    embedded(uri.path().trim_start_matches('/'))
}
