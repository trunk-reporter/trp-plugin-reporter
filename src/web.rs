//! The web pages tr-engine ships, embedded. For now only what `/api/v1/pages`
//! lists; Task 17 adds the pages themselves.

use serde::Serialize;

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

impl Pages {
    pub fn empty() -> Pages {
        Pages(Vec::new())
    }
}
