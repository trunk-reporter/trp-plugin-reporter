//! Preferences: a JSON document per key, for the web app's server-side settings.

use rusqlite::{Connection, OptionalExtension, params};

/// The document stored under `key` and when it was last written (Unix ms).
pub fn get_preference(conn: &Connection, key: &str) -> rusqlite::Result<Option<(String, i64)>> {
    conn.query_row("SELECT json, updated_ms FROM preferences WHERE key = ?1", params![key], |r| Ok((r.get(0)?, r.get(1)?))).optional()
}

/// Store `json` under `key`, replacing what was there.
pub fn put_preference(conn: &Connection, key: &str, json: &str, now_ms: i64) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO preferences (key, json, updated_ms) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET json = excluded.json, updated_ms = excluded.updated_ms",
        params![key, json, now_ms],
    )?;
    Ok(())
}
