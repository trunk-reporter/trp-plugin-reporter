//! Key/value settings kept in the `meta` table.

use rusqlite::{Connection, OptionalExtension, Row};

use super::{SiteId, SystemId};

pub fn meta_get(conn: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0)).optional()
}

pub fn meta_set(conn: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    conn.execute("INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value)).map(|_| ())
}

/// What a control channel says about its site. `None` leaves a column as it is.
#[derive(Default, Clone, Debug)]
pub struct Identity {
    pub nac: Option<u32>,
    pub wacn: Option<u32>,
    pub sys_id: Option<u32>,
    pub rfss: Option<u32>,
    pub site_number: Option<u32>,
    pub control_channel_hz: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemRow {
    pub id: SystemId,
    pub short_name: String,
    pub kind: String,
    pub name: String,
    pub sysid: Option<u32>,
    pub wacn: Option<u32>,
    pub nac: Option<u32>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SiteRow {
    pub id: SiteId,
    pub system_id: SystemId,
    pub name: String,
    pub nac: Option<u32>,
    pub rfss: Option<u32>,
    pub site_number: Option<u32>,
    pub control_channel_hz: Option<u64>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
}

const SYSTEM_COLS: &str = "id, short_name, kind, name, sysid, wacn, nac, first_seen_ms, last_seen_ms";
const SITE_COLS: &str = "id, system_id, name, nac, rfss, site_number, control_channel_hz, first_seen_ms, last_seen_ms";

fn system_row(r: &Row<'_>) -> rusqlite::Result<SystemRow> {
    Ok(SystemRow {
        id: r.get(0)?,
        short_name: r.get(1)?,
        kind: r.get(2)?,
        name: r.get(3)?,
        sysid: r.get(4)?,
        wacn: r.get(5)?,
        nac: r.get(6)?,
        first_seen_ms: r.get(7)?,
        last_seen_ms: r.get(8)?,
    })
}

fn site_row(r: &Row<'_>) -> rusqlite::Result<SiteRow> {
    Ok(SiteRow {
        id: r.get(0)?,
        system_id: r.get(1)?,
        name: r.get(2)?,
        nac: r.get(3)?,
        rfss: r.get(4)?,
        site_number: r.get(5)?,
        control_channel_hz: r.get::<_, Option<i64>>(6)?.map(|h| h as u64),
        first_seen_ms: r.get(7)?,
        last_seen_ms: r.get(8)?,
    })
}

/// Insert the system or touch it. `name` is replaced only by a non-empty one; `kind` is fixed at insert.
pub fn ensure_system(conn: &Connection, short_name: &str, kind: &str, name: &str, now_ms: i64) -> rusqlite::Result<SystemId> {
    conn.query_row(
        "INSERT INTO systems (short_name, kind, name, first_seen_ms, last_seen_ms) VALUES (?1, ?2, ?3, ?4, ?4) \
         ON CONFLICT(short_name) DO UPDATE SET last_seen_ms = excluded.last_seen_ms, \
         name = CASE WHEN excluded.name = '' THEN name ELSE excluded.name END RETURNING id",
        (short_name, kind, name, now_ms),
        |r| r.get(0),
    )
}

/// Insert the site or touch it. A conventional system's one site takes the system's `short_name`.
pub fn ensure_site(conn: &Connection, system: SystemId, name: &str, now_ms: i64) -> rusqlite::Result<SiteId> {
    conn.query_row(
        "INSERT INTO sites (system_id, name, first_seen_ms, last_seen_ms) VALUES (?1, ?2, ?3, ?3) \
         ON CONFLICT(system_id, name) DO UPDATE SET last_seen_ms = excluded.last_seen_ms RETURNING id",
        (system, name, now_ms),
        |r| r.get(0),
    )
}

/// Record what a control channel stated about a site, and copy sysid/wacn/nac up to its system.
pub fn set_identity(conn: &Connection, site: SiteId, id: &Identity, now_ms: i64) -> rusqlite::Result<()> {
    let system: SystemId = conn.query_row(
        "UPDATE sites SET nac = COALESCE(?2, nac), rfss = COALESCE(?3, rfss), site_number = COALESCE(?4, site_number), \
         control_channel_hz = COALESCE(?5, control_channel_hz), last_seen_ms = ?6 WHERE id = ?1 RETURNING system_id",
        (site, id.nac, id.rfss, id.site_number, id.control_channel_hz.map(|h| h as i64), now_ms),
        |r| r.get(0),
    )?;
    conn.execute(
        "UPDATE systems SET sysid = COALESCE(?2, sysid), wacn = COALESCE(?3, wacn), nac = COALESCE(?4, nac), last_seen_ms = ?5 WHERE id = ?1",
        (system, id.sys_id, id.wacn, id.nac, now_ms),
    )?;
    Ok(())
}

pub fn system_by_short_name(conn: &Connection, short_name: &str) -> rusqlite::Result<Option<SystemRow>> {
    conn.query_row(&format!("SELECT {SYSTEM_COLS} FROM systems WHERE short_name = ?1"), [short_name], system_row).optional()
}

pub fn system_by_id(conn: &Connection, id: SystemId) -> rusqlite::Result<Option<SystemRow>> {
    conn.prepare_cached(&format!("SELECT {SYSTEM_COLS} FROM systems WHERE id = ?1"))?.query_row([id], system_row).optional()
}

pub fn list_systems(conn: &Connection) -> rusqlite::Result<Vec<SystemRow>> {
    conn.prepare(&format!("SELECT {SYSTEM_COLS} FROM systems ORDER BY id"))?.query_map([], system_row)?.collect()
}

pub fn list_sites(conn: &Connection, system: SystemId) -> rusqlite::Result<Vec<SiteRow>> {
    conn.prepare(&format!("SELECT {SITE_COLS} FROM sites WHERE system_id = ?1 ORDER BY id"))?.query_map([system], site_row)?.collect()
}

pub fn site_by_id(conn: &Connection, id: SiteId) -> rusqlite::Result<Option<SiteRow>> {
    conn.prepare_cached(&format!("SELECT {SITE_COLS} FROM sites WHERE id = ?1"))?.query_row([id], site_row).optional()
}

/// A system's (talkgroup count, unit count, calls started in the 24 h before `now_ms`).
pub fn system_counts(conn: &Connection, system: SystemId, now_ms: i64) -> rusqlite::Result<(i64, i64, i64)> {
    let count = |sql: &str, params: &[&dyn rusqlite::ToSql]| conn.query_row(sql, params, |r| r.get::<_, i64>(0));
    Ok((
        count("SELECT COUNT(*) FROM talkgroups WHERE system_id = ?1", &[&system])?,
        count("SELECT COUNT(*) FROM units WHERE system_id = ?1", &[&system])?,
        count("SELECT COUNT(*) FROM calls WHERE system_id = ?1 AND start_ms > ?2", &[&system, &(now_ms - 86_400_000)])?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn a_system_is_one_row_however_often_it_is_seen() {
        let s = Store::in_memory();
        let (a, b) = s.write_sync(|c| Ok((ensure_system(c, "ohmarcs", "p25", "", 1_000)?, ensure_system(c, "ohmarcs", "p25", "Ohio MARCS", 2_000)?))).unwrap();
        assert_eq!(a, b);
        let row = s.read(|c| system_by_short_name(c, "ohmarcs")).unwrap().unwrap();
        assert_eq!((row.name.as_str(), row.first_seen_ms, row.last_seen_ms, row.sysid), ("Ohio MARCS", 1_000, 2_000, None));
    }

    #[test]
    fn identity_fills_in_when_the_control_channel_states_it() {
        let s = Store::in_memory();
        let (sys, site) = s
            .write_sync(|c| {
                let sys = ensure_system(c, "ohmarcs", "p25", "", 1_000)?;
                let site = ensure_site(c, sys, "ohmarcs-dayton", 1_000)?;
                set_identity(
                    c,
                    site,
                    &Identity {
                        nac: Some(0x3A0),
                        wacn: Some(0xBEE00),
                        sys_id: Some(0x3AB),
                        rfss: Some(1),
                        site_number: Some(5),
                        control_channel_hz: Some(851_000_000),
                    },
                    2_000,
                )?;
                Ok((sys, site))
            })
            .unwrap();
        let check = |s: &Store| {
            let system = s.read(|c| system_by_id(c, sys)).unwrap().unwrap();
            assert_eq!((system.sysid, system.wacn, system.nac), (Some(0x3AB), Some(0xBEE00), Some(0x3A0)));
            let row = s.read(|c| site_by_id(c, site)).unwrap().unwrap();
            assert_eq!((row.nac, row.rfss, row.site_number, row.control_channel_hz), (Some(0x3A0), Some(1), Some(5), Some(851_000_000)));
            assert_eq!(row.system_id, sys);
        };
        check(&s);
        s.write_sync(move |c| set_identity(c, site, &Identity::default(), 3_000)).unwrap();
        check(&s);
    }

    #[test]
    fn sites_are_unique_per_system_and_name() {
        let s = Store::in_memory();
        let (sys, a, b) = s
            .write_sync(|c| {
                let sys = ensure_system(c, "ohmarcs", "p25", "", 1_000)?;
                Ok((sys, ensure_site(c, sys, "dayton", 1_000)?, ensure_site(c, sys, "dayton", 2_000)?))
            })
            .unwrap();
        assert_eq!(a, b);
        let sites = s.read(|c| list_sites(c, sys)).unwrap();
        assert_eq!(sites.len(), 1);
        assert_eq!((sites[0].first_seen_ms, sites[0].last_seen_ms), (1_000, 2_000));
        assert_eq!(s.read(list_systems).unwrap().len(), 1);
    }
}
