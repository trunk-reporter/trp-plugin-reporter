//! The live event bus behind the SSE stream: a replay ring plus filtered subscribers.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::mpsc::{self, error::TrySendError};

use crate::store::now_ms;

/// Events a subscriber may fall behind by before new ones are dropped for it.
const SUBSCRIBER_BUFFER: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    CallStart,
    CallUpdate,
    CallEnd,
    UnitEvent,
    RecorderUpdate,
    RateUpdate,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::CallStart => "call_start",
            EventKind::CallUpdate => "call_update",
            EventKind::CallEnd => "call_end",
            EventKind::UnitEvent => "unit_event",
            EventKind::RecorderUpdate => "recorder_update",
            EventKind::RateUpdate => "rate_update",
        }
    }
}

/// An event before the bus gives it an id.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEvent {
    pub kind: EventKind,
    pub sub_type: Option<String>,
    pub system_id: Option<i64>,
    pub site_id: Option<i64>,
    pub tgid: Option<u32>,
    pub unit_id: Option<u32>,
    pub emergency: bool,
    /// JSON payload.
    pub data: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// `"{unix_ms}-{seq}"`.
    pub id: String,
    pub kind: EventKind,
    pub sub_type: Option<String>,
    pub system_id: Option<i64>,
    pub site_id: Option<i64>,
    pub tgid: Option<u32>,
    pub unit_id: Option<u32>,
    pub emergency: bool,
    /// JSON payload.
    pub data: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filter {
    pub systems: Vec<i64>,
    pub sites: Vec<i64>,
    pub tgids: Vec<u32>,
    pub units: Vec<u32>,
    /// `"unit_event"` or `"unit_event:call"`.
    pub types: Vec<String>,
    pub emergency_only: bool,
}

fn ints<T: std::str::FromStr>(s: Option<&String>) -> Vec<T> {
    s.map(|s| s.split(',').filter_map(|p| p.trim().parse().ok()).collect()).unwrap_or_default()
}

fn passes<T: PartialEq>(list: &[T], value: Option<T>) -> bool {
    list.is_empty() || value.is_none_or(|v| list.contains(&v))
}

impl Filter {
    pub fn parse(query: &HashMap<String, String>) -> Filter {
        Filter {
            systems: ints(query.get("systems")),
            sites: ints(query.get("sites")),
            tgids: ints(query.get("tgids")),
            units: ints(query.get("units")),
            types: query.get("types").map(|s| s.split(',').map(str::trim).filter(|p| !p.is_empty()).map(String::from).collect()).unwrap_or_default(),
            emergency_only: query.get("emergency_only").is_some_and(|v| v == "true" || v == "1"),
        }
    }

    pub fn matches(&self, e: &Event) -> bool {
        if self.emergency_only && !e.emergency {
            return false;
        }
        if !self.types.is_empty() {
            let kind = e.kind.as_str();
            let hit = self.types.iter().any(|t| match t.split_once(':') {
                Some((k, sub)) => k == kind && e.sub_type.as_deref() == Some(sub),
                None => t == kind,
            });
            if !hit {
                return false;
            }
        }
        passes(&self.systems, e.system_id) && passes(&self.sites, e.site_id) && passes(&self.tgids, e.tgid) && passes(&self.units, e.unit_id)
    }
}

struct State {
    ring: VecDeque<Event>,
    subscribers: HashMap<u64, (Filter, mpsc::Sender<Event>)>,
}

struct Inner {
    capacity: usize,
    seq: AtomicU64,
    next_subscriber: AtomicU64,
    dropped: AtomicU64,
    state: Mutex<State>,
}

#[derive(Clone)]
pub struct Bus {
    inner: Arc<Inner>,
}

pub struct Subscription {
    pub rx: mpsc::Receiver<Event>,
    id: u64,
    bus: Bus,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus.lock().subscribers.remove(&self.id);
    }
}

impl Bus {
    pub fn new(ring: usize) -> Bus {
        Bus {
            inner: Arc::new(Inner {
                capacity: ring,
                seq: AtomicU64::new(0),
                next_subscriber: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                state: Mutex::new(State { ring: VecDeque::new(), subscribers: HashMap::new() }),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn publish(&self, new: NewEvent) -> Event {
        let seq = self.inner.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let event = Event {
            id: format!("{}-{seq}", now_ms()),
            kind: new.kind,
            sub_type: new.sub_type,
            system_id: new.system_id,
            site_id: new.site_id,
            tgid: new.tgid,
            unit_id: new.unit_id,
            emergency: new.emergency,
            data: new.data,
        };
        let mut state = self.lock();
        state.ring.push_back(event.clone());
        while state.ring.len() > self.inner.capacity {
            state.ring.pop_front();
        }
        let mut closed = Vec::new();
        for (id, (filter, tx)) in &state.subscribers {
            if !filter.matches(&event) {
                continue;
            }
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Closed(_)) => closed.push(*id),
            }
        }
        for id in closed {
            state.subscribers.remove(&id);
        }
        event
    }

    pub fn subscribe(&self, filter: Filter) -> Subscription {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
        let id = self.inner.next_subscriber.fetch_add(1, Ordering::Relaxed);
        self.lock().subscribers.insert(id, (filter, tx));
        Subscription { rx, id, bus: self.clone() }
    }

    /// Events after `last_id`; every buffered event when the id is no longer in
    /// the ring (better a duplicate than a gap).
    pub fn replay_since(&self, last_id: &str, filter: &Filter) -> Vec<Event> {
        let state = self.lock();
        let start = state.ring.iter().position(|e| e.id == last_id).map_or(0, |i| i + 1);
        state.ring.iter().skip(start).filter(|e| filter.matches(e)).cloned().collect()
    }

    /// Events dropped because a subscriber's channel was full.
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    pub fn subscriber_count(&self) -> usize {
        self.lock().subscribers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn ev(kind: EventKind, sub_type: Option<&str>, system_id: Option<i64>) -> Event {
        Event {
            id: String::new(),
            kind,
            sub_type: sub_type.map(String::from),
            system_id,
            site_id: None,
            tgid: None,
            unit_id: None,
            emergency: false,
            data: "{}".into(),
        }
    }

    fn new_ev(tgid: u32) -> NewEvent {
        NewEvent {
            kind: EventKind::CallStart,
            sub_type: None,
            system_id: Some(1),
            site_id: None,
            tgid: Some(tgid),
            unit_id: None,
            emergency: false,
            data: "{}".into(),
        }
    }

    fn seq(id: &str) -> u64 {
        id.rsplit('-').next().unwrap().parse().unwrap()
    }

    #[test]
    fn ids_are_unique_and_increasing() {
        let bus = Bus::new(8);
        let a = bus.publish(new_ev(1));
        let b = bus.publish(new_ev(1));
        let c = bus.publish(new_ev(1));
        assert!(a.id != b.id && b.id != c.id);
        assert!(seq(&a.id) < seq(&b.id) && seq(&b.id) < seq(&c.id));
    }

    #[test]
    fn the_ring_replays_after_an_id_and_everything_when_the_id_is_gone() {
        let bus = Bus::new(4);
        let ids: Vec<String> = (0..6).map(|_| bus.publish(new_ev(1)).id).collect();
        let all = Filter::default();
        let after: Vec<String> = bus.replay_since(&ids[3], &all).into_iter().map(|e| e.id).collect();
        assert_eq!(after, vec![ids[4].clone(), ids[5].clone()]);
        let gone: Vec<String> = bus.replay_since("0-0", &all).into_iter().map(|e| e.id).collect();
        assert_eq!(gone, ids[2..].to_vec());
    }

    #[test]
    fn filters_and_together_and_let_blank_dimensions_through() {
        let f = Filter::parse(&q(&[("systems", "1"), ("types", "unit_event:call,call_start")]));
        assert!(f.matches(&ev(EventKind::UnitEvent, Some("call"), Some(1))));
        assert!(!f.matches(&ev(EventKind::UnitEvent, Some("end"), Some(1))));
        assert!(f.matches(&ev(EventKind::CallStart, None, Some(1))));
        assert!(!f.matches(&ev(EventKind::CallStart, None, Some(2))));
        assert!(Filter::parse(&q(&[("systems", "1")])).matches(&ev(EventKind::RecorderUpdate, None, None)));
    }

    #[test]
    fn emergency_only_and_bad_numbers() {
        let f = Filter::parse(&q(&[("emergency_only", "true"), ("tgids", "x,5")]));
        assert_eq!(f.tgids, vec![5]);
        assert!(!f.matches(&ev(EventKind::CallStart, None, None)));
        let mut e = ev(EventKind::CallStart, None, None);
        e.emergency = true;
        assert!(f.matches(&e));
    }

    #[tokio::test]
    async fn subscribers_get_only_what_their_filter_passes() {
        let bus = Bus::new(8);
        let mut sub = bus.subscribe(Filter::parse(&q(&[("tgids", "101")])));
        bus.publish(new_ev(101));
        bus.publish(new_ev(202));
        assert_eq!(sub.rx.try_recv().unwrap().tgid, Some(101));
        assert!(sub.rx.try_recv().is_err());
        drop(sub);
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn a_full_subscriber_drops_events_and_counts_them() {
        let bus = Bus::new(8);
        let _sub = bus.subscribe(Filter::default());
        for _ in 0..66 {
            bus.publish(new_ev(1));
        }
        assert_eq!(bus.dropped(), 2);
    }
}
