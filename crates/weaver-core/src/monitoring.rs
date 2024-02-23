//! Useful data structures and functions for monitoring parts of weaver

use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter, Pointer};
use std::hash::Hash;
use std::ops::Index;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local};
use derive_more::From;
use indexmap::IndexMap;
use indexmap::map::Entry;
use parking_lot::{Mutex, RwLock};

/// A monitor
pub trait Monitor: Send + Sync {
    /// Gets the name of the monitor
    fn name(&self) -> &str;

    /// Gets the stats of a monitor
    fn stats(&mut self) -> Stats;
}

impl Debug for dyn Monitor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("dyn Monitor")
            .field("name", &self.name())
            .finish()
    }
}

/// Some type that can monitored
pub trait Monitorable {
    /// Creates a monitor
    fn monitor(&self) -> Box<dyn Monitor>;
}

impl<M: Monitor + Clone + 'static> Monitorable for M {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(self.clone())
    }
}

/// Monitor service is responsible for registering and storing monitors.
///
/// Can be cloned freely.
#[derive(Clone, Default)]
pub struct MonitorCollector {
    monitors: Arc<RwLock<Vec<Box<dyn Monitor>>>>,
}

impl Debug for MonitorCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitorService")
            .field("monitors", &self.monitors.read().len())
            .finish()
    }
}

impl MonitorCollector {
    /// Creates a new, empty monitor service
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a new monitor to the monitor service
    pub fn push(&mut self, monitor: impl Monitor + 'static) {
        self.monitors.write().push(Box::new(monitor))
    }

    /// This monitor service will not monitor this monitorable
    pub fn push_monitorable<M: Monitorable + ?Sized>(&mut self, monitor: &M) {
        self.push(DelegateMonitor::new(monitor.monitor()))
    }

    /// Collects all stats
    pub fn all(&self) -> Stats {
        let mut hashmap = IndexMap::<String, Stats>::new();
        let mut monitors = self.monitors.write();
        for monitor in monitors.iter_mut() {
            let stats = monitor.stats();
            let name = monitor.name();
            let mut entry = hashmap.entry(name.to_string());
            match entry {
                Entry::Occupied(mut occ) => {
                    occ.insert(occ.get().merge(&stats));
                }
                Entry::Vacant(v) => {
                    v.insert(stats);
                }
            }
        }
        Stats::Dict(hashmap)
    }

    /// Make this collection into a monitor
    pub fn into_monitor(self, name: impl AsRef<str>) -> impl Monitor {
        monitor_fn(name, move || self.all())
    }
}

impl<M: Monitor + 'static> FromIterator<M> for MonitorCollector {
    fn from_iter<T: IntoIterator<Item = M>>(iter: T) -> Self {
        Self {
            monitors: Arc::new(RwLock::new(
                iter.into_iter()
                    .map(|m| Box::new(m) as Box<dyn Monitor>)
                    .collect(),
            )),
        }
    }
}

impl FromIterator<Box<dyn Monitor>> for MonitorCollector {
    fn from_iter<T: IntoIterator<Item = Box<dyn Monitor>>>(iter: T) -> Self {
        Self {
            monitors: Arc::new(RwLock::new(iter.into_iter().collect())),
        }
    }
}

struct FnMonitor {
    name: String,
    monitor: Box<dyn FnMut() -> Stats + Send + Sync>,
}

impl Monitor for FnMonitor {
    fn name(&self) -> &str {
        &*self.name
    }

    fn stats(&mut self) -> Stats {
        (self.monitor)()
    }
}

/// Create a monitor from a function
pub fn monitor_fn<S: AsRef<str>, R: Into<Stats>, F: FnMut() -> R + Send + Sync + 'static>(
    name: S,
    mut callback: F,
) -> impl Monitor {
    FnMonitor {
        name: name.as_ref().to_string(),
        monitor: Box::new(move || callback().into()),
    }
}

/// A delegate monitor
struct DelegateMonitor {
    delegate: Box<dyn Monitor>,
}

impl DelegateMonitor {
    fn new(delegate: Box<dyn Monitor>) -> Self {
        Self { delegate }
    }
}

impl Monitor for DelegateMonitor {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn stats(&mut self) -> Stats {
        self.delegate.stats()
    }
}

#[derive(Clone)]
pub struct SharedMonitor {
    name: String,
    lock: Arc<Mutex<Box<dyn Monitor>>>
}

impl SharedMonitor {
    /// Create a new shared monitor
    pub fn new<M : Monitor + 'static>(monitor: M) -> Self {
        let name = monitor.name().to_string();
        Self { name, lock: Arc::new(Mutex::new(Box::new(monitor)))}
    }
}

impl From<Box<dyn Monitor>> for SharedMonitor {
    fn from(value: Box<dyn Monitor>) -> Self {
        let name = value.name().to_string();
        Self { name, lock: Arc::new(Mutex::new(value))}
    }
}

impl Monitor for SharedMonitor {
    fn name(&self) -> &str {
        &self.name
    }

    fn stats(&mut self) -> Stats {
        self.lock.lock().stats()
    }
}

/// Stats data provided by a monitor.
///
/// Similar to JSON structure but with more literal types like
/// - Timestamps (via the Instant)
/// - Duration
/// - Throughput (Value / second)
#[derive(From, PartialEq, Clone, Default)]
pub enum Stats {
    Array(Vec<Stats>),
    Dict(IndexMap<String, Stats>),
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),

    Timestamp(DateTime<Local>),
    Duration(Duration),
    #[from(ignore)]
    Throughput(f64),

    #[from(ignore)]
    SubStats(Vec<Stats>),
    #[default]
    Null,
}

impl Debug for Stats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Stats::Array(a) | Stats::SubStats(a) => a.fmt(f),
            Stats::Dict(d) => d.fmt(f),
            Stats::String(val) => {
                write!(f, "{val:?}")
            }
            Stats::Float(val) => {
                write!(f, "{val:?}")
            }
            Stats::Integer(val) => {
                write!(f, "{val:?}")
            }
            Stats::Boolean(val) => {
                write!(f, "{val:?}")
            }
            Stats::Timestamp(instant) => {
                write!(f, "{:?}", instant)
            }
            Stats::Duration(duration) => {
                write!(f, "{:?}", duration)
            }
            Stats::Throughput(v) => {
                write!(f, "{v:.3?}/sec")
            }

            Stats::Null => {
                write!(f, "null")
            }
        }
    }
}

impl Stats {
    pub fn as_array(&self) -> Option<&[Stats]> {
        if let Stats::Array(stats) = &self {
            Some(stats.as_slice())
        } else {
            None
        }
    }

    pub fn as_sub_stats(&self) -> Option<&[Stats]> {
        if let Stats::SubStats(stats) = &self {
            Some(stats.as_slice())
        } else {
            None
        }
    }

    pub fn as_dict(&self) -> Option<&IndexMap<String, Stats>> {
        if let Stats::Dict(stats) = &self {
            Some(stats)
        } else {
            None
        }
    }

    /// For [Stats::SubStats] where all stats are [Stats::Integer], [Stats::Float], or [Stats::Throughput], a mean value is
    /// calculated.
    ///
    /// This is performed recursively when applicable.
    pub fn mean(&self) -> Stats {
        match self {
            Stats::Dict(dict) => {
                Stats::Dict(dict.iter().map(|(k, v)| (k.clone(), v.mean())).collect())
            }
            Stats::SubStats(sub_stats) => {
                if sub_stats.iter().all(|s| matches!(s, Stats::Integer(_))) {
                    let (sum, count) = sub_stats.iter().fold((0, 0), |(sum, count), next| {
                        let &Stats::Integer(i) = next else {
                            unreachable!()
                        };
                        (sum + i, count + 1)
                    });
                    Stats::Integer((sum as f64 / count as f64).round() as i64)
                } else if sub_stats.iter().all(|s| matches!(s, Stats::Float(_))) {
                    let (sum, count) = sub_stats.iter().fold((0.0, 0), |(sum, count), next| {
                        let &Stats::Float(i) = next else {
                            unreachable!()
                        };
                        (sum + i, count + 1)
                    });
                    Stats::Float(sum / count as f64)
                } else if sub_stats.iter().all(|s| matches!(s, Stats::Throughput(_))) {
                    let (sum, count) = sub_stats.iter().fold((0.0, 0), |(sum, count), next| {
                        let &Stats::Throughput(i) = next else {
                            unreachable!()
                        };
                        (sum + i, count + 1)
                    });
                    Stats::Throughput(sum / count as f64)
                } else {
                    Stats::SubStats(sub_stats.iter().map(|s| s.mean()).collect())
                }
            }
            _ => self.clone(),
        }
    }

    /// Merges stat values.
    ///
    /// Dict values are merged per key. Arrays are concatted, and sub-stats are merged
    pub fn merge(&self, other: &Self) -> Self {
        let mut this = self.merge_self();
        let mut other = other.merge_self();
        match (this, other) {
            (Self::Dict(l), Self::Dict(r)) => Self::Dict(l.iter().chain(r.iter()).fold(
                IndexMap::with_capacity(l.len() + r.len()),
                |mut map, (k, v)| {
                    let mut v = v.merge_self();
                    match map.entry(k.to_string()) {
                        Entry::Occupied(mut occ) => {
                            *occ.get_mut() = occ.get().merge(&v);
                        }
                        Entry::Vacant(vacant) => {
                            vacant.insert(v);
                        }
                    }
                    map
                },
            )),
            (Self::Array(l), Self::Array(r)) => {
                // if all l and r are dict stats, merge
                Self::Array(l.iter().chain(r.iter()).map(Self::merge_self).collect())
            }
            (Self::SubStats(l), Self::SubStats(r)) => l
                .iter()
                .chain(r.iter())
                .map(Self::merge_self)
                .reduce(|l, r| l.merge(&r))
                .unwrap_or_else(|| Stats::Null),
            (Self::SubStats(l), r) => Self::SubStats([l.clone(), vec![r]].concat()),
            (l, Self::SubStats(r)) => Self::SubStats([vec![l], r.clone()].concat()),
            (l, r) => Self::SubStats(vec![l.clone(), r.clone()]),
        }
    }
    fn merge_self(&self) -> Self {
        let mut output = self.clone();
        if let Self::SubStats(stats) = &mut output {
            output = stats
                .iter()
                .cloned()
                .reduce(|l, r| Stats::merge(&l, &r))
                .unwrap_or_else(|| Self::Null);
        }
        output
    }
}

impl<T: Into<Stats>> FromIterator<(String, T)> for Stats {
    fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
        Self::Dict(IndexMap::from_iter(
            iter.into_iter().map(|(k, v)| (k, v.into())),
        ))
    }
}

impl<T: Into<Stats>> FromIterator<(&'static str, T)> for Stats {
    fn from_iter<I: IntoIterator<Item = (&'static str, T)>>(iter: I) -> Self {
        Self::Dict(IndexMap::from_iter(
            iter.into_iter().map(|(k, v)| (k.to_string(), v.into())),
        ))
    }
}

impl From<&MonitorCollector> for Stats {
    fn from(value: &MonitorCollector) -> Self {
        value.all()
    }
}

impl Index<usize> for Stats {
    type Output = Stats;

    fn index(&self, index: usize) -> &Self::Output {
        &self.as_array().expect("is not an array")[index]
    }
}

impl<Q> Index<&Q> for Stats
where
    String: Borrow<Q>,
    Q: Eq + Hash + ?Sized,
{
    type Output = Stats;

    fn index(&self, index: &Q) -> &Self::Output {
        &self.as_dict().expect("is not a dict")[index]
    }
}

impl From<&str> for Stats {
    fn from(value: &str) -> Self {
        Stats::String(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::monitoring::{monitor_fn, MonitorCollector, Stats};

    #[test]
    fn test_monitor_service() {
        let mut monitors_service = MonitorCollector::new();
        monitors_service.push(monitor_fn("test", || Stats::Throughput(15.)));
        let stats = monitors_service.all();
        assert_eq!(stats["test"], Stats::Throughput(15.));
        monitors_service.push(monitor_fn("test", || Stats::Throughput(30.)));
        let stats = monitors_service.all();
        assert_eq!(
            stats["test"].as_sub_stats().unwrap(),
            [Stats::Throughput(15.0), Stats::Throughput(30.0)]
        );
    }

    #[test]
    fn test_merge_stats() {
        let stats1 = Stats::from_iter([("s1", 13.0), ("s2", 13.0)]);
        let stats2 = Stats::from_iter([("s1", 15.0), ("s2", 19.0)]);
        let merged = stats1.merge(&stats2);
        let Stats::Dict(dict) = merged else {
            panic!("must be a dict")
        };
        let s1 = &dict["s1"];
        assert_eq!(
            s1,
            &Stats::SubStats(vec![Stats::Float(13.0), Stats::Float(15.0)])
        );

        let s2 = &dict["s2"];
        assert_eq!(
            s2,
            &Stats::SubStats(vec![Stats::Float(13.0), Stats::Float(19.0)])
        );
    }
}
