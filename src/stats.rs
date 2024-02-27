use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::{self, Duration, SystemTime};

use bioyino_metric::aggregate::{Aggregate, AggregateCalculator};
use bioyino_metric::{Metric, MetricTypeName};
use dipstick::{Graphite, Input, InputScope, Labels};
use log::warn as logw;
use serde::Serialize;
use bytes::{Bytes, BytesMut, BufMut};
use once_cell::sync::Lazy;
use slog::{info, o, Logger};
use crossbeam_channel::Sender;

use slog::error;
use tokio::sync::RwLock;

use bioyino_metric::{name::MetricName,  StatsdMetric, StatsdType, FromF64};

use crate::config;
use crate::errors::GeneralError;
use crate::fast_task::FastTask;
use crate::slow_task::SlowTask;
use crate::Float;

pub struct Stats {
    pub egress_carbon: AtomicUsize,

    // metric count sent to carbon 
    pub egress_carbon_metrics: AtomicUsize,

    pub egress_peer: AtomicUsize,
    pub ingress: AtomicUsize,
    pub ingress_metrics: AtomicUsize,
    pub ingress_metrics_peer: AtomicUsize,
    pub drops: AtomicUsize,

    // metric count offloaded from slow cache
    pub slow_cache_rotated_metrics: AtomicUsize,

    // metric count loaded to slow cache from fast cache
    pub slow_cache_joined_metrics: AtomicUsize,

    // metric count aggregated before send
    pub aggregated_metrics: AtomicUsize,
    
    pub parse_errors: AtomicUsize,
    pub agg_errors: AtomicUsize,
    pub peer_errors: AtomicUsize,
    pub queue_errors: AtomicUsize,
}

pub static STATS: Stats = Stats {
    egress_carbon: AtomicUsize::new(0),
    egress_carbon_metrics: AtomicUsize::new(0),
    egress_peer: AtomicUsize::new(0),
    ingress: AtomicUsize::new(0),
    ingress_metrics: AtomicUsize::new(0),
    ingress_metrics_peer: AtomicUsize::new(0),
    drops: AtomicUsize::new(0),
    slow_cache_rotated_metrics: AtomicUsize::new(0),
    slow_cache_joined_metrics: AtomicUsize::new(0),
    aggregated_metrics: AtomicUsize::new(0),
    parse_errors: AtomicUsize::new(0),
    agg_errors: AtomicUsize::new(0),
    peer_errors: AtomicUsize::new(0),
    queue_errors: AtomicUsize::new(0),
};

pub static STATS_SNAP: Lazy<RwLock<OwnSnapshot>> = Lazy::new(|| RwLock::new(OwnSnapshot::default()));

#[macro_export]
macro_rules! s {
    ($path:ident) => {{
        crate::stats::STATS.$path.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }};
}

#[derive(Clone)]
pub struct OwnSnapshot {
    pub ts: u128,
    pub data: Vec<(Bytes, Float)>,
}

impl Default for OwnSnapshot {
    fn default() -> Self {
        Self {
            ts: SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .map_err(GeneralError::Time)
                .expect("getting system time")
                .as_millis(),
                data: Vec::new(),
        }
    }
}


impl OwnSnapshot {
    pub(crate) fn render(&self, json: bool) -> Bytes {
        let mut buf = BytesMut::new();

        if !json {
            let ts = self.ts.to_string();
            for (name, value) in &self.data {
                buf.extend_from_slice(&name[..]);
                buf.extend_from_slice(&b" "[..]);
                // write somehow doesn't extend buffer size giving "cannot fill buffer" error
                buf.reserve(64);
                let mut writer = buf.writer();
                dtoa::write(&mut writer, *value).map(|_|()).unwrap_or(()); // TODO: think if we should not ignore float error
                buf = writer.into_inner();
                buf.extend_from_slice(&b" "[..]);
                buf.extend_from_slice(ts.as_bytes());
                buf.extend_from_slice(&b"\n"[..]);
            }

        } else {

            #[derive(Serialize)]
            struct JsonSnap {
                ts: u128,
                metrics: HashMap<String, Float>,
            }

            let snap = JsonSnap {
                ts: self.ts,
                metrics: HashMap::from_iter(
                    self
                    .data
                    .iter()
                    .map(|(name, value)| (String::from_utf8_lossy(&name[..]).to_string(), *value)),
                ),
            };
            let mut writer = buf.writer();
            serde_json::to_writer_pretty(&mut writer, &snap).unwrap_or(());
            buf = writer.into_inner();
        }
        buf.freeze()
    }
}

// A future to send own stats. Never gets ready.
pub struct OwnStats {
    interval: u64,
    prefix: String,
    next_chan: usize,
    slow_chan: Sender<SlowTask>,
    fast_chans: Vec<Sender<FastTask>>,
    log: Logger,
    carbon_config: config::Carbon,
    cache: HashMap<MetricName, Metric<Float>>,
}

impl OwnStats {
    pub fn new(
        interval: u64, 
        prefix: String, 
        slow_chan: Sender<SlowTask>, 
        fast_chans: Vec<Sender<FastTask>>, 
        log: Logger,
        carbon_config: config::Carbon,
    ) -> Self {
        let log = log.new(o!("source"=>"stats"));
        Self {
            interval,
            prefix,
            next_chan: fast_chans.len(),
            slow_chan,
            fast_chans,
            log,
            carbon_config,
            cache: HashMap::new(),
        }
    }

    fn format_metric_carbon(&self, buf: &mut BytesMut, suffix: &[u8], is_old: bool) -> MetricName {
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(&b"."[..]);
        buf.extend_from_slice(suffix);
        
        if is_old {
            buf.extend_from_slice(&b"."[..]);
            buf.extend_from_slice(&b"old"[..]);
        }

        let name = MetricName::new_untagged(buf.split());
        name
    }

    fn next_chan(&mut self) -> &Sender<FastTask> {
        self.next_chan = if self.next_chan >= (self.fast_chans.len() - 1) { 0 } else { self.next_chan + 1 };
        &self.fast_chans[self.next_chan]
    }

    fn count(&mut self) -> Result<OwnSnapshot, String> {
        let mut buf = BytesMut::with_capacity((self.prefix.len() + 10) * 7); // 10 is suffix len, 7 is number of metrics
        let mut snapshot = OwnSnapshot::default();
        let s_interval = if self.interval > 0 { self.interval as f64 / 1000f64 } else { 1f64 };
        let s_interval = Float::from_f64(s_interval);

        let metrics = Graphite::send_to(self.carbon_config.address.clone())
            .map_err(
                |e| format!("failed to connect to graphite at {}: {}", 
                self.carbon_config.address.clone(), e
            ))?
            .metrics();

        macro_rules! add_metric {
            ($value:ident, $suffix:expr) => {
                let $value = STATS.$value.swap(0, Ordering::Relaxed) as Float;
                if self.interval > 0 {
                    snapshot.data.push((Bytes::copy_from_slice(($suffix).as_bytes()), $value / s_interval));

                    let metric = StatsdMetric::new($value, StatsdType::Counter, None).unwrap();

                    let name = self.format_metric_carbon(&mut buf, $suffix.as_bytes(), false);
                    update_metric(&mut self.cache, name.clone(), metric.clone());
                    let aggs = vec![Aggregate::<f64>::Count];
                    for (name, metric) in self.cache.iter() {
                        let name = name.clone();
                        let mut metric = metric.clone();

                        let mut calculator = AggregateCalculator::new(&mut metric, &aggs);
                        let v = calculator.nth(0).unwrap().unwrap().1;
                        let counter = metrics.counter(String::from_utf8(name.name.to_ascii_lowercase()).unwrap().as_str());
                        counter.write(v as isize, Labels::default());
                    }

                    let name = self.format_metric_carbon(&mut buf, $suffix.as_bytes(), true);
                    let chan = self.next_chan();
                    chan
                        .send(FastTask::Accumulate(name, metric))
                        .map_err(|_| s!(queue_errors))
                        .unwrap_or(());
                }
            };
        }

        add_metric!(egress_carbon, "egress-carbon");
        add_metric!(egress_carbon_metrics, "egress-carbon-metrics");
        add_metric!(egress_peer, "egress-peer");
        add_metric!(ingress, "ingress");
        add_metric!(ingress_metrics, "ingress-metric");
        add_metric!(ingress_metrics_peer, "ingress-metric-peer");
        add_metric!(drops, "drop");
        add_metric!(slow_cache_rotated_metrics, "slow_cache_rotated_metrics");
        add_metric!(slow_cache_joined_metrics, "slow_cache_joined_metrics");
        add_metric!(aggregated_metrics, "aggregated_metrics");
        add_metric!(agg_errors, "agg-error");
        add_metric!(parse_errors, "parse-error");
        add_metric!(queue_errors, "queue-error");
        add_metric!(peer_errors, "peer-error");

        // queue len has other type, so macro does not fit here
        let chlen = Float::from_f64(self.slow_chan.len() as f64);
        let qlen = StatsdMetric::new(chlen, StatsdType::Gauge(None), None).unwrap();
        snapshot.data.push((Bytes::copy_from_slice(("slow-q-len").as_bytes()), chlen));
        let name = self.format_metric_carbon(&mut buf, "slow-q-len".as_bytes(), false);

        let chan = self.next_chan();
        chan
            .send(FastTask::Accumulate(name, qlen))
            .map_err(|_| s!(queue_errors))
            .unwrap_or(());

        if self.interval > 0 {
            info!(self.log, "stats";
                "egress-c" => format!("{:2}", egress_carbon / s_interval),
                "egress-p" => format!("{:2}", egress_peer / s_interval),
                "ingress" => format!("{:2}", ingress / s_interval),
                "ingress-m" => format!("{:2}", ingress_metrics / s_interval),
                "ingress-m-p" => format!("{:2}", ingress_metrics_peer / s_interval),
                "drops" => format!("{:2}", drops / s_interval),
                "a-err" => format!("{:2}", agg_errors / s_interval),
                "p-err" => format!("{:2}", parse_errors / s_interval),
                "pe-err" => format!("{:2}", peer_errors / s_interval),
                "qu-err" => format!("{:2}", queue_errors / s_interval),
                "qu-len" => format!("{:2}", chlen),
            );
        };
        
        Ok(snapshot)
    }

    pub async fn run(mut self) {
        let now = tokio::time::Instant::now();
        let dur = Duration::from_millis(if self.interval < 100 { 1000 } else { self.interval }); // avoid too short intervals
        let mut interval = tokio::time::interval_at(now + dur, dur);
        loop {
            interval.tick().await;
            match tokio::task::block_in_place(||{
                self.count()
            }) {
                Ok(snapshot) => {
                    let mut prev = STATS_SNAP.write().await;
                    *prev = snapshot;
                },
                Err(e) => error!(self.log, "failed to send metrics: {}", e)
            };
        }
    }
}

fn update_metric(cache: &mut HashMap<MetricName, Metric<Float>>, name: MetricName, metric: StatsdMetric<Float>) {
    let ename = name.clone();
    let em = MetricTypeName::from_statsd_metric(&metric);
    if em == MetricTypeName::CustomHistogram {
        s!(agg_errors);
        logw!("skipped histogram, histograms are not supported");
        return;
    }

    match cache.entry(name) {
        Entry::Occupied(ref mut entry) => {
            entry.get_mut().accumulate_statsd(metric).unwrap_or_else(|_| {
                let mtype = MetricTypeName::from_metric(&entry.get());
                logw!(
                    "could not accumulate {:?}: type '{}' into type '{}'",
                    String::from_utf8_lossy(&ename.name[..]),
                    em.to_string(),
                    mtype.to_string(),
                );
                s!(agg_errors);
            });
        }

        Entry::Vacant(entry) => match Metric::from_statsd(&metric, 1, None) {
            Ok(metric) => {
                entry.insert(metric);
            }
            Err(e) => {
                s!(agg_errors);
                logw!(
                    "could not create new metric from statsd metric at {:?}: {}",
                    String::from_utf8_lossy(&ename.name[..]),
                    e
                )
            }
        },
    }
}
