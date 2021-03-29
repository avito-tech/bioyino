use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{self, Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use slog::{info, o, Logger};
use crossbeam_channel::Sender;

use bioyino_metric::{name::MetricName,  StatsdMetric, StatsdType};

use crate::errors::GeneralError;
use crate::fast_task::FastTask;
use crate::slow_task::SlowTask;
use crate::Float;

pub struct Stats {
    pub egress_carbon: AtomicUsize,
    pub egress_peer: AtomicUsize,
    pub ingress: AtomicUsize,
    pub ingress_metrics: AtomicUsize,
    pub ingress_metrics_peer: AtomicUsize,
    pub drops: AtomicUsize,
    pub parse_errors: AtomicUsize,
    pub agg_errors: AtomicUsize,
    pub peer_errors: AtomicUsize,
    pub queue_errors: AtomicUsize,
}

pub static STATS: Stats = Stats {
    egress_carbon: AtomicUsize::new(0),
    egress_peer: AtomicUsize::new(0),
    ingress: AtomicUsize::new(0),
    ingress_metrics: AtomicUsize::new(0),
    ingress_metrics_peer: AtomicUsize::new(0),
    drops: AtomicUsize::new(0),
    parse_errors: AtomicUsize::new(0),
    agg_errors: AtomicUsize::new(0),
    peer_errors: AtomicUsize::new(0),
    queue_errors: AtomicUsize::new(0),
};

pub static STATS_SNAP: Lazy<Mutex<OwnSnapshot>> = Lazy::new(|| Mutex::new(OwnSnapshot::default()));

#[macro_export]
macro_rules! s {
    ($path:ident) => {{
        crate::stats::STATS.$path.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }};
}

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

// A future to send own stats. Never gets ready.
pub struct OwnStats {
    interval: u64,
    prefix: String,
    slow_chan: Sender<SlowTask>,
    fast_chans: Vec<Sender<FastTask>>,
    log: Logger,
}

impl OwnStats {
    pub fn new(interval: u64, prefix: String, slow_chan: Sender<SlowTask>, fast_chans: Vec<Sender<FastTask>>, log: Logger) -> Self {
        let log = log.new(o!("source"=>"stats"));
        Self {
            interval,
            prefix,
            slow_chan,
            fast_chans,
            log,
        }
    }

    fn format_metric_carbon(&self, buf: &mut BytesMut, suffix: &[u8]) -> MetricName {
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(&b"."[..]);
        buf.extend_from_slice(suffix);
        let name = MetricName::new_untagged(buf.split());
        name
    }

    fn count(&self, next_chan: &Sender<FastTask>) {
        let mut buf = BytesMut::with_capacity((self.prefix.len() + 10) * 7); // 10 is suffix len, 7 is number of metrics
        let mut snapshot = OwnSnapshot::default();
        let s_interval = if self.interval > 0 { self.interval as f64 / 1000f64 } else { 1f64 };

        macro_rules! add_metric {
            ($value:ident, $suffix:expr) => {
                let $value = STATS.$value.swap(0, Ordering::Relaxed) as Float;
                if self.interval > 0 {
                    snapshot.data.push((Bytes::copy_from_slice(($suffix).as_bytes()), $value / s_interval));
                    let metric = StatsdMetric::new($value, StatsdType::Counter, None).unwrap();
                    let name = self.format_metric_carbon(&mut buf, $suffix.as_bytes());
                    next_chan
                        .send(FastTask::Accumulate(name, metric))
                        .map_err(|_| s!(queue_errors))
                        .unwrap_or(())
                }
            };
        }

        add_metric!(egress_carbon, "egress-carbon");
        add_metric!(egress_peer, "egress-peer");
        add_metric!(ingress, "ingress");
        add_metric!(ingress_metrics, "ingress-metric");
        add_metric!(ingress_metrics_peer, "ingress-metric-peer");
        add_metric!(drops, "drop");
        add_metric!(agg_errors, "agg-error");
        add_metric!(parse_errors, "parse-error");
        add_metric!(queue_errors, "queue-error");
        add_metric!(peer_errors, "peer-error");

        // queue len has other type, so macro does not fit here
        let chlen = self.slow_chan.len() as f64;
        let qlen = StatsdMetric::new(chlen, StatsdType::Gauge(None), None).unwrap();
        snapshot.data.push((Bytes::copy_from_slice(("slow-q-len").as_bytes()), chlen));
        let name = self.format_metric_carbon(&mut buf, "slow-w-len".as_bytes());
        next_chan
            .send(FastTask::Accumulate(name, qlen))
            .map_err(|_| s!(queue_errors))
            .unwrap_or(());

        // update global snapshot
        {
            let mut prev = STATS_SNAP.lock().unwrap();
            *prev = snapshot;
        }
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
        }
    }

    pub async fn run(self) {
        let now = tokio::time::Instant::now();
        let dur = Duration::from_millis(if self.interval < 100 { 1000 } else { self.interval }); // avoid too short intervals
        let mut interval = tokio::time::interval_at(now + dur, dur);
        let mut next_chan = self.fast_chans.len();
        let num_chans = self.fast_chans.len();
        loop {
            interval.tick().await;
            tokio::task::block_in_place(||{
                next_chan = if next_chan >= (num_chans - 1) { 0 } else { next_chan + 1 };
                let chan = &self.fast_chans[next_chan];
                self.count(chan);

            });
        }
    }
}
