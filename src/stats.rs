use std::sync::{Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{self, SystemTime, Duration};

use futures3::sink::{SinkExt};
use futures3::channel::mpsc::Sender;
use once_cell::sync::Lazy;
use slog::{Logger, o, info};
use bytes::{BytesMut, Bytes};

use bioyino_metric::{name::MetricName, Metric, MetricType};

use crate::errors::GeneralError;
use crate::task::Task;
use crate::Float;

pub struct Stats {
    pub egress: AtomicUsize,
    pub ingress: AtomicUsize,
    pub ingress_metrics: AtomicUsize,
    pub drops: AtomicUsize, pub parse_errors: AtomicUsize,
    pub agg_errors: AtomicUsize,
    pub peer_errors: AtomicUsize,
    pub queue_errors: AtomicUsize,
}

pub static STATS: Stats = Stats {
    egress: AtomicUsize::new(0),
    ingress: AtomicUsize::new(0),
    ingress_metrics: AtomicUsize::new(0),
    drops: AtomicUsize::new(0), parse_errors: AtomicUsize::new(0),
    agg_errors: AtomicUsize::new(0),
    peer_errors: AtomicUsize::new(0),
    queue_errors: AtomicUsize::new(0),
};

pub static STATS_SNAP: Lazy<Mutex<OwnSnapshot>> = Lazy::new(|| Mutex::new(OwnSnapshot::default()));

#[macro_export]
macro_rules! s {
    ($path:ident) => {{
        crate::stats::STATS
            .$path
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }};
}

pub struct OwnSnapshot {
    pub ts: u128,
    pub data: Vec<(Bytes, Float)>,
}

impl Default for OwnSnapshot {
    fn default() -> Self {
        Self {
            ts: SystemTime::now().duration_since(time::UNIX_EPOCH).map_err(GeneralError::Time).expect("getting system time").as_millis(),
            data: Vec::new()
        }
    }
}

// A future to send own stats. Never gets ready.
pub struct OwnStats {
    interval: u64,
    prefix: String,
    chans: Vec<Sender<Task>>,
    next_chan: usize,
    log: Logger,
}

impl OwnStats {
    pub fn new(
        interval: u64,
        prefix: String,
        chans: Vec<Sender<Task>>,
        log: Logger
    ) -> Self {
        let log = log.new(o!("source"=>"stats"));
        Self {
            interval,
            prefix,
            chans,
            next_chan: 0,
            log,
        }
    }

    fn format_metric_carbon(&self, buf: &mut BytesMut, suffix: &[u8], value: Float) -> (MetricName, Metric<Float>) {
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(&b"."[..]);
        buf.extend_from_slice(suffix);
        let name = MetricName::new_untagged(buf.split());
        let metric = Metric::new(value, MetricType::Counter, None, None).unwrap();
        (name, metric)
    }

    async fn count(&mut self) {
        let mut buf = BytesMut::with_capacity((self.prefix.len() + 10) * 7); // 10 is suffix len, 7 is number of metrics
        let mut snapshot = OwnSnapshot::default();
        let s_interval = if self.interval > 0 {
            self.interval as f64 / 1000f64
        } else {
            1f64
        };

        // we will start from 1st worker, not 0th, but who cares, we rotate them every second
        self.next_chan += 1;
        // this also covers situation when the number of workers = 1
        if self.next_chan >= self.chans.len() {
            self.next_chan  = 0;
        }

        macro_rules! add_metric {
            ($value:ident, $suffix:expr) => {
                let $value = STATS.$value.swap(0, Ordering::Relaxed) as Float;
                if self.interval > 0 {
                    snapshot.data.push((Bytes::copy_from_slice(($suffix).as_bytes()), $value / s_interval));
                    let (name, metric) = self.format_metric_carbon(&mut buf, $suffix.as_bytes(), $value);
                    self
                        .chans[self.next_chan]
                        .clone()
                        .send(Task::AddMetric(name, metric)).await.map_err(|_| s!(queue_errors)).unwrap_or(());
                }
            };
        };

        add_metric!(egress, "egress");
        add_metric!(ingress, "ingress");
        add_metric!(ingress_metrics, "ingress-metric");
        add_metric!(drops, "drop");
        add_metric!(agg_errors, "agg-error");
        add_metric!(parse_errors, "parse-error");
        add_metric!(queue_errors, "queue-error");
        add_metric!(peer_errors, "peer-error");
        {
            let mut prev = STATS_SNAP.lock().unwrap();
            *prev = snapshot;
        }
        if self.interval > 0 {
            info!(self.log, "stats";
                "egress" => format!("{:2}", egress / s_interval),
                "ingress" => format!("{:2}", ingress / s_interval),
                "ingress-m" => format!("{:2}", ingress_metrics / s_interval),
                "drops" => format!("{:2}", drops / s_interval),
                "a-err" => format!("{:2}", agg_errors / s_interval),
                "p-err" => format!("{:2}", parse_errors / s_interval),
                "pe-err" => format!("{:2}", peer_errors / s_interval),
                "qu-err" => format!("{:2}", queue_errors / s_interval),
            );
        }
    }

    pub async fn run(mut self) {
        let now = tokio2::time::Instant::now();
        let dur = Duration::from_millis(if self.interval < 100 { 1000 } else { self.interval }); // avoid too short intervals
        let mut interval = tokio2::time::interval_at(now + dur, dur);
        loop {
            interval.tick().await;
            self.count().await;
        }
    }
}
