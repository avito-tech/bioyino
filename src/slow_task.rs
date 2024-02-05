use std::sync::Arc;
use std::sync::atomic::Ordering;

use crossbeam_channel::Sender;
use futures::channel::oneshot;
use log::warn as logw;
use slog::{error, info, Logger};

use bioyino_metric::{name::MetricName, Metric, MetricTypeName};

use crate::aggregate::{aggregate_task, AggregationData};
use crate::cache::{RotatedCache, SharedCache};
use crate::config::System;

use crate::stats::STATS;
use crate::{s, Cache, Float};

#[derive(Debug)]
pub enum SlowTask {
    AddMetric(MetricName, Metric<Float>),
    AddMetrics(Vec<(MetricName, Metric<Float>)>),
    Join(Arc<Cache>),
    AddSnapshot(Vec<(MetricName, Metric<Float>)>),
    Rotate(Option<oneshot::Sender<RotatedCache>>),
    Aggregate(AggregationData),
}

pub fn start_slow_threads(log: Logger, config: &System) -> Result<Sender<SlowTask>, std::io::Error> {
    info!(log, "starting counting threads");
    let threads = config.w_threads;
    let (tx, rx) = crossbeam_channel::bounded(config.task_queue_size);

    let cache = SharedCache::new();

    for i in 0..threads {
        let tlog = log.clone();
        let rx = rx.clone();
        let cache = cache.clone();
        let elog = log.clone();
        std::thread::Builder::new().name(format!("bioyino_cnt{}", i)).spawn(move || {
            let mut runner = SlowTaskRunner::new(tlog, cache);
            loop {
                match rx.recv() {
                    Ok(task) => {
                        runner.run(task);
                    }
                    Err(_) => {
                        error!(elog, "all sending threads have closed connections");
                        return;
                    }
                }
            }
        })?;
    }

    Ok(tx)
}

pub struct SlowTaskRunner {
    cache: SharedCache,
    log: Logger,
}

impl SlowTaskRunner {
    pub fn new(log: Logger, cache: SharedCache) -> Self {
        Self { cache, log }
    }

    fn update_metric(&self, name: MetricName, metric: Metric<Float>) {
        let ename = name.clone();
        let em = MetricTypeName::from_metric(&metric);
        self.cache.accumulate(name, metric).unwrap_or_else(|_| {
            logw!(
                "could not accumulate in long cache at {:?} new type '{}'",
                String::from_utf8_lossy(&ename.name[..]),
                em.to_string(),
            );
            s!(agg_errors);
        });
    }

    pub fn run(&mut self, task: SlowTask) {
        match task {
            SlowTask::AddMetric(name, metric) => self.update_metric(name, metric),
            SlowTask::AddMetrics(mut list) => {
                list.drain(..).map(|(name, metric)| self.update_metric(name, metric)).last();
            }
            SlowTask::Join(cache) => {
                cache.iter().map(
                    |(name, metric)| {
                        s!(slow_cache_joined_metrics);
                        self.update_metric(name.clone(), metric.clone());
                    }
                ).last();
            }
            SlowTask::AddSnapshot(mut list) => {
                list.drain(..).map(|(name, metric)| self.update_metric(name, metric)).last();
            }
            SlowTask::Rotate(channel) => {
                let rotated = self.cache.rotate(channel.is_some());
                STATS.slow_cache_rotated_metrics.fetch_add(rotated.len(), Ordering::Relaxed);
                if let Some(c) = channel {
                    let log = self.log.clone();
                    c.send(rotated).unwrap_or_else(|_| {
                        s!(queue_errors);
                        info!(log, "task could not send rotated metric, receiving thread may be dead");
                    });
                }
            }
            SlowTask::Aggregate(data) => aggregate_task(data),
        }
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
}
