use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use crossbeam_channel::Sender;
use futures::channel::oneshot;
use log::warn as logw;
use slog::{error, info, warn, Logger};

use bioyino_metric::parser::{MetricParser, MetricParsingError, ParseErrorHandler};
use bioyino_metric::{name::MetricName, Metric, MetricTypeName, StatsdMetric};

use crate::config::System;

use crate::{s, Cache, Float};

// Fast task should not block for a long time because it is responsible for
// parsing metrics coming from UDP ans storing them parsed in a short-living local cache
// which should be given away to be used in snapshots or global long-living cache
#[derive(Debug)]
pub enum FastTask {
    Parse(u64, BytesMut),
    TakeSnapshot(oneshot::Sender<Cache>),
}

pub fn start_fast_threads(log: Logger, config: Arc<System>) -> Result<Vec<Sender<FastTask>>, std::io::Error> {
    info!(log, "starting parsing threads");
    let threads = config.p_threads;
    let mut chans = Vec::with_capacity(threads);
    for i in 0..threads {
        let (tx, rx) = crossbeam_channel::bounded(config.task_queue_size);

        chans.push(tx);
        let tlog = log.clone();
        let elog = log.clone();
        let cf = config.clone();
        std::thread::Builder::new().name(format!("bioyino_fast{}", i)).spawn(move || {
            let mut runner = FastTaskRunner::new(tlog, cf, 8192);
            loop {
                match rx.recv() {
                    Ok(task) => {
                        runner.run(task);
                    }
                    Err(_) => {
                        error!(elog, "UDP thread closed connection");
                        return;
                    }
                }
            }
        })?;
    }

    Ok(chans)
}

fn update_metric(cache: &mut Cache, name: MetricName, metric: StatsdMetric<Float>) {
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

#[derive(Debug)]
pub struct FastTaskRunner {
    short: HashMap<MetricName, Metric<Float>>,
    buffers: HashMap<u64, (usize, BytesMut)>,
    names_arena: BytesMut,
    config: Arc<System>,
    log: Logger,
}

impl FastTaskRunner {
    pub fn new(log: Logger, config: Arc<System>, cap: usize) -> Self {
        Self {
            short: HashMap::with_capacity(cap),
            buffers: HashMap::with_capacity(cap),
            names_arena: BytesMut::new(),
            config,
            log,
        }
    }

    pub fn run(&mut self, task: FastTask) {
        match task {
            FastTask::Parse(addr, buf) => {
                let log = if self.config.metrics.log_parse_errors { Some(self.log.clone()) } else { None };
                let buf = {
                    let len = buf.len();
                    let (_, ref mut prev_buf) = self
                        .buffers
                        .entry(addr)
                        .and_modify(|(times, _)| {
                            *times = 0;
                        })
                        .or_insert((0, BytesMut::with_capacity(len)));
                    prev_buf.reserve(buf.len());
                    prev_buf.put(buf);
                    prev_buf
                };

                let parser = MetricParser::new(
                    buf,
                    self.config.metrics.max_unparsed_buffer,
                    self.config.metrics.max_tags_len,
                    TaskParseErrorHandler(log),
                );

                for (mut name, metric) in parser {
                    s!(ingress_metrics);
                    if name.has_tags() && self.config.metrics.create_untagged_copy {
                        self.names_arena.extend_from_slice(name.name_without_tags());
                        let untagged = MetricName::new_untagged(self.names_arena.split());
                        update_metric(&mut self.short, untagged, metric.clone());
                    }
                    update_metric(&mut self.short, name, metric);
                }
            }
            FastTask::TakeSnapshot(channel) => {
                // we need our cache to be sent for processing
                // in place of it we need a new cache with most probably the same size
                // BUT if we use exactly the same size, it may grow infinitely in long term
                // so we halve the size so it could be reduced if ingress flow amounts
                // become lower

                let mut rotated = HashMap::with_capacity(self.short.len() / 2);
                std::mem::swap(&mut self.short, &mut rotated);
                channel.send(rotated).unwrap_or_else(|_| {
                    s!(queue_errors);
                    info!(self.log, "task could not send snapshot, receiving thread may be dead");
                });
                let interval = self.config.carbon.interval;
                self.buffers.retain(|_, (ref mut times, _)| {
                    *times += 1;
                    *times < interval as usize * 5
                });
            }
        }
    }
}

struct TaskParseErrorHandler(Option<Logger>);

impl ParseErrorHandler for TaskParseErrorHandler {
    fn handle(&self, input: &[u8], _pos: usize, e: MetricParsingError) {
        s!(parse_errors);
        if let Some(ref log) = self.0 {
            if let Ok(string) = std::str::from_utf8(input) {
                // TODO better error formatting instead of Debug
                warn!(log, "parsing error"; "buffer"=> format!("{:?}", string), "position"=>format!("{}", e.position.translate_position(input)), "error"=>format!("{:?}", e));
            } else {
                warn!(log, "parsing error (bad unicode)"; "buffer"=> format!("{:?}", input), "position"=>format!("{}",e.position.translate_position(input) ), "error"=>format!("{:?}", e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bioyino_metric::{name::TagFormat, MetricValue};

    use crate::util::{new_test_graphite_name as new_name, prepare_log};

    #[test]
    fn accumulate_tagged_metrics() {
        let mut data = BytesMut::new();

        // ensure metrics with same tags (going probably in different orders) go to same aggregate
        data.extend_from_slice(b"gorets;t2=v2;t1=v1:1000|c");
        data.extend_from_slice(b"\ngorets;t1=v1;t2=v2:1000|c");

        // ensure metrics with same name but different tags go to different aggregates
        data.extend_from_slice(b"\ngorets;t1=v1;t2=v3:1000|c");

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = FastTaskRunner::new(prepare_log("aggregate_tagged"), Arc::new(config), 16);
        runner.run(FastTask::Parse(2, data));

        let mut intermediate = Vec::new();
        intermediate.resize(9000, 0u8);
        let mode = TagFormat::Graphite;

        // must be aggregated into two as sum
        let key = MetricName::new("gorets;t1=v1;t2=v2".into(), mode, &mut intermediate).unwrap();
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Counter(2000f64));

        // must be aggregated into separate
        let key = MetricName::new("gorets;t1=v1;t2=v3".into(), mode, &mut intermediate).unwrap();
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Counter(1000f64));
    }

    #[test]
    fn create_untagged_copy() {
        let mut data = BytesMut::new();
        data.extend_from_slice(b"tagged.metric;t2=v2;t1=v1:1000|c");

        let mut config = System::default();
        config.metrics.create_untagged_copy = true;
        let mut runner = FastTaskRunner::new(prepare_log("aggregate_with_copy"), Arc::new(config), 16);
        // "send" metric two times
        runner.run(FastTask::Parse(2, data.clone()));
        runner.run(FastTask::Parse(2, data));

        assert_eq!(runner.short.len(), 2, "additional metrics apepar from nowhere");
        // must be aggregated into two as sum
        let key = new_name("tagged.metric;t1=v1;t2=v2");
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Counter(2000f64));
        assert_eq!(metric.updates(), 2f64);

        // ensure "independent" untagged version of tagged metric also exists with same values
        let key = new_name("tagged.metric");
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Counter(2000f64));
        assert_eq!(metric.updates(), 2.);
    }

    #[test]
    fn parse_trashed_metric_buf() {
        let mut data = BytesMut::new();
        data.extend_from_slice(b"trash\ngorets1:+1000|g\nTRASH\ngorets2;tag3=shit;t2=fuck:-1000|g|@0.5\nMORE;tra=sh;|TrasH\nFUUU");

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = FastTaskRunner::new(prepare_log("parse_trashed"), Arc::new(config), 16);
        runner.run(FastTask::Parse(2, data));

        let key = new_name("gorets1");
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Gauge(1000f64));

        // expect tags to be sorted after parsing
        let key = new_name("gorets2;t2=fuck;tag3=shit");
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value(), &MetricValue::Gauge(-1000f64));
    }
}
