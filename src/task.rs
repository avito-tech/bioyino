use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::{Future, Sink};
use slog::{debug, warn, Logger};
use tokio::runtime::current_thread::spawn;

use bioyino_metric::parser::{MetricParser, MetricParsingError, ParseErrorHandler};
use bioyino_metric::{name::MetricName, Metric};

use crate::aggregate::{aggregate_task, AggregationData};
use crate::config::System;

use crate::{Cache, Float, AGG_ERRORS, DROPS, INGRESS_METRICS, PARSE_ERRORS, PEER_ERRORS};

#[derive(Debug)]
pub enum Task {
    Parse(u64, BytesMut),
    AddMetric(MetricName, Metric<Float>),
    AddMetrics(Vec<(MetricName, Metric<Float>)>),
    AddSnapshot(Vec<(MetricName, Metric<Float>)>),
    TakeSnapshot(oneshot::Sender<Cache>),
    Rotate(Option<UnboundedSender<Cache>>),
    Aggregate(AggregationData),
}

fn update_metric(cache: &mut Cache, name: MetricName, metric: Metric<Float>) {
    match cache.entry(name) {
        Entry::Occupied(ref mut entry) => {
            entry.get_mut().accumulate(metric).unwrap_or_else(|_| {
                AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
            });
        }
        Entry::Vacant(entry) => {
            entry.insert(metric);
        }
    };
}

#[derive(Debug)]
pub struct TaskRunner {
    long: HashMap<MetricName, Metric<Float>>,
    short: HashMap<MetricName, Metric<Float>>,
    buffers: HashMap<u64, (usize, BytesMut)>,
    config: Arc<System>,
    log: Logger,
}

impl TaskRunner {
    pub fn new(log: Logger, config: Arc<System>, cap: usize) -> Self {
        Self { long: HashMap::with_capacity(cap), short: HashMap::with_capacity(cap), buffers: HashMap::with_capacity(cap), config, log }
    }

    pub fn run(&mut self, task: Task) {
        match task {
            Task::Parse(addr, buf) => {
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

                let parser = MetricParser::new(buf, self.config.metrics.max_unparsed_buffer, self.config.metrics.max_tags_len, TaskParseErrorHandler(log));

                for (name, metric) in parser {
                    INGRESS_METRICS.fetch_add(1, Ordering::Relaxed);
                    update_metric(&mut self.short, name, metric);
                }
            }
            Task::AddMetric(name, metric) => update_metric(&mut self.short, name, metric),
            Task::AddMetrics(mut list) => {
                list.drain(..).map(|(name, metric)| update_metric(&mut self.short, name, metric)).last();
            }
            Task::AddSnapshot(mut list) => {
                // snapshots go to long cache to avoid being duplicated to other nodes
                list.drain(..).map(|(name, metric)| update_metric(&mut self.long, name, metric)).last();
            }
            Task::TakeSnapshot(channel) => {
                // clone short cache for further sending
                let short = self.short.clone();
                // join short cache to long cache removing data from short
                {
                    let mut long = &mut self.long; // self.long cannot be borrowed in map, so we borrow it earlier
                    self.short.drain().map(|(name, metric)| update_metric(&mut long, name, metric)).last();
                }

                // self.short now contains empty hashmap because of draining
                // give a copy of snapshot to requestor
                channel.send(short).unwrap_or_else(|_| {
                    PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    debug!(self.log, "shapshot not sent");
                });
            }
            Task::Rotate(channel) => {
                // this was the code before and it probably was not optimal because of copying lots
                // of data potentially
                //let rotated = self.long.clone();
                //self.long.clear();
                //self.long = HashMap::with_capacity(rotated.len());

                // we need our long cache to be sent for processing
                // in place of it we need a new cache with most probably the same size
                // BUT if we use exactly the same size, it may grow infinitely in long term
                // so we use a half of it
                let mut rotated = HashMap::with_capacity(self.long.len() / 2);
                std::mem::swap(&mut self.long, &mut rotated);
                let log = self.log.clone();
                channel.map(|c| {
                    let log = log.clone();
                    let respond = c.send(rotated).map(|_| ()).map_err(move |_| {
                        debug!(log, "rotated data not sent");
                        DROPS.fetch_add(1, Ordering::Relaxed);
                    });
                    spawn(respond);
                });

                self.buffers.retain(|_, (ref mut times, _)| {
                    *times += 1;
                    *times < 5
                });
            }

            Task::Aggregate(data) => aggregate_task(data),
        }
    }

    // used in tests in peer.rs
    pub fn get_long_entry(&self, e: &MetricName) -> Option<&Metric<Float>> {
        self.long.get(e)
    }
    pub fn get_short_entry(&self, e: &MetricName) -> Option<&Metric<Float>> {
        self.short.get(e)
    }
}

struct TaskParseErrorHandler(Option<Logger>);

impl ParseErrorHandler for TaskParseErrorHandler {
    fn handle(&self, input: &[u8], pos: usize, e: MetricParsingError) {
        PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
        if let Some(ref log) = self.0 {
            if let Ok(string) = std::str::from_utf8(input) {
                // TODO better error formatting instead of Debug
                warn!(log, "parsing error"; "buffer"=> format!("{:?}", string), "position"=>format!("{}", pos), "error"=>format!("{:?}", e));
            } else {
                warn!(log, "parsing error (bad unicode)"; "buffer"=> format!("{:?}", input), "position"=>format!("{}", pos), "error"=>format!("{:?}", e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bioyino_metric::MetricType;

    use crate::util::prepare_log;

    #[test]
    fn parse_trashed_metric_buf() {
        let mut data = BytesMut::new();
        data.extend_from_slice(b"trash\ngorets1:+1000|g\nTRASH\ngorets2:-1000;tag3=shit;t2=fuck|g|@0.5\nMORETrasH\nFUUU");

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = TaskRunner::new(prepare_log("parse_trashed"), Arc::new(config), 16);
        runner.run(Task::Parse(2, data));

        let key = MetricName::new("gorets1".into(), None);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(1i8)));
        assert_eq!(metric.sampling, None);

        // expect tags to be sorted after parsing
        let key = MetricName::new("gorets2;t2=fuck;t3=shit".into(), None);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(-1i8)));
        assert_eq!(metric.sampling, Some(0.5f32));
    }
}
