use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};

use bytes::{BufMut, BytesMut};
use futures3::channel::mpsc::UnboundedSender;
use futures3::channel::oneshot;
use futures3::SinkExt;
use log::warn as logw;
use slog::{info, warn, Logger};

use tokio2::spawn;

use bioyino_metric::parser::{MetricParser, MetricParsingError, ParseErrorHandler};
use bioyino_metric::{name::MetricName, Metric};

use crate::aggregate::{aggregate_task, AggregationData};
use crate::config::System;

use crate::{s, Cache, Float, ConsensusKind, IS_LEADER};

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
    let ename = name.clone();
    let em = metric.clone();
    match cache.entry(name) {
        Entry::Occupied(ref mut entry) => {
            let mtype = { entry.get().mtype.clone() };
            entry.get_mut().accumulate(metric.clone()).unwrap_or_else(|_| {
                logw!(
                    "could not accumulate {:?} : {:?} into {:?} in task",
                    String::from_utf8_lossy(&ename.name[..]),
                    em,
                    mtype
                );
                s!(agg_errors);
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
    names_arena: BytesMut,
    config: Arc<System>,
    log: Logger,
}

impl TaskRunner {
    pub fn new(log: Logger, config: Arc<System>, cap: usize) -> Self {
        Self {
            long: HashMap::with_capacity(cap),
            short: HashMap::with_capacity(cap),
            buffers: HashMap::with_capacity(cap),
            names_arena: BytesMut::new(),
            config,
            log,
        }
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
            Task::AddMetric(name, metric) => update_metric(&mut self.short, name, metric),
            Task::AddMetrics(mut list) => {
                list.drain(..).map(|(name, metric)| update_metric(&mut self.short, name, metric)).last();
            }
            Task::AddSnapshot(mut list) => {
                // snapshots go to long cache to avoid being duplicated to other nodes
                // we also skip sorting tags in this mode, considering them being already sorted by
                // other node
                list.drain(..).map(|(name, metric)| update_metric(&mut self.long, name, metric)).last();
            }
            Task::TakeSnapshot(channel) => {
                let is_leader = IS_LEADER.load(Ordering::SeqCst);
                let short = if !is_leader && self.config.consensus == ConsensusKind::None {
                    // there is special case used in agents: when we are not leader and there is
                    // no consensus, that cannot make us leader, there is no point of aggregating
                    // long cache at all because it will never be sent anywhere
                    let mut prev_short = HashMap::with_capacity(self.short.len());
                    std::mem::swap(&mut prev_short, &mut self.short);
                    prev_short
                } else {
                    // clone short cache for further sending
                    let short = self.short.clone();
                    // join short cache to long cache removing data from short
                    {
                        let mut long = &mut self.long; // self.long cannot be borrowed in map, so we borrow it earlier
                        self.short.drain().map(|(name, metric)| update_metric(&mut long, name, metric)).last();
                    }
                    short
                };
                // self.short now contains empty hashmap because of draining
                // give a copy of snapshot to requestor
                channel.send(short).unwrap_or_else(|_| {
                    s!(queue_errors);
                    info!(self.log, "task could not send snapshot, receiving thread may be dead");
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
                // so we halve the size so it could be reduced if ingress flow amounts
                // become lower
                let mut rotated = HashMap::with_capacity(self.long.len() / 2);
                std::mem::swap(&mut self.long, &mut rotated);
                if let Some(mut c) = channel {
                    let log = self.log.clone();
                    spawn(async move { c.send(rotated).await
                        .unwrap_or_else(|_|{
                            s!(queue_errors);
                            info!(log, "task could not send rotated metric, receiving thread may be dead");
                        }); });
                }
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
    use bioyino_metric::{name::TagFormat, MetricType};

    use crate::util::{new_test_graphite_name as new_name, prepare_log};

    #[test]
    fn aggregate_tagged_metrics() {
        let mut data = BytesMut::new();

        // ensure metrics with same tags (going probably in different orders) go to same aggregate
        data.extend_from_slice(b"gorets;t2=v2;t1=v1:1000|c");
        data.extend_from_slice(b"\ngorets;t1=v1;t2=v2:1000|c");

        // ensure metrics with same name but different tags go to different aggregates
        data.extend_from_slice(b"\ngorets;t1=v1;t2=v3:1000|c");

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = TaskRunner::new(prepare_log("aggregate_tagged"), Arc::new(config), 16);
        runner.run(Task::Parse(2, data));

        let mut intermediate = Vec::new();
        intermediate.resize(9000, 0u8);
        let mode = TagFormat::Graphite;

        // must be aggregated into two as sum
        let key = MetricName::new("gorets;t1=v1;t2=v2".into(), mode, &mut intermediate).unwrap();
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 2000f64);
        assert_eq!(metric.mtype, MetricType::Counter);

        // must be aggregated into separate
        let key = MetricName::new("gorets;t1=v1;t2=v3".into(), mode, &mut intermediate).unwrap();
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Counter);
    }

    #[test]
    fn create_untagged_copy() {
        let mut data = BytesMut::new();
        data.extend_from_slice(b"tagged.metric;t2=v2;t1=v1:1000|c");

        let mut config = System::default();
        config.metrics.create_untagged_copy = true;
        let mut runner = TaskRunner::new(prepare_log("aggregate_with_copy"), Arc::new(config), 16);
        // "send" metric two times
        runner.run(Task::Parse(2, data.clone()));
        runner.run(Task::Parse(2, data));

        assert_eq!(runner.short.len(), 2, "additional metrics apepar from nowhere");
        // must be aggregated into two as sum
        let key = new_name("tagged.metric;t1=v1;t2=v2");
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 2000f64);
        assert_eq!(metric.mtype, MetricType::Counter);
        assert_eq!(metric.update_counter, 2);

        // ensure "independent" untagged version of tagged metric also exists with same values
        let key = new_name("tagged.metric");
        assert!(runner.short.contains_key(&key), "could not find {:?}", key);
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 2000f64);
        assert_eq!(metric.mtype, MetricType::Counter);
        assert_eq!(metric.update_counter, 2);
    }

    #[test]
    fn parse_trashed_metric_buf() {
        let mut data = BytesMut::new();
        data.extend_from_slice(b"trash\ngorets1:+1000|g\nTRASH\ngorets2;tag3=shit;t2=fuck:-1000|g|@0.5\nMORE;tra=sh;|TrasH\nFUUU");

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = TaskRunner::new(prepare_log("parse_trashed"), Arc::new(config), 16);
        runner.run(Task::Parse(2, data));

        let key = new_name("gorets1");
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(1i8)));
        assert_eq!(metric.sampling, None);

        // expect tags to be sorted after parsing
        let key = new_name("gorets2;t2=fuck;tag3=shit");
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(-1i8)));
        assert_eq!(metric.sampling, Some(0.5f32));
    }

    /*
       TODO: e2e for tasks
       #[test]
       fn parse_then_aggregate() {
       let mut data = BytesMut::new();

    // ensure metrics with same tags (going probably in different orders) go to same aggregate
    data.extend_from_slice(b"gorets;t2=v2;t1=v1:1000|c");
    data.extend_from_slice(b"\ngorets;t1=v1;t2=v2:1000|c");

    // ensure metrics with same name but different tags go to different aggregates
    data.extend_from_slice(b"\ngorets;t1=v1;t2=v3:1000|c");

    let mut config = System::default();
    config.metrics.log_parse_errors = true;
    let mut runner = TaskRunner::new(prepare_log("aggregate_tagged"), Arc::new(config), 16);
    runner.run(Task::Parse(2, data));

    dbg!(&runner.short);
    // must be aggregated into two as sum
    let key = MetricName::new("gorets;t1=v1;t2=v2".into(), None);
    assert!(runner.short.contains_key(&key), "could not find {:?}", key);
    let metric = runner.short.get(&key).unwrap().clone();
    assert_eq!(metric.value, 2000f64);
    assert_eq!(metric.mtype, MetricType::Counter);

    // must be aggregated into separate
    let key = MetricName::new("gorets;t1=v1;t2=v3".into(), None);
    assert!(runner.short.contains_key(&key), "could not find {:?}", key);
    let metric = runner.short.get(&key).unwrap().clone();
    assert_eq!(metric.value, 1000f64);
    assert_eq!(metric.mtype, MetricType::Counter);
    }
    */
}
