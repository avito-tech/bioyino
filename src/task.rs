use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use combine::error::UnexpectedParse;
use combine::Parser;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::{Future, Sink};
use slog::Logger;
use tokio::runtime::current_thread::spawn;

use config::System;
use metric::Metric;
use parser::metric_parser;
use util::AggregateOptions;

use {Cache, Float, AGG_ERRORS, DROPS, INGRESS_METRICS, PARSE_ERRORS, PEER_ERRORS};

#[derive(Debug)]
pub struct AggregateData {
    pub buf: BytesMut,
    pub name: Bytes,
    pub metric: Metric<Float>,
    pub options: AggregateOptions,
    pub response: UnboundedSender<(Bytes, Float)>,
}

#[derive(Debug)]
pub enum Task {
    Parse(u64, BytesMut),
    AddMetric(Bytes, Metric<Float>),
    AddMetrics(Vec<(Bytes, Metric<Float>)>),
    AddSnapshot(Vec<(Bytes, Metric<Float>)>),
    TakeSnapshot(oneshot::Sender<Cache>),
    Rotate(oneshot::Sender<Cache>),
    Aggregate(AggregateData),
}

fn update_metric(cache: &mut Cache, name: Bytes, metric: Metric<Float>) {
    match cache.entry(name) {
        Entry::Occupied(ref mut entry) => {
            entry.get_mut().aggregate(metric).unwrap_or_else(|_| {
                AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
            });
        }
        Entry::Vacant(entry) => {
            entry.insert(metric);
        }
    };
}

fn cut_bad(log: Option<Logger>, buf: &mut Bytes) -> Option<usize> {
    PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
    match buf.iter().position(|&c| c == 10u8) {
        Some(pos) if pos <= buf.len() - 1 => {
            if let Some(log) = log {
                warn!(log, "dropping buffer: {:?}", &buf[0..pos + 1]);
            }
            buf.advance(pos + 1);
            Some(pos)
        }
        Some(_) => None,
        None => None,
    }
}

#[derive(Debug)]
pub struct TaskRunner {
    long: HashMap<Bytes, Metric<Float>>,
    short: HashMap<Bytes, Metric<Float>>,
    buffers: HashMap<u64, (usize, BytesMut)>,
    config: Arc<System>,
    log: Logger,
}

impl TaskRunner {
    pub fn new(log: Logger, config: Arc<System>, cap: usize) -> Self {
        Self {
            long: HashMap::with_capacity(cap),
            short: HashMap::with_capacity(cap),
            buffers: HashMap::with_capacity(cap),
            config,
            log,
        }
    }

    pub fn run(&mut self, task: Task) {
        match task {
            Task::Parse(addr, buf) => {
                let log = if self.config.metrics.log_parse_errors {
                    Some(self.log.clone())
                } else {
                    None
                };
                let mut buf = {
                    let len = buf.len();
                    let (_, ref mut prev_buf) = self
                        .buffers
                        .entry(addr)
                        .and_modify(|(times, _)| {
                            *times = 0;
                        }).or_insert((0, BytesMut::with_capacity(len)));
                    prev_buf.reserve(buf.len());
                    prev_buf.put(buf);
                    prev_buf.clone().freeze()
                };

                let parsed = self.parse_and_insert(log, buf);
                self.buffers.entry(addr).and_modify(|(_, buf)| {
                    buf.advance(parsed);
                });
            }
            Task::AddMetric(name, metric) => update_metric(&mut self.short, name, metric),
            Task::AddMetrics(mut list) => {
                list.drain(..)
                    .map(|(name, metric)| update_metric(&mut self.short, name, metric))
                    .last();
            }
            Task::AddSnapshot(mut list) => {
                // snapshots go to long cache to avoid being duplicated to other nodes
                list.drain(..)
                    .map(|(name, metric)| update_metric(&mut self.long, name, metric))
                    .last();
            }
            Task::TakeSnapshot(channel) => {
                let mut short = HashMap::with_capacity(self.short.len());
                ::std::mem::swap(&mut short, &mut self.short);

                // self.short now contains empty hashmap
                // join a copy of data in short cache to long cache
                short
                    .iter()
                    .map(|(name, metric)| {
                        update_metric(&mut self.long, name.clone(), metric.clone())
                    }).last();

                // TODO: we could do this without additional copying if borrowck would allow us
                // this. Maybe NLL would help us with that in Rust 2018:
                //self.short
                //.drain()
                //.map(|(name, metric)| update_metric(&mut self.long, name, metric))
                //    .last();

                // give a copy of snapshot to requestor
                channel.send(short).unwrap_or_else(|_| {
                    PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    debug!(self.log, "shapshot not sent");
                });
            }
            Task::Rotate(channel) => {
                let rotated = self.long.clone();
                self.long.clear();
                let log = self.log.clone();
                channel.send(rotated).unwrap_or_else(|_| {
                    debug!(log, "rotated data not sent");
                    DROPS.fetch_add(1, Ordering::Relaxed);
                });

                self.buffers.retain(|_, (ref mut times, _)| {
                    *times += 1;
                    *times < 5
                });
            }

            Task::Aggregate(data) => aggregate_task(data),
        }
    }

    fn parse_and_insert(&mut self, log: Option<Logger>, mut buf: Bytes) -> usize {
        // Cloned buf is shallow copy, so input and buf are the same bytes.
        // We are going to parse the whole slice, so for parser we use input as readonly
        // while buf follows the parser progress and is cut to get only names
        // so they are zero-copied
        let mut input: &[u8] = &(buf.clone());
        let mut parser = metric_parser::<Float>();
        loop {
            let buflen = buf.len();
            match parser.parse(&input) {
                Ok(((name, value, mtype, sampling), rest)) => {
                    // name is always at the beginning of the buf
                    let name = buf.split_to(name.len());
                    buf.advance(buflen - rest.len() - name.len());
                    input = rest;

                    // check if name is valid UTF-8
                    if let Err(_) = ::std::str::from_utf8(&name) {
                        if let Some(pos) = cut_bad(log.clone(), &mut buf) {
                            input = input.split_at(pos + 1).1;
                            continue;
                        } else {
                            return rest.len();
                            //break;
                        }
                    }

                    let metric = match Metric::<Float>::new(value, mtype, None, sampling) {
                        Ok(metric) => metric,
                        Err(_) => {
                            if let Some(pos) = cut_bad(log.clone(), &mut buf) {
                                input = input.split_at(pos + 1).1;
                                continue;
                            } else {
                                return rest.len();
                                //break;
                            }
                        }
                    };

                    INGRESS_METRICS.fetch_add(1, Ordering::Relaxed);
                    update_metric(&mut self.short, name, metric);
                    if rest.len() == 0 {
                        return 0;
                        //break;
                    }
                }
                Err(UnexpectedParse::Eoi) => {
                    return buflen;
                    //break;
                }
                Err(_e) => {
                    if let Some(pos) = cut_bad(log.clone(), &mut buf) {
                        input = input.split_at(pos + 1).1;
                        continue;
                    } else {
                        return input.len();
                        //break;
                    }
                }
            }
        }
    }

    // used in tests in peer.rs
    pub fn get_long_entry(&self, e: &Bytes) -> Option<&Metric<Float>> {
        self.long.get(e)
    }
    pub fn get_short_entry(&self, e: &Bytes) -> Option<&Metric<Float>> {
        self.short.get(e)
    }
}

pub fn aggregate_task(data: AggregateData) {
    let AggregateData {
        mut buf,
        name,
        metric,
        options,
        response,
    } = data;
    let upd = if let Some(options) = options.update_counter {
        if metric.update_counter > options.threshold {
            // + 2 is for dots
            let cut_len = options.prefix.len() + name.len() + options.suffix.len() + 2;
            buf.reserve(cut_len);
            if options.prefix.len() > 0 {
                buf.put_slice(&options.prefix);
                buf.put_slice(b".");
            }

            buf.put_slice(&name);
            if options.suffix.len() > 0 {
                buf.put_slice(b".");
                buf.put_slice(&options.suffix);
            }

            let counter = buf.take().freeze();
            Some((counter, metric.update_counter.into()))
        } else {
            None
        }
    } else {
        None
    };

    metric
        .into_iter()
        .map(move |(suffix, value)| {
            buf.extend_from_slice(&name);
            buf.extend_from_slice(suffix.as_bytes());
            let name = buf.take().freeze();
            (name, value)
        }).chain(upd)
        .map(|data| {
            spawn(
                response
                    .clone()
                    .send(data)
                    .map_err(|_| {
                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                    }).map(|_| ()),
            );
        }).last();
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::MetricType;

    use util::prepare_log;

    #[test]
    fn parse_trashed_metric_buf() {
        let mut data = Bytes::new();
        data.extend_from_slice(
            b"trash\ngorets1:+1000|g\nTRASH\ngorets2:-1000|g|@0.5\nMORETrasH\nFUUU",
        );

        let mut config = System::default();
        config.metrics.log_parse_errors = true;
        let mut runner = TaskRunner::new(prepare_log("parse_thrashed"), Arc::new(config), 16);
        runner.parse_and_insert(Some(prepare_log("thrashed")), data);

        let key: Bytes = "gorets1".into();
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(1i8)));
        assert_eq!(metric.sampling, None);

        let key: Bytes = "gorets2".into();
        let metric = runner.short.get(&key).unwrap().clone();
        assert_eq!(metric.value, 1000f64);
        assert_eq!(metric.mtype, MetricType::Gauge(Some(-1i8)));
        assert_eq!(metric.sampling, Some(0.5f32));
    }
}
