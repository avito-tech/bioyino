use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use combine::primitives::FastResult;
use futures::Sink;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;

use metric::Metric;
use parser::metric_parser;
use util::AggregateOptions;

use {Cache, Float, AGG_ERRORS, DROPS, INGRESS_METRICS, LONG_CACHE, PARSE_ERRORS, PEER_ERRORS,
     SHORT_CACHE};

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
    Parse(Bytes),
    AddMetrics(Cache),
    JoinSnapshot(Vec<Cache>),
    TakeSnapshot(oneshot::Sender<Cache>),
    Rotate(oneshot::Sender<Cache>),
    Aggregate(AggregateData),
}

impl Task {
    pub fn run(self) {
        match self {
            Task::Parse(buf) => {
                let mut input: &[u8] = &buf;
                let mut size_left = buf.len();
                let mut parser = metric_parser::<Float>();
                loop {
                    match parser.parse_stream_consumed(&mut input) {
                        FastResult::ConsumedOk(((name, metric), rest)) => {
                            INGRESS_METRICS.fetch_add(1, Ordering::Relaxed);
                            size_left -= rest.len();
                            if size_left == 0 {
                                break;
                            }
                            input = rest;
                            SHORT_CACHE.with(|c| {
                                match c.borrow_mut().entry(name) {
                                    Entry::Occupied(ref mut entry) => {
                                        entry.get_mut().aggregate(metric).unwrap_or_else(|_| {
                                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                        });
                                    }
                                    Entry::Vacant(entry) => {
                                        entry.insert(metric);
                                    }
                                };
                            });
                        }
                        FastResult::EmptyOk(_) | FastResult::EmptyErr(_) => {
                            break;
                        }
                        FastResult::ConsumedErr(_e) => {
                            // println!(
                            //"error parsing {:?}: {:?}",
                            //String::from_utf8(input.to_vec()),
                            //_e
                            //);
                            PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
                            // try to skip bad metric taking all bytes before \n
                            match input.iter().position(|&c| c == 10u8) {
                                Some(pos) if pos < input.len() - 1 => {
                                    input = input.split_at(pos + 1).1;
                                }
                                Some(_) => {
                                    break;
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Task::AddMetrics(mut cache) => {
                SHORT_CACHE.with(move |c| {
                    let mut short = c.borrow_mut();
                    cache
                        .drain()
                        .map(|(name, metric)| {
                            match short.entry(name) {
                                Entry::Occupied(ref mut entry) => {
                                    entry.get_mut().aggregate(metric).unwrap_or_else(|_| {
                                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    });
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(metric);
                                }
                            };
                        })
                        .last();
                });
            }
            Task::JoinSnapshot(mut shot) => {
                LONG_CACHE.with(move |c| {
                    let mut long = c.borrow_mut();
                    shot.drain(..)
                        .flat_map(|hmap| hmap.into_iter())
                        .map(|(name, metric)| {
                            match long.entry(name) {
                                Entry::Occupied(ref mut entry) => {
                                    entry.get_mut().aggregate(metric).unwrap_or_else(|_| {
                                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    });
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(metric);
                                }
                            };
                        })
                        .last();
                });
            }
            Task::TakeSnapshot(channel) => {
                let short = SHORT_CACHE.with(|c| {
                    let short = c.borrow().clone();
                    c.borrow_mut().clear();
                    short
                });
                // Aggregate short cache into long
                LONG_CACHE.with(|c| {
                    let mut long = c.borrow_mut();
                    let mut short = short.clone();
                    short
                        .drain()
                        .map(|(name, metric)| {
                            match long.entry(name) {
                                Entry::Occupied(ref mut entry) => {
                                    entry.get_mut().aggregate(metric).unwrap_or_else(|_| {
                                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    });
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(metric);
                                }
                            };
                        })
                        .last();
                });
                channel.send(short).unwrap_or_else(|_| {
                    PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    // TODO debug log
                    //    println!("shapshot not sent");
                });
            }
            Task::Rotate(channel) => {
                LONG_CACHE.with(|c| {
                    let rotated = c.borrow().clone();
                    c.borrow_mut().clear();
                    channel.send(rotated).unwrap_or_else(|_| {
                        println!("rotated data not sent");
                        DROPS.fetch_add(1, Ordering::Relaxed);
                    });
                });
            }
            Task::Aggregate(AggregateData {
                mut buf,
                name,
                metric,
                options,
                mut response,
            }) => {
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

                let aggregated = metric
                    .into_iter()
                    .map(move |(suffix, value)| {
                        buf.extend_from_slice(&name);
                        buf.extend_from_slice(suffix.as_bytes());
                        let name = buf.take().freeze();
                        (name, value)
                    })
                    .chain(upd)
                    .map(|data| {
                        response.start_send(data).map_err(|_| {
                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                        });
                    })
                    .last();
                response.poll_complete().map_err(|_| {
                    AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                });
            }
        }
    }
}
