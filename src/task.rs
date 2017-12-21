use futures::sync::oneshot;
use metric::Metric;

use parser::metric_parser;
use bytes::Bytes;
use std::collections::hash_map::Entry;
use combine::primitives::FastResult;
use std::sync::atomic::Ordering;
use {SHORT_CACHE, LONG_CACHE, Cache, INGRESS_METRICS, PARSE_ERRORS, AGG_ERRORS, PEER_ERRORS,
     DROPS, Float};

#[derive(Debug)]
pub enum Task {
    Parse(Bytes),
    JoinSnapshot(Vec<Cache>),
    TakeSnapshot(oneshot::Sender<Cache>),
    Rotate(oneshot::Sender<Cache>),
    //Join(String, Vec<Metric<f64>>, oneshot::Sender<(String, Metric<f64>)>),
    Join(Metric<Float>, Metric<Float>, oneshot::Sender<Metric<Float>>),
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
                        FastResult::EmptyOk(_) |
                        FastResult::EmptyErr(_) => {
                            break;
                        }
                        FastResult::ConsumedErr(_e) => {
                            //println!(
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
            Task::Join(mut metric1, metric2, channel) => {
                metric1.aggregate(metric2).unwrap_or_else(|_| {
                    AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                });
                channel.send(metric1).unwrap_or_else(|_| {
                    DROPS.fetch_add(1, Ordering::Relaxed);
                });
            }
        }
    }
}
