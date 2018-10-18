use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use combine::Parser;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot;
use futures::Sink;

use metric::Metric;
use parser::metric_parser;
use util::AggregateOptions;

use {
    Cache, Float, AGG_ERRORS, DROPS, INGRESS_METRICS, LONG_CACHE, PARSE_ERRORS, PEER_ERRORS,
    SHORT_CACHE,
};

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

impl Task {
    pub fn run(self) {
        match self {
            Task::Parse(buf) => parse_and_insert(buf),
            Task::AddMetric(name, metric) => SHORT_CACHE.with(move |c| {
                let mut short = c.borrow_mut();
                update_metric(&mut short, name, metric);
            }),
            Task::AddMetrics(mut list) => SHORT_CACHE.with(move |c| {
                let mut short = c.borrow_mut();
                list.drain(..)
                    .map(|(name, metric)| update_metric(&mut short, name, metric))
                    .last();
            }),
            Task::AddSnapshot(mut list) => {
                LONG_CACHE.with(move |c| {
                    // snapshots go to long cache to avoid being duplicated to other nodes
                    let mut long = c.borrow_mut();
                    list.drain(..)
                        .map(|(name, metric)| update_metric(&mut long, name, metric))
                        .last();
                })
            }
            Task::TakeSnapshot(channel) => {
                let mut short = SHORT_CACHE.with(|c| {
                    let short = c.borrow().clone();
                    c.borrow_mut().clear();
                    short
                });

                channel.send(short.clone()).unwrap_or_else(|_| {
                    PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                    // TODO debug log
                    //    println!("shapshot not sent");
                });

                // Aggregate short cache into long
                LONG_CACHE.with(|c| {
                    let mut long = c.borrow_mut();
                    short
                        .drain()
                        .map(|(name, metric)| update_metric(&mut long, name, metric))
                        .last();
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
                //println!("AGGG {:?} {:?}", buf, name);
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
                        response
                            .start_send(data)
                            .map_err(|_| {
                                AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                            }).map(|_| ())
                            .unwrap_or(());
                    }).last();
                response
                    .poll_complete()
                    .map_err(|_| {
                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                    }).map(|_| ())
                    .unwrap_or_else(|_| ());
            }
        }
    }
}

fn cut_bad(buf: &mut Bytes) -> Option<usize> {
    PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
    match buf.iter().position(|&c| c == 10u8) {
        Some(pos) if pos <= buf.len() - 1 => {
            buf.advance(pos + 1);
            Some(pos)
        }
        Some(_) => None,
        None => None,
    }
}

fn parse_and_insert(mut buf: Bytes) {
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
                    if let Some(pos) = cut_bad(&mut buf) {
                        input = input.split_at(pos + 1).1;
                        continue;
                    } else {
                        break;
                    }
                }

                let metric = match Metric::<Float>::new(value, mtype, None, sampling) {
                    Ok(metric) => metric,
                    Err(_) => {
                        if let Some(pos) = cut_bad(&mut buf) {
                            input = input.split_at(pos + 1).1;
                            continue;
                        } else {
                            break;
                        }
                    }
                };

                INGRESS_METRICS.fetch_add(1, Ordering::Relaxed);
                SHORT_CACHE.with(|c| {
                    let mut short = c.borrow_mut();
                    update_metric(&mut short, name, metric);
                });
                if rest.len() == 0 {
                    break;
                }
            }
            Err(_e) => {
                if let Some(pos) = cut_bad(&mut buf) {
                    input = input.split_at(pos + 1).1;
                    continue;
                } else {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::MetricType;

    #[test]
    fn parse_trashed_metric_buf() {
        let mut data = Bytes::new();
        data.extend_from_slice(
            b"trash\ngorets1:+1000|g\nTRASH\ngorets2:-1000|g|@0.5\nMORETrasH\nFUUU",
        );

        parse_and_insert(data);

        SHORT_CACHE.with(|c| {
            let c = c.borrow();
            let key: Bytes = "gorets1".into();
            let metric = c.get(&key).unwrap().clone();
            assert_eq!(metric.value, 1000f64);
            assert_eq!(metric.mtype, MetricType::Gauge(Some(1i8)));
            assert_eq!(metric.sampling, None);

            let key: Bytes = "gorets2".into();
            let metric = c.get(&key).unwrap().clone();
            assert_eq!(metric.value, 1000f64);
            assert_eq!(metric.mtype, MetricType::Gauge(Some(-1i8)));
            assert_eq!(metric.sampling, Some(0.5f32));
        });
    }
}
