use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::Either;
use futures::stream::futures_unordered;
use futures::sync::mpsc::{Sender, UnboundedSender};
use futures::sync::oneshot;
use futures::{Async, Future, IntoFuture, Poll, Sink, Stream};
use slog::Logger;
use tokio::executor::current_thread::spawn;
use tokio::timer::{Delay, Interval};

use metric::{Metric, MetricType};
use task::{AggregateData, Task};
use {Cache, Float, AGG_ERRORS, DROPS, EGRESS, INGRESS, INGRESS_METRICS, PARSE_ERRORS, PEER_ERRORS};

// A future to send own stats. Never gets ready.
pub struct OwnStats {
    interval: u64,
    prefix: String,
    timer: Interval,
    chan: Sender<Task>,
    log: Logger,
}

impl OwnStats {
    pub fn new(interval: u64, prefix: String, chan: Sender<Task>, log: Logger) -> Self {
        let log = log.new(o!("source"=>"stats"));
        let now = Instant::now();
        let dur = Duration::from_millis(interval);
        Self {
            interval,
            prefix,
            timer: Interval::new(now + dur, dur),
            chan,
            log,
        }
    }

    pub fn get_stats(&mut self) {
        let mut metrics = HashMap::new();
        macro_rules! add_metric {
            ($global:ident, $value:ident, $suffix:expr) => {
                let $value = $global.swap(0, Ordering::Relaxed) as Float;
                if self.interval > 0 {
                    metrics.insert(
                        self.prefix.clone() + "." + $suffix,
                        Metric::new($value, MetricType::Counter, None).unwrap(),
                        );
                }
            };
        };
        add_metric!(EGRESS, egress, "egress");
        add_metric!(INGRESS, ingress, "ingress");
        add_metric!(INGRESS_METRICS, ingress_m, "ingress-metric");
        add_metric!(AGG_ERRORS, agr_errors, "agg-error");
        add_metric!(PARSE_ERRORS, parse_errors, "parse-error");
        add_metric!(PEER_ERRORS, peer_errors, "peer-error");
        add_metric!(DROPS, drops, "drop");
        if self.interval > 0 {
            let s_interval = self.interval as f64;
            info!(self.log, "stats";
                  "egress" => format!("{:2}", egress / s_interval),
                  "ingress" => format!("{:2}", ingress / s_interval),
                  "ingress-m" => format!("{:2}", ingress_m / s_interval),
                  "a-err" => format!("{:2}", agr_errors / s_interval),
                  "p-err" => format!("{:2}", parse_errors / s_interval),
                  "pe-err" => format!("{:2}", peer_errors / s_interval),
                  "drops" => format!("{:2}", drops / s_interval),
                  );
        }
        let log = self.log.clone();
        spawn(
            self.chan
            .clone()
            .send(Task::AddMetrics(metrics))
            .map(|_| ())
            .map_err(move |_| warn!(log, "stats future could not send metric to task")),
            );
    }
}

impl Future for OwnStats {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.timer.poll() {
                Ok(Async::Ready(Some(_))) => {
                    self.get_stats();
                }
                Ok(Async::Ready(None)) => unreachable!(),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_) => return Err(()),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateCounterOptions {
    pub threshold: u32,
    pub prefix: Bytes,
    pub suffix: Bytes,
}

#[derive(Debug, Clone)]
pub struct AggregateOptions {
    pub is_leader: bool,
    pub update_counter: Option<UpdateCounterOptions>,
}

pub struct Aggregator {
    options: AggregateOptions,
    chans: Vec<Sender<Task>>,
    // a channel where we receive rotated metrics from tasks
    //rx: UnboundedReceiver<Cache>,
    // a channel(supposedly from a backend) where we pass aggregated metrics to
    // TODO this probably needs to be generic stream
    tx: UnboundedSender<(Bytes, Float)>,
}

impl Aggregator {
    pub fn new(
        options: AggregateOptions,
        chans: Vec<Sender<Task>>,
        tx: UnboundedSender<(Bytes, Float)>,
        ) -> Self {
        Self { options, chans, tx }
    }
}

impl IntoFuture for Aggregator {
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { options, chans, tx } = self;
        let metrics = chans.into_iter().map(|chan| {
            let (tx, rx) = oneshot::channel();
            // TODO: change oneshots to single channel
            // to do that, task must run in new tokio, then we will not have to pass handle to it
            //handle.spawn(chan.send(Task::Rotate(tx)).then(|_| Ok(())));
            chan.send(Task::Rotate(tx))
                .map_err(|_| ())
                .and_then(|_| rx.and_then(|m| Ok(m)).map_err(|_| ()))
        });

        if options.is_leader {
            let accumulate = futures_unordered(metrics)//.for_each(|| {
                .fold(HashMap::new(), move |mut acc: Cache, metrics| {
                    metrics
                        .into_iter()
                        .map(|(name, metric)| {
                            if acc.contains_key(&name) {
                                acc.get_mut(&name).unwrap()
                                    .aggregate(metric).unwrap_or_else(|_| {AGG_ERRORS.fetch_add(1, Ordering::Relaxed);});
                            } else {
                                acc.insert(name, metric);
                            }
                        })
                    .last();
                    Ok(acc)
                        // })
        });

        let aggregate = accumulate.and_then(move |accumulated| {
            accumulated
                .into_iter()
                .inspect(|_| {
                    EGRESS.fetch_add(1, Ordering::Relaxed);
                })
            .map(move |(name, metric)| {
                let buf = BytesMut::with_capacity(1024);
                let task = Task::Aggregate(AggregateData {
                    buf,
                    name: Bytes::from(name),
                    metric,
                    options: options.clone(),
                    response: tx.clone(),
                });
                // as of now we just run each task in the current thread
                // there is a reason we should not in general run the task in the counting workers:
                // workers will block on heavy computation and may cause metrics goind to them over
                // network to be dropped because of backpressure
                // at the same time counting aggregation is not urgent because of current backend(carbon/graphite)
                // nature where one can send metrics with any timestamp
                // TODO: at some day counting workers will probably work in work-stealing mode,
                // after that we probably will be able to run task in common mode
                task.run();
            })
            .last();
            Ok(())
        });
        Box::new(aggregate)
        } else {
            // only get metrics from threads
            let not_leader = futures_unordered(metrics).for_each(|_| Ok(()));
            Box::new(not_leader)
        }
    }
}

pub struct BackoffRetryBuilder {
    pub delay: u64,
    pub delay_mul: f32,
    pub delay_max: u64,
    pub retries: usize,
}

impl Default for BackoffRetryBuilder {
    fn default() -> Self {
        Self {
            delay: 250,
            delay_mul: 2f32,
            delay_max: 5000,
            retries: 25,
        }
    }
}

impl BackoffRetryBuilder {
    pub fn spawn<F>(self, action: F) -> BackoffRetry<F>
        where
        F: IntoFuture + Clone,
        {
            let inner = Either::A(action.clone().into_future());
            BackoffRetry {
                action,
                inner: inner,
                options: self,
            }
        }
}

/// TCP client that is able to reconnect with customizable settings
pub struct BackoffRetry<F: IntoFuture> {
    action: F,
    inner: Either<F::Future, Delay>,
    options: BackoffRetryBuilder,
}

impl<F> Future for BackoffRetry<F>
where
F: IntoFuture + Clone,
{
    type Item = F::Item;
    type Error = Option<F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let (rotate_f, rotate_t) = match self.inner {
                // we are polling a future currently
                Either::A(ref mut future) => match future.poll() {
                    Ok(Async::Ready(item)) => {
                        return Ok(Async::Ready(item));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => {
                        if self.options.retries == 0 {
                            return Err(Some(e));
                        } else {
                            (true, false)
                        }
                    }
                },
                Either::B(ref mut timer) => match timer.poll() {
                    // we are waiting for the delay
                    Ok(Async::Ready(())) => (false, true),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(_) => unreachable!(), // timer should not return error
                },
            };

            if rotate_f {
                self.options.retries -= 1;
                let delay = self.options.delay as f32 * self.options.delay_mul;
                let delay = if delay <= self.options.delay_max as f32 {
                    delay as u64
                } else {
                    self.options.delay_max as u64
                };
                let delay = Delay::new(Instant::now() + Duration::from_millis(delay));
                self.inner = Either::B(delay);
            } else if rotate_t {
                self.inner = Either::A(self.action.clone().into_future());
            }
        }
    }
}
