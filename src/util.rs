use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::Either;
use futures::stream::futures_unordered;
use futures::sync::mpsc::{Sender, UnboundedSender};
use futures::sync::oneshot;
use futures::{Async, Future, IntoFuture, Poll, Sink, Stream};
use tokio_core::reactor::{Handle, Timeout};

use task::{AggregateData, Task};
use {Cache, Float, AGG_ERRORS, EGRESS};

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
    type Future = Box<Future<Item = Self::Item, Error = ()>>;

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
                            .last()
                            .unwrap();
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
    pub fn spawn<F>(self, handle: &Handle, action: F) -> BackoffRetry<F>
    where
        F: IntoFuture + Clone,
    {
        let inner = Either::A(action.clone().into_future());
        BackoffRetry {
            handle: handle.clone(),
            action,
            inner: inner,
            options: self,
        }
    }
}

/// TCP client that is able to reconnect with customizable settings
pub struct BackoffRetry<F: IntoFuture> {
    handle: Handle,
    action: F,
    inner: Either<F::Future, Timeout>,
    options: BackoffRetryBuilder,
}

impl<F> BackoffRetry<F>
where
    F: IntoFuture,
{
    fn rotate(&mut self) {
        self.options.retries -= 1;
        let delay = self.options.delay as f32 * self.options.delay_mul;
        let delay = if delay <= self.options.delay_max as f32 {
            delay as u64
        } else {
            self.options.delay_max as u64
        };
        let delay =
            Timeout::new_at(Instant::now() + Duration::from_millis(delay), &self.handle).unwrap();

        self.inner = Either::B(delay)
    }
}

impl<F> Future for BackoffRetry<F>
where
    F: IntoFuture + Clone,
{
    type Item = F::Item;
    type Error = Option<F::Error>;

    fn poll(&mut self) -> Poll<F::Item, Option<F::Error>> {
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
                self.rotate();
            }
            if rotate_t {
                self.inner = Either::A(self.action.clone().into_future());
            }
        }
    }
}
