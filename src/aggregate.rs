use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::stream::futures_unordered;
use futures::sync::mpsc::{Sender, UnboundedSender};
use futures::sync::oneshot;
use futures::{Future, IntoFuture, Sink, Stream};
use tokio::executor::current_thread::spawn;

use bytes::{Bytes, BytesMut};
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, ThreadPoolBuilder};
use serde_derive::{Deserialize, Serialize};
use slog::{debug, info, Logger};

use bioyino_metric::Aggregate;

use crate::task::{aggregate_task, AggregateData, Task};
use crate::util::UpdateCounterOptions;
use crate::{Cache, Float};
use crate::{AGG_ERRORS, DROPS, EGRESS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum AggregationMode {
    Single,
    #[serde(alias = "common-pool", alias = "common_pool")]
    Common,
    #[serde(alias = "separate-pool", alias = "separate_pool")]
    Separate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum AggregationDestination {
    Smart,
    Name,
    Tags,
    Both,
}

#[derive(Debug, Clone)]
pub struct AggregateOptions {
    pub is_leader: bool,
    pub update_counter: Option<UpdateCounterOptions>,
    pub mode: AggregationMode,
    pub destination: AggregationDestination,
    pub replacements: Arc<HashMap<Aggregate<Float>, String>>,
    pub multi_threads: usize,
}

pub struct Aggregator {
    options: AggregateOptions,
    chans: Vec<Sender<Task>>,
    // a channel where we receive rotated metrics from tasks
    //rx: UnboundedReceiver<Cache>,
    // a channel(supposedly from a backend) where we pass aggregated metrics to
    // TODO this probably needs to be generic stream
    tx: UnboundedSender<(Bytes, Float)>,
    log: Logger,
}

impl Aggregator {
    pub fn new(options: AggregateOptions, chans: Vec<Sender<Task>>, tx: UnboundedSender<(Bytes, Float)>, log: Logger) -> Self {
        Self { options, chans, tx, log }
    }
}

impl IntoFuture for Aggregator {
    type Item = ();
    type Error = ();
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { options, chans, tx, log } = self;
        let metrics = chans.clone().into_iter().map(|chan| {
            let (tx, rx) = oneshot::channel();
            // TODO: change oneshots to single channel
            // to do that, task must run in new tokio, then we will not have to pass handle to it
            //handle.spawn(chan.send(Task::Rotate(tx)).then(|_| Ok(())));
            chan.send(Task::Rotate(tx)).map_err(|_| ()).and_then(|_| rx.and_then(|m| Ok(m)).map_err(|_| ()))
        });

        if !options.is_leader {
            info!(log, "not leader - clearing metrics");
            // only get metrics from threads
            let not_leader = futures_unordered(metrics).for_each(|_| Ok(()));
            return Box::new(not_leader);
        }

        info!(log, "leader accumulating metrics");
        let accumulate = futures_unordered(metrics).fold(HashMap::new(), move |mut acc: Cache, metrics| {
            metrics
                .into_iter()
                .map(|(name, metric)| {
                    if acc.contains_key(&name) {
                        acc.get_mut(&name).unwrap().accumulate(metric).unwrap_or_else(|_| {
                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                        });
                    } else {
                        acc.insert(name, metric);
                    }
                })
                .last();
            Ok(acc)
        });

        let aggregate = accumulate.and_then(move |accumulated| {
            debug!(log, "leader aggregating metrics");

            match options.mode {
                AggregationMode::Single => {
                    accumulated
                        .into_iter()
                        .inspect(|_| {
                            EGRESS.fetch_add(1, Ordering::Relaxed);
                        })
                        .map(move |(name, metric)| {
                            let buf = BytesMut::with_capacity(1024);
                            let task_data = AggregateData { buf, name, metric, options: options.clone(), response: tx.clone() };
                            aggregate_task(task_data);
                        })
                        .last();
                }
                AggregationMode::Common => {
                    accumulated
                        .into_iter()
                        .inspect(|_| {
                            EGRESS.fetch_add(1, Ordering::Relaxed);
                        })
                        .enumerate()
                        .map(move |(num, (name, metric))| {
                            let buf = BytesMut::with_capacity(1024);
                            let task_data = AggregateData { buf, name, metric, options: options.clone(), response: tx.clone() };
                            spawn(chans[num % chans.len()].clone().send(Task::Aggregate(task_data)).map(|_| ()).map_err(|_| {
                                DROPS.fetch_add(1, Ordering::Relaxed);
                            }));
                        })
                        .last();
                }
                AggregationMode::Separate => {
                    let pool = ThreadPoolBuilder::new().thread_name(|i| format!("bioyino_crb{}", i).into()).num_threads(options.multi_threads).build().unwrap();
                    pool.install(|| {
                        accumulated
                            .into_par_iter()
                            .inspect(|_| {
                                EGRESS.fetch_add(1, Ordering::Relaxed);
                            })
                            .for_each(move |(name, metric)| {
                                let buf = BytesMut::with_capacity(1024);
                                let task_data = AggregateData { buf, name, metric, options: options.clone(), response: tx.clone() };
                                aggregate_task(task_data);
                            });
                    });
                }
            };

            Ok(())
        });
        Box::new(aggregate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use crate::util::prepare_log;

    use futures::sync::mpsc;
    use tokio::runtime::current_thread::Runtime;
    use tokio::timer::Delay;

    use bioyino_metric::{name::MetricName, Metric, MetricType};

    #[test]
    fn parallel_aggregation_rayon() {
        let log = prepare_log("test_parallel_aggregation");
        let mut chans = Vec::new();
        let (tx, rx) = mpsc::channel(5);
        chans.push(tx);

        let mut runtime = Runtime::new().expect("creating runtime for main thread");

        let options = AggregateOptions {
            //
            is_leader: true,
            update_counter: None,
            mode: AggregationMode::Separate,
            destination: AggregationDestination::Smart,
            replacements: Arc::new(crate::config::default_replacements()),
            multi_threads: 2,
        };

        let (backend_tx, backend_rx) = mpsc::unbounded();

        let aggregator = Aggregator::new(options, chans, backend_tx, log.clone()).into_future();

        // Emulate rotation in task
        let counter = std::sync::atomic::AtomicUsize::new(0);
        let rotate = rx.for_each(move |task| {
            if let Task::Rotate(response) = task {
                let mut cache = HashMap::new();
                for i in 0..10 {
                    let mut metric = Metric::new(0f64, MetricType::Timer(Vec::new()), None, None).unwrap();
                    for j in 1..100 {
                        let new_metric = Metric::new(j.into(), MetricType::Timer(Vec::new()), None, None).unwrap();
                        metric.accumulate(new_metric).unwrap();
                    }

                    let counter = counter.fetch_add(1, Ordering::Relaxed);
                    cache.insert(MetricName::new(format!("some.test.metric.{}.{}", i, counter).into(), None), metric);
                }
                response.send(cache).unwrap();
            }
            Ok(())
        });

        runtime.spawn(rotate);
        runtime.spawn(aggregator);

        let receive = backend_rx.collect().map(|result| assert_eq!(result.len(), 120));
        runtime.spawn(receive);

        let test_timeout = Instant::now() + Duration::from_secs(1);
        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
