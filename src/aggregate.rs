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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
