use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::sync::mpsc::{self, Sender, UnboundedSender};
use futures::{Future, IntoFuture, Sink, Stream};
use tokio::executor::current_thread::spawn;

use bytes::{Bytes, BytesMut};
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, ThreadPoolBuilder};
use serde_derive::{Deserialize, Serialize};
use slog::{info, warn, Logger};

use bioyino_metric::{
    aggregate::{Aggregate, AggregateCalculator},
    name::{AggregationDestination, MetricName},
    Metric,
};

use crate::config::Aggregation;
use crate::task::Task;
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

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub update_count_threshold: u32,
    pub mode: AggregationMode,
    pub destination: AggregationDestination,
    pub ms_aggregates: Vec<Aggregate<Float>>,
    pub postfix_replacements: HashMap<Aggregate<Float>, String>,
    pub tag_replacements: HashMap<Aggregate<Float>, String>,
    pub multi_threads: usize,
}

impl AggregationOptions {
    pub(crate) fn from_config(config: Aggregation, log: Logger) -> Arc<Self> {
        let Aggregation { update_count_threshold, mode, destination, ms_aggregates, mut postfix_replacements, mut tag_replacements, threads, tag_name } = config;
        let multi_threads = match threads {
            Some(value) if mode == AggregationMode::Separate => value,
            Some(_) => {
                info!(log, "aggregation.threads parameter only works in \"separate\" mode and will be ignored");
                0
            }
            None if mode == AggregationMode::Separate => 0,
            _ => 0,
        };

        // to avoid parameters inconsistency remove the tag where it is not needed
        postfix_replacements.remove(&Aggregate::AggregateTag);

        // and place the configured value to aggregates
        tag_replacements.insert(Aggregate::AggregateTag, tag_name);

        Arc::new(AggregationOptions { update_count_threshold, mode, destination, ms_aggregates, postfix_replacements, tag_replacements, multi_threads })
    }
}

pub struct Aggregator {
    is_leader: bool,
    options: Arc<AggregationOptions>,
    chans: Vec<Sender<Task>>,
    // a channel where we receive rotated metrics from tasks
    //rx: UnboundedReceiver<Cache>,
    // a channel(supposedly from a backend) where we pass aggregated metrics to
    // TODO this probably needs to be generic stream
    tx: UnboundedSender<(Bytes, Float)>,
    log: Logger,
}

impl Aggregator {
    pub fn new(is_leader: bool, options: Arc<AggregationOptions>, chans: Vec<Sender<Task>>, tx: UnboundedSender<(Bytes, Float)>, log: Logger) -> Self {
        Self { is_leader, options, chans, tx, log }
    }
}

impl IntoFuture for Aggregator {
    type Item = ();
    type Error = ();
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { is_leader, options, chans, tx, log } = self;
        let (task_tx, task_rx) = mpsc::unbounded();

        let response_chan = if is_leader {
            Some(task_tx)
        } else {
            info!(log, "not leader, clearing metrics");
            None
        };

        let ext_log = log.clone();
        // regardless of leader state send rotate tasks with or without response channel
        let send_log = log.clone();
        let send_tasks = futures::future::join_all(chans.clone().into_iter().map(move |chan| {
            let send_log = send_log.clone();
            chan.send(Task::Rotate(response_chan.clone())).map_err(move |_| {
                warn!(send_log, "could not send data to task, most probably the task thread is dead");
            })
        }))
        .map(|_| ());

        // when we are not leader the aggregator job is done here: send_tasks will delete metrics
        if !is_leader {
            return Box::new(send_tasks);
        }

        // from now we consider us being a leader

        info!(log, "leader accumulating metrics");

        let acc_log = log.clone();
        let accumulate = send_tasks.and_then(move |_| {
            info!(acc_log, "collecting responses from tasks");
            task_rx.fold(HashMap::new(), move |mut acc: Cache, metrics| {
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
            })
        });

        let aggregate = accumulate.and_then(move |accumulated| {
            info!(log, "leader aggregating metrics"; "amount"=>format!("{}", accumulated.len()));

            match options.mode {
                AggregationMode::Single => {
                    accumulated
                        .into_iter()
                        .inspect(|_| {
                            EGRESS.fetch_add(1, Ordering::Relaxed);
                        })
                        .map(move |(name, metric)| {
                            let buf = BytesMut::with_capacity(1024);
                            let task_data = AggregationData { buf, name, metric, options: options.clone(), response: tx.clone() };
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
                            let task_data = AggregationData { buf, name, metric, options: options.clone(), response: tx.clone() };
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
                                let task_data = AggregationData { buf, name, metric, options: options.clone(), response: tx.clone() };
                                aggregate_task(task_data);
                            });
                    });
                }
            };

            info!(ext_log, "done aggregating");
            Ok(())
        });
        Box::new(aggregate)
    }
}

#[derive(Debug)]
pub struct AggregationData {
    pub buf: BytesMut,
    pub name: MetricName,
    pub metric: Metric<Float>,
    pub options: Arc<AggregationOptions>,
    pub response: UnboundedSender<(Bytes, Float)>,
}

pub fn aggregate_task(data: AggregationData) {
    let AggregationData { mut buf, name, mut metric, options, response } = data;

    let mode = options.mode;
    // take all required aggregates
    let calculator = AggregateCalculator::new(&mut metric, &options.ms_aggregates);
    calculator
        // count all of them that are countable (filtering None) and leaving the aggregate itself
        .filter_map(|result| result)
        // set corresponding name
        .filter_map(|(idx, value)| {
            let aggregate = &options.ms_aggregates[idx];
            match aggregate {
                &Aggregate::UpdateCount if value < Float::from(options.update_count_threshold) => {
                    // skip aggregates below update counter threshold
                    None
                }
                _ => {
                    if name.put_with_aggregate(&mut buf, options.destination, aggregate, &options.postfix_replacements, &options.tag_replacements).is_err() {
                        // TODO log error or count metric
                        AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                        return None;
                    };

                    //buf.extend_from_slice(&name.name[..]);
                    Some((buf.take().freeze(), value))
                }
            }
        })
        .map(|data| {
            let respond = response
                .clone()
                .send(data)
                .map_err(|_| {
                    AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                })
                .map(|_| ());

            match mode {
                AggregationMode::Separate => {
                    // In the separate mode there is no tokio runtime, so we just run future
                    // synchronously
                    respond.wait().unwrap()
                }
                _ => spawn(respond),
            }
        })
        .last();
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

    use crate::config;

    #[test]
    fn parallel_aggregation_rayon() {
        let log = prepare_log("test_parallel_aggregation");
        let mut chans = Vec::new();
        let (tx, rx) = mpsc::channel(5);
        chans.push(tx);

        let mut runtime = Runtime::new().expect("creating runtime for main thread");

        let mut options = AggregationOptions {
            //
            update_count_threshold: 1,
            mode: AggregationMode::Separate,
            destination: AggregationDestination::Smart,
            ms_aggregates: config::default_ms_aggregates(),
            postfix_replacements: config::default_replacements(),
            tag_replacements: config::default_replacements(),
            multi_threads: 2,
        };
        options.ms_aggregates.push(Aggregate::Percentile(0.8f64));
        options.postfix_replacements.insert(Aggregate::Percentile(0.8f64), "percentile80".to_string());
        // TODO: check tag replacements
        //options.tag_replacements.insert(Aggregate::Percentile(0.8f64), "p80".to_string());
        options.postfix_replacements.insert(Aggregate::Min, "lower".to_string());
        let options = Arc::new(options);

        let (backend_tx, backend_rx) = mpsc::unbounded();

        let aggregator = Aggregator::new(true, options, chans, backend_tx, log.clone()).into_future();

        let counter = std::sync::atomic::AtomicUsize::new(0);
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

        let sent_cache = cache.clone();
        let rotate = rx.for_each(move |task| {
            if let Task::Rotate(Some(response)) = task {
                // Emulate rotation in task
                spawn(response.send(sent_cache.clone()).map(|_| ()).map_err(|_| panic!("send error")));
            }
            Ok(())
        });

        runtime.spawn(rotate);
        runtime.spawn(aggregator);

        let required_aggregates = vec![
            "count",
            "last",
            "lower", // min is replaced with lower
            "max",
            "median",
            "mean",
            "sum",
            "updates",
            "percentile80", // custom percentile
            "percentile.75",
            "percentile.95",
            "percentile.98",
            "percentile.99",
            "percentile.999",
        ]; //.map(|a|a.to_string().collect();
        let required_len = required_aggregates.len();
        let receive = backend_rx.collect().map(move |result| {
            // ensure aggregated data has ONLY required aggregates and no other shit
            for (n, _) in cache {
                // each metric should have all aggregates
                for agg in &required_aggregates {
                    let mut required_name = BytesMut::new();
                    required_name.extend_from_slice(&n.name[..]);
                    required_name.extend_from_slice(&b"."[..]);
                    required_name.extend_from_slice(agg.as_bytes());
                    let required_name = required_name.freeze();
                    assert!(result.iter().position(|(n, _)| n == &required_name).is_some(), "could not find {:?}", required_name);
                }

                // length should match the length of aggregates
                let prefix_len = result.iter().filter(|(res_n, _)| res_n.starts_with(&n.name[..])).count();

                assert_eq!(prefix_len, required_len, "found other than required aggregates for {}: {:?}", String::from_utf8_lossy(&n.name), result.iter().filter(|(res_n, _)| res_n.starts_with(&n.name[..])).collect::<Vec<_>>());
            }
        });
        runtime.spawn(receive);

        let test_timeout = Instant::now() + Duration::from_secs(2);
        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
