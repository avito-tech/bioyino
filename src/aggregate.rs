use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::sync::mpsc::{self, Sender, UnboundedSender};
use futures::{Future, IntoFuture, Sink, Stream};
use tokio::executor::current_thread::spawn;

use log::warn as logw;
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, ThreadPoolBuilder};
use serde_derive::{Deserialize, Serialize};
use slog::{info, warn, Logger};

use bioyino_metric::{
    aggregate::{Aggregate, AggregateCalculator},
    name::{AggregationDestination, MetricName},
    Metric,
};

use crate::config::{Aggregation, ConfigError};
use crate::task::Task;
use crate::{Cache, Float};
use crate::{AGG_ERRORS, DROPS};

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
    pub update_count_threshold: Float,
    pub mode: AggregationMode,
    pub destination: AggregationDestination,
    pub aggregates: Vec<Aggregate<Float>>,
    pub postfix_replacements: HashMap<Aggregate<Float>, String>,
    pub prefix_replacements: HashMap<Aggregate<Float>, String>,
    pub tag_replacements: HashMap<Aggregate<Float>, String>,
    pub multi_threads: usize,
}

impl AggregationOptions {
    pub(crate) fn from_config(config: Aggregation, log: Logger) -> Result<Arc<Self>, ConfigError> {
        let Aggregation {
            update_count_threshold,
            mode,
            destination,
            ms_aggregates,
            postfix_replacements,
            prefix_replacements,
            tag_replacements,
            threads,
            tag_name,
        } = config;

        let multi_threads = match threads {
            Some(value) if mode == AggregationMode::Separate => value,
            Some(_) => {
                info!(log, "aggregation.threads parameter only works in \"separate\" mode and will be ignored");
                0
            }
            None if mode == AggregationMode::Separate => 0,
            _ => 0,
        };

        let mut opts = Self {
            //
            update_count_threshold: Float::from(update_count_threshold),
            mode,
            destination,
            multi_threads,
            aggregates: Vec::new(),
            postfix_replacements: HashMap::new(),
            prefix_replacements: HashMap::new(),
            tag_replacements: HashMap::new(),
        };

        // value aggregate cannot be specified in config and should always exist
        opts.aggregates.push(Aggregate::Value);

        // all replacements are additive, so we need to take a full list and only override it with
        // the specified keys and values

        // Our task here is parse aggregates and ensure they all have corresponsing replacement
        for agg in ms_aggregates {
            let parsed: Aggregate<Float> = agg.clone().try_into().map_err(ConfigError::BadAggregate)?;

            match opts.aggregates.iter().position(|p| p == &parsed) {
                Some(_) => continue, // don't process duplicates
                None => opts.aggregates.push(parsed.clone()),
            }

            let repl = match postfix_replacements.get(&agg) {
                Some(repl) => repl.clone(),
                None => {
                    // we should get here only for custom percentiles, because other aggregates
                    // already exist in replacements map
                    if agg.starts_with("percentile-") {
                        agg.replace("-", ".")
                    } else {
                        return Err(ConfigError::BadAggregate(agg));
                    }
                }
            };

            opts.postfix_replacements.insert(parsed.clone(), repl.clone());

            opts.prefix_replacements
                .insert(parsed.clone(), prefix_replacements.get(&agg).cloned().unwrap_or_default());

            if destination != AggregationDestination::Name {
                // only put tag replacements when aggregation into tags is enabled
                let repl = match tag_replacements.get(&agg) {
                    Some(repl) => repl.clone(),
                    None => {
                        // we should get here only for custom percentiles, because other aggregates
                        // already exist in replacements map
                        if agg.starts_with("percentile-") {
                            agg.replace("-", ".")
                        } else {
                            return Err(ConfigError::BadAggregate(agg));
                        }
                    }
                };
                opts.tag_replacements.insert(parsed, repl.clone());
            }
        }

        if destination != AggregationDestination::Name {
            opts.tag_replacements.insert(Aggregate::AggregateTag, tag_name.clone());
        }
        Ok(Arc::new(opts))
    }
}

pub struct Aggregator {
    is_leader: bool,
    options: Arc<AggregationOptions>,
    chans: Vec<Sender<Task>>,
    // a channel where we receive rotated metrics from tasks
    tx: UnboundedSender<(MetricName, Aggregate<Float>, Float)>,
    log: Logger,
}

impl Aggregator {
    pub fn new(
        is_leader: bool,
        options: Arc<AggregationOptions>,
        chans: Vec<Sender<Task>>,
        tx: UnboundedSender<(MetricName, Aggregate<Float>, Float)>,
        log: Logger,
    ) -> Self {
        Self {
            is_leader,
            options,
            chans,
            tx,
            log,
        }
    }
}

impl IntoFuture for Aggregator {
    type Item = ();
    type Error = ();
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            is_leader,
            options,
            chans,
            tx,
            log,
        } = self;
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
                                logw!("accumulation error");
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
                        .map(move |(name, metric)| {
                            let task_data = AggregationData {
                                name,
                                metric,
                                options: options.clone(),
                                response: tx.clone(),
                            };
                            aggregate_task(task_data);
                        })
                        .last();
                }
                AggregationMode::Common => {
                    accumulated
                        .into_iter()
                        .enumerate()
                        .map(move |(num, (name, metric))| {
                            let task_data = AggregationData {
                                name,
                                metric,
                                options: options.clone(),
                                response: tx.clone(),
                            };
                            spawn(chans[num % chans.len()].clone().send(Task::Aggregate(task_data)).map(|_| ()).map_err(|_| {
                                DROPS.fetch_add(1, Ordering::Relaxed);
                            }));
                        })
                        .last();
                }
                AggregationMode::Separate => {
                    let pool = ThreadPoolBuilder::new()
                        .thread_name(|i| format!("bioyino_crb{}", i))
                        .num_threads(options.multi_threads)
                        .build()
                        .unwrap();
                    pool.install(|| {
                        accumulated.into_par_iter().for_each(move |(name, metric)| {
                            let task_data = AggregationData {
                                name,
                                metric,
                                options: options.clone(),
                                response: tx.clone(),
                            };
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
    pub name: MetricName,
    pub metric: Metric<Float>,
    pub options: Arc<AggregationOptions>,
    pub response: UnboundedSender<(MetricName, Aggregate<Float>, Float)>,
}

pub fn aggregate_task(data: AggregationData) {
    let AggregationData {
        name,
        mut metric,
        options,
        response,
    } = data;

    let mode = options.mode;
    // take all required aggregates
    let calculator = AggregateCalculator::new(&mut metric, &options.aggregates);
    calculator
        // count all of them that are countable (filtering None) and leaving the aggregate itself
        .filter_map(|result| result)
        // set corresponding name
        .filter_map(|(idx, value)| {
            let aggregate = &options.aggregates[idx];
            match aggregate {
                Aggregate::UpdateCount => {
                    if value < options.update_count_threshold {
                        // skip aggregates below update counter threshold
                        None
                    } else {
                        Some((name.clone(), aggregate.clone(), value))
                    }
                }
                _ => Some((name.clone(), aggregate.clone(), value)),
            }
        })
        .map(|data| {
            let respond = response
                .clone()
                .send(data)
                .map_err(|_| {
                    logw!("error receiving task response");
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
    use std::convert::TryFrom;
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

        let mut config = config::Aggregation::default();
        config.ms_aggregates.push("percentile-80".into());
        /*config.postfix_replacements.insert("percentile-80".into(), "percentile80".into());
        config.postfix_replacements.insert("min".into(), "lower".into());
        */
        // TODO: check tag replacements
        //config.tag_replacements.insert("percentile-80".into(), "p80".into());
        config.update_count_threshold = 1;
        config.mode = AggregationMode::Separate;
        config.threads = Some(2);
        let options = AggregationOptions::from_config(config, log.clone()).unwrap();

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
            cache.insert(MetricName::from_raw_parts(format!("some.test.metric.{}.{}", i, counter).into(), None), metric);
        }

        // the result of timer aggregations we want is each key mapped to name
        let required_aggregates: Vec<(MetricName, Aggregate<f64>)> = cache
            .keys()
            .map(|key| {
                config::default_ms_aggregates()
                    .into_iter()
                    .map(|agg| Aggregate::<f64>::try_from(agg).unwrap())
                    .chain(Some(Aggregate::Percentile(0.8)))
                    .map(move |agg| (key.clone(), agg))
            })
            .flatten()
            .collect();

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

        let required_len = required_aggregates.len();
        let receive = backend_rx.collect().map(move |result| {
            // ensure aggregated data has ONLY required aggregates and no other shit
            for (n, _) in cache {
                // each metric should have all aggregates
                for (rname, ragg) in &required_aggregates {
                    assert!(
                        result.iter().position(|(name, ag, _)| (name, ag) == (&rname, &ragg)).is_some(),
                        "could not find {:?}",
                        ragg
                    );
                    //dbg!(result);
                }

                // length should match the length of aggregates

                assert_eq!(
                    result.len(),
                    required_len,
                    "found other than required aggregates for {}",
                    String::from_utf8_lossy(&n.name),
                );
            }
        });
        runtime.spawn(receive);

        let test_timeout = Instant::now() + Duration::from_secs(2);
        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
