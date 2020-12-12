use std::collections::HashMap;
use std::sync::Arc;

use futures3::channel::mpsc::{self, Sender, UnboundedSender};
use futures3::{SinkExt, StreamExt, TryFutureExt};
use tokio2::spawn;

use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, ThreadPoolBuilder};
use serde_derive::{Deserialize, Serialize};
use slog::{info, warn, Logger};

use bioyino_metric::{
    aggregate::{Aggregate, AggregateCalculator},
    metric::MetricTypeName,
    name::{MetricName, NamingOptions},
    Metric,
};

use crate::config::{all_aggregates, Aggregation, ConfigError, Naming, RoundTimestamp};
use crate::task::Task;
use crate::{s, Float};

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
    pub round_timestamp: RoundTimestamp,
    pub mode: AggregationMode,
    pub multi_threads: usize,
    pub update_count_threshold: Float,
    pub aggregates: HashMap<MetricTypeName, Vec<Aggregate<Float>>>,
    pub namings: HashMap<(MetricTypeName, Aggregate<Float>), NamingOptions>,
}

impl AggregationOptions {
    pub(crate) fn from_config(config: Aggregation, interval: Float, naming: HashMap<MetricTypeName, Naming>, log: Logger) -> Result<Arc<Self>, ConfigError> {
        let Aggregation {
            round_timestamp,
            mode,
            threads,
            update_count_threshold,
            aggregates,
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
            round_timestamp,
            mode,
            multi_threads,
            update_count_threshold: Float::from(update_count_threshold),
            aggregates: HashMap::new(),
            namings: HashMap::new(),
        };

        // deny MetricTypeName::Default for aggregate list
        if aggregates.contains_key(&MetricTypeName::Default) {
            return Err(ConfigError::BadAggregate("\"default\"".to_string()));
        }

        // First task: deal with aggregates to be counted
        // consider 2 cases:
        // 1. aggregates is not specified at all, then the default value of all_aggregates() has
        //    been used already
        // 2. aggregates is defined partially per type, then we take only replacements specified
        //    for type and take others from defaults wich is in all_aggregates()

        // fill options with configured aggregates
        for (ty, aggs) in aggregates {
            if let Some(aggs) = aggs {
                let mut dedup = HashMap::new();
                for agg in aggs.into_iter() {
                    if dedup.contains_key(&agg) {
                        warn!(log, "removed duplicate aggregate \"{}\" for \"{}\"", agg.to_string(), ty.to_string());
                    } else {
                        dedup.insert(agg, ());
                    }
                }

                let aggs = dedup.into_iter().map(|(k, _)| k).collect();
                opts.aggregates.insert(ty, aggs);
            }
        }

        // set missing values defaults
        // NOTE: this is not joinable with previous cycle
        let all_aggregates = all_aggregates();
        for (ty, aggs) in all_aggregates {
            if !opts.aggregates.contains_key(&ty) {
                opts.aggregates.insert(ty, aggs);
            };
        }

        // Second task: having all type+aggregate pairs, fill naming options for them considering
        // the defaults

        let default_namings = crate::config::default_namings();
        for (ty, aggs) in &opts.aggregates {
            for agg in aggs {
                let naming = if let Some(option) = naming.get(ty) {
                    option.clone()
                } else if let Some(default) = naming.get(&MetricTypeName::Default) {
                    default.clone()
                } else {
                    default_namings.get(&ty).expect("default naming settings not found, contact developer").clone()
                };
                let mut noptions = naming.as_options(agg);
                if noptions.tag_value == "" {
                    // we only allow to change tag globally, but to allow removing it for value
                    // aggregate we use an empty value as a 'signal'
                    noptions.tag = b""[..].into();
                }
                if let Aggregate::Rate(None) = agg {
                    opts.namings.insert((ty.clone(), Aggregate::Rate(Some(interval))), noptions);
                } else {
                    opts.namings.insert((ty.clone(), agg.clone()), noptions);
                }
            }
        }

        // Now we can set the correct rate value
        for aggs in opts.aggregates.values_mut() {
            for agg in aggs {
                if let Aggregate::Rate(ref mut r) = agg {
                    *r = Some(interval)
                }
            }
        }

        Ok(Arc::new(opts))
    }
}

pub struct Aggregator {
    is_leader: bool,
    options: Arc<AggregationOptions>,
    chans: Vec<Sender<Task>>,
    // a channel where we receive rotated metrics from tasks
    tx: UnboundedSender<(MetricName, MetricTypeName, Aggregate<Float>, Float)>,
    log: Logger,
}

impl Aggregator {
    pub fn new(
        is_leader: bool,
        options: Arc<AggregationOptions>,
        chans: Vec<Sender<Task>>,
        tx: UnboundedSender<(MetricName, MetricTypeName, Aggregate<Float>, Float)>,
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

    pub async fn run(self) {
        let Self {
            is_leader,
            options,
            chans,
            tx,
            log,
        } = self;
        let (task_tx, mut task_rx) = mpsc::unbounded();

        let response_chan = if is_leader {
            Some(task_tx)
        } else {
            info!(log, "not leader, clearing metrics");
            drop(task_tx);
            None
        };

        let ext_log = log.clone();

        // regardless of leader state send rotate tasks with or without response channel
        // we don't need to await send, because we are waiting on task_rx eventually
        let mut handles = Vec::new();
        for chan in &chans {
            let mut chan = chan.clone();
            let rchan = response_chan.clone();
            let handle = spawn(async move { chan.send(Task::Rotate(rchan)).map_err(|_| s!(queue_errors)).await });
            handles.push(handle);
        }
        drop(response_chan);
        // wait for senders to do their job
        futures3::future::join_all(handles).await;

        // when we are not leader the aggregator job is done here: send_tasks will delete metrics
        if !is_leader {
            return;
        }

        // from now we consider us being a leader

        let mut cache: HashMap<MetricName, Vec<Metric<Float>>> = HashMap::new();
        while let Some(metrics) = task_rx.next().await {
            //     #[allow(clippy::map_entry)] // clippy offers us the entry API here, but it doesn't work without additional cloning
            for (name, metric) in metrics {
                let entry = cache.entry(name).or_default();
                entry.push(metric);
            }
        }

        info!(log, "leader aggregating metrics"; "amount"=>format!("{}", cache.len()));

        match options.mode {
            AggregationMode::Single => {
                cache
                    .into_iter()
                    .map(move |(name, metrics)| {
                        let task_data = AggregationData {
                            name,
                            metrics,
                            options: options.clone(),
                            response: tx.clone(),
                        };
                        aggregate_task(task_data);
                    })
                .last();
                }
            AggregationMode::Common => {
                cache
                    .into_iter()
                    .enumerate()
                    .map(move |(num, (name, metrics))| {
                        let task_data = AggregationData {
                            name,
                            metrics,
                            options: options.clone(),
                            response: tx.clone(),
                        };
                        let mut chan = chans[num % chans.len()].clone();
                        spawn(async move { chan.send(Task::Aggregate(task_data)).await });
                    })
                .last();
                }
            AggregationMode::Separate => {
                let pool = ThreadPoolBuilder::new()
                    .thread_name(|i| format!("bioyino_agg{}", i))
                    .num_threads(options.multi_threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    cache.into_par_iter().for_each(move |(name, metrics)| {
                        let task_data = AggregationData {
                            name,
                            metrics,
                            options: options.clone(),
                            response: tx.clone(),
                        };
                        aggregate_task(task_data);
                    });
                });
            }
        };

        info!(ext_log, "done aggregating");
    }
}

#[derive(Debug)]
pub struct AggregationData {
    pub name: MetricName,
    pub metrics: Vec<Metric<Float>>,
    pub options: Arc<AggregationOptions>,
    pub response: UnboundedSender<(MetricName, MetricTypeName, Aggregate<Float>, Float)>,
}

pub fn aggregate_task(data: AggregationData) {
    let AggregationData {
        name,
        mut metrics,
        options,
        response,
    } = data;

    // accumulate vector of metrics into a single metric first
    let first = if let Some(metric) = metrics.pop() {
        metric
    } else {
        // empty metric case is not possible actually
        s!(agg_errors);
        return;
    };

    let mut metric = metrics.into_iter().fold(first, |mut acc, next| {
        acc.accumulate(next).unwrap_or_else(|_| {
            s!(agg_errors);
        });
        acc
    });

    let mode = options.mode;
    let typename = MetricTypeName::from_metric(&metric);
    let aggregates = if let Some(agg) = options.aggregates.get(&typename) {
        agg
    } else {
        s!(agg_errors);
        return;
    };

    // take all required aggregates
    let calculator = AggregateCalculator::new(&mut metric, aggregates);
    calculator
        // count all of them that are countable (filtering None)
        .filter_map(|result| result)
        // set corresponding name
        .filter_map(|(idx, value)| {
            let aggregate = &aggregates[idx];
            match aggregate {
                Aggregate::UpdateCount => {
                    if value < options.update_count_threshold {
                        // skip aggregates below update counter threshold
                        None
                    } else {
                        Some((name.clone(), typename, *aggregate, value))
                    }
                }
                _ => Some((name.clone(), typename, *aggregate, value)),
            }
        })
    .map(|data| {
        let mut response = response.clone();
        let respond = async move { response.send(data).await };

        match mode {
            AggregationMode::Separate => {
                // In the separate mode there is no runtime, so we just run future
                // synchronously
                futures3::executor::block_on(respond).expect("responding thread: error sending aggregated metrics back");
            }
            _ => {
                spawn(respond);
            }
        }
    })
    .last();
    }

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::util::prepare_log;

    use futures3::channel::mpsc;
    use tokio2::runtime::Builder;
    use tokio2::time::delay_for;

    use bioyino_metric::{name::MetricName, Metric, MetricType};

    use crate::config;

    #[test]
    fn parallel_aggregation_ms_rayon() {
        let log = prepare_log("test_parallel_aggregation_ms");
        let mut chans = Vec::new();
        let (tx, mut rx) = mpsc::channel(5);
        chans.push(tx);

        let mut runtime = Builder::new()
            .thread_name("bio_agg_test")
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for test");

        let mut config = config::Aggregation::default();

        let timer = MetricTypeName::Timer;
        // add 0.8th percentile to timer aggregates
        config
            .aggregates
            .get_mut(&timer)
            .unwrap()
            .as_mut()
            .unwrap()
            .push(Aggregate::Percentile(0.8, 80));

        //"percentile-80".into());
        /*config.postfix_replacements.insert("percentile-80".into(), "percentile80".into());
          config.postfix_replacements.insert("min".into(), "lower".into());
          */
        // TODO: check tag replacements
        //config.tag_replacements.insert("percentile-80".into(), "p80".into());
        config.update_count_threshold = 1;
        config.mode = AggregationMode::Separate;
        config.threads = Some(2);

        let naming = config::default_namings(); //;.get(&timer).unwrap().clone();

        let options = AggregationOptions::from_config(config, 30f64, naming, log.clone()).unwrap();

        let (backend_tx, backend_rx) = mpsc::unbounded();

        let aggregator = Aggregator::new(true, options, chans, backend_tx, log.clone());

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
                config::all_aggregates()
                    .get(&timer)
                    .unwrap()
                    .clone()
                    .into_iter()
                    .map(|agg| Aggregate::<f64>::try_from(agg).unwrap())
                    .map(|agg| if agg == Aggregate::Rate(None) {
                        Aggregate::Rate(Some(30f64))
                    } else {
                        agg
                    })
                .chain(Some(Aggregate::Percentile(0.8, 80)))
                    .map(move |agg| (key.clone(), agg))
            })
        .flatten()
            .collect();

        let sent_cache = cache.clone();
        let rotate = async move {
            while let Some(task) = rx.next().await {
                if let Task::Rotate(Some(mut response)) = task {
                    let sent_cache = sent_cache.clone();
                    // Emulate rotation in task
                    spawn(async move { response.send(sent_cache).await });
                }
            }
        };

        runtime.spawn(rotate);
        runtime.spawn(aggregator.run());

        let required_len = required_aggregates.len();

        // When things happen inside threads, panics are catched by runtime.
        // So test did not fail correctly event on asserts inside
        // To avoid this, we make a fail counter which we check afterwards

        use std::sync::Mutex;
        let fails = Arc::new(Mutex::new(0usize));

        let inner_fails = fails.clone();
        let receive = async move {
            let result = backend_rx.collect::<Vec<_>>().await;
            // ensure aggregated data has ONLY required aggregates and no other shit
            for (n, _) in cache {
                // each metric should have all aggregates
                for (rname, ragg) in &required_aggregates {
                    if !result
                        .iter()
                            .position(|(name, _, ag, _)| {
                                //
                                //if let (&Aggregate::Rate(_), &Aggregate::Rate(_)) = (ag, &ragg) {
                                if false {
                                    // compare rate aggregate without internal value
                                    &name == &rname
                                } else {
                                    (name, ag) == (&rname, &ragg)
                                }
                            })
                            .is_some()
                            {
                                let mut fails = inner_fails.lock().unwrap();
                                *fails += 1;
                                println!("could not find {:?}", ragg);
                            }
                            }

                    // length should match the length of aggregates

                    if result.len() != required_len {
                        let mut fails = inner_fails.lock().unwrap();
                        *fails += 1;
                        println!("found other than required aggregates for {}", String::from_utf8_lossy(&n.name),);
                    }
                }
            };

            runtime.spawn(receive);

            let test_delay = async { delay_for(Duration::from_secs(2)).await };
            runtime.block_on(test_delay);
            assert_eq!(Arc::try_unwrap(fails).unwrap().into_inner().unwrap(), 0);
        }
    }
