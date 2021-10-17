use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Sender as AsyncSender;

use slog::{warn, Logger};

use bioyino_metric::{
    aggregate::{Aggregate, AggregateCalculator},
    metric::MetricTypeName,
    name::{MetricName, NamingOptions},
    FromF64,
};

use crate::cache::RotatedCacheShard;
use crate::config::{all_aggregates, Aggregation, ConfigError, Naming, RoundTimestamp};
use crate::{s, Float};

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub round_timestamp: RoundTimestamp,
    pub update_count_threshold: Float,
    pub aggregates: HashMap<MetricTypeName, Vec<Aggregate<Float>>>,
    pub namings: HashMap<(MetricTypeName, Aggregate<Float>), NamingOptions>,
}

impl AggregationOptions {
    pub(crate) fn from_config(config: Aggregation, interval: Float, naming: HashMap<MetricTypeName, Naming>, log: Logger) -> Result<Arc<Self>, ConfigError> {
        let Aggregation {
            round_timestamp,
            update_count_threshold,
            aggregates,
        } = config;

        let mut opts = Self {
            round_timestamp,
            update_count_threshold: Float::from_f64(update_count_threshold as f64),
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

pub type Aggregated = (MetricName, MetricTypeName, Aggregate<Float>, Float);

#[derive(Debug)]
pub struct AggregationData {
    pub metrics: RotatedCacheShard,
    pub options: Arc<AggregationOptions>,
    pub response: AsyncSender<Vec<Aggregated>>,
}

pub fn aggregate_task(data: AggregationData) {
    let AggregationData { metrics, options, response } = data;

    let mut result = Vec::new();
    for (name, metric) in metrics.into_iter() {
        // remove mutex
        let mut metric = metric.into_inner().unwrap();

        let typename = MetricTypeName::from_metric(&metric);
        // find all needed aggregates by type name
        let aggregates = if let Some(agg) = options.aggregates.get(&typename) {
            agg
        } else {
            s!(agg_errors);
            return;
        };

        // take all required aggregates
        let calculator = AggregateCalculator::new(&mut metric, aggregates);

        calculator
            // count all of them that are countable, i.e. filtering None
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
        .map(|data| result.push(data))
            .last();
        }

    futures::executor::block_on(response.send(result)).unwrap_or(());
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::TryFrom;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::util::prepare_log;

    use futures::channel::oneshot;
    use tokio::runtime::Builder;
    use tokio::time::sleep;

    use bioyino_metric::aggregate::Aggregate;
    use bioyino_metric::{name::MetricName, Metric, MetricTypeName, MetricValue};

    use crate::config;
    use crate::slow_task::{start_slow_threads, SlowTask};

    #[test]
    fn aggregation() {
        let log = prepare_log("test_parallel_aggregation");

        let mut config = config::System::default();
        let slow_chan = start_slow_threads(log.clone(), &config).expect("starting slow threads");
        let runtime = Builder::new_multi_thread()
            .thread_name("bio_agg_test")
            .enable_all()
            .build()
            .expect("creating runtime for test");

        let config = &mut config.aggregation;
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
        //*config.postfix_replacements.insert("percentile-80".into(), "percentile80".into());
        //config.postfix_replacements.insert("min".into(), "lower".into());
        // TODO: check tag replacements
        //config.tag_replacements.insert("percentile-80".into(), "p80".into());
        config.update_count_threshold = 1;

        let naming = config::default_namings(); //;.get(&timer).unwrap().clone();

        let agg_opts = AggregationOptions::from_config(config.clone(), 30., naming, log.clone()).unwrap();

        let counter = std::sync::atomic::AtomicUsize::new(0);

        // Create some cache data, send it to be joined into long cache
        let mut cache = HashMap::new();
        for i in 0..10u16 { // u16 can be converted to f32
            let mut metric = Metric::new(MetricValue::Timer(vec![0.]), None, 1.);
            for j in 1..100u16 {
                let new_metric = Metric::new(MetricValue::Timer(vec![j.into()]), None, 1.);
                metric.accumulate(new_metric).unwrap();
            }

            let counter = counter.fetch_add(1, Ordering::Relaxed);
            cache.insert(MetricName::from_raw_parts(format!("some.test.metric.{}.{}", i, counter).into(), None), metric);
        }
        slow_chan.send(SlowTask::Join(Arc::new(cache.clone()))).unwrap();

        // right after cache send a rotation task
        let (tx, rx) = oneshot::channel();
        slow_chan.send(SlowTask::Rotate(Some(tx))).unwrap();


        // the result of timer aggregations we want is each key mapped to name
        let required_aggregates: Vec<(MetricName, Aggregate<Float>)> = cache
            .keys()
            .map(|key| {
                config::all_aggregates()
                    .get(&timer)
                    .unwrap()
                    .clone()
                    .into_iter()
                    .map(|agg| Aggregate::<Float>::try_from(agg).unwrap())
                    .map(|agg| if agg == Aggregate::Rate(None) { Aggregate::Rate(Some(30.)) } else { agg })
                    .chain(Some(Aggregate::Percentile(0.8, 80)))
                    .map(move |agg| (key.clone(), agg))
            })
        .flatten()
            .collect();

        let required_len = required_aggregates.len();

        // When things happen inside threads, panics are catched by runtime.
        // So test did not fail correctly event on asserts inside
        // To avoid this, we make a fail counter which we check afterwards

        use std::sync::Mutex;
        let fails = Arc::new(Mutex::new(0usize));
        let inner_fails = fails.clone();
        let receiver = async move {
            let rotated = rx.await.unwrap();

            let (agg_tx, mut agg_rx) = async_channel::unbounded();

            // then send each shard to be aggregated separately
            for shard in rotated {
                let agg_data = AggregationData {
                    metrics: shard,
                    options: agg_opts.clone(),
                    response: agg_tx.clone(),
                };
                slow_chan.send(SlowTask::Aggregate(agg_data)).unwrap();
                    }

            use futures::StreamExt;
            // wait for answers nd collect data for further sending
            let mut aggregated = Vec::new();
            while let Some(blob) = agg_rx.next().await {
                // we want to send data in chunks but if shard has been sent,
                // there is no point of holding the whole shitload of metrics
                // all together, so we wrap blobs in an Arc, so it is dropped
                // when all chunks of it are sent
                aggregated.push(blob);
            }

            // ensure aggregated data has ONLY required aggregates and no other shit
            //for (n, _) in aggregated.iter().flatten() {
            // each metric should have all aggregates
            for (rname, ragg) in &required_aggregates {
                if !aggregated
                    .iter()
                        .flatten()
                        .position(|(name, _, ag, _)| {
                            if let (&Aggregate::Rate(_), &Aggregate::Rate(_)) = (ag, &ragg) {
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

                // length should match the length of aggregates
                let result_len = aggregated.iter().fold(0, |acc, elem| { acc + elem.len() } );
                if result_len != required_len {
                    let mut fails = inner_fails.lock().unwrap();
                    *fails += 1;
                    dbg!("found other than required aggregates: {} {}",result_len, required_len );
                }
            }
        };
        runtime.spawn(receiver);

        let test_delay = async { sleep(Duration::from_secs(2)).await };
        runtime.block_on(test_delay);
        drop(runtime);
        assert_eq!(Arc::try_unwrap(fails).unwrap().into_inner().unwrap(), 0);
        }
    }
