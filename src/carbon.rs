use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{self, Duration, SystemTime};

use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_channel::Sender;
use futures::channel::oneshot;
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use log::warn;
use slog::{debug, error, info, o, Logger};

use tokio::net::TcpStream;
use tokio::spawn;
use tokio::time::{interval_at, Instant};
use tokio_util::codec::{Decoder, Encoder};

use bioyino_metric::{aggregate::Aggregate, metric::MetricTypeName, name::MetricName, FromF64};

use crate::aggregate::{Aggregated, AggregationData, AggregationOptions};
use crate::config::{Aggregation, Carbon, Naming, RoundTimestamp};
use crate::errors::GeneralError;
use crate::slow_task::SlowTask;
use crate::util::{bound_stream, resolve_with_port, retry_with_backoff, Backoff};
use crate::{s, Float, IS_LEADER};

pub async fn carbon_timer(
    log: Logger,
    mut options: Carbon,
    aggregation: Aggregation,
    naming: HashMap<MetricTypeName, Naming>,
    chan: Sender<SlowTask>,
) -> Result<(), GeneralError> {
    let dur = Duration::from_millis(options.interval);
    let mut carbon_timer = interval_at(Instant::now() + dur, dur);
    if options.chunks == 0 {
        options.chunks = 1
    }

    let interval = Float::from_f64(options.interval as f64 / 1000f64);
    let agg_opts = AggregationOptions::from_config(aggregation, interval, naming, log.clone())?;
    let log = log.new(o!("task"=>"carbon"));
    loop {
        carbon_timer.tick().await;
        let is_leader = IS_LEADER.load(Ordering::SeqCst);
        if is_leader {
            info!(log, "leader ready to aggregate metrics");

            // create rotation task, send it and wait for an answer
            let (tx, rx) = oneshot::channel();
            chan.send(SlowTask::Rotate(Some(tx))).unwrap_or_else(|_| {
                error!(log, "error sending rotation task");
            });
            let rotated = if let Ok(r) = rx.await {
                r
            } else {
                error!(log, "error receiving metric cache, worker thread may have panicked");
                continue;
            };

            let (agg_tx, mut agg_rx) = async_channel::unbounded();

            // then send each shard to be aggregated separately
            for shard in rotated {
                let agg_data = AggregationData {
                    metrics: shard,
                    options: agg_opts.clone(),
                    response: agg_tx.clone(),
                };
                chan.send(SlowTask::Aggregate(agg_data)).unwrap_or_else(|_| {
                    error!(log, "error sending aggregation task");
                });
            }
            drop(agg_tx);

            // wait for answers nd collect data for further sending
            let mut aggregated = Vec::new();
            while let Some(blob) = agg_rx.next().await {
                // we want to send data in chunks but if shard has been sent,
                // there is no point of holding the whole shitload of metrics
                // all together, so we wrap blobs in an Arc, so it is dropped
                // when all chunks of it are sent
                aggregated.push(blob);
            }

            info!(log, "done aggregating");

            let sender = CarbonSender::new(log.clone(), aggregated, agg_opts.clone(), options.clone())?;
            spawn(sender.run());
        } else {
            info!(log, "not leader, clearing metrics");
            chan.send(SlowTask::Rotate(None)).unwrap_or_else(|_| {
                error!(log, "error sending rotation task");
            });
        }
    }
}

fn rechunk<T>(mut input: Vec<Vec<T>>, chunks: usize) -> Vec<Vec<Vec<T>>> {
    if input.len() == 0 {
        return Vec::new();
    }

    if chunks == 1 {
        let mut res = Vec::new();
        res.push(input);
        return res;
    }

    let mut len = 0;
    for shard in &input {
        len += shard.len();
    }

    let chunk_size = if len > chunks { len / chunks } else { 1 };

    let mut recipient = Vec::with_capacity(chunks);
    let mut len_required = chunk_size;

    // create first vector of chunks, we will push vectors here
    recipient.push(Vec::new());

    // create first donor
    let mut donor = input.pop().unwrap();
    loop {
        let last = recipient.len() - 1;
        if donor.len() >= len_required {
            // donor has more len than required
            let new = donor.split_off(donor.len() - len_required); // remove that amount from donor
            recipient[last].push(new); // give it as a part of current chunk
            len_required = chunk_size; // reset the required len
            recipient.push(Vec::new()); // recipient is satisfied
        } else {
            // donor has not enough len
            // give what is left to recipient
            len_required -= donor.len();
            recipient[last].push(donor);
            // get a new donor or end
            if let Some(new) = input.pop() {
                donor = new
            } else {
                break;
            }
        }
    }

    if recipient.len() > 1 && recipient[recipient.len() - 1].is_empty() {
        recipient.pop().unwrap();
    }
    recipient
}

pub struct CarbonSender {
    ts: u64,
    log: Logger,
    metrics: Vec<Vec<Aggregated>>,
    agg_opts: Arc<AggregationOptions>,
    backend_opts: Carbon,
}

impl CarbonSender {
    pub fn new(log: Logger, metrics: Vec<Vec<Aggregated>>, agg_opts: Arc<AggregationOptions>, backend_opts: Carbon) -> Result<Self, GeneralError> {
        let ts = SystemTime::now().duration_since(time::UNIX_EPOCH).map_err(GeneralError::Time)?.as_secs();

        let log = log.new(o!("ts"=>ts.clone()));
        Ok(Self {
            ts,
            log,
            metrics,
            agg_opts,
            backend_opts,
        })
    }

    pub async fn run(self) {
        let Self {
            ts,
            log,
            metrics,
            agg_opts,
            backend_opts,
        } = self;

        let total_len = metrics.iter().fold(0, |acc, elem| acc + elem.len());
        if total_len == 0 {
            info!(log, "metric set is empty, not sending");
            return;
        }

        // at this point we have metrics as Vec<Vec((some, shit))>
        // but we want to send them in other chunk sizes without iterating
        // over those, probably very big, vectors, so we change the chunks
        // using maybe a few allocations but avoiding lots of iterations

        let chunks = rechunk(metrics, backend_opts.chunks);

        for (nth, chunk) in chunks.into_iter().enumerate() {
            let options = CarbonClientOptions {
                addr: backend_opts.address.clone(),
                bind: backend_opts.bind_address,
                interval: backend_opts.interval,
                agg: agg_opts.clone(),
            };

            let backoff = Backoff {
                delay: backend_opts.connect_delay,
                delay_mul: backend_opts.connect_delay_multiplier,
                delay_max: backend_opts.connect_delay_max,
                retries: backend_opts.send_retries,
            };

            let retry_log = log.clone();
            let chunk = Arc::new(chunk);
            let retrier = retry_with_backoff(backoff, move || {
                let client = CarbonBackend::new(options.clone(), ts, chunk.clone(), retry_log.clone());
                client.run()
            });

            let elog = log.clone();
            spawn(async move {
                retrier.await.unwrap_or_else(move |e| {
                    error!(elog, "failed to send chunk to graphite"; "chunk" => format!("{}", nth), "error"=>format!("{:?}",e));
                })
            });
        }
    }
}

#[derive(Clone)]
pub struct CarbonClientOptions {
    pub addr: String,
    pub bind: Option<SocketAddr>,
    pub interval: u64, // the timer is external, this is only used to calculate timestamp rounding
    pub agg: Arc<AggregationOptions>,
}

#[derive(Clone)]
pub struct CarbonBackend {
    options: CarbonClientOptions,
    ts: Bytes,
    metrics: Arc<Vec<Vec<Aggregated>>>,
    log: Logger,
}

impl CarbonBackend {
    pub(crate) fn new(options: CarbonClientOptions, ts: u64, metrics: Arc<Vec<Vec<Aggregated>>>, log: Logger) -> Self {
        // we have interval in milliseconds
        // but we need a timestamp to be rounded to seconds
        let interval = options.interval / 1000;
        let ts = match options.agg.round_timestamp {
            RoundTimestamp::Down => ts - (ts % interval),
            RoundTimestamp::No => ts,
            RoundTimestamp::Up => ts - (ts % interval) + interval,
        };
        let ts = ts.to_string();
        let log = log.new(o!("module"=>"carbon backend"));

        Self {
            options,
            metrics,
            log,
            ts: ts.into(),
        }
    }

    async fn run(self) -> Result<(), GeneralError> {
        let Self { options, ts, metrics, log } = self;
        let addr = resolve_with_port(&options.addr, 2003).await?;

        let conn = match options.bind {
            Some(bind_addr) => match bound_stream(&bind_addr) {
                Ok(std_stream) => TcpStream::from_std(std_stream).map_err(|e| GeneralError::Io(e))?,
                Err(e) => {
                    return Err(GeneralError::Io(e));
                }
            },
            None => TcpStream::connect(&addr).await?,
        };

        let chunk_len = metrics.iter().fold(0, |acc, elem| acc + elem.len());

        if chunk_len == 0 {
            debug!(log, "empty chunk, skipping");
            return Ok(());
        }

        info!(log, "sending metrics chunk"; "metrics"=>format!("{}", chunk_len));
        let mut writer = CarbonCodec::new(ts.clone(), options.agg.clone()).framed(conn);

        for m in metrics.iter().flatten() {
            writer.send(m).await?
        }

        info!(log, "finished");
        Ok(())
    }
}

pub struct CarbonCodec {
    ts: Bytes,
    options: Arc<AggregationOptions>,
}

impl CarbonCodec {
    pub fn new(ts: Bytes, options: Arc<AggregationOptions>) -> Self {
        Self { ts, options }
    }
}

// item: Metric name, type name, aggregate and counted value
impl<'a> Encoder<&'a (MetricName, MetricTypeName, Aggregate<Float>, Float)> for CarbonCodec {
    type Error = GeneralError;

    fn encode(&mut self, item: &'a (MetricName, MetricTypeName, Aggregate<Float>, Float), buf: &mut BytesMut) -> Result<(), Self::Error> {
        let (name, typename, aggregate, value) = item;
        let options = &self.options;
        if name.put_with_options(buf, typename.clone(), aggregate.clone(), &options.namings).is_err() {
            warn!("could not serialize '{:?}' with {:?}", &name.name[..], aggregate);
            s!(agg_errors);
            return Ok(());
        };

        buf.extend_from_slice(&b" "[..]);
        // write somehow doesn't extend buffer size giving "cannot fill sholw buffer" error
        buf.reserve(64);
        if let Err(e) = dtoa::write(&mut buf.writer(), *value) {
            warn!("buffer write error {:?}", e);
            s!(agg_errors);
            return Ok(());
        }
        buf.extend_from_slice(&b" "[..]);
        buf.extend_from_slice(&self.ts[..]);
        buf.extend_from_slice(&b"\n"[..]);
        s!(egress_carbon);
        Ok(())
    }
}

impl Decoder for CarbonCodec {
    type Item = ();
    type Error = GeneralError;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use futures::TryFutureExt;
    use tokio::net::TcpListener;
    use tokio::runtime::Builder as RBuilder;
    use tokio::time::sleep;
    use tokio_util::codec::{Decoder, LinesCodec};

    use crate::config::{self, Aggregation};
    use crate::util::prepare_log;
    use bioyino_metric::name::TagFormat;

    #[test]
    fn test_carbon_protocol_output() {
        let log = prepare_log("test_carbon_protocol");
        let mut intermediate = Vec::with_capacity(128);
        intermediate.resize(128, 0u8);

        let runtime = RBuilder::new_current_thread()
            .thread_name("bio_carbon_test")
            .enable_all()
            .build()
            .expect("creating runtime for carbon test");

        let ts = 1574745744_u64; // this is 24 seconds before start of the minute

        let name = MetricName::new(
            BytesMut::from("complex.test.bioyino_tagged;tag2=val2;tag1=value1"),
            TagFormat::Graphite,
            &mut intermediate,
        )
        .unwrap();

        let mut agg_opts = Aggregation::default();
        agg_opts.round_timestamp = RoundTimestamp::Up;
        let naming = config::default_namings();

        let agg_opts = AggregationOptions::from_config(agg_opts, 30., naming, log.clone()).unwrap();
        let options = CarbonClientOptions {
            addr: "127.0.0.1:2003".parse().unwrap(),
            bind: None,
            interval: 30000,
            agg: agg_opts,
        };

        //pub(crate) fn new(options: CarbonClientOptions, ts: u64, metrics: Arc<Vec<(MetricName, MetricTypeName, Aggregate<Float>, Float)>>, log: Logger) -> Self
        let backend = CarbonBackend::new(
            options,
            ts,
            Arc::new(vec![vec![(name, MetricTypeName::Gauge, Aggregate::Value, 42.)]]),
            log.clone(),
        );

        let server = async {
            let listener = TcpListener::bind(&"127.0.0.1:2003".parse::<::std::net::SocketAddr>().unwrap()).await.unwrap();
            // we need only one accept here yet
            let (conn, _) = listener.accept().await.expect("server getting next connection");
            let mut codec = LinesCodec::new().framed(conn);
            while let Some(line) = codec.next().await {
                let line = line.unwrap();
                // with "up" rounding the timestamp have to be rounded to 30th second which is at 1574745750
                assert_eq!(&line, "complex.test.bioyino_tagged;tag1=value1;tag2=val2 42 1574745750");
            }
        };

        runtime.spawn(server);
        runtime.spawn(backend.run().map_err(|_| panic!("codec error")));

        let test_delay = async { sleep(Duration::from_secs(2)).await };
        runtime.block_on(test_delay);
    }

    #[test]
    fn test_rechunk() {
        let sizes = vec![1, 2, 6, 13, 22, 22, 10, 1, 1, 1, 1, 1, 1, 25, 25, 120, 121, 122, 1, 1, 1];
        let mut vecs = Vec::new();
        let mut j = 0;
        for size in sizes {
            let mut v = Vec::new();
            for i in 0..size {
                v.push(j * 100 + i);
            }
            vecs.push(v);
            j += 1;
        }

        for chunks in 1..25 {
            let res = rechunk(vecs.clone(), chunks);
            let mut lens = Vec::new();
            for res in res.into_iter() {
                let chunk_len = res.into_iter().fold(0, |acc, elem| acc + elem.len());
                lens.push(chunk_len);
            }
            //dbg!(&lens);
            if lens.len() > 1 {
                let last = lens.len() - 2;
                for i in 0..last {
                    assert_eq!(lens[i], lens[i + 1]);
                }
            }
        }
    }
}
