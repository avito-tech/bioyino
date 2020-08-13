use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{self, SystemTime, Duration};

use bytes::{Bytes, BytesMut, buf::BufMutExt};
use futures3::channel::mpsc::{self, Sender};
use futures3::{TryFutureExt};
use futures3::sink::{SinkExt};
use futures3::stream::{StreamExt};

use log::warn;
use slog::{error, info, o, Logger, debug};

use tokio2::net::TcpStream;
use tokio2::runtime::Builder;
use tokio2::time::{interval_at, Instant};
use tokio_util::codec::{Decoder, Encoder};

use bioyino_metric::{aggregate::Aggregate, name::MetricName, metric::MetricTypeName};

use crate::aggregate::{AggregationOptions, Aggregator};
use crate::config::{Carbon, RoundTimestamp, Aggregation, Naming, ConfigError};
use crate::errors::GeneralError;
use crate::task::Task;
use crate::util::{bound_stream, resolve_with_port, Backoff, retry_with_backoff};
use crate::{Float, s, IS_LEADER};

pub async fn carbon_timer(log: Logger, mut options: Carbon, aggregation: Aggregation, naming: HashMap<MetricTypeName, Naming>, chans: Vec<Sender<Task>>) -> Result<(), GeneralError> {
    let chans = chans.clone();

    let dur = Duration::from_millis(options.interval);
    let mut carbon_timer = interval_at(Instant::now() + dur, dur);
    if options.chunks == 0 {
        options.chunks = 1
    }
    let agg_opts = AggregationOptions::from_config(aggregation, naming, log.clone())?;
    let log = log.new(o!("thread"=>"carbon"));
    loop {
        carbon_timer.tick().await;
        let worker = CarbonWorker::new(log.clone(), agg_opts.clone(), options.clone(), chans.clone())?;
        std::thread::Builder::new()
            .name("bioyino_carbon".into())
            .spawn(move || worker.run()).map_err(|e|ConfigError::Io("spawning carbon thread".into(), e))?;
    }
}

pub struct CarbonWorker {
    ts: u64,
    log: Logger,
    agg_opts: Arc<AggregationOptions>,
    backend_opts: Carbon,
    chans: Vec<Sender<Task>>,
}

impl CarbonWorker {
    pub fn new(log: Logger, agg_opts: Arc<AggregationOptions>, backend_opts: Carbon, chans: Vec<Sender<Task>>) -> Result<Self, GeneralError> {
        let ts = SystemTime::now().duration_since(time::UNIX_EPOCH).map_err(GeneralError::Time)?.as_secs();
        Ok(Self {
            ts,
            log,
            agg_opts,
            backend_opts,
            chans,
        })
    }

    pub fn run(self) {
        let Self {
            ts,
            log,
            agg_opts,
            backend_opts,
            chans,
        } = self;

        debug!(log, "carbon thread running");
        let mut runtime = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for carbon backend thread");

        let is_leader = IS_LEADER.load(Ordering::SeqCst);

        let options = agg_opts.clone();

        if is_leader {
            info!(log, "leader ready to aggregate metrics");
            let (backend_tx, backend_rx) = mpsc::unbounded();
            let aggregator = Aggregator::new(is_leader, options.clone(), chans, backend_tx, log.clone());

            runtime.spawn(aggregator.run());

            let carbon_sender = async {
                let metrics = backend_rx.collect::<Vec<_>>().await;
                let carbon_log = log.clone();
                let carbon = backend_opts.clone();
                let chunk_size = if metrics.len() > carbon.chunks { metrics.len() / carbon.chunks } else { 1 };
                for (nth, chunk) in metrics.chunks(chunk_size).enumerate() {
                    let retry_log = carbon_log.clone();
                    let elog = carbon_log.clone();
                    let options = CarbonClientOptions {
                        addr: backend_opts.address.clone(),
                        bind: backend_opts.bind_address,
                        interval: carbon.interval,
                        options: options.clone(),
                    };

                    let backoff = Backoff {
                        delay: backend_opts.connect_delay,
                        delay_mul: backend_opts.connect_delay_multiplier,
                        delay_max: backend_opts.connect_delay_max,
                        retries: backend_opts.send_retries,
                    };
                    let options = options.clone();
                    let chunk = Arc::new(chunk.to_vec());
                    let retrier = retry_with_backoff(backoff, move || {
                        let client = CarbonBackend::new(options.clone(), ts, chunk.clone(), retry_log.clone());
                        client.run()
                    });
                    retrier.await.unwrap_or_else(move |e| {
                        error!(elog.clone(), "failed to send chunk to graphite"; "chunk" => format!("{}", nth), "error"=>format!("{:?}",e));
                    });
                }
            };
            runtime.block_on(carbon_sender);
        } else {
            info!(log, "not leader, removing metrics");
            let (backend_tx, _) = mpsc::unbounded();
            let aggregator = Aggregator::new(is_leader, options, chans, backend_tx, log.clone());
            runtime.block_on(aggregator.run())
        }
    }
}

#[derive(Clone)]
pub struct CarbonClientOptions {
    pub addr: String,
    pub bind: Option<SocketAddr>,
    pub interval: u64, // the timer is external, this is only used to calculate timestamp rounding
    pub options: Arc<AggregationOptions>,
}

#[derive(Clone)]
pub struct CarbonBackend {
    options: CarbonClientOptions,
    ts: Bytes,
    metrics: Arc<Vec<(MetricName, MetricTypeName, Aggregate<Float>, Float)>>,
    log: Logger,
}

impl CarbonBackend {
    pub(crate) fn new(options: CarbonClientOptions, ts: u64, metrics: Arc<Vec<(MetricName, MetricTypeName, Aggregate<Float>, Float)>>, log: Logger) -> Self {
        // we have interval in milliseconds
        // but we need a timestamp to be rounded to seconds
        let interval = options.interval / 1000;
        let ts = match options.options.round_timestamp {
            RoundTimestamp::Down => ts - (ts % interval),
            RoundTimestamp::No => ts,
            RoundTimestamp::Up => ts - (ts % interval) + interval,
        };
        let ts = ts.to_string();
        let log = log.new(o!("module"=>"carbon backend", "ts"=>ts.clone()));

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
                Ok(std_stream) => TcpStream::connect_std(std_stream, &addr).map_err(|e|GeneralError::Io(e)).await?,
                Err(e) => {
                    return Err(GeneralError::Io(e));
                }
            },
            None => TcpStream::connect(&options.addr).await?,
        };

        info!(log, "sending metrics chunk"; "metrics"=>format!("{}", metrics.len()));
        let mut writer = CarbonCodec::new(ts.clone(), options.options.clone()).framed(conn);

        for m in metrics.iter().cloned() {
            writer.send(m).await?
        };

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
impl Encoder<(MetricName, MetricTypeName, Aggregate<Float>, Float)> for CarbonCodec {
    type Error = GeneralError;

    fn encode(&mut self, item: (MetricName, MetricTypeName, Aggregate<Float>, Float), buf: &mut BytesMut) -> Result<(), Self::Error> {
        let (name, typename, aggregate, value) = item;
        let options = &self.options;
        if name
            .put_with_options(
                buf,
                typename,
                aggregate,
                &options.namings,
            )
                .is_err()
        {
            warn!("could not serialize '{:?}' with {:?}", &name.name[..], aggregate);
            s!(agg_errors);
            return Ok(());
        };

        buf.extend_from_slice(&b" "[..]);
        // write somehow doesn't extend buffer size giving "cannot fill sholw buffer" error
        buf.reserve(64);
        if let Err(e) = ftoa::write(&mut buf.writer(), value) {
            warn!("buffer write error {:?}", e);
            s!(agg_errors);
            return Ok(());
        }
        buf.extend_from_slice(&b" "[..]);
        buf.extend_from_slice(&self.ts[..]);
        buf.extend_from_slice(&b"\n"[..]);
        s!(egress);
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

    use tokio2::net::TcpListener;
    use tokio2::runtime::Builder as RBuilder;
    use tokio2::time::{delay_for};
    use tokio2::stream::StreamExt;
    use tokio_util::codec::{Decoder, LinesCodec};

    use crate::config::{self, Aggregation};
    use crate::util::prepare_log;
    use bioyino_metric::name::TagFormat;

    #[test]
    fn test_carbon_protocol_output() {
        let log = prepare_log("test_carbon_protocol");
        let mut intermediate = Vec::with_capacity(128);
        intermediate.resize(128, 0u8);

        let mut runtime = RBuilder::new()
            .thread_name("bio_carbon_test")
            .basic_scheduler()
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

        let agg_opts = AggregationOptions::from_config(agg_opts, naming, log.clone()).unwrap();
        let options = CarbonClientOptions {
            addr: "127.0.0.1:2003".parse().unwrap(),
            bind: None,
            interval: 30000,
            options: agg_opts,
        };

        //pub(crate) fn new(options: CarbonClientOptions, ts: u64, metrics: Arc<Vec<(MetricName, MetricTypeName, Aggregate<Float>, Float)>>, log: Logger) -> Self
        let backend = CarbonBackend::new(options, ts, Arc::new(vec![(name, MetricTypeName::Gauge, Aggregate::Value, 42f64)]), log.clone());

        let server = async {
            let mut listener = TcpListener::bind(&"127.0.0.1:2003".parse::<::std::net::SocketAddr>().unwrap()).await.unwrap();
            // we need only one accept here yet
            let (conn, _) = listener.accept().await.expect("server getting next connection");
            let mut codec =  LinesCodec::new().framed(conn);
            while let Some(line) = codec.next().await {
                let line = line.unwrap();
                // with "up" rounding the timestamp have to be rounded to 30th second which is at 1574745750
                assert_eq!(&line, "complex.test.bioyino_tagged;tag1=value1;tag2=val2 42 1574745750");
            }
        };

        runtime.spawn(server);
        runtime.spawn(backend.run().map_err(|_| panic!("codec error")));

        let test_delay = async { delay_for(Duration::from_secs(2)).await };
        runtime.block_on(test_delay);
    }
}
