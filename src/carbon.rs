use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use failure::Error;
use ftoa;
use futures::future::{err, Either};
use futures::stream;
use futures::{Future, IntoFuture, Sink, Stream};
use log::warn;
use slog::{info, Logger};
use tokio::net::TcpStream;
use tokio_codec::{Decoder, Encoder};

use bioyino_metric::{aggregate::Aggregate, name::MetricName};

use crate::aggregate::AggregationOptions;
use crate::errors::GeneralError;
use crate::util::bound_stream;
use crate::{Float, AGG_ERRORS, EGRESS};

#[derive(Clone)]
pub struct CarbonClientOptions {
    pub addr: SocketAddr,
    pub bind: Option<SocketAddr>,
    pub options: Arc<AggregationOptions>,
    //pub round: RoundTimestamp,
    //pub interval: u32,
}

#[derive(Clone)]
pub struct CarbonBackend {
    options: CarbonClientOptions,
    ts: Bytes,
    metrics: Arc<Vec<(MetricName, Aggregate<Float>, Float)>>,
    log: Logger,
}

impl CarbonBackend {
    pub(crate) fn new(options: CarbonClientOptions, ts: Duration, metrics: Arc<Vec<(MetricName, Aggregate<Float>, Float)>>, log: Logger) -> Self {
        let ts = ts.as_secs();
//        let ts = ts - (ts % 30) + 30;
        Self {
            options,
            metrics,
            log,
            //ts: ts.as_secs().to_string().into(),
            ts: ts.to_string().into(),
        }
    }
}

impl IntoFuture for CarbonBackend {
    type Item = ();
    type Error = GeneralError;
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { options, ts, metrics, log } = self;
        let stream_future = match options.bind {
            Some(bind_addr) => match bound_stream(&bind_addr) {
                Ok(std_stream) => Either::A(TcpStream::connect_std(std_stream, &options.addr, &tokio::reactor::Handle::default())),
                Err(e) => Either::B(err(e)),
            },
            None => Either::A(TcpStream::connect(&options.addr)),
        };

        let elog = log.clone();
        let future = stream_future.map_err(GeneralError::Io).and_then(move |conn| {
            info!(log, "carbon backend sending metrics"; "ts" => format!("{}", String::from_utf8_lossy(&ts[..])));
            let writer = CarbonCodec::new(ts.clone(), options.options.clone()).framed(conn);
            let metric_stream = stream::iter_ok::<_, ()>(SharedIter::new(metrics));
            metric_stream
                .map_err(|_| GeneralError::CarbonBackend)
                .forward(writer.sink_map_err(|_| GeneralError::CarbonBackend))
                .map(move |_| info!(log, "carbon backend finished"))
                .map_err(move |e| {
                    info!(elog, "carbon backend error");
                    e
                })
        });

        Box::new(future)
    }
}

pub struct SharedIter<T> {
    inner: Arc<Vec<T>>,
    current: usize,
}

impl<T> SharedIter<T> {
    pub fn new(inner: Arc<Vec<T>>) -> Self {
        Self { inner, current: 0 }
    }
}

impl<T: Clone> Iterator for SharedIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let n = self.inner.get(self.current).cloned();
        self.current += 1;
        n
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

impl Decoder for CarbonCodec {
    type Item = ();
    // It could be a separate error here, but it's useless, since there is no errors in process of
    // encoding
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        unreachable!()
    }
}

impl Encoder for CarbonCodec {
    //type Item = (Bytes, Bytes, Bytes); // Metric name, value and timestamp
    type Item = (MetricName, Aggregate<Float>, Float); // Metric name, aggregate and counted value
    type Error = Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let (name, aggregate, value) = item;
        let options = &self.options;
        if name
            .put_with_aggregate(
                buf,
                options.destination,
                &aggregate,
                &options.postfix_replacements,
                &options.prefix_replacements,
                &options.tag_replacements,
            )
            .is_err()
        {
            // TODO don't log error maybe
            //warn!("could not serialize '{:?}' with {:?}", &name.name[..], aggregate);
            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };

        buf.extend_from_slice(&b" "[..]);
        // write somehow doesn't extend buffer size giving "cannot fill sholw buffer" error
        buf.reserve(64);
        if let Err(e) = ftoa::write(&mut buf.writer(), value) {
            warn!("write error {:?}", e);
            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        buf.extend_from_slice(&b" "[..]);
        buf.extend_from_slice(&self.ts[..]);
        buf.extend_from_slice(&b"\n"[..]);
        EGRESS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use std::time::{Duration, Instant};
    use tokio::codec::LinesCodec;
    use tokio::runtime::current_thread::Runtime;
    use tokio::timer::Delay;

    use crate::config::Aggregation;
    use crate::util::prepare_log;
    use bioyino_metric::name::TagFormat;

    #[test]
    fn test_carbon_protocol_output() {
        let test_timeout = Instant::now() + Duration::from_secs(1);
        let log = prepare_log("test_carbon_protocol");
        let mut intermediate = Vec::with_capacity(128);
        intermediate.resize(128, 0u8);

        let mut runtime = Runtime::new().expect("creating runtime for carbon test");

        let ts = 1574745744u64;

        let name = MetricName::new(
            BytesMut::from("complex.test.bioyino_tagged;tag2=val2;tag1=value1"),
            TagFormat::Graphite,
            &mut intermediate,
        )
        .unwrap();
        let agg_opts = AggregationOptions::from_config(Aggregation::default(), log.clone()).unwrap();
        let options = CarbonClientOptions {
            addr: "127.0.0.1:2003".parse().unwrap(),
            bind: None,
            options: agg_opts,
        };
        let backend = CarbonBackend::new(options, Duration::from_secs(ts), Arc::new(vec![(name, Aggregate::Value, 42f64)]), log.clone());

        let server = tokio::net::TcpListener::bind(&"127.0.0.1:2003".parse::<::std::net::SocketAddr>().unwrap())
            .unwrap()
            .incoming()
            .map_err(|e| panic!(e))
            .for_each(move |conn| {
                LinesCodec::new().framed(conn).for_each(|line| {
                    assert_eq!(&line, "complex.test.bioyino_tagged;tag1=value1;tag2=val2 42 1574745744");
                    Ok(())
                })
            })
            .map_err(move |e| panic!(e));

        runtime.spawn(server);
        runtime.spawn(backend.into_future().map_err(|_| panic!("codec error")));

        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
