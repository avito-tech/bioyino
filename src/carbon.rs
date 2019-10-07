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
use slog::{info, Logger};
use tokio::net::TcpStream;
use tokio_codec::{Decoder, Encoder};

use crate::errors::GeneralError;

use crate::util::bound_stream;
use crate::{Float, AGG_ERRORS};

#[derive(Clone)]
pub struct CarbonClientOptions {
    pub addr: SocketAddr,
    pub bind: Option<SocketAddr>,
}

#[derive(Clone)]
pub struct CarbonBackend {
    options: CarbonClientOptions,
    metrics: Arc<Vec<(Bytes, Bytes, Bytes)>>,
    log: Logger,
}

impl CarbonBackend {
    pub(crate) fn new(options: CarbonClientOptions, ts: Duration, metrics: Arc<Vec<(Bytes, Float)>>, log: Logger) -> Self {
        let ts: Bytes = ts.as_secs().to_string().into();

        let buf = BytesMut::with_capacity(metrics.len() * 200); // 200 is an approximate for full metric name + value
        let (metrics, _) = metrics.iter().fold((Vec::new(), buf), |(mut acc, mut buf), (name, metric)| {
            let mut wr = buf.writer();
            let buf = match ftoa::write(&mut wr, *metric) {
                Ok(()) => {
                    buf = wr.into_inner();
                    let metric = buf.take().freeze();
                    acc.push((name.clone(), metric, ts.clone()));
                    buf
                }
                Err(_) => {
                    AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                    wr.into_inner()
                }
            };
            (acc, buf)
        });
        let metrics = Arc::new(metrics);
        let self_ = Self { options, metrics, log };
        self_
    }
}

impl IntoFuture for CarbonBackend {
    type Item = ();
    type Error = GeneralError;
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { options, metrics, log } = self;
        let stream_future = match options.bind {
            Some(bind_addr) => match bound_stream(&bind_addr) {
                Ok(std_stream) => Either::A(TcpStream::connect_std(std_stream, &options.addr, &tokio::reactor::Handle::default())),
                Err(e) => Either::B(err(e)),
            },
            None => Either::A(TcpStream::connect(&options.addr)),
        };

        let elog = log.clone();
        let future = stream_future.map_err(GeneralError::Io).and_then(move |conn| {
            info!(log, "carbon backend sending metrics");
            let writer = CarbonCodec::new().framed(conn);
            let metric_stream = stream::iter_ok::<_, ()>(SharedIter::new(metrics));
            metric_stream.map_err(|_| GeneralError::CarbonBackend).forward(writer.sink_map_err(|_| GeneralError::CarbonBackend)).map(move |_| info!(log, "carbon backend finished")).map_err(move |e| {
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
        let n = self.inner.get(self.current).map(|i| i.clone());
        self.current += 1;
        n
    }
}

pub struct CarbonCodec;

impl CarbonCodec {
    pub fn new() -> Self {
        CarbonCodec //(PhantomData)
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
    type Item = (Bytes, Bytes, Bytes); // Metric name, suffix value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let len = m.0.len() + 1 + m.1.len() + 1 + m.2.len() + 1;
        buf.reserve(len);
        buf.put(m.0);
        buf.put(" ");
        buf.put(m.1);
        buf.put(" ");
        buf.put(m.2);
        buf.put("\n");
        Ok(())
    }
}
