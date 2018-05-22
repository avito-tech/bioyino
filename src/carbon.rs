use std::borrow::Borrow;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use failure::Error;
use futures::future::ok;
use futures::stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{Future, IntoFuture, Stream};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_io::AsyncRead;
use tokio_io::codec::{Decoder, Encoder};

use config;
use errors::GeneralError;
use metric::Metric;
use util::{BackoffRetry, BackoffRetryBuilder};

use {Float, EGRESS};

#[derive(Clone)]
pub struct CarbonBackend {
    addr: SocketAddr,
    handle: Handle,
    options: config::Carbon,
    ts: Bytes,

    metrics: Arc<Vec<(Bytes, Float)>>,
    //    rx: UnboundedReceiver<(Bytes, Float)>,
}

impl CarbonBackend {
    pub(crate) fn new(
        addr: SocketAddr,
        options: config::Carbon,
        ts: Duration,
        handle: &Handle,
        metrics: Arc<Vec<(Bytes, Float)>>,
        //) -> (Self, UnboundedSender<(Bytes, Float)>) {
    ) -> Self {
        let ts: Bytes = ts.as_secs().to_string().into();
        let self_ = Self {
            addr,
            handle: handle.clone(),
            options,
            ts,
            metrics,
            //rx,
        };
        //(self_, tx)
        self_
    }
}

impl IntoFuture for CarbonBackend {
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = ()>>;

    fn into_future(self) -> Self::Future {
        let Self {
            addr,
            handle,
            options,
            ts,
            metrics,
            //rx,
        } = self;

        /*
        let gather = rx.collect().map(|vec| Arc::new(vec));

        let pusher = gather.and_then(|metrics| {
            let conn = TcpStream::connect(&addr, &handle).map_err(|e| GeneralError::Io(e));
            let future = conn.and_then(move |conn| {
                let writer = conn.framed(CarbonCodec::new());
                let metric_stream = stream::iter_ok(gather);
                metric_stream.forward(writer)
            });
            let retried = BackoffRetryBuilder::default();
            retried.spawn(&handle, future)
        });
        */
        let pusher = ok(());

        Box::new(pusher)
    }
}

pub struct CarbonCodec<B>(PhantomData<B>);

impl<B> CarbonCodec<B> {
    pub fn new() -> Self {
        CarbonCodec(PhantomData)
    }
}

impl<B> Decoder for CarbonCodec<B>
where
    B: Borrow<str>,
{
    type Item = ();
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        unreachable!()
    }
}

impl<B> Encoder for CarbonCodec<B>
where
    B: Borrow<str>,
{
    type Item = (String, String, B); // Metric name, suffix value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let m2 = m.2.borrow();
        let len = m.0.len() + 1 + m.1.len() + 1 + m2.len() + 1;
        buf.reserve(len);
        buf.put(m.0);
        buf.put(" ");
        buf.put(m.1);
        buf.put(" ");
        buf.put(m2);
        buf.put("\n");

        EGRESS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
