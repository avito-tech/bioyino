//use std::net::SocketAddr;
use std::sync::Arc;
use std::ops::{Add, Sub, AddAssign, SubAssign, Div};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::fmt::Display;

use {PARSE_ERRORS, INGRESS, EGRESS, AGG_ERRORS};

use std::str::FromStr;
use bytes::{Bytes, BytesMut, BufMut};
use metric::Metric;
use parser::metric_parser;
use smallvec::SmallVec;
use tokio_core::reactor::Handle;
use tokio_core::net::UdpSocket;
use tokio_io::codec::{Encoder, Decoder};
use futures::future::Executor;
use futures::{IntoFuture, Future};
use futures::future::{lazy, ok, loop_fn, Loop};

use std::sync::atomic::Ordering;
use combine::primitives::FastResult;
use combine::Parser;
use failure::Error;

use chashmap::CHashMap;

#[derive(Debug)]
pub struct StatsdServer<E, F>
where
    E: Executor<Box<Future<Item = (), Error = ()> + Send + 'static>>,

    F: Copy + PartialEq + Debug,
{
    socket: UdpSocket,
    handle: Handle,
    executor: E,
    cache: Arc<CHashMap<String, Metric<F>>>,
    buf: BytesMut,
}

impl<E, F> StatsdServer<E, F>
where
E: Executor<Box<Future<Item = (), Error = ()> + Send + 'static>> + 'static,
F: Copy + PartialEq + Debug,
{
    pub fn new(
        socket: UdpSocket,
        handle: &Handle,
        executor: E,
        cache: Arc<CHashMap<String, Metric<F>>>,
        buf: BytesMut,
        ) -> Self {
        Self {
            socket,
            handle: handle.clone(),
            executor,
            cache,
            buf,
        }
    }

}

impl<E, F> IntoFuture for StatsdServer<E, F>
where
    E: Executor<
        Box<
            Future<Item = (), Error = ()>
                + Send
                + 'static,
        >,
    >
        + 'static,
    F: FromStr
        + Add<Output = F>
        + AddAssign
        + Sub<Output = F>
        + SubAssign
        + Div<Output = F>
        + Clone
        + Copy
        + PartialOrd
        + PartialEq
        + Into<f64>
        + Sync
        + Send
        + Debug
        + 'static,
{
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = ()>>;

    fn into_future(self) -> Self::Future {

        let Self {
            socket,
            handle,
            executor,
            cache,
            buf,
        } = self;

        let newbuf = buf.clone();
        let newcache = cache.clone();
        let newhandle = handle.clone();

        use futures::sync::oneshot::channel;
        let (tx, rx) = channel();

        let future = socket
            .recv_dgram(buf)
            .map_err(|e| println!("error receiving UDP packet {:?}", e))
            .and_then(move |(socket, data, size, addr)| {
                let future = lazy(move || {
                    let mut input: &[u8] = &data[0..size];
                    let mut size_left = size;
                    let mut parser = metric_parser::<F>();
                    loop {

                        let len = match parser.parse_stream_consumed(&mut input) {
                            FastResult::ConsumedOk(((name, metric), rest)) => {
                                INGRESS.fetch_add(1, Ordering::Relaxed);
                                size_left -= rest.len();
                                if size_left == 0 {
                                    break;
                                }
                                input = rest;
                                let metric = metric.clone();
                                cache.alter(name, |old| {
                                    //
                                    match old {
                                        None => Some(metric),
                                        Some(mut old) => {
                                            let res = old.aggregate(metric);
                                            if res.is_err() {
                                                AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Some(old)
                                        }
                                    }
                                });
                            }
                            FastResult::EmptyOk(_) |
                            FastResult::EmptyErr(_) => {
                                break;
                            }
                            FastResult::ConsumedErr(e) => {
                                //println!("PARSE ERR {:?}", e);
                                PARSE_ERRORS.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        };
                    }
                    tx.send(())?;
                    Ok(())
                });
                handle.spawn(
                    rx.map_err(move |_| println!("counting canceled"))
                        .and_then(move |_| {
                            StatsdServer::new(
                                socket,
                                &newhandle,
                                newhandle.remote().clone(),
                                newcache,
                                newbuf,
                            ).into_future()
                        }),
                );
                executor.execute(Box::new(future)).map_err(|e| {
                    println!("error spawning parser {:?}", e)
                })
            });
        Box::new(future)
    }
}

pub struct CarCodec;
impl Decoder for CarCodec {
    type Item = ();
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        unreachable!()
    }
}

impl Encoder for CarCodec {
    type Item = (String, String, String); // Metric name, suffix value and timestamp
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

        EGRESS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
// 3rd field is an already converted timestamp - unix time in seconds
pub type OutMetric<F: Display> = Arc<(String, Metric<F>, String)>;

pub struct CarbonCodec<F> {
    _p: PhantomData<F>,
}

impl<F> Default for CarbonCodec<F> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<F> Decoder for CarbonCodec<F> {
    type Item = ();
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        unreachable!()
    }
}

impl<F> Encoder for CarbonCodec<F>
where
    F: Display + Copy + PartialEq + Debug,
{
    type Item = OutMetric<F>; // Metric name, suffix value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let &(ref prefix, ref value, _) = &*m;

        let formatted = format!("{}", value.value);

        let len = (&*m.0).len() + prefix.len() + 1 + formatted.len() + 1 + &m.2.len() + 1;
        buf.reserve(len);
        buf.put(&*m.0);
        buf.put(prefix);
        buf.put(" ");

        buf.put(formatted + " ");
        buf.put(&m.2);
        buf.put("\n");

        EGRESS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
