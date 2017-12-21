//use std::ops::{Add, Sub, AddAssign, SubAssign, Div};
//use std::fmt::Debug;
//use std::fmt::Display;

use {INGRESS, EGRESS, DROPS};

use bytes::{BytesMut, BufMut};
use tokio_core::reactor::Handle;
use tokio_core::net::UdpSocket;
use tokio_io::codec::{Encoder, Decoder};
use futures::{IntoFuture, Future, Sink};
use futures::sync::mpsc;

use std::sync::atomic::Ordering;
use failure::Error;

use task::Task;

#[derive(Debug)]
//pub struct StatsdServer<E, F>
//pub struct StatsdServer<E>
pub struct StatsdServer {
    socket: UdpSocket,
    handle: Handle,
    //executor: E,
    //cache: Arc<CHashMap<String, Metric<F>>>,
    //cache: Arc<Mutex<HashMap<String, Metric<f64>>>>,
    chans: Vec<mpsc::Sender<Task>>,
    buf: BytesMut,
    buf_queue_size: usize,
    bufsize: usize,
    next: usize,
    readbuf: BytesMut,
}

//impl<E, F> StatsdServer<E, F>
impl StatsdServer {
    pub fn new(
        socket: UdpSocket,
        handle: &Handle,
        chans: Vec<mpsc::Sender<Task>>,
        buf: BytesMut,
        buf_queue_size: usize,
        bufsize: usize,
        next: usize,
        readbuf: BytesMut,
    ) -> Self {
        Self {
            socket,
            handle: handle.clone(),
            chans,
            buf,
            buf_queue_size,
            bufsize,
            next,
            readbuf,
        }
    }
}

//impl<E, F> IntoFuture for StatsdServer<E, F>
//impl<E> IntoFuture for StatsdServer<E>
impl IntoFuture for StatsdServer {
    /*
       where
    //E: Executor<Box<Future<Item = (), Error = ()> + Send + 'static>>
    //+ Clone
    //+ 'static,
    //F: FromStr
    //+ Add<Output = F>
    //+ AddAssign
    //+ Sub<Output = F>
    //+ SubAssign
    //+ Div<Output = F>
    //+ Clone
    //+ Copy
    //+ PartialOrd
    //+ PartialEq
    //+ Into<f64>
    //+ Sync
    //+ Send
    //+ Debug
    //    + 'static,
    */
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = ()>>;

    fn into_future(self) -> Self::Future {
        let Self {
            socket,
            handle,
            chans,
            mut buf,
            buf_queue_size,
            bufsize,
            next,
            readbuf,
        } = self;

        let newhandle = handle.clone();
        let future = socket
            .recv_dgram(readbuf)
            .map_err(|e| println!("error receiving UDP packet {:?}", e))
            .and_then(move |(socket, received, size, _addr)| {
                INGRESS.fetch_add(1, Ordering::Relaxed);
                if size == 0 {
                    return Ok(());
                }

                buf.put(&received[0..size]);

                if buf.remaining_mut() < bufsize {
                    let (chan, next) = if next >= chans.len() {
                        (chans[0].clone(), 1)
                    } else {
                        (chans[next].clone(), next + 1)
                    };
                    let newbuf = BytesMut::with_capacity(buf_queue_size * bufsize);
                    handle.spawn(
                        chan.send(Task::Parse(buf.freeze()))
                            .map_err(|_| { DROPS.fetch_add(1, Ordering::Relaxed); })
                            .and_then(move |_| {
                                StatsdServer::new(
                                    socket,
                                    &newhandle,
                                    chans,
                                    newbuf,
                                    buf_queue_size,
                                    bufsize,
                                    next,
                                    received,
                                ).into_future()
                            }),
                    );
                } else {
                    handle.spawn(
                        StatsdServer::new(
                            socket,
                            &newhandle,
                            chans,
                            buf,
                            buf_queue_size,
                            bufsize,
                            next,
                            received,
                        ).into_future(),
                    );
                }
                Ok(())
            });
        Box::new(future)
    }
}

pub struct CarbonCodec;
impl Decoder for CarbonCodec {
    type Item = ();
    type Error = Error;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        unreachable!()
    }
}

impl Encoder for CarbonCodec {
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
