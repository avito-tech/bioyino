//use std::ops::{Add, Sub, AddAssign, SubAssign, Div};
//use std::fmt::Debug;
//use std::fmt::Display;

use {EGRESS, DROPS};

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
    ) -> Self {
        Self {
            socket,
            handle: handle.clone(),
            chans,
            buf,
            buf_queue_size,
            bufsize,
            next,
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
        } = self;

        let newhandle = handle.clone();
        let newchans = chans.clone();

        if buf.remaining_mut() <= bufsize {
            buf.reserve(buf_queue_size * bufsize);
        }

        // leave one more byte for '\n'
        let mut readbuf = buf.split_to(bufsize + 1);
        // reading from UDP socket requires buffer to be preallocated
        unsafe { readbuf.advance_mut(bufsize) }
        let future = socket
            .recv_dgram(readbuf)
            .map_err(|e| println!("error receiving UDP packet {:?}", e))
            .and_then(move |(socket, mut received, size, _addr)| {
                if size == 0 {
                    return Ok(());
                }
                if received.last().unwrap() != &10u8 {
                    received.put("\n");
                }
                received.truncate(size);
                let (chan, next) = if next >= chans.len() {
                    (chans[0].clone(), 1)
                } else {
                    (chans[next].clone(), next + 1)
                };
                handle.spawn(
                    chan.send(Task::Parse(received))
                        .map_err(|_| { DROPS.fetch_add(1, Ordering::Relaxed); })
                        .and_then(move |_| {
                            StatsdServer::new(
                                socket,
                                &newhandle,
                                newchans,
                                buf,
                                buf_queue_size,
                                bufsize,
                                next,
                            ).into_future()
                        }),
                );
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

/*
pub struct PeerCodec {
    inbuf: BytesMut,
}

impl Decoder for PeerCodec {
    type Item = ();
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        use bytes::{IntoBuf, Buf};
        let buf = buf.clone();
        let deserialized: ::serde_json::Value = ::serde_json::from_reader(buf.into_buf().reader())
            .unwrap();
        println!("{:?}", deserialized);
        return Ok(None);
    }
}

impl Encoder for PeerCodec {
    type Item = (String, Metric<f64>);
    type Error = Error;

    fn encode(
        &mut self,
        (name, metric): Self::Item,
        buf: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let mut jmap = ::serde_json::Map::new();
        jmap.insert(name, ::serde_json::to_value(metric).unwrap());
        let serialized = ::serde_json::to_string(&jmap).expect("deserializing metric");
        buf.reserve(serialized.len());
        buf.put(serialized);

        //EGRESS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
*/
