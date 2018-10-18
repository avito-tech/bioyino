use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use {DROPS, INGRESS};

use bytes::{BufMut, BytesMut};
use futures::sync::mpsc;
use futures::{Future, IntoFuture, Sink};
use tokio::executor::current_thread::spawn;
use tokio::net::UdpSocket;

use task::Task;

#[derive(Debug)]
pub struct StatsdServer {
    socket: UdpSocket,
    chans: Vec<mpsc::Sender<Task>>,
    buf: BytesMut,
    buffer_flush_length: usize,
    bufsize: usize,
    next: usize,
    readbuf: BytesMut,
    flush_flags: Arc<Vec<AtomicBool>>,
    thread_idx: usize,
}

impl StatsdServer {
    pub fn new(
        socket: UdpSocket,
        chans: Vec<mpsc::Sender<Task>>,
        buf: BytesMut,
        buffer_flush_length: usize,
        bufsize: usize,
        next: usize,
        readbuf: BytesMut,
        flush_flags: Arc<Vec<AtomicBool>>,
        thread_idx: usize,
    ) -> Self {
        Self {
            socket,
            chans,
            buf,
            buffer_flush_length,
            bufsize,
            next,
            readbuf,
            flush_flags,
            thread_idx,
        }
    }
}

impl IntoFuture for StatsdServer {
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = ()>>;

    fn into_future(self) -> Self::Future {
        let Self {
            socket,
            chans,
            mut buf,
            buffer_flush_length,
            bufsize,
            next,
            readbuf,
            flush_flags,
            thread_idx,
        } = self;

        let future = socket
            .recv_dgram(readbuf)
            .map_err(|e| println!("error receiving UDP packet {:?}", e))
            .and_then(move |(socket, received, size, _addr)| {
                INGRESS.fetch_add(1, Ordering::Relaxed);
                if size == 0 {
                    return Ok(());
                }

                buf.put(&received[0..size]);

                let flush = flush_flags
                    .get(thread_idx)
                    .unwrap()
                    .swap(false, Ordering::SeqCst);
                if buf.remaining_mut() < bufsize || buf.len() >= buffer_flush_length || flush {
                    let (chan, next) = if next >= chans.len() {
                        (chans[0].clone(), 1)
                    } else {
                        (chans[next].clone(), next + 1)
                    };
                    let newbuf = BytesMut::with_capacity(buffer_flush_length);

                    spawn(
                        chan.send(Task::Parse(buf.freeze()))
                            .map_err(|_| {
                                DROPS.fetch_add(1, Ordering::Relaxed);
                            }).and_then(move |_| {
                                StatsdServer::new(
                                    socket,
                                    chans,
                                    newbuf,
                                    buffer_flush_length,
                                    bufsize,
                                    next,
                                    received,
                                    flush_flags,
                                    thread_idx,
                                ).into_future()
                            }),
                    );
                } else {
                    spawn(
                        StatsdServer::new(
                            socket,
                            chans,
                            buf,
                            buffer_flush_length,
                            bufsize,
                            next,
                            received,
                            flush_flags,
                            thread_idx,
                        ).into_future(),
                    );
                }
                Ok(())
            });
        Box::new(future)
    }
}
