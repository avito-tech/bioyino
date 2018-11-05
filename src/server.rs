use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};
use std::net::SocketAddr;

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
    bufmap: HashMap<SocketAddr, BytesMut>,
    buffer_flush_length: usize,
    bufsize: usize,
    recv_counter: usize,
    next: usize,
    readbuf: BytesMut,
    flush_flags: Arc<Vec<AtomicBool>>,
    thread_idx: usize,
}

impl StatsdServer {
    pub fn new(
        socket: UdpSocket,
        chans: Vec<mpsc::Sender<Task>>,
        bufmap: HashMap<SocketAddr, BytesMut>,
        buffer_flush_length: usize,
        bufsize: usize,
        recv_counter: usize,
        next: usize,
        readbuf: BytesMut,
        flush_flags: Arc<Vec<AtomicBool>>,
        thread_idx: usize,
        ) -> Self {
        Self {
            socket,
            chans,
            bufmap,
            buffer_flush_length,
            bufsize,
            recv_counter,
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
            mut bufmap,
            buffer_flush_length,
            bufsize,
            mut recv_counter,
            next,
            readbuf,
            flush_flags,
            thread_idx,
        } = self;

        let future = socket
            .recv_dgram(readbuf)
            .map_err(|e| println!("error receiving UDP packet {:?}", e))
            .and_then(move |(socket, received, size, addr)| {
                INGRESS.fetch_add(1, Ordering::Relaxed);
                if size == 0 {
                    return Ok(());
                }

                {
                    let buf = bufmap.entry(addr).or_insert(BytesMut::with_capacity(buffer_flush_length));
                    recv_counter += size;
                    buf.put(&received[0..size]);
                }

                let flush = flush_flags
                    .get(thread_idx)
                    .unwrap()
                    .swap(false, Ordering::SeqCst);
                if recv_counter >= buffer_flush_length || flush {

                    bufmap.drain().map(|(addr, buf)|{
                        let mut hasher = DefaultHasher::new();
                        addr.hash(&mut hasher);
                        let ahash = hasher.finish();
                        let (chan, next) = if next >= chans.len() {
                            (chans[0].clone(), 1)
                        } else {
                            (chans[next].clone(), next + 1)
                        };

                        spawn(
                            chan.send(Task::Parse(ahash, buf.freeze()))
                            .map_err(|_| {
                                DROPS.fetch_add(1, Ordering::Relaxed);
                            }).map(|_|()))
                    }).last();

                    spawn(StatsdServer::new(
                            socket,
                            chans,
                            bufmap,
                            buffer_flush_length,
                            bufsize,
                            0,
                            next,
                            received,
                            flush_flags,
                            thread_idx,
                            ).into_future()
                    );
                } else {
                    spawn(
                        StatsdServer::new(
                            socket,
                            chans,
                            bufmap,
                            buffer_flush_length,
                            bufsize,
                            recv_counter,
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
