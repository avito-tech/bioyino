use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use crossbeam_channel::Sender;
use futures::future::pending;
use tokio::net::UdpSocket;
use tokio::runtime::Builder;
use socket2::{Domain, Protocol, Socket, Type};

use slog::{info, error, o, warn, Logger};

use crate::config::System;
use crate::s;
use crate::stats::STATS;
use crate::fast_task::FastTask;

pub(crate) fn start_async_udp(
    log: Logger,
    listen: SocketAddr,
    chans: &[Sender<FastTask>],
    config: Arc<System>,
    n_threads: usize,
    greens: usize,
    async_sockets: usize,
    bufsize: usize,
    flush_flags: Arc<Vec<AtomicBool>>,
) {
    info!(log, "multimessage is disabled, starting in async UDP mode");

    // Create a pool of listener sockets
    let mut sockets = Vec::new();
    for _ in 0..async_sockets {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).expect("creating UDP socket");
        socket.set_reuse_address(true).expect("reusing address");
        socket.set_reuse_port(true).expect("reusing port");
        socket.bind(&listen.into()).expect("binding");

        sockets.push(socket);
    }

    for i in 0..n_threads {
        // Each thread gets the clone of a socket pool
        let sockets = sockets.iter().map(|s| s.try_clone().unwrap()).collect::<Vec<_>>();

        let chans = chans.to_owned();
        let flush_flags = flush_flags.clone();
        let config = config.clone();
        let log = log.clone();
        std::thread::Builder::new()
            .name(format!("bioyino_audp{}", i))
            .spawn(move || {
                // each thread runs it's own runtime
                let runtime = Builder::new_current_thread().enable_all().build().expect("creating runtime for test");
                // Inside each green thread
                for _ in 0..greens {
                    // start a listener for all sockets
                    for socket in sockets.iter() {
                        let chans = chans.clone();
                        // create UDP listener
                        let socket = socket.try_clone().expect("cloning socket");

                        runtime.spawn(async_statsd_server(
                                log.clone(),
                                socket.into(),
                                chans.clone(),
                                config.clone(),
                                bufsize,
                                i,
                                flush_flags.clone(),
                        ));
                    }
                }

                runtime.block_on(pending::<()>())
            })
        .expect("creating UDP reader thread");
        }
}

pub async fn async_statsd_server(
    log: Logger,
    socket: std::net::UdpSocket,
    mut chans: Vec<Sender<FastTask>>,
    config: Arc<System>,
    bufsize: usize,
    thread_idx: usize,
    flush_flags: Arc<Vec<AtomicBool>>,
) {
    let mut bufmap = HashMap::new();
    let mut readbuf = Vec::with_capacity(bufsize);
    readbuf.resize(bufsize, 0);
    let mut recv_counter = 0;
    let mut next = thread_idx;
    let log = log.new(o!("source"=>"async_udp_server", "tid"=>thread_idx));
    let socket = UdpSocket::from_std(socket).expect("adding socket to event loop");
    loop {
        let (size, addr) = match socket.recv_from(&mut readbuf).await {
            Ok((0, _)) => {
                // size = 0 means EOF
                warn!(log, "exiting on EOF");
                break;
            }
            Ok((size, addr)) => (size, addr),
            Err(e) => {
                error!(log, "error receiving UDP packet {:?}", e);
                break;
            }
        };
        // we only get here in case of success
        STATS.ingress.fetch_add(size, Ordering::Relaxed);

        let buf = bufmap
            .entry(addr)
            .or_insert_with(|| BytesMut::with_capacity(config.network.buffer_flush_length));
        recv_counter += size;
        // check we can fit the buffer
        if buf.remaining_mut() < bufsize {
            buf.reserve(size + 1)
        }
        buf.put_slice(&readbuf[..size]);

        let flush = flush_flags.get(thread_idx).unwrap().swap(false, Ordering::SeqCst);
        if recv_counter >= config.network.buffer_flush_length || flush {
            for (addr, buf) in bufmap.drain() {
                let mut hasher = DefaultHasher::new();
                addr.hash(&mut hasher);
                let ahash = hasher.finish();
                let chan = if config.metrics.consistent_parsing {
                    let chlen = chans.len();
                    &mut chans[ahash as usize % chlen]
                } else {
                    if next >= chans.len() {
                        next = 0;
                    }
                    let chan = &mut chans[next];
                    next += 1;
                    chan
                };
                chan.send(FastTask::Parse(ahash, buf)).unwrap_or_else(|_| s!(drops));
            }
        }
    }
}
