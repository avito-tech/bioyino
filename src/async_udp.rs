use std::{collections::hash_map::DefaultHasher, sync::atomic::AtomicBool};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::io;

use bytes::{BufMut, BytesMut};
use crossbeam_channel::Sender;
use futures::future::{pending, select};
use tokio::{net::UdpSocket, sync::Notify};
use tokio::runtime::Builder;
use socket2::{Domain, Protocol, Socket, Type};

use slog::{ error, o, warn, Logger};

use crate::config::System;
use crate::s;
use crate::stats::STATS;
use crate::fast_task::FastTask;
use crate::errors::GeneralError;

pub(crate) fn start_async_udp(
    log: Logger, chans: &[Sender<FastTask>], config: Arc<System>
) -> Arc<Notify> {


    let flush_notify_ret = Arc::new(Notify::new());

    let chans = chans.to_vec();

    let flush_notify = flush_notify_ret.clone();
    // to provide a multithreading listerning of multiple sockets, we start a separate
    // tokio runtime with a separate threadpool of `n_threads`
    std::thread::Builder::new()
        .name("bioyino_udp".into())
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .thread_name("bioyino_audp")
                .worker_threads(config.n_threads)
                .enable_all()
                .build()
                .expect("creating runtime for async UDP threads");

            runtime.spawn(async move {
                // Create a pool of listener sockets
                for _ in 0..config.network.async_sockets {
                    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).expect("creating UDP socket");
                    socket.set_reuse_address(true).expect("reusing address");
                    socket.set_reuse_port(true).expect("reusing port");
                    socket.set_nonblocking(true).expect("setting nonblocking");
                    socket.bind(&config.network.listen.into()).expect("binding");

                    let socket = UdpSocket::from_std(socket.into()).expect("adding socket to event loop");
                    let socket = Arc::new(socket);

                    // for every single socket we want a number of green threads (specified by `greens`) listening for it
                    for _ in 0..config.network.greens {

                        let config = config.clone();
                        let log = log.clone();
                        let flush_notify = flush_notify.clone();
                        let chans = chans.clone();
                        let socket = socket.clone();
                        let worker = AsyncUdpWorker::new(
                            log,
                            chans,
                            config,
                            socket,
                            flush_notify,
                        );

                        tokio::spawn(worker.run());
                    }
                }
            });

            runtime.block_on(pending::<()>())
        })
    .expect("starting thread for async UDP threadpool");

    flush_notify_ret
}

struct AsyncUdpWorker {
    log: Logger,
    chans: Vec<Sender<FastTask>>,
    config: Arc<System>,
    socket: Arc<UdpSocket>,
    flush: Arc<Notify>,
}

impl AsyncUdpWorker {

    fn new(
        log: Logger,
        chans: Vec<Sender<FastTask>>,
        config: Arc<System>,
        socket: Arc<UdpSocket>,
        flush: Arc<Notify>,
    ) -> Self {

        Self {
            log,
            chans,
            config,
            socket,
            flush,
        }
    }

    async fn run(
        self,
    ) -> Result<(), GeneralError> {

        let Self {
            log,
            mut chans,
            config,
            socket,
            flush,
        } = self;
        let mut  bufmap = HashMap::new();
        let bufsize = config.network.bufsize;
        let mut readbuf = Vec::with_capacity(bufsize);
        readbuf.resize(bufsize, 0);
        let log = log.new(o!("source"=>"async_udp_worker"));

        let do_flush = AtomicBool::new(false);
        let mut next_chan = config.p_threads;
        let mut recv_counter = 0usize;

        loop {

            let f1 = async {
                socket.readable().await
            };

            let f2 = async {
                flush.notified().await;
                do_flush.swap(true, Ordering::Relaxed);
            };

            futures::pin_mut!(f1);
            futures::pin_mut!(f2);

            select(f1, f2).await;

            loop {
                // read everything from socket while it's readable
                let (size, addr) = match socket.try_recv_from(&mut readbuf) {
                    Ok((0, _)) => {
                        // size = 0 means EOF
                        warn!(log, "exiting on EOF");
                        return Ok(())
                    }
                    Ok((size, addr)) => (size, addr),
                    Err(e)  if e.kind() == io::ErrorKind::WouldBlock => {
                        //continue
                        break
                            //return Ok(())
                    }
                    Err(e) => {
                        error!(log, "error reading UDP socket {:?}", e);
                        return Err(GeneralError::Io(e))
                    }
                };

                // we only get here in case of success
                STATS.ingress.fetch_add(size, Ordering::Relaxed);

                let buf = bufmap
                    .entry(addr)
                    .or_insert_with(|| BytesMut::with_capacity(config.network.buffer_flush_length));
                recv_counter += size;
                // check we can fit the buffer
                if buf.remaining_mut() < config.network.bufsize {
                    buf.reserve(size + 1)
                }
                buf.put_slice(&readbuf[..size]);

                let flush = do_flush.swap(false, Ordering::Relaxed);
                if recv_counter >= config.network.buffer_flush_length || flush {
                    for (addr, buf) in bufmap.drain() {
                        let mut hasher = DefaultHasher::new();
                        addr.hash(&mut hasher);
                        let ahash = hasher.finish();
                        let chan = if config.metrics.consistent_parsing {
                            let chlen = chans.len();
                            &mut chans[ahash as usize % chlen]
                        } else {
                            if next_chan >= chans.len() {
                                next_chan = 0;
                            }
                            let chan = &mut chans[next_chan];
                            next_chan += 1;
                            chan
                        };
                        chan.send(FastTask::Parse(ahash, buf)).unwrap_or_else(|_| s!(drops));
                    }
                }
            }
        }
    }
}
