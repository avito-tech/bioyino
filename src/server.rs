use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use futures3::channel::mpsc;
use futures3::SinkExt;
use tokio2::net::UdpSocket;

use slog::{error, o, warn, Logger};

use crate::config::System;
use crate::s;
use crate::stats::STATS;
use crate::task::Task;

pub async fn async_statsd_server(
    log: Logger,
    socket: std::net::UdpSocket,
    mut chans: Vec<mpsc::Sender<Task>>,
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
    let mut socket = UdpSocket::from_std(socket).expect("adding socket to event loop");
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
        buf.put_slice(&readbuf);

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
                chan.send(Task::Parse(ahash, buf)).await.unwrap_or_else(|_| s!(drops));
            }
        }
    }
}
