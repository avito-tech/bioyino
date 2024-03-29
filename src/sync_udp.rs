use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::io;
use std::net::SocketAddr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use bytes::{BufMut, BytesMut};
use crossbeam_channel::Sender;
use slog::{info, o, warn, Logger};
use socket2::{Domain, Protocol, Socket, Type};
use std::os::unix::io::AsRawFd;

use crate::config::System;
use crate::fast_task::FastTask;
use crate::stats::STATS;

pub(crate) fn start_sync_udp(
    log: Logger,
    listen: SocketAddr,
    chans: &[Sender<FastTask>],
    config: Arc<System>,
    n_threads: usize,
    bufsize: usize,
    mm_packets: usize,
    mm_async: bool,
    mm_timeout: u64,
) -> Arc<Vec<AtomicBool>> {
    info!(log, "multimessage enabled, starting in sync UDP mode"; "socket-is-blocking"=>!mm_async, "packets"=>mm_packets);

    // It is crucial for recvmmsg to have one socket per many threads
    // to avoid drops because at lease two threads have to work on socket
    // simultaneously
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).expect("creating UDP socket");
    socket.set_reuse_address(true).expect("reusing address");
    socket.set_reuse_port(true).expect("reusing port");
    socket.set_nonblocking(mm_async).expect("setting O_NONBLOCK option");
    socket.bind(&listen.into()).expect("binding");

    let mm_timeout = if mm_timeout == 0 { config.network.buffer_flush_time } else { mm_timeout };

    // For each out sync thread we create the buffer flush timer, that sets the atomic value to 1
    // every interval
    let mut flush_flags = Vec::new();

    for _ in 0..n_threads {
        flush_flags.push(AtomicBool::new(false));
    }

    let flush_flags = Arc::new(flush_flags);

    for i in 0..n_threads {
        let chans = chans.to_owned();
        let log = log.new(o!("source"=>"mudp_thread"));

        let sck = socket.try_clone().unwrap();
        let flush_flags = flush_flags.clone();
        let config = config.clone();
        thread::Builder::new()
            .name(format!("bioyino_mudp{}", i))
            .spawn(move || {
                let fd = sck.as_raw_fd();
                {
                    // <--- this limits the use of `use::libc::*` scope
                    use libc::*;

                    let chlen = chans.len();
                    let mut next_chan = chlen - 1;

                    // store mmsghdr array so Rust won't free it's memory
                    let mut mheaders: Vec<mmsghdr> = Vec::with_capacity(mm_packets);

                    // allocate space for address information
                    // addr len is 16 bytes for ipv6 address + 4 bytes for port totalling 20 bytes
                    // the structure is only used for advanced information and not copied anywhere
                    // so it can be initialized before main cycle
                    let mut addrs: Vec<[u8; 20]> = Vec::with_capacity(mm_packets);
                    addrs.resize(mm_packets, [0; 20]);

                    //for i in 0..mm_packets { // clippy said iterator may be faster
                    for addr in addrs.iter_mut().take(mm_packets) {
                        let m = mmsghdr {
                            msg_hdr: msghdr {
                                //msg_name: addrs[i].as_mut_ptr() as *mut c_void,
                                msg_name: addr.as_mut_ptr() as *mut c_void,
                                msg_namelen: 20 as socklen_t,
                                msg_iov: null_mut(),     // this will change later
                                msg_iovlen: 0,           // this will change later
                                msg_control: null_mut(), // we won't need this
                                msg_controllen: 0,       // and this
                                msg_flags: 0,            // and of couse this
                            },
                            msg_len: 0,
                        };
                        mheaders.push(m);
                    }

                    let mhptr = mheaders.as_mut_ptr();
                    let mhlen = mheaders.len();

                    let flags = if mm_async { MSG_WAITFORONE } else { 0 };

                    // this will store resulting per-source buffers
                    let mut bufmap = HashMap::new();
                    let mut total_received = 0;
                    let min_bytes = mm_packets * mm_packets * bufsize;
                    let rowsize = bufsize * mm_packets;

                    let mut recv_buffer = Vec::new();
                    recv_buffer.reserve_exact(min_bytes);

                    // prepare scatter-gather buffers (iovecs)
                    // We allocate (mm_packets x mm_packets*bufsize) matrix to guarantee fitting of all
                    // the messages into memory. For doing this se have to consider 2 edge cases here.
                    // We know that recvmmsg places all messages from a single source to
                    // the same iovecs bucket. That is the first case is when all data come from
                    // single address, so we will have row filled with bytes. The second case is when
                    // all data come from different addresses, so buffers are filled in
                    // columns. The default value - 1500 does not consider IP fragmentation here, so the ideal
                    // value would be maximum IP packet size (~ 65535 - 8 = 65507), but this is the
                    // rare case in modern networks, at least at datacenters, which are our
                    // main use case.

                    recv_buffer.resize(min_bytes, 0);
                    // we don't want rust to forget about intermediate iovecs so we put them into
                    // separate vector
                    let mut chunks = Vec::with_capacity(mm_packets);

                    for i in 0..mm_packets {
                        let chunk = iovec {
                            iov_base: recv_buffer[i * rowsize..i * rowsize + rowsize].as_mut_ptr() as *mut c_void,
                            iov_len: rowsize,
                        };
                        chunks.push(chunk);
                        // put the result to mheaders
                        mheaders[i].msg_hdr.msg_iov = &mut chunks[i];
                        mheaders[i].msg_hdr.msg_iovlen = 1;
                    }

                    loop {
                        // timeout is mutable and changed by every recvmmsg call, so it MUST be inside loop
                        // creating timeout as &mut fails because it's supposedly not dropped
                        let mut timeout = if mm_timeout > 0 {
                            timespec {
                                tv_sec: (mm_timeout / 1000u64) as i64,
                                tv_nsec: ((mm_timeout % 1000u64) * 1_000_000u64) as i64,
                            }
                        } else {
                            timespec { tv_sec: 0, tv_nsec: 0 }
                        };

                        let res = unsafe {
                            recvmmsg(
                                fd as c_int,
                                mhptr,
                                mhlen as c_uint,
                                flags,
                                if mm_timeout > 0 { &mut timeout } else { null_mut() },
                            )
                        };

                        if res == 0 {
                            // skip this shit
                        } else if res > 0 {
                            let messages = res as usize;
                            // we've received some messages
                            for i in 0..messages {
                                let mlen = mheaders[i].msg_len as usize;
                                total_received += mlen;

                                STATS.ingress.fetch_add(mlen, Ordering::Relaxed);

                                // create address entry in messagemap
                                let entry = bufmap.entry(addrs[i]).or_insert_with(|| BytesMut::with_capacity(mlen));

                                // check we can fit the buffer
                                if entry.remaining_mut() < mlen + 1 {
                                    entry.reserve(mlen)
                                }

                                // and put the buffer into the map
                                entry.put(&recv_buffer[i * rowsize..i * rowsize + mlen]);

                                // reset address to be used in next cycle
                                addrs[i] = [0; 20];
                                mheaders[i].msg_hdr.msg_namelen = 20;
                            }

                            // when it's time to send bytes, send them
                            let flush = flush_flags.get(i).unwrap().swap(false, Ordering::SeqCst);
                            if flush || total_received >= config.network.buffer_flush_length {
                                total_received = 0;
                                bufmap
                                    .drain()
                                    .map(|(addr, mut buf)| {
                                        // in some ideal world we want all values from the same host to be parsed by the
                                        // same thread, but this could cause load unbalancing between threads in some
                                        // corner cases, i.e. when only few hosts are sending most
                                        // of the metrics
                                        // TODO: we can work this around by fast-scanning buffer
                                        // for newlines. if more than 2 newlines are there, buffer
                                        // can be split into 3 parts and the middle part can be
                                        // cropped from the buffer and relatively safely given to other nodes for
                                        // parsing. It would be WAY better to do this in counting
                                        // nodes rather than networking ones, but could be harder
                                        // than it seems because of queue reordering
                                        let mut hasher = DefaultHasher::new();
                                        hasher.write(&addr);
                                        let ahash = hasher.finish();

                                        let chan = if config.metrics.consistent_parsing {
                                            &chans[ahash as usize % chlen]
                                        } else {
                                            next_chan = if next_chan >= (chlen - 1) { 0 } else { next_chan + 1 };
                                            &chans[next_chan]
                                        };
                                        let buf = buf.split();
                                        let buflen = buf.len();
                                        chan.try_send(FastTask::Parse(ahash, buf))
                                            .map_err(|_| {
                                                STATS.drops.fetch_add(buflen, Ordering::Relaxed);
                                            })
                                            .unwrap_or(());
                                    })
                                    .last();
                            }
                        } else {
                            let errno = unsafe { *__errno_location() };
                            if errno == EAGAIN {
                            } else {
                                warn!(log, "UDP receive error";
                                    "code"=> format!("{}", res),
                                    "error"=>format!("{}", io::Error::last_os_error())
                                );
                            }
                        }
                    }
                }
            })
            .expect("starting multimsg thread");
    }
    flush_flags
}
