use std::io;
use std::net::SocketAddr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use server::StatsdServer;
use task::Task;
use {DROPS, INGRESS};

use bytes::{BufMut, BytesMut};
use futures::future::empty;
use futures::sync::mpsc::Sender;
use futures::IntoFuture;
use net2::unix::UnixUdpBuilderExt;
use net2::UdpBuilder;
use slog::Logger;
use std::os::unix::io::AsRawFd;
use tokio::net::UdpSocket;
use tokio::runtime::current_thread::Runtime;

pub(crate) fn start_sync_udp(
    log: Logger,
    listen: SocketAddr,
    chans: &Vec<Sender<Task>>,
    n_threads: usize,
    bufsize: usize,
    mm_packets: usize,
    mm_async: bool,
    mm_timeout: u64,
    buffer_flush_time: u64,
    buffer_flush_length: usize,
    flush_flags: Arc<Vec<AtomicBool>>,
) {
    info!(log, "multimessage enabled, starting in sync UDP mode"; "socket-is-blocking"=>mm_async, "packets"=>mm_packets);

    // It is crucial for recvmmsg to have one socket per many threads
    // to avoid drops because at lease two threads have to work on socket
    // simultaneously
    let socket = UdpBuilder::new_v4().unwrap();
    socket.reuse_address(true).unwrap();
    socket.reuse_port(true).unwrap();
    let sck = socket.bind(listen).unwrap();
    sck.set_nonblocking(mm_async).unwrap();

    let mm_timeout = if mm_timeout == 0 {
        buffer_flush_time
    } else {
        mm_timeout
    };

    for i in 0..n_threads {
        let chans = chans.clone();
        let log = log.new(o!("source"=>"mudp_thread"));

        let sck = sck.try_clone().unwrap();
        let flush_flags = flush_flags.clone();
        thread::Builder::new()
            .name(format!("bioyino_mudp{}", i).into())
            .spawn(move || {
                let fd = sck.as_raw_fd();
                {
                    // <--- this limits `use::libc::*` scope
                    use libc::*;

                    let mut ichans = chans.iter().cycle();

                    // a vector to avoid dropping iovec structures
                    let mut iovecs = Vec::with_capacity(mm_packets);

                    // a vector to avoid dropping message buffers
                    let mut message_vec = Vec::new();

                    let mut v: Vec<mmsghdr> = Vec::with_capacity(mm_packets);
                    for _ in 0..mm_packets {
                        let mut buf = Vec::with_capacity(bufsize);
                        buf.resize(bufsize, 0);

                        let mut iov = Vec::with_capacity(1);
                        iov.resize(
                            1,
                            iovec {
                                iov_base: buf.as_mut_ptr() as *mut c_void,
                                iov_len: bufsize as size_t,
                            },
                        );
                        let m = mmsghdr {
                            msg_hdr: msghdr {
                                msg_name: null_mut(),
                                msg_namelen: 0 as socklen_t,
                                msg_iov: iov.as_mut_ptr(),
                                msg_iovlen: iov.len() as size_t,
                                msg_control: null_mut(),
                                msg_controllen: 0,
                                msg_flags: 0,
                            },
                            msg_len: 0,
                        };
                        v.push(m);
                        iovecs.push(iov);
                        message_vec.push(buf);
                    }

                    let vp = v.as_mut_ptr();
                    let vlen = v.len();

                    // This is the buffer we fill with metrics and periodically send to tasks
                    // Due to implementation of BytesMut it's real size is bigger, so for even less
                    // allocations, we split the filled part and leave the rest for future bytes
                    let mut b = BytesMut::with_capacity(buffer_flush_length);

                    let flags = if mm_async { MSG_WAITFORONE } else { 0 };

                    loop {
                        // timeout is mutable and changed by every recvmmsg call, so it MUST be inside loop
                        // creating timeout as &mut fails because it's supposedly not dropped
                        let mut timeout = if mm_timeout > 0 {
                            timespec {
                                tv_sec: (mm_timeout / 1000u64) as i64,
                                tv_nsec: ((mm_timeout % 1000u64) * 1_000_000u64) as i64,
                            }
                        } else {
                            timespec {
                                tv_sec: 0,
                                tv_nsec: 0,
                            }
                        };

                        let res =
                            unsafe { recvmmsg(fd as c_int, vp, vlen as c_uint, flags, if mm_timeout > 0 {&mut timeout} else {null_mut()}) };

                        if res == 0 {
                            // skip this shit
                        } else if res > 0 {
                            let messages = res as usize;
                            // Check if we can fit all packets into buffer
                            let mut total_bytes = 0;
                            for i in 0..messages {
                                total_bytes += v[i].msg_len as usize;
                            }

                            // if we cannot, allocate more
                            if b.remaining_mut() < total_bytes {
                                b.reserve(buffer_flush_length)
                            }

                            // put packets into buffer
                            for i in 0..messages {
                                let len = v[i].msg_len as usize;
                                b.put(&message_vec[i][0..len]);
                            }

                            // when it's time to send bytes, send them
                            let flush = flush_flags.get(i).unwrap().swap(false, Ordering::SeqCst);
                            if flush || b.len() >= buffer_flush_length {
                                let mut chan = ichans.next().unwrap().clone();
                                INGRESS.fetch_add(messages as usize, Ordering::Relaxed);
                                chan.try_send(Task::Parse(b.take().freeze()))
                                    .map_err(|_| {
                                        warn!(log, "error sending buffer(queue full?)");
                                        DROPS.fetch_add(messages as usize, Ordering::Relaxed);
                                    }).unwrap_or(());
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
            }).expect("starting multimsg thread");
    }
}

pub(crate) fn start_async_udp(
    log: Logger,
    listen: SocketAddr,
    chans: &Vec<Sender<Task>>,
    n_threads: usize,
    greens: usize,
    async_sockets: usize,
    bufsize: usize,
    buffer_flush_length: usize,
    flush_flags: Arc<Vec<AtomicBool>>,
) {
    info!(log, "multimessage is disabled, starting in async UDP mode");

    // Create a pool of listener sockets
    let mut sockets = Vec::new();
    for _ in 0..async_sockets {
        let socket = UdpBuilder::new_v4().unwrap();
        socket.reuse_address(true).unwrap();
        socket.reuse_port(true).unwrap();
        let socket = socket.bind(&listen).unwrap();
        sockets.push(socket);
    }

    for i in 0..n_threads {
        // Each thread gets the clone of a socket pool
        let sockets = sockets
            .iter()
            .map(|s| s.try_clone().unwrap())
            .collect::<Vec<_>>();

        let chans = chans.clone();
        let flush_flags = flush_flags.clone();
        thread::Builder::new()
            .name(format!("bioyino_udp{}", i).into())
            .spawn(move || {
                // each thread runs it's own runtime
                let mut runtime = Runtime::new().expect("creating runtime for counting worker");

                // Inside each green thread
                for _ in 0..greens {
                    // start a listener for all sockets
                    for socket in sockets.iter() {
                        let buf = BytesMut::with_capacity(buffer_flush_length);

                        let mut readbuf = BytesMut::with_capacity(bufsize);
                        unsafe { readbuf.set_len(bufsize) }
                        let chans = chans.clone();
                        // create UDP listener
                        let socket = socket.try_clone().expect("cloning socket");
                        let socket =
                            UdpSocket::from_std(socket, &::tokio::reactor::Handle::current())
                                .expect("adding socket to event loop");

                        let server = StatsdServer::new(
                            socket,
                            chans.clone(),
                            buf,
                            buffer_flush_length,
                            bufsize,
                            i,
                            readbuf,
                            flush_flags.clone(),
                            i,
                        );

                        runtime.spawn(server.into_future());
                    }
                }

                runtime
                    .block_on(empty::<(), ()>())
                    .expect("starting runtime for async UDP");
            }).expect("creating UDP reader thread");
    }
}
