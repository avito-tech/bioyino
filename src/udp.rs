use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use std::ptr::null_mut;

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
    task_queue_size: usize,
    buffer_flush: u64,
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

                    // This is the buffer we fill with metrics and periodically send to
                    // tasks
                    // To avoid allocations we make it bigger than multimsg message count
                    // Also, it can be the huge value already allocated here, so for even less
                    // allocations, we split the filled part and leave the rest for future bytes
                    let mut b = BytesMut::with_capacity(bufsize * mm_packets * task_queue_size);

                    // We cannot count on allocator to allocate a value close to capacity
                    // it can be much more than that and stall our buffer for too long
                    // So we set our chunk size ourselves and send buffer when this value
                    // exhausted, but we only allocate when buffer becomes empty
                    let mut chunks = task_queue_size as isize;
                    let flags = if mm_async { MSG_WAITFORONE } else { 0 };
                    loop {
                        let timeout = if buffer_flush > 0 {
                            &mut timespec {
                                tv_sec: buffer_flush as i64 / 1000,
                                tv_nsec: (buffer_flush as i64 % 1000) * 1_000_000,
                            }
                        } else {
                            null_mut()
                        };

                        let res =
                            unsafe { recvmmsg(fd as c_int, vp, vlen as c_uint, flags, timeout) };

                        if res >= 0 {
                            let end = res as usize;

                            // Check if we can fit all packets into buffer
                            let mut total_bytes = 0;
                            for i in 0..end {
                                total_bytes += v[i].msg_len as usize;
                            }
                            // newlines
                            total_bytes += end - 1;

                            // if we cannot, allocate more
                            if b.remaining_mut() < total_bytes {
                                b.reserve(bufsize * mm_packets * task_queue_size)
                            }

                            // put packets into buffer
                            for i in 0..end {
                                let len = v[i].msg_len as usize;

                                b.put(&message_vec[i][0..len]);
                                chunks -= end as isize;
                            }

                            // when it's time to send bytes, send them
                            let flush = flush_flags.get(i).unwrap().swap(false, Ordering::SeqCst);
                            if chunks <= 0 || flush {
                                let mut chan = ichans.next().unwrap().clone();
                                INGRESS.fetch_add(res as usize, Ordering::Relaxed);
                                chan.try_send(Task::Parse(b.take().freeze()))
                                    .map_err(|_| {
                                        warn!(log, "error sending buffer(queue full?)");
                                        DROPS.fetch_add(res as usize, Ordering::Relaxed);
                                    }).unwrap_or(());
                                chunks = task_queue_size as isize;
                            }
                        } else {
                            let errno = unsafe { *__errno_location() };
                            if errno == EAGAIN {
                            } else {
                                warn!(log, "UDP receive error";
                                      "code"=> format!("{}",res),
                                      "error"=>format!("{}", io::Error::last_os_error())
                                )
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
    task_queue_size: usize,
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
                        let buf = BytesMut::with_capacity(task_queue_size * bufsize);

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
                            task_queue_size,
                            bufsize,
                            i,
                            readbuf,
                            task_queue_size * bufsize,
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
