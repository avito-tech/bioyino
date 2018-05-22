// General
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_term;

// Options
#[macro_use]
extern crate clap;
extern crate toml;

// Network
extern crate bytes;
#[macro_use]
extern crate futures;
extern crate hyper;
extern crate libc;
extern crate net2;
extern crate num_cpus;
extern crate resolve;
extern crate tokio_core;
extern crate tokio_io;

// Other
extern crate bincode;
extern crate combine;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate dtoa;
extern crate itoa;
extern crate serde_json;

pub mod bigint;
pub mod carbon;
pub mod config;
pub mod consul;
pub mod errors;
pub mod metric;
pub mod parser;
pub mod peer;
pub mod server;
pub mod task;
pub mod util;

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT};
use std::thread;
use std::time::{self, Duration, SystemTime};

use failure::Fail;
use slog::{Drain, Level};

use bytes::{Bytes, BytesMut};
use futures::future::{join_all, lazy, loop_fn, ok, Either, Executor, Loop};
use futures::sync::{mpsc, oneshot};
use futures::{empty, Future, IntoFuture, Sink, Stream};
use tokio_core::net::{TcpStream, UdpSocket};
use tokio_core::reactor::{Core, Interval};
use tokio_io::AsyncRead;

use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;
use resolve::resolver;

use carbon::{CarbonBackend, CarbonCodec};
use config::{Command, Consul, Metrics, Network, System};
use consul::ConsulConsensus;
use errors::GeneralError;
use metric::{Metric, MetricType};
use peer::{PeerCommandClient, PeerServer, PeerSnapshotClient};
use server::StatsdServer;
use task::Task;
use util::{AggregateOptions, Aggregator, UpdateCounterOptions};

pub type Float = f64;
pub type Cache = HashMap<String, Metric<Float>>;
thread_local!(static LONG_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));
thread_local!(static SHORT_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));

pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static PEER_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS_METRICS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

pub static CAN_LEADER: AtomicBool = ATOMIC_BOOL_INIT;
pub static IS_LEADER: AtomicBool = ATOMIC_BOOL_INIT;
pub static FORCE_LEADER: AtomicBool = ATOMIC_BOOL_INIT;

pub fn try_resolve(s: &str) -> SocketAddr {
    s.parse().unwrap_or_else(|_| {
        // for name that have failed to be parsed we try to resolve it via DNS
        let mut split = s.split(':');
        let host = split.next().unwrap(); // Split always has first element
        let port = split.next().expect("port not found");
        let port = port.parse().expect("bad port value");

        let first_ip = resolver::resolve_host(host)
            .expect("failed resolving backend name")
            .next()
            .expect("at least one IP address required");
        SocketAddr::new(first_ip, port)
    })
}

fn main() {
    let (system, command) = System::load();

    let System {
        verbosity,
        network:
            Network {
                listen,
                peer_listen,
                bufsize,
                multimessage,
                mm_packets,
                greens,
                snum,
                nodes,
                snapshot_interval,
            },
        consul:
            Consul {
                start_disabled: consul_disable,
                agent,
                session_ttl: consul_session_ttl,
                renew_time: consul_renew_time,
                key_name: consul_key,
            },
        metrics:
            Metrics {
                //           max_metrics,
                mut count_updates,
                update_counter_prefix,
                update_counter_suffix,
                update_counter_threshold,
            },
        carbon,
        n_threads,
        w_threads,
        stats_interval: s_interval,
        task_queue_size,
        stats_prefix,
    } = system;

    let verbosity = Level::from_str(&verbosity).expect("bad verbosity");

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let nodes = nodes
        .into_iter()
        .map(|node| try_resolve(&node))
        .collect::<Vec<_>>();

    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, verbosity).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"bioyino"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    if let Command::Query(command, dest) = command {
        let dest = try_resolve(&dest);
        let command = PeerCommandClient::new(&rlog, dest, &handle, command);
        core.run(command.into_future()).unwrap_or_else(|e| {
            warn!(rlog,
                  "error sending command";
                  "dest"=>format!("{}",  &dest),
                  "error"=> format!("{}", e),
                  )
        });
        return;
    }

    if count_updates && update_counter_prefix.len() == 0 && update_counter_suffix.len() == 0 {
        warn!(rlog, "update counting suffix and prefix are empty, update counting disabled to avoid metric rewriting");
        count_updates = false;
    }

    let update_counter_prefix: Bytes = update_counter_prefix.into();
    let update_counter_suffix: Bytes = update_counter_suffix.into();

    // Start counting threads
    let mut chans = Vec::with_capacity(w_threads);
    for i in 0..w_threads {
        let (tx, rx) = ::futures::sync::mpsc::channel(task_queue_size);
        chans.push(tx);
        thread::Builder::new()
            .name(format!("bioyino_cnt{}", i).into())
            .spawn(move || {
                let mut core = Core::new().unwrap();
                let future = rx.for_each(move |task: Task| lazy(|| ok(task.run())));
                core.run(future).unwrap();
            })
            .expect("starting counting worker thread");
    }

    let ichans = chans.clone();
    let stats_prefix = stats_prefix.trim_right_matches(".").to_string();

    let log = rlog.new(o!("source"=>"stats_thread"));
    std::thread::spawn(move || {
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let stimer = Interval::new(
            Duration::from_millis({
                if s_interval > 0 {
                    s_interval
                } else {
                    5000
                }
            }),
            &handle,
        ).unwrap();

        let shandle = handle.clone();
        let stats = stimer
            .map_err(|e| GeneralError::Io(e))
            .for_each(move |()| {
                let mut metrics = HashMap::new();
                {
                    let mut add_metric = |value: Float, suffix| {
                        if s_interval > 0 {
                            metrics.insert(
                                stats_prefix.clone() + "." + suffix,
                                Metric::new(value, MetricType::Counter, None).unwrap(),
                            );
                        }
                    };
                    let egress = EGRESS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(egress, "egress");
                    let ingress = INGRESS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(ingress, "ingress");
                    let ingress_m = INGRESS_METRICS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(ingress_m, "ingress-metric");
                    let agr_errors = AGG_ERRORS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(agr_errors, "agg-error");
                    let parse_errors = PARSE_ERRORS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(parse_errors, "parse-error");
                    let peer_errors = PEER_ERRORS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(peer_errors, "peer-error");
                    let drops = DROPS.swap(0, Ordering::Relaxed) as Float;
                    add_metric(drops, "drop");

                    if s_interval > 0 {
                        info!(log, "stats";
                              "egress" => format!("{:2}", egress / s_interval as f64),
                              "ingress" => format!("{:2}", ingress / s_interval as f64),
                              "ingress-m" => format!("{:2}", ingress_m / s_interval as f64),
                              "a-err" => format!("{:2}", agr_errors / s_interval as f64),
                              "p-err" => format!("{:2}", parse_errors / s_interval as f64),
                              "pe-err" => format!("{:2}", peer_errors / s_interval as f64),
                              "drops" => format!("{:2}", drops / s_interval as f64),
                              );
                    }
                }
                let next_chan = ichans[0].clone();
                shandle
                    .clone()
                    .spawn(next_chan.send(Task::AddMetrics(metrics)).then(|_| Ok(())));
                Ok(())
            })
            .then(|_| Ok(()));

        handle.spawn(stats);
        core.run(empty::<(), ()>()).unwrap();
    });

    //for node in nodes.iter().cloned() {
    //let tchans = chans.clone();
    //let shandle = handle.clone();
    let elog = rlog.clone();
    let snapshot = PeerSnapshotClient::new(
        &elog,
        nodes.clone(),
        Duration::from_millis(snapshot_interval as u64),
        &handle,
        &chans,
    ).into_future()
        .map_err(move |e| {
            PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
            info!(elog, "error sending snapshot";"error"=>format!("{}", e));
            //debug!(log, "error sending snapshot";"error"=>format!("{}", e), "destination"=>format!("{}", node));
        });
    handle.spawn(snapshot);
    //}

    let peer_server = PeerServer::new(&rlog, peer_listen, &handle, &chans, &nodes);

    let elog = rlog.clone();
    // TODO restart server after error
    handle.spawn(peer_server.into_future().then(move |e| {
        warn!(elog, "shot server gone with error: {:?}", e);
        Ok(())
    }));

    // TODO (maybe) change to option, not-depending on number of nodes
    if nodes.len() > 0 {
        if consul_disable {
            CAN_LEADER.store(false, Ordering::SeqCst);
            IS_LEADER.store(false, Ordering::SeqCst);
        } else {
            CAN_LEADER.store(true, Ordering::SeqCst);
        }

        let mut consensus = ConsulConsensus::new(&rlog, agent, consul_key, &handle);
        consensus.set_session_ttl(Duration::from_millis(consul_session_ttl as u64));
        consensus.set_renew_time(Duration::from_millis(consul_renew_time as u64));
        handle.spawn(consensus.into_future().then(|_| Ok(())));
    } else {
        IS_LEADER.store(true, Ordering::SeqCst);
        CAN_LEADER.store(false, Ordering::SeqCst);
    }

    let tchans = chans.clone();
    let tlog = rlog.clone();

    let carbon_timer = Interval::new(Duration::from_millis(carbon.interval), &handle).unwrap();
    let carbon_timer = carbon_timer
        .map_err(|e| GeneralError::Io(e))
        .for_each(move |()| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .map_err(|e| GeneralError::Time(e))?;

            let backend_addr = try_resolve(&carbon.address);
            let tchans = tchans.clone();
            let tlog = tlog.clone();

            let update_counter_prefix = update_counter_prefix.clone();
            let update_counter_suffix = update_counter_suffix.clone();
            let options = options.clone();
            thread::Builder::new()
                .name("bioyino_carbon".into())
                .spawn(move || {
                    let mut core = Core::new().unwrap();
                    let handle = core.handle();

                    let is_leader = IS_LEADER.load(Ordering::SeqCst);

                    debug!(tlog, "leader sending metrics");

                    let elog = tlog.clone();
                    let options = AggregateOptions {
                        is_leader,
                        update_counter: if count_updates {
                            Some(UpdateCounterOptions {
                                threshold: update_counter_threshold,
                                prefix: update_counter_prefix,
                                suffix: update_counter_suffix,
                            })
                        } else {
                            None
                        },
                    };

                    if is_leader {
                        //  let (backend, backend_tx) =
                        let backend = CarbonBackend::new(
                            backend_addr,
                            carbon,
                            ts,
                            &handle,
                            Arc::new(Vec::new()),
                        );

                        let (backend_tx, backend_rx) = mpsc::unbounded();
                        let aggregator = Aggregator::new(options, tchans, backend_tx);

                        handle.spawn(aggregator.into_future());

                        //let backend = backend_rx.cocllect();

                        core.run(backend.into_future().then(|_| Ok::<(), ()>(())))
                            .unwrap_or_else(
                                |e| warn!(tlog, "Failed to send to graphite"; "error"=>e),
                            );
                    } else {
                        let (backend_tx, _) = mpsc::unbounded();
                        let aggregator = Aggregator::new(options, tchans, backend_tx).into_future();
                        core.run(aggregator.then(|_| Ok::<(), ()>(())))
                            .unwrap_or_else(
                                |e| warn!(tlog, "Failed to join aggregated metrics"; "error"=>e),
                            );
                    }
                })
                .expect("starting thread for sending to graphite");
            Ok(())
        });

    if multimessage {
        use std::os::unix::io::AsRawFd;

        // It is crucial for recvmmsg to have one socket per many threads
        // to avoid drops because at lease two threads have to work on socket
        // simultaneously
        let socket = UdpBuilder::new_v4().unwrap();
        socket.reuse_address(true).unwrap();
        socket.reuse_port(true).unwrap();
        let sck = socket.bind(listen).unwrap();

        for i in 0..n_threads {
            let chans = chans.clone();
            let remote = core.remote();
            let log = rlog.new(o!("source"=>"mudp_thread"));

            let sck = sck.try_clone().unwrap();
            thread::Builder::new()
                .name(format!("bioyino_mudp{}", i).into())
                .spawn(move || {
                    let fd = sck.as_raw_fd();
                    let messages = mm_packets;
                    {
                        // <--- this limits `use::libc::*` scope
                        use libc::*;
                        use std::ptr::null_mut;

                        let mut ichans = chans.iter().cycle();

                        // a vector to avoid dropping iovec structures
                        let mut iovecs = Vec::with_capacity(messages);

                        // a vector to avoid dropping message buffers
                        let mut message_vec = Vec::new();

                        let mut v: Vec<mmsghdr> = Vec::with_capacity(messages);
                        for _ in 0..messages {
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
                        let mut b = BytesMut::with_capacity(bufsize * messages * task_queue_size);

                        // We cannot count on allocator to allocate a value close to capacity
                        // it can be much more than that and stall our buffer for too long
                        // So we set our chunk size ourselves and send buffer when this value
                        // exhausted, but we only allocate when buffer becomes empty
                        let mut chunks = task_queue_size as isize;

                        loop {
                            let res = unsafe {
                                recvmmsg(
                                    fd as c_int,
                                    vp,
                                    vlen as c_uint,
                                    MSG_WAITFORONE,
                                    null_mut(),
                                )
                            };

                            use bytes::BufMut;
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
                                    b.reserve(bufsize * messages * task_queue_size)
                                }

                                // put packets into buffer
                                for i in 0..end {
                                    let len = v[i].msg_len as usize;

                                    b.put(&message_vec[i][0..len]);
                                    chunks -= end as isize;
                                }

                                // when it's time to send bytes, send them
                                if chunks <= 0 {
                                    let chan = ichans.next().unwrap().clone();
                                    INGRESS.fetch_add(1, Ordering::Relaxed);
                                    remote
                                        .execute(
                                            chan.send(Task::Parse(b.take().freeze()))
                                                .map_err(|_| {
                                                    DROPS.fetch_add(1, Ordering::Relaxed);
                                                })
                                                .then(|_| Ok(())),
                                        )
                                        .unwrap_or_else(|e| {
                                            warn!(log, "exec error: {:?}", e);
                                        });
                                    chunks = task_queue_size as isize;
                                }
                            } else {
                                let errno = unsafe { *__errno_location() };
                                if errno == EAGAIN {
                                } else {
                                    warn!(log, "UDP receive error";
                                          "code"=> format!("{}",res),
                                          "error"=>format!("{}", ::std::io::Error::last_os_error())
                                         )
                                }
                            }
                        }
                    }
                })
                .expect("starting multimsg thread");
        }
    } else {
        // Create a pool of listener sockets
        let mut sockets = Vec::new();
        for _ in 0..snum {
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
            thread::Builder::new()
                .name(format!("bioyino_udp{}", i).into())
                .spawn(move || {
                    // each thread runs it's own core
                    let mut core = Core::new().unwrap();
                    let handle = core.handle();

                    // Inside each green thread
                    for _ in 0..greens {
                        for socket in sockets.iter() {
                            let buf = BytesMut::with_capacity(task_queue_size * bufsize);

                            let mut readbuf = BytesMut::with_capacity(bufsize);
                            unsafe { readbuf.set_len(bufsize) }
                            let chans = chans.clone();
                            // create UDP listener
                            let socket = socket.try_clone().expect("cloning socket");
                            let socket = UdpSocket::from_socket(socket, &handle)
                                .expect("adding socket to event loop");

                            let server = StatsdServer::new(
                                socket,
                                &handle,
                                chans.clone(),
                                buf,
                                task_queue_size,
                                bufsize,
                                i,
                                readbuf,
                                task_queue_size * bufsize,
                            );

                            handle.spawn(server.into_future());
                        }
                    }

                    core.run(::futures::future::empty::<(), ()>()).unwrap();
                })
                .expect("creating UDP reader thread");
        }
    }

    core.run(carbon_timer).unwrap();
}
