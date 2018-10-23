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
extern crate tokio;
extern crate tokio_core;

// Other
extern crate bincode;
extern crate combine;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate ftoa;
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
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT};
use std::thread;
use std::time::{self, Duration, Instant, SystemTime};

use slog::{Drain, Level};

use bytes::{Bytes, BytesMut};
use futures::future::{empty, lazy, ok};
use futures::sync::mpsc;
use futures::{Future, IntoFuture, Stream};

use tokio::net::UdpSocket;
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Interval;
use tokio_core::reactor::Core;

use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;
use resolve::resolver;

use carbon::CarbonBackend;
use config::{Command, Consul, Metrics, Network, System};
use consul::ConsulConsensus;
use errors::GeneralError;
use metric::Metric;
use peer::{PeerCommandClient, PeerServer, PeerSnapshotClient};
use server::StatsdServer;
use task::Task;
use util::{AggregateOptions, Aggregator, BackoffRetryBuilder, OwnStats, UpdateCounterOptions};

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
    } = system.clone();

    let verbosity = Level::from_str(&verbosity).expect("bad verbosity");

    let mut runtime = Runtime::new().expect("creating runtime for main thread");

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
        let command = PeerCommandClient::new(rlog.clone(), dest.clone(), command);

        runtime.block_on(command.into_future()).unwrap_or_else(|e| {
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
    let log = rlog.new(o!("thread" => "main"));
    // Start counting threads
    info!(log, "starting counting threads");
    let mut chans = Vec::with_capacity(w_threads);
    for i in 0..w_threads {
        let (tx, rx) = mpsc::channel(task_queue_size);
        chans.push(tx);
        thread::Builder::new()
            .name(format!("bioyino_cnt{}", i).into())
            .spawn(move || {
                let mut runtime = Runtime::new().expect("creating runtime for counting worker");
                let future = rx.for_each(move |task: Task| lazy(|| ok(task.run())));
                runtime.block_on(future).expect("worker thread failed");
            })
        .expect("starting counting worker thread");
    }

    let stats_prefix = stats_prefix.trim_right_matches(".").to_string();

    // Spawn future gatering bioyino own stats
    let own_stat_chan = chans[0].clone();
    let own_stat_log = rlog.clone();
    info!(log, "starting own stats counter");
    let own_stats = OwnStats::new(s_interval, stats_prefix, own_stat_chan, own_stat_log);
    runtime.spawn(own_stats);

    info!(log, "starting snapshot sender");
    let snap_log = rlog.clone();
    let snap_err_log = rlog.clone();
    let snapshot = PeerSnapshotClient::new(
        &snap_log,
        nodes.clone(),
        Duration::from_millis(snapshot_interval as u64),
        &chans,
        ).into_future()
        .map_err(move |e| {
            PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
            info!(snap_err_log, "error sending snapshot";"error"=>format!("{}", e));
        });
    runtime.spawn(snapshot);

    // settings afe for asap restart
    info!(log, "starting snapshot receiver");
    let peer_server_ret = BackoffRetryBuilder {
        delay: 1,
        delay_mul: 1f32,
        delay_max: 1,
        retries: ::std::usize::MAX,
    };
    let serv_log = rlog.clone();
    let peer_server = PeerServer::new(rlog.clone(), peer_listen, chans.clone(), nodes.clone());
    let peer_server = peer_server_ret.spawn(peer_server).map_err(move |e| {
        warn!(serv_log, "shot server gone with error: {:?}", e);
    });

    runtime.spawn(peer_server);

    // TODO (maybe) change to option, not-depending on number of nodes
    if nodes.len() > 0 {
        info!(log, "consul is enabled, starting consul consensus");
        if consul_disable {
            CAN_LEADER.store(false, Ordering::SeqCst);
            IS_LEADER.store(false, Ordering::SeqCst);
        } else {
            CAN_LEADER.store(true, Ordering::SeqCst);
        }
        let consul_log = rlog.clone();
        thread::Builder::new()
            .name("bioyino_consul".into())
            .spawn(move || {
                let mut core = Core::new().unwrap();
                let handle = core.handle();

                let mut consensus = ConsulConsensus::new(&consul_log, agent, consul_key, &handle);
                consensus.set_session_ttl(Duration::from_millis(consul_session_ttl as u64));
                consensus.set_renew_time(Duration::from_millis(consul_renew_time as u64));
                core.run(consensus.into_future().map_err(|_| ()))
                    .expect("running core for Consul consensus");
            })
        .expect("starting thread for running consul");
    } else {
        info!(log, "consul is disabled, starting as leader");
        IS_LEADER.store(true, Ordering::SeqCst);
        CAN_LEADER.store(false, Ordering::SeqCst);
    }

    info!(log, "starting carbon backend");
    let tchans = chans.clone();
    let carbon_log = rlog.clone();

    let dur = Duration::from_millis(carbon.interval);
    let carbon_timer = Interval::new(Instant::now() + dur, dur);
    let carbon_timer = carbon_timer
        .map_err(|e| GeneralError::Timer(e))
        .for_each(move |_tick| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .map_err(|e| GeneralError::Time(e))?;

            let backend_addr = try_resolve(&carbon.address);
            let tchans = tchans.clone();
            let carbon_log = carbon_log.clone();

            let update_counter_prefix = update_counter_prefix.clone();
            let update_counter_suffix = update_counter_suffix.clone();
            let backend_opts = system.carbon.clone();
            thread::Builder::new()
                .name("bioyino_carbon".into())
                .spawn(move || {
                    let mut runtime = match Runtime::new() {
                        Ok(runtime) => runtime,
                        Err(e) => {
                            error!(carbon_log, "creating runtime for backend"; "error"=>e.to_string());
                            return;
                        }
                    };

                    let is_leader = IS_LEADER.load(Ordering::SeqCst);

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
                        info!(carbon_log, "leader sending metrics");
                        let (backend_tx, backend_rx) = mpsc::unbounded();
                        let aggregator = Aggregator::new(options, tchans, backend_tx).into_future();

                        runtime.spawn(aggregator);

                        let backend = backend_rx
                            .inspect(|_| {
                                EGRESS.fetch_add(1, Ordering::Relaxed);
                            })
                        .collect()
                            .and_then(|metrics| {
                                let backend =
                                    CarbonBackend::new(backend_addr, ts, Arc::new(metrics));

                                let retrier = BackoffRetryBuilder {
                                    delay: backend_opts.connect_delay,
                                    delay_mul: backend_opts.connect_delay_multiplier,
                                    delay_max: backend_opts.connect_delay_max,
                                    retries: backend_opts.send_retries,
                                };
                                retrier.spawn(backend).map_err(|e| {
                                    error!(carbon_log, "Failed to send to graphite"; "error"=>format!("{:?}",e));
                                }) // TODO error
                            });

                        runtime.block_on(backend).unwrap_or_else(|e| {
                            error!(carbon_log, "Failed to send to graphite"; "error"=>e);
                        });
                    } else {
                        info!(carbon_log, "not leader, removing metrics");
                        let (backend_tx, _) = mpsc::unbounded();
                        let aggregator = Aggregator::new(options, tchans, backend_tx).into_future();
                        runtime
                            .block_on(aggregator.then(|_| Ok::<(), ()>(())))
                            .unwrap_or_else(
                                |e| error!(carbon_log, "Failed to join aggregated metrics"; "error"=>e),
                                );
                    }
                })
            .expect("starting thread for sending to graphite");
            Ok(())
        });

    let tlog = rlog.clone();
    runtime.spawn(carbon_timer.map_err(move |e| {
        warn!(tlog, "error running carbon"; "error"=>e.to_string());
    }));

    if multimessage {
        info!(log, "multimessage enabled, starting in sync UDP mode");
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
                                    let mut chan = ichans.next().unwrap().clone();
                                    INGRESS.fetch_add(res as usize, Ordering::Relaxed);
                                    chan.try_send(Task::Parse(b.take().freeze()))
                                        .map_err(|_| {
                                            warn!(log, "error sending buffer(queue full?)");
                                            DROPS.fetch_add(res as usize, Ordering::Relaxed);
                                        })
                                    .unwrap_or(());
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
                })
            .expect("starting multimsg thread");
        }
    } else {
        info!(log, "multimessage is disabled, starting in async UDP mode");
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
                                task_queue_size,
                                );

                            runtime.spawn(server.into_future());
                        }
                    }

                    runtime
                        .block_on(empty::<(), ()>())
                        .expect("starting runtime for async UDP");
                })
            .expect("creating UDP reader thread");
        }
    }

    runtime
        .block_on(empty::<(), ()>())
        .expect("running runtime in main thread");
}
