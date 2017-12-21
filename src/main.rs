// General
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate slog_scope;

// Options
extern crate clap;
#[macro_use]
extern crate structopt_derive;
extern crate structopt;

// Network
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate bytes;
extern crate num_cpus;
extern crate resolve;
extern crate net2;
extern crate libc;
extern crate hyper;

// Other
extern crate combine;
extern crate num;
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;

pub mod parser;
pub mod errors;
pub mod metric;
pub mod codec;
pub mod bigint;
pub mod task;
pub mod consul;
pub mod peer;

use std::collections::HashMap;
use std::time::{self, Duration, SystemTime};
use std::thread;
use std::net::{SocketAddr};
use std::cell::RefCell;
use std::sync::atomic::{ATOMIC_USIZE_INIT, ATOMIC_BOOL_INIT, AtomicUsize, AtomicBool, Ordering};

use failure::Fail;
use structopt::StructOpt;
use slog::Drain;

use bytes::BytesMut;
use futures::{Stream, Future, Sink, empty, IntoFuture};
use futures::future::{join_all, Loop, loop_fn, ok, lazy, Executor};
use futures::sync::oneshot;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, TcpStream};
use tokio_io::AsyncRead;

use resolve::resolver;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use errors::GeneralError;
use metric::{Metric, MetricType};
use codec::{StatsdServer, CarbonCodec};
use consul::{ConsulConsensus};
use peer::{PeerServer, PeerSnapshotClient, PeerCommand, PeerCommandClient};

use task::Task;

pub type Float = f64;
pub type Cache = HashMap<String, Metric<Float>>;
thread_local!(static LONG_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));
thread_local!(static SHORT_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));

//pub type Log = Box<Drain<Ok=(),Err=::slog::Error>>;
pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static PEER_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS_METRICS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

pub const EPSILON: f64 = 0.01;
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

#[derive(StructOpt, Debug, Serialize, Deserialize)]
#[structopt(about = "StatsD-compatible async metric aggregator")]
struct Options {
    #[structopt(short = "l", long = "listen", default_value="127.0.0.1:8125")]
    /// Address and UDP port to listen for statsd metrics on
    listen: SocketAddr,

    #[structopt(short = "L", long = "peer-listen", default_value="127.0.0.1:8136")]
    /// Address and port for replication server to listen on
    peer_listen: SocketAddr,

    #[structopt( short = "b", long = "backend", value_name = "IP:PORT", default_value="127.0.0.1:2003")]
    /// IP and port of the carbon-protocol backend to send aggregated data to
    backend: String,
    #[structopt(short = "i", long = "s-interval", default_value = "30000")]
    /// How often send metrics to carbon backend
    interval: Option<u64>, // u64 has a special meaning in structopt

    #[structopt(short = "n", long = "nthreads", default_value = "4")]
    /// Number of async UDP network worker threads, use 0 to use all CPU cores, use any negative to disable
    /// async UDP threads
    nthreads: isize,

    #[structopt(short = "m", long = "mthreads", default_value = "0")]
    /// Number of multimessage(revcmmsg) worker threads, use 0 to use no threads of this type
    mthreads: usize,

    #[structopt(short = "M", long = "msize", default_value = "1000")]
    /// Number of multimessage packets to receive at once
    msize: usize,

    #[structopt(short = "c", long = "cthreads", default_value = "4")]
    /// Number of aggregating threads, set to 0 to use all CPU cores
    cthreads: usize,

    #[structopt(short = "p", long = "pool", default_value = "4")]
    /// Socket pool size
    snum: usize,

    #[structopt(short = "g", long = "greens", default_value = "4")]
    /// Number of green threads per worker thread
    greens: usize,

    #[structopt(short = "s", long = "interval", help = "How often to gather own stats", default_value = "5000")]
    s_interval: Option<u64>,

    #[structopt(short = "B", long = "bufsize", default_value = "1500")]
    /// UDP buffer size for single packet. Needs to be around MTU.
    bufsize: usize,

    #[structopt(short = "q", long = "task-queue-size", default_value = "2048")]
    /// task queue size for single counting thread
    task_queue_size: usize,

    #[structopt(short = "A", long = "agent", default_value = "127.0.0.1:8500")]
    /// Consul agent address
    agent: SocketAddr,

    #[structopt(long = "consul-session-ttl", default_value = "11000")]
    /// TTL of consul session, ms (min 10s)
    consul_session_ttl: usize,

    #[structopt(long = "consul-renew-time", default_value = "1000")]
    /// How often to renew consul session, ms (min 10s)
    consul_renew_time: usize,

    #[structopt(long = "consul-disable", default_value = "false")]
    /// Start in disabled leader finding mode
    consul_disable: bool,

    #[structopt(long = "nodes")]
    /// List of nodes to replicate metrics to
    nodes: Vec<SocketAddr>,

    #[structopt(short = "t", long = "snapshot-interval", default_value = "1000")]
    /// Interval to send snapshots to nodes, ms
    snapshot_interval: usize,

    #[structopt(short = "Q", long = "query")]
    /// Connect to server wiht a query
    query: Option<PeerCommand>,

    #[structopt(short = "S", long = "stats-prefix", default_value="resources.monitoring.bioyino")]
    /// Prefix to send own metrics with
    stats_prefix: String,
}

fn main() {
    let Options {
        listen,
        peer_listen,
        backend,
        nthreads,
        mut cthreads,
        mthreads,
        msize,
        snum,
        greens,
        interval,
        s_interval,
        bufsize,
        task_queue_size,
        agent,
        consul_session_ttl,
        consul_renew_time,
        consul_disable,
        nodes,
        snapshot_interval,
        query,
        stats_prefix,
    } = Options::from_args();


    // Check options
    let nthreads = if nthreads == 0 {
        num_cpus::get()
    } else if nthreads < 0 {
        0
    } else {
        nthreads as usize
    };

    if cthreads == 0 {
        cthreads = num_cpus::get();
    }

    if greens == 0 {
        panic!("Number of green threads cannot be zero")
    }

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(interval.unwrap()), &handle).unwrap();

    let backend_addr = try_resolve(&backend);

    let s_interval = s_interval.unwrap() as f64 / 1000f64;

    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"bioyino"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    if let Some(command) = query {
        let command = PeerCommandClient::new(&rlog, peer_listen, &handle, command);
        core.run(command.into_future())
            .unwrap_or_else(
                |e| warn!(rlog,
                          "error sending command";
                          "dest"=>format!("{}",  &peer_listen),
                          "error"=> format!("{}", e),
                          )
                );
        return
    }

    // Start counting threads
    let mut chans = Vec::with_capacity(cthreads);
    for i in 0..cthreads {
        let (tx, rx) = ::futures::sync::mpsc::channel(task_queue_size);
        chans.push(tx);
        thread::Builder::new()
            .name(format!("bioyino_cnt{}", i).into())
            .spawn(move || {
                let mut core = Core::new().unwrap();
                let future = rx.for_each(move |task: Task|{
                    lazy(|| ok(
                            task.run()
                            ))
                });
                core.run(future).unwrap();
            }).expect("starting counting worker thread");
    }

    let ichans = chans.clone();
    let stats_prefix = stats_prefix.trim_right_matches(".").to_string();

    let log = rlog.new(o!("source"=>"stats_thread"));
    std::thread::spawn(move ||{
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let stimer = Interval::new(Duration::from_millis((s_interval * 1000f64) as u64), &handle).unwrap();

        let shandle = handle.clone();
        let stats = stimer
            .map_err(|e| GeneralError::Io(e))
            .for_each( move |()| {
                let mut metrics = HashMap::new();
                {
                    let mut add_metric = |value: Float, suffix| {
                        metrics.insert(stats_prefix.clone() + "."+suffix, Metric::new(value, MetricType::Gauge(None), None).unwrap())
                    };
                    let egress = EGRESS.swap(0, Ordering::Relaxed) as Float / s_interval;
                    add_metric(egress, "egress");
                    let ingress = INGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(ingress, "ingress");
                    let ingress_m = INGRESS_METRICS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(ingress_m, "ingress-metric");
                    let agr_errors = AGG_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(agr_errors, "agg-error");
                    let parse_errors = PARSE_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(parse_errors, "parse-error");
                    let peer_errors = PEER_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(peer_errors, "peer-error");
                    let drops = DROPS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                    add_metric(drops, "drop");

                    info!(log, "stats";
                          "egress" => format!("{:2}", egress),
                          "ingress" => format!("{:2}", ingress),
                          "ingress-m" => format!("{:2}", ingress_m),
                          "a-err" => format!("{:2}", agr_errors),
                          "p-err" => format!("{:2}", parse_errors),
                          "pe-err" => format!("{:2}", peer_errors),
                          "drops" => format!("{:2}", drops),
                          );
                }
                let next_chan = ichans[0].clone();
                shandle.clone().spawn(next_chan.send(Task::JoinSnapshot(vec![metrics])).then(|_|Ok(())));
                Ok(())
            }).then(|_|Ok(()));

        handle.spawn(stats);
        core.run(empty::<(), ()>()).unwrap();
    });

    for node in nodes.iter().cloned() {
        let tchans = chans.clone();
        let shandle = handle.clone();
        let elog = rlog.clone();
        let snapshot = PeerSnapshotClient::new(&rlog, node.clone(), Duration::from_millis(snapshot_interval as u64), &shandle, &tchans)
            .into_future()
            .map_err(move |e| {
                //println!("error sending snapshot to {:?}: {:?}", node, e);
                PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                debug!(elog, "error sending snapshot: {:?}", e);
            });
        handle.spawn(snapshot);
    }

    let peer_server = PeerServer::new(&rlog, peer_listen, &handle, &chans, &nodes);

    let elog = rlog.clone();
    // TODO restart server after error
    handle.spawn(peer_server
                 .into_future()
                 .then(move |e|{warn!(elog, "shot server gone with error: {:?}", e); Ok(())}));
    // TODO (maybe) change to option, not-depending on number of nodes
    if nodes.len() > 0 {
        if consul_disable {
            CAN_LEADER.store(false, Ordering::SeqCst);
            IS_LEADER.store(false, Ordering::SeqCst);
        } else {
            CAN_LEADER.store(true, Ordering::SeqCst);
        }
        let mut consensus = ConsulConsensus::new(
            &rlog,
            agent,
            &handle);
        consensus.set_session_ttl(Duration::from_millis(consul_session_ttl as u64));
        consensus.set_renew_time(Duration::from_millis(consul_renew_time as u64));
        handle.spawn(consensus.into_future().then(|_|Ok(())));
    } else {
        IS_LEADER.store(true, Ordering::SeqCst);
        CAN_LEADER.store(false, Ordering::SeqCst);
    }

    let tchans = chans.clone();
    let tlog = rlog.clone();
    let timer = timer
        .map_err(|e| GeneralError::Io(e))
        .for_each(move |()| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH).map_err(|e| GeneralError::Time(e))?;
            let ts = ts.as_secs().to_string();

            let addr = backend_addr.clone();
            let tchans = tchans.clone();
            let tlog = tlog.clone();
            thread::Builder::new()
                .name("bioyino_carbon".into())
                .spawn(move ||{
                    let mut core = Core::new().unwrap();
                    let handle = core.handle();

                    let metrics = tchans.clone().into_iter().map(|chan| {
                        let (tx, rx) = oneshot::channel();
                        handle.spawn(chan.send(Task::Rotate(tx)).then(|_|Ok(())));
                        rx
                            .map_err(|_|GeneralError::FutureSend)
                    })
                    .collect::<Vec<_>>();
                    let is_leader = IS_LEADER.load(Ordering::SeqCst);
                    if is_leader {
                        debug!(tlog, "leader sending metrics");
                        let future = join_all(metrics).and_then(move |metrics|{
                            // Join all metrics into hashmap by only pushing everything to vector
                            let metrics = metrics
                                .into_iter()
                                .filter(|m|m.len() > 0)
                                .fold(HashMap::new(), |mut acc, m|{
                                    m.into_iter()
                                        .map(|(name, metric)|{
                                            let entry = acc.entry(name).or_insert(Vec::new());
                                            entry.push(metric);
                                        }).last().unwrap();
                                    acc
                                });

                            // now a difficult part: send every metric to
                            // be aggregated on a separate worker
                            let future = metrics.into_iter()
                                .filter(|&(_, ref m)| m.len() > 0)
                                .enumerate()
                                .map(move |(start, (name, mut metricvec))| {
                                    let first = metricvec.pop().unwrap(); // zero sized vectors were filtered out before
                                    let chans = tchans.clone();
                                    // future-based fold-like
                                    let looped = loop_fn((first, metricvec, start%chans.len()), move |(acc, mut metricvec, next)|{
                                        let next = if next >= chans.len() - 1 {
                                            0
                                        } else {
                                            next+1
                                        };
                                        let next_chan = chans[next].clone();
                                        match metricvec.pop() {
                                            // if vector has more elements
                                            Some(metric) => {
                                                //ok((acc, metricvec))
                                                //    .map(|v|Loop::Continue(v))
                                                let (tx, rx) = oneshot::channel();
                                                let send = next_chan.send(Task::Join(acc, metric, tx))
                                                    .map_err(|_|GeneralError::FutureSend)
                                                    .and_then(move |_|{
                                                        rx
                                                            .map_err(|_|GeneralError::FutureSend)
                                                            .map(move |m| Loop::Continue((m, metricvec, next)))
                                                    });
                                                // Send acc to next worker
                                                Box::new(send) as Box<Future<Item=Loop<_, _>, Error=_>>
                                            }
                                            None => Box::new(ok(acc).map(|v|Loop::Break(v))), // the result of the future is a name and aggregated metric
                                        }
                                    });
                                    looped.map(|m|(name, m))
                                });

                            join_all(future)
                        });

                        let elog = tlog.clone();
                        let pusher = future
                            .and_then(|metrics|{
                                TcpStream::connect(&addr, &handle)
                                    .map_err(|e| GeneralError::Io(e))
                                    //.map_err(|e|e.compat().into())
                                    .map(move |conn| (conn, metrics))
                            })
                        .map_err(|e|e.compat().into())
                            // wait for both: all results from all channels and tcp connection to be ready
                            .and_then(move |(conn, metrics)| {
                                let writer = conn.framed(CarbonCodec);
                                let aggregated = metrics.into_iter().flat_map(move |(name, value)|{
                                    let ts = ts.clone();
                                    value.into_iter().map(move |(suffix, value)|{
                                        (name.clone() + suffix, value, ts.clone())
                                    })
                                })
                                .inspect(|_|{
                                    EGRESS.fetch_add(1, Ordering::Relaxed);
                                });
                                let s = ::futures::stream::iter_ok(aggregated)
                                    .map_err(|e| GeneralError::Io(e));

                                writer.send_all(s)
                                    .map_err(move |e|{warn!(elog, "carbon send failed";"error"=>format!("{}", e)); e})
                            });

                        core.run(pusher.then(|_|Ok::<(), ()>(()))).unwrap_or_else(|e|warn!(tlog, "Failed to send to graphite"; "error"=>e));
                    } else {
                        core.run(join_all(metrics).then(|_|Ok::<(), ()>(()))).unwrap_or_else(|e|warn!(tlog, "Failed to join aggregated metrics"; "error"=>e));
                    }
                }).expect("starting thread for sending to graphite");
            Ok(())
        });

    // Create a pool of listener sockets
    let mut sockets = Vec::new();
    for _ in 0..snum {
        let socket = UdpBuilder::new_v4().unwrap();
        socket.reuse_address(true).unwrap();
        socket.reuse_port(true).unwrap();
        let socket = socket.bind(&listen).unwrap();
        sockets.push(socket);
    }

    for i in 0..nthreads {
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
                        let buf = BytesMut::with_capacity(task_queue_size*bufsize);

                        let mut readbuf = BytesMut::with_capacity(bufsize);
                        unsafe {readbuf.set_len(bufsize)}
                        let chans = chans.clone();
                        // create UDP listener
                        let socket = socket.try_clone().expect("cloning socket");
                        let socket = UdpSocket::from_socket(socket, &handle).expect("adding socket to event loop");

                        let server = StatsdServer::new(
                            socket,
                            &handle,
                            chans.clone(),
                            buf,
                            task_queue_size,
                            bufsize,
                            i, readbuf);

                        handle.spawn(server.into_future()); // TODO: process io error
                    }
                }

                core.run(::futures::future::empty::<(), ()>()).unwrap();
            }).expect("creating UDP reader thread");
    }

    // TODO: this is too dirty
    drop(sockets);

    use std::os::unix::io::AsRawFd;
    let socket = UdpBuilder::new_v4().unwrap();
    socket.reuse_address(true).unwrap();
    socket.reuse_port(true).unwrap();
    let sck = socket.bind(listen).unwrap();

    let fd = sck.as_raw_fd();
    for i in 0..mthreads {
        let chans = chans.clone();
        let remote = core.remote();
        let log = rlog.new(o!("source"=>"mudp_thread"));
        thread::Builder::new()
            .name(format!("bioyino_mudp{}", i).into())
            .spawn(move || {
                let messages = msize;
                { // <--- this limits `use::libc::*` scope
                    use std::ptr::{null_mut};
                    use libc::*;

                    let mut ichans = chans.iter().cycle();

                    // a vector to avoid dropping iovec structures
                    let mut iovecs = Vec::with_capacity(messages);

                    // a vector to avoid dropping message buffers
                    let mut message_vec = Vec::new();
                    let mut buffer = BytesMut::with_capacity(bufsize*messages);

                    // we don't care what is written in the control field, so we allocate it once
                    // and put the same pointer to all messages
                    let mut control: Vec<u8> = Vec::with_capacity(128);
                    control.resize(128, 0u8);

                    let mut v: Vec<mmsghdr> = Vec::with_capacity(messages);
                    for _ in 0..messages {
                        let mut buf = buffer.split_to(bufsize);
                        unsafe { buf.set_len(bufsize); }
                        let mut iov = Vec::with_capacity(1);
                        iov.resize(1, iovec {
                            iov_base: buf.as_mut_ptr() as *mut c_void,
                            iov_len: bufsize as size_t,
                        });
                        let mut control: Vec<u8> = Vec::with_capacity(128);
                        control.resize(128, 0u8);
                        let m = mmsghdr {
                            msg_hdr: msghdr {
                                msg_name: null_mut(),
                                msg_namelen: 0 as socklen_t,
                                msg_iov: iov.as_mut_ptr(),
                                msg_iovlen: iov.len() as size_t,
                                msg_control: control.as_mut_ptr() as *mut c_void,
                                msg_controllen: control.len() as size_t,
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


                    let mut b = BytesMut::with_capacity(bufsize*messages);
                    loop {
                        let res = unsafe {
                            recvmmsg(
                                fd as c_int,
                                vp,
                                vlen as c_uint,
                                0,
                                null_mut(),
                                )
                        };

                        use bytes::BufMut;
                        if res >= 0 {
                            let end = res as usize;
                            for i in 0..end {
                                let len = v[i].msg_len as usize;
                                if b.remaining_mut() < len {
                                    let chan = ichans.next().unwrap().clone();
                                    INGRESS.fetch_add(1, Ordering::Relaxed);
                                    remote.execute(chan.send(Task::Parse(b.freeze())).map_err(|_|{
                                        DROPS.fetch_add(1, Ordering::Relaxed);
                                    }).then(|_|Ok(()))).unwrap_or_else(|e|{
                                        warn!(log, "exec error: {:?}", e);
                                    });
                                    b = BytesMut::with_capacity(bufsize*messages);
                                }
                                b.put(&message_vec[i][0..len]);
                            };
                        } else {
                            warn!(log, "UDP receive error";
                                  "code"=> format!("{}",res),
                                  "error"=>format!("{}", ::std::io::Error::last_os_error())
                                 )
                        }
                    }
                }
            }).expect("starting multimsg thread");
    }
    core.run(timer).unwrap();
}
