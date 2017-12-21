// General
extern crate failure;
#[macro_use]
extern crate failure_derive;

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
extern crate itertools;
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
use metric::Metric;
use codec::{StatsdServer, CarbonCodec};
use consul::{ConsulConsensus};
use peer::{PeerServer, PeerSnapshotClient, PeerCommand, PeerCommandClient};

use task::Task;

pub type Float = f64;
pub type Cache = HashMap<String, Metric<Float>>;
thread_local!(static LONG_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));
thread_local!(static SHORT_CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));

pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
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

    #[structopt( short = "b", long = "backend", help = "IP and port of a backend to send aggregated data to.", value_name = "IP:PORT", default_value="127.0.0.1:2003")]
    backend: String,

    //#[structopt(short = "n", long = "nthreads", help = "Number of network worker threads, use 0 to use all CPU cores, use any negative to use none", default_value = "4")]
    #[structopt(short = "n", long = "nthreads", help = "Number of network worker threads, use 0 to use all CPU cores, use any negative to use none", default_value = "4")]
    nthreads: isize,

    #[structopt(short = "m", long = "mthreads", help = "Number of multimessage(revcmmsg) worker threads, use 0 to use no threads of this type", default_value = "0")]
    mthreads: usize,

    #[structopt(short = "M", long = "msize", help = "multimessage packets at once", default_value = "1000")]
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

    #[structopt(short = "i", long = "s-interval", help = "How often send metrics to Graphite", default_value = "30000")]
    interval: Option<u64>, // u64 has a special meaning in structopt

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

    #[structopt(long = "nodes")]
    /// List of nodes to replicate metrics to
    nodes: Vec<SocketAddr>,

    #[structopt(short = "t", long = "snapshot-interval", help = "Snapshot sending interval, ms", default_value = "1000")]
    snapshot_interval: usize,

    #[structopt(short = "Q", long = "query", help = "Connect to server wiht a query")]
    query: Option<PeerCommand>,

    //#[structopt(short = "P", long = "stats-prefix", help = "Prefix to add to own metrics", default_value=".brubers")]
    //stats_prefix: String,
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
        nodes,
        snapshot_interval,
        query,
    } = Options::from_args();

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

    if let Some(command) = query {
        let command = PeerCommandClient::new(peer_listen, &handle, command);
        core.run(command.into_future()).unwrap();
        return
    }

    std::thread::spawn(move ||{
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let stimer = Interval::new(Duration::from_millis((s_interval * 1000f64) as u64), &handle).unwrap();
        let stats = stimer
            .map_err(|e| GeneralError::Io(e))
            .for_each( move |()| {
                let egress = EGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let ingress = INGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let ingress_m = INGRESS_METRICS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let agr_errors = AGG_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let parse_errors = PARSE_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let drops = DROPS.swap(0, Ordering::Relaxed) as f64 / s_interval;

                println!("egress/ingress/ingress-m/a-errors/p-errors/drops:\n{:.2}/{:.2}/{:.2}/{:.2}/{:.2}/{:.2}", egress, ingress, ingress_m, agr_errors, parse_errors, drops);
                Ok(())
            }).then(|_|Ok(()));

        handle.spawn(stats);
        core.run(empty::<(), ()>()).unwrap();
    });

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

    for node in nodes.iter().cloned() {
        let tchans = chans.clone();
        let shandle = handle.clone();
        let snapshot = PeerSnapshotClient::new(node.clone(), Duration::from_millis(snapshot_interval as u64), &shandle, &tchans)
            .into_future()
            .map_err(|e| {
                //println!("error sending snapshot to {:?}: {:?}", node, e);
                println!("error sending snapshot: {:?}", e);
            });
        handle.spawn(snapshot);
    }

    let peer_server = PeerServer::new(peer_listen, &handle, &chans, &nodes);
    // TODO restart server after error
    handle.spawn(peer_server
                 .into_future()
                 .then(|e|{println!("shot server gone with error: {:?}", e); Ok(())}));
    // TODO (maybe) change to option, not-depending on number of nodes
    if nodes.len() > 0 {
        CAN_LEADER.store(true, Ordering::SeqCst);
        let mut consensus = ConsulConsensus::new(
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
    let timer = timer
        .map_err(|e| GeneralError::Io(e))
        .for_each(move |()| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH).map_err(|e| GeneralError::Time(e))?;
            let ts = ts.as_secs().to_string();

            let addr = backend_addr.clone();
            let tchans = tchans.clone();
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
                        println!("Leader sending metrics");
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
                                    .map_err(|e|{println!("send error: {:?}", e); e})
                            });

                        core.run(pusher.then(|_|Ok::<(), ()>(()))).unwrap_or_else(|e|println!("Failed to send to graphite: {:?}", e));
                    } else {
                        core.run(join_all(metrics).then(|_|Ok::<(), ()>(()))).unwrap_or_else(|e|println!("Failed to join aggregated metrics: {:?}", e));
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
                                        println!("exec error: {:?}", e);
                                    });
                                    b = BytesMut::with_capacity(bufsize*messages);
                                }
                                b.put(&message_vec[i][0..len]);
                            };
                        } else {
                            println!("receive error: {:?} {:?}", res, ::std::io::Error::last_os_error())
                        }
                    }
                }
            }).expect("starting multimsg thread");
    }
    core.run(timer).unwrap();
}
