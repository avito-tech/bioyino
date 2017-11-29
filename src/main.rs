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
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate bytes;
extern crate num_cpus;
extern crate resolve;
extern crate net2;

// Other
extern crate itertools;
extern crate combine;
extern crate num;
extern crate quantiles;

pub mod parser;
pub mod errors;
pub mod metric;
pub mod codec;
pub mod bigint;
pub mod task;

use std::time::{self, Duration, SystemTime};
use std::thread;
use std::net::SocketAddr;

use structopt::StructOpt;

use futures::{Stream, Future, Sink, empty, IntoFuture};
use futures::future::join_all;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, TcpStream};
use tokio_io::AsyncRead;

use resolve::resolver;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use errors::GeneralError;
use metric::Metric;
use codec::{StatsdServer, CarbonCodec};
use failure::Fail;
use std::collections::HashMap;
use futures::sync::oneshot;
use bytes::BytesMut;

use task::Task;
use std::cell::RefCell;

pub type Cache = HashMap<String, Metric<f64>>;
thread_local!(static CACHE: RefCell<HashMap<String, Metric<f64>>> = RefCell::new(HashMap::with_capacity(8192)));

use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};
pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

pub const EPSILON: f64 = 0.01;

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

#[derive(StructOpt, Debug)]
#[structopt(about = "StatsD-compatible async metric aggregator")]
struct Options {
    #[structopt(short = "l", long = "listen", help = "Address and port to listen to")]
    listen: SocketAddr,

    #[structopt( short = "b", long = "backend", help = "IP and port of a backend to send aggregated data to.", value_name = "IP:PORT")]
    backend: String,

    #[structopt(short = "n", long = "nthreads", help = "Number of network worker threads, use 0 to use all CPU cores", default_value = "4")]
    nthreads: usize,

    #[structopt(short = "c", long = "cthreads", help = "Number of counting threads, use 0 to use all CPU cores", default_value = "4")]
    cthreads: usize,

    #[structopt(short = "p", long = "pool", help = "Socket pool size", default_value = "4")]
    snum: usize,

    #[structopt(short = "g", long = "greens", help = "Number of green threads per worker hread", default_value = "4")]
    greens: usize,

    #[structopt(short = "i", long = "s-interval", help = "How often send metrics to Graphite", default_value = "30000")]
    interval: Option<u64>, // u64 has a special meaning in structopt

    #[structopt(short = "s", long = "interval", help = "How often to gather own stats", default_value = "5000")]
    s_interval: Option<u64>,

    // default = standard ethernet MTU + a little
    #[structopt(short = "B", long = "bufsize", help = "buffer size for single packet", default_value = "1500")]
    bufsize: usize,

    #[structopt(short = "q", long = "task-queue-size", help = "queue size for tasks on single counting thread", default_value = "2048")]
    task_queue_size: usize,

    //#[structopt(short = "P", long = "stats-prefix", help = "Prefix to add to own metrics", default_value=".brubers")]
    //stats_prefix: String,
}

fn main() {
    let Options {
        listen,
        backend,
        mut nthreads,
        mut cthreads,
        snum,
        greens,
        interval,
        s_interval,
        bufsize,
        task_queue_size,

    } = Options::from_args();

    if nthreads == 0 {
        nthreads = num_cpus::get();
    }

    if cthreads == 0 {
        cthreads = num_cpus::get();
    }

    if greens == 0 {
        panic!("Number of green threads cannot be zero")
    }

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(interval.unwrap()), &handle).unwrap();

    let addr = try_resolve(&backend);

    let s_interval = s_interval.unwrap() as f64 / 1000f64;

    std::thread::spawn(move ||{
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let stimer = Interval::new(Duration::from_millis((s_interval * 1000f64) as u64), &handle).unwrap();
        let stats = stimer
            .map_err(|e| GeneralError::Io(e))
            .for_each( move |()| {
                let egress = EGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let ingress = INGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let agr_errors = AGG_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let parse_errors = PARSE_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
                let drops = DROPS.swap(0, Ordering::Relaxed) as f64 / s_interval;

                println!("egress/ingress/a-errors/p-errors/drops: {:.2}/{:.2}/{:.2}/{:.2}/{:.2}", egress, ingress, agr_errors, parse_errors, drops);
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

                use futures::future::{ok,lazy};
                let future = rx.for_each(move |task: Task|{
                    lazy(|| ok(
                            task.run()
                            ))
                });
                core.run(future).unwrap();
            }).expect("starting counting worker thread");
    }

    let tchans = chans.clone();
    let timer = timer
        .map_err(|e| GeneralError::Io(e))
        .for_each(move |()| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH).map_err(|e| GeneralError::Time(e))?;
            let ts = ts.as_secs().to_string();

            let addr = addr.clone();
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
                                use futures::future::{Loop, loop_fn, ok};
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

                    let pusher = TcpStream::connect(&addr, &handle)
                        .map_err(|e|e.compat().into())
                        // waitt for both: all results from all channels and tcp connection to be ready
                        .join(future.map_err(|e|e.compat().into()))
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
                        });

                    core.run(pusher.then(|_|Ok::<(), ()>(()))).unwrap_or_else(|e|println!("Failed to send to graphite: {:?}", e));
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
                            i);

                        handle.spawn(server.into_future()); // TODO: process io error
                    }
                }

                core.run(::futures::future::empty::<(), ()>()).unwrap();
            }).expect("creating UDP reader thread");
    }
    core.run(timer).unwrap();
}
