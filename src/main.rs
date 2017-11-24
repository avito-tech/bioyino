// General
extern crate error_chain;
#[macro_use]
extern crate derive_error_chain;
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
extern crate combine;
extern crate chashmap;
extern crate num;
extern crate smallvec;
extern crate quantiles;

pub mod parser;
pub mod errors;
pub mod metric;
pub mod codec;
pub mod bigint;

use std::sync::Arc;
use std::time::{self, Duration, SystemTime};
use std::thread;
use std::net::SocketAddr;

use structopt::StructOpt;
use chashmap::CHashMap;

use futures::{Stream, Future, Sink, empty, IntoFuture};
use futures::future::Executor;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, TcpStream};
use tokio_io::AsyncRead;

use resolve::resolver;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use bytes::Bytes;
use errors::GeneralError;
use metric::Metric;
use codec::{StatsdServer, CarbonCodec, CarCodec};
use failure::{ResultExt, Fail};

use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};
pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;

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

    #[structopt(short = "n", long = "nthreads", help = "Number of worker threads, use 0 to use all CPU cores", default_value = "0")]
    nthreads: usize,

    #[structopt(short = "p", long = "pool", help = "Socket pool size", default_value = "4")]
    snum: usize,

    #[structopt(short = "g", long = "greens", help = "Number of green threads per worker hread", default_value = "4")]
    greens: usize,

    #[structopt(short = "i", long = "s-interval", help = "How often send metrics to Graphite", default_value = "30000")]
    interval: Option<u64>, // u64 has a special meaning in structopt

    #[structopt(short = "s", long = "interval", help = "How often to gather own stats", default_value = "5000")]
    s_interval: Option<u64>,

    // default = standard ethernet MTU + a little
    #[structopt(short = "B", long = "bufsize", help = "buffer size for each green thread", default_value = "1500")]
    bufsize: usize,

    //#[structopt(short = "P", long = "stats-prefix", help = "Prefix to add to own metrics", default_value=".brubers")]
    //stats_prefix: String,
}

fn main() {
    let Options {
        listen,
        backend,
        mut nthreads,
        snum,
        greens,
        interval,
        s_interval,
        bufsize,

    } = Options::from_args();

    if nthreads == 0 {
        nthreads = num_cpus::get();
    }

    if greens == 0 {
        panic!("Number of green threads cannot be zero")
    }

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(interval.unwrap()), &handle).unwrap();
    let cache = Arc::new(CHashMap::<String, Metric<f64>>::new());

    // Main thread should do the backend sending
    let addr = try_resolve(&backend);
    let tcache = cache.clone();

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

                println!("egress/ingress/a-errors/p-errors: {:.2}/{:.2}/{:.2}/{:.2}", egress, ingress, agr_errors, parse_errors);
                Ok(())
            }).then(|_|Ok(()));

        handle.spawn(stats);
        core.run(empty::<(), ()>()).unwrap();
    });

    let timer = timer
        .map_err(|e| GeneralError::Io(e))
        .for_each(move |()| {
            let ts = SystemTime::now()
                .duration_since(time::UNIX_EPOCH).map_err(|e| GeneralError::Time(e))?;
            let ts = ts.as_secs().to_string();
            let old_cache = tcache.clear();
            let len = old_cache.len();


            let addr = addr.clone();
            thread::spawn(move ||{
                let mut core = Core::new().unwrap();
                let handle = core.handle();

                let aggregated = old_cache.into_iter().flat_map(move |(name, value)|{
                    let ts = ts.clone();
                    value.into_iter().map(move |(suffix, value)|{
                        (name.clone() + suffix, value, ts.clone())
                    })
                }).collect::<Vec<_>>();
                //println!("Cache len: {:?} Agg len: {:?} {:?}", len, aggregated.len(), ts1.elapsed());

                let pusher = TcpStream::connect(&addr, &handle)
                    .map_err(|e|e.compat().into())
                    .and_then(move |conn| {
                        conn.set_send_buffer_size(4096);
                        //let writer = conn.framed(CarbonCodec::default());
                        let writer = conn.framed(CarCodec);

                        let s = ::futures::stream::iter_ok(aggregated);
                        let s = s
                            .inspect(|_|{// @ (name, value, ts)|{
                                ////let flushed = value.flush();
                                ////let data = (name.clone(), flushed, ts.clone());
                                ////Arc::new(data)
                                //data

                                EGRESS.fetch_add(1, Ordering::Relaxed);
                            })
                            .map_err(|e| GeneralError::Io(e));

                            writer.send_all(s)
                            });

                    handle.spawn(pusher.then(|_|Ok(())));
                    //    Ok(())

                    core.run(empty::<(), ()>());
                    });
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

            let cache = cache.clone();
            for i in 0..nthreads {
                // Each thread gets the clone of a socket pool
                let sockets = sockets
                    .iter()
                    .map(|s| s.try_clone().unwrap())
                    .collect::<Vec<_>>();

                let cache = cache.clone();
                thread::Builder::new()
                    .name(format!("bioyino_worker{}", i).into())
                    .spawn(move || {

                        // each thread runs it's own core
                        let mut core = Core::new().unwrap();
                        let handle = core.handle();
                        use bytes::{BytesMut, BufMut};
                        let mut buf = BytesMut::with_capacity(bufsize*greens);
                        let len = buf.remaining_mut();
                        unsafe { buf.advance_mut(len) }

                        // Inside each green thread
                        for _ in 0..greens {
                            let buf_part = buf.split_to(bufsize);
                            for socket in sockets.iter() {
                                // create UDP listener
                                let socket = socket.try_clone().expect("cloning socket");
                                let socket = UdpSocket::from_socket(socket, &handle).expect("adding socket to event loop");

                                let server = StatsdServer::new(socket, &handle, handle.remote().clone(),cache.clone(), buf_part.clone());

                                handle.spawn(server.into_future()); // TODO: process io error
                            }
                        }
                        core.run(::futures::future::empty::<(), ()>()).unwrap();
                    }).expect("creating worker thread");
            }
            core.run(timer).unwrap();
        }
