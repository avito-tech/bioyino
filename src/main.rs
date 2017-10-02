// General
extern crate error_chain;
#[macro_use]
extern crate derive_error_chain;
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

pub mod parser;
pub mod errors;
pub mod metric;
pub mod codec;

use std::sync::Arc;
use std::time::{Duration, SystemTime, self};
use std::thread;
use std::net::SocketAddr;

use structopt::StructOpt;
use chashmap::CHashMap;

use futures::{Stream, Future, Sink};
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, TcpStream};
use tokio_io::AsyncRead;

use resolve::resolver;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use errors::ErrorKind;
use metric::Metric;
use codec::{StatsdCodec, CarbonCodec};

use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};
pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;

pub fn try_resolve(s: &str) -> SocketAddr {
    s.parse()
        .unwrap_or_else(|_| {
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

    #[structopt(short = "b", long = "backend", help = "IP and port of a backend to send aggregated data to.", value_name="IP:PORT")]
    backend: String,

    #[structopt(short = "n", long = "nthreads", help = "Number of worker threads, use 0 to use all CPU cores", default_value = "0")]
    nthreads: usize,

    #[structopt(short = "i", long = "sampling interval", help = "How often send metrics to Graphite", default_value = "10000")]
    interval: Option<u64>, // u64 has a special meaning in structopt

    #[structopt(short = "s", long = "interval", help = "How often to gather own stats", default_value = "5000")]
    s_interval: Option<u64>,

    //#[structopt(short = "P", long = "stats-prefix", help = "Prefix to add to own metrics", default_value=".brubers")]
    //stats_prefix: String,
}

fn main() {
    let mut opts = Options::from_args();

    if opts.nthreads == 0 {
        opts.nthreads = num_cpus::get();
    }

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(opts.interval.unwrap()), &handle).unwrap();
    let stimer = Interval::new(Duration::from_millis(opts.s_interval.unwrap()), &handle).unwrap();
    let cache = Arc::new(CHashMap::<String, Metric>::new());

    // Main thread should do the backend sending
    let addr = try_resolve(&opts.backend);
    let tcache = cache.clone();

    let s_interval = opts.s_interval.unwrap() as f64 / 1000f64;

    let stats = stimer
        .map_err(|e| ErrorKind::Io(e))
        .for_each( move |()| {
            let egress = EGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
            let ingress = INGRESS.swap(0, Ordering::Relaxed) as f64 / s_interval;
            let agr_errors = AGG_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;
            let parse_errors = PARSE_ERRORS.swap(0, Ordering::Relaxed) as f64 / s_interval;

            println!("egress/ingress/a-errors/p-errors: {:.2}/{:.2}/{:.2}/{:.2}", egress, ingress, agr_errors, parse_errors);
            Ok(())
        }).then(|_|Ok(()));

    handle.spawn(stats);

    let timer = timer
        .map_err(|e| ErrorKind::Io(e))
        .for_each( move |()| {


            let tcache = tcache.clone();
            let ts = try!(SystemTime::now()
                          .duration_since(time::UNIX_EPOCH).map_err(|e| ErrorKind::Time(e)));
            let ts = ts.as_secs().to_string();
            let pusher = TcpStream::connect(&addr, &handle)
                .map_err(|e| ErrorKind::Io(e).into())
                .and_then(move |conn| {
                    let writer = conn.framed(CarbonCodec);

                    let old_cache = tcache.clear();
                    let iterator = old_cache.into_iter().map(|kv|{
                        Ok::<_, ()>(kv)
                    });

                    let s = ::futures::stream::iter(iterator);
                    let s = s.map(move |(name, mut value)|{
                        let flushed = value.flush();
                        let data = (name.clone(), flushed, ts.clone());
                        Arc::new(data)
                    })
                    .map_err(|_| ErrorKind::Future);

                    writer.send_all(s)
                });

            handle.spawn(pusher.then(|_|Ok(())));
            Ok(())
        });

    // Bind to server socket
    let socket = UdpBuilder::new_v4().unwrap();
    socket.reuse_address(true).unwrap();
    socket.reuse_port(true).unwrap();
    let socket = socket.bind(&opts.listen).unwrap();

    // Start threads
    for _ in 0..opts.nthreads {
        let socket = socket.try_clone().unwrap();
        // each thread task needs it's own reference to cache
        let cache = cache.clone();
        thread::spawn(move || {
            // each thread runs it's own core
            let mut core = Core::new().unwrap();
            let handle = core.handle();

            let cache = cache.clone();
            // create server now
            let socket = UdpSocket::from_socket(socket, &handle).unwrap();
            let server = socket.framed(StatsdCodec);
            let server = server.for_each(move |metrics| {
                if let Some(metrics) = metrics {
                    metrics
                        .into_iter() // for each metric in vector
                        .map(|(name, metric)| {
                            cache.alter(name, |old| {
                                match old {
                                    None => Some(metric),
                                    Some(mut old) => {
                                        let res = old.aggregate(metric);
                                        if res.is_err() {
                                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Some(old)
                                    }
                                }
                            })
                        })
                    .last();
                };
                Ok(())
            });

            core.run(server).unwrap();
        });
    }
    core.run(timer).unwrap();
}
