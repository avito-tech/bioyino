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
extern crate libc;
extern crate hyper;

// Other
extern crate itertools;
extern crate combine;
extern crate num;
extern crate quantiles;
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod parser;
pub mod errors;
pub mod metric;
pub mod codec;
pub mod bigint;
pub mod task;

use std::collections::HashMap;
use std::time::{self, Duration, SystemTime};
use std::thread;
use std::net::{SocketAddr, IpAddr};
use std::cell::RefCell;
use std::sync::{Arc,Mutex};
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use failure::Fail;
use structopt::StructOpt;

use bytes::BytesMut;
use futures::{Stream, Future, Sink, empty, IntoFuture};
use futures::future::{join_all, Loop, loop_fn, ok, lazy, Executor};
use futures::sync::oneshot;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, TcpStream};
use tokio_io::AsyncRead;
use tokio_io::codec::length_delimited;

use resolve::resolver;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use hyper::header::{ContentType, ContentLength};
use serde_json::{Value, from_slice};

use errors::GeneralError;
use metric::Metric;
use codec::{StatsdServer, CarbonCodec};
use task::Task;

pub type Float = f64;
pub type Cache = HashMap<String, Metric<Float>>;
thread_local!(static CACHE: RefCell<HashMap<String, Metric<Float>>> = RefCell::new(HashMap::with_capacity(8192)));

pub static PARSE_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static INGRESS_METRICS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

pub const EPSILON: f64 = 0.01;
pub const KEY: &'static str = "service/bioyino/lock";

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

    #[structopt(short = "P", long = "peer-listen", help = "Address and port to listen for snapshot requests", default_value="127.0.0.1:8136")]
    peer_listen: SocketAddr,

    #[structopt( short = "b", long = "backend", help = "IP and port of a backend to send aggregated data to.", value_name = "IP:PORT")]
    backend: String,

    //#[structopt(short = "n", long = "nthreads", help = "Number of network worker threads, use 0 to use all CPU cores, use any negative to use none", default_value = "4")]
    #[structopt(short = "n", long = "nthreads", help = "Number of network worker threads, use 0 to use all CPU cores, use any negative to use none", default_value = "4")]
    nthreads: isize,

    #[structopt(short = "m", long = "mthreads", help = "Number of multimessage(revcmmsg) worker threads, use 0 to use no threads of this type", default_value = "0")]
    mthreads: usize,

    #[structopt(short = "M", long = "msize", help = "multimessage packets at once", default_value = "1000")]
    msize: usize,

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

    #[structopt(short = "A", long = "agent", help = "Consul agent address", default_value = "127.0.0.1:8500")]
    agent: SocketAddr,

    #[structopt(long = "nodes", help = "List of foreign nodes")]
    nodes: Vec<SocketAddr>,

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
        nodes
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

    let is_leader = Arc::new(Mutex::new(false));

    for node in nodes.iter().cloned() {
        let tchans = chans.clone();
        let shandle = handle.clone();
        // Create periodic metric sender
        // TODO probably change this to send everything on signel timer
        let snapshot_timer = Interval::new(Duration::from_millis(5000), &handle).unwrap();

        let snapshot = snapshot_timer
            .map_err(|e| GeneralError::Io(e))
            .for_each(move |()| {
                let handle = shandle.clone();
                let tchans = tchans.clone();
                let metrics = tchans.clone().into_iter().map(|chan| {
                    let (tx, rx) = oneshot::channel();
                    shandle.spawn(chan.send(Task::Snapshot(tx)).then(|_|Ok(())));
                    rx
                        .map_err(|_|GeneralError::FutureSend)
                })
                .collect::<Vec<_>>();
                let future = join_all(metrics).and_then(move |mut metrics|{
                    metrics.retain(|m|m.len() > 0);
                    Ok(metrics)
                });

                TcpStream::connect(&node, &handle)
                    .map_err(|e|GeneralError::Io(e))
                    // waitt for both: all results from all channels and tcp connection to be ready
                    .join(future
                          .map_err(|_|GeneralError::FutureSend.into()))
                    // waitt for both: all results from all channels and tcp connection to be ready
                    .and_then(move |(conn, metrics)| {
                        let serialized = ::serde_json::to_string(&metrics).expect("deserializing metric");
                        let writer = length_delimited::Builder::new()
                            .length_field_length(4)
                            .new_write(conn);

                        writer.send(serialized)
                            .map_err(|e|GeneralError::Io(e))
                    })
                .then(|e|{ if e.is_err() {println!("shot send error: {:?}", e)}; Ok(())})
            });

        handle.spawn(snapshot.then(|_|Ok(())));
    }

    let snapshots = Arc::new(Mutex::new(HashMap::<IpAddr, Vec<Cache>>::with_capacity(nodes.len())));

    let shots = snapshots.clone();
    let snapshot_server = ::tokio_core::net::TcpListener::bind(&peer_listen,&handle)
        .expect("listening peer port")
        .incoming()
        .map_err(|_|())
        .for_each(move |(conn, inaddr)| {
            let reader = length_delimited::Builder::new()
                .length_field_length(4)
                .new_read(conn)
                .map_err(|e|GeneralError::Io(e));

            let ip = inaddr.ip();
            let shots = shots.clone();
            reader.for_each(move |m|{
                // TODO we don't really need serialization here until we decide to send metrics
                match ::serde_json::from_slice(&m) {
                    Ok(shot) => {
                        let mut shots = shots.lock().unwrap();
                        shots.insert(ip.clone(), shot);  // TODO: ignore err;  replacing previous shapshot is ok
                    }
                    Err(e) => println!("error parsing snapshot: {:?}", e),
                };
                Ok(())
            })
            .then(|e|{if e.is_err() {println!("shot server recv error: {:?}", e)};Ok(())})
        });

    handle.spawn(snapshot_server.then(|e|{println!("shot server gone: {:?}", e); Ok(())}));
    if nodes.len() > 0 {
        // create HTTP client for consul agent leader
        let consul = ::hyper::Client::new(&handle);
        let mut session_req = ::hyper::Request::new(
            ::hyper::Method::Put,
            format!("http://{}/v1/session/create", agent).parse().expect("bad session create url")
            );

        let b = "{\"TTL\": \"11s\", \"LockDelay\": \"11s\"}";
        session_req.set_body(b);
        // Override sending request as multipart
        session_req.headers_mut().set(ContentLength(b.len() as u64));
        session_req.headers_mut().set(ContentType::form_url_encoded());
        let shandle = handle.clone();

        let ses_is_leader = is_leader.clone();
        let c_session = consul
            .request(session_req)
            .and_then(move |resp|{
                resp.body().concat2().and_then(move |body|{
                    let resp: Value = from_slice(&body).expect("parsing consul request");
                    println!("Session: {:?}", resp.as_object().unwrap().get("ID").unwrap().as_str().unwrap());
                    Ok(resp.as_object().unwrap().get("ID").unwrap().as_str().unwrap().to_string())
                })
            })
        .map_err(|e|{
            println!("session creation error: {:?}", e);
            e
        })
        .and_then(move |sid|{
            let is_leader = ses_is_leader.clone();
            let handle = shandle.clone();
            // Create session renew future to refresh session every 4 sec
            let s_renew_timer = Interval::new(Duration::from_millis(4000), &handle).unwrap();
            let shandle = handle.clone();
            let sid1 = sid.clone();
            let session_renew = s_renew_timer
                .map_err(|_|())
                .for_each(move |_| {
                    let mut renew_req = ::hyper::Request::new(
                        ::hyper::Method::Put,
                        format!("http://{}/v1/session/renew/{}", agent, sid1).parse().expect("bad session renew url")
                        );
                    let b = "{\"TTL\": \"11s\"";
                    renew_req.set_body(b);
                    renew_req.headers_mut().set(ContentLength(b.len() as u64));
                    renew_req.headers_mut().set(ContentType::form_url_encoded());

                    let renew_client = ::hyper::Client::new(&shandle);
                    renew_client.request(renew_req)
                        .and_then(move |resp|{
                            if resp.status() != hyper::StatusCode::Ok {
                                let status = resp.status().clone();
                                let body = resp.body().concat2().wait().expect("decode body");
                                println!("renew error: {:?} {:?}", status, String::from_utf8(body.to_vec()));
                            };
                            Ok(())
                        }).map_err(|e|{println!("session renew error: {:?}", e);()})
                });

            handle.spawn(session_renew.then(|res|{if res.is_err() {println!("renew error: {:?}", res)}; Ok(())}));
            // create key acquire future
            let shandle = handle.clone();
            let acquire_timer = Interval::new(Duration::from_millis(5000), &handle).unwrap();
            let acquire = acquire_timer
                .map_err(|_|())
                .for_each(move |_| {

                    let is_leader = is_leader.clone();
                    let req = ::hyper::Request::new(
                        ::hyper::Method::Put,
                        format!("http://{}/v1/kv/{}/?acquire={}", agent, KEY, sid).parse().expect("bad key acquire url")
                        );

                    let acquire_client = ::hyper::Client::new(&shandle);
                    acquire_client.request(req).and_then(move |resp|{
                        resp.body().concat2().and_then(move |body|{
                            let resp: Value = from_slice(&body).expect("parsing consul request");
                            let acquired = resp.as_bool().unwrap();
                            {
                                let mut is_leader = is_leader.lock().unwrap();
                                if *is_leader != acquired {
                                    println!("Leader state change: {} -> {}", *is_leader,  acquired);
                                }
                                *is_leader = acquired;
                            }
                            Ok(())
                        })
                    })
                    .map_err(|e|{println!("consul acquire error: {:?}", e);()})
                });

            handle.spawn(acquire.map_err(|e|{println!("consul acquire error: {:?}", e);()}));
            Ok(())
        });

        handle.spawn(c_session.map_err(|e|{println!("consul session error: {:?}", e);()}));
    } else {
        let mut is_leader = is_leader.lock().unwrap();
        *is_leader = true;
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

            let shots = snapshots.clone();

            let is_leader = is_leader.clone();
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
                    let is_leader = is_leader.lock().unwrap();
                    if *is_leader {
                        println!("Leader sending metrics");
                        let future = join_all(metrics).and_then(move |metrics|{
                            // Join all metrics into hashmap by only pushing everything to vector
                            let mut metrics = metrics
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

                            let mut shots = shots.lock().unwrap();
                            shots.drain()
                                .map(|(_, mut shot_maps)|{ // each shot contains vector of hashmaps
                                    let len = shot_maps.len();
                                    shot_maps
                                        .drain(0..len) // drain the vector getting hashmaps
                                        .map(|map|{
                                            map.into_iter()
                                                .map(|(name, metric)|{
                                                    let entry = metrics.entry(name).or_insert(Vec::new());
                                                    entry.push(metric.clone());
                                                }).last()
                                        }).last()
                                }).last();
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
                            // waitt for both: all results from all channels and tcp connection to be ready

                            /*
                               TcpStream::connect(&addr, &handle)
                               .map_err(|e|e.compat().into())
                            // waitt for both: all results from all channels and tcp connection to be ready
                            .join(future.map_err(|e|e.compat().into()))
                            */
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
