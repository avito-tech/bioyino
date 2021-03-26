use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use capnp::message::{Builder, ReaderOptions};
use slog::{debug, error, info, o, warn, Logger};
use thiserror::Error;

use crossbeam_channel::Sender;
use futures::channel::oneshot;

use futures::future::{TryFutureExt, join_all};
use ring_channel::{ring_channel, RingReceiver, RingSender};
use tokio::{net::{TcpListener, TcpStream},
spawn,
time::{interval_at, Instant},
io::AsyncWriteExt,
};

use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_stream::StreamExt;

use bioyino_metric::protocol_capnp::{message as cmsg, message::Builder as CBuilder};
use bioyino_metric::protocol_v2_capnp::{message as cmsgv2, message::Builder as CBuilderV2};
use bioyino_metric::{Metric, metric::ProtocolVersion};

use crate::slow_task::SlowTask;
use crate::fast_task::FastTask;
use crate::util::{bound_stream, resolve_with_port, Backoff};
use crate::config::System;
use crate::{s, Cache, Float, ConsensusKind, IS_LEADER};

const CAPNP_READER_OPTIONS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: Some(8 * 1024 * 1024 * 1024),
    nesting_limit: 16,
};

#[derive(Error, Debug)]
pub enum PeerError {
    #[error("I/O error: {}", _0)]
    Io(#[from] ::std::io::Error),

    #[error("Error when creating timer: {}", _0)]
    Timer(#[from] ::tokio1::timer::Error),

    #[error("error sending task to worker thread")]
    TaskSend,

    #[error("server received incorrect message")]
    BadMessage,

    #[error("bad command")]
    BadCommand,

    #[error("response not sent")]
    Response,

    #[error("shapshot has been dropped")]
    SnapshotDropped,

    #[error("shapshot write timeout")]
    SnapshotWriteTimeout,

    #[error("decoding capnp error: {}", _0)]
    Capnp(#[from] capnp::Error),

    #[error("decoding capnp schema error: {}", _0)]
    CapnpSchema(#[from] capnp::NotInSchema),

    #[error("decoding metric error: {}", _0)]
    Metric(#[from] bioyino_metric::MetricError),

    #[error("other peer error: {}", _0)]
    Other(#[from] crate::util::OtherError),

    #[error("getting connections from socket")]
    TcpIncoming,
}

#[derive(Clone, Debug)]
pub struct NativeProtocolServer {
    log: Logger,
    listen: SocketAddr,
    chan: Sender<SlowTask>
}

impl NativeProtocolServer {
    pub fn new(log: Logger, listen: SocketAddr, chan: Sender<SlowTask>) -> Self {
        Self {
            log: log.new(o!("source"=>"canproto-peer-server", "ip"=>format!("{}", listen.clone()))),
            listen,
            chan,
        }
    }

    pub(crate) async fn run(self) -> Result<(), PeerError> {
        let Self { log, listen, chan } = self;
        let listener = TcpListener::bind(&listen).await?;

        loop {
            let (conn, peer_addr) = listener.accept().await?;
            let mut conn = conn.compat();

            let log = log.new(o!("remote"=>peer_addr));

            //           debug!(log, "got new connection");
            let elog = log.clone();
            let chan = chan.clone();
            let receiver = async move {
                let elog = log.clone();
                //let transport = capnp_futures::ReadStream::new(&mut conn, CAPNP_READER_OPTIONS);
                //while let Some(reader) = futures::stream::TryStreamExt::try_next(&mut transport).await?
                loop {
                    let reader = if let Ok(reader) = capnp_futures::serialize::read_message(&mut conn, CAPNP_READER_OPTIONS).await {
                        reader
                    } else {
                        break;
                    };
                    tokio::task::block_in_place(|| -> Result<(), PeerError> {
                        let task = {
                            let elog = elog.clone();
                            //                        debug!(log, "received peer message");
                            parse_capnp(log.clone(), reader).map_err(move |e| {
                                warn!(elog, "bad incoming message"; "error" => e.to_string());
                                e
                            })?
                        };
                        if let Some(task) = task {
                            chan.send(task)
                                .map_err(|_| {
                                    s!(queue_errors);
                                    PeerError::TaskSend
                                })?;
                        }
                        Ok(())
                    })?;
                }
                Ok::<(), PeerError>(())
            }
            .map_err(move |e| {
                s!(peer_errors);
                warn!(elog, "snapshot server reading error"; "error"=>format!("{:?}", e));
            });

            spawn(receiver);
        }
    }
}

fn parse_capnp(log: Logger, reader: capnp::message::Reader<capnp::serialize::OwnedSegments>) -> Result<Option<SlowTask>, PeerError> {
    if let Ok(reader) = reader.get_root::<cmsgv2::Reader>() {
        return match reader.which()? {
            cmsgv2::Noop(()) => {
                Ok(None)
            }
            cmsgv2::Snapshot(reader) => {
                let reader = reader?;
                let mut metrics = Vec::new();
                reader
                    .iter()
                    .map(|reader| Metric::<Float>::from_capnp(reader).map(|(name, metric)| metrics.push((name, metric))))
                    .last();

                debug!(log, "received snapshot v2"; "metrics"=>format!("{}", metrics.len()));
                Ok(Some(SlowTask::AddSnapshot(metrics)))
            }
        }
    }

    let reader = reader.get_root::<cmsg::Reader>()?;
    match reader.which()? {
        cmsg::Single(reader) => {
            let reader = reader?;
            let (name, metric) = Metric::<Float>::from_capnp_v1(reader)?;
            Ok(Some(SlowTask::AddMetric(name, metric)))
        }
        cmsg::Multi(reader) => {
            let reader = reader?;
            let mut metrics = Vec::new();
            reader
                .iter()
                .map(|reader| Metric::<Float>::from_capnp_v1(reader).map(|(name, metric)| metrics.push((name, metric))))
                .last();
            Ok(Some(SlowTask::AddMetrics(metrics)))
        }
        cmsg::Snapshot(reader) => {
            let reader = reader?;
            let mut metrics = Vec::new();
            reader
                .iter()
                .map(|reader| Metric::<Float>::from_capnp_v1(reader).map(|(name, metric)| metrics.push((name, metric))))
                .last();

            debug!(log, "received snapshot v1"; "metrics"=>format!("{}", metrics.len()));
            Ok(Some(SlowTask::AddSnapshot(metrics)))
        }
    }
}

pub struct NativeProtocolSnapshot {
    log: Logger,
    config: Arc<System>,
    interval: Duration,
    fast_chans: Vec<Sender<FastTask>>,
    slow_chan: Sender<SlowTask>,
    snapshots: NonZeroUsize,
}

impl NativeProtocolSnapshot {
    pub fn new(log: &Logger,
        //nodes: Vec<String>, client_bind: Option<SocketAddr>,
        //interval: Duration,
        config: Arc<System>,
        fast_chans: &[Sender<FastTask>],
        slow_chan: Sender<SlowTask>) -> Self {
        let snapshots = if config.network.max_snapshots == 0 {
            warn!(log, "max_snapshots set to 0 in config, this is incorrect, real value is set to 1");
            1
        } else {
            config.network.max_snapshots
        };
        let interval = Duration::from_millis(config.network.snapshot_interval as u64);
        Self {
            log: log.new(o!("source"=>"peer-client")),
            config,
            fast_chans: fast_chans.to_owned(),
            slow_chan,
            snapshots: NonZeroUsize::new(snapshots).unwrap(),
            interval,
        }
    }

    pub(crate) async fn run(self) -> Result<(), PeerError> {
        let Self {
            log,
            config,
            fast_chans,
            slow_chan,
            snapshots,
            interval,
        } = self;
        // Snapshots come every `interval`. When one of the nodes goes down it is possible to leak
        // all the memory if interval is less than backoff period because in this case snapshots will
        // come faster than sender can handle them. Though backoff period of the length of one
        // snapshot is too short and also unacceptable.
        // So to avoid infinite memory leaking we break the procedures of taking
        // a snapshot and sending it to nodes with ring buffer, storing only latest N snapshots.
        // Since all metrics is lost after some time on remote node, it is only reasonable to store
        // few latest intervals. This is the reason of using a ring buffer instead of a channel.

        let mut node_chans = config.network.nodes
            .iter()
            .map(|address| {
                let (tx, rx): (RingSender<Vec<Arc<Cache>>>, RingReceiver<Vec<Arc<Cache>>>) = ring_channel(snapshots);
                let log = log.clone();
                let options = SnapshotClientOptions {
                    address: address.clone(),
                    bind: config.network.peer_client_bind,
                    proto_version: config.network.peer_protocol.clone(),
                };

                let client = SnapshotSender::new(rx, options.clone(), log.clone());
                spawn(client.run());
                tx
            })
        .collect::<Vec<_>>();

        let mut timer = interval_at(Instant::now() + interval, interval);
        loop {
            timer.tick().await;
            // send snapshot requests to workers
            let mut outs = Vec::new();
            tokio::task::block_in_place(||{
                for chan in &fast_chans {
                    let (tx, rx) = oneshot::channel();
                    chan.send(FastTask::TakeSnapshot(tx)).unwrap_or(());
                    outs.push(rx.unwrap_or_else(|_| {
                        s!(queue_errors);
                        HashMap::new()
                    }));
                }
            });

            // and start waiting for them
            let mut caches = join_all(outs).await;
            caches.retain(|m| !m.is_empty());

            // snapshots are relatively big and we don't want to copy them,
            // so we wrap them in Arc first

            let caches = caches.into_iter().map(Arc::new).collect::<Vec<_>>();
            // Send snapshots to aggregation if required
            let is_leader = IS_LEADER.load(Ordering::SeqCst);

            // there is special case used in agents: when we are not leader and there is
            // no consensus, that cannot make us leader, there is no point of aggregating
            // long cache at all because it will never be sent anywhere
            if !is_leader && config.consensus == ConsensusKind::None {
            } else  {
                tokio::task::block_in_place(||{
                    for cache in &caches {
                        slow_chan.send(SlowTask::Join(cache.clone())).unwrap_or_else(|_| {
                            s!(queue_errors);
                            info!(log, "task could not send snapshot, receiving thread may be dead");
                        });
                    }
                })
            }
            // after that clone snapshots to all nodes' queues
            for ch in &mut node_chans {
                // sender has sync send method which conflicts with one from Sink
                futures::SinkExt::send(ch, caches.clone())
                    .await
                    .map_err(|_| {
                        s!(queue_errors);
                        PeerError::SnapshotDropped
                    })
                .unwrap_or(());
            }
        }
    }
}

#[derive(Clone)]
pub struct SnapshotClientOptions {
    address: String,
    bind: Option<SocketAddr>,
    proto_version: ProtocolVersion,
}

#[derive(Clone)]
pub struct SnapshotSender {
    //metrics: Arc<Vec<Cache>>,
    options: SnapshotClientOptions,
    rx: RingReceiver<Vec<Arc<Cache>>>,
    log: Logger,
}

impl SnapshotSender {
    pub fn new(rx: RingReceiver<Vec<Arc<Cache>>>, options: SnapshotClientOptions, log: Logger) -> Self {
        let log = log.new(o!("remote"=>options.address.clone()));
        Self { rx, options, log }
    }

    async fn run(self) {
        let Self { mut rx, log, options } = self;

        let eaddr = options.address.clone();

        let elog = log.clone();

        while let Some(metrics) = rx.next().await {
            let mlen = metrics.iter().fold(0, |sum, next| sum + next.len());
            debug!(log, "popped new snapshot from queue"; "metrics"=>mlen);

            if mlen == 0 {
                debug!(log, "skipped empty snapshot");
                continue;
            }
            let snapshot_message = match options.proto_version {
                ProtocolVersion::V1 => {
                    let mut snapshot_message = Builder::new_default();
                    let builder = snapshot_message.init_root::<CBuilder>();
                    let mut multi_metric = builder.init_snapshot(mlen as u32);
                    metrics
                        .iter()
                        .flat_map(|hmap| hmap.iter())
                        .enumerate()
                        .map(|(idx, (name, metric))| {
                            let mut c_metric = multi_metric.reborrow().get(idx as u32);
                            // parsing stage has a guarantee that name is a valid unicode
                            // metrics that come over capnproto also has Text type in schema,
                            // so capnproto decoder will ensure unicode here
                            let name = unsafe { ::std::str::from_utf8_unchecked(name.name_with_tags()) };
                            c_metric.set_name(&name);
                            metric.fill_capnp_v1(&mut c_metric);
                        })
                    .last();

                    // this is just an approximate capacity to avoid first small allocations
                    let mut buf = Vec::with_capacity(mlen * 8);
                    if let Err(e) = capnp::serialize::write_message(&mut buf, &snapshot_message) {
                        error!(log, "encoding packed message"; "error" => format!("{}", e));
                        return;
                    };
                    buf
                }
                ProtocolVersion::V2 => {
                    let mut snapshot_message = Builder::new_default();
                    let builder = snapshot_message.init_root::<CBuilderV2>();
                    let mut multi_metric = builder.init_snapshot(mlen as u32);
                    metrics
                        .iter()
                        .flat_map(|hmap| hmap.iter())
                        .enumerate()
                        .map(|(idx, (name, metric))| {
                            let mut c_metric = multi_metric.reborrow().get(idx as u32);
                            // parsing stage has a guarantee that name is a valid unicode
                            // metrics that come over capnproto also has Text type in schema,
                            // so capnproto decoder will ensure unicode here
                            let name = unsafe { ::std::str::from_utf8_unchecked(name.name_with_tags()) };
                            c_metric.set_name(&name);
                            metric.fill_capnp(&mut c_metric);
                        })
                    .last();

                    // this is just an approximate capacity to avoid first small allocations
                    let mut buf = Vec::with_capacity(mlen * 8);
                    if let Err(e) = capnp::serialize::write_message(&mut buf, &snapshot_message) {
                        error!(log, "encoding packed message"; "error" => format!("{}", e));
                        return;
                    };
                    buf
                }
                // capnp::serialize::write_message_to_words(&snapshot_message)
            };

            let mut backoff = Backoff {
                delay: 500,
                delay_mul: 2f32,
                delay_max: 5000,
                retries: 5,
            };

            loop {
                let connect_and_send = async {
                    let addr = resolve_with_port(&options.address, 8136).await?;
                    let mut conn = match options.bind {
                        Some(bind_addr) => {
                            let std_stream = bound_stream(&bind_addr)?;
                            TcpStream::from_std(std_stream)?
                        }
                        None => TcpStream::connect(&addr).await?,
                    };

                    //let mut conn = conn.compat_write();

                    info!(log, "writing snapshot"; "bytes" => format!("{}", snapshot_message.len()), "metrics" => format!("{}", mlen));
                    //let write = write_message(&mut conn, &snapshot_message).map_err(|e| {
                    let write = conn.write_all(&snapshot_message).map_err(|e| {
                        warn!(log, "error encoding snapshot"; "error"=>e.to_string());
                        PeerError::Io(e)
                    });

                    tokio::time::timeout(std::time::Duration::from_millis(30000), write)
                        .await
                        .map_err(|_| PeerError::SnapshotWriteTimeout)??;
                    debug!(log, "flushing");
                    conn.flush().await?;
                    debug!(log, "done");
                    Ok::<(), PeerError>(())
                };

                match connect_and_send.await {
                    Err(e) => {
                        s!(peer_errors);
                        warn!(elog, "snapshot client error, retrying"; "next_pause"=>format!("{}", backoff.next_sleep()), "error"=>format!("{:?}", e), "remote"=>format!("{}", eaddr));
                        if backoff.sleep().await.is_err() {
                            error!(elog, "snapshot client removed after giving up trying"; "error"=>format!("{:?}", e), "remote"=>format!("{}", eaddr));
                            break;
                        }
                    }
                    Ok(()) => break,
                }
                }
            }
        }
    }

#[cfg(test)]
    mod test {

        use std::net::SocketAddr;
        use std::time::{SystemTime, UNIX_EPOCH};

        use capnp::message::Builder;
        use capnp_futures::serialize::write_message;
        use crossbeam_channel::Receiver;
        use slog::{debug, Logger};

        use tokio::runtime::{Builder as RBuilder, Runtime};
        use tokio::time::sleep;
        use tokio_util::compat::TokioAsyncWriteCompatExt;

        use bioyino_metric::name::{MetricName, TagFormat};
        use bioyino_metric::{Metric, MetricValue};

        use crate::config::System;
        use crate::slow_task::SlowTaskRunner;
        use crate::util::prepare_log;
        use crate::cache::SharedCache;

        use super::*;

        fn prepare_runtime_with_server(log: Logger) -> (Runtime, Receiver<SlowTask>, SocketAddr) {
            let (tx, rx) = crossbeam_channel::bounded(5);

            let address: ::std::net::SocketAddr = "127.0.0.1:8136".parse().unwrap();
            let runtime = RBuilder::new_current_thread()
                .thread_name("bio_peer_test")
                .enable_all()
                .build()
                .expect("creating runtime for test");

            let peer_listen = address.clone();
            let server_log = log.clone();
            let peer_server = NativeProtocolServer::new(server_log.clone(), peer_listen, tx.clone());
            let peer_server = peer_server.run().inspect_err(move |e| {
                debug!(server_log, "error running snapshot server"; "error"=>format!("{}", e));
                panic!("shot server");
            });
            runtime.spawn(peer_server);

            (runtime, rx, address)
        }

        #[test]
        fn test_peer_protocol_capnp() {
            let log = prepare_log("test_peer_capnp");

            let mut config = System::default();
            config.metrics.log_parse_errors = true;
            let cache = SharedCache::new();
            let mut runner = SlowTaskRunner::new(log.clone(), cache.clone());

            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let ts = ts.as_secs() as u64;

            let outmetric = Metric::new(MetricValue::Gauge(42.), ts.into(), 1.);

            let metric = outmetric.clone();
            let (runtime, rx, address) = prepare_runtime_with_server(log.clone());

            // emulate a worker thread receiveing snapshots
            let worker_thread = std::thread::spawn(move || {
                while let Ok(task) = rx.recv() {
                    runner.run(task)
                }

                let mut interm = Vec::with_capacity(128);
                interm.resize(128, 0u8);
                let m = TagFormat::Graphite;

                let single_name = MetricName::new("complex.test.bioyino_single".into(), m, &mut interm).unwrap();
                let multi_name = MetricName::new("complex.test.bioyino_multi".into(), m, &mut interm).unwrap();
                let shot_name = MetricName::new("complex.test.bioyino_snapshot".into(), m, &mut interm).unwrap();
                let tagged_name = MetricName::new("complex.test.bioyino_tagged;tag2=val2;tag1=value1".into(), m, &mut interm).unwrap();
                assert_eq!(cache.get(&single_name), Some(outmetric.clone()));
                assert_eq!(cache.get(&multi_name), Some(outmetric.clone()));
                assert_eq!(cache.get(&shot_name), Some(outmetric.clone()));
                assert_eq!(cache.get(&tagged_name), Some(outmetric.clone()));

            });

            let sender = async move {
                let conn = TcpStream::connect(&address).await.expect("connecting tcp client");

                // Version 1 messages
                let mut single_message = Builder::new_default();
                {
                    let builder = single_message.init_root::<CBuilder>();
                    let mut c_metric = builder.init_single();
                    c_metric.set_name("complex.test.bioyino_single");
                    metric.fill_capnp_v1(&mut c_metric);
                }

                let mut tagged_message = Builder::new_default();
                {
                    let builder = tagged_message.init_root::<CBuilder>();
                    let mut c_metric = builder.init_single();
                    c_metric.set_name("complex.test.bioyino_tagged;tag2=val2;tag1=value1");
                    metric.fill_capnp_v1(&mut c_metric);
                }

                let mut multi_message = Builder::new_default();
                {
                    let builder = multi_message.init_root::<CBuilder>();
                    let multi_metric = builder.init_multi(1);
                    let mut new_metric = multi_metric.get(0);
                    new_metric.set_name("complex.test.bioyino_multi");
                    metric.fill_capnp_v1(&mut new_metric);
                }


                let mut snapshot_message = Builder::new_default();
                {
                    let builder = snapshot_message.init_root::<CBuilder>();
                    let multi_metric = builder.init_snapshot(1);
                    let mut new_metric = multi_metric.get(0);
                    new_metric.set_name("complex.test.bioyino_snapshot");
                    metric.fill_capnp_v1(&mut new_metric);
                }

                // Version 2 should work along with version 1
                let  snapshot_message_v2 = Builder::new_default();
                {
                    let builder = snapshot_message.init_root::<CBuilderV2>();
                    let multi_metric = builder.init_snapshot(1);
                    let mut new_metric = multi_metric.get(0);
                    new_metric.set_name("complex.test.bioyino_snapshot");
                    metric.fill_capnp(&mut new_metric);
                }

                let mut conn = conn.compat_write();
                write_message(&mut conn, single_message).await.unwrap();
                write_message(&mut conn, tagged_message).await.unwrap();
                write_message(&mut conn, multi_message).await.unwrap();
                write_message(&mut conn, snapshot_message).await.unwrap();
                write_message(&mut conn, snapshot_message_v2).await.unwrap();
            };

            let delayed = async {
                sleep(Duration::from_secs(1)).await;
                sender.await
            };
            runtime.spawn(delayed);

            let test_delay = async { sleep(Duration::from_secs(2)).await };
            runtime.block_on(test_delay);
            worker_thread.join().unwrap();
        }
    }
