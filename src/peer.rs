use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration};
use std::num::NonZeroUsize;
use std::collections::HashMap;

use capnp::message::{Builder, ReaderOptions};
use capnp_futures::{serialize::write_message};
use slog::{o, warn, Logger};
use thiserror::Error;

use futures3::channel::mpsc::Sender;
use futures3::channel::oneshot;
use futures3::{SinkExt};

use ring_channel::{ring_channel, RingSender, RingReceiver};
use tokio2::net::{TcpStream, TcpListener};
use tokio2::time::{interval_at, Instant};
use tokio2::spawn;
use tokio2::stream::StreamExt;
use tokio_util::compat::{Tokio02AsyncWriteCompatExt, Tokio02AsyncReadCompatExt};
use futures3::future::{TryFutureExt};
use futures3::future::join_all;

use bioyino_metric::protocol_capnp::{message as cmsg, message::Builder as CBuilder};
use bioyino_metric::{Metric};

use crate::task::Task;
use crate::util::{bound_stream, resolve_with_port, Backoff, retry_with_backoff};
use crate::{s, Cache, Float};

const CAPNP_READER_OPTIONS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: 8 * 1024 * 1024 * 1024,
    nesting_limit: 16,
};

#[derive(Error, Debug)]
pub enum PeerError {
    #[error("I/O error: {}", _0)]
    Io(#[from] ::std::io::Error),

    #[error("Error when creating timer: {}", _0)]
    Timer(#[from] ::tokio::timer::Error),

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
    chans: Vec<Sender<Task>>,
}

impl NativeProtocolServer {
    pub fn new(log: Logger, listen: SocketAddr, chans: Vec<Sender<Task>>) -> Self {
        Self {
            log: log.new(o!("source"=>"canproto-peer-server", "ip"=>format!("{}", listen.clone()))),
            listen,
            chans,
        }
    }

    pub(crate) async fn run(self) -> Result<(), PeerError> {
        let Self { log, listen, chans } = self;
        let mut listener = TcpListener::bind(&listen).await?;
        let mut incoming = listener.incoming();

        while let Some(conn) = incoming.next().await {
            let conn = conn?;
            let peer_addr = conn.peer_addr().map(|addr| addr.to_string()).unwrap_or_else(|_| "[UNCONNECTED]".into());
            let mut conn = conn.compat();

            let log = log.new(o!("remote"=>peer_addr));

            let elog = log.clone();
            let chans = chans.clone();
            let mut chans = chans.into_iter().cycle();

            let receiver = async move {
                let elog = log.clone();
                //let transport = capnp_futures::ReadStream::new(&mut conn, CAPNP_READER_OPTIONS);//.map_err(PeerError::Capnp);
                while let Ok(Some(reader)) = capnp_futures::serialize::read_message(&mut conn, CAPNP_READER_OPTIONS).await {
                    //while let Some(reader) = futures3::stream::TryStreamExt::try_next(&mut transport).await?
                    let task = {
                        let reader = reader.get_root::<cmsg::Reader>()?;

                        let elog = elog.clone();
                        parse_and_send(reader).map_err(move |e| {
                            warn!(elog, "bad incoming message"; "error" => e.to_string());
                            e
                        })?
                    };

                    let mut next_chan = chans.next().unwrap().clone();
                    next_chan.send(task).map_err(|_|{ s!(queue_errors); PeerError::TaskSend }).await?;
                }
                Ok::<(), PeerError>(())
            }.map_err(move |e| {

                s!(peer_errors);
                warn!(elog, "snapshot server reading error"; "error"=>format!("{:?}", e));
            });

            spawn(receiver);
        }
        Ok(())
    }
}

fn parse_and_send(reader: cmsg::Reader<'_>) -> Result<Task, PeerError> {
    match reader.which()? {
        cmsg::Single(reader) => {
            let reader = reader?;
            let (name, metric) = Metric::<Float>::from_capnp(reader)?;
            Ok(Task::AddMetric(name, metric))
        }
        cmsg::Multi(reader) => {
            let reader = reader?;
            let mut metrics = Vec::new();
            reader
                .iter()
                .map(|reader| Metric::<Float>::from_capnp(reader).map(|(name, metric)| metrics.push((name, metric))))
                .last();
            Ok(Task::AddMetrics(metrics))
        }
        cmsg::Snapshot(reader) => {
            let reader = reader?;
            let mut metrics = Vec::new();
            reader
                .iter()
                .map(|reader| Metric::<Float>::from_capnp(reader).map(|(name, metric)| metrics.push((name, metric))))
                .last();
            Ok(Task::AddSnapshot(metrics))
        }
    }
}

pub struct NativeProtocolSnapshot {
    log: Logger,
    nodes: Vec<String>,
    client_bind: Option<SocketAddr>,
    interval: Duration,
    chans: Vec<Sender<Task>>,
    snapshots: NonZeroUsize,
}

impl NativeProtocolSnapshot {
    pub fn new(log: &Logger, nodes: Vec<String>, client_bind: Option<SocketAddr>, interval: Duration, chans: &[Sender<Task>], mut snapshots: usize) -> Self {
        if snapshots == 0 {
            warn!(log, "snapshots cannot be 0, value is set to 1");
            snapshots = 1
        }
        Self {
            log: log.new(o!("source"=>"peer-client")),
            nodes,
            client_bind,
            interval,
            chans: chans.to_owned(),
            snapshots: NonZeroUsize::new(snapshots).unwrap(),
        }
    }

    pub(crate) async fn run(self) -> Result<(), PeerError> {
        let Self {
            log,
            nodes,
            client_bind,
            interval,
            chans,
            snapshots,
        } = self;
        // Snapshots come every `interval`. When one of the nodes goes down it is possible to leak
        // all the memory if interval is less than backoff period because in this case snapshots will
        // come faster than sender can handle them. Though backoff period of the length of one
        // snapshot is too short and also unacceptable.
        // So to avoid infinite memory leaking we break the procedures of taking
        // a snapshot and sending it to nodes with ring buffer, storing only latest N snapshots.
        // Since all metrics is lost after some time on remote node, it is only reasonable to store
        // few latest intervals. This is the reason of using a ring buffer instead of a channel.

        let mut node_chans = nodes.iter().map(|address| {
            let (tx, mut rx): (RingSender<Arc<Vec<Cache>>>, RingReceiver<Arc<Vec<Cache>>>)  = ring_channel(snapshots);
            let log = log.clone();
            let options = SnapshotClientOptions { address: address.clone(), bind: client_bind };

            let snapshot_sender = async move {
                while let Some(metrics) = rx.next().await {
                    let backoff = Backoff {
                        delay: 500,
                        delay_mul: 2f32,
                        delay_max: 5000,
                        retries: 3,
                    };
                    let elog = log.clone();
                    let log = log.clone();
                    let options = options.clone();
                    let eaddr = options.address.clone();
                    retry_with_backoff(backoff, move || {
                        let metrics =  metrics.clone();
                        let client = SnapshotSender::new(metrics, options.clone(), log.clone());
                        client.run()
                    }).await.map_err(move |e| {
                        s!(peer_errors);
                        warn!(elog, "snapshot client removed after giving up trying"; "error"=>format!("{:?}", e), "remote"=>format!("{}", eaddr));
                        e
                    }).unwrap_or(()); // don't exit on failure
                }

                Ok::<(), PeerError>(())
            };
            spawn(snapshot_sender);
            tx
        }).collect::<Vec<_>>();

        let mut timer = interval_at(Instant::now() + interval, interval);
        loop {
            timer.tick().await;
            // send snapshot requests to workers
            let mut outs = Vec::new();
            for mut chan in chans.iter().cloned() {
                let (tx, rx) = oneshot::channel();
                chan.send(Task::TakeSnapshot(tx)).await.unwrap_or(());
                outs.push(rx.unwrap_or_else(|_| {
                    s!(queue_errors);
                    HashMap::new()
                }));
            }

            // and start waiting for them
            let mut all_metrics = join_all(outs).await;
            all_metrics.retain(|m|
                !m.is_empty()
            );
            let all_metrics = Arc::new(all_metrics);
            // after that clone snapshots to all nodes' queues
            for ch in &mut node_chans {
                // sender has sync send method which conflicts with one from Sink
                futures3::SinkExt::send(ch, all_metrics.clone()).await.map_err(|_| {
                    s!(queue_errors);
                    PeerError::SnapshotDropped}).unwrap_or(());
            }
        };
    }
}

#[derive(Clone)]
pub struct SnapshotClientOptions {
    address: String,
    bind: Option<SocketAddr>,
}

#[derive(Clone)]
pub struct SnapshotSender {
    metrics: Arc<Vec<Cache>>,
    options: SnapshotClientOptions,
    log: Logger,
}

impl SnapshotSender {
    pub fn new(metrics: Arc<Vec<Cache>>, options: SnapshotClientOptions, log: Logger) -> Self {
        let log = log.new(o!("remote"=>options.address.clone()));
        Self {
            metrics, options, log
        }
    }

    async fn run(self) -> Result<(), PeerError> {
        let Self { metrics, log, options } = self;

        // error stats are counted when running this future
        let addr = resolve_with_port(&options.address, 8136).await?;
        let conn = match options.bind {
            Some(bind_addr) => {
                let std_stream = bound_stream(&bind_addr)?;
                TcpStream::connect_std(std_stream, &addr).await?
            },
            None => TcpStream::connect(&addr).await?,
        };

        let mut snapshot_message = Builder::new_default();
        {
            let builder = snapshot_message.init_root::<CBuilder>();
            let flat_len = metrics.iter().flat_map(|hmap| hmap.iter()).count();
            let mut multi_metric = builder.init_snapshot(flat_len as u32);
            metrics
                .iter()
                .flat_map(|hmap| hmap.iter())
                .enumerate()
                .map(|(idx, (name, metric))| {
                    let mut c_metric = multi_metric.reborrow().get(idx as u32);
                    let name = unsafe { ::std::str::from_utf8_unchecked(name.name_with_tags()) };
                    c_metric.set_name(name);
                    metric.fill_capnp(&mut c_metric);
                })
            .last();
            }
        write_message(conn.compat_write(), snapshot_message).map_err(move |e| {
            warn!(log, "error encoding snapshot"; "error"=>e.to_string());
            PeerError::Capnp(e)
        }).await
    }
}

#[cfg(test)]
mod test {

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    //use bytes::BytesMut;
    use capnp::message::Builder;
    use futures3::channel::mpsc::{self, Receiver};
    use slog::{Logger, debug};

    use tokio2::runtime::{Builder as RBuilder, Runtime};
    use tokio2::time::{delay_for};

    use bioyino_metric::name::{MetricName, TagFormat};
    use bioyino_metric::{Metric, MetricType};

    use crate::config::System;
    use crate::task::TaskRunner;
    use crate::util::prepare_log;

    use super::*;

    fn prepare_runtime_with_server(log: Logger) -> (Runtime, Receiver<Task>, SocketAddr) {
        let mut chans = Vec::new();
        let (tx, rx) = mpsc::channel(5);
        chans.push(tx);

        let address: ::std::net::SocketAddr = "127.0.0.1:8136".parse().unwrap();
        let runtime = RBuilder::new()
            .thread_name("bio_peer_test")
            .basic_scheduler()
            .enable_all()
            .build()
            .expect("creating runtime for test");


        let peer_listen = address.clone();
        let server_log = log.clone();
        let peer_server = NativeProtocolServer::new(server_log.clone(), peer_listen, chans.clone());
        let peer_server = peer_server
            .run()
            .inspect_err(move |e|{
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
        let mut runner = TaskRunner::new(log.clone(), Arc::new(config), 16);

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ts = ts.as_secs() as u64;

        let outmetric = Metric::new(42f64, MetricType::Gauge(None), ts.into(), None).unwrap();

        let metric = outmetric.clone();
        let (mut runtime, mut rx, address) = prepare_runtime_with_server(log.clone());

        let receiver = async move {
            while let Some(task) = rx.next().await {
                runner.run(task)
            }

            let mut interm = Vec::with_capacity(128);
            interm.resize(128, 0u8);
            let m = TagFormat::Graphite;

            let single_name = MetricName::new("complex.test.bioyino_single".into(), m, &mut interm).unwrap();
            let multi_name = MetricName::new("complex.test.bioyino_multi".into(), m, &mut interm).unwrap();
            let shot_name = MetricName::new("complex.test.bioyino_snapshot".into(), m, &mut interm).unwrap();
            let tagged_name = MetricName::new("complex.test.bioyino_tagged;tag2=val2;tag1=value1".into(), m, &mut interm).unwrap();
            assert_eq!(runner.get_long_entry(&shot_name), Some(&outmetric));
            assert_eq!(runner.get_short_entry(&single_name), Some(&outmetric));
            assert_eq!(runner.get_short_entry(&multi_name), Some(&outmetric));
            assert_eq!(runner.get_short_entry(&tagged_name), Some(&outmetric));
        };
        runtime.spawn(receiver);

        let sender  = async move {
            let conn = TcpStream::connect(&address).await.expect("connecting tcp client");

            let mut single_message = Builder::new_default();
            {
                let builder = single_message.init_root::<CBuilder>();
                let mut c_metric = builder.init_single();
                c_metric.set_name("complex.test.bioyino_single");
                metric.fill_capnp(&mut c_metric);
            }

            let mut tagged_message = Builder::new_default();
            {
                let builder = tagged_message.init_root::<CBuilder>();
                let mut c_metric = builder.init_single();
                c_metric.set_name("complex.test.bioyino_tagged;tag2=val2;tag1=value1");
                metric.fill_capnp(&mut c_metric);
            }

            let mut multi_message = Builder::new_default();
            {
                let builder = multi_message.init_root::<CBuilder>();
                let multi_metric = builder.init_multi(1);
                let mut new_metric = multi_metric.get(0);
                new_metric.set_name("complex.test.bioyino_multi");
                metric.fill_capnp(&mut new_metric);
            }

            let mut snapshot_message = Builder::new_default();
            {
                let builder = snapshot_message.init_root::<CBuilder>();
                let multi_metric = builder.init_snapshot(1);
                let mut new_metric = multi_metric.get(0);
                new_metric.set_name("complex.test.bioyino_snapshot");
                metric.fill_capnp(&mut new_metric);
            }

            let mut conn = conn.compat_write();
            write_message(&mut conn, single_message).await.unwrap();
            write_message(&mut conn, multi_message).await.unwrap();
            write_message(&mut conn, snapshot_message).await.unwrap();
            //codec
            //.send(single_message)
            //.and_then(|codec| codec.send(multi_message).and_then(|codec| codec.send(snapshot_message)))
            //.map(|_| ())
            //    .map_err(|e| println!("codec error: {:?}", e))
        };

        let delayed = async {
            delay_for(Duration::from_secs(1)).await;
            sender.await
        };
        runtime.spawn(delayed);

        let test_delay = async { delay_for(Duration::from_secs(2)).await };
        runtime.block_on(test_delay);

    }
}
