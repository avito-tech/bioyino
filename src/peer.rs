use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bincode;
use futures::future::{join_all, ok, Future, IntoFuture};
use futures::sync::mpsc::Sender;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use slog::Logger;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Handle, Interval};
use tokio_io::codec::length_delimited;
use tokio_io::codec::length_delimited::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

use task::Task;
use {Cache, CAN_LEADER, FORCE_LEADER, IS_LEADER, PEER_ERRORS};

#[derive(Fail, Debug)]
pub enum PeerError {
    #[fail(display = "I/O error: {}", _0)]
    Io(#[cause] ::std::io::Error),

    #[fail(display = "bincode decoding error {}", _0)]
    Decode(#[cause] Box<bincode::ErrorKind>),

    #[fail(display = "bincode encoding error: {}", _0)]
    Encode(#[cause] Box<bincode::ErrorKind>),

    #[fail(display = "error sending task to worker thread")]
    TaskSend,

    #[fail(display = "server received incorrect message")]
    BadMessage,

    #[fail(display = "bad command")]
    BadCommand,

    #[fail(display = "response not sent")]
    Response,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerCommand {
    LeaderDisable,
    LeaderEnable,
    ForceLeader,
    Status,
}

impl FromStr for PeerCommand {
    type Err = PeerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "leader_enable" => Ok(PeerCommand::LeaderEnable),
            "leader_disable" => Ok(PeerCommand::LeaderDisable),
            "force_leader" => Ok(PeerCommand::ForceLeader),
            "status" => Ok(PeerCommand::Status),
            _ => Err(PeerError::BadCommand),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    is_leader: bool,
    can_leader: bool,
    force_leader: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Snapshot(Vec<Cache>),
    Command(PeerCommand),
    Status(PeerStatus),
}

pub struct PeerCodec<T> {
    inner: Framed<T, Vec<u8>>,
}

impl<T> PeerCodec<T>
where
    T: AsyncRead + AsyncWrite,
{
    pub fn new(conn: T) -> Self {
        Self {
            inner: length_delimited::Builder::new()
                .length_field_length(8)
                .max_frame_length(::std::u64::MAX as usize)
                .new_framed(conn),
        }
    }
}

// Wrapper to decode  message from length-encoded frame
impl<T> Stream for PeerCodec<T>
where
    T: AsyncRead + AsyncWrite,
{
    type Item = PeerMessage;
    type Error = PeerError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let message = match try_ready!(self.inner.poll().map_err(|e| PeerError::Io(e))) {
            Some(buf) => Some(bincode::deserialize(&buf).map_err(|e| PeerError::Decode(e))?),
            None => None,
        };
        Ok(Async::Ready(message))
    }
}

// Wrapper to encode message to length-encoded frame
impl<T> Sink for PeerCodec<T>
where
    T: AsyncRead + AsyncWrite,
{
    type SinkItem = Option<PeerMessage>;
    type SinkError = PeerError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match item {
            Some(item) => {
                let message = bincode::serialize(&item).map_err(|e| PeerError::Decode(e))?;
                match self.inner.start_send(message) {
                    Ok(AsyncSink::NotReady(_)) => Ok(AsyncSink::NotReady(Some(item))),
                    Ok(AsyncSink::Ready) => Ok(AsyncSink::Ready),
                    Err(e) => Err(PeerError::Io(e)),
                }
            }
            None => Ok(AsyncSink::Ready),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete().map_err(|e| PeerError::Io(e))
    }
}

pub struct PeerServer {
    log: Logger,
    listen: SocketAddr,
    nodes: Vec<SocketAddr>,
    handle: Handle,
    chans: Vec<Sender<Task>>,
}

impl PeerServer {
    pub fn new(
        log: &Logger,
        listen: SocketAddr,
        handle: &Handle,
        chans: &Vec<Sender<Task>>,
        nodes: &Vec<SocketAddr>,
    ) -> Self {
        Self {
            log: log.new(o!("source"=>"peer-server", "ip"=>format!("{}", listen.clone()))),
            listen,
            nodes: nodes.clone(),
            handle: handle.clone(),
            chans: chans.clone(),
        }
    }
}

impl IntoFuture for PeerServer {
    type Item = ();
    type Error = PeerError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            listen,
            nodes,
            handle,
            chans,
        } = self;
        let future = ::tokio_core::net::TcpListener::bind(&listen, &handle)
            .expect("listening peer port")
            .incoming()
            .map_err(|e| PeerError::Io(e))
            .for_each(move |(conn, _inaddr)| {
                let handle = handle.clone();
                let transport = PeerCodec::new(conn);

                let log = log.clone();
                let nodes = nodes.clone();

                let chans = chans.clone();
                let mut chans = chans.into_iter().cycle();

                let (writer, reader) = transport.split();

                reader
                    .map(move |m| match m {
                        PeerMessage::Snapshot(shot) => {
                            let next_chan = chans.next().unwrap();
                            let future = next_chan
                                .send(Task::JoinSnapshot(shot))
                                .map(|_| ()) // drop next sender
                                .map_err(|_| PeerError::TaskSend);
                            let elog = log.clone();
                            handle.spawn(future.map_err(move |e| {
                                warn!(elog, "error joining snapshot: {:?}", e);
                            }));
                            None
                        }
                        PeerMessage::Command(PeerCommand::LeaderEnable) => {
                            CAN_LEADER.store(true, Ordering::SeqCst);
                            FORCE_LEADER.store(false, Ordering::SeqCst);
                            None
                        }
                        PeerMessage::Command(PeerCommand::LeaderDisable) => {
                            CAN_LEADER.store(false, Ordering::SeqCst);
                            //IS_LEADER.store(false, Ordering::SeqCst);
                            None
                        }
                        PeerMessage::Command(PeerCommand::ForceLeader) => {
                            CAN_LEADER.store(false, Ordering::SeqCst);
                            IS_LEADER.store(true, Ordering::SeqCst);
                            FORCE_LEADER.store(true, Ordering::SeqCst);
                            nodes
                                .clone()
                                .into_iter()
                                .map(|node| {
                                    let elog = log.clone();
                                    let command = PeerCommandClient::new(
                                        &log,
                                        node.clone(),
                                        &handle,
                                        PeerCommand::LeaderDisable,
                                    ).into_future()
                                        .map_err(move |e| {
                                            warn!(
                                                elog,
                                                "could not send command to {:?}: {:?}", node, e
                                            );
                                        })
                                        .then(|_| Ok(()));
                                    handle.spawn(command)
                                })
                                .last();
                            None
                        }
                        PeerMessage::Command(PeerCommand::Status) => {
                            let is_leader = IS_LEADER.load(Ordering::Relaxed);
                            let can_leader = CAN_LEADER.load(Ordering::Relaxed);
                            let force_leader = FORCE_LEADER.load(Ordering::Relaxed);
                            let status = PeerStatus {
                                is_leader,
                                can_leader,
                                force_leader,
                            };
                            Some(PeerMessage::Status(status))
                        }
                        PeerMessage::Status(_) => {
                            // TODO: log bad error or response with BadMessage to client
                            None
                        }
                    })
                    .forward(writer)
                    .then(|_| Ok(())) // don't let send errors fail the server
            });
        Box::new(future)
    }
}

pub struct PeerSnapshotClient {
    nodes: Vec<SocketAddr>,
    interval: Duration,
    handle: Handle,
    chans: Vec<Sender<Task>>,
    log: Logger,
}

impl PeerSnapshotClient {
    pub fn new(
        log: &Logger,
        nodes: Vec<SocketAddr>,
        interval: Duration,
        handle: &Handle,
        chans: &Vec<Sender<Task>>,
    ) -> Self {
        Self {
            log: log.new(o!("source"=>"peer-client")),
            nodes,
            interval,
            handle: handle.clone(),
            chans: chans.clone(),
        }
    }
}

impl IntoFuture for PeerSnapshotClient {
    type Item = ();
    type Error = PeerError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            nodes,
            interval,
            handle,
            chans,
        } = self;

        let timer = Interval::new(interval, &handle).unwrap();
        let future = timer.map_err(|e| PeerError::Io(e)).for_each(move |_| {
            let chans = chans.clone();
            let nodes = nodes.clone();

            let handle = handle.clone();
            let metrics = chans
                .into_iter()
                .map(|chan| {
                    let (tx, rx) = oneshot::channel();
                    handle.spawn(chan.send(Task::TakeSnapshot(tx)).then(|_| Ok(())));
                    rx
                })
                .collect::<Vec<_>>();

            let get_metrics = join_all(metrics).map_err(|_| PeerError::TaskSend).and_then(
                move |mut metrics| {
                    metrics.retain(|m| m.len() > 0);
                    Ok(metrics)
                },
            );

            // All nodes have to receive the same metrics
            // so we don't parallel connections and metrics fetching
            // TODO: we probably clne a lots of bytes here,
            // could've changed them to Arc
            let log = log.clone();
            get_metrics.and_then(move |metrics| {
                let clients = nodes
                    .into_iter()
                    .map(|address| {
                        let metrics = metrics.clone();
                        let log = log.clone();
                        TcpStream::connect(&address, &handle)
                            .map_err(|e| PeerError::Io(e))
                            .and_then(move |conn| {
                                let codec = PeerCodec::new(conn);
                                codec.send(Some(PeerMessage::Snapshot(metrics))).map(|_| ())
                            })
                            .map_err(move |e| {
                                PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
                                debug!(log, "error sending snapshot: {}", e)
                            })
                            .then(|_| Ok(())) // we don't want to faill the whole timer cycle because of one send error
                    })
                    .collect::<Vec<_>>();
                join_all(clients).map(|_| ())
            })
        });
        Box::new(future)
    }
}

pub struct PeerCommandClient {
    log: Logger,
    address: SocketAddr,
    handle: Handle,
    command: PeerCommand,
}

impl PeerCommandClient {
    pub fn new(log: &Logger, address: SocketAddr, handle: &Handle, command: PeerCommand) -> Self {
        Self {
            log: log.new(
                o!("source"=>"peer-command-client", "server"=>format!("{}", address.clone())),
            ),
            address,
            handle: handle.clone(),
            command,
        }
    }
}

impl IntoFuture for PeerCommandClient {
    type Item = ();
    type Error = PeerError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            address,
            handle,
            command,
        } = self;

        let resp_required = if let PeerCommand::Status = command {
            true
        } else {
            false
        };
        let future = TcpStream::connect(&address, &handle)
            .map_err(|e| PeerError::Io(e))
            .and_then(move |conn| {
                let codec = PeerCodec::new(conn);
                codec.send(Some(PeerMessage::Command(command)))
            })
            .and_then(move |codec| {
                if resp_required {
                    let resp = codec
                        .into_future()
                        .and_then(move |(status, _)| {
                            if let Some(PeerMessage::Status(status)) = status {
                                println!("status of {:?}: {:?}", address, status,);
                            } else {
                                warn!(log, "Unknown response from server: {:?}", status);
                            }
                            Ok(())
                        })
                        .then(|_| Ok(()));
                    Box::new(resp) as Box<Future<Item = Self::Item, Error = Self::Error>>
                } else {
                    Box::new(ok::<(), PeerError>(()))
                }
            });

        Box::new(future)
    }
}
