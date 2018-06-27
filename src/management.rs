use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use capnp;
use capnp::message::{Builder, ReaderOptions};
use capnp_futures::ReadStream;
use futures::future::{err, join_all, ok, Future, IntoFuture};
use futures::sync::mpsc::Sender;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use serde_json;
use slog::Logger;
use tokio;
use tokio::executor::current_thread::spawn;
use tokio::net::{TcpListener, TcpStream};
use tokio::timer::Interval;
use tokio_io::codec::length_delimited;
use tokio_io::codec::length_delimited::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

use hyper::service::{NewService, Service};
use hyper::{self, Body, Method, Request, Response, StatusCode};

use metric::{Metric, MetricError};
use protocol_capnp::message as cmsg;

use failure::Compat;
use task::Task;
use {Cache, ConsensusState, Float, CONSENSUS_STATE, IS_LEADER, PEER_ERRORS};

#[derive(Fail, Debug)]
pub enum MgmtError {
    #[fail(display = "I/O error: {}", _0)]
    Io(#[cause] ::std::io::Error),

    #[fail(display = "Http error: {}", _0)]
    Http(#[cause] hyper::Error),

    #[fail(display = "JSON decoding error {}", _0)]
    Decode(#[cause] serde_json::error::Error),

    #[fail(display = "JSON encoding error: {}", _0)]
    Encode(#[cause] serde_json::error::Error),

    #[fail(display = "bad command")]
    BadCommand,

    #[fail(display = "response not sent")]
    Response,
}

// Top level list of available commands
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MgmtCommand {
    // server will answer with ServerStatus message
    Status,
    // send a command to consensus module
    ConsensusCommand(ConsensusAction, LeaderAction),
}

impl FromStr for MgmtCommand {
    type Err = Compat<MgmtError>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // FIXME:
        unimplemented!()
    }
}

// Turn consensus off for time(in milliseconds).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConsensusAction {
    // enable consensus leadership
    Enable,
    // disable consensus leadership changes consensus will be turned off
    Disable,
    // Pause consensus leadership.
    // The consensus module will still work and interact with others,
    // but any leadership changes will not be counted by backend as internal leader state
    Pause,
    // resume consensus from pause
    Resume,
}

// Along with the consensus state change the internal leadership state can be changed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaderAction {
    Unchanged,
    Enable,
    Disable,
}

// this is what answered as server response
#[derive(Debug, Serialize, Deserialize)]
struct ServerStatus {
    leader_status: bool,
    consensus_status: ConsensusState,
}

pub struct MgmtServer;
impl Service for MgmtServer {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = Compat<MgmtError>;
    type Future = Box<Future<Item = Response<Self::ResBody>, Error = Self::Error>>;
    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let mut response = Response::new(Body::empty());

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => {
                *response.body_mut() = Body::from(
                    "Available endpoints:
                status - will show server status 
                consensus - posting will change consensus state",
                );
            }
            (&Method::GET, "/status") => {
                *response.body_mut() = Body::from("HI");
            }
            (&Method::POST, "/consensus") => {
                // we'll be back
            }
            _ => {
                *response.status_mut() = StatusCode::NOT_FOUND;
            }
        }
        Box::new(ok(response))
    }
}

#[derive(Clone, Debug)]
pub struct MgmtClient {
    log: Logger,
    address: SocketAddr,
    command: MgmtCommand,
}

impl MgmtClient {
    pub fn new(log: Logger, address: SocketAddr, command: MgmtCommand) -> Self {
        Self {
            log: log.new(
                o!("source"=>"management-client", "server"=>format!("{}", address.clone())),
            ),
            address,
            command,
        }
    }
}

impl IntoFuture for MgmtClient {
    type Item = ();
    type Error = MgmtError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            address,
            command,
        } = self;
        let mut req = hyper::Request::default();
        match command {
            MgmtCommand::Status => {
                *req.method_mut() = Method::GET;
                *req.uri_mut() = format!("http://{}/status", address)
                    .parse()
                    .expect("creating url for management command ");

                let client = hyper::Client::new();
                let future = client.request(req).then(move |res| match res {
                    Err(e) => Box::new(err(MgmtError::Http(e))),
                    Ok(resp) => {
                        if resp.status() != StatusCode::OK {
                            let body = resp.into_body()
                                .concat2()
                                .map_err(|e| MgmtError::Http(e))
                                .map(move |body| {
                                    // FIXME unwrap
                                    let status: ServerStatus =
                                        serde_json::from_slice(&*body).unwrap();
                                    println!("{:?}", status);
                                });
                            Box::new(body) as Box<Future<Item = (), Error = MgmtError>>
                        } else {
                            Box::new(ok(println!("Bad status returned from server")))
                        }
                    }
                });
                Box::new(future)
            }
            MgmtCommand::ConsensusCommand(consensus_action, leader_action) => {
                let mut constate = CONSENSUS_STATE.lock().unwrap();

                match consensus_action {
                    ConsensusAction::Enable | ConsensusAction::Resume => {
                        *constate = ConsensusState::Enabled;
                    }
                    ConsensusAction::Disable => {
                        *constate = ConsensusState::Disabled;
                    }
                    ConsensusAction::Pause => {
                        *constate = ConsensusState::Paused;
                    }
                    ConsensusAction::Resume => {
                        *constate = ConsensusState::Enabled;
                    }
                }

                match leader_action {
                    LeaderAction::Enable => {
                        IS_LEADER.store(true, Ordering::SeqCst);
                    }
                    LeaderAction::Disable => {
                        IS_LEADER.store(false, Ordering::SeqCst);
                    }
                    _ => (),
                }

                Box::new(ok(println!("State is set")))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::SocketAddr;
    use std::thread;
    use {slog, slog_async, slog_term};

    use bytes::Bytes;
    use capnp::message::Builder;
    use futures::sync::mpsc::{self, Receiver};
    use metric::{Metric, MetricType};
    use slog::Drain;
    use slog::Logger;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::runtime::current_thread::Runtime;
    use tokio::timer::Delay;

    use {LONG_CACHE, SHORT_CACHE};

    use super::*;
    fn prepare_log() -> Logger {
        // Set logging
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
        let drain = slog_async::Async::new(filter).build().fuse();
        let rlog = slog::Logger::root(drain, o!("program"=>"test"));
        return rlog;
    }

    fn prepare_runtime_with_server(
        test_timeout: Instant,
    ) -> (
        Runtime,
        Vec<Sender<Task>>,
        Receiver<Task>,
        Logger,
        SocketAddr,
    ) {
        let rlog = prepare_log();
        let mut chans = Vec::new();
        let (tx, rx) = mpsc::channel(5);
        chans.push(tx);

        let address: ::std::net::SocketAddr = "127.0.0.1:8136".parse().unwrap();
        let mut runtime = Runtime::new().expect("creating runtime for main thread");

        let c_peer_listen = address.clone();
        let c_serv_log = rlog.clone();
        let peer_server = NativeProtocolServer::new(rlog.clone(), c_peer_listen, chans.clone())
            .into_future()
            .map_err(move |e| {
                warn!(c_serv_log, "shot server gone with error: {:?}", e);
            });
        runtime.spawn(peer_server);

        (runtime, chans, rx, rlog, address)
    }

}
