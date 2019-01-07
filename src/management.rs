use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;

use futures::future::{err, ok, Future, IntoFuture};
use futures::Stream;
use serde_json;
use serde;
use slog::{Logger, warn, o, info};

use hyper::service::Service;
use hyper::{self, Body, Method, Request, Response, StatusCode};

use failure_derive::Fail;
use serde_derive::{Serialize, Deserialize};

use failure::{Compat, Fail as FailTrait};
use crate::{ConsensusState, CONSENSUS_STATE, IS_LEADER};

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
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum MgmtCommand {
    // server will answer with ServerStatus message
    Status,
    // send a command to consensus module
    ConsensusCommand(ConsensusAction, LeaderAction),
}

// Turn consensus off for time(in milliseconds).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ConsensusAction {
    // enable consensus leadership
    Enable,
    // disable consensus leadership changes, consensus will be turned off
    Disable,
    // Pause consensus leadership.
    // The consensus module will still work and interact with others,
    // but any leadership changes will not be counted by backend as internal leader state
    Pause,
    // resume consensus from pause
    Resume,
}

impl FromStr for ConsensusAction {
    type Err = Compat<MgmtError>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "enable" | "enabled" => Ok(ConsensusAction::Enable),
            "disable" | "disabled" => Ok(ConsensusAction::Disable),
            "pause" | "paused" => Ok(ConsensusAction::Pause),
            "resume" | "resumed" | "unpause" | "unpaused" => Ok(ConsensusAction::Resume),
            _ => Err(MgmtError::BadCommand.compat()),
        }
    }
}

// Along with the consensus state change the internal leadership state can be changed
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum LeaderAction {
    Unchanged,
    Enable,
    Disable,
}

impl FromStr for LeaderAction {
    type Err = Compat<MgmtError>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchanged" | "unchange" => Ok(LeaderAction::Unchanged),
            "enable" | "enabled" => Ok(LeaderAction::Enable),
            "disable" | "disabled" => Ok(LeaderAction::Disable),
            _ => Err(MgmtError::BadCommand.compat()),
        }
    }
}
// this is what answered as server response
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct ServerStatus {
    leader_status: bool,
    consensus_status: ConsensusState,
}

impl ServerStatus {
    fn new() -> Self {
        let state = &*CONSENSUS_STATE.lock().unwrap();
        Self {
            leader_status: IS_LEADER.load(Ordering::SeqCst),
            consensus_status: state.clone(),
        }
    }
}

pub struct MgmtServer {
    log: Logger,
}

impl MgmtServer {
    pub fn new(log: Logger, address: &SocketAddr) -> Self {
        Self {
            log: log.new(o!("source"=>"management-server", "server"=>format!("{}", address))),
        }
    }
}

impl Service for MgmtServer {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;
    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let mut response = Response::new(Body::empty());

        let log = self.log.clone();
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => {
                *response.body_mut() = Body::from(
                    "Available endpoints:
    status - will show server status
    consensus - posting will change consensus state",
    );
                Box::new(ok(response))
            }
            (&Method::GET, "/status") => {
                let status = ServerStatus::new();
                let body = serde_json::to_vec_pretty(&status).unwrap(); // TODO unwrap
                *response.body_mut() = Body::from(body);
                Box::new(ok(response))
            }
            (&Method::GET, _) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
                Box::new(ok(response))
            }
            (&Method::POST, "/consensus") => {
                let fut = req.into_body().concat2().map(move |body| {
                    match serde_json::from_slice(&*body) {
                        Ok(MgmtCommand::ConsensusCommand(consensus_action, leader_action)) => {
                            {
                                // free a lock to avoid server status deadlocking
                                let mut constate = CONSENSUS_STATE.lock().unwrap();

                                *constate = match consensus_action {
                                    ConsensusAction::Enable | ConsensusAction::Resume => ConsensusState::Enabled,
                                    ConsensusAction::Disable => ConsensusState::Disabled,
                                    ConsensusAction::Pause => ConsensusState::Paused,
                                };
                            }

                            match leader_action {
                                LeaderAction::Enable => {
                                    IS_LEADER.store(true, Ordering::SeqCst);
                                }
                                LeaderAction::Disable => {
                                    IS_LEADER.store(false, Ordering::SeqCst);
                                }
                                _ => (),
                            };

                            *response.status_mut() = StatusCode::OK;

                            let status = ServerStatus::new();
                            let body = serde_json::to_vec_pretty(&status).unwrap(); // TODO unwrap
                            *response.body_mut() = Body::from(body);
                            info!(log, "state changed"; "consensus_state"=>format!("{:?}", status.consensus_status), "leader_state"=>status.leader_status);

                            response
                        }
                        Ok(command) => {
                            info!(log, "bad command received"; "command"=>format!("{:?}", command));
                            *response.status_mut() = StatusCode::BAD_REQUEST;

                            response
                        }
                        Err(e) => {
                            info!(log, "error parsing command"; "error"=>e.to_string());
                            *response.status_mut() = StatusCode::BAD_REQUEST;

                            response
                        }
                    }
                });

                Box::new(fut)
            }
            (&Method::POST, _) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
                Box::new(ok(response))
            }
            _ => {
                *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
                Box::new(ok(response))
            }
        }
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
            log: log
                .new(o!("source"=>"management-client", "server"=>format!("{}", address.clone()))),
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

        info!(log, "received command {:?}", command);
        match command {
            MgmtCommand::Status => {
                *req.method_mut() = Method::GET;
                *req.uri_mut() = format!("http://{}/status", address)
                    .parse()
                    .expect("creating url for management command ");

                let client = hyper::Client::new();
                let clog = log.clone();
                let future = client.request(req).then(move |res| match res {
                    Err(e) => Box::new(err(MgmtError::Http(e))),
                    Ok(resp) => {
                        if resp.status() == StatusCode::OK {
                            let body = resp
                                .into_body()
                                .concat2()
                                .map_err(|e| MgmtError::Http(e))
                                .map(move |body| {
                                    match serde_json::from_slice::<ServerStatus>(&*body) {
                                        Ok(status) => {
                                            println!("{:?}", status);
                                        }
                                        Err(e) => {
                                            println!(
                                                "Error parsing server response: {}",
                                                e.to_string()
                                            );
                                        }
                                    }
                                });
                            Box::new(body) as Box<Future<Item = (), Error = MgmtError>>
                        } else {
                            Box::new(ok(warn!(
                                        clog,
                                        "Bad status returned from server: {:?}", resp
                            )))
                        }
                    }
                });
                Box::new(future)
            }
            command @ MgmtCommand::ConsensusCommand(_, _) => {
                *req.method_mut() = Method::POST;
                *req.uri_mut() = format!("http://{}/consensus", address)
                    .parse()
                    .expect("creating url for management command");
                let body = serde_json::to_vec_pretty(&command).unwrap();
                *req.body_mut() = Body::from(body);

                let client = hyper::Client::new();
                let clog = log.clone();
                let future = client.request(req).then(move |res| match res {
                    Err(e) => Box::new(err(MgmtError::Http(e))),
                    Ok(resp) => {
                        if resp.status() == StatusCode::OK {
                            let body = resp
                                .into_body()
                                .concat2()
                                .map_err(|e| MgmtError::Http(e))
                                .map(move |body| {
                                    match serde_json::from_slice::<ServerStatus>(&*body) {
                                        Ok(status) => {
                                            println!("New server state: {:?}", status);
                                        }
                                        Err(e) => {
                                            println!(
                                                "Error parsing server response: {}",
                                                e.to_string()
                                            );
                                        }
                                    }
                                });
                            Box::new(body) as Box<Future<Item = (), Error = MgmtError>>
                        } else {
                            Box::new(ok(warn!(
                                        clog,
                                        "Bad status returned from server: {:?}", resp
                            )))
                        }
                    }
                });
                Box::new(future)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use {slog, slog_async, slog_term};

    use slog::Drain;
    use slog::Logger;
    use tokio::runtime::current_thread::Runtime;
    use tokio::timer::Delay;

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

    fn prepare_runtime_with_server() -> (Runtime, Logger, SocketAddr) {
        let rlog = prepare_log();

        let address: ::std::net::SocketAddr = "127.0.0.1:8137".parse().unwrap();
        let mut runtime = Runtime::new().expect("creating runtime for main thread");

        let c_serv_log = rlog.clone();
        let c_serv_err_log = rlog.clone();
        let s_addr = address.clone();
        let server = hyper::Server::bind(&address)
            .serve(move || ok::<_, hyper::Error>(MgmtServer::new(c_serv_log.clone(), &s_addr)))
            .map_err(move |e| {
                warn!(c_serv_err_log, "management server gone with error: {:?}", e);
            });

        runtime.spawn(server);

        (runtime, rlog, address)
    }

    #[test]
    fn management_command() {
        let test_timeout = Instant::now() + Duration::from_secs(3);
        let (mut runtime, log, address) = prepare_runtime_with_server();

        let command = MgmtCommand::Status;
        let client = MgmtClient::new(log.clone(), address, command);

        // let server some time to settle
        // then test the status command
        let d = Delay::new(Instant::now() + Duration::from_secs(1));
        let delayed = d
            .map_err(|_| ())
            .and_then(move |_| client.into_future().map_err(|e| panic!(e)));
        runtime.spawn(delayed);

        // then send a status change command
        let d = Delay::new(Instant::now() + Duration::from_secs(2));
        let delayed = d.map_err(|_| ()).and_then(move |_| {
            let command =
                MgmtCommand::ConsensusCommand(ConsensusAction::Enable, LeaderAction::Enable);
            let client = MgmtClient::new(log.clone(), address, command);

            client
                .into_future()
                .map_err(|e| panic!("{:?}", e))
                .map(move |_| {
                    // ensure state has changed
                    let state = ServerStatus::new();
                    assert_eq!(
                        state,
                        ServerStatus {
                            consensus_status: ConsensusState::Enabled,
                            leader_status: true,
                        }
                    )
                })
        });
        runtime.spawn(delayed);

        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
