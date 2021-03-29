use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use bytes::{BytesMut, BufMut};
use futures::future::{ok, Future};
use slog::{info, o, warn, Logger};

use hyper::service::Service;
use hyper::{self, body::to_bytes, http, Body, client::Client, Method, Request, Response, StatusCode};

use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::stats::STATS_SNAP;
use crate::GeneralError;
use crate::{Float, CONSENSUS_STATE, IS_LEADER};

#[derive(Error, Debug)]
pub enum MgmtError {
    #[error("I/O error: {}", _0)]
    Io(#[from] ::std::io::Error),

    #[error("Http error: {}", _0)]
    Http(#[from] hyper::Error),

    #[error("Http error: {}", _0)]
    HttpProto(#[from] http::Error),

    #[error("JSON encode/decode error {}", _0)]
    Json(#[from] serde_json::error::Error),

    #[error("bad command")]
    BadCommand,

    #[error("bad response from management server")]
    BadResponse,

    #[error("response not sent")]
    Response,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ConsensusState {
    Enabled,
    Paused,
    Disabled,
}

impl FromStr for ConsensusState {
    type Err = GeneralError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "enabled" | "enable" => Ok(ConsensusState::Enabled),
            "disabled" | "disable" => Ok(ConsensusState::Disabled),
            "pause" | "paused" => Ok(ConsensusState::Paused),
            _ => Err(GeneralError::UnknownState),
        }
    }
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
    type Err = MgmtError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "enable" | "enabled" => Ok(ConsensusAction::Enable),
            "disable" | "disabled" => Ok(ConsensusAction::Disable),
            "pause" | "paused" => Ok(ConsensusAction::Pause),
            "resume" | "resumed" | "unpause" | "unpaused" => Ok(ConsensusAction::Resume),
            _ => Err(MgmtError::BadCommand),
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
    type Err = MgmtError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchanged" | "unchange" => Ok(LeaderAction::Unchanged),
            "enable" | "enabled" => Ok(LeaderAction::Enable),
            "disable" | "disabled" => Ok(LeaderAction::Disable),
            _ => Err(MgmtError::BadCommand),
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

pub struct MgmtService {
    log: Logger,
}

impl MgmtService {
    pub fn new(log: Logger) -> Self {
        Self {
            //    log: log.new(o!("source"=>"management-server", "server"=>format!("{}", address))),
            log,
        }
    }
}

impl Service<Request<Body>> for MgmtService {
    type Response = Response<Body>;
    type Error = MgmtError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let mut response = Response::new(Body::empty());

        let log = self.log.clone();
        let (path, query) = match req.uri().path_and_query() {
            Some(pq) => {
                let query = pq.query().map(|q| url::form_urlencoded::parse(q.as_bytes()));
                (pq.path(), query)
            }
            None => {
                *response.status_mut() = StatusCode::BAD_REQUEST;
                return Box::pin(ok(response));
            }
        };
        match (req.method(), path, query) {
            (&Method::GET, "/", _) => {
                *response.body_mut() = Body::from(
                    "Available endpoints:
    /status - will show server status
    /stats - will show server exported metrics
    /consensus - posting will change consensus state",
                );
                Box::pin(ok(response))
            }
            (&Method::GET, "/status", _) => {
                let status = ServerStatus::new();
                let body = serde_json::to_vec_pretty(&status).unwrap_or(b"internal error".to_vec());
                *response.body_mut() = Body::from(body);
                Box::pin(ok(response))
            }
            (&Method::GET, "/stats", qry) => {
                let mut buf = BytesMut::new();
                let mut json = false;
                if let Some(qry) = qry {
                    for (k, v) in qry {
                        if k == "format" && v == "json" {
                            json = true;
                            break;
                        }
                    }
                }
                let snapshot = {
                    let snapshot = STATS_SNAP.lock().unwrap();
                    snapshot.clone()
                };
                if !json {
                    let ts = snapshot.ts.to_string();
                    for (name, value) in &snapshot.data {
                        buf.extend_from_slice(&name[..]);
                        buf.extend_from_slice(&b" "[..]);
                        // write somehow doesn't extend buffer size giving "cannot fill buffer" error
                        buf.reserve(64);
                        let mut writer = buf.writer();
                        ftoa::write(&mut writer, *value).unwrap_or(()); // TODO: think if we should not ignore float error
                        buf = writer.into_inner();
                        buf.extend_from_slice(&b" "[..]);
                        buf.extend_from_slice(ts.as_bytes());
                        buf.extend_from_slice(&b"\n"[..]);
                    }

                } else {

                    #[derive(Serialize)]
                    struct JsonSnap {
                        ts: u128,
                        metrics: HashMap<String, Float>,
                    }

                    let snap = JsonSnap {
                        ts: snapshot.ts,
                        metrics: HashMap::from_iter(
                            snapshot
                            .data
                            .iter()
                            .map(|(name, value)| (String::from_utf8_lossy(&name[..]).to_string(), *value)),
                        ),
                    };
                    let mut writer = buf.writer();
                    serde_json::to_writer_pretty(&mut writer, &snap).unwrap_or(());
                    buf = writer.into_inner();
                }
                *response.body_mut() = Body::from(buf.freeze());
                Box::pin(ok(response))
            }
            (&Method::GET, _, _) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
                Box::pin(ok(response))
            }
            (&Method::POST, "/consensus", _) => {
                let fut = async move {
                    let body = to_bytes(req.into_body()).await?;
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
                            info!(log.clone(), "state changed"; "consensus_state"=>format!("{:?}", status.consensus_status), "leader_state"=>status.leader_status);

                            Ok(response)
                        }
                        Ok(command) => {
                            info!(log, "bad command received"; "command"=>format!("{:?}", command));
                            *response.status_mut() = StatusCode::BAD_REQUEST;

                            Ok(response)
                        }
                        Err(e) => {
                            info!(log, "error parsing command"; "error"=>e.to_string());
                            *response.status_mut() = StatusCode::BAD_REQUEST;

                            Ok(response)
                        }
                    }
                };

                Box::pin(fut)
            }
            (&Method::POST, _, _) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
                Box::pin(ok(response))
            }
            _ => {
                *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
                Box::pin(ok(response))
            }
        }
    }
}

pub struct MgmtServer(pub Logger, pub SocketAddr);

impl<T> Service<T> for MgmtServer {
    type Response = MgmtService;
    type Error = std::io::Error;
    type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        ok(MgmtService::new(
                self.0.new(o!("source"=>"management-server", "server"=>format!("{}", self.1.clone()))),
        ))
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
            log: log.new(o!("source"=>"management-client", "server"=>format!("{}", address.clone()))),
            address,
            command,
        }
    }

    pub async fn run(self) -> Result<(), MgmtError> {
        let Self { log, address, command } = self;
        let mut req = Request::default();

        info!(log, "received command {:?}", command);
        match command {
            MgmtCommand::Status => {
                *req.method_mut() = Method::GET;
                *req.uri_mut() = format!("http://{}/status", address).parse().expect("creating url for management command ");

                let client = Client::new();
                let clog = log.clone();
                let resp = client.request(req).await?;
                if resp.status() != StatusCode::OK {
                    warn!(clog, "Bad status returned from server: {:?}", resp);
                    return Err(MgmtError::BadResponse);
                }
                let body = to_bytes(resp.into_body()).await?;
                let parsed = serde_json::from_slice::<ServerStatus>(&body);
                match parsed {
                    Ok(status) => {
                        println!("{:?}", status);
                    }
                    Err(e) => {
                        println!("Error parsing server response: {}", e.to_string());
                    }
                }
                Ok::<(), MgmtError>(())
            }
            command @ MgmtCommand::ConsensusCommand(_, _) => {
                *req.method_mut() = Method::POST;
                *req.uri_mut() = format!("http://{}/consensus", address).parse().expect("creating url for management command");
                let body = serde_json::to_vec_pretty(&command).unwrap();
                *req.body_mut() = Body::from(body);

                let client = Client::new();
                let clog = log.clone();
                let resp = client.request(req).await?;
                if resp.status() != StatusCode::OK {
                    warn!(clog, "Bad status returned from server: {:?}", resp);
                    return Err(MgmtError::BadResponse);
                }
                let body = to_bytes(resp.into_body()).await?;
                let parsed = serde_json::from_slice::<ServerStatus>(&body);
                match parsed {
                    Ok(status) => {
                        println!("New server state: {:?}", status);
                    }
                    Err(e) => {
                        println!("Error parsing server response: {}", e.to_string());
                    }
                }
                Ok::<(), MgmtError>(())
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::SocketAddr;
    use std::time::Duration;
    use {slog, slog_async, slog_term};

    use slog::{Drain, Logger};
    use tokio::runtime::{Builder, Runtime};
    use tokio::time::sleep;

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

        let mgmt_listen: ::std::net::SocketAddr = "127.0.0.1:8137".parse().unwrap();
        let runtime = Builder::new_current_thread().enable_all().build().expect("creating runtime for testing management server");

        let m_serv_log = rlog.clone();
        let m_server = async move { hyper::Server::bind(&mgmt_listen).serve(MgmtServer(m_serv_log, mgmt_listen)).await };

        runtime.spawn(m_server);

        (runtime, rlog, mgmt_listen)
    }

    #[test]
    fn management_command() {
        let (runtime, log, address) = prepare_runtime_with_server();

        let check = async move {
            // let server some time to settle
            // then test the commands
            let d = sleep(Duration::from_secs(1));
            d.await;

            // status
            let command = MgmtCommand::Status;
            let client = MgmtClient::new(log.clone(), address, command);
            client.run().await.expect("status command");

            // consensus state change command
            let command = MgmtCommand::ConsensusCommand(ConsensusAction::Enable, LeaderAction::Enable);
            let client = MgmtClient::new(log.clone(), address, command);

            client.run().await.expect("consensus command");
            // ensure state has changed
            let state = ServerStatus::new(); // actual status is taken from global var
            assert_eq!(
                state,
                ServerStatus {
                    consensus_status: ConsensusState::Enabled,
                    leader_status: true
                }
            );
        };

        runtime.spawn(check);
        let test_delay = async { sleep(Duration::from_secs(3)).await };
        runtime.block_on(test_delay);
    }
}
