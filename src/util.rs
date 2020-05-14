use std::ffi::CStr;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::net::TcpStream as StdTcpStream;
use std::sync::atomic::Ordering;
use std::time::{Duration};

use thiserror::Error;

use futures3::future::{Future as Future3, TryFutureExt};
use net2::{TcpBuilder, unix::UnixTcpBuilderExt};
use resolve::resolver;
use slog::{o, warn, Drain, Logger};
use trust_dns_resolver::TokioAsyncResolver;

use crate::{ConsensusState, CONSENSUS_STATE, IS_LEADER};

#[cfg(test)]
use bioyino_metric::name::MetricName;

#[derive(Error, Debug)]
pub enum OtherError {
    #[error("resolving")]
    Resolving(#[from] trust_dns_resolver::error::ResolveError),

    #[error("integer parsing error")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("no IP addresses found for {}", _0)]
    NotFound(String),
}

pub fn prepare_log(root: &'static str) -> Logger {
    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    slog::Logger::root(drain, o!("program"=>"test", "test"=>root))
}

pub fn try_resolve(s: &str) -> SocketAddr {
    s.parse().unwrap_or_else(|_| {
        // for name that have failed to be parsed we try to resolve it via DNS
        let mut split = s.split(':');
        let host = split.next().unwrap(); // Split always has first element
        let port = split.next().expect("port not found");
        let port = port.parse().expect("bad port value");

        let first_ip = resolver::resolve_host(host)
            .unwrap_or_else(|_| panic!("failed resolving {:}", &host))
            .next()
            .expect("at least one IP address required");
        SocketAddr::new(first_ip, port)
    })
}

pub fn bound_stream(addr: &SocketAddr) -> Result<StdTcpStream, io::Error> {
    let builder = TcpBuilder::new_v4()?;
    builder.reuse_address(true)?;
    builder.reuse_port(true)?;
    builder.bind(addr)?;
    builder.to_tcp_stream()
}

//pub fn reusing_listener(addr: &SocketAddr) -> Result<TcpListener, io::Error> {
//let builder = TcpBuilder::new_v4()?;
//builder.reuse_address(true)?;
//builder.bind(addr)?;

//// backlog parameter will be limited by SOMAXCONN on Linux, which is usually set to 128
//let listener = builder.listen(65536)?;
//listener.set_nonblocking(true)?;
//TcpListener::from_std(listener, &tokio::reactor::Handle::default())
//}

pub async fn resolve_with_port(host: &str, default_port: u16) -> Result<SocketAddr, OtherError> {
    let (host, port) = if let Some(pos) = host.find(':') {
        let (host, port) = host.split_at(pos);
        let port = &port[1..]; // remove ':'
        let port: u16 = port.parse()?;
        (host, port)
    } else {
        (host, default_port)
    };

    let ip = resolve_to_first(host).await?;

    Ok(SocketAddr::new(ip, port))
}

pub async fn resolve_to_first(host: &str) -> Result<IpAddr, OtherError> {
    let resolver =
        TokioAsyncResolver::tokio_from_system_conf().await?;

    let response = resolver.lookup_ip(host).await?;

    // Run the lookup until it resolves or errors
    // There can be many addresses associated with the name,
    response
        .iter()
        .next()
        .ok_or(OtherError::NotFound(host.to_string()))
}

/// Get hostname. Copypasted from some crate
pub fn get_hostname() -> Option<String> {
    let len = 255;
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr() as *mut libc::c_char;

    unsafe {
        if libc::gethostname(ptr, len as libc::size_t) != 0 {
            return None;
        }
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

pub fn switch_leader(acquired: bool, log: &Logger) {
    let should_set = {
        let state = &*CONSENSUS_STATE.lock().unwrap();
        // only set leader when consensus is enabled
        state == &ConsensusState::Enabled
    };
    if should_set {
        let is_leader = IS_LEADER.load(Ordering::SeqCst);
        if is_leader != acquired {
            warn!(log, "leader state change: {} -> {}", is_leader, acquired);
        }
        IS_LEADER.store(acquired, Ordering::SeqCst);
    }
}

#[cfg(test)]
pub(crate) fn new_test_graphite_name(s: &'static str) -> MetricName {
    let mut intermediate = Vec::new();
    intermediate.resize(9000, 0u8);
    let mode = bioyino_metric::name::TagFormat::Graphite;

    MetricName::new(s.into(), mode, &mut intermediate).unwrap()
}

#[derive(Clone)]
pub struct Backoff {
    pub delay: u64,
    pub delay_mul: f32,
    pub delay_max: u64,
    pub retries: usize,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            delay: 500,
            delay_mul: 2f32,
            delay_max: 10000,
            retries: std::usize::MAX,
        }
    }
}

impl Backoff {
    pub async fn sleep(&mut self) -> Result<usize, ()> {
        if self.retries == 0 {
            Err(())
        } else {
            self.retries -= 1;
            let delay = self.next_sleep();

            tokio2::time::delay_for(Duration::from_millis(delay)).await;
            Ok(self.retries)
        }
    }

    pub fn next_sleep(&self) -> u64 {
        let delay = self.delay as f32 * self.delay_mul;
        if delay <= self.delay_max as f32 {
            delay as u64
        } else {
            self.delay_max as u64
        }
    }
}

// TODO maybe let caller know it was out of tries, not just the last error
pub async fn retry_with_backoff<F, I, R, E>(mut bo: Backoff, mut f: F) -> Result<R, E>
where
    I: Future3<Output = Result<R, E>>,
    F: FnMut() -> I,
    {
        loop {
            match f().await {
                r @ Ok(_) => {
                    break r
                }
                Err(e) => {
                    bo.sleep().map_err(|()| e).await?;
                    continue;
                }
            }
        }
    }
