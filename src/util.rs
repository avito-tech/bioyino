use libc;
use std::ffi::CStr;
use std::io;
use std::net::SocketAddr;
use std::net::TcpStream as StdTcpStream;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::Either;
use futures::sync::mpsc::Sender;
use futures::{Async, Future, IntoFuture, Poll, Sink, Stream};
use net2::TcpBuilder;
use resolve::resolver;
use slog::{info, o, warn, Drain, Logger};
use tokio::executor::current_thread::spawn;
use tokio::net::TcpListener;
use tokio::timer::{Delay, Interval};

use crate::task::Task;
use crate::Float;
use crate::{AGG_ERRORS, DROPS, EGRESS, INGRESS, INGRESS_METRICS, PARSE_ERRORS, PEER_ERRORS};
use bioyino_metric::{Metric, MetricType};

use crate::{ConsensusState, CONSENSUS_STATE, IS_LEADER};

pub fn prepare_log(root: &'static str) -> Logger {
    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"test", "test"=>root));
    return rlog;
}

pub fn try_resolve(s: &str) -> SocketAddr {
    s.parse().unwrap_or_else(|_| {
        // for name that have failed to be parsed we try to resolve it via DNS
        let mut split = s.split(':');
        let host = split.next().unwrap(); // Split always has first element
        let port = split.next().expect("port not found");
        let port = port.parse().expect("bad port value");

        let first_ip = resolver::resolve_host(host).expect(&format!("failed resolving {:}", &host)).next().expect("at least one IP address required");
        SocketAddr::new(first_ip, port)
    })
}

pub fn bound_stream(addr: &SocketAddr) -> Result<StdTcpStream, io::Error> {
    let builder = TcpBuilder::new_v4()?;
    builder.bind(addr)?;
    builder.to_tcp_stream()
}

pub fn reusing_listener(addr: &SocketAddr) -> Result<TcpListener, io::Error> {
    let builder = TcpBuilder::new_v4()?;
    builder.reuse_address(true)?;
    builder.bind(addr)?;

    // backlog parameter will be limited by SOMAXCONN on Linux, which is usually set to 128
    let listener = builder.listen(65536)?;
    listener.set_nonblocking(true)?;
    TcpListener::from_std(listener, &tokio::reactor::Handle::default())
}

// TODO impl this correctly and use instead of try_resolve
// PROFIT: gives libnss-aware behaviour
/*
   fn _try_resolve_nss(name: &str) {
   use std::io;
   use std::ffi::CString;
   use std::ptr::{null_mut, null};
   use libc::*;

   let domain= CString::new(Vec::from(name)).unwrap().into_raw();
   let mut result: *mut addrinfo = null_mut();

   let success = unsafe {
   getaddrinfo(domain, null_mut(), null(), &mut result)
   };

   if success != 0 {
//        let errno = unsafe { *__errno_location() };
println!("{:?}", io::Error::last_os_error());
} else {
let mut cur = result;
while cur != null_mut() {
unsafe{
println!("LEN {:?}", (*result).ai_addrlen);
println!("DATA {:?}", (*(*result).ai_addr).sa_data);
cur = (*result).ai_next;
}
}
}
}
*/

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

// A future to send own stats. Never gets ready.
pub struct OwnStats {
    interval: u64,
    prefix: String,
    timer: Interval,
    chan: Sender<Task>,
    log: Logger,
}

impl OwnStats {
    pub fn new(interval: u64, prefix: String, chan: Sender<Task>, log: Logger) -> Self {
        let log = log.new(o!("source"=>"stats"));
        let now = Instant::now();
        let dur = Duration::from_millis(if interval < 100 { 1000 } else { interval }); // exclude too small intervals
        Self { interval, prefix, timer: Interval::new(now + dur, dur), chan, log }
    }

    pub fn get_stats(&mut self) {
        let mut buf = BytesMut::with_capacity((self.prefix.len() + 10) * 7); // 10 is suffix len, 7 is number of metrics
        macro_rules! add_metric {
            ($global:ident, $value:ident, $suffix:expr) => {
                let $value = $global.swap(0, Ordering::Relaxed) as Float;
                if self.interval > 0 {
                    buf.put(&self.prefix);
                    buf.put(".");
                    buf.put(&$suffix);
                    let name = buf.take().freeze();
                    let metric = Metric::new($value, MetricType::Counter, None, None).unwrap();
                    let log = self.log.clone();
                    let sender = self.chan.clone().send(Task::AddMetric(name, metric)).map(|_| ()).map_err(move |_| warn!(log, "stats future could not send metric to task"));
                    spawn(sender);
                }
            };
        };
        add_metric!(EGRESS, egress, "egress");
        add_metric!(INGRESS, ingress, "ingress");
        add_metric!(INGRESS_METRICS, ingress_m, "ingress-metric");
        add_metric!(AGG_ERRORS, agr_errors, "agg-error");
        add_metric!(PARSE_ERRORS, parse_errors, "parse-error");
        add_metric!(PEER_ERRORS, peer_errors, "peer-error");
        add_metric!(DROPS, drops, "drop");
        if self.interval > 0 {
            let s_interval = self.interval as f64 / 1000f64;

            info!(self.log, "stats";
                  "egress" => format!("{:2}", egress / s_interval),
                  "ingress" => format!("{:2}", ingress / s_interval),
                  "ingress-m" => format!("{:2}", ingress_m / s_interval),
                  "a-err" => format!("{:2}", agr_errors / s_interval),
                  "p-err" => format!("{:2}", parse_errors / s_interval),
                  "pe-err" => format!("{:2}", peer_errors / s_interval),
                  "drops" => format!("{:2}", drops / s_interval),
                  );
        }
    }
}

impl Future for OwnStats {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.timer.poll() {
                Ok(Async::Ready(Some(_))) => {
                    self.get_stats();
                }
                Ok(Async::Ready(None)) => unreachable!(),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_) => return Err(()),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateCounterOptions {
    pub threshold: u32,
    pub prefix: Bytes,
    pub suffix: Bytes,
}

#[derive(Clone, Debug)]
/// Builder for `BackoffRetry`, delays are specified in milliseconds
pub struct BackoffRetryBuilder {
    pub delay: u64,
    pub delay_mul: f32,
    pub delay_max: u64,
    pub retries: usize,
}

impl Default for BackoffRetryBuilder {
    fn default() -> Self {
        Self { delay: 250, delay_mul: 2f32, delay_max: 5000, retries: 25 }
    }
}

impl BackoffRetryBuilder {
    pub fn spawn<F>(self, action: F) -> BackoffRetry<F>
        where
        F: IntoFuture + Clone,
        {
            let inner = Either::A(action.clone().into_future());
            BackoffRetry { action, inner: inner, options: self }
        }
}

/// TCP client that is able to reconnect with customizable settings
pub struct BackoffRetry<F: IntoFuture> {
    action: F,
    inner: Either<F::Future, Delay>,
    options: BackoffRetryBuilder,
}

impl<F> Future for BackoffRetry<F>
where
F: IntoFuture + Clone,
{
    type Item = F::Item;
    type Error = Option<F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let (rotate_f, rotate_t) = match self.inner {
                // we are polling a future currently
                Either::A(ref mut future) => match future.poll() {
                    Ok(Async::Ready(item)) => {
                        return Ok(Async::Ready(item));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => {
                        if self.options.retries == 0 {
                            return Err(Some(e));
                        } else {
                            (true, false)
                        }
                    }
                },
                Either::B(ref mut timer) => {
                    match timer.poll() {
                        // we are waiting for the delay
                        Ok(Async::Ready(())) => (false, true),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(_) => unreachable!(), // timer should not return error
                    }
                }
            };

            if rotate_f {
                self.options.retries -= 1;
                let delay = self.options.delay as f32 * self.options.delay_mul;
                let delay = if delay <= self.options.delay_max as f32 { delay as u64 } else { self.options.delay_max as u64 };
                let delay = Delay::new(Instant::now() + Duration::from_millis(delay));
                self.inner = Either::B(delay);
            } else if rotate_t {
                self.inner = Either::A(self.action.clone().into_future());
            }
        }
    }
}
