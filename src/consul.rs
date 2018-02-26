use std::sync::atomic::Ordering;
use std::net::SocketAddr;
use std::time::Duration;

use hyper;
use hyper::header::{ContentLength, ContentType};
use tokio_core::reactor::{Handle, Interval, Timeout};
use futures::Stream;
use futures::future::{err, loop_fn, ok, Future, IntoFuture, Loop};
use serde_json::{self, from_slice};
use slog::Logger;
use {CAN_LEADER, FORCE_LEADER, IS_LEADER};

#[derive(Fail, Debug)]
pub enum ConsulError {
    #[fail(display = "session create error: {}", _0)]
    Session(String),

    #[fail(display = "server responded with bad status code '{}': {}", _0, _1)]
    HttpStatus(hyper::StatusCode, String),

    #[fail(display = "agent connection timed out")]
    ConnectionTimeout,

    #[fail(display = "Http error: {}", _0)]
    Http(
        #[cause]
        hyper::Error
    ),

    #[fail(display = "Parsing response: {}", _0)]
    Parsing(
        #[cause]
        serde_json::Error
    ),
    #[fail(display = "I/O error {}", _0)]
    Io(
        #[cause]
        ::std::io::Error
    ),

    #[fail(display = "{}", _0)]
    Renew(String),
}

#[derive(Deserialize)]
struct ConsulSessionResponse {
    #[serde(rename = "ID")]
    id: String,
}

pub struct ConsulConsensus {
    log: Logger,
    agent: SocketAddr,
    handle: Handle,
    key: String,
    session_ttl: Duration,
    renew_time: Duration,
    error_pause: Duration,
}

impl ConsulConsensus {
    pub fn new(log: &Logger, agent: SocketAddr, key: String, handle: &Handle) -> Self {
        Self {
            log: log.new(o!("source"=>"consensus")),
            agent,
            handle: handle.clone(),
            key: key,
            session_ttl: Duration::from_secs(7),
            renew_time: Duration::from_secs(1),
            error_pause: Duration::from_secs(1),
        }
    }

    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }

    pub fn set_session_ttl(&mut self, ttl: Duration) {
        self.session_ttl = ttl;
    }

    pub fn set_renew_time(&mut self, ttl: Duration) {
        self.renew_time = ttl;
    }

    pub fn set_error_pause(&mut self, pause: Duration) {
        self.error_pause = pause;
    }
}

impl IntoFuture for ConsulConsensus {
    type Item = ();
    type Error = ConsulError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            agent,
            handle,
            key,
            session_ttl,
            renew_time,
            error_pause,
        } = self;

        let renew_loop = loop_fn((), move |()| {
            let key = key.clone();
            let handle = handle.clone();
            let log = log.clone();
            let session = ConsulSession {
                log: log.new(o!("source"=>"consul-session")),
                agent: agent.clone(),
                handle: handle.clone(),
                ttl: session_ttl.clone(),
            };

            let renewlog = log.clone();
            // this tries to reconnect to consul infinitely
            let loop_session = loop_fn(session, move |session| {
                let log = log.clone();
                let new_session = session.clone();
                session.into_future().then(move |res| match res {
                    Err(e) => {
                        warn!(log, "error getting consul session"; "error" => format!("{}", e));
                        ok(Loop::Continue(new_session))
                    }
                    Ok(None) => {
                        warn!(log, "timed out getting consul session");
                        ok(Loop::Continue(new_session))
                    }
                    Ok(Some(s)) => ok(Loop::Break(s)),
                })
            });

            let rhandle = handle.clone();
            // the returned future will work until renew error
            let renew =
                // make connection, then start to renew
                loop_session.and_then(move |sid| {
                    let handle = rhandle.clone();
                    let timer = Interval::new(renew_time, &handle).unwrap();

                    timer.map_err(|e| ConsulError::Io(e)).for_each(move |_| {
                        let log = renewlog.clone();
                        let can_leader = CAN_LEADER.load(Ordering::SeqCst);
                        // do work only if taking leadership is enabled
                        if can_leader {
                            let renew = ConsulRenew {
                                agent: agent,
                                handle: handle.clone(),
                                sid: sid.clone(),
                                ttl: session_ttl,
                            }.into_future()
                            .map_err(move |e| {
                                warn!(log, "session renew error"; "error"=> format!("{}",e));
                                e
                            });

                            let log = renewlog.clone();
                            let acquire = ConsulAcquire {
                                log: log.new(o!("source"=>"consul-acquire")),
                                agent,
                                handle: handle.clone(),
                                sid: sid.clone(),
                                key: key.clone(),
                            }.into_future()
                            .map_err(move |e| {
                                warn!(log, "session acquire error"; "error"=>format!("{:?}", e));
                                e
                            });

                            Box::new(renew.join(acquire).map(|_|()))
                        } else {
                            IS_LEADER.store(FORCE_LEADER.load(Ordering::SeqCst), Ordering::SeqCst);
                            Box::new(ok(())) as Box<Future<Item=(), Error=ConsulError>>
                        }
                    })
                });

            // restart the whole loop as soon as ANY future exits with any result
            // (is is supposed to exit only with error)
            renew.then(move |_| {
                Timeout::new(error_pause, &handle)
                    .unwrap()
                    .then(move |_| Ok(Loop::Continue(())))
            })
        });
        Box::new(renew_loop)
    }
}

#[derive(Clone)]
pub struct ConsulSession {
    log: Logger,
    agent: SocketAddr,
    handle: Handle,
    ttl: Duration,
}

impl IntoFuture for ConsulSession {
    type Item = Option<String>;
    type Error = ConsulError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            agent,
            handle,
            ttl,
        } = self;
        // create HTTP client for consul agent leader
        let client = hyper::Client::new(&handle);
        let mut session_req = hyper::Request::new(
            hyper::Method::Put,
            format!("http://{}/v1/session/create", agent)
                .parse()
                .expect("bad session create url"),
        );

        let ttl_ns = ttl.as_secs() * 1000000000u64 + ttl.subsec_nanos() as u64;
        let b = format!(
            "{{\"TTL\": \"{}ns\", \"LockDelay\": \"{}ns\"}}",
            ttl_ns,
            ttl_ns
        );
        let bodylen = b.len() as u64;
        session_req.set_body(b);
        // Override sending request as multipart
        session_req.headers_mut().set(ContentLength(bodylen));
        session_req.headers_mut().set(
            ContentType::form_url_encoded(),
        );

        let thandle = handle.clone();
        let c_session = client
            .request(session_req)
            .map_err(|e| ConsulError::Http(e))
            .and_then(move |resp| {
                let status = resp.status();
                if status == hyper::StatusCode::Ok {
                    let body = resp.body()
                        .concat2()
                        .map_err(|e| ConsulError::Http(e))
                        .and_then(move |body| {
                            let resp: ConsulSessionResponse =
                                try!(from_slice(&body).map_err(|e| ConsulError::Parsing(e)));
                            debug!(log, "new session"; "id"=>format!("{}", resp.id));
                            Ok(Some(resp.id))
                        });
                    Box::new(body) as Box<Future<Item = Option<String>, Error = ConsulError>>
                } else {
                    let body = resp.body().concat2().map_err(|e| ConsulError::Http(e));
                    // TODO make this into option
                    let sleep = Timeout::new(Duration::from_millis(1000), &thandle)
                        .unwrap()
                        .map_err(|e| ConsulError::Io(e));
                    let future = sleep.join(body).then(move |res| match res {
                        Ok((_, body)) => Err::<Option<String>, _>(ConsulError::HttpStatus(
                            status,
                            format!("{:?}", String::from_utf8(body.to_vec())),
                        )),
                        Err(e) => Err(e),
                    });
                    Box::new(future)
                }
            });
        let timeout = Timeout::new(ttl, &handle).unwrap();
        let future = timeout
            .map_err(|e| ConsulError::Io(e))
            .map(|_| None)
            .select(c_session)
            .map(|res| res.0)
            .map_err(|e| e.0);
        Box::new(future)
    }
}

pub struct ConsulRenew {
    agent: SocketAddr,
    handle: Handle,
    sid: String,
    ttl: Duration,
}

impl IntoFuture for ConsulRenew {
    type Item = ();
    type Error = ConsulError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            agent,
            handle,
            sid,
            ttl,
        } = self;
        let mut renew_req = hyper::Request::new(
            hyper::Method::Put,
            format!("http://{}/v1/session/renew/{}", agent, sid.clone())
                .parse()
                .expect("creating session renew url"),
        );

        let ttl_ns = ttl.as_secs() * 1000000000u64 + ttl.subsec_nanos() as u64;
        let b = format!("{{\"TTL\": \"{}ns\"}}", ttl_ns);
        let bodylen = b.len() as u64;

        renew_req.set_body(b);
        renew_req.headers_mut().set(ContentLength(bodylen));
        renew_req.headers_mut().set(ContentType::form_url_encoded());

        let renew_client = hyper::Client::new(&handle);
        let future = renew_client.request(renew_req).then(move |res| match res {
            Err(e) => Box::new(err(ConsulError::Http(e))),
            Ok(resp) => {
                if resp.status() != hyper::StatusCode::Ok {
                    let status = resp.status().clone();
                    let body = resp.body()
                        .concat2()
                        .map_err(|e| ConsulError::Http(e))
                        .and_then(move |body| {
                            let msg = format!(
                                "renew error: {:?} {:?}",
                                status,
                                String::from_utf8(body.to_vec())
                            );
                            Err(ConsulError::Renew(msg))
                        });
                    Box::new(body) as Box<Future<Item = (), Error = ConsulError>>
                } else {
                    Box::new(ok(()))
                }
            }
        });
        Box::new(future)
    }
}

pub struct ConsulAcquire {
    log: Logger,
    agent: SocketAddr,
    handle: Handle,
    sid: String,
    key: String,
}

impl IntoFuture for ConsulAcquire {
    type Item = ();
    type Error = ConsulError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            agent,
            handle,
            sid,
            key,
        } = self;

        let req = hyper::Request::new(
            hyper::Method::Put,
            format!("http://{}/v1/kv/{}/?acquire={}", agent, key, sid)
                .parse()
                .expect("bad key acquire url"),
        );

        let client = hyper::Client::new(&handle);
        let acquire = client
            .request(req)
            .map_err(|e| ConsulError::Http(e))
            .and_then(move |resp| {
                resp.body()
                    .concat2()
                    .map_err(|e| ConsulError::Http(e))
                    .and_then(move |body| {
                        let mut acquired: bool =
                            try!(from_slice(&body).map_err(|e| ConsulError::Parsing(e)));

                        let force_leader = FORCE_LEADER.load(Ordering::SeqCst);
                        let is_leader = IS_LEADER.load(Ordering::SeqCst);
                        if force_leader {
                            acquired = true
                        }
                        IS_LEADER.store(acquired, Ordering::SeqCst);
                        if is_leader != acquired {
                            warn!(log, "leader state change: {} -> {}", is_leader, acquired);
                        }
                        Ok(())
                    })
            });
        Box::new(acquire)
    }
}
