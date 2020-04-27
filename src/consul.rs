use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::future::{err, loop_fn, ok, Either, Future, IntoFuture, Loop};
use futures::Stream;
use hyper;
use hyper::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Method, StatusCode};
use mime::WWW_FORM_URLENCODED;
use serde_json::{self, from_slice};
use slog::{debug, o, warn, Logger};
use tokio::timer::{self, Delay, Interval};

use failure_derive::Fail;
use serde_derive::Deserialize;

use crate::util::switch_leader;
use crate::{ConsensusState, CONSENSUS_STATE};

#[derive(Fail, Debug)]
pub enum ConsulError {
    #[fail(display = "session create error: {}", _0)]
    Session(String),

    #[fail(display = "server responded with bad status code '{}': {}", _0, _1)]
    HttpStatus(hyper::StatusCode, String),

    #[fail(display = "agent connection timed out")]
    ConnectionTimeout,

    #[fail(display = "Http error: {}", _0)]
    Http(#[cause] hyper::Error),

    #[fail(display = "Parsing response: {}", _0)]
    Parsing(#[cause] serde_json::Error),
    #[fail(display = "I/O error {}", _0)]
    Io(#[cause] ::std::io::Error),

    #[fail(display = "{}", _0)]
    Renew(String),

    #[fail(display = "creating timer: {}", _0)]
    Timer(timer::Error),
}

#[derive(Deserialize)]
struct ConsulSessionResponse {
    #[serde(rename = "ID")]
    id: String,
}

pub struct ConsulConsensus {
    log: Logger,
    agent: SocketAddr,
    key: String,
    session_ttl: Duration,
    renew_time: Duration,
    error_pause: Duration,
}

impl ConsulConsensus {
    pub fn new(log: &Logger, agent: SocketAddr, key: String) -> Self {
        Self {
            log: log.new(o!("source"=>"consensus")),
            agent,
            key,
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
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            log,
            agent,
            key,
            session_ttl,
            renew_time,
            error_pause,
        } = self;

        let renew_loop = loop_fn((), move |()| {
            let key = key.clone();
            let log = log.clone();
            let session = ConsulSession {
                log: log.new(o!("source"=>"consul-session")),
                agent,
                ttl: session_ttl,
            };

            let renewlog = log.clone();
            /*
            let session_retrier = BackoffRetryBuilder {
            delay: 1000,
            delay_mul: 1f32,
            delay_max: 1000,
            retries: ::std::usize::MAX,
            };

            let session = session_retrier.spawn(session);
            */
            // this tries to reconnect to consul infinitely
            let loop_session = loop_fn(session, move |session| {
                let log = log.clone();
                let new_session = session.clone();
                let should_connect = {
                    let state = &*CONSENSUS_STATE.lock().unwrap();
                    // connect to consul only in Enabled/Paused state
                    state != &ConsensusState::Disabled
                };

                if should_connect {
                    Either::A(session.into_future().then(move |res| match res {
                        Err(e) => {
                            warn!(log, "error getting consul session"; "error" => format!("{}", e));
                            let new_session = new_session.clone();
                            Box::new(Delay::new(Instant::now() + error_pause).then(move |_| Ok(Loop::Continue(new_session))))
                                as Box<dyn Future<Item = Loop<_, _>, Error = _> + Send>
                            //ok(Loop::Continue(new_session))
                        }
                        Ok(None) => {
                            warn!(log, "timed out getting consul session");
                            Box::new(ok(Loop::Continue(new_session)))
                        }
                        Ok(Some(s)) => Box::new(ok(Loop::Break(s))),
                    }))
                } else {
                    Either::B(Delay::new(Instant::now() + error_pause).then(move |_| Ok(Loop::Continue(new_session))))
                }
            });

            // the returned future will work until renew error
            let renew =
                // make connection, then start to renew
                loop_session
                .and_then(move |sid| {
                    let timer = Interval::new(Instant::now()+renew_time, renew_time);

                    timer.map_err(ConsulError::Timer).for_each(move |_| {
                        let log = renewlog.clone();
                        let should_renew = {
                            let state = &*CONSENSUS_STATE.lock().unwrap();
                            // renew the key only in Enabled/Paused state
                            state != &ConsensusState::Disabled
                        };
                        if should_renew {
                            let renew = ConsulRenew {
                                agent,
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
                                sid: sid.clone(),
                                key: key.clone(),
                            }.into_future()
                            .map_err(move |e| {
                                warn!(log, "session acquire error"; "error"=>format!("{:?}", e));
                                e
                            });

                            Either::A(renew.join(acquire).map(|_|()))
                        } else {
                            Either::B(ok(()))// as Box<Future<Item=(), Error=ConsulError>>)
                        }
                    })
                });

            // restart the whole loop as soon as ANY future exits with any result
            // (is is supposed to exit only with error)
            renew.then(move |_| Delay::new(Instant::now() + error_pause).then(move |_| Ok(Loop::Continue(()))))
        });
        Box::new(renew_loop)
    }
}

#[derive(Clone)]
pub struct ConsulSession {
    log: Logger,
    agent: SocketAddr,
    ttl: Duration,
}

impl IntoFuture for ConsulSession {
    type Item = Option<String>;
    type Error = ConsulError;
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self { log, agent, ttl } = self;
        // create HTTP client for consul agent leader
        let client = hyper::Client::<hyper::client::HttpConnector, _>::new();
        let mut session_req = hyper::Request::default();
        *session_req.method_mut() = Method::PUT;
        *session_req.uri_mut() = format!("http://{}/v1/session/create", agent).parse().expect("bad session create url");

        let ttl_ns = ttl.as_secs() * 1_000_000_000u64 + ttl.subsec_nanos() as u64;
        let b = format!("{{\"TTL\": \"{}ns\", \"LockDelay\": \"{}ns\"}}", ttl_ns, ttl_ns);
        let bodylen = b.len() as u64;
        *session_req.body_mut() = hyper::Body::from(b);
        // Override sending request as multipart
        session_req
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from_str(&format!("{}", bodylen)).unwrap());
        session_req
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_str(WWW_FORM_URLENCODED.as_str()).unwrap());

        let c_session = client.request(session_req).map_err(ConsulError::Http).and_then(move |resp| {
            let status = resp.status();
            if status == StatusCode::OK {
                let body = resp.into_body().concat2().map_err(ConsulError::Http).and_then(move |body| {
                    let resp: ConsulSessionResponse =
                        //try!(from_slice(&body).map_err(|e| ConsulError::Parsing(e)));
                        from_slice(&body).map_err(ConsulError::Parsing)?;
                    debug!(log, "new session"; "id"=>resp.id.to_string());
                    Ok(Some(resp.id))
                });
                Box::new(body) as Box<dyn Future<Item = Option<String>, Error = ConsulError> + Send>
            } else {
                let body = resp.into_body().concat2().map_err(ConsulError::Http);
                // TODO make this into option
                let sleep = Delay::new(Instant::now() + Duration::from_millis(1000)).map_err(ConsulError::Timer);
                let future = sleep.join(body).then(move |res| match res {
                    Ok((_, body)) => Err::<Option<String>, _>(ConsulError::HttpStatus(status, format!("{:?}", String::from_utf8(body.to_vec())))),
                    Err(e) => Err(e),
                });
                Box::new(future)
            }
        });
        let timeout = Delay::new(Instant::now() + ttl);
        let future = timeout
            .map_err(ConsulError::Timer)
            .map(|_| None)
            .select(c_session)
            .map(|res| res.0)
            .map_err(|e| e.0);
        Box::new(future)
    }
}

pub struct ConsulRenew {
    agent: SocketAddr,
    sid: String,
    ttl: Duration,
}

impl IntoFuture for ConsulRenew {
    type Item = ();
    type Error = ConsulError;
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self { agent, sid, ttl } = self;
        let mut renew_req = hyper::Request::default();
        *renew_req.method_mut() = Method::PUT;
        *renew_req.uri_mut() = format!("http://{}/v1/session/renew/{}", agent, sid.clone())
            .parse()
            .expect("creating session renew url");

        let ttl_ns = ttl.as_secs() * 1_000_000_000u64 + ttl.subsec_nanos() as u64;
        let b = format!("{{\"TTL\": \"{}ns\"}}", ttl_ns);
        let bodylen = b.len() as u64;

        *renew_req.body_mut() = hyper::Body::from(b);
        // Override sending request as multipart
        renew_req
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from_str(&format!("{}", bodylen)).unwrap());
        renew_req
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_str(WWW_FORM_URLENCODED.as_str()).unwrap());

        let renew_client = hyper::Client::new();
        let future = renew_client.request(renew_req).then(move |res| match res {
            Err(e) => Box::new(err(ConsulError::Http(e))),
            Ok(resp) => {
                if resp.status() != StatusCode::OK {
                    let status = resp.status();
                    let body = resp.into_body().concat2().map_err(ConsulError::Http).and_then(move |body| {
                        let msg = format!("renew error: {:?} {:?}", status, String::from_utf8(body.to_vec()));
                        Err(ConsulError::Renew(msg))
                    });
                    Box::new(body) as Box<dyn Future<Item = (), Error = ConsulError> + Send>
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
    sid: String,
    key: String,
}

impl IntoFuture for ConsulAcquire {
    type Item = ();
    type Error = ConsulError;
    type Future = Box<dyn Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self { log, agent, sid, key } = self;

        let mut req = hyper::Request::default();
        *req.method_mut() = Method::PUT;
        *req.uri_mut() = format!("http://{}/v1/kv/{}/?acquire={}", agent, key, sid).parse().expect("bad key acquire url");
        //.body(hyper::Body::empty())
        //.expect("building acquire request");

        let client = hyper::Client::new();
        let acquire = client.request(req).map_err(ConsulError::Http).and_then(move |resp| {
            resp.into_body().concat2().map_err(ConsulError::Http).and_then(move |body| {
                let acquired: bool =
                    //try!(from_slice(&body).map_err(|e| ConsulError::Parsing(e)));
                    from_slice(&body).map_err(ConsulError::Parsing)?;

                switch_leader(acquired, &log);
                // let should_set = {
                //let state = &*CONSENSUS_STATE.lock().unwrap();
                //// only set leader when consensus is enabled
                //state == &ConsensusState::Enabled
                //};
                //if should_set {
                //let is_leader = IS_LEADER.load(Ordering::SeqCst);
                //if is_leader != acquired {
                //warn!(log, "leader state change: {} -> {}", is_leader, acquired);
                //}
                //IS_LEADER.store(acquired, Ordering::SeqCst);
                //}
                Ok(())
            })
        });
        Box::new(acquire)
    }
}
