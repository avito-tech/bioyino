// General
//pub mod bigint;
pub mod aggregate;
pub mod carbon;
pub mod config;
pub mod consul;
pub mod errors;
pub mod management;
pub mod peer;
pub mod raft;
pub mod server;
pub mod task;
pub mod udp;
pub mod util;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{self, Duration, Instant, SystemTime};

use slog::{error, info, o, Drain, Level};

use bytes::Bytes;
use futures::future::{empty, ok};
use futures::sync::mpsc;
use futures::{Future, IntoFuture, Stream};
use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
use slog::warn;

use tokio::runtime::current_thread::{spawn, Runtime};
use tokio::timer::{Delay, Interval};

use crate::udp::{start_async_udp, start_sync_udp};
use metric::metric::Metric;

use crate::aggregate::{AggregateOptions, AggregationMode, Aggregator};
use crate::carbon::{CarbonBackend, CarbonClientOptions};
use crate::config::{Command, Consul, Metrics, Network, System};
use crate::consul::ConsulConsensus;
use crate::errors::GeneralError;
use crate::management::{MgmtClient, MgmtServer};
use crate::peer::{NativeProtocolServer, NativeProtocolSnapshot};
use crate::raft::start_internal_raft;
use crate::task::{Task, TaskRunner};
use crate::util::{try_resolve, BackoffRetryBuilder, OwnStats, UpdateCounterOptions};

// floating type used all over the code, can be changed to f32, to use less memory at the price of
// precision
// TODO: make in into compilation feature
pub type Float = f64;

// a type to store pre-aggregated data
pub type Cache = HashMap<Bytes, Metric<Float>>;

// statistic counters
pub static PARSE_ERRORS: AtomicUsize = AtomicUsize::new(0);
pub static AGG_ERRORS: AtomicUsize = AtomicUsize::new(0);
pub static PEER_ERRORS: AtomicUsize = AtomicUsize::new(0);
pub static INGRESS: AtomicUsize = AtomicUsize::new(0);
pub static INGRESS_METRICS: AtomicUsize = AtomicUsize::new(0);
pub static EGRESS: AtomicUsize = AtomicUsize::new(0);
pub static DROPS: AtomicUsize = AtomicUsize::new(0);

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ConsensusKind {
    None,
    Consul,
    Internal,
}

lazy_static! {
    pub static ref CONSENSUS_STATE: Mutex<ConsensusState> = { Mutex::new(ConsensusState::Disabled) };
}

pub static IS_LEADER: AtomicBool = AtomicBool::new(false);

fn main() {
    let (system, command) = System::load();

    let config = system.clone();
    let System {
        verbosity,
        network: Network {
            listen,
            peer_listen,
            peer_client_bind,
            mgmt_listen,
            bufsize,
            multimessage,
            mm_packets,
            mm_async,
            mm_timeout,
            buffer_flush_time,
            buffer_flush_length: _,
            greens,
            async_sockets,
            nodes,
            snapshot_interval,
        },
        raft,
        consul: Consul { start_as: consul_start_as, agent, session_ttl: consul_session_ttl, renew_time: consul_renew_time, key_name: consul_key },
        metrics: Metrics {
            //           max_metrics,
            mut count_updates,
            update_counter_prefix,
            update_counter_suffix,
            update_counter_threshold,
            aggregation_mode,
            aggregation_threads,
            consistent_parsing: _,
            log_parse_errors: _,
            max_unparsed_buffer: _,
        },
        carbon,
        n_threads,
        w_threads,
        stats_interval: s_interval,
        task_queue_size,
        start_as_leader,
        stats_prefix,
        consensus,
    } = system;

    let verbosity = Level::from_str(&verbosity).expect("bad verbosity");

    let mut runtime = Runtime::new().expect("creating runtime for main thread");

    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, verbosity).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"bioyino"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    if let Command::Query(command, dest) = command {
        let dest = try_resolve(&dest);
        let command = MgmtClient::new(rlog.clone(), dest.clone(), command);

        runtime.block_on(command.into_future()).unwrap_or_else(|e| {
            warn!(rlog,
            "error sending command";
            "dest"=>format!("{}",  &dest),
            "error"=> format!("{}", e),
            )
        });
        return;
    }

    if count_updates && update_counter_prefix.len() == 0 && update_counter_suffix.len() == 0 {
        warn!(rlog, "update counting suffix and prefix are empty, update counting disabled to avoid metric rewriting");
        count_updates = false;
    }

    let update_counter_prefix: Bytes = update_counter_prefix.into();
    let update_counter_suffix: Bytes = update_counter_suffix.into();

    let config = Arc::new(config);
    let log = rlog.new(o!("thread" => "main"));

    // Init task options before initializing task threads

    // Start counting threads
    info!(log, "starting counting threads");
    let mut chans = Vec::with_capacity(w_threads);
    for i in 0..w_threads {
        let (tx, rx) = mpsc::channel(task_queue_size);
        chans.push(tx);
        let tlog = log.clone();
        let cf = config.clone();
        thread::Builder::new()
            .name(format!("bioyino_cnt{}", i).into())
            .spawn(move || {
                let runner = TaskRunner::new(tlog, cf, 8192);
                let mut runtime = Runtime::new().expect("creating runtime for counting worker");
                let future = rx
                    .fold(runner, move |mut runner, task: Task| {
                        runner.run(task);
                        Ok(runner)
                    })
                    .map(|_| ())
                    .map_err(|_| ());
                //        let future = rx.for_each(|task: Task| ok(runner.run(task)));
                runtime.block_on(future).expect("worker thread failed");
            })
            .expect("starting counting worker thread");
    }

    let stats_prefix = stats_prefix.trim_end_matches(".").to_string();

    // Spawn future gatering bioyino own stats
    let own_stat_chan = chans[0].clone();
    let own_stat_log = rlog.clone();
    info!(log, "starting own stats counter");
    let own_stats = OwnStats::new(s_interval, stats_prefix, own_stat_chan, own_stat_log);
    runtime.spawn(own_stats);

    info!(log, "starting snapshot sender");
    let snap_log = rlog.clone();
    let snap_err_log = rlog.clone();

    let snapshot = NativeProtocolSnapshot::new(&snap_log, nodes, peer_client_bind, Duration::from_millis(snapshot_interval as u64), &chans).into_future().map_err(move |e| {
        PEER_ERRORS.fetch_add(1, Ordering::Relaxed);
        info!(snap_err_log, "error sending snapshot";"error"=>format!("{}", e));
    });
    runtime.spawn(snapshot);

    // settings safe for asap restart
    info!(log, "starting snapshot receiver");
    let peer_server_ret = BackoffRetryBuilder { delay: 1, delay_mul: 1f32, delay_max: 1, retries: ::std::usize::MAX };

    let peer_server = NativeProtocolServer::new(rlog.clone(), peer_listen, chans.clone());
    let peer_server = peer_server_ret
        .spawn(peer_server)
        // with unlimited number of retries, BackoffRetry will never return any error
        // server logs all erros inside itself
        .map_err(|_| ());

    runtime.spawn(peer_server);

    // Init leader state before starting backend
    IS_LEADER.store(start_as_leader, Ordering::SeqCst);

    let consensus_log = rlog.clone();

    match consensus {
        ConsensusKind::Internal => {
            let log = log.clone();
            let flog = log.clone();
            thread::Builder::new()
                .name("bioyino_raft".into())
                .spawn(move || {
                    let mut runtime = Runtime::new().expect("creating runtime for raft thread");
                    if start_as_leader {
                        warn!(log, "Starting as leader with enabled consensus. More that one leader is possible before consensus settle up.");
                    }
                    let d = Delay::new(Instant::now() + Duration::from_millis(raft.start_delay));
                    let log = log.clone();
                    let delayed = d.map_err(|_| ()).and_then(move |_| {
                        let mut con_state = CONSENSUS_STATE.lock().unwrap();
                        *con_state = ConsensusState::Enabled;
                        info!(log, "starting internal consensus"; "initial_state"=>format!("{:?}", *con_state));
                        start_internal_raft(raft, consensus_log);
                        Ok(())
                    });

                    runtime.spawn(delayed);
                    runtime.block_on(empty::<(), ()>()).expect("raft thread failed");

                    info!(flog, "consensus thread stopped");
                })
                .expect("starting counting worker thread");
        }
        ConsensusKind::Consul => {
            if start_as_leader {
                warn!(log, "Starting as leader with enabled consensus. More that one leader is possible before consensus settle up.");
            }
            {
                let mut con_state = CONSENSUS_STATE.lock().unwrap();
                info!(log, "starting consul consensus"; "initial_state"=>format!("{:?}", con_state));
                *con_state = consul_start_as;
            }

            let mut consensus = ConsulConsensus::new(&consensus_log, agent, consul_key);
            consensus.set_session_ttl(Duration::from_millis(consul_session_ttl as u64));
            consensus.set_renew_time(Duration::from_millis(consul_renew_time as u64));
            runtime.spawn(consensus.into_future().map_err(|_| ())); // TODO errors
        }
        ConsensusKind::None => {
            if !start_as_leader {
                // starting as non-leader in this mode can be useful for agent mode
                // so we don't disorient user with warnings
                info!(log, "Starting as non-leader with disabled consensus. No metrics will be sent until leader is switched on by command");
            }
        }
    }

    info!(log, "starting management server");
    let m_serv_log = rlog.clone();
    let m_serv_err_log = rlog.clone();
    let m_server = hyper::Server::bind(&mgmt_listen).serve(move || ok::<_, hyper::Error>(MgmtServer::new(m_serv_log.clone(), &mgmt_listen))).map_err(move |e| {
        warn!(m_serv_err_log, "management server gone with error: {:?}", e);
    });

    runtime.spawn(m_server);

    info!(log, "starting carbon backend");
    let tchans = chans.clone();
    let carbon_log = rlog.clone();

    let dur = Duration::from_millis(carbon.interval);
    let carbon_timer = Interval::new(Instant::now() + dur, dur);
    let mut carbon_config = carbon.clone();
    if carbon_config.chunks == 0 {
        carbon_config.chunks = 1
    }
    let multi_threads = match aggregation_threads {
        Some(value) if aggregation_mode == AggregationMode::Separate => value,
        Some(_) => {
            info!(carbon_log, "aggregation_threads parameter only works in \"separate\" mode and will be ignored");
            0
        }
        None if aggregation_mode == AggregationMode::Separate => 0,
        _ => 0,
    };

    let carbon_timer = carbon_timer.map_err(|e| GeneralError::Timer(e)).for_each(move |_tick| {
        let ts = SystemTime::now().duration_since(time::UNIX_EPOCH).map_err(|e| GeneralError::Time(e))?;

        let backend_addr = try_resolve(&carbon.address);
        let tchans = tchans.clone();
        let carbon_log = carbon_log.clone();

        let update_counter_prefix = update_counter_prefix.clone();
        let update_counter_suffix = update_counter_suffix.clone();
        let backend_opts = carbon_config.clone();
        let aggregation_mode = aggregation_mode.clone();
        thread::Builder::new()
            .name("bioyino_carbon".into())
            .spawn(move || {
                let carbon_log = carbon_log.clone();
                let runtime_log = carbon_log.clone();

                let mut runtime = match Runtime::new() {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        error!(carbon_log, "creating runtime for backend"; "error"=>e.to_string());
                        return;
                    }
                };

                let is_leader = IS_LEADER.load(Ordering::SeqCst);

                let options = AggregateOptions {
                    is_leader,
                    update_counter: if count_updates { Some(UpdateCounterOptions { threshold: update_counter_threshold, prefix: update_counter_prefix, suffix: update_counter_suffix }) } else { None },
                    aggregation_mode,
                    multi_threads,
                };

                if is_leader {
                    info!(carbon_log, "leader sending metrics");
                    let (backend_tx, backend_rx) = mpsc::unbounded();
                    let aggregator = Aggregator::new(options, tchans, backend_tx, carbon_log.clone()).into_future();

                    runtime.spawn(aggregator);

                    let handle = runtime.handle();
                    let carbon_sender = backend_rx
                        .inspect(|_| {
                            EGRESS.fetch_add(1, Ordering::Relaxed);
                        })
                        .collect()
                        .map(move |metrics| {
                            let carbon_log = carbon_log.clone();
                            let carbon = backend_opts.clone();
                            let chunk_size = metrics.len() / carbon.chunks;
                            // TODO we could do this without allocations
                            // but in rust it's not so easy with these types
                            // probably Pin API would help
                            // probably changing to Arc<[Metric]> would
                            metrics
                                .chunks(chunk_size)
                                .map(move |metrics| {
                                    let options = CarbonClientOptions { addr: backend_addr, bind: backend_opts.bind_address };
                                    let backend = CarbonBackend::new(options, ts, Arc::new(metrics.to_vec()), carbon_log.clone());
                                    let retrier = BackoffRetryBuilder { delay: backend_opts.connect_delay, delay_mul: backend_opts.connect_delay_multiplier, delay_max: backend_opts.connect_delay_max, retries: backend_opts.send_retries };
                                    let carbon_log = carbon_log.clone();
                                    let retrier = retrier.spawn(backend).map_err(move |e| {
                                        error!(carbon_log.clone(), "Failed to send to graphite"; "error"=>format!("{:?}",e));
                                    });
                                    spawn(retrier);
                                })
                                .last();
                        });

                    handle.spawn(carbon_sender).unwrap_or_else(|e| {
                        error!(runtime_log, "spawning sender"; "error"=>format!("{:?}", e));
                    });           ;
                    runtime.run().unwrap_or_else(|e| {
                        error!(runtime_log, "Failed to send to graphite"; "error"=>format!("{:?}", e));
                    });
                // runtime.block_on(backend).unwrap_or_else(|e| {
                //error!(carbon_log, "Failed to send to graphite"; "error"=>e);
                // });
                } else {
                    info!(carbon_log, "not leader, removing metrics");
                    let (backend_tx, _) = mpsc::unbounded();
                    let aggregator = Aggregator::new(options, tchans, backend_tx, carbon_log.clone()).into_future();
                    runtime.block_on(aggregator.then(|_| Ok::<(), ()>(()))).unwrap_or_else(|e| error!(carbon_log, "Failed to join aggregated metrics"; "error"=>e));
                }
            })
            .expect("starting thread for sending to graphite");
        Ok(())
    });

    let tlog = rlog.clone();
    runtime.spawn(carbon_timer.map_err(move |e| {
        warn!(tlog, "error running carbon"; "error"=>e.to_string());
    }));

    // For each thread we create
    let mut flush_flags = Arc::new(Vec::new());
    if let Some(flags) = Arc::get_mut(&mut flush_flags) {
        for _ in 0..n_threads {
            flags.push(AtomicBool::new(false));
        }
    }

    if buffer_flush_time > 0 {
        let dur = Duration::from_millis(buffer_flush_time);
        let flush_timer = Interval::new(Instant::now() + dur, dur);

        let tlog = rlog.clone();
        let flags = flush_flags.clone();
        let flush_timer = flush_timer.map_err(|e| GeneralError::Timer(e)).for_each(move |_tick| {
            info!(tlog, "buffer flush requested");
            flags.iter().map(|flag| flag.swap(true, Ordering::SeqCst)).last();
            Ok(())
        });
        let tlog = rlog.clone();
        runtime.spawn(flush_timer.map_err(move |e| {
            warn!(tlog, "error running buffer flush timer"; "error"=>e.to_string());
        }));
    }

    if multimessage {
        start_sync_udp(log, listen, &chans, config.clone(), n_threads, bufsize, mm_packets, mm_async, mm_timeout, flush_flags.clone());
    } else {
        start_async_udp(log, listen, &chans, config.clone(), n_threads, greens, async_sockets, bufsize, flush_flags.clone());
    }

    runtime.block_on(empty::<(), ()>()).expect("running runtime in main thread");
}
