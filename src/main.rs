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
pub mod stats;
pub mod task;
pub mod udp;
pub mod util;

use std::collections::HashMap;
use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{panic, process, thread};

use slog::{info, o};

use futures::future::empty;
use futures::{Future as Future1, IntoFuture};
use futures3::channel::mpsc;
use futures3::stream::StreamExt;

use futures3::future::{pending, TryFutureExt};

use serde_derive::{Deserialize, Serialize};
use slog::{error, warn};

use tokio2::runtime::Builder;
use tokio2::time::interval_at;

use tokio::runtime::current_thread::Runtime;
use tokio::timer::Delay;

use bioyino_metric::metric::Metric;
use bioyino_metric::name::MetricName;
use once_cell::sync::Lazy;

use crate::carbon::carbon_timer;
use crate::config::{Command, Consul, Network, System};
use crate::consul::ConsulConsensus;
use crate::errors::GeneralError;
use crate::management::MgmtClient;
use crate::peer::{NativeProtocolServer, NativeProtocolSnapshot};
use crate::raft::start_internal_raft;
pub use crate::stats::OwnStats;
use crate::task::TaskRunner;
use crate::udp::{start_async_udp, start_sync_udp};
use crate::util::{retry_with_backoff, setup_logging, try_resolve, Backoff};

// floating type used all over the code, can be changed to f32, to use less memory at the price of
// precision
// TODO: make in into compilation feature
pub type Float = f64;

// a type to store pre-aggregated data
pub type Cache = HashMap<MetricName, Metric<Float>>;

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

pub static CONSENSUS_STATE: Lazy<Mutex<ConsensusState>> = Lazy::new(|| Mutex::new(ConsensusState::Disabled));
pub static IS_LEADER: AtomicBool = AtomicBool::new(false);

fn main() {
    let (system, command) = System::load().expect("loading config");

    let config = system.clone();
    #[allow(clippy::unneeded_field_pattern)]
    let System {
        verbosity_console,
        verbosity_syslog,
        daemon,
        network:
            Network {
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
                max_snapshots,
            },
        raft,
        consul:
            Consul {
                start_as: consul_start_as,
                agent,
                session_ttl: consul_session_ttl,
                renew_time: consul_renew_time,
                key_name: consul_key,
            },
        metrics: _,
        aggregation,
        naming,
        carbon,
        n_threads,
        w_threads,
        c_threads,
        stats_interval: s_interval,
        task_queue_size,
        start_as_leader,
        stats_prefix,
        consensus,
    } = system;

    if daemon && verbosity_syslog.is_off() {
        eprintln!("syslog is disabled, while daemon mode is on, no logging will be performed");
    }

    // Since daemonizing closes all opened resources, inclusing syslog, we mut do it before everything else
    if daemon {
        let result = unsafe { libc::daemon(0, 0) };
        if result < 0 {
            println!("daemonize failed: {}", io::Error::last_os_error());
            std::process::exit(1);
        }
    }

    let rlog = setup_logging(daemon, verbosity_console, verbosity_syslog);

    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    slog_stdlog::init().unwrap();

    let mut runtime = Builder::new()
        .thread_name("bioyino_main")
        .threaded_scheduler()
        .core_threads(c_threads)
        .enable_all()
        .build()
        .expect("creating runtime for main thread");

    if let Command::Query(command, dest) = command {
        let dest = try_resolve(&dest);
        let command = MgmtClient::new(rlog.clone(), dest, command);

        runtime.block_on(command.run()).unwrap_or_else(|e| {
            warn!(rlog,
                "error sending command";
                "dest"=>format!("{}",  &dest),
                "error"=> format!("{}", e),
            )
        });
        return;
    }

    let config = Arc::new(config);
    let log = rlog.new(o!("thread" => "main"));

    // To avoid strange side effects, like blocking the whole process in unknown state
    // we prefer to exit after any panic in any thread
    // So we set the panic hook to exit
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(42);
    }));

    // Start counting threads
    info!(log, "starting counting threads");
    let mut chans = Vec::with_capacity(w_threads);
    for i in 0..w_threads {
        let (tx, mut rx) = mpsc::channel(task_queue_size);
        chans.push(tx);
        let tlog = log.clone();
        let cf = config.clone();
        thread::Builder::new()
            .name(format!("bioyino_cnt{}", i))
            .spawn(move || {
                let mut runner = TaskRunner::new(tlog, cf, 8192);
                let mut runtime = Builder::new().basic_scheduler().enable_all().build().expect("creating runtime for test");

                let tasks = async {
                    while let Some(task) = rx.next().await {
                        runner.run(task);
                    }
                };

                runtime.block_on(tasks);
            })
            .map_err(|e| error!(log, "worker thread dead: {:?}", e))
            .expect("starting counting worker thread");
    }

    let own_stat_log = log.clone();
    let stats_prefix = stats_prefix.trim_end_matches('.').to_string();
    // Spawn future gatering bioyino own stats
    info!(own_stat_log, "starting own stats counter");
    let own_stats = OwnStats::new(s_interval, stats_prefix, chans.clone(), own_stat_log);
    runtime.spawn(async { own_stats.run().await });

    let compat_log = rlog.new(o!("thread" => "compat"));
    let snap_err_log = compat_log.clone();

    // TODO: unfortunately, the old entities are using timers and/or older tokio spawn function
    // therefore they are incompatible between old and new tokio runtimes, even using `compat`
    // adaptor trait
    //
    // this thread cen be eliminated after migrating sender and receiver to latest capnp and std
    // futures
    thread::Builder::new()
        .name("bioyino_compat".into())
        .spawn(move || {
            let mut runtime = Runtime::new().expect("creating runtime for counting worker");

            // Init leader state before starting backend
            IS_LEADER.store(start_as_leader, Ordering::SeqCst);

            let consensus_log = compat_log.clone();

            match consensus {
                ConsensusKind::Internal => {
                    let log = compat_log.clone();
                    let flog = compat_log.clone();
                    thread::Builder::new()
                        .name("bioyino_raft".into())
                        .spawn(move || {
                            let mut runtime = Runtime::new().expect("creating runtime for raft thread");
                            if start_as_leader {
                                warn!(
                                    log,
                                    "Starting as leader with enabled consensus. More that one leader is possible before consensus settle up."
                                );
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
                    warn!(
                        compat_log,
                        "CONSUL CONSENSUS IS DEPRECATED AND WILL BE REMOVED IN VERSION 0.8, CONSIDER USING BUILT IN RAFT"
                    );

                    if start_as_leader {
                        warn!(
                            compat_log,
                            "Starting as leader with enabled consensus. More that one leader is possible before consensus settle up."
                        );
                    }
                    {
                        let mut con_state = CONSENSUS_STATE.lock().unwrap();
                        info!(compat_log, "starting consul consensus"; "initial_state"=>format!("{:?}", con_state));
                        *con_state = consul_start_as;
                    }

                    let mut consensus = ConsulConsensus::new(&consensus_log, agent, consul_key);
                    consensus.set_session_ttl(Duration::from_millis(consul_session_ttl as u64));
                    consensus.set_renew_time(Duration::from_millis(consul_renew_time as u64));
                    runtime.spawn(consensus.into_future().map_err(|_| ()));
                }
                ConsensusKind::None => {
                    if !start_as_leader {
                        // starting as non-leader in this mode can be useful for agent mode
                        // so we don't disorient user with warnings
                        info!(
                            compat_log,
                            "Starting as non-leader with disabled consensus. No metrics will be sent until leader is switched on by command"
                        );
                    }
                }
            }

            runtime.block_on(empty::<(), ()>()).expect("compat thread failed");
        })
        .expect("starting compat thread");

    // settings safe for asap restart
    info!(log, "starting snapshot receiver");
    let peer_server_bo = Backoff {
        delay: 1,
        delay_mul: 1f32,
        delay_max: 1,
        retries: ::std::usize::MAX,
    };

    let server_chans = chans.clone();
    let server_log = log.clone();
    let peer_server = retry_with_backoff(peer_server_bo, move || {
        let server_log = server_log.clone();
        let peer_server = NativeProtocolServer::new(server_log.clone(), peer_listen, server_chans.clone());
        peer_server.run().inspect_err(move |e| {
            info!(server_log, "error running snapshot server"; "error"=>format!("{}", e));
        })
    });
    runtime.spawn(peer_server);

    info!(log, "starting snapshot sender");
    let snapshot = NativeProtocolSnapshot::new(
        &log,
        nodes,
        peer_client_bind,
        Duration::from_millis(snapshot_interval as u64),
        &chans,
        max_snapshots,
    );

    runtime.spawn(snapshot.run().map_err(move |e| {
        s!(peer_errors);
        info!(snap_err_log, "error running snapshot sender"; "error"=>format!("{}", e));
    }));

    info!(log, "starting management server");
    let m_serv_log = rlog.clone();
    let m_server = async move { hyper13::Server::bind(&mgmt_listen).serve(management::MgmtServer(m_serv_log, mgmt_listen)).await };

    runtime.spawn(m_server);

    info!(log, "starting carbon backend");
    let carbon_log = log.clone();
    let carbon_t = carbon_timer(log.clone(), carbon, aggregation, naming, chans.clone())
        .map_err(move |e| error!(carbon_log, "running carbon thread"; "error" => format!("{}", e)));
    runtime.spawn(carbon_t);

    // For each out sync thread we create the buffer flush timer, that sets the atomic value to 1
    // every interval
    let mut flush_flags = Arc::new(Vec::new());
    if let Some(flags) = Arc::get_mut(&mut flush_flags) {
        for _ in 0..n_threads {
            flags.push(AtomicBool::new(false));
        }
    }

    if buffer_flush_time > 0 {
        let flags = flush_flags.clone();
        let flush_timer = async move {
            let dur = Duration::from_millis(buffer_flush_time);
            let mut timer = interval_at(tokio2::time::Instant::now() + dur, dur);

            loop {
                timer.tick().await;
                info!(rlog, "buffer flush requested");
                flags.iter().map(|flag| flag.swap(true, Ordering::SeqCst)).last();
            }
        };
        runtime.spawn(flush_timer);
    }

    if multimessage {
        start_sync_udp(
            log,
            listen,
            &chans,
            config.clone(),
            n_threads,
            bufsize,
            mm_packets,
            mm_async,
            mm_timeout,
            flush_flags.clone(),
        );
    } else {
        start_async_udp(
            log,
            listen,
            &chans,
            config.clone(),
            n_threads,
            greens,
            async_sockets,
            bufsize,
            flush_flags.clone(),
        );
    }

    runtime.block_on(pending::<()>());
}
