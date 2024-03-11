// General
pub mod aggregate;
pub mod carbon;
pub mod config;
pub mod errors;
pub mod management;
pub mod peer;
pub mod raft;
pub mod async_udp;
pub mod sync_udp;
pub mod stats;
pub mod fast_task;
pub mod slow_task;
pub mod util;
pub mod cache;

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{panic, process, thread};

use slog::{info, o, debug};

use futures::future::{pending, TryFutureExt};

use slog::{error, warn};

use tokio::runtime::Builder;
use tokio::time::interval_at;

use tokio1::runtime::current_thread::Runtime;
use tokio1::timer::Delay;

use bioyino_metric::metric::Metric;
use bioyino_metric::name::MetricName;
use once_cell::sync::Lazy;

use crate::carbon::carbon_timer;
use crate::config::{Command, Network, System};
use crate::errors::GeneralError;
use crate::management::MgmtClient;
use crate::peer::{NativeProtocolServer, NativeProtocolSnapshot};
use crate::raft::start_internal_raft;
pub use crate::stats::OwnStats;
use crate::fast_task::start_fast_threads;
use crate::slow_task::start_slow_threads;
use crate::sync_udp::start_sync_udp;
use crate::async_udp::start_async_udp;
use crate::util::{retry_with_backoff, setup_logging, try_resolve, Backoff};
use crate::management::ConsensusState;
use crate::config::ConsensusKind;

// floating type used all over the code, can be changed to f32, to use less memory at the price of
// precision
#[cfg(feature = "f32")]
pub type Float = f32;

#[cfg(not(feature = "f32"))]
pub type Float = f64;

// a type to store pre-aggregated data
pub type Cache = HashMap<MetricName, Metric<Float>>;

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
                mgmt_listen,
                bufsize,
                multimessage,
                mm_packets,
                mm_async,
                mm_timeout,
                buffer_flush_time,
                ..
            },
            raft,
            metrics: _,
            aggregation,
            naming,
            carbon,
            n_threads,
            a_threads,
            stats_interval: s_interval,
            start_as_leader,
            stats_prefix,
            consensus,
            ..
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

    let runtime = Builder::new_multi_thread()
        .thread_name("bioyino_async")
        .worker_threads(a_threads)
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

    let elog = log.clone();
    let (fast_chans, fast_prio_chans) = start_fast_threads(log.clone(), config.clone())
        .map_err(move |e| error!(elog, "starting parsing worker threads"; "error" => format!("{}", e)))
        .expect("starting parsing worker thread");

    let elog = log.clone();
    let slow_chan = start_slow_threads(log.clone(), &config)
        .map_err(move |e| error!(elog, "starting counting worker threads"; "error" => format!("{}", e)))
        .expect("starting counting worker threads");

    let own_stat_log = log.clone();
    let stats_prefix = stats_prefix.trim_end_matches('.').to_string();
    // Spawn future gatering bioyino own stats
    info!(own_stat_log, "starting own stats counter");
    let own_stats = OwnStats::new(
        s_interval, stats_prefix, slow_chan.clone(), 
        fast_prio_chans.clone(), own_stat_log, carbon.clone(),
        aggregation.clone(), naming.clone(),
    );
    runtime.spawn(own_stats.run());

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
            use futures1::future::empty;
            use futures1::Future as Future1;
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

    let server_chan = slow_chan.clone();
    let server_log = log.clone();
    let peer_server = retry_with_backoff(peer_server_bo, move || {
        let server_log = server_log.clone();
        let peer_server = NativeProtocolServer::new(server_log.clone(), peer_listen, server_chan.clone());
        peer_server.run().inspect_err(move |e| {
            info!(server_log, "error running snapshot server"; "error"=>format!("{}", e));
        })
    });
    runtime.spawn(peer_server);

    info!(log, "starting snapshot sender");
    let snapshot = NativeProtocolSnapshot::new(
        &log,
        config.clone(),
        &fast_prio_chans,
        slow_chan.clone(),
    );

    runtime.spawn(snapshot.run().map_err(move |e| {
        s!(peer_errors);
        info!(snap_err_log, "error running snapshot sender"; "error"=>format!("{}", e));
    }));

    info!(log, "starting management server");
    let m_serv_log = rlog.clone();
    let m_server = async move { hyper::Server::bind(&mgmt_listen).serve(management::MgmtServer(m_serv_log, mgmt_listen)).await };

    runtime.spawn(m_server);

    info!(log, "starting carbon backend");
    let carbon_log = log.clone();
    let carbon_t = carbon_timer(log.clone(), carbon, aggregation, naming, slow_chan.clone())
        .map_err(move |e| error!(carbon_log, "running carbon thread"; "error" => format!("{}", e)));
    runtime.spawn(carbon_t);


    if multimessage {
        let flush_flags = start_sync_udp(
            log,
            listen,
            &fast_chans,
            config.clone(),
            n_threads,
            bufsize,
            mm_packets,
            mm_async,
            mm_timeout,
        );
        // spawn a flushing timer if required
        if buffer_flush_time > 0 {
            let flush_timer = async move {
                let dur = Duration::from_millis(buffer_flush_time);
                let mut timer = interval_at(tokio::time::Instant::now() + dur, dur);

                loop {
                    timer.tick().await;
                    debug!(rlog, "buffer flush requested");
                    flush_flags.iter().map(|flag| flag.swap(true, Ordering::SeqCst)).last();
                }
            };
            runtime.spawn(flush_timer);
        }
    } else {
        info!(log, "multimessage is disabled, starting in async UDP mode");
        let flush_sender = start_async_udp(log, &fast_chans, config.clone());

        if buffer_flush_time > 0 {
            let flush_timer = async move {
                let dur = Duration::from_millis(buffer_flush_time);
                let mut timer = interval_at(tokio::time::Instant::now() + dur, dur);

                loop {
                    timer.tick().await;
                    debug!(rlog, "buffer flush requested");
                    flush_sender.notify_waiters();
                }
            };
            runtime.spawn(flush_timer);
        }
    };

    runtime.block_on(pending::<()>());
}
