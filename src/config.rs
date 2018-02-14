use std::net::SocketAddr;
use std::fs::File;
use std::io::Read;
use peer::PeerCommand;
use toml;
use clap::{Arg, SubCommand};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub(crate) struct System {
    /// Logging level
    pub verbosity: String,

    /// Network settings
    pub network: Network,

    /// Consul settings
    pub consul: Consul,

    /// Metric settings
    pub metrics: Metrics,

    /// Number of networking threads, use 0 for number of CPUs
    pub n_threads: usize,

    /// Number of aggregating(worker) threads, set to 0 to use all CPU cores
    pub w_threads: usize,

    /// How often to gather own stats, in ms. Use 0 to disable (stats are still gathered, but not included in
    /// metric dump)
    pub stats_interval: u64,

    /// queue size for single counting thread before packet is dropped
    pub task_queue_size: usize,

    /// Prefix to send own metrics with
    pub stats_prefix: String,
}


impl Default for System {
    fn default() -> Self {
        Self {
            verbosity: "warn".to_string(),
            network: Network::default(),
            consul: Consul::default(),
            metrics: Metrics::default(),
            n_threads: 4,
            w_threads: 4,
            stats_interval: 10000,
            task_queue_size: 2048,
            stats_prefix: "resources.monitoring.bioyino".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub(crate) struct Metrics {
    // TODO: Maximum metric array size, 0 for unlimited
    //  max_metrics: usize,

    /// Should we provide metrics with top update numbers
    pub count_updates: bool,

    /// Prefix for metric update statistics
    pub update_counter_prefix: String,

    /// Suffix for metric update statistics
    pub update_counter_suffix: String,

    /// Minimal update count to be reported
    pub update_counter_threshold: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            //           max_metrics: 0,
            count_updates: true,
            update_counter_prefix: "resources.monitoring.bioyino.updates".to_string(),
            update_counter_suffix: String::new(),
            update_counter_threshold: 200,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub(crate) struct Network {
    /// Address and UDP port to listen for statsd metrics on
    pub listen: SocketAddr,

    /// Address and port for replication/command server to listen on
    pub peer_listen: SocketAddr,

    /// IP and port of the carbon-protocol backend to send aggregated data to
    pub backend: String,

    /// How often send metrics to carbon backend, ms
    pub backend_interval: u64,

    /// UDP buffer size for single packet. Needs to be around MTU. Packet's bytes after that value
    /// may be lost
    pub bufsize: usize,

    /// Enable multimessage(recvmmsg) mode
    pub multimessage: bool,

    /// Number of multimessage packets to receive at once if in multimessage mode
    pub mm_packets: usize,

    /// Nmber of green threads for single-message mode
    pub greens: usize,

    /// Socket pool size for single-message mode
    pub snum: usize,

    /// List of nodes to replicate metrics to
    pub nodes: Vec<String>,

    /// Interval to send snapshots to nodes, ms
    pub snapshot_interval: usize,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8125".parse().unwrap(),
            peer_listen: "127.0.0.1:8136".parse().unwrap(),
            backend: "127.0.0.1:2003".to_string(),
            backend_interval: 30000,
            bufsize: 1500,
            multimessage: false,
            mm_packets: 100,
            greens: 4,
            snum: 4,
            nodes: Vec::new(),
            snapshot_interval: 1000,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub(crate) struct Consul {
    /// Start in disabled leader finding mode
    pub start_disabled: bool,

    /// Consul agent address
    pub agent: SocketAddr,

    /// TTL of consul session, ms (consul cannot set it to less than 10s)
    pub session_ttl: usize,

    /// How often to renew consul session, ms
    pub renew_time: usize,
}

impl Default for Consul {
    fn default() -> Self {
        Self {
            start_disabled: false,
            agent: "127.0.0.1:8500".parse().unwrap(),
            session_ttl: 11000,
            renew_time: 1000,
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Daemon,
    Query(PeerCommand, String),
}

impl System {
    pub fn load() -> (Self, Command) {
        // This is a first copy of args - with the "config" option
        let app = app_from_crate!()
            .arg(
                Arg::with_name("config")
                .help("configuration file path")
                .long("config")
                .short("c")
                .required(true)
                .takes_value(true)
                .default_value("/etc/bioyino/bioyino.toml"),
                )
            .arg(
                Arg::with_name("verbosity")
                .short("v")
                .help("logging level")
                .takes_value(true)
                .default_value("warn"),
                )
            .subcommand(
                SubCommand::with_name("query")
                .about("send a request to running peer server")
                .arg(Arg::with_name("peer_command").index(1))
                .arg(Arg::with_name("server").default_value("127.0.0.1:8136")),
                )
            .get_matches();

        let config = value_t!(app.value_of("config"), String).expect("config file must be string");
        let mut file = File::open(&config).expect(&format!("opening config file at {}", &config));
        let mut config_str = String::new();
        file.read_to_string(&mut config_str)
            .expect("reading config file");
        let system: System = toml::de::from_str(&config_str).expect("parsing config");
        if let Some(matches) = app.subcommand_matches("query") {
            let cmd =
                value_t!(matches.value_of("peer_command"), PeerCommand).expect("bad peer command");
            let server = value_t!(matches.value_of("server"), String).expect("bad server");
            (system, Command::Query(cmd, server))
        } else {
            (system, Command::Daemon)
        }
    }
}
