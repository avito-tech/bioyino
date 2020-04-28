use std::{collections::HashMap, fmt::Debug, fs::File, io::Read, net::SocketAddr, ops::Range, time::Duration};

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version, value_t, Arg, SubCommand};
use toml;

use serde_derive::{Deserialize, Serialize};

use bytes::Bytes;
use raft_tokio::RaftOptions;
use thiserror::Error;

use crate::aggregate::AggregationMode;
use crate::management::{ConsensusAction, LeaderAction, MgmtCommand};
use crate::{ConsensusKind, ConsensusState, Float};
use bioyino_metric::{
    aggregate::Aggregate,
    metric::MetricTypeName,
    name::{AggregationDestination, NamingOptions},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct System {
    /// Logging level
    pub verbosity: String,

    /// Enable logging to syslog
    pub syslog: bool,

    /// Network settings
    pub network: Network,

    /// Internal Raft settings
    pub raft: Raft,

    /// Consul settings
    pub consul: Consul,

    /// Metric settings
    pub metrics: Metrics,

    /// Aggregation settings
    pub aggregation: Aggregation,

    /// Metric naming overrides by metric type
    pub naming: HashMap<MetricTypeName, Naming>,

    /// Carbon backend settings
    pub carbon: Carbon,

    /// Number of networking threads, use 0 for number of CPUs
    pub n_threads: usize,

    /// Number of aggregating(worker) threads, set to 0 to use all CPU cores
    pub w_threads: usize,

    /// queue size for single counting thread before packet is dropped
    pub task_queue_size: usize,

    /// Should we start as leader state enabled or not
    pub start_as_leader: bool,

    /// How often to gather own stats, in ms. Use 0 to disable (stats are still gathered, but not included in
    /// metric dump)
    pub stats_interval: u64,

    /// Prefix to send own metrics with
    pub stats_prefix: String,

    /// Consensus kind to use
    pub consensus: ConsensusKind,
}

impl Default for System {
    fn default() -> Self {
        Self {
            verbosity: "warn".to_string(),
            syslog: false,
            network: Network::default(),
            raft: Raft::default(),
            consul: Consul::default(),
            metrics: Metrics::default(),
            aggregation: Aggregation::default(),
            naming: default_namings(),
            carbon: Carbon::default(),
            n_threads: 4,
            w_threads: 4,
            stats_interval: 10000,
            task_queue_size: 2048,
            start_as_leader: false,
            stats_prefix: "resources.monitoring.bioyino".to_string(),
            consensus: ConsensusKind::None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Metrics {
    /// Consistent parsing
    pub consistent_parsing: bool,

    /// Whether we should spam parsing errors in logs
    pub log_parse_errors: bool,

    /// Maximum length of data parser can keep in buffer befor considering it trash and throwing
    /// away
    pub max_unparsed_buffer: usize,

    /// Maximum length of tags part of a metric
    pub max_tags_len: usize,

    /// An option to create a copy of tagged metric without tags
    pub create_untagged_copy: bool,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            consistent_parsing: true,
            log_parse_errors: false,
            max_unparsed_buffer: 10000,
            max_tags_len: 9000,
            create_untagged_copy: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Aggregation {
    /// Timestamp rounding
    pub round_timestamp: RoundTimestamp,

    /// Choose the way of aggregation
    pub mode: AggregationMode,

    /// Number of threads when aggregating in "multi" mode
    pub threads: Option<usize>,

    /// Minimal update count to be reported
    pub update_count_threshold: u32,

    /// list of aggregates to count for metrics by type
    pub aggregates: HashMap<MetricTypeName, Option<Vec<Aggregate<Float>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum RoundTimestamp {
    /// round timestamp to closest interval down
    Down,
    /// do not round the timestamp
    No,
    /// round timestamp to closest interval up
    Up,
}

impl Default for Aggregation {
    fn default() -> Self {
        Self {
            round_timestamp: RoundTimestamp::No,
            mode: AggregationMode::Single,
            threads: None,
            update_count_threshold: 200,
            aggregates: all_aggregates().into_iter().map(|(k, v)| (k, Some(v))).collect(),
        }
    }
}

pub(crate) fn all_aggregates() -> HashMap<MetricTypeName, Vec<Aggregate<Float>>> {
    let mut map = bioyino_metric::aggregate::possible_aggregates();
    let timers = map.get_mut(&MetricTypeName::Timer).expect("only BUG in possible_aggregates can panic here");
    timers.push(Aggregate::Percentile(0.75, 75));
    timers.push(Aggregate::Percentile(0.95, 95));
    timers.push(Aggregate::Percentile(0.98, 98));
    //timers.push(Aggregate::Percentile(0.99)); // 99th already exists
    timers.push(Aggregate::Percentile(0.999, 999));
    map
}

pub fn default_namings() -> HashMap<MetricTypeName, Naming> {
    let mut m = HashMap::new();
    for (ty, aggs) in all_aggregates() {
        // for each type there will be own naming defaults
        let mut tag_values = HashMap::new();
        let mut postfixes = HashMap::new();
        for agg in &aggs {
            tag_values.insert(agg.clone(), agg.to_string());
            postfixes.insert(agg.clone(), agg.to_string());
        }
        let naming = Naming {
            destination: AggregationDestination::Smart,
            prefix: String::new(),
            prefix_overrides: None,
            tag: "aggregate".to_string(),
            tag_values,
            postfixes,
        };
        m.insert(ty, naming);
    }
    m
}

/// there is a difference between prefixes and postfixes/tags due to their semantics
/// prefix is usually and most probably global, so people often want one
/// postfix and tags are dynamic in all cases, therefore some global postfix or tag
/// only exist when aggregates are put each in it's own namespace which is very rare
/// due to being inconvenient
/// WARNING: Since the structure's default is metric type dependent, The Default impl does not
/// show the reality and exists only due to serde::Deserialize requiring it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Naming {
    /// Where to put aggregate postfix
    pub destination: AggregationDestination,

    /// global default prefix
    pub prefix: String,

    /// prefix options per aggregate
    pub prefix_overrides: Option<HashMap<Aggregate<Float>, String>>,

    /// names for aggregate postfixes
    pub postfixes: HashMap<Aggregate<Float>, String>,

    /// the default tag name(i.e. key) to be used for aggregation
    pub tag: String,

    /// replacements for aggregate tag values, naming is <tag>=<tag_value>
    pub tag_values: HashMap<Aggregate<Float>, String>,
}

impl Default for Naming {
    fn default() -> Self {
        Self {
            destination: AggregationDestination::Smart,
            prefix: String::new(),
            prefix_overrides: None,
            tag: "aggregate".to_string(),
            tag_values: HashMap::new(),
            postfixes: HashMap::new(),
        }
    }
}

impl Naming {
    pub(crate) fn as_options(&self, agg: &Aggregate<Float>) -> NamingOptions {
        let prefix = if let Some(ref overrides) = self.prefix_overrides {
            if let Some(ov) = overrides.get(agg) {
                Bytes::copy_from_slice(ov.as_bytes())
            } else {
                Bytes::copy_from_slice(self.prefix.as_bytes())
            }
        } else {
            Bytes::copy_from_slice(self.prefix.as_bytes())
        };

        let tag_value = Bytes::copy_from_slice(self.tag_values.get(agg).unwrap_or(&agg.to_string()).as_bytes());
        let postfix = Bytes::copy_from_slice(self.postfixes.get(agg).unwrap_or(&agg.to_string()).as_bytes());
        NamingOptions {
            prefix,
            tag: Bytes::copy_from_slice(self.tag.as_bytes()),
            tag_value,
            postfix,
            destination: self.destination.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Carbon {
    // TODO: will be used when multiple backends support is implemented
    ///// Enable sending to carbon protocol backend
    //pub enabled: bool,
    /// IP and port of the carbon-protocol backend to send aggregated data to
    pub address: String,

    /// client bind address
    pub bind_address: Option<SocketAddr>,

    /// How often to send metrics to this backend, ms
    pub interval: u64,

    /// How much to sleep when connection to backend fails, ms
    pub connect_delay: u64,

    /// Multiply delay to this value for each consequent connection failure
    pub connect_delay_multiplier: f32,

    /// Maximum retry delay, ms
    pub connect_delay_max: u64,

    /// How much times to retry when sending data to backend before giving up and dropping all metrics
    /// note, that 0 means 1 try
    pub send_retries: usize,

    /// The whole metrica array can be split into smaller chunks for each chunk to be sent
    /// in a separate connection. This is a workaround for go-carbon and carbon-c-relay doing
    /// per-connection processing and working ineffectively when lots of metrics is sent in one
    /// connection
    pub chunks: usize,
}

impl Default for Carbon {
    fn default() -> Self {
        Self {
            //            enabled: true,
            address: "127.0.0.1:2003".to_string(),
            bind_address: None,
            interval: 30000,
            connect_delay: 250,
            connect_delay_multiplier: 2f32,
            connect_delay_max: 10000,
            send_retries: 30,
            chunks: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Network {
    /// Address and UDP port to listen for statsd metrics on
    pub listen: SocketAddr,

    /// Address and port for replication server to listen on
    pub peer_listen: SocketAddr,

    /// Snapshot client bind address
    pub peer_client_bind: Option<SocketAddr>,

    /// Address and port for management server to listen on
    pub mgmt_listen: SocketAddr,

    /// UDP buffer size for single packet. Needs to be around MTU. Packet's bytes after that value
    /// may be lost
    pub bufsize: usize,

    /// Enable multimessage(recvmmsg) mode
    pub multimessage: bool,

    /// Number of multimessage packets to receive at once if in multimessage mode
    pub mm_packets: usize,

    /// Number of multimessage packets to receive at once if in multimessage mode
    pub mm_async: bool,

    /// A timeout to return from multimessage mode syscall
    pub mm_timeout: u64,

    /// A timer to flush incoming buffer making sure metrics are not stuck there
    pub buffer_flush_time: u64,

    /// A length of incoming buffer to flush it making sure metrics are not stuck there
    pub buffer_flush_length: usize,

    /// Nmber of green threads for single-message mode
    pub greens: usize,

    /// Socket pool size for single-message mode
    pub async_sockets: usize,

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
            peer_client_bind: None,
            mgmt_listen: "127.0.0.1:8137".parse().unwrap(),
            bufsize: 1500,
            multimessage: false,
            mm_packets: 100,
            mm_async: false,
            mm_timeout: 0,
            buffer_flush_length: 0,
            buffer_flush_time: 0,
            greens: 4,
            async_sockets: 4,
            nodes: Vec::new(),
            snapshot_interval: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Consul {
    /// Start in disabled leader finding mode
    pub start_as: ConsensusState,

    /// Consul agent address
    pub agent: SocketAddr,

    /// TTL of consul session, ms (consul cannot set it to less than 10s)
    pub session_ttl: usize,

    /// How often to renew consul session, ms
    pub renew_time: usize,

    /// Name of ke to be locked in consul
    pub key_name: String,
}

impl Default for Consul {
    fn default() -> Self {
        Self {
            start_as: ConsensusState::Disabled,
            agent: "127.0.0.1:8500".parse().unwrap(),
            session_ttl: 11000,
            renew_time: 1000,
            key_name: "service/bioyino/lock".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Raft {
    /// Delay raft after start (ms)
    pub start_delay: u64,

    /// Raft heartbeat timeout (ms)
    pub heartbeat_timeout: u64,

    /// Raft heartbeat timeout (ms)
    pub election_timeout_min: u64,

    /// Raft heartbeat timeout (ms)
    pub election_timeout_max: u64,

    /// Name of this node. By default is taken by resolving hostname in DNS.
    pub this_node: Option<String>,

    /// List of Raft nodes, may include this_node
    pub nodes: HashMap<String, u64>,

    /// Bind raft client to specific IP when connecting nodes
    pub client_bind: Option<SocketAddr>,
}

impl Default for Raft {
    fn default() -> Self {
        Self {
            start_delay: 0,
            heartbeat_timeout: 250,
            election_timeout_min: 500,
            election_timeout_max: 750,
            this_node: None,
            nodes: HashMap::new(),
            client_bind: None,
        }
    }
}

impl Raft {
    pub fn get_raft_options(&self) -> RaftOptions {
        RaftOptions {
            heartbeat_timeout: Duration::from_millis(self.heartbeat_timeout),
            election_timeout: Range {
                start: Duration::from_millis(self.election_timeout_min),
                end: Duration::from_millis(self.election_timeout_max),
            },
        }
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("I/O error while {0}")]
    Io(String, #[source] ::std::io::Error),

    #[error("config file must be string")]
    MustBeString,

    #[error("parsing toml")]
    Toml(#[from] ::toml::de::Error),

    #[error("bad aggregate spec: '{}'", _0)]
    BadAggregate(String),

    #[error("bad value for '{}': '{}'", _0, _1)]
    BadValue(String, String),

    #[error("unknown type name '{}'", _0)]
    BadTypeName(String),
}

#[derive(Debug)]
pub enum Command {
    Daemon,
    Query(MgmtCommand, String),
}

impl System {
    pub fn load() -> Result<(Self, Command), ConfigError> {
        // This is a first copy of args - with the "config" option
        let app = app_from_crate!()
            .long_version(concat!(crate_version!(), " ", env!("VERGEN_COMMIT_DATE"), " ", env!("VERGEN_SHA_SHORT")))
            .arg(
                Arg::with_name("config")
                    .help("configuration file path")
                    .long("config")
                    .short("c")
                    .required(true)
                    .takes_value(true)
                    .default_value("/etc/bioyino/bioyino.toml"),
            )
            .arg(Arg::with_name("verbosity").short("v").help("logging level").takes_value(true))
            .subcommand(
                SubCommand::with_name("query")
                    .about("send a management command to running bioyino server")
                    .arg(Arg::with_name("host").short("h").default_value("127.0.0.1:8137"))
                    .subcommand(SubCommand::with_name("status").about("get server state"))
                    .subcommand(
                        SubCommand::with_name("consensus")
                            .arg(Arg::with_name("action").index(1))
                            .arg(Arg::with_name("leader_action").index(2).default_value("unchanged")),
                    ),
            )
            .get_matches();

        let config = value_t!(app.value_of("config"), String).map_err(|_| ConfigError::MustBeString)?;
        let mut file = File::open(&config).map_err(|e| ConfigError::Io(format!("opening config file at {}", &config).to_string(), e))?;
        let mut config_str = String::new();
        file.read_to_string(&mut config_str)
            .map_err(|e| ConfigError::Io("reading config file".into(), e))?;
        let mut system: System = toml::de::from_str(&config_str).map_err(ConfigError::Toml)?;

        if let Some(v) = app.value_of("verbosity") {
            system.verbosity = v.into()
        }

        // all parameter postprocessing goes here
        system.prepare()?;

        // now parse command
        if let Some(query) = app.subcommand_matches("query") {
            let server = value_t!(query.value_of("host"), String).expect("bad server");
            if let Some(_) = query.subcommand_matches("status") {
                Ok((system, Command::Query(MgmtCommand::Status, server)))
            } else if let Some(args) = query.subcommand_matches("consensus") {
                let c_action = value_t!(args.value_of("action"), ConsensusAction).expect("bad consensus action");
                let l_action = value_t!(args.value_of("leader_action"), LeaderAction).expect("bad leader action");
                Ok((system, Command::Query(MgmtCommand::ConsensusCommand(c_action, l_action), server)))
            } else {
                // shold be unreachable
                unreachable!("clap bug?")
            }
        } else {
            Ok((system, Command::Daemon))
        }
    }

    pub fn prepare(&mut self) -> Result<(), ConfigError> {
        // it is not OK to specify 0 chunks
        if self.carbon.chunks == 0 {
            return Err(ConfigError::BadValue(
                "system.carbon.chunks".into(),
                "number of chunks cannot be 0, use 1 to send without splitting".into(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::aggregate::AggregationOptions;
    use crate::util::prepare_log;

    #[test]
    fn parsing_full_config() {
        let config = "test/fixtures/full.toml";
        let mut file = File::open(config).expect(&format!("opening config file at {}", &config));
        let mut config_str = String::new();
        file.read_to_string(&mut config_str).expect("reading config file");
        //let mut system: System = toml::de::from_str(&config_str).expect("parsing config");
        let mut config: System = toml::de::from_str(&config_str).expect("parsing config");
        config.prepare().expect("preparing config");

        let log = prepare_log("parse_full_config test");
        let aopts = AggregationOptions::from_config(config.aggregation, config.naming, log).expect("checking aggregate options");
        for (ty, _) in all_aggregates() {
            // all_aggregates contain all types and all possible aggregate pairs
            // so we must check aggregation_options has all of them after conversion
            assert!(aopts.aggregates.get(&ty).is_some(), "{:?} <= {:?}", ty, aopts.aggregates);
            assert!(aopts.aggregates.get(&ty).unwrap().len() > 0, "{:?}", ty);
        }

        for (ty, aggs) in &aopts.aggregates {
            for agg in aggs.into_iter() {
                assert!(aopts.namings.get(&(ty.clone(), agg.clone())).is_some(), "no naming for {:?}, {:?}", ty, agg);
            }
        }
    }

    #[test]
    fn parsing_documented_config() {
        let config = "config.toml";
        let mut file = File::open(config).expect(&format!("opening config file at {}", &config));
        let mut config_str = String::new();
        file.read_to_string(&mut config_str).expect("reading config file");
        //let mut system: System = toml::de::from_str(&config_str).expect("parsing config");
        let _: System = toml::de::from_str(&config_str).expect("parsing documented config");
    }
}
