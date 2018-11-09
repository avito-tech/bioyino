use std::collections::HashMap;

use rand::random;

use slog::Logger;

use futures::future::lazy;
use tokio::runtime::current_thread::spawn;

use raft_tokio::raft_consensus::persistent_log::mem::MemLog;
use raft_tokio::raft_consensus::state::ConsensusState;
use raft_tokio::raft_consensus::state_machine::null::NullStateMachine;
use raft_tokio::raft_consensus::ServerId;

////use raft_tokio::raft::RaftPeerProtocol;
use config::Raft;
use raft_tokio::raft::{BiggerIdSolver, ConnectionSolver};
use raft_tokio::start_raft_tcp;
use raft_tokio::Notifier;
use util::{get_hostname, switch_leader, try_resolve};

#[derive(Clone)]
pub struct LeaderNotifier(Logger);

impl Notifier for LeaderNotifier {
    fn state_changed(&mut self, old: ConsensusState, new: ConsensusState) {
        if old != new {
            if new == ConsensusState::Leader {
                switch_leader(true, &self.0)
            } else if old == ConsensusState::Leader {
                switch_leader(false, &self.0)
            }
        }
    }
}

// we reuse the type to avoid creating a new one
impl ConnectionSolver for LeaderNotifier {
    fn solve(&self, is_client: bool, local_id: ServerId, remote_id: ServerId) -> bool {
        return BiggerIdSolver.solve(is_client, local_id, remote_id);
    }
}

pub(crate) fn start_internal_raft(options: Raft, logger: Logger) {
    let this = if let Some(name) = options.this_node.clone() {
        try_resolve(&name)
    } else {
        let hostname = get_hostname().expect("getting own hostname") + ":8138";
        try_resolve(&hostname)
    };

    let mut this_id = None;
    if options.nodes.len() < 3 {
        warn!(
            logger,
            "raft requires at least 3 nodes, this may work not as intended"
        );
    }

    // resolve nodes and generate random ServerId
    let mut nodes = options
        .nodes
        .iter()
        .map(|(node, id)| {
            let addr = try_resolve(node);
            if addr == this {
                this_id = Some(ServerId::from(*id))
            }
            (ServerId::from(*id), addr)
        }).collect::<HashMap<_, _>>();

    //let id = this_id/.expect("list of nodes must contain own hostname");
    use raft_tokio::raft_consensus::ServerId;
    let id = this_id.unwrap_or_else(|| {
        let id: ServerId = random::<u64>().into();
        nodes.insert(id, this);
        id
    });
    // prepare consensus
    let raft_log = MemLog::new();
    let sm = NullStateMachine;
    let notifier = LeaderNotifier(logger.clone());
    let solver = notifier.clone();
    let options = options.get_raft_options();

    // Create the raft runtime
    let raft = lazy(move || {
        start_raft_tcp(id, nodes, raft_log, sm, notifier, options, logger, solver);
        Ok(())
    });

    spawn(raft);
}
