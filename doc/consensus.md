# Consensus types and modes of operation

Consensus in bioyino is responsible for selecting leader node. Only leader node sends metrics when period comes to end.

Bioyino currently supports two types of consensus:
* None - no consensus
* Internal - builtin implementation using Raft protocol
* Consul - the distributed lock using Hashicorp Consul

The type of consensus is chosen by `consensus` option in config. After that all consensus-specific settings
are specified in corresponding `[consul]` and `[raft]` sections.

## No consensus
In no consensus mode one should ensure that `start-as-leader` parameter is set to true in configuration file. Since
there is no entity to switch leadership, inode will send no metrics with leader disabled mode.

## Consul
Consul consensus requires a working consul cluster. Bioyino will connect to agents and try to get a lock on key.

Consul limitations allow key TTL to be only not less than 10 seconds. So the worst case must consider that leader
will be wrong for around this time.

## Builtin Raft
In this mode nodes connect to each other using Raft. The leader node selected by Raft will be the Bioyino leader node.
Nodes switch fast, but administrator cannot decide which one is to be the leader now.

### Delay
Since there is no possibility to decide who is leader, the node that just started can become leader too fast. Even at
the very start. In such case node will not have the full set of metrics to be sent, and may _ruin_ the aggregation for
at least one aggregation period. To avoid such behaviour, the raft can be set to only start after some delay, when node
has gathered enough metrics. Also, there is a parameter setting consensus mode at the start, so node can be forced to
start without leadership. One should be especially careful in such case, because this opens a chance that no leader at
all would exist among nodes and no metrics is to be sent at all.


## Paused and disabled consensus states
Both `Paused` and `Disabled` states' goal is to stop any influence to leadership status regardless of consensus
leadership state.

In a Paused state consensus still works by itself. For Consul, it means the lock(it is taken) is still updated.
For Raft, it means a node still sends heartbeat requests to other nodes and is considered alive by them.

In a Disabled state Consul stops updating its lock. In Raft disabling it is currently not supported, so the only way
to turn it off atm is to turn off the whole bioyino process on node.

## Switching consensus and leadership state
Leadership is usually expected to switch atomically with the consensus state, so both states are switched with a single
command or HTTP request. Actually, all the commands  sent by bioyino are just HTTP requests with JSON-encoded body,
so all the same actions can be done using any HTTP client.

*NOTE*: sending commands using `bioyino query` still requires a configuration file to determine some settings,
so in all cases mentioned below we suppose the config exists at the default location. 
In other cases all commands should be explicitly specified with config file path. I.e. 
`bioyino --config /some/path/config.toml query ...`

`bioyino query status` shows current consensus and leadership switches

`bioyino query consensus` allows switching consensus state and leadership on any node in the cluster

 Full command spec:

`bioyino query [--node <node>] consensus <enable|disable|pause|resume> [<unchanged|enabled|disabled>]`

First option here corresponds to consensus state, and the second one is optional(default=unchanged) for the leader state

JSON spec is the same, with the same meaning for list elements: `{  "consensus-command": [ "enable", "enable" ] }`


