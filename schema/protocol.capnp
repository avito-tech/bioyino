@0xd87a49a1c493df22;

# This schema defines a way to deliver metrics in both ways:
# as pre-aggregated shapshots and as new metrics
# Please note, that capnproto allows to skip sending any fields
# if they are separate types, so  there is almost no need to integrate
# option-like type into schema type system.
# Bio will try to accept unspecified fields with some defaults,
# but may fail if it cannot get ones it needs

# A message type for using in network interactions when metrics are involved
# the difference between snapshot and multi is that snapshot will be sent
# only to backend not to other nodes in the network
struct Message {
    union {
        single @0 :Metric;
        multi @1 :List(Metric);
        snapshot @2 :List(Metric);
    }
}

# A message type for internal messaging, should not be used by clients
# WARNING: This is reserved for future, only some commands may work
struct PeerCommand {
    union {
        # pause consensus leadership changes, see description below
        pauseConsensus @0 :PauseConsensusCommand;

        # Resume consensus. Leader will be set to one from consensus
        resumeConsensus @1 :Void;

        # server will answer with ServerStatus message
        status @2 :Void;
    }
}

# Turn consensus off for time(in milliseconds). The consensus module will still work, but signals 
# on leadership changes will be ignored
# Leader state will be unchanged if setLeader is 0
# Leader will be disabled if setLeader is < 0
# Leader will be enabled if setLeader is > 0
struct PauseConsensusCommand {
    pause @0 :UInt64;
    setLeader @1 :Int8;
}

struct ServerStatus {
    leaderStatus @0 :Bool;
    consensusPaused @1 :UInt64;
}

struct Metric {

    # everyone should have a name, even metrics
    name @0 :Text;

    # each metric has a value when it's sent
    value @1 :Float64;

    # some types also imply additional internal values depending if metric type
    type @2 :MetricType;

    # a timesamp can optionally be sent, i.e. for historic reasons
    timestamp @3 :Timestamp;

    # additional useful data about metric
    meta @4 :MetricMeta;
}

struct Timestamp {
    ts @0 :UInt64;
}

struct MetricType {
    union {
        # counter value is stored inside it's value
        counter @0 :Void;

        # for diff counter the metric value stores current counter value
        # the internal value stores last received counter change fr differentiating
        diffCounter @1 :Float64;

        # timer holds all values for further stats counting
        timer @2 :List(Float64);

        # gauge can work as a counter too when `+value` or `-value` is received
        gauge @3 :Gauge;

        # someday we will support this... conributions are welcomed if you need any of those
        #   histogram @4 :...
        #   set @5 :...
    }
}

struct Gauge {
    union {
        unsigned @0 :Void;
        signed @1 :Int8;
    }
}

struct MetricMeta {
    sampling @0 :Sampling;
    updateCounter @1 :UInt32;
}

struct Sampling  {
    sampling @0 :Float32;
}
