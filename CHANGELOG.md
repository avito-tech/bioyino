# Release 0.6.0 #

## Incompatible changes ##
* aggregation options now have separate section. Changed parameters:
    * `metrics.aggregation-mode` -> `aggregation.mode`
    * `metrics.aggregation-threads` -> `aggregation.threads`

* Update counter is now an aggregate. Options changed:
    * `metrics.update-counter-prefix` is no option anymore.
    * `metrics.update-counter-suffix` moved to replacements(see below) naturally
    * `metrics.update-counter-threshold` is now `aggregate.update-count-threshold` (note it is now *count instead of counter*)

## Major changes ##
* graphite-style tags are now supported:
    * aggregates can be placed in tags instead of postfixes
    * postfix and tag names can be customized
    * a new option `metrics.create-untagged-copy` is available to create a copy of untagged metric for compatibility
      the option is false by default and needs to be enabled explicitly
* a list of particular aggregates is now customizable for ms-type metrics

See config.toml for configuration instructions

## Minor changes
* more unit testing

# Release 0.5.1 #
* Fixed bug where parallel aggregation was panicking if turned on

# Release 0.5.0 #

## Significant changes
* set metric type is now available
* metric parser has been rewritten, battle-tested, and now considers many corner cases and limits
* all TCP clients now support binding to specific address/interface for complicated ip/interface configurations
* aggregation can now be done in different parallelization modes depending on incoming metric workloads

## Less significant changes
* some documentation and examples has beed added
* snapshot sending now has retries if sends get unsuccessful
* carbon client now has an option to send metrics in many parallel connections
* some very core functionality is in a separate crate now and can be used in products willing to deliver metrics faster
* code migrated to Rust 2018
* code modularity, readability and documentation has been improved
* obviously a pack of bug fixes have been done based on production usage

# Release 0.4.0 #

This release is notable by internal Raft implementation and schema-based node interconnection, opening a route for large, cluster-aware installations.

User-visible changes:
 * Built-in Raft implemented: multi-node configuration can now be standalone(not requiring Consul) when running on 3 or more nodes
 * Snapshotting has been reworked. Serialization layer migrated to Cap'n'Proto for backwards compatibility.
 * Metrics can now be received using Cap'n'Proto representation in addition to statsd format. This opens a possibility for agent-mode operation, where multiple bioyinos gather metrics in some place, closer to producer, and sending their snapshots to central place for aggregation using TCP and allowing to avoid UDP inconsistency problems.
 * Commands for changing internal state now allow to change consensus and leadership state independently, making possible to manage server leadership state flexibly in critical situations.
 * Commanding interface is now HTTP, which allows to interact with bioyino without having the original binary
 * Carbon backend client is now prone to network problems, trying to reconnect if/when carbon server is inaccessible
 * Incoming buffers are now almost-guaranteed to be delivered for aggregation after some time or amount of data received, which wasn't true for 0.3.0 and frustrated almost every early-adopter

 Developer-visible changes:
 * Most of buffer and string processing is now zero-copy using the excellent `bytes` crate
 * Buffer processing is now made per incoming host, giving better support for hosts sending many metrics to a single UDP socket
 * Lots of bufs fixed including parsing and some corner case processing
 * Library dependencies are updated to lastest versions of everything including `hyper`, `tokio`, `futures`, `combine`, etc.
