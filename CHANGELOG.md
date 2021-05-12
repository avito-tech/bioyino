# Release 0.8.0 #
**WARNING!** This release has **lots** of incompatible changes. Please make sure to read the section below before doing
important changes in critical infrastructures.

## Incompatible changes ##
* Consul support has been completely removed. Ensure to remove the consul section from your configuration files and use internal Raft.
* Threading model received a big rework and should lead to lower memory consumption and lower latencies, especially on higher metric cardinalities. See docs/threading.md for more details.
* Due to changes in threading:
    * the following options are removed: `aggregation.mode`, `aggregation.threads`
    * the following option has been added: `p-threads`
    * the `c-threads` option is now `a-threads`
* statsd sampling rate is now completely supported
* The peer protocol and it's capnp schema is now considered only for internal use. It received a new version and is better structured for this purpose and internal
metrics representation. Due to this changes:
    * the new option `network.peer-protocol` has been added to specify an exact version
    * version 2 **is the default value in 0.8.0**, please consider setting `network.peer-protocol = "1"` explicitly in client configs before upgrading
    * version 1 will be removed in 0.9.0, all users are recommended to migrate to using v2 since it's release
* diff-counter metric type has been deprecated to being unituitive and therefore avoided of being unused by anyone
* bioynio's own internal metric - `egress` is now called `egress-carbon` which better points to it's real meaning

See config.toml for further configuration instructions

## Major changes ##
* `.rate` aggregate has been added to show number of incoming values per second
* new internal own metrics added - `egress-peer` - showing number of metrics sent to peers per second

## Internal changes ##
* the internal structure of metrics has changed to make a type system more helping
* many libraries have their versions updated to latest major versions

## Minor changes
* bioyino can now be compiled with 32-bit floats instead of 64-bit. This may bring some storage economy at the price of
lower precision
* two new internal metrics has been added:
    * `slow-q-len` - number of pending tasks in "slow" threadpool queue
    * `ingress-metrics-peer` - number of metrics received via TCP

# Release 0.7.0 #

## Incompatible changes ##
* naming options have changed again
    * naming is now per-type, each type in specific section `naming.<type>`
    * postfix, prefix and tag naming settings are now in corresponding naming sections
    * `aggregation.destination` is now specified in corresponding naming sections

* a list of aggregates is now customizable for all types of metrics
    * `aggregation.ms-aggregates` is now `aggregation.aggregates.timer`
    * all other metric types have their own `aggregation.aggregates.<type>` section

* syslog logging is available via `verbosity-syslog` configuration option
* console logging verbosity has changed name from just `verbosity` to `verbosity-console`

See config.toml for further configuration instructions

## Major changes ##
The snapshot sending has been reworked:
Instead of sending each snapshot in a single connection with backoff settings, it is  now using ring buffer for shapshots (meaning the oldest snapshot is deleted when buffer is full). This helps to avoid problems when senders were taking memory infinitely when one of nodes disappear. Snapshots are also sent in a single TCP-connection without resolve+connect overhead for each snapshot.

* `max_snapshots` option has been added, allowing to set maximum number of snapshots _per remote node_ stored in ring buffer
* new endpoint `/stats` is available at management server (HTTP on port 8137)
* becoming daemon is available via `daemon` option in config, the setting can also be overrided by `-f`/`--foreground` command line options
* fixed an incorrect behaviour when dropping of incoming data was not actually happening when queue is full, and memory was growing instead

## Internal changes ##
* the runtime has been updated to new rust async paradigm with tokio-2 and futures-3 (except raft and consul)
* many other libraries have their versions updated to latest major versions and incompatibilities fixed

## Minor changes
* added aggregate naming options document

# Release 0.6.0 #

## Incompatible changes ##
* aggregation options now have separate section. Changed parameters:
    * `metrics.aggregation-mode` -> `aggregation.mode`
    * `metrics.aggregation-threads` -> `aggregation.threads`
    * `metrics.update-counter-threshold` -> `aggregate.update-count-threshold` (note `count` instead of `counter`)

* a list of particular aggregates is now customizable for ms-type metrics
* aggregate naming is now customizable through `prefix-replacements`, `tag-replacements` and `postfix-replacements` in `aggregate` section

* update counter is now an aggregate. Options changed:
    * `metrics.update-counter-prefix` moved to `prefix-replacements` naturally
    * `metrics.update-counter-suffix` moved to `postfix-replacements` naturally

## Major changes ##
* graphite-style tags are now supported:
    * aggregates can be placed in tags instead of postfixes (see `aggregate.mode` parameter)
    * a new option `metrics.create-untagged-copy` is available to create a copy of untagged metric for compatibility
      the option is false by default and needs to be enabled explicitly
* timestamp rounding option is available as `aggregate.round-timestamp` to better match aggregation delays with rounding at receiver

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
