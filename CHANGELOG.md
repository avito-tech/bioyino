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
