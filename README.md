# Bioyino #
The StatsD server written in Rust

# Description #
A fully statsd-protocol compliant server with carbon backend
This server was written as a drop-in replacement for now unsupported [github's brubeck](https://github.com/github/brubeck/issues).

# Features #

* all basic metric types supported (gauge, counter, diff-counter, timer), new types are easy to be added
* fault tolerant: metrics are replicated to all nodes in the cluster
* clustering: all nodes gather and replicate metrics, but only leader sends metrics to backend
* precise: 64-bit floats, full metric set is stored in memory (for metric types that require post-processing), no approximation algorithms involved
* safety and security: written in memory-safe language
* networking is separate from counting to avoid dropping UDP packets as much as possible
* networking is asynchronous
* small memory footprint and low CPU consumption

# Status #
Currently works, being tested on production-grade metric stream (~1,5M metrics per second)

# Installing #
Do the usual Rust-program build-install cycle

```
$ git clone <this repo>
$ cargo build --release && strip target/release/bioyno
```

# Configuring #
As of current version no config file is read, everything is configured via commandline flags

# Contributing #

You can help project by doing the following:
* find TODOs/FIXMEs and unwraps in the code fix them and create a PR
* solve issues
* create issues to request new features
* add new features, like new metric types
* test the server on your environment and creating new issues if/when bugs found

