# Bioyino #
The StatsD server written in Rust

# Description #
Bioyino is a distributed statsd-protocol server with carbon backend.

# Features #

* all basic metric types supported (gauge, counter, diff-counter, timer), new types are easy to be added
* fault tolerant: metrics are replicated to all nodes in the cluster
* clustering: all nodes gather and replicate metrics, but only leader sends metrics to backend
* precise: 64-bit floats, full metric set is stored in memory (for metric types that require post-processing), no approximation algorithms involved
* standalone: can work without external services
* safety and security: written in memory-safe language
* networking tries to do it's best to avoid dropping UDP packets as much as possible
* networking is asynchronous
* small memory footprint and low CPU consumption

# Status #
Currently works in production at Avito, processing production-grade metric stream (~4M metrics per second on 3 nodes)

# Installing #
One of Bioyino's most powerful features - multimessage mode - require it to be working on GNU/Linux.

* Install [capnp compiler tool](https://capnproto.org/install.html) to generate schemas. It's usually downloaded using your distribution's package manager.
* Do the usual Rust-program build-install cycle. Please note, that building is always tested on latest stable version of Rust. Rust 2018 edition is required.

```
$ git clone <this repo>
$ cargo build --release && strip target/release/bioyno
```
# Build RPM package (for systemd-based distro)

1.  Install requirements (as root or with sudo)
```
    yum install -y cargo capnproto capnproto-devel
    yum install -y ruby-devel
    gem install fpm
```

2.  Build
```
    bash contrib/fpm/create_package_rpm.sh
```

# Build DEB package (for systemd-based distro)

1.  Install requirements (as root or with sudo)
```
    apt-get install -y capnproto libcapnp-dev
    apt-get install -y ruby-dev
    gem install fpm
```

2.  Build
```
    bash contrib/fpm/create_package_deb.sh
```

# Configuring #
To configure, please, see config.toml, all the options are listed there and all of them are commented.

# Contributing #

You can help project by doing the following:
* find TODOs/FIXMEs and unwraps in the code fix them and create a PR
* solve issues
* create issues to request new features
* add new features, like new metric types
* test the server on your environment and creating new issues if/when bugs found
