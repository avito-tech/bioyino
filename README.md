# Bioyino #
the StatsD server written in Rust 

# Description #
This server was written as a drop-in replacement for now unsupported (github's brubeck)[https://github.com/github/brubeck/issues]

Features:

* BigInt counting
* writes to Graphite (plaintext backend)
* uses sharded concurrent hashmap
* tries to use stack and be zero-copy bas much as possible
* ...

# Status #
Currently works, but wasn't tested in production

# Contributing #

You can help project by doing the following:
* find TODOs/FIXMEs and unwraps in the code fix them and create a PR
* solve issues
* add new features, like new counter types
* test the server on your environment and creating new issues

