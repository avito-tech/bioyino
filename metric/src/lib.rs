pub mod metric;
pub mod parser;

pub use crate::metric::*;

pub mod protocol_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/protocol_capnp.rs"));
}

