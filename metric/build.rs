extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .edition(capnpc::RustEdition::Rust2018)
        .file("schema/protocol.capnp")
        .run()
        .expect("Failed compiling messages schema");
}
