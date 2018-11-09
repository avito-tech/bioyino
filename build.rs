extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("schema/protocol.capnp")
        .run()
        .expect("Failed compiling messages schema");
}
