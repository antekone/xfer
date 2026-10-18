fn main() {
    println!("cargo:rerun-if-changed=proto/xfer.proto");
    let mut config = prost_build::Config::new();
    config.skip_debug([
        ".xfer.ipc.v1.Request",
        ".xfer.ipc.v1.Command",
        ".xfer.ipc.v1.PasswordConnect",
        ".xfer.ipc.v1.OutputChunk",
    ]);
    // Compile the schema in Rust as well: normal Cargo builds do not need a
    // platform-specific protoc binary (important for the BSD targets).
    let descriptors = protox::compile(["proto/xfer.proto"], ["proto"]).expect("parse IPC schema");
    config
        .compile_fds(descriptors)
        .expect("generate IPC bindings");
}
