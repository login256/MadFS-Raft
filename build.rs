extern crate cc;

fn main() -> std::io::Result<()> {
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");
    let proto = "proto/proto.proto";
    tonic_build::configure().compile_with_config(config, &["proto/proto.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed={}", proto);

    cc::Build::new()
    .file("c/sqlite.c")
    .compile("libsqlitesl.a");
    Ok(())
}
