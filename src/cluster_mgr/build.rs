// build.rs

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cc_request.proto");
    println!("cargo:rerun-if-changed=proto/log.proto");

    tonic_build::configure()
        .build_server(false) // Set to true if you need server code
        .compile(&["proto/cc_request.proto", "proto/log.proto"], &["proto"])?;
    Ok(())
}
