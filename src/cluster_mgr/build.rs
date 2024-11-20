// build.rs

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    println!("cargo:warning=OUT_DIR is {}", out_dir);

    println!("cargo:rerun-if-changed=proto/cc_request.proto");

    tonic_build::configure()
        .build_server(false) // Set to true if you need server code
        .compile(&["proto/cc_request.proto"], &["proto"])?;
    Ok(())
}
