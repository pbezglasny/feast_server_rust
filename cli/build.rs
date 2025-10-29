fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &["../feast-server-core/protos/feast/serving/ServingService.proto"],
            &["../feast-server-core/protos"],
        )?;

    println!(
        "cargo:rerun-if-changed=../feast-server-core/protos/feast/serving/ServingService.proto"
    );
    println!("cargo:rerun-if-changed=../feast-server-core/protos/feast/types/Value.proto");
    println!("cargo:rerun-if-changed=../feast-server-core/protos");

    Ok(())
}
