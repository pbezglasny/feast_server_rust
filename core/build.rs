use glob::glob;
// use protobuf_codegen;
// use protoc_bin_vendored;
use std::io::Result;

fn main() -> Result<()> {
    let protos = glob("protos/**/*.proto")
        .unwrap()
        .map(|res| res.unwrap().as_path().to_owned())
        .collect::<Vec<_>>();
    // protobuf_codegen::Codegen::new()
    //     .protoc()
    //     .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
    //     .includes(&["protos"])
    //     .inputs(protos)
    //     .cargo_out_dir("protos_out")
    //     .run_from_script();
    prost_build::compile_protos(&protos, &["protos"])?;
    Ok(())
}
