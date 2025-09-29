use glob::glob;
use std::io::Result;

fn main() -> Result<()> {
    let protos = glob("protos/**/*.proto")
        .unwrap()
        .map(|res| res.unwrap().as_path().to_owned())
        .collect::<Vec<_>>();
    prost_build::compile_protos(&protos, &["protos"])?;
    Ok(())
}
