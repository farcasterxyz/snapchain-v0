fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("src/proto/blocks.proto")?;
    tonic_build::configure()
        // .type_attribute(".", "#[derive(Eq, PartialOrd, Ord)]")// For malachite
        .compile(&["src/proto/blocks.proto"], &["src/proto"])?;
    Ok(())
}
