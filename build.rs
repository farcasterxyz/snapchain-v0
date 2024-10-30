fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("src/proto/blocks.proto")?;
    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder.type_attribute("snapchain.ShardHash", "#[derive(Eq, PartialOrd, Ord)]");

    builder.compile(&["src/proto/blocks.proto"], &["src/proto"])?;
    Ok(())
}
