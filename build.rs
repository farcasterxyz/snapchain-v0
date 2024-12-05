fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = tonic_build::configure();

    // Custom type attributes required for malachite
    builder = builder
        .type_attribute("ShardHash", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Height", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        // TODO: this generates a lot of code, perhaps choose specific structures
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    // TODO: auto-discover proto files
    builder.compile(
        &[
            "src/proto/admin_rpc.proto",
            "src/proto/blocks.proto",
            "src/proto/rpc.proto",
            "src/proto/message.proto",
            "src/proto/onchain_event.proto",
            "src/proto/hub_event.proto",
            "src/proto/username_proof.proto",
            "src/proto/sync_trie.proto",
        ],
        &["src/proto"],
    )?;

    Ok(())
}
