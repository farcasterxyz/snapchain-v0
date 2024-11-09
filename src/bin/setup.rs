use libp2p::identity::ed25519::SecretKey;

#[tokio::main]
async fn main() {
    // Create 4 nodes by default
    let nodes = 4;

    // create directory at the root of the project if it doesn't exist
    if !std::path::Path::new("nodes").exists() {
        std::fs::create_dir("nodes").expect("Failed to create nodes directory");
    }

    for i in 1..=nodes {
        let id = i;

        if !std::path::Path::new(format!("nodes/{id}").as_str()).exists() {
            std::fs::create_dir(format!("nodes/{id}")).expect("Failed to create node directory");
        }
        let secret_key = hex::encode(SecretKey::generate());
        let rpc_port = 3383 + i;

        let config_file_content = format!(
            r#"
id = {id}
rpc_address=0.0.0.0:{rpc_port}

[consensus]
private_key = "{secret_key}"
            "#,
        );

        std::fs::write(
            format!("nodes/{id}/snapchain.toml", id = id),
            config_file_content,
        )
        .expect("Failed to write config file");
        // Print a message
    }
    println!("Created configs for {nodes} nodes");
}
