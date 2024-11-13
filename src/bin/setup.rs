use libp2p::identity::ed25519::SecretKey;

#[tokio::main]
async fn main() {
    // Create 4 nodes by default
    let nodes = 4;

    // create directory at the root of the project if it doesn't exist
    if !std::path::Path::new("nodes").exists() {
        std::fs::create_dir("nodes").expect("Failed to create nodes directory");
    }

    let base_rpc_port = 3382;
    let base_gossip_port = 50050;
    for i in 1..=nodes {
        let id = i;

        if !std::path::Path::new(format!("nodes/{id}").as_str()).exists() {
            std::fs::create_dir(format!("nodes/{id}")).expect("Failed to create node directory");
        }
        let secret_key = hex::encode(SecretKey::generate());
        let rpc_port = base_rpc_port + i;
        let gossip_port = base_gossip_port + i;
        let host = "127.0.0.1";
        let rpc_address = format!("{host}:{rpc_port}");
        let gossip_multi_addr = format!("/ip4/127.0.0.1/udp/{gossip_port}/quic-v1");
        let other_nodes_addresses = (1..=nodes)
            .filter(|&x| x != id)
            .map(|x| format!("/ip4/127.0.0.1/udp/{:?}/quic-v1", base_gossip_port + x))
            .collect::<Vec<String>>()
            .join(",");

        let config_file_content = format!(
            r#"
id = {id}
rpc_address="{rpc_address}"
rocksdb_dir=".rocks{i}"

[gossip]
address="{gossip_multi_addr}"
bootstrap_peers = "{other_nodes_addresses}"

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
