use clap::Parser;
use libp2p::identity::ed25519::SecretKey;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Delay for proposing a value (e.g. "250ms")
    #[arg(long, value_parser = parse_duration)]
    propose_value_delay: Duration,
}

fn parse_duration(arg: &str) -> Result<Duration, String> {
    humantime::parse_duration(arg).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Create 4 nodes by default
    let nodes = 4;

    // Create "nodes" directory if it doesn't exist
    if !std::path::Path::new("nodes").exists() {
        std::fs::create_dir("nodes").expect("Failed to create nodes directory");
    }

    let base_rpc_port = 3382;
    let base_gossip_port = 50050;

    for i in 1..=nodes {
        let id = i;
        let db_dir = format!("nodes/{}/.rocks", id); // Correct path for the node-specific directory

        if !std::path::Path::new(&format!("nodes/{id}")).exists() {
            std::fs::create_dir(format!("nodes/{id}")).expect("Failed to create node directory");
        } else {
            // If the RocksDB directory exists, remove it
            if std::path::Path::new(&db_dir).exists() {
                std::fs::remove_dir_all(&db_dir).expect("Failed to remove .rocks directory");
            }
        }

        // Generate a secret key (use random for generating keys)
        let secret_key = hex::encode(SecretKey::random().to_bytes());

        let rpc_port = base_rpc_port + i;
        let gossip_port = base_gossip_port + i;
        let host = format!("172.100.0.1{}", i); // Fix IP formatting
        let rpc_address = format!("{host}:{rpc_port}");
        let gossip_multi_addr = format!("/ip4/{host}/udp/{gossip_port}/quic-v1");

        // Generate a list of addresses of other nodes
        let other_nodes_addresses = (1..=nodes)
            .filter(|&x| x != id)
            .map(|x| format!("/ip4/172.100.0.1{x}/udp/{}/quic-v1", base_gossip_port + x))
            .collect::<Vec<String>>()
            .join(",");

        // Convert propose_value_delay to a string for TOML config
        let propose_value_delay = humantime::format_duration(args.propose_value_delay);

        // Generate config file content
        let config_file_content = format!(
            r#"
id = {id}
rpc_address = "{rpc_address}"
rocksdb_dir = "{db_dir}"

[gossip]
address = "{gossip_multi_addr}"
bootstrap_peers = "{other_nodes_addresses}"

[consensus]
private_key = "{secret_key}"
propose_value_delay = "{propose_value_delay}"
            "#,
            id = id,
            rpc_address = rpc_address,
            db_dir = db_dir,
            gossip_multi_addr = gossip_multi_addr,
            other_nodes_addresses = other_nodes_addresses,
            secret_key = secret_key,
            propose_value_delay = propose_value_delay,
        );

        // Clean up whitespace
        let config_file_content = config_file_content.trim().to_string() + "\n";

        // Write config file for each node
        std::fs::write(
            format!("nodes/{}/snapchain.toml", id), // Fixed path format
            config_file_content,
        )
        .expect("Failed to write config file");

        // Optionally print the node-specific message here
        println!("Created config for node {id}");
    }

    println!("Created configs for {nodes} nodes");
}
