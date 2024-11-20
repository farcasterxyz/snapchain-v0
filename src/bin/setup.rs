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

    // create directory at the root of the project if it doesn't exist
    if !std::path::Path::new("nodes").exists() {
        std::fs::create_dir("nodes").expect("Failed to create nodes directory");
    }

    let base_rpc_port = 3382;
    let base_gossip_port = 50050;
    for i in 1..=nodes {
        let id = i;
        let db_dir = format!(".rocks");

        if !std::path::Path::new(format!("nodes/{id}").as_str()).exists() {
            std::fs::create_dir(format!("nodes/{id}")).expect("Failed to create node directory");
        } else {
            if std::path::Path::new(db_dir.clone().as_str()).exists() {
                std::fs::remove_dir_all(db_dir.clone()).expect("Failed to remove .rocks directory");
            }
        }
        let secret_key = hex::encode(SecretKey::generate());
        let rpc_port = base_rpc_port + i;
        let gossip_port = base_gossip_port + i;
        let host = format!("172.100.0.1{i}");
        let rpc_address = format!("{host}:{rpc_port}");
        let gossip_multi_addr = format!("/ip4/{host}/udp/{gossip_port}/quic-v1");
        let other_nodes_addresses = (1..=nodes)
            .filter(|&x| x != id)
            .map(|x| format!("/ip4/172.100.0.1{x}/udp/{:?}/quic-v1", base_gossip_port + x))
            .collect::<Vec<String>>()
            .join(",");

        let propose_value_delay = humantime::format_duration(args.propose_value_delay);

        let config_file_content = format!(
            r#"
id = {id}
rpc_address="{rpc_address}"
rocksdb_dir="{db_dir}"

[gossip]
address="{gossip_multi_addr}"
bootstrap_peers = "{other_nodes_addresses}"

[consensus]
private_key = "{secret_key}"
propose_value_delay = "{propose_value_delay}"
            "#
        );

        // clean up whitespace
        let config_file_content = config_file_content.trim().to_string() + "\n";

        std::fs::write(
            format!("nodes/{id}/snapchain.toml", id = id),
            config_file_content,
        )
        .expect("Failed to write config file");
        // Print a message
    }
    println!("Created configs for {nodes} nodes");
}
