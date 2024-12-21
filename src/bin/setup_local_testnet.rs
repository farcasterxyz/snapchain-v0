use clap::Parser;
use libp2p::identity::ed25519::SecretKey;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Delay for proposing a value (e.g. "250ms")
    #[arg(long, value_parser = parse_duration, default_value = "250ms")]
    propose_value_delay: Duration,

    #[arg(long, default_value = "")]
    l1_rpc_url: String,

    #[arg(long, default_value = "")]
    l2_rpc_url: String,

    #[arg(long, default_value = "108864739")]
    start_block_number: u64,

    #[arg(long, default_value = "0")]
    stop_block_number: u64,

    /// Statsd prefix. note: node ID will be appended before config file written
    #[arg(long, default_value = "snapchain")]
    statsd_prefix: String,

    #[arg(long, default_value = "127.0.0.1:8125")]
    statsd_addr: String,

    #[arg(long, default_value = "false")]
    statsd_use_tags: bool,
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
        let db_dir = format!("nodes/{id}/.rocks");

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
        let host = format!("127.0.0.1");
        let rpc_address = format!("{host}:{rpc_port}");
        let gossip_multi_addr = format!("/ip4/{host}/udp/{gossip_port}/quic-v1");
        let other_nodes_addresses = (1..=nodes)
            .filter(|&x| x != id)
            .map(|x| format!("/ip4/127.0.0.1/udp/{:?}/quic-v1", base_gossip_port + x))
            .collect::<Vec<String>>()
            .join(",");

        let propose_value_delay = humantime::format_duration(args.propose_value_delay);

        let statsd_prefix = format!("{}{}", args.statsd_prefix, id);
        let statsd_addr = args.statsd_addr.clone();
        let statsd_use_tags = args.statsd_use_tags;
        let l1_rpc_url = args.l1_rpc_url.clone();
        let l2_rpc_url = args.l2_rpc_url.clone();
        let start_block_number = args.start_block_number;
        let stop_block_number = args.stop_block_number;

        let config_file_content = format!(
            r#"
rpc_address="{rpc_address}"
rocksdb_dir="{db_dir}"
l1_rpc_url="{l1_rpc_url}"

[statsd]
prefix="{statsd_prefix}"
addr="{statsd_addr}"
use_tags={statsd_use_tags}

[gossip]
address="{gossip_multi_addr}"
bootstrap_peers = "{other_nodes_addresses}"

[consensus]
private_key = "{secret_key}"
propose_value_delay = "{propose_value_delay}"

[onchain_events]
rpc_url= "{l2_rpc_url}"
start_block_number = {start_block_number}
stop_block_number = {stop_block_number}
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
