pub mod consensus;
pub mod core;
pub mod network;
pub mod connectors;

use clap::Parser;
use futures::stream::StreamExt;
use libp2p::identity::ed25519::Keypair;
use malachite_config::TimeoutConfig;
use malachite_metrics::{Metrics, SharedRegistry};
use prost::Message;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::{select, time};
use tracing_subscriber::EnvFilter;
use connectors::fname::Fetcher;


use crate::consensus::consensus::{Consensus, ConsensusMsg, ConsensusParams};
use crate::core::types::{proto, Address, Height, ShardId, SnapchainContext, SnapchainShard, SnapchainValidator, SnapchainValidatorContext, SnapchainValidatorSet};
use crate::network::gossip::{GossipEvent};
use network::gossip::SnapchainGossip;

pub enum SystemMessage {
    Consensus(ConsensusMsg<SnapchainValidatorContext>),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let base_port = 50050;
    let port = base_port + args.id;
    let addr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", port);

    println!("SnapchainService (ID: {}) listening on {}", args.id, addr);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    let keypair = Keypair::generate();

    let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

    let gossip_result = SnapchainGossip::create(keypair.clone(), addr, system_tx.clone());
    if let Err(e) = gossip_result {
        println!("Failed to create SnapchainGossip: {:?}", e);
        return Ok(());
    }

    let mut gossip = gossip_result?;
    let gossip_tx = gossip.tx.clone();

    tokio::spawn(async move {
        println!("Starting gossip");
        gossip.start().await;
        println!("Gossip Stopped");
    });

    // TODO: config to enable/disable running this fetcher
    // TODO: add start_from (and possibly stop_at) as parameter(s)
    let mut fetcher = Fetcher::new(685400u64);

    tokio::spawn(async move {
        fetcher.run().await;
    });

    let registry = SharedRegistry::global();
    let metrics = Metrics::register(registry);

    let shard = SnapchainShard::new(0); // Single shard for now
    let validator_address = Address(keypair.public().to_bytes());
    let validator = SnapchainValidator::new(shard.clone(), keypair.public().clone());
    let validator_set = SnapchainValidatorSet::new(vec![validator]);

    let consensus_params = ConsensusParams {
        start_height: Height::new(shard.shard_id(), 1),
        initial_validator_set: validator_set,
        address: validator_address.clone(),
        threshold_params: Default::default(),
    };

    let ctx = SnapchainValidatorContext::new(keypair.clone());

    let consensus_actor = Consensus::spawn(
        ctx,
        shard,
        consensus_params,
        TimeoutConfig::default(),
        metrics.clone(),
        None,
        gossip_tx.clone(),
    )
        .await
        .unwrap();

    // Create a timer for block creation
    let mut block_interval = time::interval(Duration::from_secs(2));

    let mut tick_count = 0;

    // Kick it off
    loop {
        select! {
            _ = ctrl_c() => {
                println!("Received Ctrl-C, shutting down");
                consensus_actor.stop(None);
                return Ok(());
            }
            _ = block_interval.tick() => {
                tick_count += 1;
                // Every 5 ticks, re-register the validators so that new nodes can discover each other
                if tick_count % 5 == 0 {
                    let register_validator = proto::RegisterValidator {
                        validator: Some(proto::Validator {
                            signer: keypair.public().to_bytes().to_vec(),
                            fid: 0,
                        }),
                        nonce: tick_count as u64,   // Need the nonce to avoid the gossip duplicate message check
                    };
                    println!("Registering validator with nonce: {}", register_validator.nonce);
                    gossip_tx.send(GossipEvent::RegisterValidator(register_validator)).await?;
                }
            }
            Some(msg) = system_rx.recv() => {
                match msg {
                    SystemMessage::Consensus(consensus_msg) => {
                        // Forward to consesnsus actor
                        consensus_actor.cast(consensus_msg).unwrap();
                    }
                }
            }
        }
    }
}
