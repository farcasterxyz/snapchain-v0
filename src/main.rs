use clap::Parser;
use futures::stream::StreamExt;
use libp2p::identity::ed25519::Keypair;
use malachite_config::TimeoutConfig;
use malachite_metrics::{Metrics, SharedRegistry};
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio::{select, time};
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use snapchain::consensus::consensus::{BlockProposer, Consensus, ConsensusMsg, ConsensusParams, ShardValidator, SystemMessage};
use snapchain::core::types::{
    proto, Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
    SnapchainValidatorSet,
};
use snapchain::network::gossip::GossipEvent;
use snapchain::network::gossip::SnapchainGossip;
use snapchain::proto::rpc::snapchain_service_server::SnapchainServiceServer;
use snapchain::network::server::MySnapchainService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();

    let app_config = snapchain::cfg::load_and_merge_config(args)?;

    if app_config.id == 0 {
        return Err("node id must be specified greater than 0".into());
    }

    let base_port = 50050;
    let port = base_port + app_config.id;
    let addr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", port);

    let base_grpc_port = 50060;
    let grpc_port = base_grpc_port + app_config.id;
    let grpc_addr = format!("0.0.0.0:{}", grpc_port);
    let grpc_socket_addr: SocketAddr = grpc_addr.parse()?;

    info!(
        id = app_config.id,
        addr = addr,
        grpc_addr = grpc_addr,
        "SnapchainService listening",
    );

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match app_config.log_format.as_str() {
        "text" => tracing_subscriber::fmt().with_env_filter(env_filter).init(),
        "json" => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init(),
        _ => {
            return Err(format!("Invalid log format: {}", app_config.log_format).into());
        }
    }

    let keypair = app_config.consensus.keypair().clone();

    info!("Starting Snapchain node with public key: {}", hex::encode(keypair.public().to_bytes()));

    let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

    let gossip_result = SnapchainGossip::create(keypair.clone(), addr, system_tx.clone());
    if let Err(e) = gossip_result {
        error!(error = ?e, "Failed to create SnapchainGossip");
        return Ok(());
    }

    let mut gossip = gossip_result?;
    let gossip_tx = gossip.tx.clone();

    tokio::spawn(async move {
        info!("Starting gossip");
        gossip.start().await;
        info!("Gossip Stopped");
    });

    if !app_config.fnames.disable {
        let mut fetcher = snapchain::connectors::fname::Fetcher::new(app_config.fnames.clone());

        tokio::spawn(async move {
            fetcher.run().await;
        });
    }

    if !app_config.onchain_events.rpc_url.is_empty() {
        let mut onchain_events_subscriber =
            snapchain::connectors::onchain_events::Subscriber::new(app_config.onchain_events)?;
        tokio::spawn(async move {
            let result = onchain_events_subscriber.run().await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("Error subscribing to on chain events {:#?}", e);
                }
            }
        });
    }

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    tokio::spawn(async move {
        let service = MySnapchainService::default();

        let resp = Server::builder()
            .add_service(SnapchainServiceServer::new(service))
            .serve(grpc_socket_addr)
            .await;

        let msg = "grpc server stopped";
        match resp {
            Ok(()) => error!(msg),
            Err(e) => error!(error = ?e, "{}", msg),
        }

        shutdown_tx.send(()).await.ok();
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
    let block_proposer = BlockProposer::new(validator_address.clone(), shard.clone());
    let shard_validator = ShardValidator::new(validator_address.clone(), block_proposer);
    let consensus_actor = Consensus::spawn(
        ctx,
        shard,
        consensus_params,
        TimeoutConfig::default(),
        metrics.clone(),
        None,
        gossip_tx.clone(),
        shard_validator,
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
                info!("Received Ctrl-C, shutting down");
                consensus_actor.stop(None);
                return Ok(());
            }
            _ = shutdown_rx.recv() => {
                error!("Received shutdown signal, shutting down");
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
                    info!("Registering validator with nonce: {}", register_validator.nonce);
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
