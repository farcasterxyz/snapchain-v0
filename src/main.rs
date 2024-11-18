use malachite_metrics::{Metrics, SharedRegistry};
use snapchain::proto::snapchain::Block;
use snapchain::storage::store::BlockStore;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::{select, time};
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use snapchain::consensus::consensus::SystemMessage;
use snapchain::core::types::proto;
use snapchain::network::gossip::GossipEvent;
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MySnapchainService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::rpc::snapchain_service_server::SnapchainServiceServer;
use snapchain::storage::db::RocksDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();

    let app_config = snapchain::cfg::load_and_merge_config(args)?;

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

    if app_config.id == 0 {
        return Err("node id must be specified greater than 0".into());
    }
    if app_config.clear_db {
        let db_dir = format!("{}", app_config.rocksdb_dir);
        std::fs::remove_dir_all(db_dir.clone()).unwrap();
        std::fs::create_dir_all(db_dir.clone()).unwrap();
        warn!("Cleared db at {:?}", db_dir);
    }

    let addr = app_config.gossip.address.clone();
    let grpc_addr = app_config.rpc_address.clone();
    let grpc_socket_addr: SocketAddr = grpc_addr.parse()?;
    let db_path = format!("{}/farcaster", app_config.rocksdb_dir);
    let db = Arc::new(RocksDB::new(db_path.clone().as_str()));
    db.open().unwrap();
    let block_store = BlockStore::new(db);

    info!(
        id = app_config.id,
        addr = addr,
        grpc_addr = grpc_addr,
        "SnapchainService listening",
    );

    let keypair = app_config.consensus.keypair().clone();

    info!(
        "Starting Snapchain node with public key: {}",
        hex::encode(keypair.public().to_bytes())
    );

    let (system_tx, mut system_rx) = mpsc::channel::<SystemMessage>(100);

    let gossip_result =
        SnapchainGossip::create(keypair.clone(), app_config.gossip, system_tx.clone());

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

    let registry = SharedRegistry::global();
    // Use the new non-global metrics registry when we upgrade to newer version of malachite
    let _ = Metrics::register(registry);

    let (block_tx, mut block_rx) = mpsc::channel::<Block>(100);

    let write_block_store = block_store.clone();
    tokio::spawn(async move {
        while let Some(block) = block_rx.recv().await {
            match write_block_store.put_block(block) {
                Err(err) => {
                    error!("Unable to put block in db {:#?}", err)
                }
                Ok(()) => {}
            }
        }
    });

    let node = SnapchainNode::create(
        keypair.clone(),
        app_config.consensus.clone(),
        Some(app_config.rpc_address.clone()),
        gossip_tx.clone(),
        block_tx,
        block_store.clone(),
        app_config.rocksdb_dir,
    )
    .await;

    //TODO: don't assume shard
    //TODO: remove/redo unwrap
    let messages_tx = node.messages_tx_by_shard.get(&1u32).unwrap().clone();
    let rpc_shard_stores = node.shard_stores.clone();

    let rpc_block_store = block_store.clone();
    tokio::spawn(async move {
        let service = MySnapchainService::new(rpc_block_store, rpc_shard_stores, messages_tx);

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

    // Create a timer for block creation
    let mut block_interval = time::interval(Duration::from_secs(2));

    let mut tick_count = 0;

    // Kick it off
    loop {
        select! {
            _ = ctrl_c() => {
                info!("Received Ctrl-C, shutting down");
                node.stop();
                return Ok(());
            }
            _ = shutdown_rx.recv() => {
                error!("Received shutdown signal, shutting down");
                node.stop();
                return Ok(());
            }
            _ = block_interval.tick() => {
                tick_count += 1;
                // Every 5 ticks, re-register the validators so that new nodes can discover each other
                if tick_count % 5 == 0 {
                    let nonce = tick_count as u64;
                    for i in 0..=app_config.consensus.num_shards() {
                        let current_height = block_store.max_block_number(i).unwrap_or_else(|_| 0);

                        let register_validator = proto::RegisterValidator {
                            validator: Some(proto::Validator {
                                signer: keypair.public().to_bytes().to_vec(),
                                fid: 0,
                                rpc_address: app_config.rpc_address.clone(),
                                shard_index: i,
                                current_height
                            }),
                            nonce,   // Need the nonce to avoid the gossip duplicate message check
                        };
                        gossip_tx.send(GossipEvent::RegisterValidator(register_validator)).await?;
                    }
                    info!("Registering validator with nonce: {}", nonce);

                }
            }
            Some(msg) = system_rx.recv() => {
                match msg {
                    SystemMessage::Consensus(consensus_msg) => {
                        // Forward to apropriate consesnsus actors
                        node.dispatch(consensus_msg);
                    }
                }
            }
        }
    }
}
