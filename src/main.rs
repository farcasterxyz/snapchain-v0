use malachite_metrics::{Metrics, SharedRegistry};
use snapchain::connectors::onchain_events::{L1Client, RealL1Client};
use snapchain::consensus::consensus::SystemMessage;
use snapchain::core::types::proto;
use snapchain::mempool::routing;
use snapchain::network::admin_server::{DbManager, MyAdminService};
use snapchain::network::gossip::GossipEvent;
use snapchain::network::gossip::SnapchainGossip;
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::admin_service_server::AdminServiceServer;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::storage::db::RocksDB;
use snapchain::storage::store::BlockStore;
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use std::error::Error;
use std::net;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::{select, time};
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();

    let app_config = match snapchain::cfg::load_and_merge_config(args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

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

    if app_config.clear_db {
        let db_dir = format!("{}", app_config.rocksdb_dir);
        std::fs::remove_dir_all(db_dir.clone()).unwrap();
        std::fs::create_dir_all(db_dir.clone()).unwrap();
        warn!("Cleared db at {:?}", db_dir);
    }

    if app_config.statsd.prefix == "" {
        // TODO: consider removing this check
        return Err("statsd prefix must be specified in config".into());
    }

    // TODO: parsing to SocketAddr only allows for IPs, DNS names won't work
    let (statsd_host, statsd_port) = match app_config.statsd.addr.parse::<SocketAddr>() {
        Ok(addr) => Ok((addr.ip().to_string(), addr.port())),
        Err(e) => Err(format!("invalid statsd address: {}", e)),
    }?;

    let mut db_manager = DbManager::new(app_config.rocksdb_dir.clone().as_str());
    db_manager.maybe_destroy_databases().unwrap();

    let host = (statsd_host, statsd_port);
    let socket = net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = cadence::UdpMetricSink::from(host, socket)?;
    let statsd_client =
        cadence::StatsdClient::builder(app_config.statsd.prefix.as_str(), sink).build();
    let statsd_client = StatsdClientWrapper::new(statsd_client, app_config.statsd.use_tags);

    let addr = app_config.gossip.address.clone();
    let grpc_addr = app_config.rpc_address.clone();
    let grpc_socket_addr: SocketAddr = grpc_addr.parse()?;
    let db_path = format!("{}/farcaster", app_config.rocksdb_dir);
    let db = Arc::new(RocksDB::new(db_path.clone().as_str()));
    db.open().unwrap();
    let block_store = BlockStore::new(db);

    info!(addr = addr, grpc_addr = grpc_addr, "HubService listening",);

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

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    let registry = SharedRegistry::global();
    // Use the new non-global metrics registry when we upgrade to newer version of malachite
    let _ = Metrics::register(registry);

    let node = SnapchainNode::create(
        keypair.clone(),
        app_config.consensus.clone(),
        app_config.mempool.clone(),
        Some(app_config.rpc_address.clone()),
        gossip_tx.clone(),
        None,
        block_store.clone(),
        app_config.rocksdb_dir.clone(),
        statsd_client.clone(),
        app_config.trie_branching_factor,
    )
    .await;

    let admin_service = MyAdminService::new(
        db_manager,
        node.shard_senders.clone(),
        app_config.consensus.num_shards,
        Box::new(routing::ShardRouter {}),
    );

    if !app_config.onchain_events.rpc_url.is_empty() {
        let mut onchain_events_subscriber = snapchain::connectors::onchain_events::Subscriber::new(
            app_config.onchain_events,
            node.shard_senders.clone(),
            app_config.consensus.num_shards,
            Box::new(routing::ShardRouter {}),
        )?;
        tokio::spawn(async move {
            let result = onchain_events_subscriber.run(false).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("Error subscribing to on chain events {:#?}", e);
                }
            }
        });
    }

    let rpc_shard_stores = node.shard_stores.clone();
    let rpc_shard_senders = node.shard_senders.clone();

    let rpc_block_store = block_store.clone();
    tokio::spawn(async move {
        let l1_client: Option<Box<dyn L1Client>> = match RealL1Client::new(app_config.l1_rpc_url) {
            Ok(client) => Some(Box::new(client)),
            Err(_) => None,
        };
        let service = MyHubService::new(
            rpc_block_store,
            rpc_shard_stores,
            rpc_shard_senders,
            statsd_client.clone(),
            app_config.consensus.num_shards,
            Box::new(routing::ShardRouter {}),
            l1_client,
        );

        let resp = Server::builder()
            .add_service(HubServiceServer::new(service))
            .add_service(AdminServiceServer::new(admin_service))
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

                    let ids = {
                        // prepend 0 to shard_ids for the following loop
                        let mut ids = vec![0u32];
                        ids.extend(&app_config.consensus.shard_ids);
                        ids
                    };

                    for i in ids {
                        let current_height =
                        if i == 0 {
                            block_store.max_block_number().unwrap_or_else(|_| 0)
                        } else {
                            let stores = node.shard_stores.get(&i);
                            match stores {
                                None => 0,
                                Some(stores) => stores.shard_store.max_block_number().unwrap_or_else(|_| 0)
                            }
                        };

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
