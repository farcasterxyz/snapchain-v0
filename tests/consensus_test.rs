use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use hex;
use libp2p::identity::ed25519::Keypair;
use snapchain::mempool::{mempool, routing};
use snapchain::network::server::MyHubService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::hub_service_server::HubServiceServer;
use snapchain::proto::Block;
use snapchain::storage::db::{PageOptions, RocksDB};
use snapchain::storage::store::BlockStore;
use snapchain::utils::factory::messages_factory;
use snapchain::utils::statsd_wrapper::StatsdClientWrapper;
use snapchain::{
    consensus::consensus::ConsensusMsg,
    core::types::{ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext},
    network::gossip::GossipEvent,
};
use tokio::sync::mpsc;
use tokio::time;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

struct NodeForTest {
    keypair: Keypair,
    num_shards: u32,
    node: SnapchainNode,
    gossip_rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    grpc_addr: String,
    db: Arc<RocksDB>,
    block_store: BlockStore,
}

impl Drop for NodeForTest {
    fn drop(&mut self) {
        self.db.destroy().unwrap();
        for (_, stores) in self.node.shard_stores.iter_mut() {
            stores.shard_store.db.destroy().unwrap();
        }
        self.node.stop();
    }
}

fn make_tmp_path() -> String {
    tempfile::tempdir()
        .unwrap()
        .path()
        .as_os_str()
        .to_string_lossy()
        .to_string()
}

impl NodeForTest {
    pub async fn create(keypair: Keypair, num_shards: u32, grpc_port: u32) -> Self {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let mut config = snapchain::consensus::consensus::Config::default();
        config = config.with_shard_ids((1..=num_shards).collect());

        let (gossip_tx, gossip_rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(100);

        let (block_tx, mut block_rx) = mpsc::channel::<Block>(100);
        let db = Arc::new(RocksDB::new(&make_tmp_path()));
        db.open().unwrap();
        let block_store = BlockStore::new(db.clone());
        let node = SnapchainNode::create(
            keypair.clone(),
            config,
            mempool::Config::default(),
            None,
            gossip_tx,
            Some(block_tx),
            block_store.clone(),
            make_tmp_path(),
            statsd_client.clone(),
            16,
        )
        .await;

        let node_id = node.id();
        let assert_valid_block = move |block: &Block| {
            let header = block.header.as_ref().unwrap();
            let transactions_count: usize = block
                .shard_chunks
                .iter()
                .map(|c| c.transactions.len())
                .sum();
            info!(
                hash = hex::encode(&block.hash),
                height = header.height.as_ref().map(|h| h.block_number),
                id = node_id,
                transactions = transactions_count,
                "decided block",
            );
            assert_eq!(block.shard_chunks.len(), num_shards as usize);
        };

        tokio::spawn(async move {
            while let Some(block) = block_rx.recv().await {
                assert_valid_block(&block);
            }
        });

        let grpc_addr = format!("0.0.0.0:{}", grpc_port);
        let addr = grpc_addr.clone();
        let grpc_block_store = block_store.clone();
        let grpc_shard_stores = node.shard_stores.clone();
        let grpc_shard_senders = node.shard_senders.clone();
        tokio::spawn(async move {
            let service = MyHubService::new(
                grpc_block_store,
                grpc_shard_stores,
                grpc_shard_senders,
                statsd_client.clone(),
                num_shards,
                Box::new(routing::EvenOddRouterForTest {}),
                None,
            );

            let grpc_socket_addr: SocketAddr = addr.parse().unwrap();
            let resp = Server::builder()
                .add_service(HubServiceServer::new(service))
                .serve(grpc_socket_addr)
                .await;

            let msg = "grpc server stopped";
            match resp {
                Ok(()) => error!(msg),
                Err(e) => error!(error = ?e, "{}", msg),
            }
        });

        Self {
            keypair,
            num_shards,
            node,
            gossip_rx,
            grpc_addr: grpc_addr.clone(),
            db: db.clone(),
            block_store,
        }
    }

    #[allow(dead_code)] // TODO
    pub async fn recv_gossip_event(&mut self) -> Option<GossipEvent<SnapchainValidatorContext>> {
        self.gossip_rx.recv().await
    }

    pub fn cast(&self, msg: ConsensusMsg<SnapchainValidatorContext>) {
        self.node.dispatch(msg)
    }

    pub fn start_height(&self, block_number: u64) {
        self.node.start_height(block_number);
    }

    pub fn register_keypair(&self, keypair: Keypair, rpc_address: String) {
        for i in 0..=self.num_shards {
            self.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
                SnapchainShard::new(i),
                keypair.public().clone(),
                Some(rpc_address.clone()),
                0,
            )));
        }
    }

    pub fn id(&self) -> String {
        self.node.id()
    }

    pub async fn num_blocks(&self) -> usize {
        let blocks_page = self
            .block_store
            .get_blocks(0, None, &PageOptions::default())
            .unwrap();
        blocks_page.blocks.len()
    }

    pub async fn num_shard_chunks(&self) -> usize {
        let mut count = 0;
        for (_shard_id, stores) in self.node.shard_stores.iter() {
            count += stores.shard_store.get_shard_chunks(0, None).unwrap().len();
        }

        count
    }

    pub async fn total_messages(&self) -> usize {
        let messages = self
            .block_store
            .get_blocks(0, None, &PageOptions::default())
            .unwrap()
            .blocks
            .into_iter()
            .map(|b| b.shard_chunks[0].transactions[0].user_messages.len());

        messages.len()
    }
}

pub struct TestNetwork {
    nodes: Vec<NodeForTest>,
}

impl TestNetwork {
    // These networks can be created in parallel, so make sure the base port is far enough part to avoid conflicts
    pub async fn create(num_nodes: u32, num_shards: u32, base_grpc_port: u32) -> Self {
        let mut nodes = Vec::new();
        let mut keypairs = Vec::new();
        for i in 0..num_nodes {
            let keypair = Keypair::generate();
            keypairs.push(keypair.clone());
            let node = NodeForTest::create(keypair, num_shards, base_grpc_port + i).await;
            nodes.push(node);
        }

        // Register validators
        for i in 0..num_nodes {
            for keypair in keypairs.iter() {
                nodes[i as usize]
                    .register_keypair(keypair.clone(), format!("0.0.0.0:{}", base_grpc_port + i));
            }
        }
        // Wait for the RegisterValidator message to be processed
        tokio::time::sleep(time::Duration::from_millis(200)).await;

        Self { nodes }
    }

    fn add_node(&mut self, new_node: NodeForTest) {
        for node in self.nodes.iter() {
            new_node.register_keypair(node.keypair.clone(), node.grpc_addr.clone());
            node.register_keypair(new_node.keypair.clone(), new_node.grpc_addr.clone());
        }
        self.nodes.push(new_node)
    }

    pub async fn produce_blocks(&mut self, num_blocks: u64) {
        for node in self.nodes.iter_mut() {
            node.start_height(node.num_blocks().await as u64 + 1);
        }

        let timeout = tokio::time::Duration::from_secs(5);
        let start = tokio::time::Instant::now();
        let mut timer = time::interval(tokio::time::Duration::from_millis(10));

        let num_nodes = self.nodes.len();

        let mut node_ids_with_blocks = BTreeSet::new();
        loop {
            let _ = timer.tick().await;
            for node in self.nodes.iter_mut() {
                if node.num_blocks().await >= num_blocks as usize {
                    node_ids_with_blocks.insert(node.id());
                    if node_ids_with_blocks.len() == num_nodes {
                        break;
                    }
                }
            }

            // Loop through each node, and select all other nodes to send gossip messages
            for i in 0..self.nodes.len() {
                if let Ok(gossip_event) = self.nodes[i].gossip_rx.try_recv() {
                    match gossip_event {
                        GossipEvent::BroadcastSignedProposal(proposal) => {
                            self.dispatch_to_other_nodes(
                                i,
                                ConsensusMsg::ReceivedSignedProposal(proposal.clone()),
                            );
                        }
                        GossipEvent::BroadcastSignedVote(vote) => {
                            self.dispatch_to_other_nodes(
                                i,
                                ConsensusMsg::ReceivedSignedVote(vote.clone()),
                            );
                        }
                        GossipEvent::BroadcastFullProposal(full_proposal) => {
                            self.dispatch_to_other_nodes(
                                i,
                                ConsensusMsg::ReceivedFullProposal(full_proposal.clone()),
                            );
                        }
                        _ => {}
                    }
                }
            }

            if start.elapsed() > timeout {
                break;
            }
        }
    }

    fn dispatch_to_other_nodes(&self, i: usize, msg: ConsensusMsg<SnapchainValidatorContext>) {
        for j in 0..self.nodes.len() {
            if i != j {
                self.nodes[j].cast(msg.clone());
            }
        }
    }
}

#[tokio::test]
async fn test_basic_consensus() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let num_shards = 2;
    let mut network = TestNetwork::create(3, num_shards, 3380).await;

    let messages_tx1 = network.nodes[0]
        .node
        .shard_senders
        .get(&1u32)
        .expect("message channel should exist")
        .messages_tx
        .clone();

    tokio::spawn(async move {
        let mut i: i32 = 0;
        let prefix = vec![0, 0, 0, 0, 0, 0];
        loop {
            info!(i, "sending message");

            let mut hash = prefix.clone();
            hash.extend_from_slice(&i.to_be_bytes()); // just for now

            messages_tx1
                .send(
                    snapchain::storage::store::engine::MempoolMessage::UserMessage(
                        messages_factory::casts::create_cast_add(
                            321,
                            format!("Cast {}", i).as_str(),
                            None,
                            None,
                        ),
                    ),
                )
                .await
                .unwrap();
            i += 1;
            tokio::time::sleep(time::Duration::from_millis(200)).await;
        }
    });

    network.produce_blocks(3).await;

    for i in 0..network.nodes.len() {
        assert!(
            network.nodes[i].num_blocks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );

        assert!(
            network.nodes[i].num_shard_chunks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );

        assert!(
            network.nodes[i].total_messages().await > 0,
            "Node {} should have messages",
            i
        );
    }
}

async fn wait_for_blocks(new_node: &NodeForTest, old_node: &NodeForTest) {
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));
    loop {
        let _ = timer.tick().await;
        if new_node.num_blocks().await >= old_node.num_blocks().await {
            break;
        }
        if start.elapsed() > timeout {
            break;
        }
    }

    assert!(
        new_node.num_blocks().await >= old_node.num_blocks().await,
        "Node 4 should have confirmed blocks"
    );
    assert!(
        new_node.num_shard_chunks().await >= old_node.num_shard_chunks().await,
        "Node 4 should have confirmed shard chunks"
    );
}

#[tokio::test]
async fn test_basic_sync() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let keypair4 = Keypair::generate();

    // Set up shard and validators

    let num_shards = 1;

    let mut network = TestNetwork::create(3, num_shards, 3220).await;

    network.produce_blocks(3).await;

    for i in 0..network.nodes.len() {
        assert!(
            network.nodes[i].num_blocks().await >= 3,
            "Node {} should have confirmed blocks",
            i
        );
    }

    let node4 = NodeForTest::create(keypair4.clone(), num_shards, 3227).await;
    node4.register_keypair(keypair4.clone(), format!("0.0.0.0:{}", 3227));
    node4.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
        SnapchainShard::new(0),
        network.nodes[0].keypair.public().clone(),
        Some(network.nodes[0].grpc_addr.clone()),
        network.nodes[0].num_blocks().await as u64,
    )));
    node4.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
        SnapchainShard::new(1),
        network.nodes[0].keypair.public().clone(),
        Some(network.nodes[0].grpc_addr.clone()),
        network.nodes[0].num_shard_chunks().await as u64,
    )));

    // Node 4 won't see these blocks directly.
    network.produce_blocks(3).await;

    network.add_node(node4);

    // Node 4 picks up the blocks it missed on the first proposals from each validator.
    network.produce_blocks(1).await;

    wait_for_blocks(&network.nodes[3], &network.nodes[0]).await;
}
