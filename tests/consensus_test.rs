use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use hex;
use libp2p::identity::ed25519::Keypair;
use snapchain::network::server::MySnapchainService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::message;
use snapchain::proto::rpc::snapchain_service_server::SnapchainServiceServer;
use snapchain::proto::snapchain::Block;
use snapchain::storage::db::{PageOptions, RocksDB};
use snapchain::storage::store::{get_blocks_in_range, put_block, BlockStore};
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
            None,
            gossip_tx,
            block_tx,
            block_store.clone(),
            make_tmp_path(),
        )
        .await;

        let node_id = node.id();
        let assert_valid_block = move |block: &Block| {
            let header = block.header.as_ref().unwrap();
            let message_count = block.shard_chunks[0].transactions[0].user_messages.len();
            info!(
                hash = hex::encode(&block.hash),
                height = header.height.as_ref().map(|h| h.block_number),
                id = node_id,
                message_count,
                "decided block",
            );
            assert_eq!(block.shard_chunks.len(), num_shards as usize);
        };

        tokio::spawn(async move {
            while let Some(block) = block_rx.recv().await {
                assert_valid_block(&block);
            }
        });

        //TODO: don't assume shard
        //TODO: remove/redo unwrap
        let messages_tx = node.messages_tx_by_shard.get(&1u32).unwrap().clone();

        let grpc_addr = format!("0.0.0.0:{}", grpc_port);
        let addr = grpc_addr.clone();
        let grpc_block_store = block_store.clone();
        let grpc_shard_stores = node.shard_stores.clone();
        tokio::spawn(async move {
            let service = MySnapchainService::new(grpc_block_store, grpc_shard_stores, messages_tx);

            let grpc_socket_addr: SocketAddr = addr.parse().unwrap();
            let resp = Server::builder()
                .add_service(SnapchainServiceServer::new(service))
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
        let mut count = 0;
        for i in 0..self.num_shards {
            let blocks = self.block_store.get_blocks(0, None, i).unwrap();
            count += blocks.len()
        }
        count
    }

    pub async fn total_messages(&self) -> usize {
        let mut count = 0;
        for i in 0..self.num_shards {
            let messages = self
                .block_store
                .get_blocks(0, None, i)
                .unwrap()
                .into_iter()
                .map(|b| b.shard_chunks[0].transactions[0].user_messages.len());
            count += messages.len()
        }

        count
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

    pub async fn produce_blocks(&mut self, num_blocks: u64) {
        for node in self.nodes.iter_mut() {
            node.start_height(1);
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
        .messages_tx_by_shard
        .get(&1u32)
        .expect("message channel should exist")
        .clone();

    tokio::spawn(async move {
        let mut i: i32 = 0;
        let prefix = vec![0, 0, 0, 0, 0, 0];
        loop {
            info!(i, "sending message");

            let mut hash = prefix.clone();
            hash.extend_from_slice(&i.to_be_bytes()); // just for now

            messages_tx1
                .send(message::Message {
                    hash: hash,
                    data: None,
                    data_bytes: None,
                    hash_scheme: message::HashScheme::Blake3 as i32,
                    signature: vec![],
                    signature_scheme: 0,
                    signer: vec![],
                })
                .await
                .unwrap();
            i += 1;
            tokio::time::sleep(time::Duration::from_millis(200)).await;
        }
    });

    network.produce_blocks(3).await;

    assert!(
        network.nodes[0].num_blocks().await >= 3,
        "Node 1 should have confirmed blocks"
    );
    assert!(
        network.nodes[0].total_messages().await > 0,
        "Node 1 should have messages"
    );
    assert!(
        network.nodes[1].num_blocks().await >= 3,
        "Node 2 should have confirmed blocks"
    );
    assert!(
        network.nodes[1].total_messages().await > 0,
        "Node 2 should have messages"
    );
    assert!(
        network.nodes[2].num_blocks().await >= 3,
        "Node 3 should have confirmed blocks"
    );
    assert!(
        network.nodes[2].total_messages().await > 0,
        "Node 3 should have messages"
    );
}

#[tokio::test]
async fn test_basic_block_sync() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    let keypair4 = Keypair::generate();

    // Set up shard and validators

    let num_shards = 1;

    let mut network = TestNetwork::create(3, num_shards, 3200).await;

    network.produce_blocks(3).await;

    let node4 = NodeForTest::create(keypair4.clone(), num_shards, 3207).await;
    node4.register_keypair(keypair4.clone(), format!("0.0.0.0:{}", 3207));
    node4.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
        SnapchainShard::new(0),
        network.nodes[0].keypair.public().clone(),
        Some(network.nodes[0].grpc_addr.clone()),
        network.nodes[0].num_blocks().await as u64,
    )));

    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));
    loop {
        let _ = timer.tick().await;
        if node4.num_blocks().await >= network.nodes[0].num_blocks().await {
            break;
        }
        if start.elapsed() > timeout {
            break;
        }
    }

    assert!(
        network.nodes[0].num_blocks().await >= 3,
        "Node 1 should have confirmed blocks"
    );
    assert!(
        network.nodes[1].num_blocks().await >= 3,
        "Node 2 should have confirmed blocks"
    );
    assert!(
        network.nodes[2].num_blocks().await >= 3,
        "Node 3 should have confirmed blocks"
    );
    assert!(
        node4.num_blocks().await >= network.nodes[0].num_blocks().await,
        "Node 4 should have confirmed blocks"
    );
}
