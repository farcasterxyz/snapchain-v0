use std::net::SocketAddr;
use std::sync::Arc;

use hex;
use libp2p::identity::ed25519::Keypair;
use snapchain::consensus::consensus::BlockStore;
use snapchain::network::server::MySnapchainService;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::message;
use snapchain::proto::rpc::snapchain_service_server::SnapchainServiceServer;
use snapchain::proto::snapchain::Block;
use snapchain::{
    consensus::consensus::ConsensusMsg,
    core::types::{ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext},
    network::gossip::GossipEvent,
};
use tokio::sync::{mpsc, Mutex};
use tokio::{select, time};
use tonic::transport::Server;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

struct NodeForTest {
    keypair: Keypair,
    num_shards: u32,
    node: SnapchainNode,
    gossip_rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    block_store: Arc<Mutex<BlockStore>>,
}

impl NodeForTest {
    pub async fn create(keypair: Keypair, num_shards: u32, grpc_port: u32) -> Self {
        let config = snapchain::consensus::consensus::Config::default();

        let (gossip_tx, gossip_rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(100);

        let (block_tx, mut block_rx) = mpsc::channel::<Block>(100);
        let node =
            SnapchainNode::create(keypair.clone(), config, None, 0, gossip_tx, block_tx).await;

        let block_store = Arc::new(Mutex::new(BlockStore::new()));
        let write_block_store = block_store.clone();
        let assert_valid_block = |block: &Block| {
            let header = block.header.as_ref().unwrap();
            let message_count = block.shard_chunks[0].transactions[0].user_messages.len();
            info!(
                hash = hex::encode(&block.hash),
                height = header.height.as_ref().map(|h| h.block_number),
                message_count,
                "decided block",
            );

            assert_eq!(block.shard_chunks.len(), 1);
        };
        tokio::spawn(async move {
            while let Some(block) = block_rx.recv().await {
                assert_valid_block(&block);
                let mut block_store = write_block_store.lock().await;
                block_store.put_block(block);
            }
        });

        //TODO: don't assume shard
        //TODO: remove/redo unwrap
        let messages_tx = node.shard_messages.get(&1u32).unwrap().clone();

        let rpc_server_block_store = block_store.clone();
        tokio::spawn(async move {
            let service = MySnapchainService::new(rpc_server_block_store, messages_tx);

            let grpc_addr = format!("0.0.0.0:{}", grpc_port);
            let grpc_socket_addr: SocketAddr = grpc_addr.parse().unwrap();
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

    pub async fn num_blocks(&self) -> usize {
        self.block_store.lock().await.get_blocks(0, None).len()
    }

    pub fn stop(&self) {
        self.node.stop();
    }
}

#[tokio::test]
async fn test_basic_consensus() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();

    // Create validator keys
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();

    let num_shards = 1;

    let mut node1 = NodeForTest::create(keypair1.clone(), num_shards, 3381).await;
    let mut node2 = NodeForTest::create(keypair2.clone(), num_shards, 3382).await;
    let mut node3 = NodeForTest::create(keypair3.clone(), num_shards, 3383).await;

    // Register validators
    for keypair in vec![keypair1, keypair2, keypair3] {
        node1.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3381));
        node2.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3382));
        node3.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3383));
    }

    //sleep 2 seconds to wait for validators to register
    tokio::time::sleep(time::Duration::from_secs(2)).await;

    let messages_tx1 = node1
        .node
        .shard_messages
        .get(&1u32)
        .expect("message channel should exist")
        .clone();

    tokio::spawn(async move {
        let mut i: i32 = 0;
        loop {
            info!(i, "sending message");
            messages_tx1
                .send(message::Message {
                    hash: i.to_be_bytes().to_vec(), // just for now
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
            tokio::time::sleep(time::Duration::from_millis(5)).await;
        }
    });

    // Kick off consensus
    node1.start_height(1);
    node2.start_height(1);
    node3.start_height(1);

    // Wait for gossip messages with a timeout
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));

    loop {
        select! {
            // Wire up the gossip messages to the other nodes
            // TODO: dedup
            Some(gossip_event) = node1.recv_gossip_event() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node2.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node3.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            _ = timer.tick() => {
                if node1.num_blocks().await == 3
                    && node2.num_blocks().await == 3
                    && node3.num_blocks().await == 3
                {
                    break;
                }
                if start.elapsed() > timeout {
                    break;
                }
            }
        }
    }

    assert!(
        node1.num_blocks().await >= 3,
        "Node 1 should have confirmed blocks"
    );
    assert!(
        node2.num_blocks().await >= 3,
        "Node 2 should have confirmed blocks"
    );
    assert!(
        node3.num_blocks().await >= 3,
        "Node 3 should have confirmed blocks"
    );

    // Clean up
    node1.stop();
    node2.stop();
    node3.stop();
}

#[tokio::test]
async fn test_basic_block_sync() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .try_init();

    // Create validator keys
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();
    let keypair4 = Keypair::generate();

    // Set up shard and validators

    let num_shards = 1;

    let mut node1 = NodeForTest::create(keypair1.clone(), num_shards, 3384).await;
    let mut node2 = NodeForTest::create(keypair2.clone(), num_shards, 3385).await;
    let mut node3 = NodeForTest::create(keypair3.clone(), num_shards, 3386).await;

    // Register validators
    for keypair in vec![keypair1.clone(), keypair2.clone(), keypair3.clone()] {
        node1.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3384));
        node2.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3385));
        node3.register_keypair(keypair.clone(), format!("0.0.0.0:{}", 3386));
    }

    //sleep 2 seconds to wait for validators to register
    tokio::time::sleep(time::Duration::from_secs(2)).await;

    // Kick off consensus
    node1.start_height(1);
    node2.start_height(1);
    node3.start_height(1);

    // Wait for gossip messages with a timeout
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));

    loop {
        // Separate select because we can't borrow node twice
        select! {
            // Wire up the gossip messages to the other nodes
            // TODO: dedup
            Some(gossip_event) = node1.recv_gossip_event() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node2.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node3.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            Some(gossip_event) = node3.gossip_rx.recv() => {
                match gossip_event {
                    GossipEvent::BroadcastSignedProposal(proposal) => {
                        node1.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedProposal(proposal.clone()));
                    },
                    GossipEvent::BroadcastSignedVote(vote) => {
                        node1.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                        node2.cast(ConsensusMsg::ReceivedSignedVote(vote.clone()));
                    }
                    GossipEvent::BroadcastFullProposal(full_proposal) => {
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));
                    }
                    _ => {}}
            }
            _ = timer.tick() => {
                if start.elapsed() > timeout {
                    break;
                }
            }
        }
    }

    let node4 = NodeForTest::create(keypair4.clone(), num_shards, 3387).await;
    node4.register_keypair(keypair4.clone(), format!("0.0.0.0:{}", 3387));
    node4.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
        SnapchainShard::new(0),
        keypair1.public().clone(),
        Some(format!("127.0.0.1:{}", 3384)),
        node1.num_blocks().await as u64,
    )));

    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));
    loop {
        let _ = timer.tick().await;
        if node4.num_blocks().await >= node1.num_blocks().await {
            break;
        }
        if start.elapsed() > timeout {
            break;
        }
    }

    assert!(
        node1.num_blocks().await >= 3,
        "Node 1 should have confirmed blocks"
    );
    assert!(
        node2.num_blocks().await >= 3,
        "Node 2 should have confirmed blocks"
    );
    assert!(
        node3.num_blocks().await >= 3,
        "Node 3 should have confirmed blocks"
    );
    assert!(
        node4.num_blocks().await >= node1.num_blocks().await,
        "Node 4 should have confirmed blocks"
    );

    // Clean up
    node1.stop();
    node2.stop();
    node3.stop();
    node4.stop();
}
