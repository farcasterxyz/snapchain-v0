use libp2p::identity::ed25519::Keypair;
use malachite_metrics::{Metrics, SharedRegistry};
use ractor::ActorRef;
use snapchain::consensus::consensus::{Decision, ShardProposer, ShardValidator, TxDecision};
use snapchain::core::types::proto;
use snapchain::node::snapchain_node::SnapchainNode;
use snapchain::proto::snapchain::Block;
use snapchain::{
    consensus::consensus::ConsensusMsg,
    core::types::{
        Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
        SnapchainValidatorSet,
    },
    network::gossip::GossipEvent,
};
use tokio::sync::mpsc;
use tokio::{select, time};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

struct NodeForTest {
    keypair: Keypair,
    num_shards: u32,
    node: SnapchainNode,
    gossip_rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    block_rx: mpsc::Receiver<Block>,
}

impl NodeForTest {
    pub async fn create(keypair: Keypair, num_shards: u32) -> Self {
        let config = snapchain::consensus::consensus::Config::default();

        let (gossip_tx, gossip_rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(100);
        let (block_tx, block_rx) = mpsc::channel::<Block>(100);
        let node =
            SnapchainNode::create(keypair.clone(), config, None, 0, gossip_tx, vec![block_tx])
                .await;

        Self {
            keypair,
            num_shards,
            node,
            gossip_rx,
            block_rx,
        }
    }

    pub async fn recv_block_decision(&mut self) -> Option<Block> {
        self.block_rx.recv().await
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

    pub fn register_keypair(&self, keypair: Keypair) {
        for i in 0..=self.num_shards {
            self.cast(ConsensusMsg::RegisterValidator(SnapchainValidator::new(
                SnapchainShard::new(i),
                keypair.public().clone(),
                None,
                0,
            )));
        }
    }

    pub fn stop(&self) {
        self.node.stop();
    }
}

#[tokio::test]
async fn test_basic_consensus() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    // Create validator keys
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();
    let keypair3 = Keypair::generate();

    // Set up shard and validators
    let shard = SnapchainShard::new(0);
    let validator1 = SnapchainValidator::new(shard.clone(), keypair1.public().clone(), None, 0);
    let validator2 = SnapchainValidator::new(shard.clone(), keypair2.public().clone(), None, 0);
    let validator3 = SnapchainValidator::new(shard.clone(), keypair3.public().clone(), None, 0);

    // Create validator set with all validators
    let validator_set = SnapchainValidatorSet::new(vec![
        validator1.clone(),
        validator2.clone(),
        validator3.clone(),
    ]);
    let num_shards = 1;

    let mut node1 = NodeForTest::create(keypair1.clone(), num_shards).await;
    let mut node2 = NodeForTest::create(keypair2.clone(), num_shards).await;
    let mut node3 = NodeForTest::create(keypair3.clone(), num_shards).await;

    // Register validators
    for keypair in vec![keypair1, keypair2, keypair3] {
        node1.register_keypair(keypair.clone());
        node2.register_keypair(keypair.clone());
        node3.register_keypair(keypair.clone());
    }

    //sleep 2 seconds to wait for validators to register
    tokio::time::sleep(time::Duration::from_secs(2)).await;

    // Kick off consensus
    node1.start_height(1);
    node2.start_height(1);
    node3.start_height(1);

    let mut node1_blocks_count = 0;
    let mut node2_blocks_count = 0;
    let mut node3_blocks_count = 0;

    // Wait for gossip messages with a timeout
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_millis(10));

    // create a lambda function to assert on the proposal
    let assert_valid_block = |block: &Block| {
        let header = block.header.as_ref().unwrap();
        debug!(
            "Decided block at height {:#?}, value: {:#?}",
            header.height, block.hash
        );
        assert_eq!(block.shard_chunks.len(), 1);
    };

    loop {
        select! {
            Some(block) = node1.recv_block_decision() => {
                assert_valid_block(&block);
                node1_blocks_count += 1;
            }
            Some(block) = node2.recv_block_decision() => {
                assert_valid_block(&block);
                node2_blocks_count += 1;
            }
            Some(block) = node3.recv_block_decision() => {
                assert_valid_block(&block);
                node3_blocks_count += 1;
            }

            _ = timer.tick() => {
                if node1_blocks_count == 3 && node2_blocks_count == 3 && node3_blocks_count == 3 {
                    break;
                }
                if start.elapsed() > timeout {
                    break;
                }
            }
        }

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

    assert!(
        node1_blocks_count >= 3,
        "Node 1 should have confirmed blocks"
    );
    assert!(
        node2_blocks_count >= 3,
        "Node 2 should have confirmed blocks"
    );
    assert!(
        node3_blocks_count >= 3,
        "Node 3 should have confirmed blocks"
    );

    // Clean up
    node1.stop();
    node2.stop();
    node3.stop();
}
