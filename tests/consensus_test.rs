use libp2p::identity::ed25519::Keypair;
use malachite_config::TimeoutConfig;
use malachite_consensus::Params as ConsensusParams;
use malachite_metrics::{Metrics, SharedRegistry};
use ractor::{Actor, ActorRef};
use snapchain::consensus::consensus::{BlockProposer, Decision, ShardValidator, TxDecision};
use snapchain::{
    consensus::consensus::{Consensus, ConsensusMsg},
    core::types::{
        Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
        SnapchainValidatorSet,
    },
    network::gossip::GossipEvent,
};
use tokio::sync::mpsc;
use tokio::{select, time};
use tracing::debug;
use tracing_subscriber::EnvFilter;

struct NodeForTest {
    shard: SnapchainShard,
    keypair: Keypair,
    validator_set: SnapchainValidatorSet,
    gossip_rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    consensus_actor: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
    decision_rx: mpsc::Receiver<Decision<SnapchainValidatorContext>>,
}

impl NodeForTest {
    pub async fn create(
        shard: SnapchainShard,
        keypair: Keypair,
        validator_set: SnapchainValidatorSet,
    ) -> Self {
        let metrics = Metrics::new();

        let address = Address(keypair.public().to_bytes());
        let consensus_params = ConsensusParams {
            start_height: Height::new(shard.shard_id(), 1),
            initial_validator_set: validator_set.clone(),
            address: address.clone(),
            threshold_params: Default::default(),
        };

        // Create validator context
        let ctx = SnapchainValidatorContext::new(keypair.clone());

        let (gossip_tx, gossip_rx) = mpsc::channel::<GossipEvent<SnapchainValidatorContext>>(100);
        let (decision_tx, decision_rx) = mpsc::channel::<Decision<SnapchainValidatorContext>>(100);
        let block_proposer = BlockProposer::new(address.clone(), shard.clone());
        let shard_validator = ShardValidator::new(address.clone(), Some(block_proposer), None);
        // Spawn consensus actor
        let consensus_actor = Consensus::spawn(
            ctx,
            shard.clone(),
            consensus_params,
            TimeoutConfig::default(),
            metrics.clone(),
            Some(decision_tx),
            gossip_tx.clone(),
            shard_validator,
        )
        .await
        .unwrap();
        Self {
            shard,
            keypair,
            validator_set,
            consensus_actor,
            gossip_rx,
            gossip_tx,
            decision_rx,
        }
    }

    pub fn cast(&self, msg: ConsensusMsg<SnapchainValidatorContext>) {
        self.consensus_actor.cast(msg).unwrap()
    }

    pub fn stop(&self) {
        self.consensus_actor.stop(None);
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

    let validator1_address = Address(keypair1.public().to_bytes());

    // Set up shard and validators
    let shard = SnapchainShard::new(0);
    let validator1 = SnapchainValidator::new(shard.clone(), keypair1.public().clone());
    let validator2 = SnapchainValidator::new(shard.clone(), keypair2.public().clone());
    let validator3 = SnapchainValidator::new(shard.clone(), keypair3.public().clone());

    // Create validator set with all validators
    let validator_set = SnapchainValidatorSet::new(vec![
        validator1.clone(),
        validator2.clone(),
        validator3.clone(),
    ]);

    let mut node1 =
        NodeForTest::create(shard.clone(), keypair1.clone(), validator_set.clone()).await;
    let mut node2 =
        NodeForTest::create(shard.clone(), keypair2.clone(), validator_set.clone()).await;
    let mut node3 =
        NodeForTest::create(shard.clone(), keypair3.clone(), validator_set.clone()).await;

    // Register validators
    for validator in validator_set.validators {
        node1.cast(ConsensusMsg::RegisterValidator(validator.clone()));
        node2.cast(ConsensusMsg::RegisterValidator(validator.clone()));
        node3.cast(ConsensusMsg::RegisterValidator(validator.clone()));
    }

    //sleep 2 seconds to wait for validators to register
    tokio::time::sleep(time::Duration::from_secs(2)).await;

    // Kick off consensus
    node1.cast(ConsensusMsg::StartHeight(Height::new(shard.shard_id(), 1)));
    node2.cast(ConsensusMsg::StartHeight(Height::new(shard.shard_id(), 1)));
    node3.cast(ConsensusMsg::StartHeight(Height::new(shard.shard_id(), 1)));

    let mut node1_blocks_count = 0;
    let mut node2_blocks_count = 0;
    let mut node3_blocks_count = 0;

    // Wait for gossip messages with a timeout
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let mut timer = time::interval(tokio::time::Duration::from_secs(1));

    loop {
        select! {
            Some(decision) = node1.decision_rx.recv() => {
                match decision {
                    (height, round, value) => {
                        debug!("Node 1: Decided block at height {}, round {}, value: {}", height, round, value);
                        node1_blocks_count += 1;
                    }
                }
            }
            Some(decision) = node2.decision_rx.recv() => {
                match decision {
                    (height, round, value) => {
                        debug!("Node 2: Decided block at height {}, round {}, value: {}", height, round, value);
                        node2_blocks_count += 1;
                    }
                }
            }
            Some(decision) = node3.decision_rx.recv() => {
                match decision {
                    (height, round, value) => {
                        debug!("Node 3: Decided block at height {}, round {}, value: {}", height, round, value);
                        node3_blocks_count += 1;
                    }
                }
            }

            // Wire up the gossip messages to the other nodes
            // TODO: dedup
            Some(gossip_event) = node1.gossip_rx.recv() => {
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
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
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
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
                        node3.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
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
                        node1.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
                        node2.cast(ConsensusMsg::ReceivedFullProposal(full_proposal.clone()));;
                    }
                    _ => {}}
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
