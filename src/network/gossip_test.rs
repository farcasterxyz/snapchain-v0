use crate::consensus::consensus::{ConsensusMsg, SystemMessage};
use crate::core::types::proto;
use crate::network::gossip::{Config, GossipEvent, SnapchainGossip};
use libp2p::identity::ed25519::Keypair;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

const HOST_FOR_TEST: &str = "127.0.0.1";
const PORT_FOR_TEST: u32 = 9382;

#[tokio::test]
async fn test_gossip_communication() {
    // Create two keypairs for our test nodes
    let keypair1 = Keypair::generate();
    let keypair2 = Keypair::generate();

    // Create configs with different ports
    let node1_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{PORT_FOR_TEST}/quic-v1");
    let node2_port = PORT_FOR_TEST + 1;
    let node2_addr = format!("/ip4/{HOST_FOR_TEST}/udp/{node2_port}/quic-v1");
    let config1 = Config::new(node1_addr.clone(), node2_addr.clone());
    let config2 = Config::new(node2_addr.clone(), node1_addr.clone());

    // Create channels for system messages
    let (system_tx1, _) = mpsc::channel::<SystemMessage>(100);
    let (system_tx2, mut system_rx2) = mpsc::channel::<SystemMessage>(100);

    // Create gossip instances
    let mut gossip1 = SnapchainGossip::create(keypair1.clone(), config1, system_tx1).unwrap();
    let mut gossip2 = SnapchainGossip::create(keypair2.clone(), config2, system_tx2).unwrap();

    let gossip_tx1 = gossip1.tx.clone();

    // Spawn gossip tasks
    tokio::spawn(async move {
        gossip1.start().await;
    });
    tokio::spawn(async move {
        gossip2.start().await;
    });

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a random 1.5mb string to ensure sending a large message works
    let data = "a".repeat(1_500_000);

    // Create a test message
    let test_validator = proto::Validator {
        signer: keypair1.public().to_bytes().to_vec(),
        fid: 123,
        rpc_address: data,
        shard_index: 0,
        current_height: 312,
    };
    let register_msg = proto::RegisterValidator {
        validator: Some(test_validator),
        nonce: 1,
    };

    // Send message from node1 to node2
    gossip_tx1
        .send(GossipEvent::RegisterValidator(register_msg))
        .await
        .unwrap();

    // Wait for message to be received with timeout
    let received = timeout(Duration::from_secs(5), system_rx2.recv()).await;
    assert!(received.is_ok(), "Timed out waiting for message");

    if let Ok(Some(SystemMessage::Consensus(ConsensusMsg::RegisterValidator(validator)))) = received
    {
        assert_eq!(validator.current_height, 312);
    } else {
        panic!("Received unexpected or no message");
    }
}
