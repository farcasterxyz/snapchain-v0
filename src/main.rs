mod consensus;
mod core;

use clap::Parser;
use futures::stream::StreamExt;
use libp2p::identity::ed25519::{Keypair, PublicKey};
use libp2p::{
    gossipsub, mdns, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, PeerId,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::Duration;
use tokio::{io, io::AsyncBufReadExt, select, time};
use tracing_subscriber::EnvFilter;

pub mod snapchain {
    tonic::include_proto!("snapchain");
}

use crate::snapchain::Transaction;
use snapchain::{
    Block, GossipMessage, RegisterValidator, ShardChunk, UserMessage, Validator, Vote,
};

#[derive(Debug, Clone)]
struct SnapchainState {
    blocks: Vec<Block>,
}

#[derive(Debug)]
struct SnapchainApp {
    id: u32,
    peer_id: String,
    state: SnapchainState,
    mempool: Vec<UserMessage>,
    validators: HashMap<u32, String>,
    keypair: Keypair,
}

impl ShardChunk {
    pub fn sign(&self, keypair: &Keypair) -> Vec<u8> {
        let encoded = self.encode_to_vec();
        keypair.sign(&encoded)
    }
}

impl SnapchainApp {
    fn new(id: u32, peer_id: String, keypair: Keypair) -> Self {
        let mut validators = HashMap::new();
        let public_key = hex::encode(&keypair.public().to_bytes());
        validators.insert(id, public_key); // Add self as validator
        SnapchainApp {
            id,
            peer_id,
            keypair,
            state: SnapchainState { blocks: vec![] },
            mempool: vec![],
            validators,
        }
    }

    pub fn is_leader(&self) -> bool {
        // Don't start leader election (and block production) until we have 3 validators
        if self.validators.len() < 3 {
            return false;
        }

        // TODO: Check timestamp of last block and don't propose a block if it's too soon

        // Simple round-robin leader election, based on height, trusting the ids
        // TODO: Use leader schedule in the starting block of an epcoh
        if self.height() % self.validators.len() as u64 == self.id as u64 {
            return true;
        }

        false
    }

    fn add_transaction(&mut self, user_message: UserMessage) {
        self.mempool.push(user_message);
    }

    fn height(&self) -> u64 {
        self.state.blocks.len() as u64
    }

    fn create_block(&mut self) -> Block {
        Block {
            header: None,
            votes: None,
            validators: None,
            hash: vec![],
            shard_chunks: vec![],
        }
    }

    fn apply_block(&mut self, block: Block) -> bool {
        true
    }

    fn register_validator(&mut self, id: u32, address: String) -> bool {
        if !self.validators.contains_key(&id) {
            self.validators.insert(id, address);
            println!("Registered validator with ID: {}", id);
            true
        } else {
            false
        }
    }
}

#[derive(NetworkBehaviour)]
struct SnapchainBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
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

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let keypair = Keypair::generate();

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone().into())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()
                .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(SnapchainBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let mut snapchain_app = SnapchainApp::new(args.id, swarm.local_peer_id().to_string(), keypair);

    // Create a Gossipsub topic
    let topic = gossipsub::IdentTopic::new("test-net");
    // subscribes to our topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // Listen on all assigned port for this id
    swarm.listen_on(addr.parse()?)?;

    // Create a timer for block creation
    let mut block_interval = time::interval(Duration::from_secs(2));

    let mut tick_count = 0;

    // Kick it off
    loop {
        select! {
            _ = block_interval.tick() => {
                tick_count += 1;
                // Every 5 ticks, re-register the validators so that new nodes can discover each other
                if tick_count % 5 == 0 {
                    // let register_validator = RegisterValidator {
                    //     id: snapchain_app.id,
                    //     address: hex::encode(&snapchain_app.keypair.public().to_bytes()),
                    //     nonce: tick_count as u64,   // Need the nonce to avoid the gossip duplicate message check
                    // };
                    // let gossip_message = GossipMessage {
                    //     message: Some(snapchain::gossip_message::Message::Validator(register_validator)),
                    // };
                    // let encoded_message = gossip_message.encode_to_vec();
                    //
                    // if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), encoded_message) {
                    //     println!("Failed to publish RegisterValidator message: {:?}", e);
                    // } else {
                    //     // println!("Published RegisterValidator message");
                    // }
                }

                if !snapchain_app.is_leader() {
                    continue;
                }

                let new_block = snapchain_app.create_block();
                let gossip_message = GossipMessage {
                    message: Some(snapchain::gossip_message::Message::Block(new_block.clone())),
                };
                let encoded_message = gossip_message.encode_to_vec();

                if let Err(e) = swarm
                    .behaviour_mut().gossipsub
                    .publish(topic.clone(), encoded_message) {
                    let peers = swarm.behaviour_mut().gossipsub.all_peers().count();
                    println!("Publish error for new block: {e:?}, connected peers: {peers}");
                } else {
                    println!("Published new block with height: {}", new_block.header.unwrap().height.unwrap().block_number);
                }

                // snapchain_app.apply_block(new_block);
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(SnapchainBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(SnapchainBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discover peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);

                        // TODO: Remove validator
                    }
                },
                SwarmEvent::Behaviour(SnapchainBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    match GossipMessage::decode(&message.data[..]) {
                        Ok(gossip_message) => {
                            match gossip_message.message {
                                Some(snapchain::gossip_message::Message::Block(block)) => {
                                    let height = block.header.unwrap().height.unwrap().block_number;
                                    println!("Received block with height {} from peer: {}", height, peer_id);
                                    // snapchain_app.apply_block(block);
                                },
                                Some(snapchain::gossip_message::Message::Shard(shard)) => {
                                    println!("Received shard with height {} from peer: {}", shard.header.unwrap().height.unwrap().block_number, peer_id);
                                    // Handle shard
                                },
                                Some(snapchain::gossip_message::Message::Validator(validator)) => {
                                    // println!("Received validator registration from peer: {}", peer_id);
                                    // snapchain_app.register_validator(validator.id, validator.address);
                                },
                                _ => println!("Unhandled message from peer: {}", peer_id),
                                None => println!("Received empty gossip message from peer: {}", peer_id),
                            }
                        },
                        Err(e) => println!("Failed to decode gossip message: {}", e),
                    }
                },
                SwarmEvent::Behaviour(SnapchainBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) =>
                println!(
                        "Peer: {peer_id} subscribed to topic: {topic}",
                    ),
                SwarmEvent::Behaviour(SnapchainBehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) =>
                println!(
                        "Peer: {peer_id} unsubscribed to topic: {topic}",
                    ),
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                }
                _ => {}
            }
        }
    }
}
