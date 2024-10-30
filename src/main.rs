mod consensus;
mod core;
mod network;

use clap::Parser;
use futures::stream::StreamExt;
use libp2p::identity::ed25519::Keypair;
use libp2p::{gossipsub, mdns, swarm::SwarmEvent};
use prost::Message;
use std::time::Duration;
use tokio::{select, time};
use tracing_subscriber::EnvFilter;

pub mod snapchain {
    tonic::include_proto!("snapchain");
}

use snapchain::GossipMessage;

use crate::network::gossip::SnapchainGossipEvent;
use network::gossip::SnapchainGossip;

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

    let swarm_result = SnapchainGossip::create(keypair.clone(), addr);
    if let Err(e) = swarm_result {
        println!("Failed to create SnapchainGossip: {:?}", e);
        return Ok(());
    }

    let mut swarm = swarm_result?;

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

                // let gossip_message = GossipMessage {
                //     message: Some(snapchain::gossip_message::Message::Block(new_block.clone())),
                // };
                // let encoded_message = gossip_message.encode_to_vec();
                //
                // if let Err(e) = swarm
                //     .behaviour_mut().gossipsub
                //     .publish(topic.clone(), encoded_message) {
                //     let peers = swarm.behaviour_mut().gossipsub.all_peers().count();
                //     println!("Publish error for new block: {e:?}, connected peers: {peers}");
                // } else {
                //     println!("Published new block with height: {}", new_block.header.unwrap().height.unwrap().block_number);
                // }

                // snapchain_app.apply_block(new_block);
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(SnapchainGossipEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },
                SwarmEvent::Behaviour(SnapchainGossipEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discover peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);

                        // TODO: Remove validator
                    }
                },
                SwarmEvent::Behaviour(SnapchainGossipEvent::Gossipsub(gossipsub::Event::Message {
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
                SwarmEvent::Behaviour(SnapchainGossipEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) =>
                println!(
                        "Peer: {peer_id} subscribed to topic: {topic}",
                    ),
                SwarmEvent::Behaviour(SnapchainGossipEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) =>
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
