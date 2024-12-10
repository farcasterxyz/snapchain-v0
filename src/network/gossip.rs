use crate::consensus::consensus::{ConsensusMsg, SystemMessage};
use crate::core::types::{
    proto, Proposal, ShardId, Signature, SnapchainContext, SnapchainShard, SnapchainValidator,
    SnapchainValidatorContext, Vote,
};
use futures::StreamExt;
use libp2p::identity::ed25519::Keypair;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::{gossipsub, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, Swarm};
use malachite_common::{SignedProposal, SignedVote};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

const DEFAULT_GOSSIP_PORT: u16 = 3382;
const DEFAULT_GOSSIP_HOST: &str = "127.0.0.1";
const MAX_GOSSIP_MESSAGE_SIZE: usize = 1024 * 1024 * 10; // 10 mb

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub address: String,
    pub bootstrap_peers: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            address: format!(
                "/ip4/{}/udp/{}/quic-v1",
                DEFAULT_GOSSIP_HOST, DEFAULT_GOSSIP_PORT
            ),
            bootstrap_peers: "".to_string(),
        }
    }
}

impl Config {
    pub fn new(address: String, bootstrap_peers: String) -> Self {
        Config {
            address,
            bootstrap_peers,
        }
    }

    pub fn bootstrap_addrs(&self) -> Vec<String> {
        self.bootstrap_peers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }
}

pub enum GossipEvent<Ctx: SnapchainContext> {
    BroadcastSignedVote(SignedVote<Ctx>),
    BroadcastSignedProposal(SignedProposal<Ctx>),
    BroadcastFullProposal(proto::FullProposal),
    RegisterValidator(proto::RegisterValidator),
}

#[derive(NetworkBehaviour)]
pub struct SnapchainBehavior {
    pub gossipsub: gossipsub::Behaviour,
}

pub struct SnapchainGossip {
    pub swarm: Swarm<SnapchainBehavior>,
    pub tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    rx: mpsc::Receiver<GossipEvent<SnapchainValidatorContext>>,
    system_tx: Sender<SystemMessage>,
}

impl SnapchainGossip {
    pub fn create(
        keypair: Keypair,
        config: Config,
        system_tx: Sender<SystemMessage>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
                    .max_transmit_size(MAX_GOSSIP_MESSAGE_SIZE) // maximum message size that can be transmitted
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                Ok(SnapchainBehavior { gossipsub })
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        for addr in config.bootstrap_addrs() {
            info!("Processing bootstrap peer: {:?}", addr);
            let parsed_addr: libp2p::Multiaddr = addr.parse()?;
            let opts = DialOpts::unknown_peer_id()
                .address(parsed_addr.clone())
                .build();
            info!("Dialing bootstrap peer: {:?} ({:?})", parsed_addr, addr);
            let res = swarm.dial(opts);
            if let Err(e) = res {
                warn!(
                    "Failed to dial bootstrap peer {:?}: {:?}",
                    parsed_addr.clone(),
                    e
                );
            }
        }

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // subscribes to our topic
        let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
        if let Err(e) = result {
            warn!("Failed to subscribe to topic: {:?}", e);
            return Err(Box::new(e));
        }

        // Listen on all assigned port for this id
        swarm.listen_on(config.address.parse()?)?;

        let (tx, rx) = mpsc::channel(100);
        Ok(SnapchainGossip {
            swarm,
            tx,
            rx,
            system_tx,
        })
    }

    pub async fn start(self: &mut Self) {
        loop {
            tokio::select! {
                gossip_event = self.swarm.select_next_some() => {
                    match gossip_event {
                        SwarmEvent::ConnectionEstablished {peer_id, ..} => {
                            info!("Connection established with peer: {peer_id}");
                        },
                        SwarmEvent::ConnectionClosed {peer_id, cause, ..} => {
                            info!("Connection closed with peer: {:?} due to: {:?}", peer_id, cause);
                            // TODO: Remove validator
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} subscribed to topic: {topic}"),
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic })) =>
                            info!("Peer: {peer_id} unsubscribed to topic: {topic}"),
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(address = address.to_string(), "Local node is listening");
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source: peer_id,
                            message_id: _id,
                            message,
                        })) => {
                            match proto::GossipMessage::decode(&message.data[..]) {
                                Ok(gossip_message) => {
                                    match gossip_message.gossip_message{
                                        Some(proto::gossip_message::GossipMessage::FullProposal(full_proposal)) => {
                                            let height = full_proposal.height();
                                            debug!("Received block with height {} from peer: {}", height, peer_id);
                                            let consensus_message = ConsensusMsg::ReceivedFullProposal(full_proposal);
                                            let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                            if let Err(e) = res {
                                                warn!("Failed to send system block message: {:?}", e);
                                            }
                                        },
                                        Some(proto::gossip_message::GossipMessage::Validator(validator)) => {
                                            debug!("Received validator registration from peer: {}", peer_id);
                                            if let Some(validator) = validator.validator {
                                                let public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&validator.signer);
                                                if public_key.is_err() {
                                                    warn!("Failed to decode public key from peer: {}", peer_id);
                                                    continue;
                                                }
                                                let rpc_address = validator.rpc_address;
                                                let shard_index = validator.shard_index;
                                                let validator = SnapchainValidator::new(SnapchainShard::new(shard_index), public_key.unwrap(), Some(rpc_address), validator.current_height);
                                                let consensus_message = ConsensusMsg::RegisterValidator(validator);
                                                let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                                if let Err(e) = res {
                                                    warn!("Failed to send system register validator message: {:?}", e);
                                                }
                                            }
                                        },
                                        Some(proto::gossip_message::GossipMessage::Consensus(signed_consensus_msg)) => {
                                            match signed_consensus_msg.consensus_message{
                                                Some(proto::consensus_message::ConsensusMessage::Vote(vote)) => {
                                                    let vote = Vote::from_proto(vote);
                                                    let signed_vote = SignedVote {
                                                        message: vote,
                                                        signature: Signature(signed_consensus_msg.signature),
                                                    };
                                                    let consensus_message = ConsensusMsg::ReceivedSignedVote(signed_vote);
                                                    let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                                    if let Err(e) = res {
                                                        warn!("Failed to send system vote message: {:?}", e);
                                                    }
                                                },
                                                Some(proto::consensus_message::ConsensusMessage::Proposal(proposal)) => {
                                                    let proposal = Proposal::from_proto(proposal);
                                                    let signed_proposal = SignedProposal {
                                                        message: proposal,
                                                        signature: Signature(signed_consensus_msg.signature),
                                                    };
                                                    let consensus_message = ConsensusMsg::ReceivedSignedProposal(signed_proposal);
                                                    let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                                    if let Err(e) = res {
                                                        warn!("Failed to send system proposal message: {:?}", e);
                                                    }
                                                },
                                                None => warn!("Received empty consensus message from peer: {}", peer_id),
                                            }

                                        }
                                        _ => warn!("Unhandled message from peer: {}", peer_id),
                                    }
                                },
                                Err(e) => warn!("Failed to decode gossip message: {}", e),
                            }
                        },
                        _ => {}
                    }
                }
                event = self.rx.recv() => {
                    match event {
                        Some(GossipEvent::BroadcastSignedVote(vote)) => {
                            let vote_proto = vote.to_proto();
                            let gossip_message = proto::GossipMessage {
                                gossip_message: Some(proto::gossip_message::GossipMessage::Consensus(proto::ConsensusMessage {
                                    signature: vote.signature.0,
                                    consensus_message: Some(proto::consensus_message::ConsensusMessage::Vote(vote_proto)),
                                })),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        }
                        Some(GossipEvent::BroadcastSignedProposal(proposal)) => {
                            let proposal_proto = proposal.to_proto();
                            let gossip_message = proto::GossipMessage {
                                gossip_message: Some(proto::gossip_message::GossipMessage::Consensus(proto::ConsensusMessage {
                                    signature: proposal.signature.0,
                                    consensus_message: Some(proto::consensus_message::ConsensusMessage::Proposal(proposal_proto)),
                                })),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        }
                        Some(GossipEvent::BroadcastFullProposal(full_proposal)) => {
                            let gossip_message = proto::GossipMessage {
                                gossip_message: Some(proto::gossip_message::GossipMessage::FullProposal(full_proposal)),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        },
                        Some(GossipEvent::RegisterValidator(register_validator)) => {
                            debug!("Broadcasting validator registration");
                            let gossip_message = proto::GossipMessage {
                                gossip_message: Some(proto::gossip_message::GossipMessage::Validator(register_validator)),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        },
                        None => {
                            // no-op
                        }
                    }
                }
            }
        }
    }

    fn publish(&mut self, message: Vec<u8>) {
        let topic = gossipsub::IdentTopic::new("test-net");
        if let Err(e) = self.swarm.behaviour_mut().gossipsub.publish(topic, message) {
            warn!("Failed to publish gossip message: {:?}", e);
        }
    }
}
