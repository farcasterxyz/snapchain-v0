use crate::consensus::consensus::ConsensusMsg;
use crate::core::types::{proto, Proposal, ShardId, Signature, SnapchainContext, SnapchainShard, SnapchainValidator, SnapchainValidatorContext, Vote};
use crate::{SystemMessage};
use futures::StreamExt;
use libp2p::identity::ed25519::Keypair;
use libp2p::{
    gossipsub, mdns, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, Swarm,
};
use malachite_common::{SignedProposal, SignedVote};
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

pub enum GossipEvent<Ctx: SnapchainContext> {
    BroadcastSignedVote(SignedVote<Ctx>),
    BroadcastSignedProposal(SignedProposal<Ctx>),
    BroadcastBlock(proto::BlockProposal),
    RegisterValidator(proto::RegisterValidator),
}

#[derive(NetworkBehaviour)]
pub struct SnapchainBehavior {
    pub gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
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
        addr: String,
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
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                let mdns = mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?;
                Ok(SnapchainBehavior { gossipsub, mdns })
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // subscribes to our topic
        let result = swarm.behaviour_mut().gossipsub.subscribe(&topic);
        if let Err(e) = result {
            warn!("Failed to subscribe to topic: {:?}", e);
            return Err(Box::new(e));
        }

        // Listen on all assigned port for this id
        swarm.listen_on(addr.parse()?)?;

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
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Mdns(mdns::Event::Discovered(list))) => {
                            for (peer_id, _multiaddr) in list {
                                info!("mDNS discovered a new peer: {peer_id}");
                                self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            }
                        },
                        SwarmEvent::Behaviour(SnapchainBehaviorEvent::Mdns(mdns::Event::Expired(list))) => {
                            for (peer_id, _multiaddr) in list {
                                info!("mDNS discover peer has expired: {peer_id}");
                                self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                                // TODO: Remove validator
                            }
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
                            message_id: id,
                            message,
                        })) => {
                            match proto::GossipMessage::decode(&message.data[..]) {
                                Ok(gossip_message) => {
                                    match gossip_message.message {
                                        Some(proto::gossip_message::Message::BlockProposal(block_poposal)) => {
                                            let height = block_poposal.clone().height.unwrap().block_number;
                                            debug!("Received block with height {} from peer: {}", height, peer_id);
                                            let consensus_message = ConsensusMsg::ReceivedBlockProposal(block_poposal);
                                            let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                            if let Err(e) = res {
                                                warn!("Failed to send system block message: {:?}", e);
                                            }
                                        },
                                        Some(proto::gossip_message::Message::Shard(shard)) => {
                                            debug!("Received shard with height {} from peer: {}", shard.header.unwrap().height.unwrap().block_number, peer_id);
                                        },
                                        Some(proto::gossip_message::Message::Validator(validator)) => {
                                            debug!("Received validator registration from peer: {}", peer_id);
                                            if let Some(validator) = validator.validator {
                                                let public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&validator.signer);
                                                if public_key.is_err() {
                                                    warn!("Failed to decode public key from peer: {}", peer_id);
                                                    continue;
                                                }
                                                let validator = SnapchainValidator::new(SnapchainShard::new(0), public_key.unwrap());
                                                let consensus_message = ConsensusMsg::RegisterValidator(validator);
                                                let res = self.system_tx.send(SystemMessage::Consensus(consensus_message)).await;
                                                if let Err(e) = res {
                                                    warn!("Failed to send system register validator message: {:?}", e);
                                                }
                                            }
                                        },
                                        Some(proto::gossip_message::Message::Consensus(signed_consensus_msg)) => {
                                            match signed_consensus_msg.message {
                                                Some(proto::consensus_message::Message::Vote(vote)) => {
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
                                                Some(proto::consensus_message::Message::Proposal(proposal)) => {
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
                                        None => warn!("Received empty gossip message from peer: {}", peer_id),
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
                                message: Some(proto::gossip_message::Message::Consensus(proto::ConsensusMessage {
                                    signature: vote.signature.0,
                                    message: Some(proto::consensus_message::Message::Vote(vote_proto)),
                                })),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        }
                        Some(GossipEvent::BroadcastSignedProposal(proposal)) => {
                            let proposal_proto = proposal.to_proto();
                            let gossip_message = proto::GossipMessage {
                                message: Some(proto::gossip_message::Message::Consensus(proto::ConsensusMessage {
                                    signature: proposal.signature.0,
                                    message: Some(proto::consensus_message::Message::Proposal(proposal_proto)),
                                })),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        }
                        Some(GossipEvent::BroadcastBlock(block_proposal)) => {
                            let gossip_message = proto::GossipMessage {
                                message: Some(proto::gossip_message::Message::BlockProposal(block_proposal)),
                            };
                            let encoded_message = gossip_message.encode_to_vec();
                            self.publish(encoded_message);
                        },
                        Some(GossipEvent::RegisterValidator(register_validator)) => {
                            debug!("Broadcasting validator registration");
                            let gossip_message = proto::GossipMessage {
                                message: Some(proto::gossip_message::Message::Validator(register_validator)),
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
