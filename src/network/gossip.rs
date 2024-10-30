use libp2p::identity::ed25519::Keypair;
use libp2p::{
    gossipsub, mdns, noise, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux, PeerId, Swarm,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::io;

#[derive(NetworkBehaviour)]
pub struct SnapchainGossip {
    pub gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

impl SnapchainGossip {
    pub fn create(
        keypair: Keypair,
        addr: String,
    ) -> Result<Swarm<SnapchainGossip>, Box<dyn std::error::Error>> {
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
                Ok(SnapchainGossip { gossipsub, mdns })
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // subscribes to our topic
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        // Listen on all assigned port for this id
        swarm.listen_on(addr.parse()?)?;

        Ok(swarm)
    }
}
