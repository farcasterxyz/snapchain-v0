use crate::consensus::consensus::{
    BlockProposer, Config, Consensus, ConsensusMsg, ConsensusParams, Decision, ShardProposer,
    ShardValidator,
};
use crate::core::types::{
    Address, Height, ShardId, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
    SnapchainValidatorSet,
};
use crate::network::gossip::GossipEvent;
use crate::proto::message;
use crate::proto::snapchain::Block;
use crate::storage::db::RocksDB;
use crate::storage::store::get_current_height;
use libp2p::identity::ed25519::Keypair;
use malachite_config::TimeoutConfig;
use malachite_metrics::Metrics;
use ractor::ActorRef;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

const MAX_SHARDS: u32 = 3;

pub struct SnapchainNode {
    pub consensus_actors: BTreeMap<u32, ActorRef<ConsensusMsg<SnapchainValidatorContext>>>,
    pub messages_tx_by_shard: HashMap<u32, mpsc::Sender<message::Message>>,
    pub address: Address,
}

impl SnapchainNode {
    pub async fn create(
        keypair: Keypair,
        config: Config,
        rpc_address: Option<String>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        block_tx: mpsc::Sender<Block>,
        db: Arc<RocksDB>,
    ) -> Self {
        let validator_address = Address(keypair.public().to_bytes());

        let mut consensus_actors = BTreeMap::new();

        let (shard_decision_tx, shard_decision_rx) = mpsc::channel::<Decision>(100);

        let mut shard_messages: HashMap<u32, mpsc::Sender<message::Message>> = HashMap::new();

        // Create the shard validators
        for shard_id in config.shard_ids() {
            if shard_id == 0 {
                panic!("Shard ID 0 is reserved for the block shard, created automaticaly");
            } else if shard_id > MAX_SHARDS {
                panic!("Shard ID must be between 1 and 3");
            }

            let current_height = match get_current_height(&db, shard_id) {
                Err(_) => 0,
                Ok(None) => 0,
                Ok(Some(height)) => height,
            };
            let shard = SnapchainShard::new(shard_id);
            let shard_validator = SnapchainValidator::new(
                shard.clone(),
                keypair.public().clone(),
                rpc_address.clone(),
                current_height,
            );
            let shard_validator_set = SnapchainValidatorSet::new(vec![shard_validator]);
            let shard_consensus_params = ConsensusParams {
                start_height: Height::new(shard.shard_id(), 1),
                initial_validator_set: shard_validator_set,
                address: validator_address.clone(),
                threshold_params: Default::default(),
            };
            let ctx = SnapchainValidatorContext::new(keypair.clone());
            let shard_proposer = ShardProposer::new(
                validator_address.clone(),
                shard.clone(),
                Some(shard_decision_tx.clone()),
            );

            shard_messages.insert(shard_id, shard_proposer.messages_tx());

            let shard_validator = ShardValidator::new(
                validator_address.clone(),
                shard.clone(),
                None,
                Some(shard_proposer),
            );
            let consensus_actor = Consensus::spawn(
                ctx,
                shard.clone(),
                shard_consensus_params,
                TimeoutConfig::default(),
                Metrics::new(),
                gossip_tx.clone(),
                shard_validator,
            )
            .await
            .unwrap();

            consensus_actors.insert(shard_id, consensus_actor);
        }

        // Now create the block validator
        let block_shard = SnapchainShard::new(0);

        let current_height = match get_current_height(&db, 0) {
            Err(_) => 0,
            Ok(None) => 0,
            Ok(Some(height)) => height,
        };
        // We might want to use different keys for the block shard so signatures are different and cannot be accidentally used in the wrong shard
        let block_validator = SnapchainValidator::new(
            block_shard.clone(),
            keypair.public().clone(),
            rpc_address.clone(),
            current_height,
        );
        let block_validator_set = SnapchainValidatorSet::new(vec![block_validator]);

        let block_consensus_params = ConsensusParams {
            start_height: Height::new(block_shard.shard_id(), 1),
            initial_validator_set: block_validator_set,
            address: validator_address.clone(),
            threshold_params: Default::default(),
        };

        let block_proposer = BlockProposer::new(
            validator_address.clone(),
            block_shard.clone(),
            shard_decision_rx,
            config.num_shards(),
            block_tx,
            db.clone(),
        );
        let block_validator = ShardValidator::new(
            validator_address.clone(),
            block_shard.clone(),
            Some(block_proposer),
            None,
        );
        let ctx = SnapchainValidatorContext::new(keypair.clone());
        let block_consensus_actor = Consensus::spawn(
            ctx,
            block_shard,
            block_consensus_params,
            TimeoutConfig::default(),
            Metrics::new(),
            gossip_tx.clone(),
            block_validator,
        )
        .await
        .unwrap();
        consensus_actors.insert(0, block_consensus_actor);

        Self {
            consensus_actors,
            messages_tx_by_shard: shard_messages,
            address: validator_address,
        }
    }

    pub fn id(&self) -> String {
        self.address.prefix()
    }

    pub fn stop(&self) {
        // Stop all actors
        for (_, actor) in self.consensus_actors.iter() {
            actor.stop(None);
        }
    }

    pub fn start_height(&self, block_number: u64) {
        for (shard, actor) in self.consensus_actors.iter() {
            let result = actor.cast(ConsensusMsg::StartHeight(Height::new(*shard, block_number)));
            if let Err(e) = result {
                warn!("Failed to start height: {:?}", e);
            }
        }
    }

    pub fn dispatch(&self, msg: ConsensusMsg<SnapchainValidatorContext>) {
        let shard_id = msg.shard_id();
        if let Some(actor) = self.consensus_actors.get(&shard_id) {
            let result = actor.cast(msg);
            if let Err(e) = result {
                warn!("Failed to forward message to actor: {:?}", e);
            }
        } else {
            warn!("No actor found for shard, could not forward message");
        }
    }
}
