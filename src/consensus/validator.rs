use crate::consensus::proposer::{BlockProposer, Proposer, ShardProposer};
use crate::core::types::{
    Address, Height, ShardHash, SnapchainShard, SnapchainValidator, SnapchainValidatorContext,
    SnapchainValidatorSet,
};
use crate::proto::FullProposal;
use malachite_common::{Round, ValidatorSet};
use malachite_consensus::ProposedValue;
use std::collections::HashSet;
use std::time::Duration;
use tracing::error;

pub struct ShardValidator {
    pub(crate) shard_id: SnapchainShard,

    #[allow(dead_code)] // TODO
    address: Address,

    validator_set: SnapchainValidatorSet,
    confirmed_height: Option<Height>,
    current_round: Round,
    current_height: Option<Height>,
    current_proposer: Option<Address>,
    // This should be proposer: Box<dyn Proposer> but that doesn't implement Send which is required for the actor system.
    // TODO: Fix once we remove the actor system
    block_proposer: Option<BlockProposer>,
    shard_proposer: Option<ShardProposer>,
    pub started: bool,
    pub saw_proposal_from_validator: HashSet<Address>,
}

impl ShardValidator {
    pub fn new(
        address: Address,
        shard: SnapchainShard,
        block_proposer: Option<BlockProposer>,
        shard_proposer: Option<ShardProposer>,
    ) -> ShardValidator {
        ShardValidator {
            shard_id: shard.clone(),
            address: address.clone(),
            validator_set: SnapchainValidatorSet::new(vec![]),
            confirmed_height: None,
            current_round: Round::new(0),
            current_height: None,
            current_proposer: None,
            block_proposer,
            shard_proposer,
            started: false,
            saw_proposal_from_validator: HashSet::new(),
        }
    }

    pub fn get_validator_set(&self) -> SnapchainValidatorSet {
        self.validator_set.clone()
    }

    pub fn validator_count(&self) -> usize {
        self.validator_set.count()
    }

    pub fn get_current_height(&self) -> u64 {
        if let Some(p) = &self.block_proposer {
            return p.get_confirmed_height().block_number;
        } else if let Some(p) = &self.shard_proposer {
            return p.get_confirmed_height().block_number;
        }
        panic!("No proposer set on validator");
    }

    pub fn add_validator(&mut self, validator: SnapchainValidator) -> bool {
        self.validator_set.add(validator)
    }

    pub fn start(&mut self) {
        self.started = true;
    }

    pub fn saw_proposal_from_validator(&self, address: Address) -> bool {
        self.saw_proposal_from_validator.contains(&address)
    }

    pub async fn sync_against_validator(&mut self, validator: &SnapchainValidator) {
        if let Some(p) = &mut self.block_proposer {
            match p.sync_against_validator(&validator).await {
                Ok(()) => {}
                Err(err) => error!("Error registering validator {:#?}", err),
            };
        } else if let Some(p) = &mut self.shard_proposer {
            match p.sync_against_validator(&validator).await {
                Ok(()) => {}
                Err(err) => error!("Error registering validator {:#?}", err),
            }
        }
    }

    pub fn start_round(&mut self, height: Height, round: Round, proposer: Address) {
        self.current_height = Some(height);
        self.current_round = round;
        self.current_proposer = Some(proposer);
    }

    pub async fn decide(&mut self, height: Height, _: Round, value: ShardHash) {
        if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer
                .decide(height, self.current_round, value)
                .await;
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer
                .decide(height, self.current_round, value)
                .await;
        } else {
            panic!("No proposer set");
        }
        self.confirmed_height = Some(height);
        self.current_round = Round::Nil;
    }

    pub fn add_proposed_value(
        &mut self,
        full_proposal: FullProposal,
    ) -> ProposedValue<SnapchainValidatorContext> {
        let value = full_proposal.shard_hash();
        let validity = if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.add_proposed_value(&full_proposal)
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.add_proposed_value(&full_proposal)
        } else {
            panic!("No proposer set");
        };

        self.saw_proposal_from_validator
            .insert(full_proposal.proposer_address());
        ProposedValue {
            height: full_proposal.height(),
            round: full_proposal.round(),
            validator_address: full_proposal.proposer_address(),
            value,
            validity,
            extension: None,
        }
    }

    pub async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal {
        if let Some(block_proposer) = &mut self.block_proposer {
            block_proposer.propose_value(height, round, timeout).await
        } else if let Some(shard_proposer) = &mut self.shard_proposer {
            shard_proposer.propose_value(height, round, timeout).await
        } else {
            panic!("No proposer set");
        }
    }
}
