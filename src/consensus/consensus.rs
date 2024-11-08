use malachite_common::{ValidatorSet, Validity};
use std::collections::{BTreeMap};
use std::time::Duration;

use async_trait::async_trait;
use libp2p::identity::ed25519::{Keypair, SecretKey};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use malachite_common::{
    Context, Extension, Round, SignedProposal, SignedProposalPart,
    SignedVote, Timeout, TimeoutStep,
};
use malachite_config::TimeoutConfig;
use malachite_consensus::{Effect, ProposedValue, Resume, SignedConsensusMsg};
use malachite_metrics::Metrics;

use crate::consensus::timers::{TimeoutElapsed, TimerScheduler};
use crate::core::types::proto::{ShardHash};
use crate::core::types::{proto, Address, Height, ShardId, SnapchainContext, SnapchainShard, SnapchainValidator, SnapchainValidatorContext, SnapchainValidatorSet};
use crate::network::gossip::GossipEvent;
use crate::core::types::proto::{Block, BlockHeader, Height as ProtoHeight};
pub use malachite_consensus::Params as ConsensusParams;
pub use malachite_consensus::State as ConsensusState;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use crate::proto::snapchain::{FullProposal, ShardChunk};

pub type ConsensusRef<Ctx> = ActorRef<ConsensusMsg<Ctx>>;

pub type Decision<Ctx> = (<Ctx as Context>::Height, Round, <Ctx as Context>::Value);
pub type TxDecision<Ctx> = mpsc::Sender<Decision<Ctx>>;

pub enum SystemMessage {
    Consensus(ConsensusMsg<SnapchainValidatorContext>),
}

type Timers<Ctx> = TimerScheduler<Timeout, ConsensusMsg<Ctx>>;

impl<Ctx: Context + SnapchainContext> From<TimeoutElapsed<Timeout>> for ConsensusMsg<Ctx> {
    fn from(msg: TimeoutElapsed<Timeout>) -> Self {
        ConsensusMsg::TimeoutElapsed(msg)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub private_key: String,
}

impl Config {
    pub fn keypair(&self) -> Keypair {
        let bytes = hex::decode(&self.private_key).unwrap();
        let secret_key = SecretKey::try_from_bytes(bytes);
        Keypair::from(secret_key.unwrap())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            private_key: hex::encode(SecretKey::generate()),
        }
    }
}

#[derive(Debug)]
pub enum ConsensusMsg<Ctx: SnapchainContext> {
    // Inputs
    /// Start consensus for the given height
    StartHeight(Ctx::Height),
    /// The proposal builder has built a value and can be used in a new proposal consensus message
    ProposeValue(Ctx::Height, Round, Ctx::Value, Option<Extension>),
    /// Received and assembled the full value proposed by a validator
    ReceivedProposedValue(ProposedValue<Ctx>),

    /// Received an event from the gossip layer
    ReceivedSignedVote(SignedVote<Ctx>),
    ReceivedSignedProposal(SignedProposal<Ctx>),
    ReceivedProposalPart(SignedProposalPart<Ctx>),

    ReceivedFullProposal(FullProposal),
    RegisterValidator(SnapchainValidator),

    TimeoutElapsed(TimeoutElapsed<Timeout>),
}

struct Timeouts {
    config: TimeoutConfig,
}

impl Timeouts {
    pub fn new(config: TimeoutConfig) -> Self {
        Self { config }
    }

    fn reset(&mut self, config: TimeoutConfig) {
        self.config = config;
    }

    fn duration_for(&self, step: TimeoutStep) -> Duration {
        match step {
            TimeoutStep::Propose => self.config.timeout_propose,
            TimeoutStep::Prevote => self.config.timeout_prevote,
            TimeoutStep::Precommit => self.config.timeout_precommit,
            TimeoutStep::Commit => self.config.timeout_commit,
        }
    }

    fn increase_timeout(&mut self, step: TimeoutStep) {
        let c = &mut self.config;
        match step {
            TimeoutStep::Propose => c.timeout_propose += c.timeout_propose_delta,
            TimeoutStep::Prevote => c.timeout_prevote += c.timeout_prevote_delta,
            TimeoutStep::Precommit => c.timeout_precommit += c.timeout_precommit_delta,
            TimeoutStep::Commit => (),
        };
    }
}

pub trait Proposer {
    // Create a new block/shard chunk for the given height that will be proposed for confirmation to the other validators
    fn propose_value(&mut self, height: Height, round: Round) -> proto::full_proposal::ProposedValue;
    // Receive a block/shard chunk proposed by another validator and return whether it is valid
    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity;
    // Consensus has confirmed the block/shard_chunk, apply it to the local state
    fn decide(&mut self, height: Height, round: Round, value: ShardHash);
}

pub struct ShardProposer {
    shard_id: SnapchainShard,
    address: Address,
    chunks: Vec<ShardChunk>,
    proposed_chunks: BTreeMap<ShardHash, ShardChunk>,
}

impl ShardProposer {

}

impl Proposer for ShardProposer {
    fn propose_value(&mut self, height: Height, round: Round) -> proto::full_proposal::ProposedValue {
        todo!()
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        todo!()
    }

    fn decide(&mut self, height: Height, round: Round, value: ShardHash) {
        todo!()
    }
}


pub struct BlockProposer {
    shard_id: SnapchainShard,
    address: Address,
    blocks: Vec<Block>,
    proposed_blocks: BTreeMap<ShardHash, Block>,
}

impl BlockProposer {
    pub fn new(address: Address, shard_id: SnapchainShard) -> BlockProposer {
        BlockProposer {
            shard_id,
            address,
            blocks: vec![],
            proposed_blocks: BTreeMap::new(),
        }
    }
}

impl Proposer for BlockProposer {
    fn propose_value(&mut self, height: Height, round: Round) -> proto::full_proposal::ProposedValue {
        let previous_block = self.blocks.last();
        let parent_hash = match previous_block {
            Some(block) => block.hash.clone(),
            None => vec![0, 32],
        };
        let block_header = BlockHeader {
            parent_hash,
            chain_id: 0,
            version: 0,
            shard_headers_hash: vec![],
            validators_hash: vec![],
            timestamp: 0,
            height: Some(ProtoHeight {
                block_number: height.block_number,
                shard_index: height.shard_index as u32,
            }),
        };
        let hash = blake3::hash(&block_header.encode_to_vec())
            .as_bytes()
            .to_vec();

        let block = Block {
            header: Some(block_header),
            hash: hash.clone(),
            validators: None,
            votes: None,
            shard_chunks: vec![],
        };

        let shard_hash = ShardHash {
            hash: hash.clone(),
            shard_index: height.shard_index as u32,
        };
        self.proposed_blocks.insert(shard_hash, block.clone());
        proto::full_proposal::ProposedValue::Block(block)
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Block(block)) = full_proposal.proposed_value.clone() {
            self.proposed_blocks.insert(full_proposal.value(), block);
        }
        Validity::Valid  // TODO: Validate proposer signature?
    }

    fn decide(&mut self, height: Height, round: Round, value: ShardHash) {
        let block = self.proposed_blocks.get(&value);
        if block.is_some() {
            self.blocks.push(block.unwrap().clone());
            self.proposed_blocks.remove(&value);
        }
    }
}

pub struct ShardValidator {
    shard_id: SnapchainShard,
    address: Address,
    validator_set: SnapchainValidatorSet,
    confirmed_height: Option<Height>,
    current_round: Round,
    current_height: Option<Height>,
    current_proposer: Option<Address>,
    proposer: BlockProposer,
}

impl ShardValidator {
    pub fn new(address: Address, proposer: BlockProposer) -> ShardValidator {
        let shard = SnapchainShard::new(0);
        ShardValidator {
            shard_id: shard.clone(),
            address: address.clone(),
            validator_set: SnapchainValidatorSet::new(vec![]),
            confirmed_height: None,
            current_round: Round::new(0),
            current_height: None,
            current_proposer: None,
            proposer,
        }
    }

    pub fn get_validator_set(&self) -> SnapchainValidatorSet {
        self.validator_set.clone()
    }

    pub fn add_validator(&mut self, validator: SnapchainValidator) -> bool {
        self.validator_set.add(validator)
    }

    pub fn start_round(&mut self, height: Height, round: Round, proposer: Address) {
        self.current_height = Some(height);
        self.current_round = round;
        self.current_proposer = Some(proposer);
    }

    pub fn decide(&mut self, height: Height, _: Round, value: ShardHash) {
        self.proposer.decide(height, self.current_round, value);
        self.confirmed_height = Some(height);
        self.current_round = Round::Nil;
    }

    pub fn add_proposed_value(&mut self, full_proposal: FullProposal) -> ProposedValue<SnapchainValidatorContext> {
        let value = full_proposal.value();
        let validity = self.proposer.add_proposed_value(&full_proposal);

        ProposedValue {
            height: full_proposal.height(),
            round: full_proposal.round(),
            validator_address: full_proposal.proposer_address(),
            value,
            validity,
            extension: None,
        }
    }

    pub fn propose_value(&mut self, height: Height, round: Round) -> FullProposal {
        let proposed_value = self.proposer.propose_value(height, round);

        FullProposal {
            height: Some(height.to_proto()),
            round: round.as_i64(),
            proposed_value: Some(proposed_value),
            proposer: self.address.to_vec(),
        }
    }
}

pub struct Consensus {
    ctx: SnapchainValidatorContext,
    params: ConsensusParams<SnapchainValidatorContext>,
    timeout_config: TimeoutConfig,
    metrics: Metrics,
    shard_id: SnapchainShard,
    tx_decision: Option<TxDecision<SnapchainValidatorContext>>,
}

// pub type ConsensusMsg<Ctx> = ConsensusMsg<Ctx>;

type ConsensusInput<Ctx> = malachite_consensus::Input<Ctx>;

pub struct State<Ctx: SnapchainContext> {
    /// Scheduler for timers
    timers: Timers<Ctx>,

    /// Timeouts configuration
    timeouts: Timeouts,

    /// The state of the consensus state machine
    consensus: ConsensusState<Ctx>,

    /// The set of validators (by address) we are connected to.
    shard_validator: ShardValidator,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
}

impl Consensus {
    pub fn new(
        ctx: SnapchainValidatorContext,
        shard_id: SnapchainShard,
        params: ConsensusParams<SnapchainValidatorContext>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
        tx_decision: Option<TxDecision<SnapchainValidatorContext>>,
    ) -> Self {
        Self {
            ctx,
            shard_id,
            params,
            timeout_config,
            metrics,
            tx_decision,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        ctx: SnapchainValidatorContext,
        shard_id: SnapchainShard,
        params: ConsensusParams<SnapchainValidatorContext>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
        tx_decision: Option<TxDecision<SnapchainValidatorContext>>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_validator: ShardValidator,
    ) -> Result<ActorRef<ConsensusMsg<SnapchainValidatorContext>>, ractor::SpawnErr> {
        let node = Self::new(ctx, shard_id, params, timeout_config, metrics, tx_decision);

        let (actor_ref, _) = Actor::spawn(None, node, (gossip_tx, shard_validator)).await?;
        Ok(actor_ref)
    }

    async fn process_input(
        &self,
        myself: &ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        state: &mut State<SnapchainValidatorContext>,
        input: ConsensusInput<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        malachite_consensus::process!(
            input: input,
            state: &mut state.consensus,
            metrics: &self.metrics,
            with: effect => {
                self.handle_effect(myself, &mut state.shard_validator, &mut state.timers, &mut state.timeouts, state.gossip_tx.clone(), effect).await
            }
        )
    }

    async fn handle_msg(
        &self,
        myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        state: &mut State<SnapchainValidatorContext>,
        msg: ConsensusMsg<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            ConsensusMsg::StartHeight(height) => {
                let validator_set = state.shard_validator.get_validator_set();
                debug!("Starting height: {height} with {:?} validators", validator_set.count());
                let result = self
                    .process_input(
                        &myself,
                        state,
                        ConsensusInput::StartHeight(height, validator_set),
                    )
                    .await;

                if let Err(e) = result {
                    error!("Error when starting height {height}: {e:?}");
                }

                Ok(())
            }

            ConsensusMsg::ProposeValue(height, round, value, _) => {
                let result = self
                    .process_input(
                        &myself,
                        state,
                        ConsensusInput::ProposeValue(height, round, value, None),
                    )
                    .await;

                if let Err(e) = result {
                    error!("Error when processing ProposeValue message: {e:?}");
                }

                Ok(())
            }

            ConsensusMsg::ReceivedSignedVote(vote) => {
                debug!("Received vote: {:?} for height: {:?}, round: {:?} at {:?}", vote.shard_hash, vote.height, vote.round, self.params.address);
                if let Err(e) = self
                    .process_input(&myself, state, ConsensusInput::Vote(vote))
                    .await
                {
                    error!("Error when processing vote: {e:?}");
                }
                Ok(())
            }

            ConsensusMsg::ReceivedSignedProposal(proposal) => {
                debug!("Received proposal: {:?} for height: {:?}, round: {:?} at {:?}", proposal.shard_hash, proposal.height, proposal.round, self.params.address);
                if let Err(e) = self
                    .process_input(&myself, state, ConsensusInput::Proposal(proposal))
                    .await
                {
                    error!("Error when processing proposal: {e:?}");
                }
                Ok(())
            }

            ConsensusMsg::RegisterValidator(validator) => {
                let address = validator.address.to_hex();
                if !state.shard_validator.add_validator(validator.clone()) {
                    // We already saw that peer, ignoring...
                    return Ok(());
                }



                let connected_peers = state.shard_validator.validator_set.count();
                info!("Connected to peer {address}. Total peers: {:?}", connected_peers);
                // let total_peers = state.consensus.driver.validator_set().count() - 1;

                // println!("Connected to {connected_peers}/{total_peers} peers");

                self.metrics.connected_peers.inc();

                if connected_peers == 3 {
                    info!("Enough peers ({connected_peers}) connected to start consensus");

                    let height = state.consensus.driver.height();
                    let validator_set = state.shard_validator.get_validator_set();
                
                    let result = self
                        .process_input(
                            &myself,
                            state,
                            ConsensusInput::StartHeight(height, validator_set),
                        )
                        .await;

                    if let Err(e) = result {
                        error!("Error when starting height {height}: {e:?}");
                    }
                }
                Ok(())
            }

            ConsensusMsg::ReceivedProposalPart(part) => {
                // TODO: implement
                Ok(())
            }

            ConsensusMsg::ReceivedFullProposal(full_proposal) => {
                let height = Height::from_proto(full_proposal.height.clone().unwrap());
                debug!("Received proposed value: {:?} at {:?}", height, self.params.address);
                let proposed_value = state.shard_validator.add_proposed_value(full_proposal);

                let result = self
                    .process_input(&myself, state, ConsensusInput::ReceivedProposedValue(proposed_value))
                    .await;

                if let Err(e) = result {
                    error!("Error when processing GossipEvent message: {e:?}");
                }

                Ok(())
            }

            ConsensusMsg::TimeoutElapsed(elapsed) => {
                let Some(timeout) = state.timers.intercept_timer_msg(elapsed) else {
                    // Timer was cancelled or already processed, ignore
                    return Ok(());
                };

                state.timeouts.increase_timeout(timeout.step);

                if matches!(timeout.step, TimeoutStep::Prevote | TimeoutStep::Precommit) {
                    warn!(step = ?timeout.step, "Timeout elapsed");
                }

                let result = self
                    .process_input(&myself, state, ConsensusInput::TimeoutElapsed(timeout))
                    .await;

                if let Err(e) = result {
                    error!("Error when processing TimeoutElapsed message: {e:?}");
                }

                Ok(())
            }
            ConsensusMsg::ReceivedProposedValue(value) => {
                debug!("Received proposed value: {:?} for height: {:?}, round: {:?} at {:?}", value.value, value.height, value.round, self.params.address);
                let result = self
                    .process_input(&myself, state, ConsensusInput::ReceivedProposedValue(value))
                    .await;

                if let Err(e) = result {
                    error!("Error when processing GossipEvent message: {e:?}");
                }

                Ok(())
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn handle_effect(
        &self,
        myself: &ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        shard_validator: &mut ShardValidator,
        timers: &mut Timers<SnapchainValidatorContext>,
        timeouts: &mut Timeouts,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        effect: Effect<SnapchainValidatorContext>,
    ) -> Result<Resume<SnapchainValidatorContext>, ActorProcessingErr> {
        match effect {
            Effect::ResetTimeouts => {
                timeouts.reset(self.timeout_config);
                Ok(Resume::Continue)
            }

            Effect::CancelAllTimeouts => {
                timers.cancel_all();
                Ok(Resume::Continue)
            }

            Effect::CancelTimeout(timeout) => {
                timers.cancel(&timeout);
                Ok(Resume::Continue)
            }

            Effect::ScheduleTimeout(timeout) => {
                let duration = timeouts.duration_for(timeout.step);
                timers.start_timer(timeout, duration);
                Ok(Resume::Continue)
            }

            Effect::StartRound(height, round, proposer) => {
                debug!("Starting height: {height}, round: {round}, proposer: {proposer}");
                shard_validator.start_round(height, round, proposer);
                Ok(Resume::Continue)
            }

            Effect::VerifySignature(msg, pk) => {
                use malachite_consensus::ConsensusMsg as Msg;

                let start = Instant::now();

                let valid = match msg.message {
                    Msg::Vote(v) => self.ctx.verify_signed_vote(&v, &msg.signature, &pk),
                    Msg::Proposal(p) => self.ctx.verify_signed_proposal(&p, &msg.signature, &pk),
                };

                self.metrics
                    .signature_verification_time
                    .observe(start.elapsed().as_secs_f64());

                Ok(Resume::SignatureValidity(valid))
            }

            Effect::Broadcast(gossip_msg) => {
                match gossip_msg {
                    SignedConsensusMsg::Proposal(proposal) => {
                        debug!("Broadcasting proposal gossip message: {:?} {:?} from {:?}", proposal.height, proposal.round, proposal.proposer);
                        gossip_tx
                            .send(GossipEvent::BroadcastSignedProposal(proposal))
                            .await?;
                    }
                    SignedConsensusMsg::Vote(vote) => {
                        debug!("Broadcasting vote gossip message: {:?} {:?} {:?} from {:?}", vote.vote_type, vote.height, vote.round, vote.voter);
                        gossip_tx
                            .send(GossipEvent::BroadcastSignedVote(vote))
                            .await?;
                    }
                }

                Ok(Resume::Continue)
            }

            Effect::GetValue(height, round, timeout) => {
                let timeout_duration = timeouts.duration_for(timeout.step);
                let full_proposal = shard_validator.propose_value(height, round);

                let value = full_proposal.value();
                // Sleep before proposing the value so we don't produce blocks too fast
                tokio::time::sleep(Duration::from_millis(500)).await;

                debug!("Proposing value: {value} for height: {height}, round: {round}");
                let result = myself.cast(ConsensusMsg::ProposeValue(height, round, value, None));
                if let Err(e) = result {
                    error!("Error when forwarding locally proposed value: {e:?}");
                }

                gossip_tx.send(GossipEvent::BroadcastFullProposal(full_proposal)).await?;

                Ok(Resume::Continue)
            }

            Effect::GetValidatorSet(height) => {
                Ok(Resume::ValidatorSet(
                    height,
                    Some(shard_validator.get_validator_set()),
                ))
            },

            Effect::Decide {
                height,
                round,
                value,
                commits,
            } => {
                if let Some(tx_decision) = &self.tx_decision {
                    let _ = tx_decision.send((height, round, value.clone())).await;
                }
                info!("Deciding value: {value} for height: {height} at {:?} with {:?} commits", self.params.address, commits.len());
                shard_validator.decide(height, round, value.clone());
                let result = myself.cast(ConsensusMsg::StartHeight(height.increment()));
                if let Err(e) = result {
                    error!("Error when starting next height after decision on {height}: {e:?}");
                }
                Ok(Resume::Continue)
            }
        }
    }
}

#[async_trait]
impl Actor for Consensus {
    type Msg = ConsensusMsg<SnapchainValidatorContext>;
    type State = State<SnapchainValidatorContext>;
    type Arguments = (mpsc::Sender<GossipEvent<SnapchainValidatorContext>>, ShardValidator);

    #[tracing::instrument(name = "consensus", skip_all)]
    async fn pre_start(
        &self,
        myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        args: Self::Arguments,
    ) -> Result<State<SnapchainValidatorContext>, ActorProcessingErr> {
        Ok(State {
            timers: Timers::new(myself),
            timeouts: Timeouts::new(self.timeout_config),
            consensus: ConsensusState::new(self.ctx.clone(), self.params.clone()),
            shard_validator: args.1,
            gossip_tx: args.0,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        state: &mut State<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        state.timers.cancel_all();
        // Add ourselves to the validator set
        state.shard_validator.add_validator(SnapchainValidator::new(self.shard_id.clone(), self.ctx.public_key()));
        Ok(())
    }

    #[tracing::instrument(
        name = "consensus",
        skip_all,
        fields(
            height = %state.consensus.driver.height(),
            round = %state.consensus.driver.round()
        )
    )]
    async fn handle(
        &self,
        myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        msg: ConsensusMsg<SnapchainValidatorContext>,
        state: &mut State<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        self.handle_msg(myself, state, msg).await
    }

    #[tracing::instrument(
        name = "consensus",
        skip_all,
        fields(
            height = %state.consensus.driver.height(),
            round = %state.consensus.driver.round()
        )
    )]
    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut State<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        info!("Stopping...");

        state.timers.cancel_all();

        Ok(())
    }
}
