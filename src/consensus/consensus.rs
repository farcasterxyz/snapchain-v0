use async_trait::async_trait;
use libp2p::identity::ed25519::{Keypair, SecretKey};
use malachite_common::ValidatorSet;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use malachite_common::{
    Context, Extension, Round, SignedProposal, SignedProposalPart, SignedVote, Timeout, TimeoutStep,
};
use malachite_config::TimeoutConfig;
use malachite_consensus::{Effect, ProposedValue, Resume, SignedConsensusMsg};
use malachite_metrics::Metrics;

use crate::consensus::timers::{TimeoutElapsed, TimerScheduler};
use crate::consensus::validator::{self, ShardValidator};
use crate::core::types::{
    Height, ShardId, SnapchainContext, SnapchainShard, SnapchainValidator,
    SnapchainValidatorContext,
};
use crate::network::gossip::GossipEvent;
use crate::proto::snapchain::FullProposal;
pub use malachite_consensus::Params as ConsensusParams;
pub use malachite_consensus::State as ConsensusState;
use ractor::time::send_after;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

pub type ConsensusRef<Ctx> = ActorRef<ConsensusMsg<Ctx>>;
pub type Decision = FullProposal;
pub type TxDecision = mpsc::Sender<Decision>;
pub type RxDecision = mpsc::Receiver<Decision>;

pub enum SystemMessage {
    Consensus(ConsensusMsg<SnapchainValidatorContext>),
}

type Timers<Ctx> = TimerScheduler<Timeout, ConsensusMsg<Ctx>>;

impl<Ctx: Context + SnapchainContext> From<TimeoutElapsed<Timeout>> for ConsensusMsg<Ctx> {
    fn from(msg: TimeoutElapsed<Timeout>) -> Self {
        ConsensusMsg::TimeoutElapsed(msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub private_key: String,
    pub shard_ids: String,

    #[serde(with = "humantime_serde")]
    pub propose_value_delay: Duration,
}

impl Config {
    pub fn keypair(&self) -> Keypair {
        let bytes = hex::decode(&self.private_key).unwrap();
        let secret_key = SecretKey::try_from_bytes(bytes);
        Keypair::from(secret_key.unwrap())
    }

    pub fn shard_ids(&self) -> Vec<u32> {
        self.shard_ids
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect()
    }

    pub fn num_shards(&self) -> u32 {
        self.shard_ids().len() as u32
    }

    pub fn with_shard_ids(&self, shard_ids: Vec<u32>) -> Self {
        Self {
            private_key: self.private_key.clone(),
            shard_ids: shard_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join(","),
            propose_value_delay: self.propose_value_delay,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            private_key: hex::encode(SecretKey::generate()),
            shard_ids: "1".to_string(),
            propose_value_delay: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Clone)]
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

impl ConsensusMsg<SnapchainValidatorContext> {
    pub fn shard_id(&self) -> u32 {
        match self {
            ConsensusMsg::StartHeight(height) => height.shard_index,
            ConsensusMsg::ProposeValue(height, _, _, _) => height.shard_index,
            ConsensusMsg::ReceivedProposedValue(proposed) => proposed.height.shard_index,
            ConsensusMsg::ReceivedSignedVote(vote) => vote.height.shard_index,
            ConsensusMsg::ReceivedSignedProposal(proposal) => proposal.height.shard_index,
            ConsensusMsg::ReceivedFullProposal(full_proposal) => full_proposal.height().shard_index,
            ConsensusMsg::RegisterValidator(validator) => validator.shard_index,

            _ => panic!("Requested shard ID for unsupported message type"),
        }
    }
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

pub struct Consensus {
    ctx: SnapchainValidatorContext,
    params: ConsensusParams<SnapchainValidatorContext>,
    timeout_config: TimeoutConfig,
    metrics: Metrics,
    shard_id: SnapchainShard,
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
    name: String,
}

impl Consensus {
    pub fn new(
        ctx: SnapchainValidatorContext,
        shard_id: SnapchainShard,
        params: ConsensusParams<SnapchainValidatorContext>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
    ) -> Self {
        Self {
            ctx,
            shard_id,
            params,
            timeout_config,
            metrics,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        ctx: SnapchainValidatorContext,
        shard_id: SnapchainShard,
        params: ConsensusParams<SnapchainValidatorContext>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        shard_validator: ShardValidator,
    ) -> Result<ActorRef<ConsensusMsg<SnapchainValidatorContext>>, ractor::SpawnErr> {
        let node = Self::new(ctx, shard_id, params, timeout_config, metrics);

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
                self.start_height(&myself, state, height).await?;
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
                debug!(
                    "Received vote: {:?} for height: {:?}, round: {:?} at {:?}",
                    vote.shard_hash, vote.height, vote.round, self.params.address
                );
                if !state.shard_validator.started {
                    warn!("Consensus not started yet when receiving vote, starting");
                    self.start_height(&myself, state, vote.height.clone())
                        .await?;
                }

                if let Err(e) = self
                    .process_input(&myself, state, ConsensusInput::Vote(vote))
                    .await
                {
                    error!("Error when processing vote: {e:?}");
                }
                Ok(())
            }

            ConsensusMsg::ReceivedSignedProposal(proposal) => {
                debug!(
                    "Received proposal: {:?} for height: {:?}, round: {:?} at {:?}",
                    proposal.shard_hash, proposal.height, proposal.round, self.params.address
                );

                if !state.shard_validator.started {
                    warn!("Consensus not started yet when receiving proposal, starting");
                    self.start_height(&myself, state, proposal.height.clone())
                        .await?;
                }

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

                let connected_peers = state.shard_validator.validator_count();
                info!(
                    "Connected to peer {address}. Total peers: {:?}",
                    connected_peers
                );
                // let total_peers = state.consensus.driver.validator_set().count() - 1;

                // println!("Connected to {connected_peers}/{total_peers} peers");

                self.metrics.connected_peers.inc();

                if connected_peers == 3 {
                    info!("Enough peers ({connected_peers}) connected to start consensus");

                    let height = state.consensus.driver.height();
                    send_after(Duration::from_secs(10), myself.get_cell(), move || {
                        info!("Starting consensus");
                        ConsensusMsg::<SnapchainValidatorContext>::StartHeight(height)
                    });
                }
                Ok(())
            }

            ConsensusMsg::ReceivedProposalPart(_part) => {
                // TODO: implement
                Ok(())
            }

            ConsensusMsg::ReceivedFullProposal(full_proposal) => {
                let height = full_proposal.height.clone().unwrap();
                debug!(
                    "Received proposed value: {:?} at {:?}",
                    height, self.params.address
                );

                let current_height = state.shard_validator.get_current_height();

                // TODO(aditi): Make 1000 configurable.
                if height.block_number > current_height + 1000 {
                    panic!("Node is too far behind to join consensus. Try restarting. Current block number {}. Expected block number {}", current_height, height.block_number )
                }

                if !state
                    .shard_validator
                    .saw_proposal_from_validator(full_proposal.proposer_address())
                    && (height.block_number > current_height + 1)
                {
                    let validator_set = state.shard_validator.get_validator_set();
                    match validator_set.get_by_address(&full_proposal.proposer_address()) {
                        None => {
                            error!("Missing validator {}", full_proposal.proposer_address());
                        }
                        Some(validator) => {
                            state
                                .shard_validator
                                .sync_against_validator(&validator)
                                .await
                        }
                    };
                    match self.start_height(&myself, state, height).await {
                        Ok(()) => {}
                        Err(err) => {
                            error!("Error starting consensus at height {}. {}", height, err);
                        }
                    }
                }

                let proposed_value = state.shard_validator.add_proposed_value(full_proposal);

                let result = self
                    .process_input(
                        &myself,
                        state,
                        ConsensusInput::ReceivedProposedValue(proposed_value),
                    )
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
                debug!(
                    "Received proposed value: {:?} for height: {:?}, round: {:?} at {:?}",
                    value.value, value.height, value.round, self.params.address
                );
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

    async fn start_height(
        &self,
        myself: &ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        state: &mut State<SnapchainValidatorContext>,
        height: Height,
    ) -> Result<(), ActorProcessingErr> {
        if state.shard_validator.started && state.consensus.driver.height() >= height {
            warn!(
                "Requested start height is lower than current height, ignoring: {:?}",
                height
            );
            return Ok(());
        }
        let validator_set = state.shard_validator.get_validator_set();
        debug!(
            "Starting height: {height} with {:?} validators",
            validator_set.count()
        );
        let result = self
            .process_input(
                &myself,
                state,
                ConsensusInput::StartHeight(height, validator_set),
            )
            .await;
        state.shard_validator.start();

        if let Err(e) = result {
            error!("Error when starting height {height}: {e:?}");
        }

        Ok(())
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
                        debug!(
                            "Broadcasting proposal gossip message: {:?} {:?} from {:?}",
                            proposal.height, proposal.round, proposal.proposer
                        );
                        gossip_tx
                            .send(GossipEvent::BroadcastSignedProposal(proposal))
                            .await?;
                    }
                    SignedConsensusMsg::Vote(vote) => {
                        debug!(
                            "Broadcasting vote gossip message: {:?} {:?} {:?} from {:?}",
                            vote.vote_type, vote.height, vote.round, vote.voter
                        );
                        gossip_tx
                            .send(GossipEvent::BroadcastSignedVote(vote))
                            .await?;
                    }
                }

                Ok(Resume::Continue)
            }

            Effect::GetValue(height, round, timeout) => {
                let timeout = timeouts.duration_for(timeout.step);
                let full_proposal = shard_validator.propose_value(height, round, timeout).await;

                let value = full_proposal.shard_hash();

                debug!("Proposing value: {value} for height: {height}, round: {round}");
                let result = myself.cast(ConsensusMsg::ProposeValue(height, round, value, None));
                if let Err(e) = result {
                    error!("Error when forwarding locally proposed value: {e:?}");
                }

                gossip_tx
                    .send(GossipEvent::BroadcastFullProposal(full_proposal))
                    .await?;

                Ok(Resume::Continue)
            }

            Effect::GetValidatorSet(height) => Ok(Resume::ValidatorSet(
                height,
                Some(shard_validator.get_validator_set()),
            )),

            Effect::Decide {
                height,
                round,
                value,
                commits,
            } => {
                info!(
                    "Deciding value: {value} for height: {height} at {:?} with {:?} commits",
                    self.params.address,
                    commits.len()
                );
                shard_validator.decide(height, round, value.clone()).await;
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
    type Arguments = (
        mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        ShardValidator,
    );

    #[tracing::instrument(name = "consensus", skip_all)]
    async fn pre_start(
        &self,
        myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        args: Self::Arguments,
    ) -> Result<State<SnapchainValidatorContext>, ActorProcessingErr> {
        let address_prefix = self.params.address.prefix();
        let name = if args.1.shard_id.shard_id() == 0 {
            format!("{:} Block", address_prefix)
        } else {
            format!("{:} Shard {:}", address_prefix, args.1.shard_id.shard_id())
        };
        Ok(State {
            timers: Timers::new(myself),
            timeouts: Timeouts::new(self.timeout_config),
            consensus: ConsensusState::new(self.ctx.clone(), self.params.clone()),
            shard_validator: args.1,
            gossip_tx: args.0,
            name,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<ConsensusMsg<SnapchainValidatorContext>>,
        state: &mut State<SnapchainValidatorContext>,
    ) -> Result<(), ActorProcessingErr> {
        state.timers.cancel_all();
        // Add ourselves to the validator set
        state.shard_validator.add_validator(SnapchainValidator::new(
            self.shard_id.clone(),
            self.ctx.public_key(),
            None,
            state.shard_validator.get_current_height(),
        ));
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
        let span = tracing::info_span!("node", name = %state.name);
        let _enter = span.enter();
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
