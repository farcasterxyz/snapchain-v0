use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use libp2p::PeerId;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use malachite_common::{
    Context, Extension, Height, NilOrVal, Round, SignedMessage, SignedProposal, SignedProposalPart,
    SignedVote, Timeout, TimeoutStep, ValidatorSet, VoteType,
};
use malachite_config::TimeoutConfig;
use malachite_consensus::{Effect, ProposedValue, Resume, SignedConsensusMsg};
use malachite_metrics::Metrics;

use crate::consensus::timers::{TimeoutElapsed, TimerScheduler};
use crate::core::types::{SnapchainContext, Validator};
pub use malachite_consensus::Params as ConsensusParams;
pub use malachite_consensus::State as ConsensusState;
use tokio::time::Instant;

pub type ConsensusRef<Ctx> = ActorRef<ConsensusMsg<Ctx>>;

pub type TxDecision<Ctx> = mpsc::Sender<(<Ctx as Context>::Height, Round, <Ctx as Context>::Value)>;
type Timers<Ctx> = TimerScheduler<Timeout, ConsensusMsg<Ctx>>;

impl<Ctx: Context + SnapchainContext> From<TimeoutElapsed<Timeout>> for ConsensusMsg<Ctx> {
    fn from(msg: TimeoutElapsed<Timeout>) -> Self {
        ConsensusMsg::TimeoutElapsed(msg)
    }
}

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

    RegisterValidator(Validator),

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

pub struct Consensus<Ctx>
where
    Ctx: SnapchainContext,
{
    ctx: Ctx,
    params: ConsensusParams<Ctx>,
    timeout_config: TimeoutConfig,
    metrics: Metrics,
    shard_id: Ctx::ShardId,
    tx_decision: Option<TxDecision<Ctx>>,
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
    connected_validators: BTreeSet<String>,
}

impl<Ctx> Consensus<Ctx>
where
    Ctx: SnapchainContext,
{
    pub fn new(
        ctx: Ctx,
        shard_id: Ctx::ShardId,
        params: ConsensusParams<Ctx>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
        tx_decision: Option<TxDecision<Ctx>>,
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
        ctx: Ctx,
        shard_id: Ctx::ShardId,
        params: ConsensusParams<Ctx>,
        timeout_config: TimeoutConfig,
        metrics: Metrics,
        tx_decision: Option<TxDecision<Ctx>>,
    ) -> Result<ActorRef<ConsensusMsg<Ctx>>, ractor::SpawnErr> {
        let node = Self::new(ctx, shard_id, params, timeout_config, metrics, tx_decision);

        let (actor_ref, _) = Actor::spawn(None, node, ()).await?;
        Ok(actor_ref)
    }

    async fn process_input(
        &self,
        myself: &ActorRef<ConsensusMsg<Ctx>>,
        state: &mut State<Ctx>,
        input: ConsensusInput<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        malachite_consensus::process!(
            input: input,
            state: &mut state.consensus,
            metrics: &self.metrics,
            with: effect => {
                self.handle_effect(myself, &mut state.timers, &mut state.timeouts, effect).await
            }
        )
    }

    async fn handle_msg(
        &self,
        myself: ActorRef<ConsensusMsg<Ctx>>,
        state: &mut State<Ctx>,
        msg: ConsensusMsg<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            ConsensusMsg::StartHeight(height) => {
                let validator_set = self.get_validator_set(height).await?;
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
                if let Err(e) = self
                    .process_input(&myself, state, ConsensusInput::Vote(vote))
                    .await
                {
                    error!("Error when processing vote: {e:?}");
                }
                Ok(())
            }

            ConsensusMsg::ReceivedSignedProposal(proposal) => {
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
                if !state.connected_validators.insert(address.clone()) {
                    // We already saw that peer, ignoring...
                    return Ok(());
                }

                println!("Connected to peer {address}");

                let connected_peers = state.connected_validators.len();
                let total_peers = state.consensus.driver.validator_set().count() - 1;

                debug!("Connected to {connected_peers}/{total_peers} peers");

                self.metrics.connected_peers.inc();

                if connected_peers == total_peers {
                    info!("Enough peers ({connected_peers}) connected to start consensus");

                    let height = state.consensus.driver.height();
                    let validator_set = self.get_validator_set(height).await?;

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

            // Msg::GossipEvent(event) => {
            //     match event {
            //         GossipEvent::PeerDisconnected(peer_id) => {
            //             info!("Disconnected from peer {peer_id}");
            //
            //             if state.connected_peers.remove(&peer_id) {
            //                 self.metrics.connected_peers.dec();
            //
            //                 // TODO: pause/stop consensus, if necessary
            //             }
            //
            //             Ok(())
            //         }
            //
            //         GossipEvent::ProposalPart(from, part) => {
            //             self.host
            //                 .call_and_forward(
            //                     |reply_to| HostMsg::ReceivedProposalPart {
            //                         from,
            //                         part,
            //                         reply_to,
            //                     },
            //                     &myself,
            //                     |value| Msg::ReceivedProposedValue(value),
            //                     None,
            //                 )
            //                 .map_err(|e| {
            //                     eyre!("Error when forwarding proposal parts to host: {e:?}")
            //                 })?;
            //
            //             Ok(())
            //         }
            //     }
            // }
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

    // #[tracing::instrument(skip(self, myself))]
    // fn get_value(
    //     &self,
    //     myself: &ActorRef<Msg<Ctx>>,
    //     height: Ctx::Height,
    //     round: Round,
    //     timeout_duration: Duration,
    // ) -> Result<(), ActorProcessingErr> {
    //     // Call `GetValue` on the Host actor, and forward the reply
    //     // to the current actor, wrapping it in `Msg::ProposeValue`.
    //     self.host.call_and_forward(
    //         |reply| HostMsg::GetValue {
    //             height,
    //             round,
    //             timeout_duration,
    //             address: self.params.address.clone(),
    //             reply_to: reply,
    //         },
    //         myself,
    //         |proposed: LocallyProposedValue<Ctx>| {
    //             Msg::<Ctx>::ProposeValue(proposed.height, proposed.round, proposed.value)
    //         },
    //         None,
    //     )?;
    //
    //     Ok(())
    // }
    //
    #[tracing::instrument(skip(self))]
    async fn get_validator_set(
        &self,
        height: Ctx::Height,
    ) -> Result<Ctx::ValidatorSet, ActorProcessingErr> {
        // let validator_set = ractor::call!(self.host, |reply_to| HostMsg::GetValidatorSet {
        //     height,
        //     reply_to
        // })
        //     .map_err(|e| eyre!("Failed to query validator set at height {height}: {e:?}"))?;

        Ok(self.params.initial_validator_set.clone())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_effect(
        &self,
        myself: &ActorRef<ConsensusMsg<Ctx>>,
        timers: &mut Timers<Ctx>,
        timeouts: &mut Timeouts,
        effect: Effect<Ctx>,
    ) -> Result<Resume<Ctx>, ActorProcessingErr> {
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
                // self.host.cast(HostMsg::StartRound {
                //     height,
                //     round,
                //     proposer,
                // })?;

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
                // TODO
                // self.gossip_consensus
                //     .cast(GossipConsensusMsg::BroadcastMsg(gossip_msg))
                //     .map_err(|e| eyre!("Error when broadcasting gossip message: {e:?}"))?;

                Ok(Resume::Continue)
            }

            Effect::GetValue(height, round, timeout) => {
                let timeout_duration = timeouts.duration_for(timeout.step);

                // self.get_value(myself, height, round, timeout_duration)
                //     .map_err(|e| eyre!("Error when asking for value to be built: {e:?}"))?;

                Ok(Resume::Continue)
            }

            Effect::GetValidatorSet(height) => {
                // let validator_set = self
                //     .get_validator_set(height)
                //     .await
                //     .map_err(|e| warn!("No validator set found for height {height}: {e:?}"))
                //     .ok();

                Ok(Resume::ValidatorSet(height, None))
            }

            Effect::Decide {
                height,
                round,
                value,
                commits,
            } => {
                if let Some(tx_decision) = &self.tx_decision {
                    let _ = tx_decision.send((height, round, value.clone())).await;
                }

                // self.host
                //     .cast(HostMsg::Decide {
                //         height,
                //         round,
                //         value,
                //         commits,
                //         consensus: myself.clone(),
                //     })
                //     .map_err(|e| eyre!("Error when sending decided value to host: {e:?}"))?;

                Ok(Resume::Continue)
            }
        }
    }
}

#[async_trait]
impl<Ctx> Actor for Consensus<Ctx>
where
    Ctx: SnapchainContext,
{
    type Msg = ConsensusMsg<Ctx>;
    type State = State<Ctx>;
    type Arguments = ();

    #[tracing::instrument(name = "consensus", skip_all)]
    async fn pre_start(
        &self,
        myself: ActorRef<ConsensusMsg<Ctx>>,
        _args: (),
    ) -> Result<State<Ctx>, ActorProcessingErr> {
        // let forward = forward(myself.clone(), Some(myself.get_cell()), Msg::GossipEvent).await?;
        //
        // self.gossip_consensus
        //     .cast(GossipConsensusMsg::Subscribe(forward))?;

        Ok(State {
            timers: Timers::new(myself),
            timeouts: Timeouts::new(self.timeout_config),
            consensus: ConsensusState::new(self.ctx.clone(), self.params.clone()),
            connected_validators: BTreeSet::new(),
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<ConsensusMsg<Ctx>>,
        state: &mut State<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        state.timers.cancel_all();
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
        myself: ActorRef<ConsensusMsg<Ctx>>,
        msg: ConsensusMsg<Ctx>,
        state: &mut State<Ctx>,
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
        state: &mut State<Ctx>,
    ) -> Result<(), ActorProcessingErr> {
        info!("Stopping...");

        state.timers.cancel_all();

        Ok(())
    }
}
