use malachite_common::{ValidatorSet, Validity};
use std::collections::BTreeMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;

use async_trait::async_trait;
use libp2p::identity::ed25519::{Keypair, SecretKey};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use malachite_common::{
    Context, Extension, Round, SignedProposal, SignedProposalPart, SignedVote, Timeout, TimeoutStep,
};
use malachite_config::TimeoutConfig;
use malachite_consensus::{Effect, ProposedValue, Resume, SignedConsensusMsg};
use malachite_metrics::Metrics;

use crate::consensus::timers::{TimeoutElapsed, TimerScheduler};
use crate::core::types::proto::ShardHash;
use crate::core::types::proto::{Block, BlockHeader};
use crate::core::types::{
    proto, Address, Height, ShardId, SnapchainContext, SnapchainShard, SnapchainValidator,
    SnapchainValidatorContext, SnapchainValidatorSet,
};
use crate::network::gossip::GossipEvent;
use crate::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use crate::proto::rpc::BlocksRequest;
use crate::proto::snapchain::{FullProposal, ShardChunk, ShardHeader};
use crate::proto::{message, snapchain};
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::store::{
    get_blocks_in_range, get_current_height, put_block, BlockStorageError,
};
pub use malachite_consensus::Params as ConsensusParams;
pub use malachite_consensus::State as ConsensusState;
use prost::Message;
use ractor::time::send_after;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;
use tokio::{select, time};

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
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            private_key: hex::encode(SecretKey::generate()),
            shard_ids: "1".to_string(),
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

pub trait Proposer {
    // Create a new block/shard chunk for the given height that will be proposed for confirmation to the other validators
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal;
    // Receive a block/shard chunk proposed by another validator and return whether it is valid
    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity;

    // Consensus has confirmed the block/shard_chunk, apply it to the local state
    async fn decide(&mut self, height: Height, round: Round, value: ShardHash);
}

pub struct ShardProposer {
    shard_id: SnapchainShard,
    address: Address,
    chunks: Vec<ShardChunk>,
    proposed_chunks: BTreeMap<ShardHash, FullProposal>,
    tx_decision: Option<TxDecision>,
    messages_tx: mpsc::Sender<message::Message>,
    messages_rx: mpsc::Receiver<message::Message>,
}

impl ShardProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        tx_decision: Option<TxDecision>,
    ) -> ShardProposer {
        let (messages_tx, mut messages_rx) = mpsc::channel::<message::Message>(100);

        ShardProposer {
            shard_id,
            address,
            chunks: vec![],
            proposed_chunks: BTreeMap::new(),
            tx_decision,
            messages_tx,
            messages_rx,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<message::Message> {
        self.messages_tx.clone()
    }
}

impl Proposer for ShardProposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        _timeout: Duration,
    ) -> FullProposal {
        // Sleep before proposing the value so we don't produce blocks too fast
        // TODO: rethink/reconsider
        tokio::time::sleep(Duration::from_millis(250)).await;

        let previous_chunk = self.chunks.last();
        let parent_hash = match previous_chunk {
            Some(chunk) => chunk.hash.clone(),
            None => vec![0, 32],
        };
        let shard_header = ShardHeader {
            parent_hash,
            timestamp: 0,
            height: Some(height.clone()),
            shard_root: vec![],
        };
        let hash = blake3::hash(&shard_header.encode_to_vec())
            .as_bytes()
            .to_vec();

        let it = iter::from_fn(|| self.messages_rx.try_recv().ok());
        let user_messages: Vec<message::Message> = it.collect();

        // TODO: remove
        if user_messages.len() > 0 {
            debug!(count = user_messages.len(), "got fc messages");
        }

        let chunk = ShardChunk {
            header: Some(shard_header),
            hash: hash.clone(),
            transactions: vec![snapchain::Transaction {
                fid: 1234,                      //TODO
                account_root: vec![5, 5, 6, 6], //TODO
                system_messages: vec![],        //TODO
                user_messages,
            }],
            votes: None,
        };

        let shard_hash = ShardHash {
            hash: hash.clone(),
            shard_index: height.shard_index as u32,
        };
        let proposal = FullProposal {
            height: Some(height.clone()),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Shard(chunk)),
            proposer: self.address.to_vec(),
        };
        self.proposed_chunks.insert(shard_hash, proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Shard(_)) =
            full_proposal.proposed_value.clone()
        {
            self.proposed_chunks
                .insert(full_proposal.shard_hash(), full_proposal.clone());
        }
        Validity::Valid // TODO: Validate proposer signature?
    }

    async fn decide(&mut self, _height: Height, _round: Round, value: ShardHash) {
        if let Some(proposal) = self.proposed_chunks.get(&value) {
            if let Some(tx_decision) = &self.tx_decision {
                let _ = tx_decision.send(proposal.clone()).await;
            }
            self.chunks.push(proposal.shard_chunk().unwrap());
            self.proposed_chunks.remove(&value);
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockProposerError {
    #[error("Block missing header")]
    BlockMissingHeader,

    #[error("Block missing height")]
    BlockMissingHeight,

    #[error("No peers")]
    NoPeers,

    #[error(transparent)]
    RpcTransportError(#[from] tonic::transport::Error),

    #[error(transparent)]
    RpcResponseError(#[from] tonic::Status),

    #[error(transparent)]
    BlockStorageError(#[from] BlockStorageError),
}
#[derive(Default, Clone)]
pub struct BlockStore {
    db: Arc<RocksDB>,
}

impl BlockStore {
    pub fn new(db: Arc<RocksDB>) -> BlockStore {
        BlockStore { db }
    }

    pub fn put_block(&self, block: Block) -> Result<(), BlockProposerError> {
        put_block(&self.db, block).map_err(|err| BlockProposerError::BlockStorageError(err))
    }

    pub fn max_block_number(&self, shard_index: u32) -> Result<u64, BlockProposerError> {
        let current_height = get_current_height(&self.db, shard_index)?;
        match current_height {
            None => Ok(0),
            Some(height) => Ok(height),
        }
    }

    pub fn get_blocks(
        &self,
        start_block_number: u64,
        stop_block_number: Option<u64>,
        shard_index: u32,
    ) -> Result<Vec<Block>, BlockProposerError> {
        let blocks = get_blocks_in_range(
            &self.db,
            &PageOptions::default(),
            shard_index,
            start_block_number,
            stop_block_number,
        )?;
        let blocks_in_range = blocks
            .blocks
            .into_iter()
            .filter(|block| match &block.header {
                None => false,
                Some(header) => match &header.height {
                    None => false,
                    Some(height) => match stop_block_number {
                        None => height.block_number >= start_block_number,
                        Some(stop_block_number) => {
                            height.block_number >= start_block_number
                                && height.block_number <= stop_block_number
                        }
                    },
                },
            })
            .collect();
        Ok(blocks_in_range)
    }
}

pub struct BlockProposer {
    shard_id: SnapchainShard,
    address: Address,
    blocks: Vec<Block>,
    proposed_blocks: BTreeMap<ShardHash, FullProposal>,
    pending_chunks: BTreeMap<u64, Vec<ShardChunk>>,
    shard_decision_rx: RxDecision,
    num_shards: u32,
    block_tx: mpsc::Sender<Block>,
    block_store: BlockStore,
}

impl BlockProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        shard_decision_rx: RxDecision,
        num_shards: u32,
        block_tx: mpsc::Sender<Block>,
        block_store: BlockStore,
    ) -> BlockProposer {
        BlockProposer {
            shard_id,
            address,
            blocks: vec![],
            proposed_blocks: BTreeMap::new(),
            pending_chunks: BTreeMap::new(),
            shard_decision_rx,
            num_shards,
            block_tx,
            block_store,
        }
    }

    async fn collect_confirmed_shard_chunks(
        &mut self,
        height: Height,
        timeout: Duration,
    ) -> Vec<ShardChunk> {
        let requested_height = height.block_number;

        let mut poll_interval = time::interval(Duration::from_millis(10));

        // convert to deadline
        let deadline = Instant::now() + timeout;
        loop {
            let timeout = time::sleep_until(deadline);
            select! {
                _ = poll_interval.tick() => {
                    if let Ok(decision) = self.shard_decision_rx.try_recv() {
                       if let Some(proto::full_proposal::ProposedValue::Shard(chunk)) = decision.proposed_value {
                            let chunk_height = chunk.header.clone().unwrap().height.unwrap();
                            let chunk_block_number = chunk_height.block_number;
                            if self.pending_chunks.contains_key(&chunk_block_number) {
                                self.pending_chunks.get_mut(&chunk_block_number).unwrap().push(chunk);
                            } else {
                                self.pending_chunks.insert(chunk_block_number, vec![chunk]);
                            }
                        }
                    }
                    if let Some(chunks) = self.pending_chunks.get(&requested_height) {
                        if chunks.len() == self.num_shards as usize {
                            break;
                        }
                    }
                }
                _ = timeout => {
                    warn!("Block validator did not receive all shard chunks in time for height: {:?}", requested_height);
                    break;
                }
            }
        }

        if let Some(chunks) = self.pending_chunks.get(&requested_height) {
            chunks.clone()
        } else {
            vec![]
        }
    }

    async fn publish_new_block(&self, block: Block) {
        match self.block_tx.send(block.clone()).await {
            Err(err) => {
                error!("Erorr publishing new block {:#?}", err)
            }
            Ok(_) => {}
        }
    }

    pub async fn register_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), BlockProposerError> {
        let prev_block = self.blocks.last();
        let prev_block_number = match prev_block {
            None => 0,
            Some(prev_block) => {
                let header = prev_block
                    .header
                    .as_ref()
                    .ok_or(BlockProposerError::BlockMissingHeader)?;
                let height = header
                    .height
                    .as_ref()
                    .ok_or(BlockProposerError::BlockMissingHeight)?;
                height.block_number
            }
        };

        if validator.current_height > prev_block_number {
            match &validator.rpc_address {
                None => return Ok(()),
                Some(rpc_address) => {
                    let destination_addr = format!("http://{}", rpc_address.clone());
                    let mut rpc_client = SnapchainServiceClient::connect(destination_addr).await?;
                    let request = Request::new(BlocksRequest {
                        shard_id: self.shard_id.shard_id(),
                        start_block_number: prev_block_number + 1,
                        stop_block_number: None,
                    });
                    let missing_blocks = rpc_client.get_blocks(request).await?;
                    for block in missing_blocks.get_ref().blocks.clone() {
                        self.blocks.push(block.clone());
                        self.publish_new_block(block).await;
                    }
                }
            }
        }

        Ok(())
    }
}

impl Proposer for BlockProposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal {
        let shard_chunks = self.collect_confirmed_shard_chunks(height, timeout).await;

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
            height: Some(height.clone()),
        };
        let hash = blake3::hash(&block_header.encode_to_vec())
            .as_bytes()
            .to_vec();

        let block = Block {
            header: Some(block_header),
            hash: hash.clone(),
            validators: None,
            votes: None,
            shard_chunks,
        };

        let shard_hash = ShardHash {
            hash: hash.clone(),
            shard_index: height.shard_index as u32,
        };

        let proposal = FullProposal {
            height: Some(height.clone()),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Block(block)),
            proposer: self.address.to_vec(),
        };

        self.proposed_blocks.insert(shard_hash, proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Block(_block)) =
            full_proposal.proposed_value.clone()
        {
            self.proposed_blocks
                .insert(full_proposal.shard_hash(), full_proposal.clone());
        }
        Validity::Valid // TODO: Validate proposer signature?
    }

    async fn decide(&mut self, height: Height, _round: Round, value: ShardHash) {
        if let Some(proposal) = self.proposed_blocks.get(&value) {
            self.publish_new_block(proposal.block().unwrap()).await;
            self.blocks.push(proposal.block().unwrap());
            self.proposed_blocks.remove(&value);
            self.pending_chunks.remove(&height.block_number);
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
    // This should be proposer: Box<dyn Proposer> but that doesn't implement Send which is required for the actor system.
    // TODO: Fix once we remove the actor system
    block_proposer: Option<BlockProposer>,
    shard_proposer: Option<ShardProposer>,
    started: bool,
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
        }
    }

    pub fn get_validator_set(&self) -> SnapchainValidatorSet {
        self.validator_set.clone()
    }

    pub fn get_current_height(&self) -> Result<u64, BlockProposerError> {
        match &self.block_proposer {
            None => Ok(0),
            Some(block_proposer) => block_proposer
                .block_store
                .max_block_number(self.shard_id.shard_id()),
        }
    }

    pub fn add_validator(&mut self, validator: SnapchainValidator) -> bool {
        self.validator_set.add(validator)
    }

    pub fn start(&mut self) {
        self.started = true;
    }

    pub async fn sync_with_new_validator(&mut self, validator: &SnapchainValidator) {
        match &mut self.block_proposer {
            None => {}
            Some(block_proposer) => match block_proposer.register_validator(&validator).await {
                Ok(()) => {}
                Err(err) => error!("Error registering validator {:#?}", err),
            },
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

                let connected_peers = state.shard_validator.validator_set.count();
                info!(
                    "Connected to peer {address}. Total peers: {:?}",
                    connected_peers
                );
                // let total_peers = state.consensus.driver.validator_set().count() - 1;

                // println!("Connected to {connected_peers}/{total_peers} peers");
                state
                    .shard_validator
                    .sync_with_new_validator(&validator)
                    .await;

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
            state.shard_validator.get_current_height()?,
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
