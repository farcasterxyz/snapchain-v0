use crate::core::types::{proto, Address, Height, ShardHash, SnapchainShard, SnapchainValidator};
use crate::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use crate::proto::rpc::{BlocksRequest, ShardChunksRequest};
use crate::proto::snapchain::{Block, BlockHeader, FullProposal, ShardChunk, ShardHeader};
use crate::storage::store::engine::{BlockEngine, ShardEngine, ShardStateChange};
use crate::storage::store::BlockStorageError;
use malachite_common::{Round, Validity};
use prost::Message;
use std::collections::BTreeMap;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{Instant, interval};
use tonic::Request;
use tracing::{error, warn};

const FARCASTER_EPOCH: u64 = 1609459200; // January 1, 2021 UTC

pub fn current_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - FARCASTER_EPOCH
}

pub trait Proposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        timeout: Duration,
    ) -> FullProposal;

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity;

    async fn decide(&mut self, height: Height, round: Round, value: ShardHash);

    fn get_confirmed_height(&self) -> Height;

    async fn register_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct ShardProposer {
    shard_id: SnapchainShard,
    address: Address,
    proposed_chunks: BTreeMap<ShardHash, FullProposal>,
    tx_decision: mpsc::Sender<ShardChunk>,
    engine: ShardEngine,
    propose_value_delay: Duration,
}

impl ShardProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        engine: ShardEngine,
        tx_decision: mpsc::Sender<ShardChunk>,
        propose_value_delay: Duration,
    ) -> ShardProposer {
        ShardProposer {
            shard_id,
            address,
            proposed_chunks: BTreeMap::new(),
            tx_decision,
            engine,
            propose_value_delay,
        }
    }

    async fn publish_new_shard_chunk(&self, shard_chunk: ShardChunk) {
        if let Err(err) = self.tx_decision.send(shard_chunk).await {
            error!("Error publishing new shard chunk: {:?}", err);
        }
    }
}

impl Proposer for ShardProposer {
    async fn propose_value(
        &mut self,
        height: Height,
        round: Round,
        _timeout: Duration,
    ) -> FullProposal {
        tokio::time::sleep(self.propose_value_delay).await;

        let previous_chunk = self.engine.get_last_shard_chunk();
        let parent_hash = previous_chunk.map_or_else(|| vec![0, 32], |chunk| chunk.hash.clone());

        let state_change = self.engine.propose_state_change(self.shard_id.shard_id());
        let shard_header = ShardHeader {
            parent_hash,
            timestamp: current_time(),
            height: Some(height.clone()),
            shard_root: state_change.new_state_root.clone(),
        };
        let hash = blake3::hash(&shard_header.encode_to_vec()).as_bytes().to_vec();

        let chunk = ShardChunk {
            header: Some(shard_header),
            hash: hash.clone(),
            transactions: state_change.transactions.clone(),
            votes: None,
        };

        let shard_hash = ShardHash {
            hash: hash.clone(),
            shard_index: height.shard_index as u32,
        };
        let proposal = FullProposal {
            height: Some(height),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Shard(chunk)),
            proposer: self.address.to_vec(),
        };

        self.proposed_chunks.insert(shard_hash, proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Shard(chunk)) = &full_proposal.proposed_value {
            self.proposed_chunks.insert(full_proposal.shard_hash(), full_proposal.clone());

            let state = ShardStateChange {
                shard_id: chunk.header.clone().unwrap().height.unwrap().shard_index,
                new_state_root: chunk.header.clone().unwrap().shard_root.clone(),
                transactions: chunk.transactions.clone(),
            };

            if self.engine.validate_state_change(&state) {
                Validity::Valid
            } else {
                error!("Invalid state change for shard: {:?}", state.shard_id);
                Validity::Invalid
            }
        } else {
            error!("Invalid proposed value: {:?}", full_proposal.proposed_value);
            Validity::Invalid
        }
    }

    async fn decide(&mut self, _height: Height, _round: Round, value: ShardHash) {
        if let Some(proposal) = self.proposed_chunks.remove(&value) {
            if let Some(shard_chunk) = proposal.shard_chunk() {
                self.publish_new_shard_chunk(shard_chunk).await;
                self.engine.commit_shard_chunk(shard_chunk);
            }
        }
    }

    fn get_confirmed_height(&self) -> Height {
        self.engine.get_confirmed_height()
    }

    async fn register_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let prev_block_number = self.engine.get_confirmed_height().block_number;

        if validator.current_height > prev_block_number {
            if let Some(rpc_address) = &validator.rpc_address {
                let destination_addr = format!("http://{}", rpc_address);
                let mut rpc_client = SnapchainServiceClient::connect(destination_addr).await?;
                let request = Request::new(ShardChunksRequest {
                    shard_id: self.shard_id.shard_id(),
                    start_block_number: prev_block_number + 1,
                    stop_block_number: None,
                });
                let missing_shard_chunks = rpc_client.get_shard_chunks(request).await?;

                for shard_chunk in missing_shard_chunks.get_ref().shard_chunks.clone() {
                    self.engine.commit_shard_chunk(shard_chunk);
                }
            }
        }

        Ok(())
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

pub struct BlockProposer {
    shard_id: SnapchainShard,
    address: Address,
    proposed_blocks: BTreeMap<ShardHash, FullProposal>,
    pending_chunks: BTreeMap<u64, Vec<ShardChunk>>,
    shard_decision_rx: mpsc::Receiver<ShardChunk>,
    num_shards: u32,
    block_tx: mpsc::Sender<Block>,
    engine: BlockEngine,
}

impl BlockProposer {
    pub fn new(
        address: Address,
        shard_id: SnapchainShard,
        shard_decision_rx: mpsc::Receiver<ShardChunk>,
        num_shards: u32,
        block_tx: mpsc::Sender<Block>,
        engine: BlockEngine,
    ) -> BlockProposer {
        BlockProposer {
            shard_id,
            address,
            proposed_blocks: BTreeMap::new(),
            pending_chunks: BTreeMap::new(),
            shard_decision_rx,
            num_shards,
            block_tx,
            engine,
        }
    }

    async fn collect_confirmed_shard_chunks(
        &mut self,
        height: Height,
        timeout: Duration,
    ) -> Vec<ShardChunk> {
        let requested_height = height.block_number;
        let mut poll_interval = interval(Duration::from_millis(10));
        let deadline = Instant::now() + timeout;

        loop {
            let timeout = time::sleep_until(deadline);
            select! {
                _ = poll_interval.tick() => {
                    if let Ok(chunk) = self.shard_decision_rx.try_recv() {
                        let chunk_height = chunk.header.clone().unwrap().height.unwrap().block_number;
                        self.pending_chunks.entry(chunk_height).or_insert_with(Vec::new).push(chunk);
                    }
                    if let Some(chunks) = self.pending_chunks.get(&requested_height) {
                        if chunks.len() == self.num_shards as usize {
                            break;
                        }
                    }
                }
                _ = timeout => {
                    warn!("Timeout: Block validator did not receive all shard chunks in time for height: {:?}", requested_height);
                    break;
                }
            }
        }

        self.pending_chunks.remove(&requested_height).unwrap_or_default()
    }

    async fn publish_new_block(&self, block: Block) {
        if let Err(err) = self.block_tx.send(block.clone()).await {
            error!("Error publishing new block {:#?}", err);
        }
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

        let previous_block = self.engine.get_last_block();
        let parent_hash = previous_block.map_or_else(|| vec![0, 32], |block| block.hash.clone());

        let block_header = BlockHeader {
            parent_hash,
            chain_id: 0,
            version: 0,
            shard_headers_hash: vec![],
            validators_hash: vec![],
            timestamp: current_time(),
            height: Some(height.clone()),
        };
        let hash = blake3::hash(&block_header.encode_to_vec()).as_bytes().to_vec();

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
            height: Some(height),
            round: round.as_i64(),
            proposed_value: Some(proto::full_proposal::ProposedValue::Block(block)),
            proposer: self.address.to_vec(),
        };

        self.proposed_blocks.insert(shard_hash, proposal.clone());
        proposal
    }

    fn add_proposed_value(&mut self, full_proposal: &FullProposal) -> Validity {
        if let Some(proto::full_proposal::ProposedValue::Block(_block)) = &full_proposal.proposed_value {
            self.proposed_blocks.insert(full_proposal.shard_hash(), full_proposal.clone());
        }
        Validity::Valid // TODO: Validate proposer signature?
    }

    async fn decide(&mut self, height: Height, _round: Round, value: ShardHash) {
        if let Some(proposal) = self.proposed_blocks.remove(&value) {
            self.publish_new_block(proposal.block().unwrap()).await;
            self.engine.commit_block(proposal.block().unwrap());
            self.pending_chunks.remove(&height.block_number);
        }
    }

    fn get_confirmed_height(&self) -> Height {
        self.engine.get_confirmed_height()
    }

    async fn register_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let prev_block_number = self.engine.get_confirmed_height().block_number;

        if validator.current_height > prev_block_number {
            if let Some(rpc_address) = &validator.rpc_address {
                let destination_addr = format!("http://{}", rpc_address);
                let mut rpc_client = SnapchainServiceClient::connect(destination_addr).await?;
                let request = Request::new(BlocksRequest {
                    shard_id: self.shard_id.shard_id(),
                    start_block_number: prev_block_number + 1,
                    stop_block_number: None,
                });
                let missing_blocks = rpc_client.get_blocks(request).await?;

                for block in missing_blocks.get_ref().blocks.clone() {
                    self.engine.commit_block(block);
                }
            }
        }

        Ok(())
    }
}
