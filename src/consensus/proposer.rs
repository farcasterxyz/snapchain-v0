use crate::core::types::{proto, Address, Height, ShardHash, ShardId, SnapchainShard, SnapchainValidator};
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
use tokio::time::Instant;
use tokio::{select, time};
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

    #[error("Error publishing block: {0}")]
    BlockPublishError(String),
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

    // Method for publishing a new block with enhanced error handling
    async fn publish_new_block(&self, block: Block) -> Result<(), BlockProposerError> {
        match self.block_tx.send(block.clone()).await {
            Err(err) => {
                let error_message = format!("Error publishing new block: {:#?}", err);
                error!("{}", error_message);
                Err(BlockProposerError::BlockPublishError(error_message)) // Return error
            }
            Ok(_) => Ok(()),
        }
    }

    // Syncing with the validator with enhanced error handling
    async fn sync_against_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), BlockProposerError> {
        let prev_block_number = self.engine.get_confirmed_height().block_number;

        match &validator.rpc_address {
            None => return Ok(()),
            Some(rpc_address) => {
                let destination_addr = format!("http://{}", rpc_address.clone());

                // Attempt to connect to the server and handle connection errors
                let mut rpc_client = SnapchainServiceClient::connect(destination_addr)
                    .await
                    .map_err(|e| BlockProposerError::RpcTransportError(e))?;

                let request = Request::new(BlocksRequest {
                    shard_id: self.shard_id.shard_id(),
                    start_block_number: prev_block_number + 1,
                    stop_block_number: None,
                });

                let missing_blocks = rpc_client.get_blocks(request).await.map_err(|e| BlockProposerError::RpcResponseError(e))?;
                for block in missing_blocks.get_ref().blocks.clone() {
                    self.engine.commit_block(block.clone());
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

        let previous_block = self.engine.get_last_block();
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
            timestamp: current_time(),
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
            self.publish_new_block(proposal.block().unwrap()).await.unwrap();
            self.engine.commit_block(proposal.block().unwrap());
            self.proposed_blocks.remove(&value);
            self.pending_chunks.remove(&height.block_number);
        }
    }

    fn get_confirmed_height(&self) -> Height {
        self.engine.get_confirmed_height()
    }

    async fn sync_against_validator(
        &mut self,
        validator: &SnapchainValidator,
    ) -> Result<(), BlockProposerError> {
        self.sync_against_validator(validator).await
    }
}
