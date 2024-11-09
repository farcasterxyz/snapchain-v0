use crate::consensus::consensus::BlockProposer;
use crate::core::types::{ShardId, SnapchainShard};
use crate::proto::message::Message;
use crate::proto::rpc::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use crate::proto::rpc::{BlocksRequest, BlocksResponse};
use crate::proto::snapchain::Block;
use hex::ToHex;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Default)]
pub struct MySnapchainService {
    block_proposers: HashMap<SnapchainShard, Arc<Mutex<BlockProposer>>>,
}

impl MySnapchainService {
    pub fn new(
        block_proposers: HashMap<SnapchainShard, Arc<Mutex<BlockProposer>>>,
    ) -> MySnapchainService {
        MySnapchainService { block_proposers }
    }
}

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(&self, request: Request<Message>) -> Result<Response<Message>, Status> {
        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received a message");

        let response = Response::new(request.into_inner());
        Ok(response)
    }

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<BlocksResponse>, Status> {
        let shard_id = request.get_ref().shard_id;
        let shard = SnapchainShard::new(shard_id as u8);
        let block_proposer = self.block_proposers.get(&shard);
        let blocks = match block_proposer {
            None => vec![],
            Some(block_proposer) => {
                let block_proposer = block_proposer.lock().await;
                block_proposer.get_blocks().clone()
            }
        };
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        let blocks_in_range = blocks.into_iter().filter(|block| match &block.header {
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
        });
        let response = Response::new(BlocksResponse {
            blocks: blocks_in_range.collect(),
        });
        Ok(response)
    }
}
