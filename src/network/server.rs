use crate::consensus::consensus::{BlockProposer, BlockStore};
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
    block_store: Arc<Mutex<BlockStore>>,
}

impl MySnapchainService {
    pub fn new(block_store: Arc<Mutex<BlockStore>>) -> MySnapchainService {
        MySnapchainService { block_store }
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
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        let block_store = self.block_store.lock().await;
        let blocks = block_store.get_blocks(start_block_number, stop_block_number);
        let response = Response::new(BlocksResponse { blocks });
        Ok(response)
    }
}
