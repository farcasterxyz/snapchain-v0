use crate::consensus::consensus::{BlockProposer, BlockStore};
use crate::core::types::{ShardId, SnapchainShard};
use crate::proto::message;
use crate::proto::rpc::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use crate::proto::rpc::{BlocksRequest, BlocksResponse};
use crate::proto::snapchain::Block;
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::store::get_blocks_in_range;
use hex::ToHex;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct MySnapchainService {
    message_tx: mpsc::Sender<message::Message>,
    db: Arc<RocksDB>,
}

impl MySnapchainService {
    pub fn new(db: Arc<RocksDB>, message_tx: mpsc::Sender<message::Message>) -> Self {
        Self { db, message_tx }
    }
}

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(
        &self,
        request: Request<message::Message>,
    ) -> Result<Response<message::Message>, Status> {
        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received a message");

        let message = request.into_inner();
        self.message_tx.send(message.clone()).await.unwrap(); // Do we need clone here? I think yes?

        let response = Response::new(message);
        Ok(response)
    }

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<BlocksResponse>, Status> {
        let shard_index = request.get_ref().shard_id;
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        match get_blocks_in_range(
            &self.db,
            &PageOptions::default(),
            shard_index,
            start_block_number,
            stop_block_number,
        ) {
            Err(err) => Err(Status::from_error(Box::new(err))),
            Ok(blocks) => {
                let response = Response::new(BlocksResponse {
                    blocks: blocks.blocks,
                });
                Ok(response)
            }
        }
    }
}
