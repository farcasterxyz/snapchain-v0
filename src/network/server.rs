use std::collections::HashMap;

use crate::core::types::{ShardId, SnapchainShard};
use crate::proto::message;
use crate::proto::rpc::snapchain_service_server::SnapchainService;
use crate::proto::rpc::{BlocksRequest, BlocksResponse, ShardChunksRequest, ShardChunksResponse};
use crate::storage::store::shard::ShardStore;
use crate::storage::store::BlockStore;
use clap::error;
use hex::ToHex;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct MySnapchainService {
    message_tx: mpsc::Sender<message::Message>,
    block_store: BlockStore,
    shard_stores: HashMap<u32, ShardStore>,
}

impl MySnapchainService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, ShardStore>,
        message_tx: mpsc::Sender<message::Message>,
    ) -> Self {
        Self {
            block_store,
            shard_stores,
            message_tx,
        }
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
        match self
            .block_store
            .get_blocks(start_block_number, stop_block_number, shard_index)
        {
            Err(err) => Err(Status::from_error(Box::new(err))),
            Ok(blocks) => {
                let response = Response::new(BlocksResponse { blocks });
                Ok(response)
            }
        }
    }

    async fn get_shard_chunks(
        &self,
        request: Request<ShardChunksRequest>,
    ) -> Result<Response<ShardChunksResponse>, Status> {
        let shard_index = request.get_ref().shard_id;
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        let shard_store = self.shard_stores.get(&shard_index);
        match shard_store {
            None => {
                // TODO(aditi): Fix this
                panic!("Missing shard store")
            }
            Some(shard_store) => {
                match shard_store.get_shard_chunks(start_block_number, stop_block_number) {
                    Err(err) => Err(Status::from_error(Box::new(err))),
                    Ok(shard_chunks) => {
                        let response = Response::new(ShardChunksResponse { shard_chunks });
                        Ok(response)
                    }
                }
            }
        }
    }
}
