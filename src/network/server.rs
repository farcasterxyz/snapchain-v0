use std::collections::HashMap;

use crate::core::error::HubError;
use crate::proto::hub_event::HubEvent;
use crate::proto::msg as message;
use crate::proto::rpc::snapchain_service_server::SnapchainService;
use crate::proto::rpc::{
    BlocksRequest, BlocksResponse, ShardChunksRequest, ShardChunksResponse, SubscribeRequest,
};
use crate::storage::store::engine::MempoolMessage;
use crate::storage::store::shard::ShardStore;
use crate::storage::store::BlockStore;
use alloy::rpc::types::request;
use futures::stream::select_all;
use futures_util::pin_mut;
use hex::ToHex;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{server, Request, Response, Status};
use tracing::info;

pub struct MySnapchainService {
    message_tx: mpsc::Sender<MempoolMessage>,
    block_store: BlockStore,
    shard_stores: HashMap<u32, ShardStore>,
    shard_events: HashMap<u32, broadcast::Sender<HubEvent>>,
}

impl MySnapchainService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, ShardStore>,
        shard_events: HashMap<u32, broadcast::Sender<HubEvent>>,
        message_tx: mpsc::Sender<MempoolMessage>,
    ) -> Self {
        Self {
            block_store,
            shard_stores,
            shard_events,
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

        info!(hash, "Received call to [submit_message] RPC");

        let message = request.into_inner();
        self.message_tx
            .send(MempoolMessage::UserMessage(message.clone()))
            .await
            .unwrap(); // Do we need clone here? I think yes?

        let response = Response::new(message);
        Ok(response)
    }

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<BlocksResponse>, Status> {
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;

        info!( {start_block_number, stop_block_number}, "Received call to [get_blocks] RPC");

        match self
            .block_store
            .get_blocks(start_block_number, stop_block_number)
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
        // TODO(aditi): Write unit tests for these functions.
        let shard_index = request.get_ref().shard_id;
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;

        info!( {shard_index, start_block_number, stop_block_number},
            "Received call to [get_shard_chunks] RPC");

        let shard_store = self.shard_stores.get(&shard_index);
        match shard_store {
            None => Err(Status::from_error(Box::new(
                HubError::invalid_internal_state("Missing shard store"),
            ))),
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

    type SubscribeStream = ReceiverStream<Result<HubEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // TODO(aditi): Use [from_id]
        // TODO(aditi): Read older events from the db
        let (server_tx, client_rx) = mpsc::channel::<Result<HubEvent, Status>>(100);
        let events_txs = match request.get_ref().shard_index {
            Some(shard_id) => {
                // TODO(aditi): Fix error handling
                vec![self.shard_events.get(&(shard_id as u32)).unwrap().clone()]
            }
            None => self.shard_events.values().cloned().collect(),
        };

        for event_tx in events_txs {
            let tx = server_tx.clone();
            tokio::spawn(async move {
                let mut event_rx = event_tx.subscribe();
                while let Ok(hub_event) = event_rx.recv().await {
                    // TODO(aditi): Fix error handling
                    tx.send(Ok(hub_event)).await.unwrap();
                }
            });
        }

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }
}
