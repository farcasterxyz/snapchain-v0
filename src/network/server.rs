use std::collections::HashMap;

use crate::core::error::HubError;
use crate::proto::hub_event::HubEvent;
use crate::proto::msg as message;
use crate::proto::rpc::snapchain_service_server::SnapchainService;
use crate::proto::rpc::{BlocksRequest, ShardChunksRequest, ShardChunksResponse, SubscribeRequest};
use crate::proto::snapchain::Block;
use crate::storage::db::PageOptions;
use crate::storage::store::engine::{MempoolMessage, Senders};
use crate::storage::store::stores::Stores;
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use hex::ToHex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{server, Request, Response, Status};
use tracing::info;

pub struct MySnapchainService {
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    shard_senders: HashMap<u32, Senders>,
    message_tx: mpsc::Sender<MempoolMessage>,
    statsd_client: StatsdClientWrapper,
}

impl MySnapchainService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, Stores>,
        shard_senders: HashMap<u32, Senders>,
        statsd_client: StatsdClientWrapper,
    ) -> Self {
        // TODO(aditi): This logic will change once a mempool exists
        let message_tx = shard_senders.get(&1u32).unwrap().messages_tx.clone();

        Self {
            block_store,
            shard_senders,
            shard_stores,
            message_tx,
            statsd_client,
        }
    }
}

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(
        &self,
        request: Request<message::Message>,
    ) -> Result<Response<message::Message>, Status> {
        let start_time = std::time::Instant::now();

        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received call to [submit_message] RPC");

        let message = request.into_inner();

        let result = self
            .message_tx
            .send(MempoolMessage::UserMessage(message.clone()))
            .await;

        match result {
            Ok(_) => {
                self.statsd_client.count("rpc.submit_message.success", 1);
            }
            Err(_) => {
                self.statsd_client.count("rpc.submit_message.failure", 1);
                return Err(Status::internal("failed to submit message"));
            }
        }

        let elapsed = start_time.elapsed().as_millis();

        let response = Response::new(message);

        self.statsd_client
            .time("rpc.submit_message.duration", elapsed as u64);

        Ok(response)
    }

    type GetBlocksStream = ReceiverStream<Result<Block, Status>>;

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<Self::GetBlocksStream>, Status> {
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<Block, Status>>(100);

        info!( {start_block_number, stop_block_number}, "Received call to [get_blocks] RPC");

        let block_store = self.block_store.clone();

        tokio::spawn(async move {
            let mut next_page_token = None;
            loop {
                match block_store.get_blocks(
                    start_block_number,
                    stop_block_number,
                    &PageOptions {
                        page_size: Some(100),
                        page_token: next_page_token,
                        reverse: false,
                    },
                ) {
                    Err(err) => {
                        server_tx.send(Err(Status::from_error(Box::new(err)))).await;
                        break;
                    }
                    Ok(block_page) => {
                        for block in block_page.blocks {
                            if let Err(err) = server_tx.send(Ok(block)).await {
                                break;
                            }
                        }

                        if block_page.next_page_token.is_none() {
                            break;
                        } else {
                            next_page_token = block_page.next_page_token;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
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

        let stores = self.shard_stores.get(&shard_index);
        match stores {
            None => Err(Status::from_error(Box::new(
                HubError::invalid_internal_state("Missing shard store"),
            ))),
            Some(stores) => {
                match stores
                    .shard_store
                    .get_shard_chunks(start_block_number, stop_block_number)
                {
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
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<HubEvent, Status>>(100);
        let events_txs = match request.get_ref().shard_index {
            Some(shard_id) => match self.shard_senders.get(&(shard_id as u32)) {
                None => {
                    return Err(Status::from_error(Box::new(
                        HubError::invalid_internal_state("Missing shard event tx"),
                    )))
                }
                Some(senders) => vec![senders.events_tx.clone()],
            },
            None => self
                .shard_senders
                .values()
                .map(|senders| senders.events_tx.clone())
                .collect(),
        };

        let shard_stores = match request.get_ref().shard_index {
            Some(shard_id) => {
                vec![self.shard_stores.get(&shard_id).unwrap()]
            }
            None => self.shard_stores.values().collect(),
        };

        let start_id = request.get_ref().from_id.unwrap_or(0);

        let mut page_token = None;
        for store in shard_stores {
            loop {
                // TODO(aditi): We should stop pulling the raw db out of the shard store and create a new store type for events to house the db.
                let old_events = HubEvent::get_events(
                    store.shard_store.db.clone(),
                    start_id,
                    None,
                    Some(PageOptions {
                        page_token: page_token.clone(),
                        page_size: None,
                        reverse: false,
                    }),
                )
                .unwrap();

                for event in old_events.events {
                    if let Err(err) = server_tx.send(Ok(event)).await {
                        return Err(Status::from_error(Box::new(err)));
                    }
                }

                page_token = old_events.next_page_token;
                if page_token.is_none() {
                    break;
                }
            }
        }

        // TODO(aditi): It's possible that events show up between when we finish reading from the db and the subscription starts. We don't handle this case in the current hub code, but we may want to down the line.
        for event_tx in events_txs {
            let tx = server_tx.clone();
            tokio::spawn(async move {
                let mut event_rx = event_tx.subscribe();
                while let Ok(hub_event) = event_rx.recv().await {
                    match tx.send(Ok(hub_event)).await {
                        Ok(_) => {}
                        Err(_) => {
                            // This means the client hung up
                            break;
                        }
                    }
                }
            });
        }

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }
}
