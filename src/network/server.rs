use crate::core::error::HubError;
use crate::mempool::routing;
use crate::proto;
use crate::proto::hub_service_server::HubService;
use crate::proto::Block;
use crate::proto::HubEvent;
use crate::proto::{BlocksRequest, ShardChunksRequest, ShardChunksResponse, SubscribeRequest};
use crate::storage::db::PageOptions;
use crate::storage::store::engine::{MempoolMessage, Senders, ShardEngine};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::store::BlockStore;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use hex::ToHex;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub struct MyHubService {
    block_store: BlockStore,
    shard_stores: HashMap<u32, Stores>,
    shard_senders: HashMap<u32, Senders>,
    num_shards: u32,
    statsd_client: StatsdClientWrapper,
}

impl MyHubService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, Stores>,
        shard_senders: HashMap<u32, Senders>,
        statsd_client: StatsdClientWrapper,
        num_shards: u32,
    ) -> Self {
        Self {
            block_store,
            shard_senders,
            shard_stores,
            statsd_client,
            num_shards,
        }
    }

    async fn submit_message_internal(
        &self,
        message: proto::Message,
        bypass_validation: bool,
    ) -> Result<proto::Message, Status> {
        let fid = message.fid();
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let dst_shard = routing::route_message(fid, self.num_shards);

        let sender = match self.shard_senders.get(&dst_shard) {
            Some(sender) => sender,
            None => {
                return Err(Status::invalid_argument(
                    "no shard sender for fid".to_string(),
                ))
            }
        };

        let stores = match self.shard_stores.get(&dst_shard) {
            Some(store) => store,
            None => {
                return Err(Status::invalid_argument(
                    "no shard store for fid".to_string(),
                ))
            }
        };

        if !bypass_validation {
            // TODO: This is a hack to get around the fact that self cannot be made mutable
            let mut readonly_engine = ShardEngine::new(
                stores.db.clone(),
                stores.trie.clone(),
                1,
                StoreLimits::default(),
                self.statsd_client.clone(),
                100,
                200,
            );
            let result = readonly_engine.simulate_message(&message);

            if let Err(err) = result {
                return Err(Status::invalid_argument(format!(
                    "Invalid message: {}",
                    err.to_string()
                )));
            }
        }

        match sender
            .messages_tx
            .try_send(MempoolMessage::UserMessage(message.clone()))
        {
            Ok(_) => {
                self.statsd_client.count("rpc.submit_message.success", 1);
                info!("successfully submitted message");
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.statsd_client
                    .count("rpc.submit_message.channel_full", 1);
                return Err(Status::resource_exhausted("channel is full"));
            }
            Err(e) => {
                self.statsd_client.count("rpc.submit_message.failure", 1);
                info!("error sending: {:?}", e.to_string());
                return Err(Status::internal("failed to submit message"));
            }
        }

        Ok(message)
    }
}

#[tonic::async_trait]
impl HubService for MyHubService {
    async fn submit_message_with_options(
        &self,
        request: Request<proto::SubmitMessageRequest>,
    ) -> Result<Response<proto::SubmitMessageResponse>, Status> {
        let start_time = std::time::Instant::now();

        let hash = request
            .get_ref()
            .message
            .as_ref()
            .map(|msg| msg.hash.encode_hex::<String>())
            .unwrap_or_default();
        info!(%hash, "Received call to [submit_message_with_options] RPC");

        let proto::SubmitMessageRequest {
            message,
            bypass_validation,
        } = request.into_inner();

        let message = match message {
            Some(msg) => msg,
            None => return Err(Status::invalid_argument("Message is required")),
        };

        let response_message = self
            .submit_message_internal(message, bypass_validation.unwrap_or(false))
            .await?;

        let response = proto::SubmitMessageResponse {
            message: Some(response_message),
        };

        self.statsd_client.time(
            "rpc.submit_message_with_options.duration",
            start_time.elapsed().as_millis() as u64,
        );

        Ok(Response::new(response))
    }

    async fn submit_message(
        &self,
        request: Request<proto::Message>,
    ) -> Result<Response<proto::Message>, Status> {
        let start_time = std::time::Instant::now();

        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received call to [submit_message] RPC");

        let message = request.into_inner();
        let response_message = self.submit_message_internal(message, false).await?;

        self.statsd_client.time(
            "rpc.submit_message.duration",
            start_time.elapsed().as_millis() as u64,
        );

        Ok(Response::new(response_message))
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
                        _ = server_tx.send(Err(Status::from_error(Box::new(err)))).await;
                        break;
                    }
                    Ok(block_page) => {
                        for block in block_page.blocks {
                            if let Err(_) = server_tx.send(Ok(block)).await {
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
        // TODO(aditi): Incorporate event types
        info!("Received call to [subscribe] RPC");
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<HubEvent, Status>>(100);
        let events_txs = match request.get_ref().shard_index {
            Some(shard_id) => match self.shard_senders.get(&(shard_id)) {
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
                vec![self.shard_stores.get(&shard_id).cloned().unwrap()]
            }
            None => self.shard_stores.values().cloned().collect(),
        };

        let start_id = request.get_ref().from_id.unwrap_or(0);

        tokio::spawn(async move {
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
                        if let Err(_) = server_tx.send(Ok(event)).await {
                            return;
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
                    loop {
                        match event_rx.recv().await {
                            Ok(hub_event) => {
                                match tx.send(Ok(hub_event)).await {
                                    Ok(_) => {}
                                    Err(_) => {
                                        // This means the client hung up
                                        break;
                                    }
                                }
                            }
                            Err(err) => {
                                error!(
                                    { err = err.to_string() },
                                    "[subscribe] error receiving from event stream"
                                )
                            }
                        }
                    }
                });
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }
}
