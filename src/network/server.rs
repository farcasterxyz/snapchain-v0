use super::rpc_extensions::{AsMessagesResponse, AsSingleMessageResponse};
use crate::connectors::onchain_events::L1Client;
use crate::core::error::HubError;
use crate::mempool::routing;
use crate::proto;
use crate::proto::hub_service_server::HubService;
use crate::proto::on_chain_event::Body;
use crate::proto::GetInfoResponse;
use crate::proto::HubEvent;
use crate::proto::UserNameProof;
use crate::proto::UserNameType;
use crate::proto::{Block, CastId, DbStats};
use crate::proto::{BlocksRequest, ShardChunksRequest, ShardChunksResponse, SubscribeRequest};
use crate::proto::{FidRequest, FidTimestampRequest};
use crate::proto::{GetInfoRequest, StorageLimitsResponse};
use crate::proto::{
    LinkRequest, LinksByFidRequest, Message, MessagesResponse, ReactionRequest,
    ReactionsByFidRequest, UserDataRequest, VerificationRequest,
};
use crate::storage::constants::OnChainEventPostfix;
use crate::storage::constants::RootPrefix;
use crate::storage::db::PageOptions;
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::message_bytes_decode;
use crate::storage::store::account::{
    CastStore, LinkStore, ReactionStore, UserDataStore, VerificationStore,
};
use crate::storage::store::engine::{MempoolMessage, Senders, ShardEngine};
use crate::storage::store::stores::Stores;
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
    message_router: Box<dyn routing::MessageRouter>,
    statsd_client: StatsdClientWrapper,
    l1_client: Option<Box<dyn L1Client>>,
}

impl MyHubService {
    pub fn new(
        block_store: BlockStore,
        shard_stores: HashMap<u32, Stores>,
        shard_senders: HashMap<u32, Senders>,
        statsd_client: StatsdClientWrapper,
        num_shards: u32,
        message_router: Box<dyn routing::MessageRouter>,
        l1_client: Option<Box<dyn L1Client>>,
    ) -> Self {
        Self {
            block_store,
            shard_senders,
            shard_stores,
            statsd_client,
            message_router,
            num_shards,
            l1_client,
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

        let dst_shard = self.message_router.route_message(fid, self.num_shards);

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
                stores.store_limits.clone(),
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

            // We're doing the ens validations here for now because we don't want ens resolution to be on the consensus critical path. Eventually this will move to the fname server.
            if let Some(message_data) = &message.data {
                match &message_data.body {
                    Some(proto::message_data::Body::UserDataBody(user_data)) => {
                        if user_data.r#type() == proto::UserDataType::Username {
                            if user_data.value.ends_with(".eth") {
                                self.validate_ens_username(fid, user_data.value.to_string())
                                    .await?;
                            }
                        };
                    }
                    Some(proto::message_data::Body::UsernameProofBody(proof)) => {
                        if proof.r#type() == UserNameType::UsernameTypeEnsL1 {
                            self.validate_ens_username_proof(fid, &proof).await?;
                        }
                    }
                    _ => {}
                }
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
                println!("error sending: {:?}", e.to_string());
                return Err(Status::internal("failed to submit message"));
            }
        }

        Ok(message)
    }

    fn get_stores_for(&self, fid: u64) -> Result<&Stores, Status> {
        let shard_id = self.message_router.route_message(fid, self.num_shards);
        match self.shard_stores.get(&shard_id) {
            Some(store) => Ok(store),
            None => Err(Status::invalid_argument(
                "no shard store for fid".to_string(),
            )),
        }
    }

    pub async fn validate_ens_username_proof(
        &self,
        fid: u64,
        proof: &UserNameProof,
    ) -> Result<(), Status> {
        match &self.l1_client {
            None => {
                // Fail validation, can be fixed with config change
                Err(Status::invalid_argument(
                    "unable to validate ens name because there's no l1 client",
                ))
            }
            Some(l1_client) => {
                let name = std::str::from_utf8(&proof.name)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                if !name.ends_with(".eth") {
                    return Err(Status::invalid_argument(
                        "invalid ens name, doesn't end with .eth",
                    ));
                }

                let resolved_ens_address = l1_client
                    .resolve_ens_name(name.to_string())
                    .await
                    .map_err(|err| Status::from_error(Box::new(err)))?
                    .to_vec();

                if resolved_ens_address != proof.owner {
                    return Err(Status::invalid_argument(
                        "invalid ens name, resolved address doesn't match proof owner address",
                    ));
                }

                let stores = self
                    .get_stores_for(fid)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                let id_register = stores
                    .onchain_event_store
                    .get_id_register_event_by_fid(fid)
                    .map_err(|err| Status::from_error(Box::new(err)))?;

                match id_register {
                    None => return Err(Status::invalid_argument("missing id registration")),
                    Some(id_register) => {
                        match id_register.body {
                            Some(Body::IdRegisterEventBody(id_register)) => {
                                // Check verified addresses if the resolved address doesn't match the custody address
                                if id_register.to != resolved_ens_address {
                                    let verification = VerificationStore::get_verification_add(
                                        &stores.verification_store,
                                        fid,
                                        &resolved_ens_address,
                                    )
                                    .map_err(|err| Status::from_error(Box::new(err)))?;

                                    match verification {
                                    None => Err(Status::invalid_argument(
                                        "invalid ens proof, no matching custody address or verified addresses",
                                    )),
                                    Some(_) => Ok(()),
                                }
                                } else {
                                    Ok(())
                                }
                            }
                            _ => return Err(Status::invalid_argument("missing id registration")),
                        }
                    }
                }
            }
        }
    }

    async fn validate_ens_username(&self, fid: u64, fname: String) -> Result<(), Status> {
        let stores = self
            .get_stores_for(fid)
            .map_err(|err| Status::from_error(Box::new(err)))?;
        let proof = UserDataStore::get_username_proof(
            &stores.user_data_store,
            &mut RocksDbTransactionBatch::new(),
            fname.as_bytes(),
        )
        .map_err(|err| Status::from_error(Box::new(err)))?;
        match proof {
            Some(proof) => self.validate_ens_username_proof(fid, &proof).await,
            None => Err(Status::invalid_argument(
                "missing username proof for username",
            )),
        }
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

        let mut message = request.into_inner();
        message_bytes_decode(&mut message);
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

    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        let mut num_fid_registrations = 0;
        let mut approx_size = 0;
        let mut num_messages = 0;
        for (_, shard_store) in self.shard_stores.iter() {
            approx_size += shard_store.db.approximate_size();
            num_messages += shard_store.trie.get_count(
                &shard_store.db,
                &mut RocksDbTransactionBatch::new(),
                &[],
            );
            num_fid_registrations += shard_store
                .db
                .count_keys_at_prefix(vec![
                    RootPrefix::OnChainEvent as u8,
                    OnChainEventPostfix::IdRegisterByFid as u8,
                ])
                .map_err(|err| Status::from_error(Box::new(err)))?
                as u64;
        }

        Ok(Response::new(GetInfoResponse {
            db_stats: Some(DbStats {
                num_fid_registrations,
                num_messages,
                approx_size,
            }),
        }))
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

    async fn get_cast(&self, request: Request<CastId>) -> Result<Response<proto::Message>, Status> {
        let cast_id = request.into_inner();
        let stores = self.get_stores_for(cast_id.fid)?;
        CastStore::get_cast_add(&stores.cast_store, cast_id.fid, cast_id.hash).as_response()
    }

    async fn get_casts_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        CastStore::get_cast_adds_by_fid(&stores.cast_store, request.fid, &options).as_response()
    }

    async fn get_all_cast_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .cast_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_reaction(
        &self,
        request: Request<ReactionRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::reaction_request::Target::TargetCastId(cast_id)) => {
                Some(proto::reaction_body::Target::TargetCastId(cast_id))
            }
            Some(proto::reaction_request::Target::TargetUrl(url)) => {
                Some(proto::reaction_body::Target::TargetUrl(url))
            }
            None => None,
        };
        ReactionStore::get_reaction_add(
            &stores.reaction_store,
            request.fid,
            request.reaction_type,
            target,
        )
        .as_response()
    }

    async fn get_reactions_by_fid(
        &self,
        request: Request<ReactionsByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        ReactionStore::get_reaction_adds_by_fid(
            &stores.reaction_store,
            request.fid,
            request.reaction_type.unwrap_or(0),
            &options,
        )
        .as_response()
    }

    async fn get_all_reaction_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .reaction_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link(&self, request: Request<LinkRequest>) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::link_request::Target::TargetFid(fid)) => {
                Some(proto::link_body::Target::TargetFid(fid))
            }
            None => None,
        };
        LinkStore::get_link_add(&stores.link_store, request.fid, request.link_type, target)
            .as_response()
    }

    async fn get_links_by_fid(
        &self,
        request: Request<LinksByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_adds_by_fid(
            &stores.link_store,
            request.fid,
            request.link_type.unwrap_or("".to_string()),
            &options,
        )
        .as_response()
    }

    async fn get_all_link_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .link_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_user_data(
        &self,
        request: Request<UserDataRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let user_data_type = proto::UserDataType::try_from(request.user_data_type)
            .map_err(|_| Status::invalid_argument("Invalid user data type"))?;
        UserDataStore::get_user_data_by_fid_and_type(
            &stores.user_data_store,
            request.fid,
            user_data_type,
        )
        .as_response()
    }

    async fn get_user_data_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        UserDataStore::get_user_data_adds_by_fid(
            &stores.user_data_store,
            request.fid,
            &options,
            None,
            None,
        )
        .as_response()
    }

    async fn get_all_user_data_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .user_data_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_verification(
        &self,
        request: Request<VerificationRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        VerificationStore::get_verification_add(
            &stores.verification_store,
            request.fid,
            &request.address,
        )
        .as_response()
    }

    async fn get_verifications_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        VerificationStore::get_verification_adds_by_fid(
            &stores.verification_store,
            request.fid,
            &options,
        )
        .as_response()
    }

    async fn get_all_verification_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .verification_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link_compact_state_message_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_compact_state_message_by_fid(&stores.link_store, request.fid, &options)
            .as_response()
    }

    async fn get_current_storage_limits_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<StorageLimitsResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let limits = stores
            .get_storage_limits(request.fid)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(limits))
    }
}
