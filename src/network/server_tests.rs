#[cfg(test)]
mod tests {
    use crate::connectors::onchain_events::ens::EnsError;
    use alloy_primitives::keccak256;
    use async_trait::async_trait;
    use base64::Engine;
    use prost::Message;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, timeout};

    use crate::connectors::fname::{FetchError, FnameTransferLookup};
    use crate::connectors::onchain_events::{Chain, ChainAPI, ChainClients};
    use crate::core::validations::{self, verification::VerificationAddressClaim};
    use crate::mempool::mempool::{self, Mempool};
    use crate::mempool::routing;
    use crate::mempool::routing::MessageRouter;
    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::{
        self, Block, ChannelMemberRequest, ChannelMembersRequest, ChannelMembershipsByFidRequest,
        ChannelModerationsRequest, ChannelOwnerRequest, ChannelOwnerResponse,
        ChannelRegisterEventType, ChannelRequest, ChannelsByAddressRequest, ChannelsByFidRequest,
        EventRequest, EventsRequest, FarcasterNetwork, FnameTransfer, HubEvent, HubEventType,
        OnChainEventType, ShardChunk, StorageUnitType, SubmitBulkMessagesRequest,
        SubmitBulkMessagesResponse, UserDataType, UserNameProof, UserNameType,
        UsernameProofRequest, VerificationAddAddressBody,
    };
    use crate::proto::{FidRequest, SignersByFidRequest, SubscribeRequest};
    use crate::storage::constants::RootPrefix;
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        make_message_primary_key, make_ts_hash, ChannelMemberStore, ChannelModerateStore,
        ChannelPinStore, ChannelUpdateStore, DerivedIndexGate, HubEventIdGenerator,
        HubEventStorageExt, ReactionStoreDef, VerificationStoreDef, CHANNEL_ID_LENGTH,
        CHANNEL_MEMBER_SLOT_CAP, CHANNEL_MODERATE_SLOT_CAP, SEQUENCE_BITS,
    };
    use crate::storage::store::block_engine::BlockEngine;
    use crate::storage::store::block_engine_test_helpers::{BlockEngineOptions, Validity};
    use crate::storage::store::engine::{Senders, ShardEngine};
    use crate::storage::store::stores::Stores;
    use crate::storage::store::test_helper::{commit_event, register_user};
    use crate::storage::store::{block_engine_test_helpers, test_helper};
    use crate::storage::trie::merkle_trie;
    use crate::storage::util::increment_vec_u8;
    use crate::utils::factory::signers::generate_signer;
    use crate::utils::factory::{events_factory, messages_factory};
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::future;
    use futures::StreamExt;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    const SHARD1_FID: u64 = test_helper::SHARD1_FID;
    const SHARD2_FID: u64 = test_helper::SHARD2_FID;

    const USER_NAME: &str = "user";
    const PASSWORD: &str = "password";

    fn fid_request(fid: u64) -> Request<FidRequest> {
        Request::new(FidRequest {
            fid,
            page_size: None,
            page_token: None,
            reverse: None,
        })
    }

    fn now_unix_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn channel_label(channel_key: &str) -> Vec<u8> {
        keccak256(channel_key.as_bytes()).to_vec()
    }

    fn owner_address(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn channel_rows(db: &RocksDB) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let prefix = vec![RootPrefix::Channel as u8];
        let mut rows = BTreeMap::new();
        db.for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            &PageOptions::default(),
            |key, value| {
                rows.insert(key.to_vec(), value.to_vec());
                Ok(false)
            },
        )
        .unwrap();
        rows
    }

    fn merge_channel_registration(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Register,
                1,
                1,
            ),
        );
    }

    fn merge_channel_transfer(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Transfer,
                2,
                1,
            ),
        );
    }

    fn merge_channel_event(block_engine: &BlockEngine, event: proto::OnChainEvent) {
        let block_stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        block_stores
            .onchain_event_store
            .merge_onchain_event(event, &mut txn)
            .unwrap();
        block_stores.onchain_event_store.db.commit(txn).unwrap();
    }

    fn merge_channel_message(block_engine: &BlockEngine, message: &proto::Message) {
        let block_stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        match message.msg_type() {
            proto::MessageType::ChannelUpdate => {
                ChannelUpdateStore::merge(
                    &block_stores.channel_update_store,
                    message,
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap();
            }
            proto::MessageType::ChannelMember => {
                ChannelMemberStore::merge(
                    &block_stores.channel_member_store,
                    message,
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap();
            }
            proto::MessageType::ChannelPin => {
                ChannelPinStore::merge(
                    &block_stores.channel_pin_store,
                    message,
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap();
            }
            proto::MessageType::ChannelModerate => {
                ChannelModerateStore::merge(
                    &block_stores.channel_moderate_store,
                    message,
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap();
            }
            other => panic!("unexpected channel message type: {other:?}"),
        }
        block_stores.db.commit(txn).unwrap();
    }

    async fn get_channel_owner_response(
        service: &MyHubService,
        channel_key: &str,
    ) -> ChannelOwnerResponse {
        service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: channel_key.to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    async fn get_channels_by_fid_keys(service: &MyHubService, fid: u64) -> Vec<String> {
        service
            .get_channels_by_fid(Request::new(ChannelsByFidRequest {
                fid,
                page_size: None,
                page_token: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .channels
            .into_iter()
            .map(|channel| channel.channel_key)
            .collect()
    }

    async fn assert_channel_owner_fid_invariant(
        service: &MyHubService,
        channel_key: &str,
        winner_fid: u64,
        loser_fid: Option<u64>,
    ) {
        let owner = get_channel_owner_response(service, channel_key).await;
        assert_eq!(owner.fid, winner_fid);

        if winner_fid != 0 {
            assert!(get_channels_by_fid_keys(service, winner_fid)
                .await
                .contains(&channel_key.to_string()));
        }
        if let Some(loser_fid) = loser_fid {
            assert!(!get_channels_by_fid_keys(service, loser_fid)
                .await
                .contains(&channel_key.to_string()));
        }
    }

    fn verification_add(fid: u64, address: Vec<u8>, timestamp: u32) -> proto::Message {
        messages_factory::verifications::create_verification_add(
            fid,
            0,
            address,
            vec![],
            vec![],
            Some(timestamp),
            None,
        )
    }

    fn verification_remove(fid: u64, address: Vec<u8>, timestamp: u32) -> proto::Message {
        messages_factory::verifications::create_verification_remove(
            fid,
            address,
            Some(timestamp),
            None,
        )
    }

    fn channels_by_address_request(address: Vec<u8>) -> Request<ChannelsByAddressRequest> {
        Request::new(ChannelsByAddressRequest {
            owner_address: address,
            page_size: None,
            page_token: None,
            reverse: None,
        })
    }

    fn channels_by_fid_request(fid: u64, page_size: Option<u32>) -> Request<ChannelsByFidRequest> {
        Request::new(ChannelsByFidRequest {
            fid,
            page_size,
            page_token: None,
        })
    }

    fn merge_verification(stores: &Stores, verification: &proto::Message) {
        let mut txn = RocksDbTransactionBatch::new();
        stores
            .verification_store
            .merge(verification, &mut txn, &test_helper::default_merge_ctx())
            .unwrap();
        stores.db.commit(txn).unwrap();
    }

    fn merge_shard_zero_verification(block_engine: &BlockEngine, verification: &proto::Message) {
        let stores = block_engine.stores();
        let mut txn = RocksDbTransactionBatch::new();
        stores
            .verification_store
            .merge(verification, &mut txn, &test_helper::default_merge_ctx())
            .unwrap();
        stores.db.commit(txn).unwrap();
    }

    fn merge_channel_owner_verification(
        stores: &Stores,
        block_engine: &BlockEngine,
        verification: &proto::Message,
    ) {
        merge_verification(stores, verification);
        merge_shard_zero_verification(block_engine, verification);
    }

    struct MockL1Client {}

    #[async_trait]
    impl ChainAPI for MockL1Client {
        async fn resolve_ens_name(
            &self,
            name: String,
        ) -> Result<alloy_primitives::Address, EnsError> {
            let address_str = match name.as_str() {
                "username.eth" => "91031dcfdea024b4d51e775486111d2b2a715871",
                "username.base.eth" => "849151d7D0bF1F34b70d5caD5149D28CC2308bf1",
                _ => return Err(EnsError::ResolverNotFound(name)),
            };
            let addr = alloy_primitives::Address::from_slice(&hex::decode(address_str).unwrap());
            future::ready(Ok(addr)).await
        }

        async fn verify_contract_signature(
            &self,
            _claim: VerificationAddressClaim,
            _body: &VerificationAddAddressBody,
        ) -> Result<(), validations::error::ValidationError> {
            future::ready(Ok(())).await
        }
    }

    async fn subscribe_and_listen(
        service: &MyHubService,
        shard_id: u32,
        from_id: Option<u64>,
        num_events_expected: u64,
        event_types: Vec<i32>,
    ) -> tokio::task::JoinHandle<()> {
        let request = Request::new(SubscribeRequest {
            event_types,
            from_id,
            shard_index: Some(shard_id),
        });
        let mut listener = service.subscribe(request).await.unwrap();

        let mut num_events_seen = 0;

        return tokio::spawn(async move {
            loop {
                let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
                if let Ok(Some(Ok(hub_event))) = event {
                    let block_number = hub_event.block_number;
                    assert!(block_number > 0);
                    assert!(hub_event.shard_index > 0);
                    num_events_seen += 1;
                    if num_events_seen == num_events_expected {
                        break;
                    }
                } else {
                    if num_events_seen == num_events_expected {
                        break;
                    }
                }
            }
            assert_eq!(num_events_seen, num_events_expected);
        });
    }

    async fn send_events(events_tx: broadcast::Sender<HubEvent>, num_events: u64) {
        for i in 0..num_events {
            events_tx
                .send(HubEvent {
                    r#type: HubEventType::MergeMessage as i32,
                    id: i,
                    body: None,
                    block_number: 1,
                    shard_index: 1,
                    timestamp: 0,
                })
                .unwrap();
        }
    }

    async fn write_events_to_db(db: Arc<RocksDB>, num_events: u64) {
        let mut txn = RocksDbTransactionBatch::new();
        for i in 0..num_events {
            HubEvent::put_event_transaction(
                &mut txn,
                &HubEvent {
                    r#type: HubEventType::MergeMessage as i32,
                    id: i,
                    body: None,
                    block_number: 1,
                    shard_index: 1,
                    timestamp: 0,
                },
            )
            .unwrap();
        }
        db.commit(txn).unwrap();
    }

    fn add_auth_header<T>(request: &mut Request<T>, username: &str, password: &str) {
        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password))
        );
        request
            .metadata_mut()
            .insert("authorization", auth.parse().unwrap());
    }

    async fn submit_message(
        service: &MyHubService,
        message: proto::Message,
    ) -> Result<tonic::Response<proto::Message>, tonic::Status> {
        let mut request = Request::new(message);
        add_auth_header(&mut request, USER_NAME, PASSWORD);
        service.submit_message(request).await
    }

    async fn submit_bulk_messages(
        service: &MyHubService,
        messages: Vec<proto::Message>,
    ) -> Result<tonic::Response<SubmitBulkMessagesResponse>, tonic::Status> {
        let mut request = Request::new(SubmitBulkMessagesRequest { messages });
        add_auth_header(&mut request, USER_NAME, PASSWORD);
        service.submit_bulk_messages(request).await
    }

    async fn make_server(
        rpc_auth: Option<String>,
        admin_rpc_auth: Option<String>,
    ) -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        [ShardEngine; 2],
        BlockEngine,
        MyHubService,
        broadcast::Sender<ShardChunk>,
        broadcast::Sender<Block>,
    ) {
        make_server_with_slot_cap(rpc_auth, admin_rpc_auth, None).await
    }

    // Like `make_server`, but lets a test shrink the channel member/moderate slot caps so
    // slot-boundary coverage doesn't have to insert thousands of rows. `None` keeps the
    // production caps.
    async fn make_server_with_slot_cap(
        rpc_auth: Option<String>,
        admin_rpc_auth: Option<String>,
        channel_slot_cap_override: Option<u32>,
    ) -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        [ShardEngine; 2],
        BlockEngine,
        MyHubService,
        broadcast::Sender<ShardChunk>,
        broadcast::Sender<Block>,
    ) {
        let (msgs_request_tx, msgs_request_rx) = mpsc::channel(100);

        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let limits = test_helper::limits::test_store_limits();
        let (engine1, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            messages_request_tx: Some(msgs_request_tx.clone()),
            channel_slot_cap_override,
            ..Default::default()
        })
        .await;
        let (engine2, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            messages_request_tx: Some(msgs_request_tx.clone()),
            shard_id: 2,
            channel_slot_cap_override,
            ..Default::default()
        })
        .await;
        let db1 = engine1.db.clone();
        let db2 = engine2.db.clone();

        let shard1_stores = Stores::new(
            db1,
            1,
            merkle_trie::MerkleTrie::new().unwrap(),
            limits.clone(),
            proto::FarcasterNetwork::Devnet,
            test_helper::statsd_client(),
        );
        let shard1_senders = engine1.get_senders();

        let shard2_stores = Stores::new(
            db2,
            2,
            merkle_trie::MerkleTrie::new().unwrap(),
            limits.clone(),
            proto::FarcasterNetwork::Devnet,
            test_helper::statsd_client(),
        );
        let shard2_senders = engine2.get_senders();
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);
        let num_shards = senders.len() as u32;

        let auth = rpc_auth.unwrap_or_else(|| format!("{}:{}", USER_NAME, PASSWORD));

        let message_router = Box::new(routing::EvenOddRouterForTest {});
        assert_eq!(message_router.route_fid(SHARD1_FID, 2), 1);
        assert_eq!(message_router.route_fid(SHARD2_FID, 2), 2);

        let (mempool_tx, mempool_rx) = mpsc::channel(1000);
        let (gossip_tx, _gossip_rx) = mpsc::channel(1000);
        let (shard_decision_tx, shard_decision_rx) = broadcast::channel(1000);
        let (block_decision_tx, block_decision_rx) = broadcast::channel(1000);
        let (block_engine, _) = block_engine_test_helpers::setup_with_options(BlockEngineOptions {
            messages_request_tx: Some(msgs_request_tx),
            channel_slot_cap_override,
            ..BlockEngineOptions::default()
        });
        let block_stores = block_engine.stores();

        let mut mempool = Mempool::new(
            mempool::Config::default(),
            engine1.network,
            mempool_rx,
            msgs_request_rx,
            num_shards,
            stores.clone(),
            block_stores.clone(),
            gossip_tx.clone(),
            shard_decision_rx,
            block_decision_rx,
            statsd_client.clone(),
        );
        tokio::spawn(async move { mempool.run().await });

        let mut chain_clients = ChainClients {
            chain_api_map: HashMap::new(),
        };
        chain_clients.chain_api_map.insert(
            Chain::EthMainnet,
            Box::new(MockL1Client {}) as Box<dyn ChainAPI>,
        );
        chain_clients.chain_api_map.insert(
            Chain::BaseMainnet,
            Box::new(MockL1Client {}) as Box<dyn ChainAPI>,
        );
        (
            stores.clone(),
            senders.clone(),
            [engine1, engine2],
            block_engine,
            MyHubService::new(
                auth,
                admin_rpc_auth.unwrap_or_default(),
                vec![],
                block_stores,
                stores,
                senders,
                statsd_client,
                num_shards,
                proto::FarcasterNetwork::Devnet,
                message_router,
                mempool_tx.clone(),
                gossip_tx.clone(),
                chain_clients,
                "0.1.2".to_string(),
                "asddef".to_string(),
                None,
                Default::default(),
            ),
            shard_decision_tx,
            block_decision_tx,
        )
    }

    #[tokio::test]
    async fn test_get_channel_owner_unknown_channel_key_returns_not_found() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let err = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "missing".to_string(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "channel not registered");
    }

    // The registry emits no onchain event when a lapsed registration's grace
    // period ends, so the endpoint reports the last known registration as-is
    // (owner still resolvable, expiry visibly in the past) and callers
    // interpret expiry themselves.
    #[tokio::test]
    async fn test_get_channel_owner_lapsed_registration_returns_record() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(1);
        let expiry = now_unix_seconds() - 1;
        merge_channel_registration(&block_engine, "lapsed", address.clone(), expiry);
        let verification = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &verification);
        merge_shard_zero_verification(&block_engine, &verification);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lapsed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD1_FID);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_registered_without_verification_returns_parked() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(2);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "parked", address.clone(), expiry);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "parked".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_ignores_pre_v21_data_shard_verification() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(3);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "verified", address.clone(), expiry);
        let verification_add = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(1),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &verification_add);

        assert!(
            crate::storage::store::account::VerificationStore::get_verification_add(
                &stores.get(&1).unwrap().verification_store,
                SHARD1_FID,
                &address,
                None,
            )
            .unwrap()
            .is_some()
        );
        assert!(
            crate::storage::store::account::VerificationStore::get_verification_add(
                &block_engine.stores().verification_store,
                SHARD1_FID,
                &address,
                None,
            )
            .unwrap()
            .is_none()
        );

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "verified".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_lww_in_shard_zero_replica() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(4);
        merge_channel_registration(
            &block_engine,
            "lww",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        let later = messages_factory::verifications::create_verification_add(
            SHARD2_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(20),
            None,
        );
        let earlier = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &later);
        merge_shard_zero_verification(&block_engine, &earlier);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lww".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD2_FID);
    }

    // Mirror of the test above with the later verification on the OTHER shard.
    // `shard_stores` is a `HashMap` iterated in an unspecified order, so running
    // both directions pins "latest ts_hash wins" rather than an accidental
    // "last shard iterated wins" — one direction alone could pass by luck of the
    // hash order.
    #[tokio::test]
    async fn test_get_channel_owner_lww_in_shard_zero_replica_reversed() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(6);
        merge_channel_registration(
            &block_engine,
            "lww_reversed",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        let later = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(20),
            None,
        );
        let earlier = messages_factory::verifications::create_verification_add(
            SHARD2_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &later);
        merge_shard_zero_verification(&block_engine, &earlier);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "lww_reversed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, SHARD1_FID);
    }

    // A verification remove deletes both the primary add and its by-address index
    // entry, so a channel that resolved to an fid must fall back to parked once
    // the owner removes their verification. This exercises the real
    // add-then-remove path end to end, distinct from the artificial orphan seed.
    #[tokio::test]
    async fn test_get_channel_owner_reverts_to_parked_after_verification_remove() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(7);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "removed", address.clone(), expiry);
        let verification_add = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            0,
            address.clone(),
            vec![],
            vec![],
            Some(10),
            None,
        );
        merge_shard_zero_verification(&block_engine, &verification_add);

        // Sanity: resolves to the verifier before the remove.
        let resolved = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "removed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resolved.fid, SHARD1_FID);

        // Later remove wins the CRDT and clears both the add and the index entry.
        let verification_remove = messages_factory::verifications::create_verification_remove(
            SHARD1_FID,
            address.clone(),
            Some(20),
            None,
        );
        merge_shard_zero_verification(&block_engine, &verification_remove);

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "removed".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_get_channel_owner_drops_orphan_index_candidate() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(5);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "orphan", address.clone(), expiry);

        let mut txn = RocksDbTransactionBatch::new();
        let orphan_hash = vec![9; 20];
        let orphan_ts_hash = make_ts_hash(100, &orphan_hash).unwrap();
        txn.put(
            VerificationStoreDef::make_verification_by_address_key(&address, SHARD1_FID),
            orphan_ts_hash.to_vec(),
        );
        block_engine.stores().db.commit(txn).unwrap();

        let response = service
            .get_channel_owner(Request::new(ChannelOwnerRequest {
                channel_key: "orphan".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.fid, 0);
        assert_eq!(response.owner_address, address);
        assert_eq!(response.expiry, expiry);
    }

    #[tokio::test]
    async fn test_channel_reads_delayed_flip_from_parked_to_verified() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(8);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "delayed_flip", address.clone(), expiry);

        let parked = get_channel_owner_response(&service, "delayed_flip").await;
        assert_eq!(parked.fid, 0);
        assert_eq!(parked.owner_address, address);
        assert_eq!(
            get_channels_by_fid_keys(&service, SHARD1_FID).await,
            Vec::<String>::new()
        );
        let by_address_parked = service
            .get_channels_by_address(channels_by_address_request(address.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address_parked.channels.len(), 1);
        assert_eq!(by_address_parked.channels[0].fid, 0);
        assert_eq!(by_address_parked.channels[0].channel_key, "delayed_flip");

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        assert_channel_owner_fid_invariant(&service, "delayed_flip", SHARD1_FID, None).await;
        let by_address_resolved = service
            .get_channels_by_address(channels_by_address_request(address))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address_resolved.channels.len(), 1);
        assert_eq!(by_address_resolved.channels[0].fid, SHARD1_FID);
        assert_eq!(by_address_resolved.next_page_token, None);
    }

    // Lapsed registrations stay listed (with their past expiry) for the same
    // reason GetChannelOwner returns them: release state is not computable
    // from chain events, and a renewal prompt needs the owner to still see
    // the channel.
    #[tokio::test]
    async fn test_channel_lists_include_lapsed_registrations() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(9);
        let expiry = now_unix_seconds() - 1;
        merge_channel_registration(&block_engine, "lapsed_list", address.clone(), expiry);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let by_address = service
            .get_channels_by_address(channels_by_address_request(address.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_address.channels.len(), 1);
        assert_eq!(by_address.channels[0].channel_key, "lapsed_list");
        assert_eq!(by_address.channels[0].fid, SHARD1_FID);
        assert_eq!(by_address.channels[0].expiry, expiry);

        assert_eq!(
            get_channels_by_fid_keys(&service, SHARD1_FID).await,
            vec!["lapsed_list".to_string()]
        );
    }

    #[tokio::test]
    async fn test_channel_reads_last_verifier_wins_regardless_of_merge_order() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(9);
        merge_channel_registration(
            &block_engine,
            "last_verifier_wins",
            address.clone(),
            now_unix_seconds() + 3600,
        );

        let later = verification_add(SHARD2_FID, address.clone(), 20);
        let earlier = verification_add(SHARD1_FID, address, 10);
        merge_channel_owner_verification(stores.get(&2).unwrap(), &block_engine, &later);
        merge_channel_owner_verification(stores.get(&1).unwrap(), &block_engine, &earlier);

        assert_channel_owner_fid_invariant(
            &service,
            "last_verifier_wins",
            SHARD2_FID,
            Some(SHARD1_FID),
        )
        .await;
    }

    #[tokio::test]
    async fn test_channel_reads_remove_fallback_then_parked() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(10);
        merge_channel_registration(
            &block_engine,
            "remove_fallback",
            address.clone(),
            now_unix_seconds() + 3600,
        );
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );
        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_add(SHARD2_FID, address.clone(), 20),
        );
        assert_channel_owner_fid_invariant(
            &service,
            "remove_fallback",
            SHARD2_FID,
            Some(SHARD1_FID),
        )
        .await;

        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_remove(SHARD2_FID, address.clone(), 30),
        );
        assert_channel_owner_fid_invariant(
            &service,
            "remove_fallback",
            SHARD1_FID,
            Some(SHARD2_FID),
        )
        .await;

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_remove(SHARD1_FID, address, 40),
        );
        let parked = get_channel_owner_response(&service, "remove_fallback").await;
        assert_eq!(parked.fid, 0);
        assert!(!get_channels_by_fid_keys(&service, SHARD1_FID)
            .await
            .contains(&"remove_fallback".to_string()));
        assert!(!get_channels_by_fid_keys(&service, SHARD2_FID)
            .await
            .contains(&"remove_fallback".to_string()));
    }

    #[tokio::test]
    async fn test_channel_reads_cold_wallet_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address_a = owner_address(11);
        let address_b = owner_address(12);
        let expiry = now_unix_seconds() + 3600;
        merge_channel_registration(&block_engine, "cold_wallet", address_a.clone(), expiry);

        let parked = get_channel_owner_response(&service, "cold_wallet").await;
        assert_eq!(parked.fid, 0);
        assert_eq!(parked.owner_address, address_a);
        assert_eq!(parked.expiry, expiry);

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_a, 10),
        );
        assert_channel_owner_fid_invariant(&service, "cold_wallet", SHARD1_FID, None).await;

        merge_channel_transfer(
            &block_engine,
            "cold_wallet",
            address_b.clone(),
            expiry + 999,
        );
        let transferred = get_channel_owner_response(&service, "cold_wallet").await;
        assert_eq!(transferred.fid, 0);
        assert_eq!(transferred.owner_address, address_b);
        assert_eq!(transferred.expiry, expiry);

        merge_channel_owner_verification(
            stores.get(&2).unwrap(),
            &block_engine,
            &verification_add(SHARD2_FID, address_b, 20),
        );
        assert_channel_owner_fid_invariant(&service, "cold_wallet", SHARD2_FID, Some(SHARD1_FID))
            .await;
    }

    #[tokio::test]
    async fn test_channel_reads_verify_with_no_channels_noop() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(13);
        merge_verification(
            stores.get(&1).unwrap(),
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let by_fid = service
            .get_channels_by_fid(channels_by_fid_request(SHARD1_FID, None))
            .await
            .unwrap()
            .into_inner();
        assert!(by_fid.channels.is_empty());
        assert_eq!(by_fid.next_page_token, None);

        let by_address = service
            .get_channels_by_address(channels_by_address_request(address))
            .await
            .unwrap()
            .into_inner();
        assert!(by_address.channels.is_empty());
        assert_eq!(by_address.next_page_token, None);
    }

    #[tokio::test]
    async fn test_channel_message_read_surface_uses_shard_zero_folds_and_indexes() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let channel_key = "message_reads";
        let channel_id = channel_label(channel_key);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner_address(55),
            now_unix_seconds() + 3600,
        );

        let empty_member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: 101,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(empty_member.state, proto::ChannelMemberState::None as i32);
        assert_eq!(empty_member.last_action_ts, None);

        let update = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelUpdate,
            proto::message_data::Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                channel_id: channel_id.clone(),
                name: Some("Channel name".to_string()),
                image_url: Some("https://example.com/image.png".to_string()),
                ..Default::default()
            }),
            Some(9),
            None,
        );
        let moderator = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: 101,
                action: proto::ChannelMemberAction::AddModerator as i32,
            }),
            Some(10),
            None,
        );
        let banned = messages_factory::create_message_with_data(
            500,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: 102,
                action: proto::ChannelMemberAction::Ban as i32,
            }),
            Some(11),
            None,
        );
        let pin_hash = vec![0xAB; 20];
        let pin = messages_factory::create_message_with_data(
            501,
            proto::MessageType::ChannelPin,
            proto::message_data::Body::ChannelPinBody(proto::ChannelPinBody {
                channel_id: channel_id.clone(),
                cast_hash: pin_hash.clone(),
            }),
            Some(12),
            None,
        );
        let moderated_hash = vec![0xCD; 20];
        let moderation = messages_factory::create_message_with_data(
            502,
            proto::MessageType::ChannelModerate,
            proto::message_data::Body::ChannelModerateBody(proto::ChannelModerateBody {
                channel_id: channel_id.clone(),
                cast_hash: moderated_hash.clone(),
                action: proto::ChannelModerateAction::Hide as i32,
            }),
            Some(13),
            None,
        );
        for message in [&update, &moderator, &banned, &pin, &moderation] {
            merge_channel_message(&block_engine, message);
        }

        let member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: 101,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(member.state, proto::ChannelMemberState::Moderator as i32);
        assert_eq!(member.last_action_ts, Some(10));

        let members = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: None,
                page_size: Some(1),
                page_token: None,
                reverse: Some(false),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(members.members.len(), 1);
        assert!(members.next_page_token.is_some());

        let moderators = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: Some(proto::ChannelMemberState::Moderator as i32),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(moderators.members.len(), 1);
        assert_eq!(moderators.members[0].fid, 101);

        let memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: 101,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(memberships.memberships.len(), 1);
        assert_eq!(memberships.memberships[0].channel_id, channel_id);
        assert_eq!(
            memberships.memberships[0].state,
            proto::ChannelMemberState::Moderator as i32
        );

        let pin_response = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: channel_label(channel_key),
            }))
            .await
            .unwrap()
            .into_inner();
        let pinned = pin_response.pin.expect("channel has a pin");
        assert_eq!(pinned.cast_hash, pin_hash);
        assert_eq!(pinned.author_fid, 501);

        let moderations = service
            .get_channel_moderations(Request::new(ChannelModerationsRequest {
                channel_id: channel_label(channel_key),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(moderations.moderations.len(), 1);
        assert_eq!(moderations.moderations[0].cast_hash, moderated_hash);
        assert_eq!(moderations.moderations[0].author_fid, 502);

        let metadata = service
            .get_channel_metadata(Request::new(ChannelRequest {
                channel_id: channel_label(channel_key),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(metadata.name.as_deref(), Some("Channel name"));
        assert_eq!(
            metadata.casting_mode,
            proto::CastingMode::MembersOnly as i32
        );
        assert_eq!(
            metadata.membership_mode,
            proto::MembershipMode::Approval as i32
        );

        let malformed = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: vec![1; 31],
            }))
            .await
            .unwrap_err();
        assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
        let unregistered = service
            .get_channel_pin(Request::new(ChannelRequest {
                channel_id: vec![9; 32],
            }))
            .await
            .unwrap_err();
        assert_eq!(unregistered.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn channel_read_page_tokens_are_scoped_to_their_own_channel() {
        // `page_token` is `optional bytes` on the wire and reaches RocksDB as a raw
        // scan bound, so a caller controls where the scan STARTS. Two shapes have to
        // be handled at the RPC edge: an empty token (what generated clients send
        // when echoing an absent `next_page_token`) must mean "first page", and a
        // token minted for another channel must be refused as caller error rather
        // than returning that channel's members under the requested channel id.
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let mut labels = [
            channel_label("token-scope-a"),
            channel_label("token-scope-b"),
        ];
        labels.sort();
        let [lower_channel, higher_channel] = labels;
        // Registered directly rather than through `merge_channel_registration`, which
        // pins block/log index and so cannot register two channels in one test.
        for (log_index, (channel_key, address_byte)) in
            [("token-scope-a", 0x61), ("token-scope-b", 0x62)]
                .into_iter()
                .enumerate()
        {
            merge_channel_event(
                &block_engine,
                events_factory::create_channel_register_event(
                    channel_key,
                    channel_label(channel_key),
                    owner_address(address_byte),
                    now_unix_seconds() + 3600,
                    ChannelRegisterEventType::Register,
                    1,
                    log_index as u32 + 1,
                ),
            );
        }

        // Two members in the lower channel so its first page yields a live cursor,
        // and one in the higher channel so a leak is distinguishable from an error.
        let mut timestamp = 20;
        for (channel, fid) in [
            (&lower_channel, 601),
            (&lower_channel, 602),
            (&higher_channel, 603),
        ] {
            merge_channel_message(
                &block_engine,
                &messages_factory::create_message_with_data(
                    500,
                    proto::MessageType::ChannelMember,
                    proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                        channel_id: channel.clone(),
                        fid,
                        action: proto::ChannelMemberAction::AddMember as i32,
                    }),
                    Some(timestamp),
                    None,
                ),
            );
            timestamp += 1;
        }

        let lower_first = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: lower_channel.clone(),
                state_filter: None,
                page_size: Some(1),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lower_first.members.len(), 1);
        assert_eq!(lower_first.members[0].fid, 601);
        let foreign_token = lower_first.next_page_token.clone().unwrap();

        // Without prefix scoping this returns fid 602 — a member of the lower
        // channel — alongside the higher channel's own 603.
        let leaked = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: higher_channel.clone(),
                state_filter: None,
                page_size: Some(10),
                page_token: Some(foreign_token.clone()),
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(leaked.code(), tonic::Code::InvalidArgument);

        let leaked_memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: 603,
                page_size: Some(10),
                page_token: Some(foreign_token),
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(leaked_memberships.code(), tonic::Code::InvalidArgument);

        // An empty token is "first page", not an error and not an internal fault.
        let empty_token_members = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: lower_channel.clone(),
                state_filter: None,
                page_size: Some(10),
                page_token: Some(Vec::new()),
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            empty_token_members
                .members
                .iter()
                .map(|member| member.fid)
                .collect::<Vec<_>>(),
            vec![601, 602]
        );

        let empty_token_memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: 603,
                page_size: Some(10),
                page_token: Some(Vec::new()),
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            empty_token_memberships
                .memberships
                .iter()
                .map(|membership| membership.channel_id.clone())
                .collect::<Vec<_>>(),
            vec![higher_channel.clone()]
        );

        let empty_token_moderations = service
            .get_channel_moderations(Request::new(ChannelModerationsRequest {
                channel_id: higher_channel,
                page_size: Some(10),
                page_token: Some(Vec::new()),
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(empty_token_moderations.moderations.is_empty());

        // A token that does belong to the requested channel still pages normally.
        let lower_second = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: lower_channel,
                state_filter: None,
                page_size: Some(10),
                page_token: lower_first.next_page_token,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            lower_second
                .members
                .iter()
                .map(|member| member.fid)
                .collect::<Vec<_>>(),
            vec![602]
        );
    }

    #[test]
    fn channel_store_errors_are_classified_by_provenance() {
        use crate::core::error::HubError;
        use crate::network::server::channel_store_error_to_status;

        // Caller input remains under bad_request.*. Matching the namespace keeps a
        // newly introduced validation error from accidentally becoming a 500.
        for err in [
            HubError::invalid_parameter("page token does not belong to the requested index"),
            HubError::invalid_parameter("channel member fid exceeds u32"),
            HubError::validation_failure("invalid channel member state filter"),
        ] {
            assert_eq!(
                channel_store_error_to_status(err.clone()).code(),
                tonic::Code::InvalidArgument,
                "{} is caller input",
                err.code
            );
        }

        // Stored-state parse failures are tagged before they leave the store and
        // therefore remain server faults without a per-code allowlist here.
        for err in [
            HubError::invalid_internal_state("invalid channel moderate action"),
            HubError::invalid_internal_state("invalid channel member action"),
            HubError::invalid_internal_state("invalid ChannelMember body"),
            HubError::invalid_internal_state("channel slot points to a missing message"),
            HubError::invalid_internal_state("channel counter has invalid length"),
            HubError::internal_db_error("rocksdb exploded"),
        ] {
            assert_eq!(
                channel_store_error_to_status(err.clone()).code(),
                tonic::Code::Internal,
                "{} is a server fault, not caller input",
                err.code
            );
        }
    }

    #[tokio::test]
    async fn channel_read_rpcs_reject_malformed_requests_before_reading() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let channel_key = "read-validation";
        let channel_id = channel_label(channel_key);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner_address(0x51),
            now_unix_seconds() + 3600,
        );

        // A fid outside a non-zero u32 cannot key a member slot. Without the explicit
        // guard, `make_member_by_fid_key`'s own `try_from` failure would surface as
        // `internal` — a server fault for what is caller input.
        for fid in [0u64, u32::MAX as u64 + 1, u64::MAX] {
            assert_eq!(
                service
                    .get_channel_member(Request::new(ChannelMemberRequest {
                        channel_id: channel_id.clone(),
                        fid,
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument,
                "get_channel_member must reject fid {fid}"
            );
            assert_eq!(
                service
                    .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                        fid,
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument,
                "get_channel_memberships_by_fid must reject fid {fid}"
            );
        }

        // An i32 outside ChannelMemberState is caller input, not a store fault.
        assert_eq!(
            service
                .get_channel_members(Request::new(ChannelMembersRequest {
                    channel_id: channel_id.clone(),
                    state_filter: Some(99),
                    page_size: None,
                    page_token: None,
                    reverse: None,
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        // `require_registered_channel` is shared by five handlers but was only ever
        // reached through get_channel_pin. Both of its branches, on all five.
        let malformed = vec![0x11; CHANNEL_ID_LENGTH - 1];
        let unregistered = vec![0x99; CHANNEL_ID_LENGTH];
        for (bad_channel_id, expected) in [
            (malformed, tonic::Code::InvalidArgument),
            (unregistered, tonic::Code::NotFound),
        ] {
            assert_eq!(
                service
                    .get_channel_member(Request::new(ChannelMemberRequest {
                        channel_id: bad_channel_id.clone(),
                        fid: 101,
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(
                service
                    .get_channel_members(Request::new(ChannelMembersRequest {
                        channel_id: bad_channel_id.clone(),
                        state_filter: None,
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(
                service
                    .get_channel_pin(Request::new(ChannelRequest {
                        channel_id: bad_channel_id.clone(),
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(
                service
                    .get_channel_moderations(Request::new(ChannelModerationsRequest {
                        channel_id: bad_channel_id.clone(),
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(
                service
                    .get_channel_metadata(Request::new(ChannelRequest {
                        channel_id: bad_channel_id.clone(),
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
        }

        // GetChannelMembershipsByFid is fid-keyed and takes no channel_id, so it has
        // no registration check at all and must NOT report NOT_FOUND for a fid with
        // no memberships — the rpc.proto contract calls this out explicitly.
        let empty = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: 424_242,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(empty.memberships.is_empty());
    }

    #[tokio::test]
    async fn unconfigured_channel_metadata_reports_the_restrictive_fold_defaults() {
        // A registered channel with no ChannelUpdate must report the SAME effective
        // policy that admission would apply to it. The fold side of that pairing was
        // already pinned in channel_store_tests; this is the server side, which used
        // to restate MembersOnly/Approval as literals. If the two ever disagreed, a
        // channel would report different permissions before and after a cosmetic-only
        // update that touched neither mode.
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let channel_key = "unconfigured";
        let channel_id = channel_label(channel_key);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner_address(0x53),
            now_unix_seconds() + 3600,
        );

        let (expected_casting, expected_membership) = ChannelUpdateStore::default_channel_modes();
        let unconfigured = service
            .get_channel_metadata(Request::new(ChannelRequest {
                channel_id: channel_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(unconfigured.name, None);
        assert_eq!(unconfigured.description, None);
        assert_eq!(unconfigured.image_url, None);
        assert_eq!(unconfigured.header, None);
        assert_eq!(unconfigured.rules, None);
        assert_eq!(unconfigured.casting_mode, expected_casting as i32);
        assert_eq!(unconfigured.membership_mode, expected_membership as i32);
        assert_eq!(
            unconfigured.casting_mode,
            proto::CastingMode::MembersOnly as i32
        );
        assert_eq!(
            unconfigured.membership_mode,
            proto::MembershipMode::Approval as i32
        );

        // A cosmetic-only update that sets neither mode must land on exactly the same
        // policy — this is the branch the "no update" defaults have to agree with.
        merge_channel_message(
            &block_engine,
            &messages_factory::create_message_with_data(
                500,
                proto::MessageType::ChannelUpdate,
                proto::message_data::Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                    channel_id: channel_id.clone(),
                    name: Some("Name only".to_string()),
                    ..Default::default()
                }),
                Some(40),
                None,
            ),
        );
        let cosmetic = service
            .get_channel_metadata(Request::new(ChannelRequest { channel_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cosmetic.name.as_deref(), Some("Name only"));
        assert_eq!(cosmetic.casting_mode, unconfigured.casting_mode);
        assert_eq!(cosmetic.membership_mode, unconfigured.membership_mode);
    }

    #[tokio::test]
    async fn channel_member_state_none_filter_returns_every_state() {
        // CHANNEL_MEMBER_STATE_NONE is the proto3 zero value, so it is exactly what a
        // client sends by leaving `state_filter` set to its default. It must mean "no
        // filter", identical to omitting the field — `channel_member_state_from_proto`
        // maps it to None and `get_channel_members` then `and_then`s it away. Making
        // that mapping total would silently give every such client empty pages.
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let channel_key = "none-filter";
        let channel_id = channel_label(channel_key);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner_address(0x52),
            now_unix_seconds() + 3600,
        );

        for (index, (fid, action)) in [
            (201u64, proto::ChannelMemberAction::AddMember),
            (202, proto::ChannelMemberAction::AddModerator),
            (203, proto::ChannelMemberAction::Ban),
        ]
        .into_iter()
        .enumerate()
        {
            merge_channel_message(
                &block_engine,
                &messages_factory::create_message_with_data(
                    500,
                    proto::MessageType::ChannelMember,
                    proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                        channel_id: channel_id.clone(),
                        fid,
                        action: action as i32,
                    }),
                    Some(index as u32 + 30),
                    None,
                ),
            );
        }

        let fids_for = |filter: Option<i32>| {
            let service = &service;
            let channel_id = channel_id.clone();
            async move {
                let mut fids = service
                    .get_channel_members(Request::new(ChannelMembersRequest {
                        channel_id,
                        state_filter: filter,
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap()
                    .into_inner()
                    .members
                    .into_iter()
                    .map(|member| member.fid)
                    .collect::<Vec<_>>();
                fids.sort();
                fids
            }
        };

        let unfiltered = fids_for(None).await;
        assert_eq!(unfiltered, vec![201, 202, 203]);
        assert_eq!(
            fids_for(Some(proto::ChannelMemberState::None as i32)).await,
            unfiltered,
            "an explicit NONE filter must behave exactly like an absent one"
        );
        // A real filter still narrows, so "NONE means everything" is not just the
        // filter being ignored altogether.
        assert_eq!(
            fids_for(Some(proto::ChannelMemberState::Banned as i32)).await,
            vec![203]
        );
    }

    #[tokio::test]
    async fn channel_fanout_end_to_end_keeps_shard_zero_replicas_and_reads_in_sync() {
        let (
            _stores,
            _senders,
            mut engines,
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        const OWNER_FID: u64 = 7101;
        const MODERATOR_FID: u64 = 7102;
        const TARGET_FID: u64 = 7103;

        for fid in [OWNER_FID, MODERATOR_FID] {
            block_engine_test_helpers::register_user(
                fid,
                block_engine_test_helpers::default_signer(),
                block_engine_test_helpers::default_custody_address(),
                1,
                &mut block_engine,
            );
        }
        let channel_key = "fanout-e2e";
        let channel_id = channel_label(channel_key);
        let owner = owner_address(0x71);
        merge_channel_registration(
            &block_engine,
            channel_key,
            owner.clone(),
            now_unix_seconds() + 3600,
        );
        merge_shard_zero_verification(
            &block_engine,
            &verification_add(OWNER_FID, owner, messages_factory::farcaster_time()),
        );

        let timestamp = messages_factory::farcaster_time() + 10;
        let update = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelUpdate,
            proto::message_data::Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                channel_id: channel_id.clone(),
                name: Some("fanout".to_string()),
                ..Default::default()
            }),
            Some(timestamp),
            None,
        );
        let add_moderator = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: MODERATOR_FID,
                action: proto::ChannelMemberAction::AddModerator as i32,
            }),
            Some(timestamp + 1),
            None,
        );
        let add_target = messages_factory::create_message_with_data(
            OWNER_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
                action: proto::ChannelMemberAction::AddMember as i32,
            }),
            Some(timestamp + 2),
            None,
        );
        let pin_hash = vec![0x72; 20];
        let pin = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelPin,
            proto::message_data::Body::ChannelPinBody(proto::ChannelPinBody {
                channel_id: channel_id.clone(),
                cast_hash: pin_hash.clone(),
            }),
            Some(timestamp + 3),
            None,
        );
        let moderated_hash = vec![0x73; 20];
        let moderation = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelModerate,
            proto::message_data::Body::ChannelModerateBody(proto::ChannelModerateBody {
                channel_id: channel_id.clone(),
                cast_hash: moderated_hash.clone(),
                action: proto::ChannelModerateAction::Hide as i32,
            }),
            Some(timestamp + 4),
            None,
        );
        // Cross-author supersede: the moderator's later consensus action replaces the owner's
        // incumbent target slot even though its embedded timestamp is deliberately older.
        let ban_target = messages_factory::create_message_with_data(
            MODERATOR_FID,
            proto::MessageType::ChannelMember,
            proto::message_data::Body::ChannelMemberBody(proto::ChannelMemberBody {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
                action: proto::ChannelMemberAction::Ban as i32,
            }),
            Some(timestamp - 1),
            None,
        );
        let workload = vec![
            update.clone(),
            add_moderator.clone(),
            add_target.clone(),
            pin.clone(),
            moderation.clone(),
            ban_target.clone(),
        ];

        let [replica_a, replica_b] = &mut engines;
        // Bring both consumers to the exact BlockEvent cursor produced while registering the
        // test authors. Those earlier blocks contribute heartbeat events; replaying them is what
        // makes the subsequent channel event's strict seqnum provenance real rather than
        // manufacturing a fresh sequence for the test.
        let initial_block_events = block_engine.stores().block_event_store;
        for seqnum in 1..=initial_block_events.max_seqnum().unwrap() {
            let event = initial_block_events
                .get_block_event_by_seqnum(seqnum)
                .unwrap()
                .unwrap();
            let replay_a = replica_a.propose_state_change(
                replica_a.shard_id(),
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                        for_shard: replica_a.shard_id(),
                        message: event.clone(),
                    },
                ],
                None,
            );
            let replay_b = replica_b.propose_state_change(
                replica_b.shard_id(),
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                        for_shard: replica_b.shard_id(),
                        message: event,
                    },
                ],
                None,
            );
            test_helper::validate_and_commit_state_change(replica_a, &replay_a).await;
            test_helper::validate_and_commit_state_change(replica_b, &replay_b).await;
        }
        for message in &workload {
            let height = block_engine.get_confirmed_height().increment();
            let state_change = block_engine.propose_state_change(
                vec![
                    crate::storage::store::mempool_poller::MempoolMessage::UserMessage(
                        message.clone(),
                    ),
                ],
                height,
                Some(crate::core::util::FarcasterTime::new(
                    message.data.as_ref().unwrap().timestamp as u64,
                )),
            );
            let block = block_engine_test_helpers::validate_and_commit_state_change(
                &mut block_engine,
                &state_change,
            );
            assert_eq!(
                block
                    .events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event.data.as_ref().and_then(|data| data.body.as_ref()),
                            Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                                if body.message.as_ref() == Some(message)
                        )
                    })
                    .count(),
                1,
                "one channel fan-out event per admitted merge"
            );

            let mut fanout_events = block.events.clone();
            fanout_events.sort_by_key(|event| event.seqnum());
            for event in fanout_events {
                let replay_a = replica_a.propose_state_change(
                    replica_a.shard_id(),
                    vec![
                        crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                            for_shard: replica_a.shard_id(),
                            message: event.clone(),
                        },
                    ],
                    None,
                );
                let replay_b = replica_b.propose_state_change(
                    replica_b.shard_id(),
                    vec![
                        crate::storage::store::mempool_poller::MempoolMessage::BlockEvent {
                            for_shard: replica_b.shard_id(),
                            message: event,
                        },
                    ],
                    None,
                );
                test_helper::validate_and_commit_state_change(replica_a, &replay_a).await;
                test_helper::validate_and_commit_state_change(replica_b, &replay_b).await;
            }
        }

        assert_eq!(replica_a.trie_root_hash(), replica_b.trie_root_hash());
        let block_stores = block_engine.stores();
        let replica_a_stores = replica_a.get_stores();
        let replica_b_stores = replica_b.get_stores();

        let shard_zero_channel_rows = channel_rows(&block_stores.db);
        assert_eq!(channel_rows(&replica_a.db), shard_zero_channel_rows);
        assert_eq!(channel_rows(&replica_b.db), shard_zero_channel_rows);

        for message in [&update, &add_moderator, &pin, &moderation, &ban_target] {
            let postfix = match message.msg_type() {
                proto::MessageType::ChannelUpdate => block_stores.channel_update_store.postfix(),
                proto::MessageType::ChannelMember => block_stores.channel_member_store.postfix(),
                proto::MessageType::ChannelPin => block_stores.channel_pin_store.postfix(),
                proto::MessageType::ChannelModerate => {
                    block_stores.channel_moderate_store.postfix()
                }
                other => panic!("unexpected type {other:?}"),
            };
            let data = message.data.as_ref().unwrap();
            let ts_hash = make_ts_hash(data.timestamp, &message.hash).unwrap();
            let key = make_message_primary_key(message.fid(), postfix, Some(&ts_hash));
            let expected = block_stores.db.get(&key).unwrap();
            assert!(expected.is_some());
            assert_eq!(replica_a.db.get(&key).unwrap(), expected);
            assert_eq!(replica_b.db.get(&key).unwrap(), expected);
            assert!(test_helper::message_exists_in_trie(replica_a, message));
            assert!(test_helper::message_exists_in_trie(replica_b, message));
            assert!(
                crate::storage::trie::merkle_trie::TrieKey::for_message(message)
                    .iter()
                    .all(|key| block_engine.trie_key_exists(test_helper::trie_ctx(), key))
            );
        }
        assert!(!test_helper::message_exists_in_trie(replica_a, &add_target));
        assert!(!test_helper::message_exists_in_trie(replica_b, &add_target));

        let merge_bodies = |engine: &ShardEngine| {
            HubEvent::get_events(engine.db.clone(), 0, None, None)
                .unwrap()
                .events
                .into_iter()
                .filter_map(|event| match event.body {
                    Some(proto::hub_event::Body::MergeMessageBody(body)) => Some(body),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let replica_a_bodies = merge_bodies(replica_a);
        let replica_b_bodies = merge_bodies(replica_b);
        assert_eq!(replica_a_bodies, replica_b_bodies);
        let ban_event = replica_a_bodies
            .iter()
            .find(|body| body.message.as_ref() == Some(&ban_target))
            .unwrap();
        assert_eq!(ban_event.deleted_messages, vec![add_target]);

        assert_eq!(
            ChannelUpdateStore::get_channel_update(
                &replica_a_stores.channel_update_store,
                &channel_id,
                None,
            )
            .unwrap(),
            ChannelUpdateStore::get_channel_update(
                &replica_b_stores.channel_update_store,
                &channel_id,
                None,
            )
            .unwrap()
        );
        assert_eq!(
            ChannelMemberStore::member_state(
                &replica_a_stores.channel_member_store,
                &channel_id,
                TARGET_FID,
                None,
            )
            .unwrap(),
            Some(crate::storage::store::account::ChannelMemberState::Banned)
        );

        let member = service
            .get_channel_member(Request::new(ChannelMemberRequest {
                channel_id: channel_id.clone(),
                fid: TARGET_FID,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(member.state, proto::ChannelMemberState::Banned as i32);
        let metadata = service
            .get_channel_metadata(Request::new(ChannelRequest {
                channel_id: channel_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(metadata.name.as_deref(), Some("fanout"));
        let memberships = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: TARGET_FID,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(memberships.memberships.len(), 1);
        assert_eq!(memberships.memberships[0].channel_id, channel_id);

        // `page_size: 0` is caller error, not an empty enumeration. Serving it as an
        // empty page with no token would assert "this channel has no members, paging
        // complete" to a client that only meant to leave the field unset.
        let members_zero = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: None,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(members_zero.code(), tonic::Code::InvalidArgument);

        let moderations_zero = service
            .get_channel_moderations(Request::new(ChannelModerationsRequest {
                channel_id: channel_id.clone(),
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(moderations_zero.code(), tonic::Code::InvalidArgument);

        let memberships_zero = service
            .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                fid: TARGET_FID,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(memberships_zero.code(), tonic::Code::InvalidArgument);

        // Omitting page_size still enumerates normally — the distinction the
        // rejection above exists to preserve.
        let members_default = service
            .get_channel_members(Request::new(ChannelMembersRequest {
                channel_id: channel_id.clone(),
                state_filter: None,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!members_default.members.is_empty());
    }

    mod channel_scenario_tests {
        use super::*;
        use crate::core::util::FarcasterTime;
        use crate::proto::{
            hub_event, message_data::Body, ChannelMemberAction, ChannelModerateAction,
            MembershipMode, MessageType,
        };
        use crate::storage::store::mempool_poller::MempoolMessage;
        use crate::storage::trie::merkle_trie::TrieKey;
        use alloy_signer_local::PrivateKeySigner;

        struct ReadSnapshot {
            member: proto::ChannelMemberResponse,
            members: proto::ChannelMembersResponse,
            pin: proto::ChannelPinResponse,
            moderations: proto::ChannelModerationsResponse,
            metadata: proto::ChannelMetadataResponse,
            memberships: proto::ChannelMembershipsResponse,
        }

        struct ScenarioDriver {
            replicas: [ShardEngine; 2],
            block_engine: BlockEngine,
            service: MyHubService,
            replayed_seqnum: u64,
        }

        #[derive(Debug, PartialEq, Eq)]
        struct ScenarioFingerprint {
            shard_zero_root: Vec<u8>,
            replica_roots: [Vec<u8>; 2],
            hub_event_cursors: [(usize, Option<u64>); 3],
            channel_rows: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
            block_event_seqnums: [u64; 3],
        }

        #[derive(Debug, PartialEq, Eq)]
        struct ScenarioEventDelta {
            hub_event_types_by_source_shard: Vec<(usize, i32)>,
            block_event_types_by_source_shard: Vec<(usize, i32)>,
        }

        impl ScenarioDriver {
            async fn new() -> Self {
                Self::new_with_slot_cap(None).await
            }

            // Builds a driver whose block engine and both replicas share a shrunken channel
            // member/moderate slot cap, so slot-boundary tests exercise the real cap-rejection
            // path without inserting the full production 8k/16k rows. `None` uses the production
            // caps.
            async fn new_with_slot_cap(channel_slot_cap_override: Option<u32>) -> Self {
                let (
                    _stores,
                    _senders,
                    replicas,
                    block_engine,
                    service,
                    _shard_decision_tx,
                    _block_decision_tx,
                ) = make_server_with_slot_cap(None, None, channel_slot_cap_override).await;
                Self {
                    replicas,
                    block_engine,
                    service,
                    replayed_seqnum: 0,
                }
            }

            fn register_user(&mut self, fid: u64, custody: Vec<u8>, storage_units: u32) {
                block_engine_test_helpers::register_user(
                    fid,
                    block_engine_test_helpers::default_signer(),
                    custody,
                    storage_units,
                    &mut self.block_engine,
                );
            }

            fn unreplayed_block_events(&self) -> (u64, Vec<proto::BlockEvent>) {
                let event_store = self.block_engine.stores().block_event_store;
                let max_seqnum = event_store.max_seqnum().unwrap();
                let events = ((self.replayed_seqnum + 1)..=max_seqnum)
                    .map(|seqnum| {
                        event_store
                            .get_block_event_by_seqnum(seqnum)
                            .unwrap()
                            .unwrap()
                    })
                    .collect();
                (max_seqnum, events)
            }

            async fn sync_new_block_events(&mut self) {
                let (max_seqnum, events) = self.unreplayed_block_events();
                if max_seqnum == self.replayed_seqnum {
                    return;
                }

                for event in events {
                    for replica in &mut self.replicas {
                        let shard_id = replica.shard_id();
                        let state_change = replica.propose_state_change(
                            shard_id,
                            vec![MempoolMessage::BlockEvent {
                                for_shard: shard_id,
                                message: event.clone(),
                            }],
                            None,
                        );
                        let proposed_root = state_change.new_state_root.clone();
                        test_helper::validate_and_commit_state_change(replica, &state_change).await;
                        assert_eq!(replica.trie_root_hash(), proposed_root);
                    }
                    assert_eq!(
                        self.replicas[0].trie_root_hash(),
                        self.replicas[1].trie_root_hash(),
                        "replicas must converge after every replayed shard-0 BlockEvent"
                    );
                }
                self.replayed_seqnum = max_seqnum;
            }

            async fn sync_new_block_events_batched(&mut self) {
                let (max_seqnum, events) = self.unreplayed_block_events();
                if max_seqnum == self.replayed_seqnum {
                    return;
                }

                // S6 fills the channel slot caps in bulk. Replay the same contiguous, mixed-fid
                // BlockEvent stream in batched proposals so cap coverage does not manufacture one
                // block per seeded row. This direct-engine harness sits below BlockReceiver, so it
                // must emulate BlockReceiver's durable confirmation and bounded tail re-drive
                // guarantee when HashMap transaction grouping transiently reorders different fids
                // and strict seqnum replay skips the unconfirmed tail.
                for replica in &mut self.replicas {
                    let shard_id = replica.shard_id();
                    let mut submissions = 0;
                    loop {
                        let confirmed =
                            replica.get_stores().block_event_store.max_seqnum().unwrap();
                        if confirmed >= max_seqnum {
                            break;
                        }
                        assert!(
                            submissions <= 3,
                            "replica shard {shard_id} did not confirm BlockEvent {max_seqnum} after the initial submission and three tail re-drives; stopped at {confirmed}"
                        );
                        let pending = events
                            .iter()
                            .filter(|event| event.seqnum() > confirmed)
                            .cloned()
                            .map(|message| MempoolMessage::BlockEvent {
                                for_shard: shard_id,
                                message,
                            })
                            .collect();
                        let state_change = replica.propose_state_change(shard_id, pending, None);
                        let proposed_root = state_change.new_state_root.clone();
                        test_helper::validate_and_commit_state_change(replica, &state_change).await;
                        assert_eq!(replica.trie_root_hash(), proposed_root);
                        submissions += 1;
                    }
                }
                assert_eq!(
                    self.replicas[0].trie_root_hash(),
                    self.replicas[1].trie_root_hash(),
                    "replicas must converge after batched shard-0 replay"
                );
                self.replayed_seqnum = max_seqnum;
            }

            fn commit_without_replay(
                &mut self,
                inputs: Vec<MempoolMessage>,
                timestamp: u32,
            ) -> Block {
                let height = self.block_engine.get_confirmed_height().increment();
                let state_change = self.block_engine.propose_state_change(
                    inputs,
                    height,
                    Some(FarcasterTime::new(timestamp as u64)),
                );
                let proposed_root = state_change.new_state_root.clone();
                let block = block_engine_test_helpers::validate_and_commit_state_change(
                    &mut self.block_engine,
                    &state_change,
                );
                assert_eq!(self.block_engine.trie_root_hash(), proposed_root);
                block
            }

            async fn commit(&mut self, inputs: Vec<MempoolMessage>, timestamp: u32) -> Block {
                let block = self.commit_without_replay(inputs, timestamp);
                self.sync_new_block_events().await;
                block
            }

            async fn commit_with_transaction_order(
                &mut self,
                inputs: Vec<MempoolMessage>,
                ordered_fids: &[u64],
                timestamp: u32,
            ) -> Block {
                let height = self.block_engine.get_confirmed_height().increment();
                let state_change = self
                    .block_engine
                    .propose_state_change_with_transaction_order_for_test(
                        inputs,
                        ordered_fids,
                        height,
                        FarcasterTime::new(timestamp as u64),
                    );
                assert_eq!(
                    state_change
                        .transactions
                        .iter()
                        .map(|transaction| transaction.fid)
                        .collect::<Vec<_>>(),
                    ordered_fids
                );
                let proposed_root = state_change.new_state_root.clone();
                let block = block_engine_test_helpers::validate_and_commit_state_change(
                    &mut self.block_engine,
                    &state_change,
                );
                assert_eq!(self.block_engine.trie_root_hash(), proposed_root);
                self.sync_new_block_events().await;
                block
            }

            async fn commit_messages(&mut self, messages: Vec<proto::Message>) -> Block {
                let timestamp = messages
                    .iter()
                    .filter_map(|message| message.data.as_ref().map(|data| data.timestamp))
                    .max()
                    .unwrap_or_else(messages_factory::farcaster_time);
                self.commit(
                    messages
                        .into_iter()
                        .map(MempoolMessage::UserMessage)
                        .collect(),
                    timestamp,
                )
                .await
            }

            async fn commit_messages_batched_replay(
                &mut self,
                messages: Vec<proto::Message>,
            ) -> Block {
                let timestamp = messages
                    .iter()
                    .filter_map(|message| message.data.as_ref().map(|data| data.timestamp))
                    .max()
                    .unwrap_or_else(messages_factory::farcaster_time);
                let block = self.commit_without_replay(
                    messages
                        .into_iter()
                        .map(MempoolMessage::UserMessage)
                        .collect(),
                    timestamp,
                );
                self.sync_new_block_events_batched().await;
                block
            }

            fn replicated_event_bodies(db: Arc<RocksDB>) -> Vec<hub_event::Body> {
                all_hub_events(db)
                    .into_iter()
                    .filter_map(|event| {
                        let body = event.body?;
                        let replicated = match &body {
                            hub_event::Body::MergeMessageBody(merge) => {
                                merge.message.as_ref().is_some_and(|message| {
                                    matches!(
                                        message.msg_type(),
                                        MessageType::LendStorage
                                            | MessageType::KeyAdd
                                            | MessageType::KeyRemove
                                            | MessageType::VerificationAddEthAddress
                                            | MessageType::VerificationRemove
                                            | MessageType::ChannelUpdate
                                            | MessageType::ChannelMember
                                            | MessageType::ChannelPin
                                            | MessageType::ChannelModerate
                                    )
                                })
                            }
                            _ => false,
                        };
                        replicated.then_some(body)
                    })
                    .collect()
            }

            fn event_bodies(db: Arc<RocksDB>) -> Vec<hub_event::Body> {
                all_hub_events(db)
                    .into_iter()
                    .filter_map(|event| match event.body {
                        Some(hub_event::Body::BlockConfirmedBody(_)) | None => None,
                        body => body,
                    })
                    .collect()
            }

            fn assert_converged(&mut self) {
                let shard_zero_stores = self.block_engine.stores();
                let expected_rows = channel_rows(&shard_zero_stores.db);
                let expected_events = Self::replicated_event_bodies(shard_zero_stores.db.clone());
                for replica in &self.replicas {
                    assert_eq!(channel_rows(&replica.db), expected_rows);
                    assert_eq!(
                        Self::replicated_event_bodies(replica.db.clone()),
                        expected_events,
                        "shard 0 and replicas must expose the same consensus merge bodies"
                    );
                }
                assert_eq!(
                    Self::event_bodies(self.replicas[0].db.clone()),
                    Self::event_bodies(self.replicas[1].db.clone()),
                    "replica HubEvent body streams must be byte-identical"
                );
                assert_eq!(
                    self.replicas[0].trie_root_hash(),
                    self.replicas[1].trie_root_hash()
                );
            }

            async fn reads(&self, channel_id: &[u8], fid: u64) -> ReadSnapshot {
                let member = self
                    .service
                    .get_channel_member(Request::new(ChannelMemberRequest {
                        channel_id: channel_id.to_vec(),
                        fid,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let members = self
                    .service
                    .get_channel_members(Request::new(ChannelMembersRequest {
                        channel_id: channel_id.to_vec(),
                        state_filter: None,
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let pin = self
                    .service
                    .get_channel_pin(Request::new(ChannelRequest {
                        channel_id: channel_id.to_vec(),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let moderations = self
                    .service
                    .get_channel_moderations(Request::new(ChannelModerationsRequest {
                        channel_id: channel_id.to_vec(),
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let metadata = self
                    .service
                    .get_channel_metadata(Request::new(ChannelRequest {
                        channel_id: channel_id.to_vec(),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let memberships = self
                    .service
                    .get_channel_memberships_by_fid(Request::new(ChannelMembershipsByFidRequest {
                        fid,
                        page_size: None,
                        page_token: None,
                        reverse: None,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                ReadSnapshot {
                    member,
                    members,
                    pin,
                    moderations,
                    metadata,
                    memberships,
                }
            }

            fn state_fingerprint(&mut self) -> ScenarioFingerprint {
                let shard_zero = self.block_engine.stores();
                let replica_a = self.replicas[0].get_stores();
                let replica_b = self.replicas[1].get_stores();
                ScenarioFingerprint {
                    shard_zero_root: self.block_engine.trie_root_hash(),
                    replica_roots: [
                        self.replicas[0].trie_root_hash(),
                        self.replicas[1].trie_root_hash(),
                    ],
                    hub_event_cursors: [
                        hub_event_cursor(&shard_zero.db),
                        hub_event_cursor(&replica_a.db),
                        hub_event_cursor(&replica_b.db),
                    ],
                    channel_rows: [
                        channel_rows(&shard_zero.db),
                        channel_rows(&replica_a.db),
                        channel_rows(&replica_b.db),
                    ],
                    block_event_seqnums: [
                        shard_zero.block_event_store.max_seqnum().unwrap(),
                        replica_a.block_event_store.max_seqnum().unwrap(),
                        replica_b.block_event_store.max_seqnum().unwrap(),
                    ],
                }
            }

            fn event_delta(
                &self,
                before: &ScenarioFingerprint,
                after: &ScenarioFingerprint,
            ) -> ScenarioEventDelta {
                let shard_zero = self.block_engine.stores();
                let replica_a = self.replicas[0].get_stores();
                let replica_b = self.replicas[1].get_stores();
                let dbs = [
                    shard_zero.db.clone(),
                    replica_a.db.clone(),
                    replica_b.db.clone(),
                ];
                let block_event_stores = [
                    shard_zero.block_event_store,
                    replica_a.block_event_store,
                    replica_b.block_event_store,
                ];

                let mut hub_event_types_by_source_shard = Vec::new();
                for (source_shard, db) in dbs.iter().enumerate() {
                    let expected_count = after.hub_event_cursors[source_shard]
                        .0
                        .checked_sub(before.hub_event_cursors[source_shard].0)
                        .unwrap();
                    let start_id = before.hub_event_cursors[source_shard]
                        .1
                        .map(|event_id| event_id + 1)
                        .unwrap_or(0);
                    let appended = HubEvent::get_events(
                        db.clone(),
                        start_id,
                        None,
                        Some(PageOptions {
                            page_size: Some(expected_count.max(1)),
                            ..Default::default()
                        }),
                    )
                    .unwrap()
                    .events;
                    assert_eq!(appended.len(), expected_count);
                    for event in appended {
                        hub_event_types_by_source_shard.push((source_shard, event.r#type));
                    }
                }

                let mut block_event_types_by_source_shard = Vec::new();
                for (source_shard, store) in block_event_stores.iter().enumerate() {
                    for seqnum in (before.block_event_seqnums[source_shard] + 1)
                        ..=after.block_event_seqnums[source_shard]
                    {
                        let event = store.get_block_event_by_seqnum(seqnum).unwrap().unwrap();
                        block_event_types_by_source_shard
                            .push((source_shard, event.data.as_ref().unwrap().r#type));
                    }
                }

                ScenarioEventDelta {
                    hub_event_types_by_source_shard,
                    block_event_types_by_source_shard,
                }
            }
        }

        fn hub_event_cursor(db: &RocksDB) -> (usize, Option<u64>) {
            let prefix = vec![RootPrefix::HubEvents as u8];
            let mut count = 0;
            let mut last_id = None;
            db.for_each_iterator_by_prefix(
                Some(prefix.clone()),
                Some(increment_vec_u8(&prefix)),
                &PageOptions::default(),
                |key, _| {
                    count += 1;
                    last_id = Some(u64::from_be_bytes(key[1..9].try_into().unwrap()));
                    Ok(false)
                },
            )
            .unwrap();
            (count, last_id)
        }

        fn all_hub_events(db: Arc<RocksDB>) -> Vec<HubEvent> {
            let mut page_token = None;
            let mut events = Vec::new();
            loop {
                let page = HubEvent::get_events(
                    db.clone(),
                    0,
                    None,
                    Some(PageOptions {
                        page_size: Some(1_000),
                        page_token,
                        ..Default::default()
                    }),
                )
                .unwrap();
                events.extend(page.events);
                let Some(next) = page.next_page_token else {
                    break;
                };
                page_token = Some(next);
            }
            events
        }

        fn channel_update(
            fid: u64,
            channel_id: &[u8],
            name: &str,
            membership_mode: Option<MembershipMode>,
            timestamp: u32,
        ) -> proto::Message {
            messages_factory::create_message_with_data(
                fid,
                MessageType::ChannelUpdate,
                Body::ChannelUpdateBody(proto::ChannelUpdateBody {
                    channel_id: channel_id.to_vec(),
                    name: Some(name.to_string()),
                    membership_mode: membership_mode.map(|mode| mode as i32),
                    ..Default::default()
                }),
                Some(timestamp),
                None,
            )
        }

        fn verification_contract_add(fid: u64, address: Vec<u8>, timestamp: u32) -> proto::Message {
            messages_factory::verifications::create_verification_add(
                fid,
                1,
                address,
                vec![],
                vec![0xB6; 32],
                Some(timestamp),
                None,
            )
        }

        fn channel_member(
            author_fid: u64,
            channel_id: &[u8],
            target_fid: u64,
            action: ChannelMemberAction,
            timestamp: u32,
        ) -> proto::Message {
            messages_factory::create_message_with_data(
                author_fid,
                MessageType::ChannelMember,
                Body::ChannelMemberBody(proto::ChannelMemberBody {
                    channel_id: channel_id.to_vec(),
                    fid: target_fid,
                    action: action as i32,
                }),
                Some(timestamp),
                None,
            )
        }

        fn channel_pin(
            fid: u64,
            channel_id: &[u8],
            cast_hash: Vec<u8>,
            timestamp: u32,
        ) -> proto::Message {
            messages_factory::create_message_with_data(
                fid,
                MessageType::ChannelPin,
                Body::ChannelPinBody(proto::ChannelPinBody {
                    channel_id: channel_id.to_vec(),
                    cast_hash,
                }),
                Some(timestamp),
                None,
            )
        }

        fn channel_moderate(
            fid: u64,
            channel_id: &[u8],
            cast_hash: Vec<u8>,
            timestamp: u32,
        ) -> proto::Message {
            messages_factory::create_message_with_data(
                fid,
                MessageType::ChannelModerate,
                Body::ChannelModerateBody(proto::ChannelModerateBody {
                    channel_id: channel_id.to_vec(),
                    cast_hash,
                    action: ChannelModerateAction::Hide as i32,
                }),
                Some(timestamp),
                None,
            )
        }

        async fn assert_rejected_without_side_effects(
            driver: &mut ScenarioDriver,
            message: &proto::Message,
            reason: &str,
        ) {
            let before = driver.state_fingerprint();
            let error = driver.block_engine.simulate_message(message).unwrap_err();
            assert!(
                error.to_string().contains(reason),
                "expected rejection containing {reason:?}, got {error:?}"
            );
            assert_eq!(driver.state_fingerprint(), before);

            // Compare two full five-tick windows. Every five consecutive Devnet shard-0 blocks
            // crosses exactly one heartbeat boundary, so the rejected window and contentless
            // control window exercise identical housekeeping even when the rejected block itself
            // happens to be the heartbeat block.
            let block = driver.commit_messages(vec![message.clone()]).await;
            assert!(block.events.iter().all(|event| !matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(message)
            )));
            let base_timestamp = message.data.as_ref().unwrap().timestamp;
            for offset in 1..5 {
                driver
                    .commit(vec![], base_timestamp.saturating_add(offset))
                    .await;
            }
            let after_rejected_window = driver.state_fingerprint();
            assert_eq!(
                after_rejected_window.shard_zero_root, before.shard_zero_root,
                "rejected window changed the shard-0 trie root"
            );
            assert_eq!(
                after_rejected_window.replica_roots, before.replica_roots,
                "rejected window changed one or more replica trie roots"
            );
            assert_eq!(
                after_rejected_window.channel_rows, before.channel_rows,
                "rejected window changed channel rows on shard 0 or a replica"
            );
            let rejected_delta = driver.event_delta(&before, &after_rejected_window);
            assert!(rejected_delta
                .hub_event_types_by_source_shard
                .iter()
                .all(|(_, event_type)| *event_type != HubEventType::MergeFailure as i32));

            let control_before = after_rejected_window;
            for offset in 5..10 {
                driver
                    .commit(vec![], base_timestamp.saturating_add(offset))
                    .await;
            }
            let control_after = driver.state_fingerprint();
            assert_eq!(
                control_after.shard_zero_root,
                control_before.shard_zero_root
            );
            assert_eq!(control_after.replica_roots, control_before.replica_roots);
            assert_eq!(control_after.channel_rows, control_before.channel_rows);
            let control_delta = driver.event_delta(&control_before, &control_after);
            assert_eq!(
                rejected_delta, control_delta,
                "a committed rejection contributed events beyond an empty five-tick control window"
            );
        }

        async fn assert_store_rejection_without_state_or_fanout(
            driver: &mut ScenarioDriver,
            message: &proto::Message,
            expected_code: &str,
            expected_reason: &str,
        ) {
            let before = driver.state_fingerprint();
            let error = driver.block_engine.simulate_message(message).unwrap_err();
            assert!(
                error.to_string().contains(expected_reason),
                "unexpected store rejection: {error:?}"
            );
            assert_eq!(driver.state_fingerprint(), before);

            let block = driver.commit_messages(vec![message.clone()]).await;
            assert!(block.events.iter().all(|event| !matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(message)
            )));
            let base_timestamp = message.data.as_ref().unwrap().timestamp;
            for offset in 1..5 {
                driver
                    .commit(vec![], base_timestamp.saturating_add(offset))
                    .await;
            }
            let after_duplicate_window = driver.state_fingerprint();
            assert_eq!(
                after_duplicate_window.shard_zero_root,
                before.shard_zero_root
            );
            assert_eq!(after_duplicate_window.replica_roots, before.replica_roots);
            assert_eq!(after_duplicate_window.channel_rows, before.channel_rows);

            let shard_zero_events = HubEvent::get_events(
                driver.block_engine.stores().db,
                before.hub_event_cursors[0]
                    .1
                    .map(|event_id| event_id + 1)
                    .unwrap_or(0),
                None,
                Some(PageOptions {
                    page_size: Some(
                        after_duplicate_window.hub_event_cursors[0].0
                            - before.hub_event_cursors[0].0,
                    ),
                    ..Default::default()
                }),
            )
            .unwrap()
            .events;
            let merge_failures = shard_zero_events
                .iter()
                .filter(|event| event.r#type() == HubEventType::MergeFailure)
                .collect::<Vec<_>>();
            assert_eq!(merge_failures.len(), 1);
            assert!(matches!(
                merge_failures[0].body.as_ref(),
                Some(hub_event::Body::MergeFailure(body))
                    if body.message.as_ref() == Some(message)
                        && body.code == expected_code
                        && body.reason == expected_reason
            ));

            let mut duplicate_delta = driver.event_delta(&before, &after_duplicate_window);
            let merge_failure_position = duplicate_delta
                .hub_event_types_by_source_shard
                .iter()
                .position(|entry| *entry == (0, HubEventType::MergeFailure as i32))
                .unwrap();
            duplicate_delta
                .hub_event_types_by_source_shard
                .remove(merge_failure_position);
            assert!(duplicate_delta
                .hub_event_types_by_source_shard
                .iter()
                .all(|(_, event_type)| *event_type != HubEventType::MergeFailure as i32));

            let control_before = after_duplicate_window;
            for offset in 5..10 {
                driver
                    .commit(vec![], base_timestamp.saturating_add(offset))
                    .await;
            }
            let control_after = driver.state_fingerprint();
            assert_eq!(
                control_after.shard_zero_root,
                control_before.shard_zero_root
            );
            assert_eq!(control_after.replica_roots, control_before.replica_roots);
            assert_eq!(control_after.channel_rows, control_before.channel_rows);
            assert_eq!(
                duplicate_delta,
                driver.event_delta(&control_before, &control_after),
                "store rejection contributed more than its one documented shard-0 MERGE_FAILURE"
            );
        }

        async fn transfer_order_driver(
            channel_key: &str,
            owner_fid: u64,
            new_owner_fid: u64,
            register_block: u32,
        ) -> (ScenarioDriver, Vec<u8>, Vec<u8>, u32) {
            let mut driver = ScenarioDriver::new().await;
            let old_owner_address = owner_address(0xC1);
            let new_owner_address = owner_address(0xC2);
            driver.register_user(owner_fid, old_owner_address.clone(), 1);
            driver.register_user(new_owner_fid, new_owner_address.clone(), 1);
            driver.sync_new_block_events().await;

            let channel_id = channel_label(channel_key);
            let timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            channel_key,
                            channel_id.clone(),
                            old_owner_address.clone(),
                            now_unix_seconds() + 3_600,
                            ChannelRegisterEventType::Register,
                            register_block,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;
            driver
                .commit_messages(vec![
                    verification_contract_add(owner_fid, old_owner_address, timestamp + 1),
                    channel_update(
                        owner_fid,
                        &channel_id,
                        "before transfer",
                        Some(MembershipMode::Open),
                        timestamp + 2,
                    ),
                ])
                .await;
            driver.assert_converged();
            (driver, channel_id, new_owner_address, timestamp + 3)
        }

        fn assert_member_collections(
            reads: &ReadSnapshot,
            channel_id: &[u8],
            expected_members: &[(u64, proto::ChannelMemberState)],
            expected_membership: Option<proto::ChannelMemberState>,
        ) {
            let member_rows = reads
                .members
                .members
                .iter()
                .map(|member| (member.fid, member.state))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                member_rows.len(),
                reads.members.members.len(),
                "member collection contains a duplicate fid"
            );
            assert_eq!(
                member_rows,
                expected_members
                    .iter()
                    .map(|(fid, state)| (*fid, *state as i32))
                    .collect::<BTreeMap<_, _>>()
            );
            assert_eq!(reads.members.next_page_token, None);

            match expected_membership {
                Some(state) => {
                    assert_eq!(reads.memberships.memberships.len(), 1);
                    assert_eq!(reads.memberships.memberships[0].channel_id, channel_id);
                    assert_eq!(reads.memberships.memberships[0].state, state as i32);
                }
                None => assert!(reads.memberships.memberships.is_empty()),
            }
            assert_eq!(reads.memberships.next_page_token, None);
        }

        fn assert_pin_and_moderation(
            reads: &ReadSnapshot,
            moderator_fid: u64,
            pin_hash: &[u8],
            moderated_hash: &[u8],
        ) {
            let pinned = reads.pin.pin.as_ref().expect("channel has a pin");
            assert_eq!(pinned.cast_hash.as_slice(), pin_hash);
            assert_eq!(pinned.author_fid, moderator_fid);
            assert_eq!(reads.moderations.moderations.len(), 1);
            assert_eq!(reads.moderations.moderations[0].cast_hash, moderated_hash);
            assert_eq!(
                reads.moderations.moderations[0].action,
                ChannelModerateAction::Hide as i32
            );
            assert_eq!(reads.moderations.moderations[0].author_fid, moderator_fid);
            assert_eq!(reads.moderations.next_page_token, None);
        }

        #[tokio::test]
        async fn s1_full_channel_lifecycle_keeps_replicas_events_and_six_reads_in_sync() {
            const OWNER_FID: u64 = 7_201;
            const NEW_OWNER_FID: u64 = 7_202;
            const MODERATOR_FID: u64 = 7_203;
            const MEMBER_FID: u64 = 7_204;
            const LEAVER_FID: u64 = 7_205;

            let mut driver = ScenarioDriver::new().await;
            let old_owner_address = owner_address(0x81);
            let new_owner_address = owner_address(0x82);
            for (fid, address) in [
                (OWNER_FID, old_owner_address.clone()),
                (NEW_OWNER_FID, new_owner_address.clone()),
                (MODERATOR_FID, owner_address(0x83)),
                (MEMBER_FID, owner_address(0x84)),
                (LEAVER_FID, owner_address(0x85)),
            ] {
                driver.register_user(fid, address, 1);
            }
            driver.sync_new_block_events().await;

            let channel_key = "scenario-lifecycle";
            let channel_id = channel_label(channel_key);
            let mut timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            channel_key,
                            channel_id.clone(),
                            old_owner_address.clone(),
                            now_unix_seconds() + 3_600,
                            ChannelRegisterEventType::Register,
                            100,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::None as i32);
            assert_member_collections(&reads, &channel_id, &[], None);
            assert_eq!(reads.pin.pin, None);
            assert!(reads.moderations.moderations.is_empty());
            assert_eq!(reads.metadata.name, None);

            timestamp += 1;
            driver
                .commit_messages(vec![verification_contract_add(
                    OWNER_FID,
                    old_owner_address.clone(),
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::None as i32);
            assert_member_collections(&reads, &channel_id, &[], None);
            assert_eq!(reads.pin.pin, None);
            assert!(reads.moderations.moderations.is_empty());
            assert_eq!(reads.metadata.name, None);

            timestamp += 1;
            driver
                .commit_messages(vec![channel_update(
                    OWNER_FID,
                    &channel_id,
                    "open",
                    Some(MembershipMode::Open),
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));
            assert_eq!(reads.metadata.membership_mode, MembershipMode::Open as i32);
            assert_eq!(reads.member.state, proto::ChannelMemberState::None as i32);
            assert_member_collections(&reads, &channel_id, &[], None);
            assert_eq!(reads.pin.pin, None);
            assert!(reads.moderations.moderations.is_empty());

            timestamp += 1;
            driver
                .commit_messages(vec![channel_member(
                    MEMBER_FID,
                    &channel_id,
                    MEMBER_FID,
                    ChannelMemberAction::AddMember,
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::Member as i32);
            assert_member_collections(
                &reads,
                &channel_id,
                &[(MEMBER_FID, proto::ChannelMemberState::Member)],
                Some(proto::ChannelMemberState::Member),
            );
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));
            assert_eq!(reads.pin.pin, None);
            assert!(reads.moderations.moderations.is_empty());

            timestamp += 1;
            driver
                .commit_messages(vec![
                    channel_member(
                        OWNER_FID,
                        &channel_id,
                        MODERATOR_FID,
                        ChannelMemberAction::AddModerator,
                        timestamp,
                    ),
                    channel_member(
                        OWNER_FID,
                        &channel_id,
                        LEAVER_FID,
                        ChannelMemberAction::AddMember,
                        timestamp + 1,
                    ),
                ])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MODERATOR_FID).await;
            assert_eq!(
                reads.member.state,
                proto::ChannelMemberState::Moderator as i32
            );
            let three_members = [
                (MODERATOR_FID, proto::ChannelMemberState::Moderator),
                (MEMBER_FID, proto::ChannelMemberState::Member),
                (LEAVER_FID, proto::ChannelMemberState::Member),
            ];
            assert_member_collections(
                &reads,
                &channel_id,
                &three_members,
                Some(proto::ChannelMemberState::Moderator),
            );
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));
            assert_eq!(reads.pin.pin, None);
            assert!(reads.moderations.moderations.is_empty());

            timestamp += 2;
            let pin_hash = vec![0x91; 20];
            let moderated_hash = vec![0x92; 20];
            driver
                .commit_messages(vec![
                    channel_pin(MODERATOR_FID, &channel_id, pin_hash.clone(), timestamp),
                    channel_moderate(
                        MODERATOR_FID,
                        &channel_id,
                        moderated_hash.clone(),
                        timestamp + 1,
                    ),
                ])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::Member as i32);
            assert_member_collections(
                &reads,
                &channel_id,
                &three_members,
                Some(proto::ChannelMemberState::Member),
            );
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            timestamp += 2;
            driver
                .commit_messages(vec![channel_member(
                    MODERATOR_FID,
                    &channel_id,
                    MEMBER_FID,
                    ChannelMemberAction::Ban,
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MEMBER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::Banned as i32);
            let banned_members = [
                (MODERATOR_FID, proto::ChannelMemberState::Moderator),
                (MEMBER_FID, proto::ChannelMemberState::Banned),
                (LEAVER_FID, proto::ChannelMemberState::Member),
            ];
            assert_member_collections(
                &reads,
                &channel_id,
                &banned_members,
                Some(proto::ChannelMemberState::Banned),
            );
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            timestamp += 1;
            driver
                .commit_messages(vec![channel_member(
                    OWNER_FID,
                    &channel_id,
                    LEAVER_FID,
                    ChannelMemberAction::RemoveMember,
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, LEAVER_FID).await;
            assert_eq!(
                reads.member.state,
                proto::ChannelMemberState::Removed as i32
            );
            let removed_member_rows = [
                (MODERATOR_FID, proto::ChannelMemberState::Moderator),
                (MEMBER_FID, proto::ChannelMemberState::Banned),
                (LEAVER_FID, proto::ChannelMemberState::Removed),
            ];
            assert_member_collections(
                &reads,
                &channel_id,
                &removed_member_rows,
                Some(proto::ChannelMemberState::Removed),
            );
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            timestamp += 1;
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            "",
                            channel_id.clone(),
                            new_owner_address.clone(),
                            0,
                            ChannelRegisterEventType::Transfer,
                            101,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;
            driver.assert_converged();
            let parked_owner = get_channel_owner_response(&driver.service, channel_key).await;
            assert_eq!(parked_owner.owner_address, new_owner_address);
            assert_eq!(parked_owner.fid, 0);
            let reads = driver.reads(&channel_id, MODERATOR_FID).await;
            assert_eq!(
                reads.member.state,
                proto::ChannelMemberState::Moderator as i32
            );
            assert_member_collections(
                &reads,
                &channel_id,
                &removed_member_rows,
                Some(proto::ChannelMemberState::Moderator),
            );
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            let frozen_messages = [
                channel_update(OWNER_FID, &channel_id, "stale", None, timestamp + 1),
                channel_pin(MODERATOR_FID, &channel_id, vec![0x93; 20], timestamp + 11),
                channel_update(
                    NEW_OWNER_FID,
                    &channel_id,
                    "unverified",
                    None,
                    timestamp + 21,
                ),
            ];
            for message in &frozen_messages {
                assert_rejected_without_side_effects(&mut driver, message, "channel is parked")
                    .await;
            }
            timestamp += 31;
            driver
                .commit_messages(vec![channel_member(
                    MODERATOR_FID,
                    &channel_id,
                    MODERATOR_FID,
                    ChannelMemberAction::RemoveMember,
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, MODERATOR_FID).await;
            assert_eq!(
                reads.member.state,
                proto::ChannelMemberState::Removed as i32
            );
            let self_left_rows = [
                (MODERATOR_FID, proto::ChannelMemberState::Removed),
                (MEMBER_FID, proto::ChannelMemberState::Banned),
                (LEAVER_FID, proto::ChannelMemberState::Removed),
            ];
            assert_member_collections(
                &reads,
                &channel_id,
                &self_left_rows,
                Some(proto::ChannelMemberState::Removed),
            );
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            timestamp += 1;
            driver
                .commit_messages(vec![verification_contract_add(
                    NEW_OWNER_FID,
                    new_owner_address,
                    timestamp,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, NEW_OWNER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::None as i32);
            assert_member_collections(&reads, &channel_id, &self_left_rows, None);
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("open"));

            driver
                .commit_messages(vec![channel_update(
                    NEW_OWNER_FID,
                    &channel_id,
                    "managed by new owner",
                    Some(MembershipMode::Approval),
                    timestamp + 1,
                )])
                .await;
            driver.assert_converged();
            let reads = driver.reads(&channel_id, NEW_OWNER_FID).await;
            assert_eq!(reads.member.state, proto::ChannelMemberState::None as i32);
            assert_member_collections(&reads, &channel_id, &self_left_rows, None);
            assert_pin_and_moderation(&reads, MODERATOR_FID, &pin_hash, &moderated_hash);
            assert_eq!(reads.metadata.name.as_deref(), Some("managed by new owner"));
            assert_eq!(
                reads.metadata.membership_mode,
                MembershipMode::Approval as i32
            );
        }

        #[tokio::test]
        async fn s3_mixed_blocks_without_lend_are_deterministic_across_authority_transitions() {
            // LendStorage is excluded from this mix; its replay interactions are
            // tracked separately.
            const OWNER_FID: u64 = 7_401;
            const REQUEST_FID: u64 = 7_402;
            const NEW_OWNER_FID: u64 = 7_403;

            let mut driver = ScenarioDriver::new().await;
            let owner_custody = PrivateKeySigner::random();
            let request_custody = PrivateKeySigner::random();
            let new_owner_address = owner_address(0xB3);
            let owner_address = owner_custody.address().as_slice().to_vec();
            driver.register_user(OWNER_FID, owner_address.clone(), 1);
            driver.register_user(
                REQUEST_FID,
                request_custody.address().as_slice().to_vec(),
                1,
            );
            driver.register_user(NEW_OWNER_FID, new_owner_address.clone(), 1);
            driver.sync_new_block_events().await;

            let channel_key = "scenario-determinism-green";
            let channel_id = channel_label(channel_key);
            let mut timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            channel_key,
                            channel_id.clone(),
                            owner_address.clone(),
                            now_unix_seconds() + 3_600,
                            ChannelRegisterEventType::Register,
                            210,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;

            timestamp += 1;
            let mixed = vec![
                verification_contract_add(OWNER_FID, owner_address.clone(), timestamp),
                channel_update(
                    OWNER_FID,
                    &channel_id,
                    "mixed",
                    Some(MembershipMode::Open),
                    timestamp + 1,
                ),
                channel_member(
                    OWNER_FID,
                    &channel_id,
                    OWNER_FID,
                    ChannelMemberAction::AddMember,
                    timestamp + 2,
                ),
                channel_pin(OWNER_FID, &channel_id, vec![0xB4; 20], timestamp + 3),
                channel_moderate(OWNER_FID, &channel_id, vec![0xB5; 20], timestamp + 4),
                messages_factory::keys::create_key_add(
                    OWNER_FID,
                    &owner_custody,
                    REQUEST_FID,
                    &request_custody,
                    &generate_signer(),
                    vec![MessageType::CastAdd],
                    3_600,
                    1,
                    timestamp + 1_000_000,
                    Some(timestamp + 5),
                ),
            ];
            let mixed_block = driver.commit_messages(mixed.clone()).await;
            for message in &mixed {
                assert_eq!(
                    mixed_block
                        .events
                        .iter()
                        .filter(|event| matches!(
                            event.data.as_ref().and_then(|data| data.body.as_ref()),
                            Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                                if body.message.as_ref() == Some(message)
                        ))
                        .count(),
                    1,
                    "mixed block must fan out exactly one event for {:?}",
                    message.msg_type()
                );
                assert!(
                    TrieKey::for_message(message).iter().all(|key| driver
                        .block_engine
                        .trie_key_exists(&merkle_trie::Context::new(), key)),
                    "mixed message {:?} must land in the proposed and committed shard-0 root",
                    message.msg_type()
                );
            }
            driver.assert_converged();

            timestamp += 6;
            let remove = verification_remove(OWNER_FID, owner_address.clone(), timestamp);
            let parked_update = channel_update(
                OWNER_FID,
                &channel_id,
                "must not merge while parked",
                None,
                timestamp + 1,
            );
            let park_block = driver
                .commit_messages(vec![remove.clone(), parked_update.clone()])
                .await;
            assert!(TrieKey::for_message(&remove).iter().all(|key| driver
                .block_engine
                .trie_key_exists(&merkle_trie::Context::new(), key)));
            assert!(TrieKey::for_message(&parked_update)
                .iter()
                .all(|key| !driver
                    .block_engine
                    .trie_key_exists(&merkle_trie::Context::new(), key)));
            assert!(park_block.events.iter().all(|event| !matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(&parked_update)
            )));
            driver.assert_converged();

            timestamp += 2;
            let reverify = verification_contract_add(OWNER_FID, owner_address.clone(), timestamp);
            let thawed_update = channel_update(
                OWNER_FID,
                &channel_id,
                "thawed",
                Some(MembershipMode::Approval),
                timestamp + 1,
            );
            let thaw_block = driver
                .commit_messages(vec![reverify.clone(), thawed_update.clone()])
                .await;
            for message in [&reverify, &thawed_update] {
                assert_eq!(
                    thaw_block
                        .events
                        .iter()
                        .filter(|event| matches!(
                            event.data.as_ref().and_then(|data| data.body.as_ref()),
                            Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                                if body.message.as_ref() == Some(message)
                        ))
                        .count(),
                    1
                );
            }
            driver.assert_converged();

            timestamp += 2;
            let stale_owner_update =
                channel_update(OWNER_FID, &channel_id, "stale owner", None, timestamp + 1);
            let transfer = events_factory::create_channel_register_event(
                "",
                channel_id.clone(),
                new_owner_address,
                0,
                ChannelRegisterEventType::Transfer,
                211,
                1,
            );
            let transfer_block = driver
                .commit(
                    vec![
                        MempoolMessage::OnchainEvent(transfer),
                        MempoolMessage::UserMessage(stale_owner_update.clone()),
                    ],
                    timestamp + 1,
                )
                .await;
            let old_owner_txn = transfer_block
                .transactions
                .iter()
                .position(|txn| txn.fid == OWNER_FID)
                .unwrap();
            let transfer_txn = transfer_block
                .transactions
                .iter()
                .position(|txn| txn.fid == 0)
                .unwrap();
            let update_landed = TrieKey::for_message(&stale_owner_update).iter().all(|key| {
                driver
                    .block_engine
                    .trie_key_exists(&merkle_trie::Context::new(), key)
            });
            assert_eq!(
                update_landed,
                old_owner_txn < transfer_txn,
                "the frozen transaction order must decide whether the old-owner action lands"
            );
            assert_eq!(
                transfer_block.events.iter().any(|event| matches!(
                    event.data.as_ref().and_then(|data| data.body.as_ref()),
                    Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                        if body.message.as_ref() == Some(&stale_owner_update)
                )),
                update_landed
            );
            driver.assert_converged();
            let reads = driver.reads(&channel_id, OWNER_FID).await;
            assert_eq!(
                reads.metadata.name.as_deref(),
                Some(if update_landed {
                    "stale owner"
                } else {
                    "thawed"
                })
            );

            let next_block_old_owner =
                channel_update(OWNER_FID, &channel_id, "next block", None, timestamp + 2);
            assert_rejected_without_side_effects(
                &mut driver,
                &next_block_old_owner,
                "channel is parked",
            )
            .await;
        }

        #[tokio::test]
        async fn s3_transfer_transaction_orders_are_explicitly_bounded_by_the_next_block() {
            // Production permits either HashMap-grouped cross-fid order. The test-only proposal
            // entry point changes only that proposer choice, then runs the same transaction
            // replay, root/event construction, validation, commit, and replica fan-out pipeline.
            const TRANSFER_FIRST_OWNER: u64 = 7_501;
            const TRANSFER_FIRST_NEW_OWNER: u64 = 7_502;
            const ACTION_FIRST_OWNER: u64 = 7_511;
            const ACTION_FIRST_NEW_OWNER: u64 = 7_512;

            // Order A: transfer applies before the old-owner action. The new address is
            // deliberately unverified, so the old owner is rejected as parked with no state,
            // event-stream, row, or trie side effect.
            let (mut transfer_first, channel_id, new_address, timestamp) = transfer_order_driver(
                "scenario-transfer-first",
                TRANSFER_FIRST_OWNER,
                TRANSFER_FIRST_NEW_OWNER,
                220,
            )
            .await;
            let rejected = channel_update(
                TRANSFER_FIRST_OWNER,
                &channel_id,
                "must stay parked",
                None,
                timestamp,
            );
            let before_transfer_first = transfer_first.state_fingerprint();
            let transfer_first_block = transfer_first
                .commit_with_transaction_order(
                    vec![
                        MempoolMessage::OnchainEvent(
                            events_factory::create_channel_register_event(
                                "",
                                channel_id.clone(),
                                new_address.clone(),
                                0,
                                ChannelRegisterEventType::Transfer,
                                221,
                                1,
                            ),
                        ),
                        MempoolMessage::UserMessage(rejected.clone()),
                    ],
                    &[0, TRANSFER_FIRST_OWNER],
                    timestamp,
                )
                .await;
            assert!(transfer_first_block.events.iter().all(|event| !matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(&rejected)
            )));
            assert!(TrieKey::for_message(&rejected)
                .iter()
                .all(|key| !transfer_first
                    .block_engine
                    .trie_key_exists(&merkle_trie::Context::new(), key)));
            transfer_first.assert_converged();
            let after_transfer_first = transfer_first.state_fingerprint();
            assert_eq!(
                after_transfer_first.channel_rows, before_transfer_first.channel_rows,
                "transfer-first rejection changed a channel message row"
            );
            for (source_shard, db) in [
                transfer_first.block_engine.stores().db,
                transfer_first.replicas[0].db.clone(),
                transfer_first.replicas[1].db.clone(),
            ]
            .into_iter()
            .enumerate()
            {
                let appended = all_hub_events(db)
                    .into_iter()
                    .skip(before_transfer_first.hub_event_cursors[source_shard].0)
                    .collect::<Vec<_>>();
                assert_eq!(
                    appended.len(),
                    after_transfer_first.hub_event_cursors[source_shard].0
                        - before_transfer_first.hub_event_cursors[source_shard].0
                );
                assert!(appended.iter().all(|event| match event.body.as_ref() {
                    Some(hub_event::Body::MergeMessageBody(body)) => {
                        body.message.as_ref() != Some(&rejected)
                    }
                    Some(hub_event::Body::MergeFailure(body)) => {
                        body.message.as_ref() != Some(&rejected)
                    }
                    _ => true,
                }));
            }
            let owner =
                get_channel_owner_response(&transfer_first.service, "scenario-transfer-first")
                    .await;
            assert_eq!(owner.owner_address, new_address);
            assert_eq!(owner.fid, 0);
            let parked_error = transfer_first
                .block_engine
                .simulate_message(&rejected)
                .unwrap_err();
            assert!(
                parked_error.to_string().contains("channel is parked"),
                "{parked_error:?}"
            );
            let next_block_rejected = channel_update(
                TRANSFER_FIRST_OWNER,
                &channel_id,
                "still parked next block",
                None,
                timestamp + 1,
            );
            assert_rejected_without_side_effects(
                &mut transfer_first,
                &next_block_rejected,
                "channel is parked",
            )
            .await;
            let reads = transfer_first
                .reads(&channel_id, TRANSFER_FIRST_OWNER)
                .await;
            assert_eq!(reads.metadata.name.as_deref(), Some("before transfer"));

            // Order B: the still-authorized old-owner update applies first, then the transfer.
            // Both commits pass proposal validation/root equality; the very next old-owner action
            // is parked, bounding the proposer-order freedom to the transfer block.
            let (mut action_first, channel_id, new_address, timestamp) = transfer_order_driver(
                "scenario-action-first",
                ACTION_FIRST_OWNER,
                ACTION_FIRST_NEW_OWNER,
                230,
            )
            .await;
            let accepted = channel_update(
                ACTION_FIRST_OWNER,
                &channel_id,
                "old owner landed first",
                None,
                timestamp,
            );
            let accepted_block = action_first
                .commit_with_transaction_order(
                    vec![
                        MempoolMessage::UserMessage(accepted.clone()),
                        MempoolMessage::OnchainEvent(
                            events_factory::create_channel_register_event(
                                "",
                                channel_id.clone(),
                                new_address.clone(),
                                0,
                                ChannelRegisterEventType::Transfer,
                                231,
                                1,
                            ),
                        ),
                    ],
                    &[ACTION_FIRST_OWNER, 0],
                    timestamp,
                )
                .await;
            assert!(TrieKey::for_message(&accepted)
                .iter()
                .all(|key| action_first
                    .block_engine
                    .trie_key_exists(&merkle_trie::Context::new(), key)));
            assert!(accepted_block.events.iter().any(|event| matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(&accepted)
            )));
            action_first.assert_converged();
            let owner =
                get_channel_owner_response(&action_first.service, "scenario-action-first").await;
            assert_eq!(owner.owner_address, new_address);
            assert_eq!(owner.fid, 0);
            let reads = action_first.reads(&channel_id, ACTION_FIRST_OWNER).await;
            assert_eq!(
                reads.metadata.name.as_deref(),
                Some("old owner landed first")
            );

            let rejected = channel_update(
                ACTION_FIRST_OWNER,
                &channel_id,
                "too late",
                None,
                timestamp + 1,
            );
            assert_rejected_without_side_effects(&mut action_first, &rejected, "channel is parked")
                .await;
        }

        #[tokio::test]
        async fn s5_adversarial_authority_sequences_commit_without_ghost_state() {
            const OWNER_FID: u64 = 7_601;
            const MODERATOR_A: u64 = 7_602;
            const MODERATOR_B: u64 = 7_603;
            const TARGET_FID: u64 = 7_604;
            const GHOST_FID: u64 = 7_605;

            let mut driver = ScenarioDriver::new().await;
            let owner_addr = owner_address(0xE1);
            for fid in [OWNER_FID, MODERATOR_A, MODERATOR_B, TARGET_FID] {
                driver.register_user(fid, owner_address((fid & 0xff) as u8), 1);
            }
            driver.sync_new_block_events().await;

            let channel_key = "scenario-adversarial";
            let channel_id = channel_label(channel_key);
            let mut timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            channel_key,
                            channel_id.clone(),
                            owner_addr.clone(),
                            now_unix_seconds() + 3_600,
                            ChannelRegisterEventType::Register,
                            300,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;
            let current_update = channel_update(
                OWNER_FID,
                &channel_id,
                "open adversarial channel",
                Some(MembershipMode::Open),
                timestamp + 2,
            );
            driver
                .commit_messages(vec![
                    verification_contract_add(OWNER_FID, owner_addr.clone(), timestamp + 1),
                    current_update.clone(),
                ])
                .await;

            timestamp += 3;
            driver
                .commit_messages(vec![channel_member(
                    OWNER_FID,
                    &channel_id,
                    MODERATOR_A,
                    ChannelMemberAction::AddModerator,
                    timestamp,
                )])
                .await;
            driver
                .commit_messages(vec![channel_member(
                    OWNER_FID,
                    &channel_id,
                    MODERATOR_A,
                    ChannelMemberAction::Ban,
                    timestamp + 1,
                )])
                .await;
            let laundering_attempt = channel_member(
                OWNER_FID,
                &channel_id,
                MODERATOR_A,
                ChannelMemberAction::RemoveModerator,
                timestamp + 2,
            );
            assert_rejected_without_side_effects(
                &mut driver,
                &laundering_attempt,
                "channel member is banned",
            )
            .await;
            driver
                .commit_messages(vec![
                    channel_member(
                        OWNER_FID,
                        &channel_id,
                        MODERATOR_A,
                        ChannelMemberAction::Unban,
                        timestamp + 3,
                    ),
                    channel_member(
                        OWNER_FID,
                        &channel_id,
                        MODERATOR_A,
                        ChannelMemberAction::AddModerator,
                        timestamp + 4,
                    ),
                    channel_member(
                        OWNER_FID,
                        &channel_id,
                        MODERATOR_B,
                        ChannelMemberAction::AddModerator,
                        timestamp + 5,
                    ),
                ])
                .await;

            let demote_bypass = channel_member(
                MODERATOR_A,
                &channel_id,
                MODERATOR_B,
                ChannelMemberAction::RemoveMember,
                timestamp + 6,
            );
            assert_rejected_without_side_effects(
                &mut driver,
                &demote_bypass,
                "invalid channel target state",
            )
            .await;

            let slots_before_ghosts = ChannelMemberStore::slot_count(
                &driver.block_engine.stores().channel_member_store,
                &channel_id,
                None,
            )
            .unwrap();
            for (offset, action) in [
                ChannelMemberAction::Unban,
                ChannelMemberAction::RemoveMember,
            ]
            .into_iter()
            .enumerate()
            {
                let ghost = channel_member(
                    OWNER_FID,
                    &channel_id,
                    GHOST_FID,
                    action,
                    timestamp + 7 + offset as u32,
                );
                assert_rejected_without_side_effects(
                    &mut driver,
                    &ghost,
                    "invalid channel target state",
                )
                .await;
                assert_eq!(
                    ChannelMemberStore::slot_count(
                        &driver.block_engine.stores().channel_member_store,
                        &channel_id,
                        None,
                    )
                    .unwrap(),
                    slots_before_ghosts,
                    "a rejected ghost action minted a permanent member slot"
                );
            }

            driver
                .commit_messages(vec![channel_member(
                    MODERATOR_A,
                    &channel_id,
                    TARGET_FID,
                    ChannelMemberAction::Ban,
                    timestamp + 9,
                )])
                .await;
            let banned_self_add = channel_member(
                TARGET_FID,
                &channel_id,
                TARGET_FID,
                ChannelMemberAction::AddMember,
                timestamp + 10,
            );
            assert_rejected_without_side_effects(
                &mut driver,
                &banned_self_add,
                "channel member is banned",
            )
            .await;

            let owner_ban = channel_member(
                MODERATOR_A,
                &channel_id,
                OWNER_FID,
                ChannelMemberAction::Ban,
                timestamp + 11,
            );
            assert_rejected_without_side_effects(
                &mut driver,
                &owner_ban,
                "channel owner cannot be banned",
            )
            .await;

            driver
                .commit_messages(vec![verification_remove(
                    OWNER_FID,
                    owner_addr.clone(),
                    timestamp + 12,
                )])
                .await;
            let parked_owner = get_channel_owner_response(&driver.service, channel_key).await;
            assert_eq!(parked_owner.owner_address, owner_addr);
            assert_eq!(parked_owner.fid, 0);
            let parked_owner_ban = channel_member(
                MODERATOR_A,
                &channel_id,
                OWNER_FID,
                ChannelMemberAction::Ban,
                timestamp + 13,
            );
            assert_rejected_without_side_effects(
                &mut driver,
                &parked_owner_ban,
                "channel is parked",
            )
            .await;
            driver
                .commit_messages(vec![verification_contract_add(
                    OWNER_FID,
                    owner_addr,
                    timestamp + 14,
                )])
                .await;

            // Two moderators are already live. Reach nine, then submit the tenth and eleventh in
            // one owner transaction so the cap observes the tenth's uncommitted counter update.
            driver
                .commit_messages(
                    (0..7)
                        .map(|index| {
                            channel_member(
                                OWNER_FID,
                                &channel_id,
                                7_700 + index,
                                ChannelMemberAction::AddModerator,
                                timestamp + 15 + index as u32,
                            )
                        })
                        .collect(),
                )
                .await;
            assert_eq!(
                ChannelMemberStore::live_moderator_count(
                    &driver.block_engine.stores().channel_member_store,
                    &channel_id,
                    None,
                )
                .unwrap(),
                9
            );
            let tenth = channel_member(
                OWNER_FID,
                &channel_id,
                7_800,
                ChannelMemberAction::AddModerator,
                timestamp + 22,
            );
            let eleventh = channel_member(
                OWNER_FID,
                &channel_id,
                7_801,
                ChannelMemberAction::AddModerator,
                timestamp + 23,
            );
            let cap_block = driver
                .commit_messages(vec![tenth.clone(), eleventh.clone()])
                .await;
            assert_eq!(
                ChannelMemberStore::live_moderator_count(
                    &driver.block_engine.stores().channel_member_store,
                    &channel_id,
                    None,
                )
                .unwrap(),
                10
            );
            assert!(TrieKey::for_message(&tenth).iter().all(|key| driver
                .block_engine
                .trie_key_exists(&merkle_trie::Context::new(), key)));
            assert!(TrieKey::for_message(&eleventh).iter().all(|key| !driver
                .block_engine
                .trie_key_exists(&merkle_trie::Context::new(), key)));
            let tenth_event = cap_block
                .events
                .iter()
                .find(|event| {
                    matches!(
                        event.data.as_ref().and_then(|data| data.body.as_ref()),
                        Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                            if body.message.as_ref() == Some(&tenth)
                    )
                })
                .unwrap()
                .clone();
            assert!(cap_block.events.iter().all(|event| !matches!(
                event.data.as_ref().and_then(|data| data.body.as_ref()),
                Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                    if body.message.as_ref() == Some(&eleventh)
            )));
            driver.assert_converged();

            assert_store_rejection_without_state_or_fanout(
                &mut driver,
                &current_update,
                "bad_request.duplicate",
                "message has already been merged",
            )
            .await;

            // The same already-applied fan-out event is below max_seqnum+1. Direct ShardEngine
            // replay freezes that decision into an empty-event proposal and leaves channel rows,
            // trie roots, merge streams, and max seqnum untouched.
            for replica in &mut driver.replicas {
                let root_before = replica.trie_root_hash();
                let rows_before = channel_rows(&replica.db);
                let merge_events_before =
                    ScenarioDriver::replicated_event_bodies(replica.db.clone());
                let seqnum_before = replica.get_stores().block_event_store.max_seqnum().unwrap();
                let shard_id = replica.shard_id();
                let state_change = replica.propose_state_change(
                    shard_id,
                    vec![MempoolMessage::BlockEvent {
                        for_shard: shard_id,
                        message: tenth_event.clone(),
                    }],
                    None,
                );
                assert!(state_change.events.is_empty());
                test_helper::validate_and_commit_state_change(replica, &state_change).await;
                assert_eq!(replica.trie_root_hash(), root_before);
                assert_eq!(channel_rows(&replica.db), rows_before);
                assert_eq!(
                    ScenarioDriver::replicated_event_bodies(replica.db.clone()),
                    merge_events_before
                );
                assert_eq!(
                    replica.get_stores().block_event_store.max_seqnum().unwrap(),
                    seqnum_before
                );
            }
        }

        #[tokio::test]
        async fn s6_member_and_moderation_caps_converge_with_paginated_by_fid_index() {
            const OWNER_FID: u64 = 7_901;
            const PAGE_FID: u64 = 7_902;
            // Pin the production caps so a regression in either constant fails here. The
            // boundary-rejection logic below (`count >= slot_cap` in `merge_slot`) is cap-value
            // independent, so we exercise it against a shrunken shared cap instead of inserting
            // the real 8k/16k rows — that shaves this test from ~80s to under a second in debug.
            // The override collapses both stores to one value, hence MEMBER_CAP == MODERATE_CAP.
            assert_eq!(CHANNEL_MEMBER_SLOT_CAP, 8_192);
            assert_eq!(CHANNEL_MODERATE_SLOT_CAP, 16_384);
            const SLOT_CAP: u32 = 24;
            const MEMBER_CAP: u32 = SLOT_CAP;
            const MODERATE_CAP: u32 = SLOT_CAP;
            const SCENARIO_EXPIRY: u64 = u64::MAX;

            let mut driver = ScenarioDriver::new_with_slot_cap(Some(SLOT_CAP)).await;
            let owner_address = owner_address(0xF1);
            driver.register_user(OWNER_FID, owner_address.clone(), 1);
            driver.sync_new_block_events().await;

            let member_key = "scenario-member-cap";
            let moderate_key = "scenario-moderate-cap";
            let member_channel = channel_label(member_key);
            let moderate_channel = channel_label(moderate_key);
            let timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![
                        MempoolMessage::OnchainEvent(
                            events_factory::create_channel_register_event(
                                member_key,
                                member_channel.clone(),
                                owner_address.clone(),
                                SCENARIO_EXPIRY,
                                ChannelRegisterEventType::Register,
                                400,
                                1,
                            ),
                        ),
                        MempoolMessage::OnchainEvent(
                            events_factory::create_channel_register_event(
                                moderate_key,
                                moderate_channel.clone(),
                                owner_address.clone(),
                                SCENARIO_EXPIRY,
                                ChannelRegisterEventType::Register,
                                400,
                                2,
                            ),
                        ),
                    ],
                    timestamp,
                )
                .await;
            driver
                .commit_messages(vec![verification_contract_add(
                    OWNER_FID,
                    owner_address.clone(),
                    timestamp + 1,
                )])
                .await;

            let member_seed = (0..MEMBER_CAP - 1)
                .map(|index| {
                    channel_member(
                        OWNER_FID,
                        &member_channel,
                        100_000 + u64::from(index),
                        ChannelMemberAction::AddMember,
                        timestamp + 2,
                    )
                })
                .collect::<Vec<_>>();
            for chunk in member_seed.chunks(2_048) {
                driver.commit_messages_batched_replay(chunk.to_vec()).await;
            }
            for count in [
                ChannelMemberStore::slot_count(
                    &driver.block_engine.stores().channel_member_store,
                    &member_channel,
                    None,
                )
                .unwrap(),
                ChannelMemberStore::slot_count(
                    &driver.replicas[0].get_stores().channel_member_store,
                    &member_channel,
                    None,
                )
                .unwrap(),
                ChannelMemberStore::slot_count(
                    &driver.replicas[1].get_stores().channel_member_store,
                    &member_channel,
                    None,
                )
                .unwrap(),
            ] {
                assert_eq!(count, MEMBER_CAP - 1);
            }
            let final_member = channel_member(
                OWNER_FID,
                &member_channel,
                100_000 + u64::from(MEMBER_CAP - 1),
                ChannelMemberAction::AddMember,
                timestamp + 3,
            );
            driver.commit_messages(vec![final_member.clone()]).await;
            driver.assert_converged();
            let member_over_cap = channel_member(
                OWNER_FID,
                &member_channel,
                100_000 + u64::from(MEMBER_CAP),
                ChannelMemberAction::AddMember,
                timestamp + 4,
            );
            assert_store_rejection_without_state_or_fanout(
                &mut driver,
                &member_over_cap,
                "bad_request.validation_failure",
                "channel slot cap exceeded",
            )
            .await;
            assert_eq!(
                ChannelMemberStore::slot_count(
                    &driver.block_engine.stores().channel_member_store,
                    &member_channel,
                    None,
                )
                .unwrap(),
                MEMBER_CAP
            );

            let cast_hash = |index: u32| {
                let mut hash = vec![0xF2; 20];
                hash[16..].copy_from_slice(&index.to_be_bytes());
                hash
            };
            let moderate_seed = (0..MODERATE_CAP - 1)
                .map(|index| {
                    channel_moderate(
                        OWNER_FID,
                        &moderate_channel,
                        cast_hash(index),
                        timestamp + 5,
                    )
                })
                .collect::<Vec<_>>();
            for chunk in moderate_seed.chunks(2_048) {
                driver.commit_messages_batched_replay(chunk.to_vec()).await;
            }
            for count in [
                ChannelModerateStore::slot_count(
                    &driver.block_engine.stores().channel_moderate_store,
                    &moderate_channel,
                    None,
                )
                .unwrap(),
                ChannelModerateStore::slot_count(
                    &driver.replicas[0].get_stores().channel_moderate_store,
                    &moderate_channel,
                    None,
                )
                .unwrap(),
                ChannelModerateStore::slot_count(
                    &driver.replicas[1].get_stores().channel_moderate_store,
                    &moderate_channel,
                    None,
                )
                .unwrap(),
            ] {
                assert_eq!(count, MODERATE_CAP - 1);
            }
            let final_moderation = channel_moderate(
                OWNER_FID,
                &moderate_channel,
                cast_hash(MODERATE_CAP - 1),
                timestamp + 6,
            );
            driver.commit_messages(vec![final_moderation.clone()]).await;
            driver.assert_converged();
            let moderate_over_cap = channel_moderate(
                OWNER_FID,
                &moderate_channel,
                cast_hash(MODERATE_CAP),
                timestamp + 7,
            );
            assert_store_rejection_without_state_or_fanout(
                &mut driver,
                &moderate_over_cap,
                "bad_request.validation_failure",
                "channel slot cap exceeded",
            )
            .await;
            assert_eq!(
                ChannelModerateStore::slot_count(
                    &driver.block_engine.stores().channel_moderate_store,
                    &moderate_channel,
                    None,
                )
                .unwrap(),
                MODERATE_CAP
            );

            // Exercise the gated by-fid index across enough channels to require several pages.
            let page_channels = (0..65u32)
                .map(|index| {
                    let key = format!("scenario-page-{index:03}");
                    (key.clone(), channel_label(&key), index)
                })
                .collect::<Vec<_>>();
            driver
                .commit(
                    page_channels
                        .iter()
                        .map(|(key, channel_id, index)| {
                            MempoolMessage::OnchainEvent(
                                events_factory::create_channel_register_event(
                                    key,
                                    channel_id.clone(),
                                    owner_address.clone(),
                                    SCENARIO_EXPIRY,
                                    ChannelRegisterEventType::Register,
                                    401,
                                    *index,
                                ),
                            )
                        })
                        .collect(),
                    timestamp + 8,
                )
                .await;
            driver
                .commit_messages(
                    page_channels
                        .iter()
                        .map(|(_, channel_id, _)| {
                            channel_member(
                                OWNER_FID,
                                channel_id,
                                PAGE_FID,
                                ChannelMemberAction::AddMember,
                                timestamp + 9,
                            )
                        })
                        .collect(),
                )
                .await;
            driver.assert_converged();

            let block_stores = driver.block_engine.stores();
            let replica_a = driver.replicas[0].get_stores();
            let replica_b = driver.replicas[1].get_stores();
            for store in [
                &block_stores.channel_member_store,
                &replica_a.channel_member_store,
                &replica_b.channel_member_store,
            ] {
                let mut page_token = None;
                let mut entries = Vec::new();
                let mut page_count = 0;
                let mut page_lengths = Vec::new();
                loop {
                    let page = ChannelMemberStore::memberships_by_fid(
                        store,
                        PAGE_FID,
                        &PageOptions {
                            page_size: Some(17),
                            page_token,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    page_count += 1;
                    page_lengths.push(page.entries.len());
                    entries.extend(page.entries);
                    let Some(next) = page.next_page_token else {
                        break;
                    };
                    page_token = Some(next);
                }
                assert_eq!(page_count, 4);
                assert_eq!(page_lengths, vec![17, 17, 17, 14]);
                assert_eq!(entries.len(), page_channels.len());
                assert_eq!(
                    entries
                        .iter()
                        .map(|entry| entry.channel_id.clone())
                        .collect::<HashSet<_>>(),
                    page_channels
                        .iter()
                        .map(|(_, channel_id, _)| channel_id.clone())
                        .collect::<HashSet<_>>()
                );
            }
        }

        #[tokio::test]
        async fn s7_malformed_pin_and_moderation_widths_never_reach_reads_or_replicas() {
            const OWNER_FID: u64 = 7_990;

            let mut driver = ScenarioDriver::new().await;
            let owner_address = owner_address(0xF7);
            driver.register_user(OWNER_FID, owner_address.clone(), 1);
            driver.sync_new_block_events().await;
            let channel_key = "scenario-widths";
            let channel_id = channel_label(channel_key);
            let timestamp = messages_factory::farcaster_time();
            driver
                .commit(
                    vec![MempoolMessage::OnchainEvent(
                        events_factory::create_channel_register_event(
                            channel_key,
                            channel_id.clone(),
                            owner_address.clone(),
                            now_unix_seconds() + 3_600,
                            ChannelRegisterEventType::Register,
                            500,
                            1,
                        ),
                    )],
                    timestamp,
                )
                .await;
            driver
                .commit_messages(vec![verification_contract_add(
                    OWNER_FID,
                    owner_address,
                    timestamp + 1,
                )])
                .await;

            let valid_pin_hash = vec![0x17; 20];
            let valid_moderation_hash = vec![0x18; 20];
            driver
                .commit_messages(vec![
                    channel_pin(
                        OWNER_FID,
                        &channel_id,
                        valid_pin_hash.clone(),
                        timestamp + 2,
                    ),
                    channel_moderate(
                        OWNER_FID,
                        &channel_id,
                        valid_moderation_hash.clone(),
                        timestamp + 3,
                    ),
                ])
                .await;
            let valid_reads = driver.reads(&channel_id, OWNER_FID).await;
            let pinned = valid_reads.pin.pin.as_ref().expect("channel has a pin");
            assert_eq!(pinned.cast_hash, valid_pin_hash);
            assert_eq!(pinned.author_fid, OWNER_FID);
            assert_eq!(valid_reads.moderations.moderations.len(), 1);
            assert_eq!(
                valid_reads.moderations.moderations[0].cast_hash,
                valid_moderation_hash
            );
            assert_eq!(
                valid_reads.moderations.moderations[0].action,
                ChannelModerateAction::Hide as i32
            );
            assert_eq!(valid_reads.moderations.moderations[0].author_fid, OWNER_FID);

            let short_pin = channel_pin(OWNER_FID, &channel_id, vec![0x27; 19], timestamp + 4);
            assert_rejected_without_side_effects(
                &mut driver,
                &short_pin,
                "channel pin cast hash must be empty or 20 bytes",
            )
            .await;
            let long_pin = channel_pin(OWNER_FID, &channel_id, vec![0x37; 21], timestamp + 14);
            assert_rejected_without_side_effects(
                &mut driver,
                &long_pin,
                "channel pin cast hash must be empty or 20 bytes",
            )
            .await;
            let short_moderation =
                channel_moderate(OWNER_FID, &channel_id, vec![0x28; 19], timestamp + 24);
            assert_rejected_without_side_effects(
                &mut driver,
                &short_moderation,
                "channel moderate cast hash must be 20 bytes",
            )
            .await;
            let long_moderation =
                channel_moderate(OWNER_FID, &channel_id, vec![0x38; 21], timestamp + 34);
            assert_rejected_without_side_effects(
                &mut driver,
                &long_moderation,
                "channel moderate cast hash must be 20 bytes",
            )
            .await;

            // Empty is the intentional pin-width exception: it is a valid unpin sentinel.
            driver
                .commit_messages(vec![channel_pin(
                    OWNER_FID,
                    &channel_id,
                    vec![],
                    timestamp + 44,
                )])
                .await;

            driver.assert_converged();
            let reads = driver.reads(&channel_id, OWNER_FID).await;
            assert!(reads.pin.pin.is_none());
            assert_eq!(reads.moderations.moderations.len(), 1);
            assert_eq!(
                reads.moderations.moderations[0].cast_hash,
                valid_moderation_hash
            );
            assert_eq!(
                reads.moderations.moderations[0].action,
                ChannelModerateAction::Hide as i32
            );
            assert_eq!(reads.moderations.moderations[0].author_fid, OWNER_FID);
            for stores in [
                driver.block_engine.stores().channel_moderate_store,
                driver.replicas[0].get_stores().channel_moderate_store,
                driver.replicas[1].get_stores().channel_moderate_store,
            ] {
                assert_eq!(
                    ChannelModerateStore::slot_count(&stores, &channel_id, None).unwrap(),
                    1
                );
            }
            for stores in [
                driver.block_engine.stores().channel_pin_store,
                driver.replicas[0].get_stores().channel_pin_store,
                driver.replicas[1].get_stores().channel_pin_store,
            ] {
                assert_eq!(
                    ChannelPinStore::get_channel_pin(&stores, &channel_id, None)
                        .unwrap()
                        .unwrap()
                        .cast_hash,
                    Vec::<u8>::new()
                );
            }
        }
    }

    // Registers `channel_key` to `owner_address` at a distinct chain position so
    // several channels can coexist under one or more owner addresses.
    fn register_channel_at(
        block_engine: &BlockEngine,
        channel_key: &str,
        owner_address: Vec<u8>,
        expiry: u64,
        log_index: u32,
    ) {
        merge_channel_event(
            block_engine,
            events_factory::create_channel_register_event(
                channel_key,
                channel_label(channel_key),
                owner_address,
                expiry,
                ChannelRegisterEventType::Register,
                1,
                log_index,
            ),
        );
    }

    async fn channels_by_fid_page(
        service: &MyHubService,
        fid: u64,
        page_size: Option<u32>,
        page_token: Option<Vec<u8>>,
    ) -> proto::ChannelsResponse {
        service
            .get_channels_by_fid(Request::new(ChannelsByFidRequest {
                fid,
                page_size,
                page_token,
            }))
            .await
            .unwrap()
            .into_inner()
    }

    // Scenario (g): a fid with two verified addresses owning four channels total,
    // walked with page_size smaller than the total. Asserts the paged union equals
    // the unpaginated result with no duplicates, that a page crosses an address
    // boundary, and that the final page's token is unset (== enumeration complete).
    #[tokio::test]
    async fn test_channel_reads_by_fid_cursor_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        // owner_address(20) < owner_address(21) lexicographically, so the composite
        // cursor visits address_a's channels before address_b's.
        let address_a = owner_address(20);
        let address_b = owner_address(21);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "rt_a1", address_a.clone(), expiry, 1);
        register_channel_at(&block_engine, "rt_a2", address_a.clone(), expiry, 2);
        register_channel_at(&block_engine, "rt_b1", address_b.clone(), expiry, 3);
        register_channel_at(&block_engine, "rt_b2", address_b.clone(), expiry, 4);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_a.clone(), 10),
        );
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address_b.clone(), 20),
        );

        // Unpaginated baseline: all four channels, no token.
        let full = channels_by_fid_page(&service, SHARD1_FID, None, None).await;
        let full_keys: Vec<String> = full
            .channels
            .iter()
            .map(|c| c.channel_key.clone())
            .collect();
        assert_eq!(full_keys, vec!["rt_a1", "rt_a2", "rt_b1", "rt_b2"]);
        assert_eq!(full.next_page_token, None);

        // Page 1 of 2 (page_size 3) must cross the address_a -> address_b boundary.
        let page1 = channels_by_fid_page(&service, SHARD1_FID, Some(3), None).await;
        assert_eq!(page1.channels.len(), 3);
        assert!(page1.next_page_token.is_some());
        let page1_owners: HashSet<Vec<u8>> = page1
            .channels
            .iter()
            .map(|c| c.owner_address.clone())
            .collect();
        assert_eq!(page1_owners.len(), 2, "page 1 should span both addresses");

        // Page 2 resumes strictly after the token and completes.
        let page2 =
            channels_by_fid_page(&service, SHARD1_FID, Some(3), page1.next_page_token.clone())
                .await;
        assert_eq!(page2.next_page_token, None);

        let mut walked: Vec<String> = page1
            .channels
            .iter()
            .chain(page2.channels.iter())
            .map(|c| c.channel_key.clone())
            .collect();
        let deduped: HashSet<String> = walked.iter().cloned().collect();
        assert_eq!(deduped.len(), walked.len(), "pages must not duplicate");
        walked.sort();
        assert_eq!(walked, vec!["rt_a1", "rt_a2", "rt_b1", "rt_b2"]);
        for channel in page1.channels.iter().chain(page2.channels.iter()) {
            assert_eq!(channel.fid, SHARD1_FID);
        }
    }

    // The composite cursor round-trips at the single-address level too, and the
    // page-size clamp caps an omitted/oversized request without dropping results.
    #[tokio::test]
    async fn test_channel_reads_by_address_pagination_round_trip() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(22);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "pa_1", address.clone(), expiry, 1);
        register_channel_at(&block_engine, "pa_2", address.clone(), expiry, 2);
        register_channel_at(&block_engine, "pa_3", address.clone(), expiry, 3);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address.clone(), 10),
        );

        let mut walked = Vec::new();
        let mut page_token = None;
        loop {
            let response = service
                .get_channels_by_address(Request::new(ChannelsByAddressRequest {
                    owner_address: address.clone(),
                    page_size: Some(2),
                    page_token: page_token.clone(),
                    reverse: None,
                }))
                .await
                .unwrap()
                .into_inner();
            for channel in &response.channels {
                assert_eq!(channel.fid, SHARD1_FID);
                walked.push(channel.channel_key.clone());
            }
            match response.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }

        assert_eq!(walked, vec!["pa_1", "pa_2", "pa_3"]);
    }

    // Pins the page-size contract shared with GetChannelsByFid: 0 is an empty
    // page with no token, not "at least one row".
    #[tokio::test]
    async fn test_channel_reads_by_address_zero_page_size_returns_empty_page() {
        let (
            _stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(24);
        merge_channel_registration(
            &block_engine,
            "zero_page",
            address.clone(),
            now_unix_seconds() + 3600,
        );

        let response = service
            .get_channels_by_address(Request::new(ChannelsByAddressRequest {
                owner_address: address,
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.channels.is_empty());
        assert_eq!(response.next_page_token, None);
    }

    // Non-EVM (Solana) and malformed verifications must never contribute an
    // owner address to the EVM-only channel resolution.
    #[tokio::test]
    async fn test_channel_reads_by_fid_excludes_non_ethereum() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let evm_address = owner_address(23);
        let solana_address = vec![7u8; 32];
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "evm_owned", evm_address.clone(), expiry, 1);

        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, evm_address, 10),
        );
        // The Solana verification's 32-byte address must be filtered out before
        // resolution. Without the protocol/length filter it would be treated as an
        // owner address and fail the 20-byte EVM validation, erroring the request.
        let solana_verification = messages_factory::verifications::create_verification_add(
            SHARD1_FID,
            2, // Protocol::Solana
            solana_address,
            vec![],
            vec![],
            Some(20),
            None,
        );
        merge_verification(stores.get(&1).unwrap(), &solana_verification);

        let keys = get_channels_by_fid_keys(&service, SHARD1_FID).await;
        assert_eq!(keys, vec!["evm_owned"]);
    }

    // A non-20-byte address is a client error, mapped to INVALID_ARGUMENT rather
    // than surfacing as an opaque internal error.
    #[tokio::test]
    async fn test_channel_reads_by_address_rejects_malformed_address() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let status = service
            .get_channels_by_address(channels_by_address_request(vec![0u8; 4]))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // Pins the accepted last-key cursor contract (shared with by-address): when a
    // page fills exactly on the final entry, a token is still returned and the
    // next request returns an empty final page with the token unset. Callers page
    // until the token is unset; a present token does not guarantee more results.
    #[tokio::test]
    async fn test_channel_reads_by_fid_exact_boundary_yields_final_empty_page() {
        let (
            stores,
            _senders,
            _engines,
            block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let address = owner_address(25);
        let expiry = now_unix_seconds() + 3600;
        register_channel_at(&block_engine, "eb_1", address.clone(), expiry, 1);
        register_channel_at(&block_engine, "eb_2", address.clone(), expiry, 2);
        merge_channel_owner_verification(
            stores.get(&1).unwrap(),
            &block_engine,
            &verification_add(SHARD1_FID, address, 10),
        );

        // Two channels, page_size 2: the page fills exactly on the last entry.
        let page1 = channels_by_fid_page(&service, SHARD1_FID, Some(2), None).await;
        assert_eq!(page1.channels.len(), 2);
        assert!(
            page1.next_page_token.is_some(),
            "boundary-aligned page still returns a token"
        );

        // Resuming yields the final empty page with no token.
        let page2 =
            channels_by_fid_page(&service, SHARD1_FID, Some(2), page1.next_page_token.clone())
                .await;
        assert!(page2.channels.is_empty());
        assert_eq!(page2.next_page_token, None);
    }

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let (
            stores,
            senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let num_shard1_pre_existing_events = 10;
        let num_shard2_pre_existing_events = 20;

        write_events_to_db(
            stores.get(&1u32).unwrap().shard_store.db.clone(),
            num_shard1_pre_existing_events,
        )
        .await;
        write_events_to_db(
            stores.get(&2u32).unwrap().shard_store.db.clone(),
            num_shard2_pre_existing_events,
        )
        .await;

        let num_shard1_events = 5;
        let num_shard2_events = 10;
        let shard1_subscriber = subscribe_and_listen(
            &service,
            1,
            Some(0),
            num_shard1_events + num_shard1_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;
        let shard2_subscriber = subscribe_and_listen(
            &service,
            2,
            Some(0),
            num_shard2_events + num_shard2_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;

        // Allow time for rpc handler to subscribe to event rx channels
        tokio::time::sleep(Duration::from_secs(1)).await;

        send_events(
            senders.get(&1u32).unwrap().events_tx.clone(),
            num_shard1_events,
        )
        .await;
        send_events(
            senders.get(&2u32).unwrap().events_tx.clone(),
            num_shard2_events,
        )
        .await;

        let _ = shard1_subscriber.await;
        let _ = shard2_subscriber.await;
    }

    #[tokio::test]
    async fn test_subscribe_with_filter_rpc() {
        let (
            stores,
            senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let num_shard1_pre_existing_events = 10;
        let num_shard2_pre_existing_events = 20;

        write_events_to_db(
            stores.get(&1u32).unwrap().shard_store.db.clone(),
            num_shard1_pre_existing_events,
        )
        .await;
        write_events_to_db(
            stores.get(&2u32).unwrap().shard_store.db.clone(),
            num_shard2_pre_existing_events,
        )
        .await;

        let num_shard1_events = 5;
        let shard1_subscriber = subscribe_and_listen(
            &service,
            1,
            Some(0),
            num_shard1_events + num_shard1_pre_existing_events,
            vec![HubEventType::MergeMessage as i32],
        )
        .await;
        let shard2_subscriber = subscribe_and_listen(
            &service,
            2,
            Some(0),
            0,
            vec![HubEventType::PruneMessage as i32],
        )
        .await;

        // Allow time for rpc handler to subscribe to event rx channels
        tokio::time::sleep(Duration::from_secs(1)).await;

        send_events(
            senders.get(&1u32).unwrap().events_tx.clone(),
            num_shard1_events,
        )
        .await;
        send_events(senders.get(&2u32).unwrap().events_tx.clone(), 0).await;

        let _ = shard1_subscriber.await;
        let _ = shard2_subscriber.await;
    }

    #[tokio::test]
    async fn test_get_event_success() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let event_id = 12345;
        let hub_event = HubEvent {
            r#type: HubEventType::MergeMessage as i32,
            id: event_id,
            body: None,
            block_number: 0,
            shard_index: 0,
            timestamp: 0,
        };

        let db = stores.get(&1u32).unwrap().shard_store.db.clone();
        let mut txn = RocksDbTransactionBatch::new();
        HubEvent::put_event_transaction(&mut txn, &hub_event).unwrap();
        db.commit(txn).unwrap();

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 1,
        });
        let response = service.get_event(request).await.unwrap();

        let hub_event_response = response.into_inner();
        assert_eq!(hub_event_response.block_number, event_id >> SEQUENCE_BITS);
        assert_eq!(hub_event_response.shard_index, 1);
        assert_eq!(hub_event_response.r#type, hub_event.r#type);
        assert_eq!(hub_event_response.id, event_id);
    }

    #[tokio::test]
    async fn test_get_event_not_found() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: 99999, // Junk event ID
            shard_index: 1,
        });
        let response = service.get_event(request).await;

        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "not_found/Event not found");
    }

    #[tokio::test]
    async fn test_get_event_invalid_shard() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let event_id = 12345;
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 999, // junk shard
        });
        let response = service.get_event(request).await;

        // Validate the response
        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "no shard store for fid");
    }

    #[tokio::test]
    async fn test_get_event_missing_shard_index() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let event_id = 12345;
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 1).await;

        let request = Request::new(proto::EventRequest {
            id: event_id,
            shard_index: 0,
        });
        let response = service.get_event(request).await;

        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "no shard store for fid");
    }

    #[tokio::test]
    async fn test_get_events() {
        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Write some test events to the DB
        write_events_to_db(stores.get(&1u32).unwrap().shard_store.db.clone(), 10).await;

        // Test getting first page
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: None,
            stop_id: None,
            page_size: Some(3),
            page_token: None,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 3);
        let next_page_token = response.get_ref().next_page_token.clone();

        // Test getting second page
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: None,
            stop_id: None,
            page_size: Some(3),
            page_token: next_page_token,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 3);

        // Test getting from only one shard
        let request = Request::new(proto::EventsRequest {
            start_id: 0,
            shard_index: Some(2),
            stop_id: None,
            page_size: Some(3),
            page_token: None,
            reverse: None,
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 0); // No events in shard 2

        // Test with start_id and stop_id with reverse pagination, on shard 1
        let request = Request::new(proto::EventsRequest {
            start_id: 2,
            shard_index: Some(1),
            stop_id: Some(8),
            page_size: Some(7),
            page_token: None,
            reverse: Some(true),
        });
        let response = service.get_events(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 6);
        assert_eq!(events[0].id, 7);
        assert_eq!(events[events.len() - 1].id, 2);
        assert!(events[0].shard_index > 0);
    }

    #[tokio::test]
    async fn test_submit_message_fails_with_error_for_invalid_messages() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Message with no fid registration
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let response = submit_message(&service, invalid_message).await.unwrap_err();

        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.validation_failure/unknown fid"
        );
        assert_eq!(
            response
                .metadata()
                .get("x-err-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "bad_request.validation_failure"
        );

        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let valid_message =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        test_helper::commit_message(&mut engine1, &valid_message).await;

        // Submitting a duplicate message should return an error
        let response = submit_message(&service, valid_message).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.duplicate/message has already been merged"
        );
        assert_eq!(
            response
                .metadata()
                .get("x-err-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "bad_request.duplicate"
        );
    }

    #[tokio::test]
    async fn test_authentication() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let user1_pass = format!("pw1-{now}");
        let user2_pass = format!("pw2-{now}");
        let auth_config = format!("user1:{user1_pass},user2:{user2_pass}");

        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(Some(auth_config), None).await;
        let message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let no_auth_request = Request::new(message.clone());
        // Providing no auth fails
        let response = service.submit_message(no_auth_request).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "missing authorization header");

        let mut invalid_creds_request = Request::new(message.clone());
        add_auth_header(&mut invalid_creds_request, "user3", &user1_pass);
        let response = service
            .submit_message(invalid_creds_request)
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::Unauthenticated);
        assert_eq!(response.message(), "invalid username or password");

        let mut valid_creds_request = Request::new(message.clone());
        add_auth_header(&mut valid_creds_request, "user2", &user2_pass);
        let response = service
            .submit_message(valid_creds_request)
            .await
            .unwrap_err();
        // Authenticated but no fid registration
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            response.message(),
            "bad_request.validation_failure/unknown fid"
        );
    }

    // The mesh diagnostic endpoints are admin-gated, and that gate must run
    // BEFORE the response cache — a cached view/topology must never reach an
    // unauthenticated caller. We can't easily prime the cache with a success in
    // this harness (the gossip receiver is dropped), but asserting that an
    // unauthenticated call returns Unauthenticated (rather than a cached value
    // or an internal gossip error) proves the auth check is ordered first.
    #[tokio::test]
    async fn test_mesh_endpoints_authenticate_before_cache() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let admin = format!("admin:secret-{now}");
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, Some(admin)).await;

        let mesh_req = || {
            Request::new(proto::GetMeshViewRequest {
                validators_only: true,
                ttl: 0,
                visited_peer_ids: vec![],
            })
        };

        // No credentials -> Unauthenticated (not a cached value, not internal).
        assert_eq!(
            service.get_mesh_view(mesh_req()).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            service
                .get_mesh_topology(mesh_req())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        // Wrong credentials -> still Unauthenticated.
        let mut bad = mesh_req();
        add_auth_header(&mut bad, "admin", "wrong");
        assert_eq!(
            service.get_mesh_view(bad).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    // Tests for submit_bulk_messages RPC endpoint

    #[tokio::test]
    async fn test_submit_bulk_messages_empty() {
        let (
            _stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Test submitting 0 messages
        let response = submit_bulk_messages(&service, vec![]).await.unwrap();
        let messages = response.into_inner().messages;
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test]
    async fn test_submit_bulk_messages_valid() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Register a user
        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        // Create 2 valid messages with different text to avoid duplicates
        let message1 = messages_factory::casts::create_cast_add(SHARD1_FID, "test1", None, None);
        let message2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let messages = vec![message1, message2];

        let response = submit_bulk_messages(&service, messages).await.unwrap();
        let responses = response.into_inner().messages;

        assert_eq!(responses.len(), 2);

        // We're expecting both messages to succeed (upto adding to the mempool, which isn't setup in this test here)
        // So, we assert for "unavailable - Error adding to mempool"
        for (i, response) in responses.iter().enumerate() {
            assert!(response.response.is_some());
            match response.response.as_ref().unwrap() {
                proto::bulk_message_response::Response::Message(_) => {
                    // Succeeds
                }
                proto::bulk_message_response::Response::MessageError(err) => {
                    // We expect mempool error for valid messages in this test setup
                    assert_eq!(err.err_code, "unavailable");
                    assert!(
                        err.message.contains("Error adding to mempool"),
                        "Message {} should fail with mempool error, got: {}",
                        i,
                        err.message
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_submit_bulk_messages_mixed() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // Register a user for the valid message
        register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        // Create 1 valid message and 1 invalid message (unknown fid)
        let valid_message =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);
        let messages = vec![valid_message, invalid_message];

        let response = submit_bulk_messages(&service, messages).await.unwrap();
        let responses = response.into_inner().messages;

        assert_eq!(responses.len(), 2);

        // First message should fail with mempool error (but passes validation)
        assert!(responses[0].response.is_some());
        match &responses[0].response {
            Some(proto::bulk_message_response::Response::Message(_)) => {
                // Succeeds
            }
            Some(proto::bulk_message_response::Response::MessageError(error)) => {
                // Should fail with mempool error, not validation error
                assert_eq!(error.err_code, "unavailable");
                assert!(error.message.contains("Error adding to mempool"));
            }
            None => panic!("Response should not be None"),
        }

        // Second message should fail
        assert!(responses[1].response.is_some());
        match &responses[1].response {
            Some(proto::bulk_message_response::Response::MessageError(error)) => {
                assert_eq!(error.err_code, "bad_request.validation_failure");
                assert_eq!(error.message, "unknown fid");
            }
            Some(proto::bulk_message_response::Response::Message(_)) => {
                panic!("Expected second message to fail");
            }
            None => panic!("Response should not be None"),
        }
    }

    #[tokio::test]
    async fn test_event_timestamp() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let cast_add = messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let cast_add2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let mut shard_chunks = vec![];

        shard_chunks
            .push(test_helper::commit_messages(&mut engine1, vec![cast_add, cast_add2]).await);

        sleep(Duration::from_secs(1)).await;

        let cast_add3 = messages_factory::casts::create_cast_add(SHARD1_FID, "test3", None, None);

        shard_chunks.push(test_helper::commit_message(&mut engine1, &cast_add3).await);

        let request = Request::new(SubscribeRequest {
            event_types: vec![HubEventType::MergeMessage as i32],
            from_id: Some(0),
            shard_index: Some(1),
        });
        let mut listener = service.subscribe(request).await.unwrap();

        let mut events = vec![];
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_secs(2) {
                break;
            }

            let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
            if let Ok(Some(Ok(hub_event))) = event {
                assert_ne!(hub_event.timestamp, 0);
                events.push(hub_event);
            }
        }

        let cast_add4 = messages_factory::casts::create_cast_add(SHARD1_FID, "test4", None, None);

        shard_chunks.push(test_helper::commit_message(&mut engine1, &cast_add4).await);

        let event = timeout(Duration::from_millis(100), listener.get_mut().next()).await;
        if let Ok(Some(Ok(hub_event))) = event {
            assert_ne!(hub_event.timestamp, 0);
            events.push(hub_event);
        }

        let assert_events = |events: &Vec<HubEvent>, shard_chunks: &Vec<ShardChunk>| {
            assert_eq!(events.len(), 4);
            assert_eq!(shard_chunks.len(), 3);
            assert_eq!(
                events[0].timestamp,
                shard_chunks[0].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[1].timestamp,
                shard_chunks[0].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[2].timestamp,
                shard_chunks[1].header.as_ref().unwrap().timestamp
            );
            assert_eq!(
                events[3].timestamp,
                shard_chunks[2].header.as_ref().unwrap().timestamp
            );
        };
        assert_events(&events, &shard_chunks);

        let req = Request::new(EventsRequest {
            start_id: events[0].id,
            stop_id: None,
            shard_index: Some(1),
            page_size: None,
            page_token: None,
            reverse: None,
        });
        let res = service.get_events(req).await.unwrap();
        let inner_res = res.into_inner();
        let filtered_events: Vec<HubEvent> = inner_res
            .events
            .into_iter()
            .filter(|event| event.r#type == HubEventType::MergeMessage as i32)
            .collect();
        assert_eq!(filtered_events.len(), 4);
        assert_events(&filtered_events, &shard_chunks);

        let req = Request::new(EventRequest {
            shard_index: 1,
            id: HubEventIdGenerator::make_event_id_for_block_number(
                shard_chunks[2]
                    .header
                    .as_ref()
                    .unwrap()
                    .height
                    .unwrap()
                    .block_number,
            ),
        });
        let res = service.get_event(req).await.unwrap();
        assert_eq!(
            res.into_inner().timestamp,
            shard_chunks[2].header.as_ref().unwrap().timestamp
        );
    }

    #[tokio::test]
    async fn test_good_ens_proof() {
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let signer = test_helper::default_signer();
        let owner = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_basename_proof() {
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let signer = test_helper::default_signer();
        let owner = hex::decode("849151d7D0bF1F34b70d5caD5149D28CC2308bf1").unwrap();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.base.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeBasename as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;
        assert!(result.is_ok());

        let user_data_add = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::Username,
            &"username.base.eth".to_string(),
            None,
            None,
        );

        // User data add fails because the proof is not committed yet
        let result = submit_message(&service, user_data_add.clone()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

        let proof_message =
            messages_factory::username_proof::create_from_proof(&username_proof, None);
        test_helper::commit_message(&mut engine1, &proof_message).await;

        // Now the user data add should succeed
        let result = submit_message(&service, user_data_add.clone()).await;
        assert_eq!(result.unwrap().into_inner(), user_data_add);
    }

    // Mock fname registry that simulates the side-effect of a real registry
    // poll: the lookup itself commits the transfer to the engine, mirroring the
    // consensus path (mempool -> block -> engine merge) that the unit test
    // fixture doesn't run. The recovery path under test still pushes the
    // transfer through the mempool; the commit-on-lookup is what ensures the
    // poll loop eventually sees the proof in the store.
    struct MockFnameLookup {
        engine: Arc<tokio::sync::Mutex<ShardEngine>>,
        transfer: FnameTransfer,
    }

    #[async_trait]
    impl FnameTransferLookup for MockFnameLookup {
        async fn lookup_fname(&self, _fname: &str) -> Result<Vec<FnameTransfer>, FetchError> {
            let mut engine = self.engine.lock().await;
            test_helper::commit_fname_transfer(&mut *engine, &self.transfer).await;
            Ok(vec![self.transfer.clone()])
        }
    }

    #[tokio::test]
    async fn test_user_data_add_username_recovers_when_fname_transfer_pending() {
        let (
            _stores,
            _senders,
            [_engine1, engine2],
            _block_engine,
            mut service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        // FID_FOR_TEST is 1234 — even, so EvenOddRouterForTest routes it to shard 2,
        // which is backed by engine2's RocksDB.
        let fid = test_helper::FID_FOR_TEST;
        let fname = "acp".to_string();
        let owner = hex::decode("711aa8ec273dae42e51732fe1be2b15ee53b00a4").unwrap();

        // Hardcoded fname transfer signed by the default fname signer, lifted
        // from test_merge_fname so we don't have to spin up a custom signer.
        let target_transfer = FnameTransfer {
            id: 1234,
            from_fid: 0,
            proof: Some(UserNameProof {
                timestamp: 1660233642,
                name: fname.as_bytes().to_vec(),
                owner: owner.clone(),
                signature: hex::decode("ebd1b040a4961c5ea751e8ec867d4af6fdbf80ade6775d33dad94ab1c0423dc64a2f684d0e48b89f2958a2385b91743647161ade04e6628a166b5bd1579d86ff1b").unwrap(),
                fid,
                r#type: UserNameType::UsernameTypeFname as i32,
            }),
        };

        let engine2 = Arc::new(tokio::sync::Mutex::new(engine2));
        {
            let mut engine = engine2.lock().await;
            test_helper::register_user(
                fid,
                test_helper::default_signer(),
                owner.clone(),
                &mut *engine,
            )
            .await;
        }

        let user_data_add = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::Username,
            &fname,
            None,
            None,
        );

        // Without recovery wired, submit_message fails because the fname proof
        // hasn't been ingested yet — the race condition this fix targets.
        let err = submit_message(&service, user_data_add.clone())
            .await
            .expect_err("expected MissingFname error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("fname is not registered"),
            "unexpected error message: {}",
            err.message()
        );

        // Wire up the mock lookup. The next submit_message should detect
        // MissingFname, drive the lookup (which lands the proof), and succeed
        // on retry within the recovery budget.
        service.set_fname_lookup_for_test(Arc::new(MockFnameLookup {
            engine: engine2.clone(),
            transfer: target_transfer.clone(),
        }));

        let response = submit_message(&service, user_data_add.clone())
            .await
            .expect("submit_message should succeed after MissingFname recovery");
        assert_eq!(response.into_inner(), user_data_add);
    }

    #[tokio::test]
    async fn test_ens_proof_with_bad_owner() {
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner: "100000000000000000".to_string().encode_to_vec(),
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        // Proof owner does not match owner of ens name
        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ens_proof_with_bad_custody_address() {
        let (
            _stores,
            _senders,
            [mut engine1, mut _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(
            fid,
            signer.clone(),
            "100000000000000000".to_string().encode_to_vec(),
            &mut engine1,
        )
        .await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner,
            signature: "signature".to_string().encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ens_proof_with_verified_address() {
        let (
            _stores,
            _senders,
            [mut _engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let signer = test_helper::default_signer();
        let fid = 2;
        let owner = test_helper::default_custody_address();
        let signature = "signature".to_string();

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine2).await;

        let verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            None,
            None,
        );

        // This read-path test needs a historical data-shard verification row, not a new direct
        // submission. V21 rejects the latter by design, so seed the store as pre-activation state.
        merge_verification(&engine2.get_stores(), &verification_add);

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: b"username.eth".to_vec(),
            owner: hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap(),
            signature: signature.encode_to_vec(),
            fid,
            r#type: UserNameType::UsernameTypeEnsL1 as i32,
        };

        let result = service
            .validate_ens_username_proof(fid, &username_proof)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cast_apis() {
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let engine1 = &mut engine1;
        let engine2 = &mut engine2;
        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine2,
        )
        .await;
        let cast_add = messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let cast_add2 = messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None);
        let cast_remove = messages_factory::casts::create_cast_remove(
            SHARD1_FID,
            &cast_add.hash,
            Some(cast_add.data.as_ref().unwrap().timestamp + 10),
            None,
        );

        let another_shard_cast =
            messages_factory::casts::create_cast_add(SHARD2_FID, "another fid", None, None);

        test_helper::commit_message(engine1, &cast_add).await;
        test_helper::commit_message(engine1, &cast_add2).await;
        test_helper::commit_message(engine1, &cast_remove).await;
        test_helper::commit_message(engine2, &another_shard_cast).await;

        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: cast_add2.hash.clone(),
            }))
            .await
            .unwrap();
        assert_eq!(response.get_ref().hash, cast_add2.hash);

        // Fetching a removed cast fails
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: cast_add.hash.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::NotFound);

        // Fetching across shards works
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD2_FID,
                hash: another_shard_cast.hash.clone(),
            }))
            .await
            .unwrap();
        assert_eq!(response.get_ref().hash, another_shard_cast.hash);

        // Fetching on the wrong shard fails
        let response = service
            .get_cast(Request::new(proto::CastId {
                fid: SHARD1_FID,
                hash: another_shard_cast.hash.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(response.code(), tonic::Code::NotFound);

        // Returns all active casts
        let all_casts_request = proto::FidRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
        };
        let response = service
            .get_casts_by_fid(Request::new(all_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2]);

        // Pagination works
        let all_casts_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(all_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2]);

        let second_page_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: response.as_ref().unwrap().get_ref().next_page_token.clone(),
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(second_page_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

        let reverse_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: Some(true),
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(reverse_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

        // Returns all casts
        let bulk_casts_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(bulk_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2, &cast_remove]);

        // Returns casts even if page token is empty
        let empty_page_token_request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: Some(vec![]),
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_cast_messages_by_fid(Request::new(empty_page_token_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2, &cast_remove]);
    }

    #[tokio::test]
    async fn test_get_casts_by_parent_hash() {
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let engine1 = &mut engine1;
        let engine2 = &mut engine2;
        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            engine2,
        )
        .await;
        let original_cast =
            messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        let timestamp = original_cast.data.as_ref().unwrap().timestamp;
        let reply_1 = messages_factory::casts::create_cast_with_parent(
            SHARD1_FID,
            "reply 1",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 1),
            None,
        );
        let reply_2 = messages_factory::casts::create_cast_with_parent(
            SHARD1_FID,
            "reply 2",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 2),
            None,
        );
        let reply_3_another_shard = messages_factory::casts::create_cast_with_parent(
            SHARD2_FID,
            "reply 3",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 3),
            None,
        );
        let reply_4_another_shard = messages_factory::casts::create_cast_with_parent(
            SHARD2_FID,
            "reply 4",
            SHARD1_FID,
            &original_cast.hash,
            Some(timestamp + 4),
            None,
        );

        test_helper::commit_message(engine1, &original_cast).await;
        test_helper::commit_message(engine1, &reply_1).await;
        test_helper::commit_message(engine1, &reply_2).await;
        test_helper::commit_message(engine2, &reply_3_another_shard).await;
        test_helper::commit_message(engine2, &reply_4_another_shard).await;

        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(1),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_1, &reply_3_another_shard]);

        let page_token = response.get_ref().next_page_token.clone();
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(2),
                page_token: page_token,
                reverse: None,
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_2, &reply_4_another_shard]);

        // Test reverse pagination
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(1),
                page_token: None,
                reverse: Some(true),
            }))
            .await
            .unwrap();

        let page_token = response.get_ref().next_page_token.clone();
        let response = service
            .get_casts_by_parent(Request::new(proto::CastsByParentRequest {
                parent: Some(proto::casts_by_parent_request::Parent::ParentCastId(
                    proto::CastId {
                        fid: SHARD1_FID,
                        hash: original_cast.hash.clone(),
                    },
                )),
                page_size: Some(2),
                page_token: page_token.clone(),
                reverse: Some(true),
            }))
            .await
            .unwrap();
        test_helper::assert_contains_all_messages(&response, &[&reply_1, &reply_3_another_shard]);
    }

    #[tokio::test]
    async fn test_storage_limits() {
        // Works with no storage
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        let response = service
            .get_current_storage_limits_by_fid(fid_request(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 0);
        assert_eq!(response.get_ref().limits.len(), 7);
        for limit in response.get_ref().limits.iter() {
            assert_eq!(limit.limit, 0);
            assert_eq!(limit.used, 0);
        }
        assert_eq!(response.get_ref().unit_details.len(), 3);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[1].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[2].unit_size, 0);

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        // register_user will give the user a single unit of 2025 storage, let add one more legacy unit and a 2024 unit for 1 of each.
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(
                SHARD1_FID,
                1,
                StorageUnitType::UnitTypeLegacy,
                false,
                FarcasterNetwork::Devnet,
            ),
        )
        .await;
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(
                SHARD1_FID,
                1,
                StorageUnitType::UnitType2024,
                false,
                FarcasterNetwork::Devnet,
            ),
        )
        .await;
        let cast_add = &messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None);
        test_helper::commit_message(&mut engine1, cast_add).await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_add(SHARD1_FID, "test2", None, None),
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_remove(
                SHARD1_FID,
                &cast_add.hash,
                Some(cast_add.data.as_ref().unwrap().timestamp + 10),
                None,
            ),
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::links::create_link_add(SHARD1_FID, "follow", SHARD2_FID, None, None),
        )
        .await;

        let response = service
            .get_current_storage_limits_by_fid(fid_request(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 3);
        assert_eq!(response.get_ref().unit_details.len(), 3);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[0].unit_type,
            proto::StorageUnitType::UnitTypeLegacy as i32
        );
        assert_eq!(
            response.get_ref().unit_details[1].unit_type,
            proto::StorageUnitType::UnitType2024 as i32
        );
        assert_eq!(response.get_ref().unit_details[1].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[2].unit_type,
            proto::StorageUnitType::UnitType2025 as i32
        );
        assert_eq!(response.get_ref().unit_details[2].unit_size, 1);

        let casts_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Casts)
            .collect::<Vec<_>>()[0];
        let configured_limits = engine1.get_stores().store_limits;
        assert_eq!(
            casts_limit.limit as u32,
            (configured_limits
                .for_type(proto::StorageUnitType::UnitType2024)
                .casts
                * 2)
                + (configured_limits
                    .for_type(proto::StorageUnitType::UnitTypeLegacy)
                    .casts)
        );
        assert_eq!(casts_limit.used, 2); // Cast remove counts as 1
        assert_eq!(casts_limit.name, "CASTS");

        let links_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Links)
            .collect::<Vec<_>>()[0];
        assert_eq!(links_limit.used, 1);

        let storage_lends_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::StorageLends)
            .collect::<Vec<_>>()[0];
        assert_eq!(storage_lends_limit.limit, 3);
        assert_eq!(storage_lends_limit.used, 0);
    }

    #[tokio::test]
    async fn test_get_info() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::commit_message(
            &mut engine1,
            &messages_factory::casts::create_cast_add(SHARD1_FID, "test", None, None),
        )
        .await;

        let response = service
            .get_info(Request::new(proto::GetInfoRequest {}))
            .await
            .unwrap();
        let info = response.get_ref();
        assert_eq!(info.num_shards, 2);
        assert_eq!(info.shard_infos.len(), 3); // +1 for the block shard
        assert_eq!(info.peer_id, "asddef");
        assert_eq!(info.version, "0.1.2");

        let block_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 0)
            .unwrap();
        assert_eq!(block_info.shard_id, 0);
        assert_eq!(block_info.num_fid_registrations, 0);
        assert_eq!(block_info.num_messages, 0);
        assert_eq!(block_info.max_height, 0);
        assert_eq!(block_info.mempool_size, 0);

        let shard1_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 1)
            .unwrap();
        assert_eq!(shard1_info.shard_id, 1);
        assert_eq!(shard1_info.num_fid_registrations, 1);
        assert_eq!(shard1_info.num_messages, 4); // 3 onchain events for registration + 1 cast add
        assert_eq!(shard1_info.max_height, 4); // Each message above was commited in a separate block
        assert_eq!(block_info.mempool_size, 0);

        let shard2_info = info
            .shard_infos
            .iter()
            .find(|info| info.shard_id == 2)
            .unwrap();
        assert_eq!(shard2_info.shard_id, 2);
        assert_eq!(shard2_info.num_fid_registrations, 0);
        assert_eq!(shard2_info.num_messages, 0);
        assert_eq!(shard2_info.max_height, 0);
        assert_eq!(block_info.mempool_size, 0);
    }

    #[tokio::test]
    async fn test_get_username_proof_ens() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let owner = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();

        // Register the user
        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        // Create an ENS username proof
        let ens_username = "test.eth";

        // Create a username proof message and store it
        let proof_message = messages_factory::username_proof::create_username_proof(
            fid,
            UserNameType::UsernameTypeEnsL1,
            ens_username.to_string(),
            owner.clone(),
            "signature".to_string(),
            messages_factory::farcaster_time() as u64,
            None,
        );

        // Commit the message to engine1
        test_helper::commit_message(&mut engine1, &proof_message).await;

        // Test get_username_proof for ENS name
        let request = Request::new(UsernameProofRequest {
            name: ens_username.as_bytes().to_vec(),
        });

        let response = service.get_username_proof(request).await;
        assert!(
            response.is_ok(),
            "Failed to get ENS username proof: {:?}",
            response.err()
        );

        let proof = response.unwrap().into_inner();
        assert_eq!(proof.fid, fid);
        assert_eq!(proof.name, ens_username.as_bytes().to_vec());
        assert_eq!(proof.r#type, UserNameType::UsernameTypeEnsL1 as i32);
    }

    #[tokio::test]
    async fn test_get_fids() {
        let (
            _stores,
            _senders,
            [mut engine1, mut engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::register_user(
            SHARD1_FID + 2, // another fid for shard 1
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        test_helper::register_user(
            SHARD2_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine2,
        )
        .await;

        let shard1_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 1,
                page_size: Some(1),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        let res = shard1_response.into_inner();
        assert_eq!(res.fids, vec![SHARD1_FID]);
        assert!(res.next_page_token.is_some());

        let shard1_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 1,
                page_size: None,
                page_token: res.next_page_token.clone(),
                reverse: None,
            }))
            .await
            .unwrap();
        assert_eq!(shard1_response.into_inner().fids, vec![SHARD1_FID + 2]);

        let shard2_response = service
            .get_fids(Request::new(proto::FidsRequest {
                shard_id: 2,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap();
        let shard2_ref = shard2_response.get_ref();
        let shard2_fids = &shard2_ref.fids;
        assert_eq!(*shard2_fids, vec![SHARD2_FID]);
        assert_eq!(shard2_ref.next_page_token, None);
    }

    #[tokio::test]
    async fn test_get_id_registry_event_by_address() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;
        // Should we write a bunch of users to test the iteration or is this sufficient?
        test_helper::register_user(
            fid,
            test_helper::default_signer(),
            owner.clone(),
            &mut engine1,
        )
        .await;

        let request = Request::new(proto::IdRegistryEventByAddressRequest {
            address: owner.clone(),
        });
        let response = service
            .get_id_registry_on_chain_event_by_address(request)
            .await
            .unwrap();
        let event = response.into_inner();
        if let Some(proto::on_chain_event::Body::IdRegisterEventBody(body)) = event.body {
            assert_eq!(body.to, owner);
        } else {
            panic!("Expected IdRegisterEventBody");
        }
    }

    #[tokio::test]
    async fn test_get_fid_address_type() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let custody_address = test_helper::default_custody_address();
        let auth_signer = generate_signer(); // Generate a signing key for auth
        let auth_key = auth_signer.verifying_key().as_bytes().to_vec(); // Auth key with keyType=2

        // Register user with custody address
        test_helper::register_user(fid, signer.clone(), custody_address.clone(), &mut engine1)
            .await;

        // Add an auth key (keyType=2)
        let auth_signer_event = events_factory::create_signer_event(
            fid,
            auth_signer,
            proto::SignerEventType::Add,
            None,
            Some(2), // keyType=2 for auth
        );
        commit_event(&mut engine1, &auth_signer_event).await;

        // Test custody address
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: custody_address.clone(),
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(result.is_custody);
        assert!(!result.is_auth);
        assert!(!result.is_verified);

        // Test auth address
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: auth_key.clone(),
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(!result.is_custody);
        assert!(result.is_auth);
        assert!(!result.is_verified);

        // Test unknown address
        let unknown_address = hex::decode("1234567890abcdef1234567890abcdef12345678").unwrap();
        let request = Request::new(proto::FidAddressTypeRequest {
            fid,
            address: unknown_address,
        });
        let response = service.get_fid_address_type(request).await.unwrap();
        let result = response.get_ref();
        assert!(!result.is_custody);
        assert!(!result.is_auth);
        assert!(!result.is_verified);
    }

    #[tokio::test]
    async fn test_get_on_chain_signers_by_fid() {
        let (
            _stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();

        // Register user to create signer event
        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let removed_signer = generate_signer();
        let add_signer_event = events_factory::create_signer_event(
            fid,
            removed_signer.clone(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        commit_event(&mut engine1, &add_signer_event).await;

        // Remove 1 signer
        let mut remove_signer_event = events_factory::create_signer_event(
            fid,
            removed_signer,
            proto::SignerEventType::Remove,
            None,
            None,
        );
        remove_signer_event.block_number = add_signer_event.block_number + 1;
        remove_signer_event.block_timestamp = add_signer_event.block_timestamp + 1;
        commit_event(&mut engine1, &remove_signer_event).await;

        let signer_event = events_factory::create_signer_event(
            fid,
            generate_signer(),
            proto::SignerEventType::Add,
            None,
            None,
        );
        commit_event(&mut engine1, &signer_event).await;

        // Non-signer key
        let signer_event = events_factory::create_signer_event(
            fid,
            generate_signer(),
            proto::SignerEventType::Add,
            None,
            Some(2),
        );
        commit_event(&mut engine1, &signer_event).await;

        // Test normal request
        let request = Request::new(FidRequest {
            fid,
            page_size: Some(1),
            page_token: None,
            reverse: None,
        });
        let response = service.get_on_chain_signers_by_fid(request).await.unwrap();
        let events = response.get_ref().events.clone();
        assert_eq!(events.len(), 1);
        assert!(events
            .iter()
            .all(|event| event.r#type() == OnChainEventType::EventTypeSigner));

        // Test pagination
        let request = Request::new(FidRequest {
            fid,
            page_size: None,
            page_token: response.get_ref().next_page_token.clone(),
            reverse: None,
        });
        let paginated_response = service.get_on_chain_signers_by_fid(request).await.unwrap();
        let events = paginated_response.get_ref().events.clone();
        // only 2 keys total, non-signer key is not returned, removed key is not returned
        if events.len() != 1 {
            // Re-query without pagination to distinguish "store missing the second add" from
            // "pagination dropped it" — this test has flaked here historically.
            let all_response = service
                .get_on_chain_signers_by_fid(Request::new(FidRequest {
                    fid,
                    page_size: None,
                    page_token: None,
                    reverse: None,
                }))
                .await
                .unwrap();
            let all_events = &all_response.get_ref().events;
            panic!(
                "expected 1 paginated event for fid={fid}, got {}. \
                 Paginated types={:?}, block_numbers={:?}. \
                 Full set has {} events: types={:?}, block_numbers={:?}",
                events.len(),
                events.iter().map(|e| e.r#type()).collect::<Vec<_>>(),
                events.iter().map(|e| e.block_number).collect::<Vec<_>>(),
                all_events.len(),
                all_events.iter().map(|e| e.r#type()).collect::<Vec<_>>(),
                all_events
                    .iter()
                    .map(|e| e.block_number)
                    .collect::<Vec<_>>(),
            );
        }
        assert!(events
            .iter()
            .all(|event| event.r#type() == OnChainEventType::EventTypeSigner));
    }

    // NEYN-10578 — unified GetSigner / GetSignersByFid surface.
    //
    // The gasless-key paths are exercised by writing records directly to the shard's
    // DB via `put_gasless_key_record` + `put_gasless_key_owner`, mirroring the seam
    // used by `key_add_store_tests`. This bypasses `merge_key_add` (which requires
    // a fully-signed EIP-712 KEY_ADD); the merge path itself is covered separately
    // by `gasless_key_merge_tests`. What we want here is to assert the RPC surface
    // joins the two stores correctly and shapes the response per the proto.
    #[tokio::test]
    async fn test_get_signer_returns_onchain_record() {
        let (
            stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let signer_key = test_helper::default_signer();
        let signer_pubkey = signer_key.verifying_key().as_bytes().to_vec();
        register_user(
            fid,
            signer_key,
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        let _ = stores;

        let request = Request::new(proto::SignerRequest {
            fid,
            signer: signer_pubkey.clone(),
        });
        let response = service.get_signer(request).await.unwrap();
        let signer = response.into_inner().signer.expect("signer present");
        assert_eq!(signer.source, proto::SignerSource::Onchain as i32);
        assert_eq!(signer.key, signer_pubkey);
        assert_eq!(signer.fid, fid);
        assert!(signer.added_at.is_some(), "added_at should be populated");
        assert!(signer.last_used_at.is_none());
        assert!(signer.scopes.is_empty());
        assert!(signer.onchain_event.is_some());
    }

    #[tokio::test]
    async fn test_get_signer_returns_gasless_record() {
        use crate::proto::{message_data::Body, KeyAddBody, MessageData, MessageType};
        use crate::storage::store::account::{
            put_gasless_key_owner, put_gasless_key_record, GaslessKeyRecord,
        };

        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let envelope = generate_signer();
        let pubkey: Vec<u8> = envelope.verifying_key().as_bytes().to_vec();

        let key_add = KeyAddBody {
            key: pubkey.clone(),
            key_type: 1,
            custody_signature: vec![0u8; 65],
            deadline: 1_700_000_000,
            nonce: 4,
            metadata: vec![],
            metadata_type: 1,
            registration_tx_hash: vec![],
            scopes: vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32],
            ttl: 86_400,
        };
        let message = proto::Message {
            data: Some(MessageData {
                r#type: MessageType::KeyAdd as i32,
                fid,
                timestamp: 100_000,
                network: proto::FarcasterNetwork::Devnet as i32,
                body: Some(Body::KeyAddBody(key_add)),
            }),
            hash: vec![0xAB; 20],
            hash_scheme: 1,
            signature: vec![0u8; 64],
            signature_scheme: 2,
            signer: pubkey.clone(),
            data_bytes: None,
        };
        let record = GaslessKeyRecord {
            message: Some(message),
            request_fid: 9152,
        };

        let shard_stores = stores.get(&1).expect("shard 1 stores");
        let mut txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(&shard_stores.db, &mut txn, fid, &pubkey, &record).unwrap();
        put_gasless_key_owner(&shard_stores.db, &mut txn, &pubkey, fid).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signer(Request::new(proto::SignerRequest {
                fid,
                signer: pubkey.clone(),
            }))
            .await
            .unwrap();
        let signer = response.into_inner().signer.expect("signer present");
        assert_eq!(signer.source, proto::SignerSource::Offchain as i32);
        assert_eq!(signer.key, pubkey);
        assert_eq!(signer.ttl, Some(86_400));
        assert_eq!(signer.nonce, Some(4));
        assert_eq!(signer.request_fid, Some(9152));
        // `added_at` is reported as Unix epoch seconds. The KEY_ADD message was
        // stamped at Farcaster-time second 100_000, so the unified Signer
        // surfaces FARCASTER_EPOCH/1000 + 100_000.
        assert_eq!(
            signer.added_at,
            Some(100_000 + crate::core::types::FARCASTER_EPOCH / 1000)
        );
        assert_eq!(
            signer.scopes,
            vec![MessageType::CastAdd as i32, MessageType::ReactionAdd as i32,]
        );
        // No CAST has been merged through this key, so last_used_at and the
        // computed expires_at should both be absent.
        assert!(signer.last_used_at.is_none());
        assert!(signer.expires_at.is_none());
        assert!(signer.onchain_event.is_none());
    }

    #[tokio::test]
    async fn test_get_signers_by_fid_unions_onchain_and_gasless() {
        use crate::proto::{message_data::Body, KeyAddBody, MessageData, MessageType};
        use crate::storage::store::account::{
            increment_gasless_key_count, put_gasless_key_owner, put_gasless_key_record,
            GaslessKeyRecord,
        };

        let (
            stores,
            _senders,
            [mut engine1, _engine2],
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;

        let onchain_key = test_helper::default_signer();
        let onchain_pubkey = onchain_key.verifying_key().as_bytes().to_vec();
        register_user(
            fid,
            onchain_key,
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;

        let gasless_envelope = generate_signer();
        let gasless_pubkey: Vec<u8> = gasless_envelope.verifying_key().as_bytes().to_vec();
        let key_add = KeyAddBody {
            key: gasless_pubkey.clone(),
            key_type: 1,
            custody_signature: vec![0u8; 65],
            deadline: 1_700_000_000,
            nonce: 1,
            metadata: vec![],
            metadata_type: 1,
            registration_tx_hash: vec![],
            scopes: vec![MessageType::CastAdd as i32],
            ttl: 3_600,
        };
        let record = GaslessKeyRecord {
            message: Some(proto::Message {
                data: Some(MessageData {
                    r#type: MessageType::KeyAdd as i32,
                    fid,
                    timestamp: 50_000,
                    network: proto::FarcasterNetwork::Devnet as i32,
                    body: Some(Body::KeyAddBody(key_add)),
                }),
                hash: vec![0xCD; 20],
                hash_scheme: 1,
                signature: vec![0u8; 64],
                signature_scheme: 2,
                signer: gasless_pubkey.clone(),
                data_bytes: None,
            }),
            request_fid: 7777,
        };
        let shard_stores = stores.get(&1).expect("shard 1 stores");
        let mut txn = RocksDbTransactionBatch::new();
        put_gasless_key_record(&shard_stores.db, &mut txn, fid, &gasless_pubkey, &record).unwrap();
        put_gasless_key_owner(&shard_stores.db, &mut txn, &gasless_pubkey, fid).unwrap();
        // Bump the per-FID gasless counter to mirror what `merge_key_add` would do —
        // the RPC reads it directly and exposes it as `gasless_signer_count`.
        increment_gasless_key_count(&shard_stores.db, &mut txn, fid).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        let signers = &body.signers;
        assert_eq!(body.gasless_signer_count, 1);
        assert_eq!(
            body.gasless_signer_limit,
            crate::core::validations::key::MAX_GASLESS_KEYS_PER_FID
        );

        // Expect at least the on-chain signer + the gasless key. Other on-chain
        // events written by `register_user` (e.g. storage rent) are filtered out
        // upstream by `get_signers`, so the on-chain side carries exactly one
        // entry — the registered signer.
        assert!(signers
            .iter()
            .any(|s| s.source == proto::SignerSource::Onchain as i32 && s.key == onchain_pubkey));
        let gasless = signers
            .iter()
            .find(|s| s.source == proto::SignerSource::Offchain as i32 && s.key == gasless_pubkey)
            .expect("gasless signer in unified response");
        assert_eq!(gasless.ttl, Some(3_600));
        assert_eq!(gasless.scopes, vec![MessageType::CastAdd as i32]);
        assert_eq!(gasless.request_fid, Some(7_777));

        // Counter store wasn't touched by the direct put above, so both
        // namespaces are absent and the API surfaces 0 / empty.
        assert_eq!(body.current_user_nonce, 0);
        assert!(body.requester_fid_nonces.is_empty());
    }

    #[tokio::test]
    async fn test_get_signers_by_fid_surfaces_nonces() {
        use crate::storage::store::account::{check_and_set_app_nonce, check_and_set_user_nonce};

        let (
            stores,
            _senders,
            _engines,
            _block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        let fid = SHARD1_FID;
        let requester_fid: u64 = 7_777;
        let shard_stores = stores.get(&1).expect("shard 1 stores");

        // No counter activity yet — `current_user_nonce` defaults to 0 and
        // `requester_fid_nonces` stays empty unless the request opts in.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 0);
        assert!(body.requester_fid_nonces.is_empty());

        // Opting in with a requester_fid still returns 0 when the app-nonce
        // counter has no entry for that requester. The response carries one
        // entry naming the requested fid.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 0);
        assert_eq!(body.requester_fid_nonces.len(), 1);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&0));

        // Advance both counters to simulate prior KEY_ADD / self-revoke
        // activity. Reading them back through the RPC should reflect the
        // exact stored values, including across a subsequent revocation
        // (the counter persists even when the per-key record is gone).
        let mut txn = RocksDbTransactionBatch::new();
        check_and_set_user_nonce(&shard_stores.db, &mut txn, fid, 3).unwrap();
        check_and_set_app_nonce(&shard_stores.db, &mut txn, requester_fid, 9).unwrap();
        shard_stores.db.commit(txn).unwrap();

        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 3);
        assert_eq!(body.requester_fid_nonces.len(), 1);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&9));

        // Omitting `requester_fids` on the next call still surfaces the
        // user-nonce, but suppresses the per-requester list.
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.current_user_nonce, 3);
        assert!(body.requester_fid_nonces.is_empty());

        // Batched lookup: request multiple requester FIDs in one call. The
        // response carries one entry per supplied FID in the same order, with
        // unknown counters reported as 0.
        let other_requester: u64 = 8_888;
        let response = service
            .get_signers_by_fid(Request::new(SignersByFidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
                requester_fids: vec![requester_fid, other_requester],
            }))
            .await
            .unwrap();
        let body = response.into_inner();
        assert_eq!(body.requester_fid_nonces.len(), 2);
        assert_eq!(body.requester_fid_nonces.get(&requester_fid), Some(&9));
        assert_eq!(body.requester_fid_nonces.get(&other_requester), Some(&0));
    }

    #[tokio::test]
    async fn test_submit_storage_lending_message() {
        let (
            _stores,
            _senders,
            _engines,
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;

        block_engine_test_helpers::register_user(
            SHARD1_FID,
            block_engine_test_helpers::default_signer(),
            block_engine_test_helpers::default_custody_address(),
            2,
            &mut block_engine,
        );

        // Create a storage lend message from SHARD1_FID to SHARD2_FID
        let storage_lend_message = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD2_FID,
            1, // units
            proto::StorageUnitType::UnitType2025,
            None,
            None,
        );

        let response = submit_message(&service, storage_lend_message.clone()).await;
        assert_eq!(response.unwrap().into_inner(), storage_lend_message);

        // Confirm that the mempool processes the message correctly and that the message can be pulled out
        let messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();

        let state_change = block_engine.propose_state_change(
            messages,
            block_engine.get_confirmed_height().increment(),
            None,
        );

        assert_eq!(
            state_change.transactions[0].user_messages[0],
            storage_lend_message
        )
    }

    #[tokio::test]
    async fn test_verification_activation_pipeline_preserves_primary_address_ownership() {
        let (
            _stores,
            _senders,
            [_engine1, mut data_engine],
            mut block_engine,
            service,
            shard_decision_tx,
            block_decision_tx,
        ) = make_server(None, None).await;
        let fid = 2u64; // EvenOddRouterForTest routes this FID's data to shard 2.
        let signer = test_helper::default_signer();
        let custody = test_helper::default_custody_address();
        block_engine_test_helpers::register_user(
            fid,
            signer.clone(),
            custody.clone(),
            1,
            &mut block_engine,
        );
        test_helper::register_user(fid, signer, custody, &mut data_engine).await;

        let timestamp = messages_factory::farcaster_time();
        let address = hex::decode("91031dcfdea024b4d51e775486111d2b2a715871").unwrap();
        let verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            address.clone(),
            hex::decode("b72c63d61f075b36fb66a9a867b50836cef19d653a3c09005628738677bcb25f25b6b6e6d2e1d69cd725327b3c020deef9e2575a22dc8ed08f88bc75718ce1cb1c").unwrap(),
            hex::decode("d74860c4bbf574d5ad60f03a478a30f990e05ac723e138a5c860cdb3095f4296").unwrap(),
            Some(timestamp),
            None,
        );

        // Submit through the real RPC routing/simulation path, then prove the message was queued
        // on shard 0 (where floor + quota admission run) rather than on the FID shard.
        assert_eq!(
            submit_message(&service, verification_add.clone())
                .await
                .unwrap()
                .into_inner(),
            verification_add
        );
        let block_messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        assert!(matches!(
            block_messages.as_slice(),
            [crate::storage::store::mempool_poller::MempoolMessage::UserMessage(message)]
                if message == &verification_add
        ));

        let add_height = block_engine.get_confirmed_height().increment();
        let add_state = block_engine.propose_state_change(block_messages, add_height, None);
        let add_block = block_engine_test_helpers::validate_and_commit_state_change(
            &mut block_engine,
            &add_state,
        );
        let _ = block_decision_tx.send(add_block.clone());
        let verification_events = add_block
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.data.as_ref().and_then(|data| data.body.as_ref()),
                    Some(proto::block_event_data::Body::MergeMessageEventBody(body))
                        if body.message.as_ref() == Some(&verification_add)
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(verification_events.len(), 1);

        // Fan the shard-0 block events into the FID shard through its public replay path.
        test_helper::commit_block_events(&mut data_engine, add_block.events.iter().collect()).await;
        assert_eq!(
            data_engine.get_verifications_by_fid(fid).unwrap().messages,
            vec![verification_add.clone()]
        );

        // The primary-address write is admitted only because replay populated the FID shard's
        // local verification store.
        let checksummed = alloy_primitives::Address::from_slice(&address).to_checksum(None);
        let primary_address = messages_factory::user_data::create_user_data_add(
            fid,
            UserDataType::UserDataPrimaryAddressEthereum,
            &checksummed,
            Some(timestamp + 1),
            None,
        );
        submit_message(&service, primary_address.clone())
            .await
            .unwrap();
        let data_messages = data_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        let primary_state =
            data_engine.propose_state_change(data_engine.shard_id(), data_messages, None);
        let primary_chunk =
            test_helper::validate_and_commit_state_change(&mut data_engine, &primary_state).await;
        let _ = shard_decision_tx.send(primary_chunk);
        assert!(test_helper::message_exists_in_trie(
            &mut data_engine,
            &primary_address,
        ));

        let verification_remove = messages_factory::verifications::create_verification_remove(
            fid,
            address,
            Some(timestamp + 2),
            None,
        );
        submit_message(&service, verification_remove.clone())
            .await
            .unwrap();
        let remove_messages = block_engine
            .mempool_poller
            .pull_messages(Duration::from_millis(100))
            .await
            .unwrap();
        let remove_height = block_engine.get_confirmed_height().increment();
        let remove_state = block_engine.propose_state_change(remove_messages, remove_height, None);
        let remove_block = block_engine_test_helpers::validate_and_commit_state_change(
            &mut block_engine,
            &remove_state,
        );
        let _ = block_decision_tx.send(remove_block.clone());
        test_helper::commit_block_events(&mut data_engine, remove_block.events.iter().collect())
            .await;

        assert!(data_engine
            .get_user_data_by_fid_and_type(fid, UserDataType::UserDataPrimaryAddressEthereum,)
            .is_err());
        assert!(!test_helper::message_exists_in_trie(
            &mut data_engine,
            &primary_address,
        ));
    }

    #[tokio::test]
    async fn test_get_all_lend_storage_messages_by_fid() {
        let (
            _stores,
            _senders,
            [_engine1, _engine2],
            mut block_engine,
            service,
            _shard_decision_tx,
            _block_decision_tx,
        ) = make_server(None, None).await;
        block_engine_test_helpers::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            5,
            &mut block_engine,
        );

        // Create storage lend messages
        let lend1 = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD1_FID + 1,
            1,
            proto::StorageUnitType::UnitType2025,
            None,
            None,
        );
        let lend2 = messages_factory::storage_lend::create_storage_lend(
            SHARD1_FID,
            SHARD2_FID + 2,
            2,
            proto::StorageUnitType::UnitType2025,
            Some(lend1.data.as_ref().unwrap().timestamp + 10),
            None,
        );

        block_engine_test_helpers::commit_message(&mut block_engine, &lend1, Validity::Valid);
        block_engine_test_helpers::commit_message(&mut block_engine, &lend2, Validity::Valid);

        // Test pagination
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend1]);

        // Test getting all messages
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: None,
            page_token: None,
            reverse: None,
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend1, &lend2]);

        // Test reverse order
        let request = proto::FidTimestampRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: Some(true),
            start_timestamp: None,
            stop_timestamp: None,
        };
        let response = service
            .get_all_lend_storage_messages_by_fid(Request::new(request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&lend2]);
    }

    // ---- channel follows -------------------------------------------------

    const FOLLOW_CHANNEL: [u8; 32] = [0xf0; 32];

    /// Writes a follow directly into a shard's reaction store, bypassing the
    /// message path. These tests are about the read fan-out, not about merging;
    /// `channel_follow_index_is_written_through_the_production_wiring` covers the
    /// merge path end to end.
    fn seed_follow(stores: &HashMap<u32, Stores>, shard_id: u32, fid: u64, followed_at: u32) {
        let store = &stores.get(&shard_id).unwrap().reaction_store;
        let mut txn = RocksDbTransactionBatch::new();
        let value = followed_at.to_be_bytes().to_vec();
        txn.put(
            ReactionStoreDef::make_follow_by_fid_key(fid, &FOLLOW_CHANNEL),
            value.clone(),
        );
        txn.put(
            ReactionStoreDef::make_follow_by_channel_key(&FOLLOW_CHANNEL, fid),
            value,
        );
        store.db().commit(txn).unwrap();
    }

    #[tokio::test]
    async fn test_get_channel_followers_spans_every_shard() {
        let (stores, _senders, _engines, _block_engine, service, _sc, _bc) =
            make_server(None, None).await;
        seed_follow(&stores, 1, SHARD1_FID, 1000);
        seed_follow(&stores, 2, SHARD2_FID, 2000);

        let response = service
            .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let mut fids: Vec<u64> = response.followers.iter().map(|f| f.fid).collect();
        fids.sort();
        let mut expected = vec![SHARD1_FID, SHARD2_FID];
        expected.sort();
        assert_eq!(fids, expected);
        assert_eq!(response.next_page_token, None);

        let count = service
            .get_channel_follower_count(Request::new(proto::ChannelFollowerCountRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(count.count, 2, "count must sum both shards");
    }

    #[tokio::test]
    async fn test_channel_follower_pagination_terminates_and_is_complete() {
        // ASYMMETRIC ON PURPOSE. With one follower per shard the two shards finish
        // in lockstep and `ShardScan::Exhausted` is never constructed, so the
        // branch that skips a finished shard goes untested — and deleting it
        // (restarting that shard on every page instead) would still pass. Shard 1
        // gets one follower and shard 2 gets three, so shard 1 is exhausted while
        // shard 2 is still paging.
        let (stores, _senders, _engines, _block_engine, service, _sc, _bc) =
            make_server(None, None).await;
        seed_follow(&stores, 1, SHARD1_FID, 1000);
        for i in 0..3 {
            seed_follow(&stores, 2, SHARD2_FID + (i * 2), 2000 + i as u32);
        }

        for reverse in [false, true] {
            let mut seen = Vec::new();
            let mut page_token = None;
            let mut pages = 0;
            let mut saw_exhausted_cursor = false;
            loop {
                let response = service
                    .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                        channel_id: FOLLOW_CHANNEL.to_vec(),
                        page_size: Some(1),
                        page_token,
                        reverse: Some(reverse),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                seen.extend(response.followers.iter().map(|f| f.fid));
                pages += 1;
                assert!(pages < 20, "pagination did not terminate");
                match response.next_page_token {
                    None => break,
                    Some(token) => {
                        // Pins that the fixture really does drive a shard to
                        // Exhausted while another is still live. If this stops
                        // holding, the test has quietly stopped covering the branch.
                        if String::from_utf8_lossy(&token).contains("Exhausted") {
                            saw_exhausted_cursor = true;
                        }
                        page_token = Some(token)
                    }
                }
            }

            // NOT deduped: a shard that restarts instead of being skipped would
            // re-emit its rows, and dedup would hide exactly that.
            assert_eq!(
                seen.len(),
                4,
                "reverse={reverse}: duplicate or missing rows"
            );
            seen.sort();
            let mut expected = vec![SHARD1_FID, SHARD2_FID, SHARD2_FID + 2, SHARD2_FID + 4];
            expected.sort();
            assert_eq!(seen, expected, "reverse={reverse}");
            assert!(
                saw_exhausted_cursor,
                "reverse={reverse}: fixture never exercised ShardScan::Exhausted"
            );
        }
    }

    #[tokio::test]
    async fn test_channel_follow_reads_reject_malformed_requests() {
        let (_stores, _senders, _engines, _block_engine, service, _sc, _bc) =
            make_server(None, None).await;

        let err = service
            .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                channel_id: vec![0u8; 31],
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let err = service
            .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
                page_size: Some(0),
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // Tokens that must be refused rather than silently mis-paged. The middle
        // two are the ones that used to slip through: a token stripped of its
        // null-valued fields decoded as "every shard exhausted" and returned an
        // empty follower list AS A SUCCESS, and a duplicated shard id passed the
        // length check whenever the token still covered the whole set.
        let bad_tokens: Vec<Vec<u8>> = vec![
            br#"[{"shard_id":7,"scan":"Exhausted"}]"#.to_vec(),
            br#"[{"shard_id":1},{"shard_id":2}]"#.to_vec(),
            br#"[{"shard_id":1,"scan":"Exhausted"},{"shard_id":1,"scan":"Exhausted"},{"shard_id":2,"scan":"Exhausted"}]"#.to_vec(),
            br#"[{"shard_id":1,"scan":"Exhausted","extra":1},{"shard_id":2,"scan":"Exhausted"}]"#.to_vec(),
        ];
        for token in bad_tokens {
            let err = service
                .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                    channel_id: FOLLOW_CHANNEL.to_vec(),
                    page_size: None,
                    page_token: Some(token.clone()),
                    reverse: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "token should be rejected: {}",
                String::from_utf8_lossy(&token)
            );
        }
    }

    #[tokio::test]
    async fn test_channel_follow_reads_do_not_require_a_registered_channel() {
        // FOLLOW_CHANNEL is never registered on shard 0. Registration lives there
        // and a follow row can exist for an unminted tokenId, so these reads must
        // answer rather than 404 — otherwise the count would disagree with
        // IsFollowingChannel for the same channel.
        let (stores, _senders, _engines, _block_engine, service, _sc, _bc) =
            make_server(None, None).await;
        seed_follow(&stores, 1, SHARD1_FID, 1000);

        let count = service
            .get_channel_follower_count(Request::new(proto::ChannelFollowerCountRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(count.count, 1);
    }

    #[tokio::test]
    async fn test_channel_follows_and_is_following_use_the_fid_home_shard() {
        let (stores, _senders, _engines, _block_engine, service, _sc, _bc) =
            make_server(None, None).await;
        seed_follow(&stores, 1, SHARD1_FID, 1000);

        let follows = service
            .get_channel_follows(Request::new(proto::ChannelFollowsRequest {
                fid: SHARD1_FID,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(follows.follows.len(), 1);
        assert_eq!(follows.follows[0].channel_id, FOLLOW_CHANNEL.to_vec());
        assert_eq!(follows.follows[0].followed_at, 1000);

        let following = service
            .is_following_channel(Request::new(proto::IsFollowingChannelRequest {
                fid: SHARD1_FID,
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(following.following);
        assert_eq!(following.followed_at, Some(1000));

        // A fid on the other shard has no follow, and must not pick up shard 1's.
        let not_following = service
            .is_following_channel(Request::new(proto::IsFollowingChannelRequest {
                fid: SHARD2_FID,
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!not_following.following);
        assert_eq!(not_following.followed_at, None);
    }

    #[tokio::test]
    async fn test_channel_follow_fan_out_refuses_when_a_shard_is_missing() {
        // A read node may host a subset of shards — `num_shards` is an independent
        // constructor argument, not `shard_stores.len()`. Answering a fan-out read
        // from a subset would report a channel as having fewer followers than it
        // has, indistinguishable from the truth, so the fan-out reads must refuse.
        // The single-shard reads have no such problem and must keep working.
        let (stores, senders, _engines, block_engine, _service, _sc, _bc) =
            make_server(None, None).await;
        seed_follow(&stores, 1, SHARD1_FID, 1000);

        let mut partial_stores = stores.clone();
        partial_stores.remove(&2);
        assert_eq!(partial_stores.len(), 1);

        let (mempool_tx, _mempool_rx) = mpsc::channel(1000);
        let (gossip_tx, _gossip_rx) = mpsc::channel(1000);
        let mut chain_clients = ChainClients {
            chain_api_map: HashMap::new(),
        };
        chain_clients.chain_api_map.insert(
            Chain::EthMainnet,
            Box::new(MockL1Client {}) as Box<dyn ChainAPI>,
        );
        let partial = MyHubService::new(
            format!("{}:{}", USER_NAME, PASSWORD),
            "".to_string(),
            vec![],
            block_engine.stores(),
            partial_stores,
            senders,
            test_helper::statsd_client(),
            // Two shards exist on the network; this node holds one.
            2,
            proto::FarcasterNetwork::Devnet,
            Box::new(routing::EvenOddRouterForTest {}),
            mempool_tx,
            gossip_tx,
            chain_clients,
            "0.1.2".to_string(),
            "asddef".to_string(),
            None,
            Default::default(),
        );

        let followers_err = partial
            .get_channel_followers(Request::new(proto::ChannelFollowersRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(followers_err.code(), tonic::Code::FailedPrecondition);

        let count_err = partial
            .get_channel_follower_count(Request::new(proto::ChannelFollowerCountRequest {
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap_err();
        assert_eq!(count_err.code(), tonic::Code::FailedPrecondition);

        // The fid-keyed reads are single-shard and must still answer for a fid
        // this node does host.
        let following = partial
            .is_following_channel(Request::new(proto::IsFollowingChannelRequest {
                fid: SHARD1_FID,
                channel_id: FOLLOW_CHANNEL.to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(following.following);

        let follows = partial
            .get_channel_follows(Request::new(proto::ChannelFollowsRequest {
                fid: SHARD1_FID,
                page_size: None,
                page_token: None,
                reverse: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(follows.follows.len(), 1);
    }
}
