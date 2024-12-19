#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use foundry_common::ens::EnsError;
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::connectors::onchain_events::L1Client;
    use crate::mempool::routing;
    use crate::mempool::routing::MessageRouter;
    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::{self, HubEvent, HubEventType, UserNameProof, UserNameType};
    use crate::proto::{FidRequest, SubscribeRequest};
    use crate::storage::db::{self, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::engine::{Senders, ShardEngine};
    use crate::storage::store::stores::Stores;
    use crate::storage::store::{test_helper, BlockStore};
    use crate::storage::trie::merkle_trie;
    use crate::utils::factory::{events_factory, messages_factory};
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::future;
    use futures::StreamExt;
    use tempfile;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    const SHARD1_FID: u64 = 121;
    const SHARD2_FID: u64 = 122;

    impl FidRequest {
        fn for_fid(fid: u64) -> Request<Self> {
            Request::new(FidRequest {
                fid,
                page_size: None,
                page_token: None,
                reverse: None,
            })
        }
    }

    struct MockL1Client {}

    #[async_trait]
    impl L1Client for MockL1Client {
        async fn resolve_ens_name(
            &self,
            _name: String,
        ) -> Result<alloy_primitives::Address, EnsError> {
            let addr =
                alloy_primitives::Address::from_slice(&test_helper::default_custody_address());
            future::ready(Ok(addr)).await
        }
    }

    async fn subscribe_and_listen(service: &MyHubService, shard_id: u32, num_events_expected: u64) {
        let mut listener = service
            .subscribe(Request::new(SubscribeRequest {
                event_types: vec![HubEventType::MergeMessage as i32],
                from_id: None,
                shard_index: Some(shard_id),
                fid_partitions: None,
                fid_partition_index: None,
            }))
            .await
            .unwrap();

        let mut num_events_seen = 0;

        tokio::spawn(async move {
            loop {
                let _event = listener.get_mut().next().await;
                num_events_seen += 1;
                if num_events_seen == num_events_expected {
                    break;
                }
            }
        });
    }

    async fn send_events(events_tx: broadcast::Sender<HubEvent>, num_events: u64) {
        for i in 0..num_events {
            events_tx
                .send(HubEvent {
                    r#type: HubEventType::MergeMessage as i32,
                    id: i,
                    body: None,
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
                },
            )
            .unwrap();
        }
        db.commit(txn).unwrap();
    }

    fn make_db(dir: &tempfile::TempDir, filename: &str) -> Arc<RocksDB> {
        let db_path = dir.path().join(filename);

        let db = Arc::new(db::RocksDB::new(db_path.to_str().unwrap()));
        db.open().unwrap();

        db
    }

    fn make_server() -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        [ShardEngine; 2],
        MyHubService,
    ) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let limits = test_helper::limits::test_store_limits();
        let (engine1, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            db_name: Some("db1.db".to_string()),
        });
        let (engine2, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: Some(limits.clone()),
            db_name: Some("db2.db".to_string()),
        });
        let db1 = engine1.db.clone();
        let db2 = engine2.db.clone();

        let (msgs_tx, _msgs_rx) = mpsc::channel(100);

        let shard1_stores = Stores::new(
            db1,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            limits.clone(),
        );
        let shard1_senders = Senders::new(msgs_tx.clone());

        let shard2_stores = Stores::new(
            db2,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            limits.clone(),
        );
        let shard2_senders = Senders::new(msgs_tx.clone());
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);
        let num_shards = senders.len() as u32;

        let blocks_dir = tempfile::TempDir::new().unwrap();
        let blocks_store = BlockStore::new(make_db(&blocks_dir, "blocks.db"));

        let message_router = Box::new(routing::EvenOddRouterForTest {});
        assert_eq!(message_router.route_message(SHARD1_FID, 2), 1);
        assert_eq!(message_router.route_message(SHARD2_FID, 2), 2);

        (
            stores.clone(),
            senders.clone(),
            [engine1, engine2],
            MyHubService::new(
                blocks_store,
                stores,
                senders,
                statsd_client,
                num_shards,
                message_router,
                Some(Box::new(MockL1Client {})),
            ),
        )
    }

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let (stores, senders, _, service) = make_server();

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
        subscribe_and_listen(
            &service,
            1,
            num_shard1_events + num_shard1_pre_existing_events,
        )
        .await;
        subscribe_and_listen(
            &service,
            2,
            num_shard2_events + num_shard2_pre_existing_events,
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
    }

    #[tokio::test]
    async fn test_submit_message_fails_with_error_for_invalid_messages() {
        let (_stores, _senders, _, service) = make_server();

        // Message with no fid registration
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let response = service
            .submit_message(Request::new(invalid_message))
            .await
            .unwrap_err();

        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(response.message(), "Invalid message: missing fid");
    }

    #[tokio::test]
    async fn test_good_ens_proof() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server();
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: "username.eth".to_string().encode_to_vec(),
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
    async fn test_ens_proof_with_bad_owner() {
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server();
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;

        test_helper::register_user(fid, signer.clone(), owner.clone(), &mut engine1).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: "username.eth".to_string().encode_to_vec(),
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
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server();
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
            name: "username.eth".to_string().encode_to_vec(),
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
        let (_stores, _senders, [mut engine1, mut _engine2], service) = make_server();
        let signer = test_helper::default_signer();
        let owner = test_helper::default_custody_address();
        let fid = SHARD1_FID;
        let signature = "signature".to_string();

        test_helper::register_user(
            fid,
            signer.clone(),
            "100000000000000000".to_string().encode_to_vec(),
            &mut engine1,
        )
        .await;

        let verification_add = messages_factory::verifications::create_verification_add(
            fid,
            0,
            owner.clone(),
            signature.clone(),
            "hash".to_string(),
            None,
            None,
        );

        test_helper::commit_message(&mut engine1, &verification_add).await;

        let username_proof = UserNameProof {
            timestamp: messages_factory::farcaster_time() as u64,
            name: "username.eth".to_string().encode_to_vec(),
            owner,
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
        let (_, _, [mut engine1, mut engine2], service) = make_server();
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
        let all_casts_request = proto::FidRequest {
            fid: SHARD1_FID,
            page_size: Some(1),
            page_token: None,
            reverse: None,
        };
        let response = service
            .get_casts_by_fid(Request::new(all_casts_request))
            .await;
        test_helper::assert_contains_all_messages(&response, &[&cast_add2]);

        // TODO: Fix pagination
        // let second_page_request = proto::FidRequest { fid: SHARD1_FID, page_size: Some(1), page_token: response.as_ref().unwrap().get_ref().next_page_token.clone(), reverse: None };
        // warn!("second_page_request: {:?}", second_page_request);
        // let response = service
        //     .get_casts_by_fid(Request::new(second_page_request))
        //     .await;
        // warn!("response: {:?}", response);
        // test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

        // let reverse_request = proto::FidRequest { fid: SHARD1_FID, page_size: Some(1), page_token: None, reverse: Some(true) };
        // let response = service
        //     .get_casts_by_fid(Request::new(reverse_request))
        //     .await;
        // test_helper::assert_contains_all_messages(&response, &[&cast_remove]);

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
    }

    #[tokio::test]
    async fn test_storage_limits() {
        // Works with no storage
        let (_, _, [mut engine1, _], service) = make_server();

        let response = service
            .get_current_storage_limits_by_fid(FidRequest::for_fid(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 0);
        assert_eq!(response.get_ref().limits.len(), 6);
        for limit in response.get_ref().limits.iter() {
            assert_eq!(limit.limit, 0);
            assert_eq!(limit.used, 0);
        }
        assert_eq!(response.get_ref().unit_details.len(), 2);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 0);
        assert_eq!(response.get_ref().unit_details[1].unit_size, 0);

        test_helper::register_user(
            SHARD1_FID,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine1,
        )
        .await;
        // register_user will give the user a single unit of storage, let add one more legacy unit and a 2024 unit for a total of 1 legacy and 2 2024 units
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(SHARD1_FID, Some(1), None, false),
        )
        .await;
        test_helper::commit_event(
            &mut engine1,
            &events_factory::create_rent_event(SHARD1_FID, None, Some(1), false),
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
            .get_current_storage_limits_by_fid(FidRequest::for_fid(SHARD1_FID))
            .await
            .unwrap();
        assert_eq!(response.get_ref().units, 3);
        assert_eq!(response.get_ref().unit_details.len(), 2);
        assert_eq!(response.get_ref().unit_details[0].unit_size, 1);
        assert_eq!(
            response.get_ref().unit_details[0].unit_type,
            proto::StorageUnitType::UnitTypeLegacy as i32
        );
        assert_eq!(
            response.get_ref().unit_details[1].unit_type,
            proto::StorageUnitType::UnitType2024 as i32
        );
        assert_eq!(response.get_ref().unit_details[1].unit_size, 2);

        let casts_limit = response
            .get_ref()
            .limits
            .iter()
            .filter(|limit| limit.store_type() == proto::StoreType::Casts)
            .collect::<Vec<_>>()[0];
        let configured_limits = engine1.get_stores().store_limits;
        assert_eq!(
            casts_limit.limit as u32,
            (configured_limits.limits.casts * 2) + (configured_limits.legacy_limits.casts)
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
    }
}
