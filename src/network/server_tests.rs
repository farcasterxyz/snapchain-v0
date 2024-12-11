#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::mempool::routing;
    use crate::mempool::routing::MessageRouter;
    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::SubscribeRequest;
    use crate::proto::{self, HubEvent, HubEventType};
    use crate::storage::db::{self, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::engine::{Senders, ShardEngine};
    use crate::storage::store::stores::{StoreLimits, Stores};
    use crate::storage::store::{test_helper, BlockStore};
    use crate::storage::trie::merkle_trie;
    use crate::utils::factory::messages_factory;
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::StreamExt;
    use tempfile;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    const SHARD1_FID: u64 = 121;
    const SHARD2_FID: u64 = 122;

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

        let (engine1, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: None,
            db_name: Some("db1.db".to_string()),
        });
        let (engine2, _) = test_helper::new_engine_with_options(test_helper::EngineOptions {
            limits: None,
            db_name: Some("db2.db".to_string()),
        });
        let db1 = engine1.db.clone();
        let db2 = engine2.db.clone();

        let (msgs_tx, _msgs_rx) = mpsc::channel(100);

        let shard1_stores = Stores::new(
            db1,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            StoreLimits::default(),
        );
        let shard1_senders = Senders::new(msgs_tx.clone());

        let shard2_stores = Stores::new(
            db2,
            merkle_trie::MerkleTrie::new(16).unwrap(),
            StoreLimits::default(),
        );
        let shard2_senders = Senders::new(msgs_tx.clone());
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);
        let num_shards = senders.len() as u32;

        let blocks_dir = tempfile::TempDir::new().unwrap();
        let blocks_store = BlockStore::new(make_db(&blocks_dir, "blocks.db"));

        let message_router = Box::new(routing::EvenOddRouterForTest {});
        assert_eq!(message_router.route_message(SHARD1_FID as u32, 2), 1);
        assert_eq!(message_router.route_message(SHARD2_FID as u32, 2), 2);

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
    async fn test_cast_apis() {
        let (_, _, [mut engine1, mut engine2], service) = make_server();
        let engine1 = &mut engine1;
        let engine2 = &mut engine2;
        test_helper::register_user(SHARD1_FID as u32, test_helper::default_signer(), engine1).await;
        test_helper::register_user(SHARD2_FID as u32, test_helper::default_signer(), engine2).await;
        let cast_add =
            messages_factory::casts::create_cast_add(SHARD1_FID as u32, "test", None, None);
        let cast_add2 =
            messages_factory::casts::create_cast_add(SHARD1_FID as u32, "test2", None, None);
        let cast_remove = messages_factory::casts::create_cast_remove(
            SHARD1_FID as u32,
            &cast_add.hash,
            Some(cast_add.data.as_ref().unwrap().timestamp + 1),
            None,
        );

        let another_shard_cast =
            messages_factory::casts::create_cast_add(SHARD2_FID as u32, "another fid", None, None);

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
    }
}
