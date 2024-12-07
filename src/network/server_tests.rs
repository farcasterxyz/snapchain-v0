#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::network::server::MyHubService;
    use crate::proto::hub_service_server::HubService;
    use crate::proto::SubscribeRequest;
    use crate::proto::{HubEvent, HubEventType};
    use crate::storage::db::{self, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::engine::Senders;
    use crate::storage::store::stores::{StoreLimits, Stores};
    use crate::storage::store::BlockStore;
    use crate::storage::trie::merkle_trie;
    use crate::utils::factory::messages_factory;
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::StreamExt;
    use tempfile;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

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

    fn make_db(filename: &str) -> Arc<RocksDB> {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join(filename);

        let db = Arc::new(db::RocksDB::new(db_path.to_str().unwrap()));
        db.open().unwrap();

        db
    }

    fn make_server() -> (HashMap<u32, Stores>, HashMap<u32, Senders>, MyHubService) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let db1 = make_db("b1.db");
        let db2 = make_db("b2.db");

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

        (
            stores.clone(),
            senders.clone(),
            MyHubService::new(
                BlockStore::new(make_db("blocks.db")),
                stores,
                senders,
                statsd_client,
                num_shards,
            ),
        )
    }

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let (stores, senders, service) = make_server();

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
        let (_stores, _senders, service) = make_server();

        // Message with no fid registration
        let invalid_message = messages_factory::casts::create_cast_add(123, "test", None, None);

        let response = service
            .submit_message(Request::new(invalid_message))
            .await
            .unwrap_err();

        assert_eq!(response.code(), tonic::Code::InvalidArgument);
        assert_eq!(response.message(), "Invalid message: missing fid");
    }
}
