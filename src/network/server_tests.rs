#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::network::server::MySnapchainService;
    use crate::proto::hub_event::{HubEvent, HubEventType};
    use crate::proto::rpc::snapchain_service_server::SnapchainService;
    use crate::proto::rpc::SubscribeRequest;
    use crate::storage::db::{self, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::engine::Senders;
    use crate::storage::store::stores::Stores;
    use crate::storage::store::BlockStore;
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use futures::StreamExt;
    use tempfile;
    use tokio::sync::{broadcast, mpsc};
    use tonic::Request;

    async fn subscribe_and_listen(
        service: &MySnapchainService,
        shard_id: u32,
        num_events_expected: u64,
    ) {
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

    fn make_server() -> (
        HashMap<u32, Stores>,
        HashMap<u32, Senders>,
        MySnapchainService,
    ) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );

        let db1 = make_db("b1.db");
        let db2 = make_db("b2.db");

        let (msgs_tx, _msgs_rx) = mpsc::channel(100);

        let shard1_stores = Stores::new(db1);
        let shard1_senders = Senders::new(msgs_tx.clone());

        let shard2_stores = Stores::new(db2);
        let shard2_senders = Senders::new(msgs_tx.clone());
        let stores = HashMap::from([(1, shard1_stores), (2, shard2_stores)]);
        let senders = HashMap::from([(1, shard1_senders), (2, shard2_senders)]);

        (
            stores.clone(),
            senders.clone(),
            MySnapchainService::new(
                BlockStore::new(make_db("blocks.db")),
                stores,
                senders,
                statsd_client,
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
}
