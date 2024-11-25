#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::network::server::MySnapchainService;
    use crate::proto::hub_event::{HubEvent, HubEventType};
    use crate::proto::rpc::snapchain_service_server::SnapchainService;
    use crate::proto::rpc::SubscribeRequest;
    use crate::storage::db;
    use crate::storage::store::BlockStore;
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

    #[tokio::test]
    async fn test_subscribe_rpc() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("b.db");

        let db = Arc::new(db::RocksDB::new(db_path.to_str().unwrap()));
        db.open().unwrap();
        let (events_tx1, _events_rx) = broadcast::channel(100);
        let (events_tx2, _events_rx) = broadcast::channel(100);
        let (msgs_tx, _msgs_rx) = mpsc::channel(100);
        let mut shard_events = HashMap::new();
        shard_events.insert(1, events_tx1.clone());
        shard_events.insert(2, events_tx2.clone());

        let service =
            MySnapchainService::new(BlockStore::new(db), HashMap::new(), shard_events, msgs_tx);

        let num_shard1_events = 5;
        let num_shard2_events = 10;
        subscribe_and_listen(&service, 1, num_shard1_events).await;
        subscribe_and_listen(&service, 2, num_shard2_events).await;

        send_events(events_tx1.clone(), num_shard1_events).await;
        send_events(events_tx2.clone(), num_shard2_events).await;
    }
}
