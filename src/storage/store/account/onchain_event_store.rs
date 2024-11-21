use std::sync::Arc;

use prost::Message;

use super::{make_fid_key, Store, StoreEventHandler};
use crate::core::error::HubError;
use crate::proto::hub_event::hub_event::Body;
use crate::proto::hub_event::{HubEvent, HubEventType, MergeOnChainEventBody};
use crate::proto::onchain_event::OnChainEvent;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch, RocksdbError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OnchainEventStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error(transparent)]
    HubError(#[from] HubError),
}

pub fn merge_onchain_event(
    db: &RocksDB,
    onchain_event: OnChainEvent,
) -> Result<(), OnchainEventStorageError> {
    let primary_key = make_fid_key(onchain_event.fid as u32);
    db.put(&primary_key, &onchain_event.encode_to_vec())?;
    Ok(())
}

pub struct OnchainEventStore {
    db: Arc<RocksDB>,
    store_event_handler: Arc<StoreEventHandler>,
}

impl OnchainEventStore {
    pub fn new(db: Arc<RocksDB>, store_event_handler: Arc<StoreEventHandler>) -> OnchainEventStore {
        OnchainEventStore {
            db,
            store_event_handler,
        }
    }

    pub fn merge_onchain_event(
        &self,
        onchain_event: OnChainEvent,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, OnchainEventStorageError> {
        merge_onchain_event(&self.db, onchain_event.clone())?;
        let hub_event = &mut HubEvent {
            r#type: HubEventType::MergeOnChainEvent as i32,
            body: Some(Body::MergeOnChainEventBody(MergeOnChainEventBody {
                on_chain_event: Some(onchain_event.clone()),
            })),
            id: 0,
        };
        let id = self
            .store_event_handler
            .commit_transaction(txn, hub_event)?;
        hub_event.id = id;
        Ok(hub_event.clone())
    }
}
