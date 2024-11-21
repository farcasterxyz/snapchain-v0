use std::sync::Arc;

use prost::Message;

use super::make_fid_key;
use crate::proto::snapchain::OnChainEvent;
use crate::storage::db::{RocksDB, RocksdbError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OnchainEventStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),
}

pub fn put_onchain_event(
    db: &RocksDB,
    onchain_event: OnChainEvent,
) -> Result<(), OnchainEventStorageError> {
    let primary_key = make_fid_key(onchain_event.fid as u32);
    db.put(&primary_key, &onchain_event.encode_to_vec())?;
    Ok(())
}

pub struct OnchainEventStore {
    db: Arc<RocksDB>,
}

impl OnchainEventStore {
    pub fn new(db: Arc<RocksDB>) -> OnchainEventStore {
        OnchainEventStore { db }
    }

    pub fn put_onchain_event(
        &mut self,
        onchain_event: OnChainEvent,
    ) -> Result<(), OnchainEventStorageError> {
        put_onchain_event(&self.db, onchain_event)
    }
}
