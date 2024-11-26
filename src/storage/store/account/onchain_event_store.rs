use std::sync::Arc;

use prost::Message;

use super::{make_fid_key, StoreEventHandler};
use crate::core::error::HubError;
use crate::proto::hub_event::hub_event::Body;
use crate::proto::hub_event::{HubEvent, HubEventType, MergeOnChainEventBody};
use crate::proto::onchain_event::{on_chain_event, OnChainEvent, OnChainEventType};
use crate::storage::constants::{OnChainEventPostfix, RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use crate::storage::util::increment_vec_u8;
use thiserror::Error;

static PAGE_SIZE: usize = 1000;

const LEGACY_STORAGE_UNIT_CUTOFF_TIMESTAMP: u32 = 1724889600;
const ONE_YEAR_IN_SECONDS: u32 = 365 * 24 * 60 * 60;

#[derive(Error, Debug)]
pub enum OnchainEventStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error(transparent)]
    HubError(#[from] HubError),

    #[error("Invalid event type calculating storage slots ")]
    InvalidStorageRentEventType,
}

/** A page of messages returned from various APIs */
pub struct OnchainEventsPage {
    pub onchain_events: Vec<OnChainEvent>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_block_number_key(block_number: u32) -> Vec<u8> {
    block_number.to_be_bytes().to_vec()
}

fn make_log_index_key(log_index: u32) -> Vec<u8> {
    log_index.to_be_bytes().to_vec()
}

fn make_onchain_event_type_prefix(onchain_event_type: OnChainEventType) -> Vec<u8> {
    vec![
        RootPrefix::OnChainEvent as u8,
        OnChainEventPostfix::OnChainEvents as u8,
        onchain_event_type as u8,
    ]
}

fn make_onchain_event_primary_key(onchain_event: &OnChainEvent) -> Vec<u8> {
    let mut primary_key = make_onchain_event_type_prefix(onchain_event.r#type());
    primary_key.extend(make_fid_key(onchain_event.fid as u32));
    primary_key.extend(make_block_number_key(onchain_event.block_number));
    primary_key.extend(make_log_index_key(onchain_event.log_index));

    primary_key
}

pub fn merge_onchain_event(
    db: &RocksDB,
    onchain_event: OnChainEvent,
) -> Result<(), OnchainEventStorageError> {
    let primary_key = make_onchain_event_primary_key(&onchain_event);
    // TODO(aditi): Incorporate secondary indices
    db.put(&primary_key, &onchain_event.encode_to_vec())?;
    Ok(())
}

pub fn get_onchain_events(
    db: &RocksDB,
    page_options: &PageOptions,
    event_type: OnChainEventType,
    fid: u32,
) -> Result<OnchainEventsPage, OnchainEventStorageError> {
    let mut start_prefix = make_onchain_event_type_prefix(event_type);
    start_prefix.extend(make_fid_key(fid));
    let stop_prefix = increment_vec_u8(&start_prefix);

    let mut onchain_events = vec![];
    let mut last_key = vec![];
    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let onchain_event = OnChainEvent::decode(value).map_err(|e| HubError::from(e))?;
            onchain_events.push(onchain_event);

            if onchain_events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|e| OnchainEventStorageError::HubError(e))?; // TODO: Return the right error
    let next_page_token = if last_key.len() > 0 {
        Some(last_key)
    } else {
        None
    };

    Ok(OnchainEventsPage {
        onchain_events,
        next_page_token,
    })
}

#[derive(Clone, Debug)]
pub struct StorageSlot {
    pub legacy_units: u32,
    pub units: u32,
    pub invalidate_at: u32,
}

impl StorageSlot {
    pub fn new(legacy_units: u32, units: u32, invalidate_at: u32) -> StorageSlot {
        StorageSlot {
            legacy_units,
            units,
            invalidate_at,
        }
    }

    pub fn from_event(
        onchain_event: &OnChainEvent,
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        if let Some(body) = &onchain_event.body {
            return match body {
                on_chain_event::Body::StorageRentEventBody(storage_rent_event) => {
                    let slot;
                    if onchain_event.block_timestamp < LEGACY_STORAGE_UNIT_CUTOFF_TIMESTAMP as u64 {
                        slot = StorageSlot::new(
                            storage_rent_event.units,
                            0,
                            onchain_event.block_timestamp as u32 + (ONE_YEAR_IN_SECONDS * 2),
                        );
                    } else {
                        slot = StorageSlot::new(
                            0,
                            storage_rent_event.units,
                            onchain_event.block_timestamp as u32 + ONE_YEAR_IN_SECONDS,
                        );
                    };
                    Ok(slot)
                }
                _ => Err(OnchainEventStorageError::InvalidStorageRentEventType),
            };
        }
        Err(OnchainEventStorageError::InvalidStorageRentEventType)
    }

    pub fn is_active(&self) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        self.invalidate_at < current_time
    }

    pub fn merge(&mut self, other: &StorageSlot) -> bool {
        if !other.is_active() {
            return false;
        }
        self.legacy_units += other.legacy_units;
        self.units += other.units;
        self.invalidate_at = std::cmp::min(self.invalidate_at, other.invalidate_at);
        true
    }
}

#[derive(Clone)]
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

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u32,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        let mut onchain_events = vec![];
        let mut next_page_token = None;
        loop {
            let onchain_events_page = get_onchain_events(
                &self.db,
                &PageOptions {
                    page_size: Some(PAGE_SIZE),
                    page_token: next_page_token,
                    reverse: false,
                },
                event_type,
                fid,
            )?;
            onchain_events.extend(onchain_events_page.onchain_events);
            if onchain_events_page.next_page_token.is_none() {
                break;
            } else {
                next_page_token = onchain_events_page.next_page_token
            }
        }

        Ok(onchain_events)
    }

    pub fn get_storage_slot_for_fid(
        &self,
        fid: u32,
    ) -> Result<StorageSlot, OnchainEventStorageError> {
        let rent_events = self.get_onchain_events(OnChainEventType::EventTypeStorageRent, fid)?;
        let mut storage_slot = StorageSlot::new(0, 0, 0);
        for rent_event in rent_events {
            storage_slot.merge(&StorageSlot::from_event(&rent_event)?);
        }
        Ok(storage_slot)
    }
}
