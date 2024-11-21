use crate::core::error::HubError;
use crate::proto::hub_event::HubEvent;
use crate::storage::constants::{RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::util::increment_vec_u8;
use prost::Message as _;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const TIMESTAMP_BITS: u32 = 41;
const SEQUENCE_BITS: u32 = 12;
pub const FARCASTER_EPOCH: u64 = 1609459200000;

fn make_event_id(timestamp: u64, seq: u64) -> u64 {
    let shifted_timestamp = timestamp << SEQUENCE_BITS;
    let padded_seq = seq & ((1 << SEQUENCE_BITS) - 1); // Ensures seq fits in SEQUENCE_BITS
    shifted_timestamp | padded_seq
}

pub struct EventsPage {
    pub events: Vec<HubEvent>,
    pub next_page_token: Option<Vec<u8>>,
}

struct HubEventIdGenerator {
    last_timestamp: u64, // ms since epoch
    last_seq: u64,
    epoch: u64,
}

impl HubEventIdGenerator {
    fn new(epoch: Option<u64>, last_timestamp: Option<u64>, last_seq: Option<u64>) -> Self {
        HubEventIdGenerator {
            epoch: epoch.unwrap_or(0),
            last_timestamp: last_timestamp.unwrap_or(0),
            last_seq: last_seq.unwrap_or(0),
        }
    }

    fn generate_id(&mut self, current_timestamp: Option<u64>) -> Result<u64, HubError> {
        let current_timestamp = current_timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64
        }) - self.epoch;

        if current_timestamp == self.last_timestamp {
            self.last_seq += 1;
        } else {
            self.last_timestamp = current_timestamp;
            self.last_seq = 0;
        }

        if self.last_timestamp >= 2u64.pow(TIMESTAMP_BITS) {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: format!("timestamp > {} bits", TIMESTAMP_BITS),
            });
        }

        if self.last_seq >= 2u64.pow(SEQUENCE_BITS) {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: format!("sequence > {} bits", SEQUENCE_BITS),
            });
        }

        Ok(make_event_id(self.last_timestamp, self.last_seq))
    }
}

pub struct StoreEventHandler {
    generator: Arc<Mutex<HubEventIdGenerator>>,
}

impl StoreEventHandler {
    pub fn new(
        epoch: Option<u64>,
        last_timestamp: Option<u64>,
        last_seq: Option<u64>,
    ) -> Arc<Self> {
        Arc::new(StoreEventHandler {
            generator: Arc::new(Mutex::new(HubEventIdGenerator::new(
                Some(epoch.unwrap_or(FARCASTER_EPOCH)),
                last_timestamp,
                last_seq,
            ))),
        })
    }

    pub fn commit_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        raw_event: &mut HubEvent,
    ) -> Result<u64, HubError> {
        // Acquire the lock so we don't generate multiple IDs. This also serves as the commit lock
        let mut generator = self.generator.lock().unwrap();

        // Generate the event ID
        let event_id = generator.generate_id(None)?;
        raw_event.id = event_id;

        HubEvent::put_event_transaction(txn, &raw_event)?;

        // These two calls are made in the JS code
        // this._storageCache.processEvent(event);
        // this.broadcastEvent(event);

        Ok(event_id)
    }
}

impl HubEvent {
    fn make_event_key(event_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 8);

        key.push(RootPrefix::HubEvents as u8); // HubEvents prefix, 1 byte
        key.extend_from_slice(&event_id.to_be_bytes());

        key
    }

    pub fn put_event_transaction(
        txn: &mut RocksDbTransactionBatch,
        event: &HubEvent,
    ) -> Result<(), HubError> {
        let key = Self::make_event_key(event.id);
        let value = event.encode_to_vec();

        txn.put(key, value);

        Ok(())
    }

    pub fn get_events(
        db: Arc<RocksDB>,
        start_id: u64,
        stop_id: Option<u64>,
        page_options: Option<PageOptions>,
    ) -> Result<EventsPage, HubError> {
        let start_prefix = Self::make_event_key(start_id);
        let stop_prefix = match stop_id {
            Some(id) => Self::make_event_key(id),
            None => increment_vec_u8(&vec![RootPrefix::HubEvents as u8 + 1]),
        };

        let mut events = Vec::new();
        let mut last_key = vec![];
        let page_options = page_options.unwrap_or_else(|| PageOptions::default());

        db.for_each_iterator_by_prefix_paged(
            Some(start_prefix),
            Some(stop_prefix),
            &page_options,
            |key, value| {
                let event = HubEvent::decode(value).map_err(|e| HubError::from(e))?;
                events.push(event);
                if events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                    last_key = key.to_vec();
                    return Ok(true); // Stop iterating
                }
                Ok(false) // Continue iterating
            },
        )?;

        Ok(EventsPage {
            events,
            next_page_token: if last_key.len() > 0 {
                Some(last_key)
            } else {
                None
            },
        })
    }
}
