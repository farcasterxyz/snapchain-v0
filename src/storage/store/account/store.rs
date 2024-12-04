use super::{
    super::super::util::{bytes_compare, vec_to_u8_24},
    delete_message_transaction, get_message, get_messages_page_by_prefix, is_message_in_time_range,
    make_message_primary_key, make_ts_hash, message_decode, message_encode,
    put_message_transaction, MessagesPage, StoreEventHandler, TS_HASH_LENGTH,
};
use crate::core::error::HubError;
use crate::proto::{
    hub_event, HubEvent, HubEventType, MergeMessageBody, PruneMessageBody, RevokeMessageBody,
};
use crate::storage::db::PageOptions;
use crate::storage::util::increment_vec_u8;
use crate::{
    proto::{link_body::Target, message_data::Body, Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::clone::Clone;
use std::string::ToString;
use std::sync::{Arc, Mutex};
use tracing::warn;

pub const FID_LOCKS_COUNT: usize = 4;
pub const PAGE_SIZE_MAX: usize = 10_000;

// #[derive(Debug, Default)]
// pub struct PageOptions {
//     pub page_size: Option<usize>,
//     pub page_token: Option<Vec<u8>>,
//     pub reverse: bool,
// }

/// The `Send` trait indicates that a type can be safely transferred between threads.
/// The `Sync` trait indicates that a type can be safely shared between threads.
/// The `StoreDef` trait is implemented for types that are both `Send` and `Sync`,
/// allowing them to be used as trait objects in the `Store` struct.
///
/// Some methods in this trait provide default implementations. These methods can be overridden
/// by implementing the trait for a specific type.
pub trait StoreDef: Send + Sync {
    fn postfix(&self) -> u8;
    fn add_message_type(&self) -> u8;
    fn remove_message_type(&self) -> u8;
    fn compact_state_message_type(&self) -> u8;

    fn is_add_type(&self, message: &Message) -> bool;
    fn is_remove_type(&self, message: &Message) -> bool;

    // If the store supports remove messages, this should return true
    fn remove_type_supported(&self) -> bool {
        self.remove_message_type() != MessageType::None as u8
    }

    fn is_compact_state_type(&self, message: &Message) -> bool;

    // If the store supports compaction state messages, this should return true
    fn compact_state_type_supported(&self) -> bool {
        self.compact_state_message_type() != MessageType::None as u8
    }

    fn build_secondary_indices(
        &self,
        _txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        _message: &Message,
    ) -> Result<(), HubError> {
        Ok(())
    }

    fn delete_secondary_indices(
        &self,
        _txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        _message: &Message,
    ) -> Result<(), HubError> {
        Ok(())
    }

    fn delete_remove_secondary_indices(
        &self,
        _txn: &mut RocksDbTransactionBatch,
        _message: &Message,
    ) -> Result<(), HubError> {
        Ok(())
    }

    fn find_merge_add_conflicts(&self, db: &RocksDB, message: &Message) -> Result<(), HubError>;
    fn find_merge_remove_conflicts(&self, db: &RocksDB, message: &Message) -> Result<(), HubError>;

    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError>;
    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError>;
    fn make_compact_state_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError>;
    fn make_compact_state_prefix(&self, fid: u32) -> Result<Vec<u8>, HubError>;

    fn get_prune_size_limit(&self) -> u32;

    fn get_merge_conflicts(
        &self,
        db: &RocksDB,
        message: &Message,
        ts_hash: &[u8; TS_HASH_LENGTH],
    ) -> Result<Vec<Message>, HubError> {
        Self::get_default_merge_conflicts(&self, db, message, ts_hash)
    }

    fn get_default_merge_conflicts(
        &self,
        db: &RocksDB,
        message: &Message,
        ts_hash: &[u8; TS_HASH_LENGTH],
    ) -> Result<Vec<Message>, HubError> {
        // The JS code does validateAdd()/validateRemove() here, but that's not needed because we
        // already validated that the message has a data field and a body field in the is_add_type()

        if self.is_add_type(message) {
            self.find_merge_add_conflicts(db, message)?;
        } else {
            self.find_merge_remove_conflicts(db, message)?;
        }

        let mut conflicts = vec![];

        if self.remove_type_supported() {
            let remove_key = self.make_remove_key(message)?;
            let remove_ts_hash = db.get(&remove_key)?;

            if remove_ts_hash.is_some() {
                let remove_compare = self.message_compare(
                    self.remove_message_type(),
                    &remove_ts_hash.clone().unwrap(),
                    message.data.as_ref().unwrap().r#type as u8,
                    &ts_hash.to_vec(),
                );

                if remove_compare > 0 {
                    return Err(HubError {
                        code: "bad_request.conflict".to_string(),
                        message: "message conflicts with a more recent remove".to_string(),
                    });
                }
                if remove_compare == 0 {
                    return Err(HubError {
                        code: "bad_request.duplicate".to_string(),
                        message: "message has already been merged".to_string(),
                    });
                }

                // If the existing remove has a lower order than the new message, retrieve the full
                // Remove message and delete it as part of the RocksDB transaction
                let maybe_existing_remove = get_message(
                    &db,
                    message.data.as_ref().unwrap().fid as u32,
                    self.postfix(),
                    &vec_to_u8_24(&remove_ts_hash)?,
                )?;

                if maybe_existing_remove.is_some() {
                    conflicts.push(maybe_existing_remove.unwrap());
                } else {
                    warn!(
                        remove_ts_hash = format!("{:x?}", remove_ts_hash.unwrap()),
                        "Message's ts_hash exists but message not found in store"
                    );
                }
            }
        }

        // Check if there is an add timestamp hash for this
        let add_key = self.make_add_key(message)?;
        let add_ts_hash = db.get(&add_key)?;

        if add_ts_hash.is_some() {
            let add_compare = self.message_compare(
                self.add_message_type(),
                &add_ts_hash.clone().unwrap(),
                message.data.as_ref().unwrap().r#type as u8,
                &ts_hash.to_vec(),
            );

            if add_compare > 0 {
                return Err(HubError {
                    code: "bad_request.conflict".to_string(),
                    message: "message conflicts with a more recent add".to_string(),
                });
            }
            if add_compare == 0 {
                return Err(HubError {
                    code: "bad_request.duplicate".to_string(),
                    message: "message has already been merged".to_string(),
                });
            }

            // If the existing add has a lower order than the new message, retrieve the full
            // Add message and delete it as part of the RocksDB transaction
            let maybe_existing_add = get_message(
                &db,
                message.data.as_ref().unwrap().fid as u32,
                self.postfix(),
                &vec_to_u8_24(&add_ts_hash)?,
            )?;

            if maybe_existing_add.is_none() {
                warn!(
                    add_ts_hash = format!("{:x?}", add_ts_hash.unwrap()),
                    "Message's ts_hash exists but message not found in store"
                );
            } else {
                conflicts.push(maybe_existing_add.unwrap());
            }
        }

        Ok(conflicts)
    }

    fn message_compare(
        &self,
        a_type: u8,
        a_ts_hash: &Vec<u8>,
        b_type: u8,
        b_ts_hash: &Vec<u8>,
    ) -> i8 {
        // Compare timestamps (first 4 bytes of ts_hash)
        let ts_compare = bytes_compare(&a_ts_hash[0..4], &b_ts_hash[0..4]);
        if ts_compare != 0 {
            return ts_compare;
        }

        if a_type == self.remove_message_type() && b_type == self.add_message_type() {
            return 1;
        }
        if a_type == self.add_message_type() && b_type == self.remove_message_type() {
            return -1;
        }

        // Compare the rest of the ts_hash to break ties
        bytes_compare(&a_ts_hash[4..24], &b_ts_hash[4..24])
    }

    fn revoke_event_args(&self, message: &Message) -> HubEvent {
        HubEvent {
            r#type: HubEventType::RevokeMessage as i32,
            body: Some(hub_event::Body::RevokeMessageBody(RevokeMessageBody {
                message: Some(message.clone()),
            })),
            id: 0,
        }
    }

    fn merge_event_args(&self, message: &Message, merge_conflicts: Vec<Message>) -> HubEvent {
        HubEvent {
            r#type: HubEventType::MergeMessage as i32,
            body: Some(hub_event::Body::MergeMessageBody(MergeMessageBody {
                message: Some(message.clone()),
                deleted_messages: match &message.data {
                    Some(data) => {
                        if data.r#type == self.compact_state_message_type() as i32 {
                            // In the case of merging compact state, we omit the deleted messages as this would
                            // result in an unbounded message size:
                            Vec::<Message>::new()
                        } else {
                            merge_conflicts
                        }
                    }
                    None => Vec::<Message>::new(),
                },
            })),
            id: 0,
        }
    }

    fn prune_event_args(&self, message: &Message) -> HubEvent {
        HubEvent {
            r#type: HubEventType::PruneMessage as i32,
            body: Some(hub_event::Body::PruneMessageBody(PruneMessageBody {
                message: Some(message.clone()),
            })),
            id: 0,
        }
    }
}

#[derive(Clone)]
pub struct Store<T>
where
    T: StoreDef + Clone,
{
    store_def: T,
    store_event_handler: Arc<StoreEventHandler>,
    fid_locks: Arc<[Mutex<()>; 4]>,
    db: Arc<RocksDB>,
}

impl<T: StoreDef + Clone> Store<T> {
    pub fn new_with_store_def(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        store_def: T,
    ) -> Store<T> {
        Store {
            store_def,
            store_event_handler,
            fid_locks: Arc::new([
                Mutex::new(()),
                Mutex::new(()),
                Mutex::new(()),
                Mutex::new(()),
            ]),
            db,
        }
    }

    pub fn store_def(&self) -> T {
        self.store_def.clone()
    }

    pub fn db(&self) -> Arc<RocksDB> {
        self.db.clone()
    }

    pub fn event_handler(&self) -> Arc<StoreEventHandler> {
        self.store_event_handler.clone()
    }

    pub fn postfix(&self) -> u8 {
        self.store_def.postfix()
    }

    pub fn get_add(&self, partial_message: &Message) -> Result<Option<Message>, HubError> {
        // First check the fid
        if partial_message.data.is_none() || partial_message.data.as_ref().unwrap().fid == 0 {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "fid is required".to_string(),
            });
        }

        let adds_key = self.store_def.make_add_key(partial_message)?;
        let message_ts_hash = self.db.get(&adds_key)?;

        if message_ts_hash.is_none() {
            return Ok(None);
        }

        get_message(
            &self.db,
            partial_message.data.as_ref().unwrap().fid as u32,
            self.store_def.postfix(),
            &vec_to_u8_24(&message_ts_hash)?,
        )
    }

    pub fn get_remove(&self, partial_message: &Message) -> Result<Option<Message>, HubError> {
        if !self.store_def.remove_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "remove type not supported".to_string(),
            });
        }

        // First check the fid
        if partial_message.data.is_none() || partial_message.data.as_ref().unwrap().fid == 0 {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "fid is required".to_string(),
            });
        }

        let removes_key = self.store_def.make_remove_key(partial_message)?;
        let message_ts_hash = self.db.get(&removes_key)?;

        if message_ts_hash.is_none() {
            return Ok(None);
        }

        get_message(
            &self.db,
            partial_message.data.as_ref().unwrap().fid as u32,
            self.store_def.postfix(),
            &vec_to_u8_24(&message_ts_hash)?,
        )
    }

    pub fn get_adds_by_fid<F>(
        &self,
        fid: u32,
        page_options: &PageOptions,
        filter: Option<F>,
    ) -> Result<MessagesPage, HubError>
    where
        F: Fn(&Message) -> bool,
    {
        let prefix = make_message_primary_key(fid, self.store_def.postfix(), None);
        let messages_page =
            get_messages_page_by_prefix(&self.db, &prefix, &page_options, |message| {
                self.store_def.is_add_type(&message)
                    && filter.as_ref().map(|f| f(&message)).unwrap_or(true)
            })?;

        Ok(messages_page)
    }

    pub fn get_removes_by_fid<F>(
        &self,
        fid: u32,
        page_options: &PageOptions,
        filter: Option<F>,
    ) -> Result<MessagesPage, HubError>
    where
        F: Fn(&Message) -> bool,
    {
        if !self.store_def.remove_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "remove type not supported".to_string(),
            });
        }

        let prefix = make_message_primary_key(fid, self.store_def.postfix(), None);
        let messages = get_messages_page_by_prefix(&self.db, &prefix, &page_options, |message| {
            self.store_def.is_remove_type(&message)
                && filter.as_ref().map(|f| f(&message)).unwrap_or(true)
        })?;

        Ok(messages)
    }

    fn put_add_compact_state_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
    ) -> Result<(), HubError> {
        if !self.store_def.compact_state_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "compact state type not supported".to_string(),
            });
        }

        let compact_state_key = self.store_def.make_compact_state_add_key(message)?;
        txn.put(compact_state_key, message_encode(&message));

        Ok(())
    }

    fn put_add_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        put_message_transaction(txn, &message)?;

        let adds_key = self.store_def.make_add_key(message)?;

        txn.put(adds_key, ts_hash.to_vec());

        self.store_def
            .build_secondary_indices(txn, ts_hash, message)?;

        Ok(())
    }

    fn delete_compact_state_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
    ) -> Result<(), HubError> {
        if !self.store_def.compact_state_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "compact state type not supported".to_string(),
            });
        }

        let compact_state_key = self.store_def.make_compact_state_add_key(message)?;
        txn.delete(compact_state_key);

        Ok(())
    }

    fn delete_add_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        self.store_def
            .delete_secondary_indices(txn, ts_hash, message)?;

        let add_key = self.store_def.make_add_key(message)?;
        txn.delete(add_key);

        delete_message_transaction(txn, message)
    }

    fn put_remove_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        if !self.store_def.remove_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "remove type not supported".to_string(),
            });
        }

        put_message_transaction(txn, &message)?;

        let removes_key = self.store_def.make_remove_key(message)?;
        txn.put(removes_key, ts_hash.to_vec());

        Ok(())
    }

    fn delete_remove_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
    ) -> Result<(), HubError> {
        if !self.store_def.remove_type_supported() {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "remove type not supported".to_string(),
            });
        }

        self.store_def
            .delete_remove_secondary_indices(txn, message)?;

        let remove_key = self.store_def.make_remove_key(message)?;
        txn.delete(remove_key);

        delete_message_transaction(txn, message)
    }

    fn delete_many_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        messages: &Vec<Message>,
    ) -> Result<(), HubError> {
        for message in messages {
            if self.store_def.is_compact_state_type(message) {
                self.delete_compact_state_transaction(txn, message)?;
            } else if self.store_def.is_add_type(message) {
                let ts_hash =
                    make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;
                self.delete_add_transaction(txn, &ts_hash, message)?;
            }
            if self.store_def.remove_type_supported() && self.store_def.is_remove_type(message) {
                self.delete_remove_transaction(txn, message)?;
            }
        }

        Ok(())
    }

    pub fn merge(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        // Grab a merge lock. The typescript code does this by individual fid, but we don't have a
        // good way of doing that efficiently here. We'll just use an array of locks, with each fid
        // deterministically mapped to a lock.
        let _fid_lock = &self.fid_locks
            [message.data.as_ref().unwrap().fid as usize % FID_LOCKS_COUNT]
            .lock()
            .unwrap();

        if !self.store_def.is_add_type(message)
            && !(self.store_def.remove_type_supported() && self.store_def.is_remove_type(message))
            && !(self.store_def.compact_state_type_supported()
                && self.store_def.is_compact_state_type(message))
        {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "invalid message type".to_string(),
            });
        }

        let ts_hash = make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;

        if self.store_def().is_compact_state_type(message) {
            self.merge_compact_state(message, txn)
        } else if self.store_def.is_add_type(message) {
            self.merge_add(&ts_hash, message, txn)
        } else {
            self.merge_remove(&ts_hash, message, txn)
        }
    }

    pub fn revoke(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        // Get the message ts_hash
        let ts_hash = make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;

        if self.store_def().is_compact_state_type(message) {
            self.delete_compact_state_transaction(txn, message)?;
        } else if self.store_def.is_add_type(message) {
            self.delete_add_transaction(txn, &ts_hash, message)?;
        } else if self.store_def.remove_type_supported() && self.store_def.is_remove_type(message) {
            self.delete_remove_transaction(txn, message)?;
        } else {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "invalid message type".to_string(),
            });
        }

        let mut hub_event = self.store_def.revoke_event_args(message);

        let id = self
            .store_event_handler
            .commit_transaction(txn, &mut hub_event)?;
        hub_event.id = id;

        Ok(hub_event)
    }

    fn read_compact_state_details(
        &self,
        message: &Message,
    ) -> Result<(u32, u32, Vec<u64>), HubError> {
        if let Some(data) = &message.data {
            if let Some(Body::LinkCompactStateBody(link_compact_body)) = &data.body {
                Ok((
                    data.fid as u32,
                    data.timestamp,
                    link_compact_body.target_fids.clone(),
                ))
            } else {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid compact state message: No link compact state body"
                        .to_string(),
                });
            }
        } else {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "Invalid compact state message: no data".to_string(),
            });
        }
    }

    pub fn merge_compact_state(
        &self,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        let mut merge_conflicts = vec![];

        // First, find if there's an existing compact state message, and if there is,
        // delete it if it is older
        let compact_state_key = self.store_def.make_compact_state_add_key(message)?;
        let existing_compact_state = self.db.get(&compact_state_key)?;

        if existing_compact_state.is_some() {
            if let Ok(existing_compact_state_message) =
                message_decode(existing_compact_state.unwrap().as_ref())
            {
                if existing_compact_state_message
                    .data
                    .as_ref()
                    .unwrap()
                    .timestamp
                    < message.data.as_ref().unwrap().timestamp
                {
                    merge_conflicts.push(existing_compact_state_message);
                } else {
                    // Can't merge an older compact state message
                    return Err(HubError {
                        code: "bad_request.conflict".to_string(),
                        message: "A newer Compact State message is already merged".to_string(),
                    });
                }
            }
        }

        let (fid, compact_state_timestamp, target_fids) =
            self.read_compact_state_details(message)?;

        // Go over all the messages for this Fid, that are older than the compact state message and
        // 1. Delete all remove messages
        // 2. Delete all add messages that are not in the target_fids list
        let prefix = &make_message_primary_key(fid, self.store_def.postfix(), None);
        self.db.for_each_iterator_by_prefix(
            Some(prefix.to_vec()),
            Some(increment_vec_u8(prefix)),
            &PageOptions::default(),
            |_key, value| {
                let message = message_decode(value)?;

                // Only if message is older than the compact state message
                if message.data.as_ref().unwrap().timestamp > compact_state_timestamp {
                    // Finish the iteration since all future messages will have greater timestamp
                    return Ok(true);
                }

                if self.store_def.is_remove_type(&message) {
                    merge_conflicts.push(message);
                } else if self.store_def.is_add_type(&message) {
                    // Get the link_body fid
                    if let Some(data) = &message.data {
                        if let Some(Body::LinkBody(link_body)) = &data.body {
                            if let Some(Target::TargetFid(target_fid)) = link_body.target {
                                if !target_fids.contains(&target_fid) {
                                    merge_conflicts.push(message);
                                }
                            }
                        }
                    }
                }

                Ok(false) // Continue the iteration
            },
        )?;

        // Delete all the merge conflicts
        self.delete_many_transaction(txn, &merge_conflicts)?;

        // Add the Link compact state message
        self.put_add_compact_state_transaction(txn, message)?;

        // Event Handler
        let mut hub_event = self.store_def.merge_event_args(message, merge_conflicts);

        let id = self
            .store_event_handler
            .commit_transaction(txn, &mut hub_event)?;

        hub_event.id = id;

        Ok(hub_event)
    }

    pub fn merge_add(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        // If the store supports compact state messages, we don't merge messages that don't exist in the compact state
        if self.store_def.compact_state_type_supported() {
            // Get the compact state message
            let compact_state_key = self.store_def.make_compact_state_add_key(message)?;
            if let Some(compact_state_message_bytes) = self.db.get(&compact_state_key)? {
                let compact_state_message = message_decode(compact_state_message_bytes.as_ref())?;

                let (_, compact_state_timestamp, target_fids) =
                    self.read_compact_state_details(&compact_state_message)?;

                if let Some(Body::LinkBody(link_body)) = &message.data.as_ref().unwrap().body {
                    if let Some(Target::TargetFid(target_fid)) = link_body.target {
                        // If the message is older than the compact state message, and the target fid is not in the target_fids list
                        if message.data.as_ref().unwrap().timestamp < compact_state_timestamp
                            && !target_fids.contains(&target_fid)
                        {
                            return Err(HubError {
                                code: "bad_request.conflict".to_string(),
                                message: format!(
                                    "Target fid {} not in the compact state target fids",
                                    target_fid
                                ),
                            });
                        }
                    }
                }
            }
        }

        // Get the merge conflicts first
        let merge_conflicts = self
            .store_def
            .get_merge_conflicts(&self.db, message, ts_hash)?;

        // Delete all the merge conflicts
        self.delete_many_transaction(txn, &merge_conflicts)?;

        // Add ops to store the message by messageKey and index the messageKey by set and by target
        self.put_add_transaction(txn, &ts_hash, message)?;

        // Event handler
        let mut hub_event = self.store_def.merge_event_args(message, merge_conflicts);

        let id = self
            .store_event_handler
            .commit_transaction(txn, &mut hub_event)?;

        hub_event.id = id;

        Ok(hub_event)
    }

    pub fn merge_remove(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        // If the store supports compact state messages, we don't merge remove messages before its timestamp
        // If the store supports compact state messages, we don't merge messages that don't exist in the compact state
        if self.store_def.compact_state_type_supported() {
            // Get the compact state message
            let compact_state_key = self.store_def.make_compact_state_add_key(message)?;
            if let Some(compact_state_message_bytes) = self.db.get(&compact_state_key)? {
                let compact_state_message = message_decode(compact_state_message_bytes.as_ref())?;

                let (_, compact_state_timestamp, _) =
                    self.read_compact_state_details(&compact_state_message)?;

                // If the message is older than the compact state message, and the target fid is not in the target_fids list
                if message.data.as_ref().unwrap().timestamp < compact_state_timestamp {
                    return Err(HubError {
                        code: "bad_request.prunable".to_string(),
                        message: format!(
                            "Remove message earlier than the compact state message will be immediately pruned",
                        ),
                    });
                }
            }
        }

        // Get the merge conflicts first
        let merge_conflicts = self
            .store_def
            .get_merge_conflicts(&self.db, message, ts_hash)?;

        // Delete all the merge conflicts
        self.delete_many_transaction(txn, &merge_conflicts)?;

        // Add ops to store the message by messageKey and index the messageKey by set and by target
        self.put_remove_transaction(txn, ts_hash, message)?;

        // Event handler
        let mut hub_event = self.store_def.merge_event_args(message, merge_conflicts);

        let id = self
            .store_event_handler
            .commit_transaction(txn, &mut hub_event)?;

        hub_event.id = id;

        Ok(hub_event)
    }

    pub fn prune_messages(
        &self,
        fid: u32,
        current_count: u32,
        max_count: u32,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, HubError> {
        let mut pruned_events = vec![];

        let mut count = current_count;

        if count <= max_count {
            return Ok(pruned_events); // Nothing to prune
        }

        let prefix = &make_message_primary_key(fid, self.store_def.postfix(), None);
        self.db.for_each_iterator_by_prefix(
            Some(prefix.to_vec()),
            Some(increment_vec_u8(prefix)),
            &PageOptions::default(),
            |_key, value| {
                if count <= max_count {
                    return Ok(true); // Stop the iteration, nothing left to prune
                }

                // Value is a message, so try to decode it
                let message = message_decode(value)?;

                // Note that compact state messages are not pruned
                if self.store_def.compact_state_type_supported()
                    && self.store_def.is_compact_state_type(&message)
                {
                    return Ok(false); // Continue the iteration
                } else if self.store_def.is_add_type(&message) {
                    let ts_hash =
                        make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;
                    self.delete_add_transaction(txn, &ts_hash, &message)?;
                } else if self.store_def.remove_type_supported()
                    && self.store_def.is_remove_type(&message)
                {
                    self.delete_remove_transaction(txn, &message)?;
                }

                // Event Handler
                let mut hub_event = self.store_def.prune_event_args(&message);
                let id = self
                    .store_event_handler
                    .commit_transaction(txn, &mut hub_event)?;

                count -= 1;

                hub_event.id = id;
                pruned_events.push(hub_event);

                Ok(false) // Continue the iteration
            },
        )?;

        Ok(pruned_events)
    }

    pub fn revoke_messages_by_signer(
        &self,
        fid: u32,
        key: &Vec<u8>,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, HubError> {
        let mut revoke_events = vec![];

        let prefix = &make_message_primary_key(fid, self.store_def.postfix(), None);
        self.db.for_each_iterator_by_prefix(
            Some(prefix.to_vec()),
            Some(increment_vec_u8(prefix)),
            &PageOptions::default(),
            |_key, value| {
                // Value is a message, so try to decode it
                let message = message_decode(value)?;

                if bytes_compare(&message.signer, key) == 0 {
                    let result = self.revoke(&message, txn);
                    match result {
                        Ok(event) => {
                            revoke_events.push(event);
                        }
                        Err(e) => {
                            warn!(
                                fid = fid,
                                hash = message.hex_hash(),
                                error = format!("{:?}", e),
                                "Error revoking message, skipping"
                            );
                        }
                    }
                }
                Ok(false) // Continue the iteration
            },
        )?;

        Ok(revoke_events)
    }

    pub fn get_all_messages_by_fid(
        &self,
        fid: u32,
        start_time: Option<u32>,
        stop_time: Option<u32>,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        let prefix = make_message_primary_key(fid, self.store_def.postfix(), None);
        let messages = get_messages_page_by_prefix(&self.db, &prefix, &page_options, |message| {
            is_message_in_time_range(start_time, stop_time, message)
                && (self.store_def.is_add_type(&message)
                    || (self.store_def.remove_type_supported()
                        && self.store_def.is_remove_type(&message)))
        })?;

        Ok(messages)
    }

    pub fn get_compact_state_messages_by_fid(
        &self,
        fid: u32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        if !self.store_def.compact_state_type_supported() {
            return Err(HubError::invalid_parameter("compact state not supported"));
        }

        match self.store_def.make_compact_state_prefix(fid) {
            Ok(prefix) => {
                let messages =
                    get_messages_page_by_prefix(&self.db, &prefix, &page_options, |message| {
                        self.store_def.compact_state_type_supported()
                            && self.store_def.is_compact_state_type(&message)
                    })?;

                Ok(messages)
            }
            Err(e) => Err(e),
        }
    }
}

// Note about dispatch - The methods are dispatched to the Store struct, which is a Box<dyn StoreDef>.
// This means the NodeJS code can pass in any store, and the Rust code will call the correct method
// for that store
impl<T: StoreDef + Clone> Store<T> {}
