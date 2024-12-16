use super::{
    get_many_messages, make_cast_id_key, make_fid_key, make_message_primary_key, make_user_key,
    read_fid_key, read_ts_hash,
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler, HASH_LENGTH, PAGE_SIZE_MAX, TRUE_VALUE, TS_HASH_LENGTH,
};
use crate::core::error::HubError;
use crate::storage::constants::{RootPrefix, UserPostfix};
use crate::storage::db::PageOptions;
use crate::storage::util::{bytes_compare, increment_vec_u8};
use crate::{
    proto::{self as message, Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::{borrow::Borrow, sync::Arc};

type Parent = message::cast_add_body::Parent;

/**
 * CastStore persists Cast messages in RocksDB using a two-phase CRDT set to guarantee eventual
 * consistency.
 *
 * A Cast is created by a user and contains 320 characters of text and upto two embedded URLs.
 * Casts are added to the Store with a CastAdd and removed with a CastRemove. A CastAdd can be
 * a child to another CastAdd or arbitrary URI.
 *
 * Cast Messages collide if their tsHash (for CastAdds) or targetTsHash (for CastRemoves) are the
 * same for the same fid. Two CastAdds can never collide since any change to message content is
 * guaranteed to result in a unique hash value. CastRemoves can collide with CastAdds and with
 * each other, and such cases are handled with Remove-Wins and Last-Write-Wins rules as follows:
 *
 * 1. Remove wins over Adds
 * 2. Highest timestamp wins
 * 3. Highest lexicographic hash wins
 *
 * CastMessages are stored ordinarily in RocksDB indexed by a unique key `fid:tsHash` which makes
 * truncating a user's earliest messages easy. Indices are built to lookup cast adds in the adds
 * set, cast removes in the removes set, cast adds that are the children of a cast add, and cast
 * adds that mention a specific user. The key-value entries created are:
 *
 * 1. fid:tsHash -> cast message
 * 2. fid:set:tsHash -> fid:tsHash (Add Set Index)
 * 3. fid:set:targetTsHash -> fid:tsHash (Remove Set Index)
 * 4. parentFid:parentTsHash:fid:tsHash -> fid:tsHash (Child Set Index)
 * 5. mentionFid:fid:tsHash -> fid:tsHash (Mentions Set Index)
 */
#[derive(Clone)]
pub struct CastStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for CastStoreDef {
    fn postfix(&self) -> u8 {
        UserPostfix::CastMessage as u8
    }

    fn add_message_type(&self) -> u8 {
        MessageType::CastAdd as u8
    }

    fn remove_message_type(&self) -> u8 {
        MessageType::CastRemove as u8
    }

    fn is_add_type(&self, message: &Message) -> bool {
        message.signature_scheme == message::SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::CastAdd as i32
            && message.data.as_ref().unwrap().body.is_some()
    }

    fn is_remove_type(&self, message: &Message) -> bool {
        message.signature_scheme == message::SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::CastRemove as i32
            && message.data.as_ref().unwrap().body.is_some()
    }

    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    fn is_compact_state_type(&self, _message: &Message) -> bool {
        false
    }

    // RemoveWins + LWW, instead of default
    fn message_compare(
        &self,
        a_type: u8,
        a_ts_hash: &Vec<u8>,
        b_type: u8,
        b_ts_hash: &Vec<u8>,
    ) -> i8 {
        // Compare message types to enforce that RemoveWins in case of LWW ties.
        if (a_type == MessageType::CastRemove as u8) && (b_type == MessageType::CastAdd as u8) {
            return 1;
        } else if (a_type == MessageType::CastAdd as u8)
            && (b_type == MessageType::CastRemove as u8)
        {
            return -1;
        }

        // Compare timestamps (first 4 bytes of tsHash) to enforce Last-Write-Wins
        let ts_compare = bytes_compare(&a_ts_hash[0..4], &b_ts_hash[0..4]);
        if ts_compare != 0 {
            return ts_compare;
        }

        // Compare the rest of the ts_hash to break ties
        bytes_compare(&a_ts_hash[4..24], &b_ts_hash[4..24])
    }

    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        if let Ok(Some(by_parent_key)) = self.by_parent_secondary_index_key(ts_hash, message) {
            txn.put(by_parent_key, vec![TRUE_VALUE]);
        }
        if let Ok(Some(by_mention_keys)) = self.by_mention_secondary_index_key(ts_hash, message) {
            for by_mention_key in by_mention_keys {
                txn.put(by_mention_key, vec![TRUE_VALUE]);
            }
        }
        Ok(())
    }

    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let by_parent_key = self.by_parent_secondary_index_key(ts_hash, message);

        if let Ok(Some(by_parent_key)) = by_parent_key {
            txn.delete(by_parent_key);
        }

        if let Ok(Some(by_mention_keys)) = self.by_mention_secondary_index_key(ts_hash, message) {
            for by_mention_key in by_mention_keys {
                txn.delete(by_mention_key);
            }
        }

        Ok(())
    }

    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let hash = match message.data.as_ref().unwrap().body.as_ref() {
            Some(message::message_data::Body::CastAddBody(_)) => message.hash.as_ref(),
            Some(message::message_data::Body::CastRemoveBody(cast_remove_body)) => {
                cast_remove_body.target_hash.as_ref()
            }
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid cast body for add key".to_string(),
                })
            }
        };
        Ok(Self::make_cast_adds_key(
            message.data.as_ref().unwrap().fid,
            hash,
        ))
    }

    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let hash = match message.data.as_ref().unwrap().body.as_ref() {
            Some(message::message_data::Body::CastAddBody(_)) => message.hash.as_ref(),
            Some(message::message_data::Body::CastRemoveBody(cast_remove_body)) => {
                cast_remove_body.target_hash.as_ref()
            }
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid cast body for remove key".to_string(),
                })
            }
        };

        Ok(Self::make_cast_removes_key(
            message.data.as_ref().unwrap().fid,
            hash,
        ))
    }

    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Cast Store doesn't support compact state".to_string(),
        })
    }

    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Cast Store doesn't support compact state".to_string(),
        })
    }

    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl CastStoreDef {
    fn by_parent_secondary_index_key(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<Option<Vec<u8>>, HubError> {
        // For cast add, make sure at least one of parentCastId or parentUrl is set
        let cast_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            message::message_data::Body::CastAddBody(cast_add_body) => cast_add_body,
            message::message_data::Body::CastRemoveBody(_) => return Ok(None),
            _ => Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "Invalid cast body".to_string(),
            })?,
        };
        let parent = cast_body.parent.as_ref().ok_or(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: "Invalid cast body".to_string(),
        })?;

        let by_parent_key = Self::make_cast_by_parent_key(
            parent,
            message.data.as_ref().unwrap().fid,
            Some(ts_hash),
        );

        Ok(Some(by_parent_key))
    }

    // Generates unique keys used to store or fetch CastAdd messages in the byParentKey index
    pub fn make_cast_by_parent_key(
        parent: &Parent,
        fid: u64,
        ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 28 + 24 + 4);

        key.push(RootPrefix::CastsByParent as u8); // CastsByParent prefix, 1 byte
        key.extend_from_slice(&Self::make_parent_key(parent));
        if ts_hash.is_some() && ts_hash.unwrap().len() == TS_HASH_LENGTH {
            key.extend_from_slice(ts_hash.unwrap());
        }
        if fid > 0 {
            key.extend_from_slice(&make_fid_key(fid));
        }

        key
    }

    pub fn make_parent_key(target: &Parent) -> Vec<u8> {
        match target {
            Parent::ParentUrl(url) => url.as_bytes().to_vec(),
            Parent::ParentCastId(cast_id) => make_cast_id_key(cast_id),
        }
    }

    fn by_mention_secondary_index_key(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<Option<Vec<Vec<u8>>>, HubError> {
        // For cast add, make sure at least one of parentCastId or parentUrl is set
        let cast_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            message::message_data::Body::CastAddBody(cast_add_body) => cast_add_body,
            message::message_data::Body::CastRemoveBody(_) => return Ok(None),
            _ => Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "Invalid cast body".to_string(),
            })?,
        };
        // Create a vector of mention keys
        if cast_body.mentions.is_empty() {
            return Ok(None);
        }
        let mut result = Vec::with_capacity(cast_body.mentions.len());
        for &mention in cast_body.mentions.iter() {
            let mention_key = Self::make_cast_by_mention_key(
                mention,
                message.data.as_ref().unwrap().fid,
                Some(ts_hash),
            );
            result.push(mention_key);
        }
        return Ok(Some(result));
    }

    // Generates unique keys used to store or fetch CastAdd messages in the adds set index
    pub fn make_cast_adds_key(fid: u64, hash: &Vec<u8>) -> Vec<u8> {
        let mut key = Vec::with_capacity(5 + 1 + 20);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::CastAdds as u8); // CastAdds postfix, 1 byte
        if hash.len() == HASH_LENGTH {
            // hash, 20 bytes
            key.extend_from_slice(hash.as_slice());
        }
        key
    }

    // Generates unique keys used to store or fetch CastAdd messages in the byMention key index
    pub fn make_cast_by_mention_key(
        mention: u64,
        fid: u64,
        ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 4 + 24 + 4);
        key.push(RootPrefix::CastsByMention as u8); // CastsByMention prefix, 1 byte
        key.extend_from_slice(&make_fid_key(mention));
        if ts_hash.is_some() && ts_hash.unwrap().len() == TS_HASH_LENGTH {
            key.extend_from_slice(ts_hash.unwrap());
        }
        if fid > 0 {
            key.extend_from_slice(&make_fid_key(fid));
        }
        key
    }

    // Generates unique keys used to store or fetch CastRemove messages in the removes set index
    pub fn make_cast_removes_key(fid: u64, hash: &Vec<u8>) -> Vec<u8> {
        let mut key = Vec::with_capacity(5 + 1 + 20);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::CastRemoves as u8); // CastAdds postfix, 1 byte
        if hash.len() == HASH_LENGTH {
            // hash, 20 bytes
            key.extend_from_slice(hash.as_slice());
        }
        key
    }
}

pub struct CastStore {}

impl CastStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<CastStoreDef> {
        Store::new_with_store_def(db, store_event_handler, CastStoreDef { prune_size_limit })
    }

    pub fn get_cast_add(
        store: &Store<CastStoreDef>,
        fid: u64,
        hash: Vec<u8>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(message::MessageData {
                fid,
                r#type: MessageType::CastAdd.into(),
                body: Some(message::message_data::Body::CastAddBody(
                    message::CastAddBody {
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            hash,
            ..Default::default()
        };

        store.get_add(&partial_message)
    }

    pub fn get_cast_remove(
        store: &Store<CastStoreDef>,
        fid: u64,
        hash: Vec<u8>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(message::MessageData {
                fid,
                r#type: MessageType::CastRemove.into(),
                body: Some(message::message_data::Body::CastRemoveBody(
                    message::CastRemoveBody {
                        target_hash: hash.clone(),
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_remove(&partial_message)
    }

    pub fn get_cast_adds_by_fid(
        store: &Store<CastStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    pub fn get_cast_removes_by_fid(
        store: &Store<CastStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    pub fn get_casts_by_parent(
        store: &Store<CastStoreDef>,
        parent: &Parent,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        let prefix = CastStoreDef::make_cast_by_parent_key(parent, 0, None);

        let mut message_keys = vec![];
        let mut last_key = vec![];

        store.db().for_each_iterator_by_prefix(
            Some(prefix.to_vec()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, _| {
                let ts_hash_offset = prefix.len();
                let fid_offset = ts_hash_offset + TS_HASH_LENGTH;

                let fid = read_fid_key(key, fid_offset);
                let ts_hash = read_ts_hash(key, ts_hash_offset);
                let message_primary_key =
                    make_message_primary_key(fid, store.postfix(), Some(&ts_hash));

                message_keys.push(message_primary_key.to_vec());
                if message_keys.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                    last_key = key.to_vec();
                    return Ok(true); // Stop iterating
                }

                Ok(false) // Continue iterating
            },
        )?;

        let messages = get_many_messages(store.db().borrow(), message_keys)?;
        let next_page_token = if last_key.len() > 0 {
            Some(last_key[prefix.len()..].to_vec())
        } else {
            None
        };

        Ok(MessagesPage {
            messages,
            next_page_token,
        })
    }

    pub fn get_casts_by_mention(
        store: &Store<CastStoreDef>,
        mention: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        let prefix = CastStoreDef::make_cast_by_mention_key(mention, 0, None);

        let mut message_keys = vec![];
        let mut last_key = vec![];

        store.db().for_each_iterator_by_prefix(
            Some(prefix.to_vec()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, _| {
                let ts_hash_offset = prefix.len();
                let fid_offset = ts_hash_offset + TS_HASH_LENGTH;

                let fid = read_fid_key(key, fid_offset);
                let ts_hash = read_ts_hash(key, ts_hash_offset);
                let message_primary_key =
                    make_message_primary_key(fid, store.postfix(), Some(&ts_hash));

                message_keys.push(message_primary_key.to_vec());
                if message_keys.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                    last_key = key.to_vec();
                    return Ok(true); // Stop iterating
                }

                Ok(false) // Continue iterating
            },
        )?;

        let messages_bytes = get_many_messages(store.db().borrow(), message_keys)?;
        let next_page_token = if last_key.len() > 0 {
            Some(last_key[prefix.len()..].to_vec())
        } else {
            None
        };

        Ok(MessagesPage {
            messages: messages_bytes,
            next_page_token,
        })
    }
}
