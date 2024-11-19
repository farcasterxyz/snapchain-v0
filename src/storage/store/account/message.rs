use prost::Message as _;

use super::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::storage::constants::{RootPrefix, UserPostfix};
use crate::storage::db::{PageOptions, RocksdbError};
use crate::{
    proto::message::{CastId, Message as MessageProto, MessageData, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};

pub const FID_BYTES: usize = 4;

pub const TS_HASH_LENGTH: usize = 24;
pub const HASH_LENGTH: usize = 20;

pub const TRUE_VALUE: u8 = 1;

/** A page of messages returned from various APIs */
pub struct MessagesPage {
    pub messages_bytes: Vec<Vec<u8>>,
    pub next_page_token: Option<Vec<u8>>,
}

pub trait IntoU8 {
    fn into_u8(self) -> u8;
}
impl IntoU8 for MessageType {
    fn into_u8(self) -> u8 {
        self as u8
    }
}

pub trait IntoI32 {
    fn into_i32(self) -> i32;
}

impl IntoI32 for MessageType {
    fn into_i32(self) -> i32 {
        self as i32
    }
}

/** Convert a specific message type (CastAdd / CastRemove) to a class of message (CastMessage) */
pub fn type_to_set_postfix(message_type: MessageType) -> UserPostfix {
    if message_type == MessageType::CastAdd || message_type == MessageType::CastRemove {
        return UserPostfix::CastMessage;
    }

    if message_type == MessageType::ReactionAdd || message_type == MessageType::ReactionRemove {
        return UserPostfix::ReactionMessage;
    }

    if message_type == MessageType::VerificationAddEthAddress
        || message_type == MessageType::VerificationRemove
    {
        return UserPostfix::VerificationMessage;
    }

    if message_type == MessageType::UserDataAdd {
        return UserPostfix::UserDataMessage;
    }

    if message_type == MessageType::LinkAdd || message_type == MessageType::LinkRemove {
        return UserPostfix::LinkMessage;
    }

    if message_type == MessageType::UsernameProof {
        return UserPostfix::UsernameProofMessage;
    }

    panic!("invalid type");
}

pub fn make_ts_hash(timestamp: u32, hash: &Vec<u8>) -> Result<[u8; TS_HASH_LENGTH], HubError> {
    // No need to check if timestamp > 2^32 because it's already a u32

    if hash.len() != HASH_LENGTH {
        return Err(HubError {
            code: "internal_error".to_string(),
            message: "hash length is not 20".to_string(),
        });
    }

    let mut ts_hash = [0u8; 24];
    // Store the timestamp as big-endian in the first 4 bytes
    ts_hash[0..4].copy_from_slice(&timestamp.to_be_bytes());
    // Store the hash in the remaining 20 bytes
    ts_hash[4..24].copy_from_slice(&hash[0..HASH_LENGTH]);

    Ok(ts_hash)
}

#[allow(dead_code)]
pub fn unpack_ts_hash(ts_hash: &[u8; TS_HASH_LENGTH]) -> (u32, [u8; HASH_LENGTH]) {
    let mut timestamp_bytes = [0u8; 4];
    timestamp_bytes.copy_from_slice(&ts_hash[0..4]);
    let timestamp = u32::from_be_bytes(timestamp_bytes);

    let mut hash = [0u8; HASH_LENGTH];
    hash.copy_from_slice(&ts_hash[4..24]);

    (timestamp, hash)
}

pub fn make_fid_key(fid: u32) -> Vec<u8> {
    fid.to_be_bytes().to_vec()
}

pub fn read_fid_key(key: &[u8]) -> u32 {
    let mut fid_bytes = [0u8; 4];
    fid_bytes.copy_from_slice(&key[0..4]);
    u32::from_be_bytes(fid_bytes)
}

pub fn make_user_key(fid: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4);
    key.push(RootPrefix::User as u8);

    key.extend_from_slice(&make_fid_key(fid));

    key
}

pub fn make_message_primary_key(
    fid: u32,
    set: u8,
    ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4 + 1 + TS_HASH_LENGTH);
    key.extend_from_slice(&make_user_key(fid));
    key.push(set);
    if ts_hash.is_some() {
        key.extend_from_slice(ts_hash.unwrap());
    }

    key
}

pub fn make_cast_id_key(cast_id: &CastId) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + HASH_LENGTH);
    key.extend_from_slice(&make_fid_key(cast_id.fid as u32));
    key.extend_from_slice(&cast_id.hash);

    key
}

pub fn get_message(
    db: &RocksDB,
    fid: u32,
    set: u8,
    ts_hash: &[u8; TS_HASH_LENGTH],
) -> Result<Option<MessageProto>, HubError> {
    let key = make_message_primary_key(fid, set, Some(ts_hash));
    // println!("get_message key: {:?}", key);

    match db.get(&key)? {
        Some(bytes) => match message_decode(bytes.as_slice()) {
            Ok(message) => Ok(Some(message)),
            Err(e) => Err(e.into()),
        },
        None => Ok(None),
    }
}

/** Read many messages.
 * Note that if a message is not found, that corresponding entry in the result will be None.
 * This is different from the behaviour of get_message, which returns an error.
 */
pub fn get_many_messages_as_bytes(
    db: &RocksDB,
    primary_keys: Vec<Vec<u8>>,
) -> Result<Vec<Vec<u8>>, HubError> {
    let mut messages = Vec::new();

    for key in primary_keys {
        if let Ok(Some(value)) = db.get(&key) {
            messages.push(value);
        } else {
            return Err(HubError::not_found(
                format!("could not get message with key: {:?}", key).as_str(),
            ));
        }
    }

    Ok(messages)
}

pub fn get_messages_page_by_prefix<F>(
    db: &RocksDB,
    prefix: &[u8],
    page_options: &PageOptions,
    filter: F,
) -> Result<MessagesPage, HubError>
where
    F: Fn(&MessageProto) -> bool,
{
    let mut messages_bytes = Vec::new();
    let mut last_key = vec![];

    db.for_each_iterator_by_prefix(Some(prefix.to_vec()), None, page_options, |key, value| {
        match message_decode(value) {
            Ok(message) => {
                if filter(&message) {
                    messages_bytes.push(value.to_vec());

                    if messages_bytes.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                        last_key = key.to_vec();
                        return Ok(true); // Stop iterating
                    }
                }

                Ok(false) // Continue iterating
            }
            Err(e) => Err(HubError::from(e)),
        }
    })?;

    let next_page_token = if last_key.len() > 0 {
        Some(last_key[prefix.len()..].to_vec())
    } else {
        None
    };

    Ok(MessagesPage {
        messages_bytes,
        next_page_token,
    })
}

pub fn message_encode(message: &MessageProto) -> Vec<u8> {
    if message.data_bytes.is_some() && message.data_bytes.as_ref().unwrap().len() > 0 {
        // Clone the message
        let mut cloned = message.clone();
        cloned.data = None;

        cloned.encode_to_vec()
    } else {
        message.encode_to_vec()
    }
}

pub fn message_decode(bytes: &[u8]) -> Result<MessageProto, RocksdbError> {
    if let Ok(mut msg) = MessageProto::decode(bytes) {
        if msg.data.is_none()
            && msg.data_bytes.is_some()
            && msg.data_bytes.as_ref().unwrap().len() > 0
        {
            if let Ok(msg_data) = MessageData::decode(msg.data_bytes.as_ref().unwrap().as_slice()) {
                msg.data = Some(msg_data);
            }
        }

        Ok(msg)
    } else {
        Err(RocksdbError::DecodeError)
    }
}

pub fn put_message_transaction(
    txn: &mut RocksDbTransactionBatch,
    message: &MessageProto,
) -> Result<(), HubError> {
    let ts_hash = make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;

    let primary_key = make_message_primary_key(
        message.data.as_ref().unwrap().fid as u32,
        type_to_set_postfix(MessageType::try_from(message.data.as_ref().unwrap().r#type).unwrap())
            as u8,
        Some(&ts_hash),
    );
    txn.put(primary_key, message_encode(&message));

    Ok(())
}

pub fn delete_message_transaction(
    txn: &mut RocksDbTransactionBatch,
    message: &MessageProto,
) -> Result<(), HubError> {
    let ts_hash = make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash)?;

    let primary_key = make_message_primary_key(
        message.data.as_ref().unwrap().fid as u32,
        type_to_set_postfix(MessageType::try_from(message.data.as_ref().unwrap().r#type).unwrap())
            as u8,
        Some(&ts_hash),
    );
    txn.delete(primary_key);

    Ok(())
}

pub fn is_message_in_time_range(
    start_time: Option<u32>,
    stop_time: Option<u32>,
    message: &MessageProto,
) -> bool {
    let start_time = start_time.unwrap_or(std::u32::MIN);
    let stop_time = stop_time.unwrap_or(std::u32::MAX);
    match &message.data {
        None => {
            // We expect all valid messages to have data
            return false;
        }
        Some(data) => return data.timestamp >= start_time && data.timestamp <= stop_time,
    };
}
