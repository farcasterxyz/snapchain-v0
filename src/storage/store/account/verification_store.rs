use super::{
    make_fid_key, make_user_key,
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler, TS_HASH_LENGTH,
};
use crate::{
    core::error::HubError,
    proto::{Protocol, SignatureScheme, VerificationAddAddressBody, VerificationRemoveBody},
};
use crate::{proto::message_data::Body, storage::db::PageOptions};
use crate::{
    proto::MessageData,
    storage::constants::{RootPrefix, UserPostfix},
};
use crate::{
    proto::{Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct VerificationStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for VerificationStoreDef {
    fn postfix(&self) -> u8 {
        UserPostfix::VerificationMessage as u8
    }

    fn add_message_type(&self) -> u8 {
        MessageType::VerificationAddEthAddress as u8
    }

    fn remove_message_type(&self) -> u8 {
        MessageType::VerificationRemove as u8
    }

    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    fn is_compact_state_type(&self, _message: &Message) -> bool {
        false
    }

    fn is_add_type(&self, message: &Message) -> bool {
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type
                == MessageType::VerificationAddEthAddress as i32
            && message.data.as_ref().unwrap().body.is_some()
            && match message.data.as_ref().unwrap().body.as_ref().unwrap() {
                Body::VerificationAddAddressBody(body) => {
                    body.protocol == Protocol::Ethereum as i32
                        || body.protocol == Protocol::Solana as i32
                }
                _ => false,
            }
    }

    fn is_remove_type(&self, message: &Message) -> bool {
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::VerificationRemove as i32
            && message.data.as_ref().unwrap().body.is_some()
            && match message.data.as_ref().unwrap().body.as_ref().unwrap() {
                Body::VerificationRemoveBody(body) => {
                    body.protocol == Protocol::Ethereum as i32
                        || body.protocol == Protocol::Solana as i32
                }
                _ => false,
            }
    }

    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "address empty".to_string(),
                })
            }
        };

        if address.is_empty() {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "address empty".to_string(),
            });
        }

        // Puts the fid into the byAddress index
        let by_address_key = Self::make_verification_by_address_key(address);
        txn.put(
            by_address_key,
            make_fid_key(message.data.as_ref().unwrap().fid),
        );

        Ok(())
    }

    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "address empty".to_string(),
                })
            }
        };

        if address.is_empty() {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "address empty".to_string(),
            });
        }

        // Delete the message key from byAddress index
        let by_address_key = Self::make_verification_by_address_key(address);
        txn.delete(by_address_key);

        Ok(())
    }

    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            Body::VerificationRemoveBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid verification body".to_string(),
                })
            }
        };

        Ok(Self::make_verification_adds_key(
            message.data.as_ref().unwrap().fid,
            address,
        ))
    }

    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            Body::VerificationRemoveBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid verification body".to_string(),
                })
            }
        };

        Ok(Self::make_verification_removes_key(
            message.data.as_ref().unwrap().fid,
            address,
        ))
    }

    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Verification Store doesn't support compact state".to_string(),
        })
    }

    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Verification Store doesn't support compact state".to_string(),
        })
    }

    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl VerificationStoreDef {
    pub fn make_verification_by_address_key(address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + address.len());

        key.push(RootPrefix::VerificationByAddress as u8);
        key.extend_from_slice(address);
        key
    }

    pub fn make_verification_adds_key(fid: u64, address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + address.len());
        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::VerificationAdds as u8);
        key.extend_from_slice(address);
        key
    }

    pub fn make_verification_removes_key(fid: u64, address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + address.len());
        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::VerificationRemoves as u8);
        key.extend_from_slice(address);
        key
    }
}

pub struct VerificationStore {}

impl VerificationStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<VerificationStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            VerificationStoreDef { prune_size_limit },
        )
    }

    pub fn get_verification_add(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        address: &[u8],
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::VerificationAddEthAddress.into(),
                body: Some(Body::VerificationAddAddressBody(
                    VerificationAddAddressBody {
                        address: address.to_vec(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_add(&partial_message)
    }

    pub fn get_verification_remove(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        address: &[u8],
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::VerificationRemove.into(),
                body: Some(Body::VerificationRemoveBody(VerificationRemoveBody {
                    address: address.to_vec(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_remove(&partial_message)
    }

    pub fn get_verification_adds_by_fid(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    pub fn get_verification_removes_by_fid(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }
}
