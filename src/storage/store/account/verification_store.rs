use tracing::info;

use super::{
    make_fid_key, make_ts_hash, make_user_key, message_decode, read_fid_key,
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler, FID_BYTES, TS_HASH_LENGTH,
};
use crate::storage::util::increment_vec_u8;
use crate::{
    core::error::HubError,
    proto::{Protocol, SignatureScheme, VerificationAddAddressBody, VerificationRemoveBody},
    storage::store::account::delete_message_transaction,
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

    fn find_merge_add_conflicts(&self, _db: &RocksDB, _message: &Message) -> Result<(), HubError> {
        // For verifications, there will be no conflicts
        Ok(())
    }

    fn find_merge_remove_conflicts(
        &self,
        _db: &RocksDB,
        _message: &Message,
    ) -> Result<(), HubError> {
        // For verifications, there will be no conflicts
        Ok(())
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
            make_fid_key(message.data.as_ref().unwrap().fid as u32),
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

    fn delete_remove_secondary_indices(
        &self,
        _txn: &mut RocksDbTransactionBatch,
        _message: &Message,
    ) -> Result<(), HubError> {
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
            message.data.as_ref().unwrap().fid as u32,
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
            message.data.as_ref().unwrap().fid as u32,
            address,
        ))
    }

    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Verification Store doesn't support compact state".to_string(),
        })
    }

    fn make_compact_state_prefix(&self, _fid: u32) -> Result<Vec<u8>, HubError> {
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

    pub fn make_verification_adds_key(fid: u32, address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + address.len());
        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::VerificationAdds as u8);
        key.extend_from_slice(address);
        key
    }

    pub fn make_verification_removes_key(fid: u32, address: &[u8]) -> Vec<u8> {
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
        fid: u32,
        address: &[u8],
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid: fid as u64,
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
        fid: u32,
        address: &[u8],
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid: fid as u64,
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
        fid: u32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    pub fn get_verification_removes_by_fid(
        store: &Store<VerificationStoreDef>,
        fid: u32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    pub fn migrate_verifications(
        store: &Store<VerificationStoreDef>,
    ) -> Result<(u32, u32), HubError> {
        let mut verifications_count = 0;
        let mut duplicates_count = 0;
        let start_prefix = vec![RootPrefix::User as u8];

        store
            .db()
            .for_each_iterator_by_prefix(
                Some(start_prefix.to_vec()),
                Some(increment_vec_u8(&start_prefix)),
                &PageOptions::default(),
                |key, value| {
                    let postfix = key[1 + FID_BYTES];
                    if postfix != store.postfix() {
                        return Ok(false); // Ignore non-verification messages
                    }

                    let message = match message_decode(value) {
                        Ok(message) => message,
                        Err(_) => return Ok(false), // Ignore invalid messages
                    };

                    if !store.store_def().is_add_type(&message) {
                        return Ok(false); // Ignore non-add messages
                    }

                    let fid = message.data.as_ref().unwrap().fid as u32;
                    let verification_add =
                        match message.data.as_ref().unwrap().body.as_ref().unwrap() {
                            Body::VerificationAddAddressBody(body) => body,
                            _ => return Ok(false), // Ignore invalid messages
                        };
                    let address = &verification_add.address;

                    let mut txn = store.db().txn();
                    let by_address_key =
                        VerificationStoreDef::make_verification_by_address_key(address);
                    let existing_fid_res = match store.db().get(&by_address_key) {
                        Ok(Some(existing_fid)) => Ok(existing_fid),
                        _ => Err(HubError {
                            code: "not_found".to_string(),
                            message: "verification not found".to_string(),
                        }),
                    };

                    if existing_fid_res.is_ok() {
                        let existing_fid = read_fid_key(&existing_fid_res.unwrap());
                        let existing_message =
                            match Self::get_verification_add(store, existing_fid, address) {
                                Ok(Some(message)) => message,
                                _ => {
                                    return Err(HubError {
                                        code: "not_found".to_string(),
                                        message: "verification not found".to_string(),
                                    })
                                }
                            };

                        let ts_hash =
                            make_ts_hash(message.data.as_ref().unwrap().timestamp, &message.hash);
                        let existing_ts_hash = make_ts_hash(
                            existing_message.data.as_ref().unwrap().timestamp,
                            &existing_message.hash,
                        );

                        if ts_hash.is_err() || existing_ts_hash.is_err() {
                            return Err(HubError {
                                code: "bad_request".to_string(),
                                message: "failed to make tsHash".to_string(),
                            });
                        }

                        let message_compare = store.store_def().message_compare(
                            store.store_def().add_message_type(),
                            &existing_ts_hash.unwrap().to_vec(),
                            store.store_def().add_message_type(),
                            &ts_hash.unwrap().to_vec(),
                        );

                        if message_compare == 0 {
                            info!(
                                "Unexpected duplicate during migration: {} {:x?}",
                                fid, address
                            );
                        } else if message_compare > 0 {
                            info!(
                                "Deleting duplicate verification for fid: {} {:x?}",
                                fid, address
                            );

                            delete_message_transaction(&mut txn, &message)?;
                            txn.put(by_address_key, make_fid_key(existing_fid));
                            duplicates_count += 1;
                        } else {
                            info!(
                                "Deleting duplicate verification for fid: {} {:x?}",
                                existing_fid, address
                            );

                            delete_message_transaction(&mut txn, &existing_message)?;
                            txn.put(by_address_key, make_fid_key(fid));
                            duplicates_count += 1;
                        }
                    } else {
                        txn.put(by_address_key, make_fid_key(fid));
                    }

                    verifications_count += 1;
                    store.db().commit(txn)?;

                    Ok(false) // Continue iterating
                },
            )
            .unwrap();

        Ok((verifications_count, duplicates_count))
    }
}
