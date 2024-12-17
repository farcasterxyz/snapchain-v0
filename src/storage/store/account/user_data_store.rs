use super::{
    is_message_in_time_range, make_user_key,
    name_registry_events::{
        delete_username_proof_transaction, get_fname_proof_by_fid, get_username_proof,
        put_username_proof_transaction,
    },
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler,
};

use crate::proto::{self};
use crate::{
    core::error::HubError,
    proto::{
        UserNameProof, {HubEvent, HubEventType, MergeUserNameProofBody},
        {SignatureScheme, UserDataBody},
    },
    storage::util::bytes_compare,
};
use crate::{proto::message_data::Body, storage::db::PageOptions};
use crate::{proto::MessageData, storage::constants::UserPostfix};
use crate::{
    proto::MessageType,
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct UserDataStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for UserDataStoreDef {
    fn postfix(&self) -> u8 {
        UserPostfix::UserDataMessage as u8
    }

    fn add_message_type(&self) -> u8 {
        MessageType::UserDataAdd as u8
    }

    fn remove_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    fn is_add_type(&self, message: &proto::Message) -> bool {
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::UserDataAdd as i32
            && message.data.as_ref().unwrap().body.is_some()
    }

    fn is_remove_type(&self, _message: &proto::Message) -> bool {
        false
    }

    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    fn is_compact_state_type(&self, _message: &proto::Message) -> bool {
        false
    }

    fn make_add_key(&self, message: &proto::Message) -> Result<Vec<u8>, HubError> {
        let user_data_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::UserDataBody(body) => body,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "UserDataAdd message missing body".to_string(),
                })
            }
        };

        let key = Self::make_user_data_adds_key(
            message.data.as_ref().unwrap().fid,
            user_data_body.r#type,
        );
        Ok(key)
    }

    fn make_remove_key(&self, _message: &proto::Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "removes not supported".to_string(),
        })
    }

    fn make_compact_state_add_key(&self, _message: &proto::Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "UserDataStore doesn't support compact state".to_string(),
        })
    }

    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "UserDataStore doesn't support compact state".to_string(),
        })
    }

    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl UserDataStoreDef {
    /**
     * Generates unique keys used to store or fetch UserDataAdd messages in the UserDataAdd set index
     *
     * @param fid farcaster id of the user who created the message
     * @param dataType type of data being added
     * @returns RocksDB key of the form <root_prefix>:<fid>:<user_postfix>:<dataType?>
     */
    fn make_user_data_adds_key(fid: u64, data_type: i32) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + 1);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::UserDataAdds as u8);
        if data_type > 0 {
            key.push(data_type as u8);
        }

        key
    }
}

pub struct UserDataStore {}

impl UserDataStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<UserDataStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            UserDataStoreDef { prune_size_limit },
        )
    }

    pub fn get_user_data_add(
        store: &Store<UserDataStoreDef>,
        fid: u64,
        r#type: i32,
    ) -> Result<Option<proto::Message>, HubError> {
        let partial_message = proto::Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::UserDataAdd as i32,
                body: Some(Body::UserDataBody(UserDataBody {
                    r#type,
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_add(&partial_message)
    }

    pub fn get_user_data_adds_by_fid(
        store: &Store<UserDataStoreDef>,
        fid: u64,
        page_options: &PageOptions,
        start_time: Option<u32>,
        stop_time: Option<u32>,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid(
            fid,
            page_options,
            Some(|message: &proto::Message| {
                return is_message_in_time_range(start_time, stop_time, message);
            }),
        )
    }

    pub fn get_user_data_by_fid_and_type(
        store: &Store<UserDataStoreDef>,
        fid: u64,
        user_data_type: proto::UserDataType,
    ) -> Result<proto::Message, HubError> {
        let result = store.get_adds_by_fid(
            fid,
            &PageOptions::default(),
            Some(|message: &proto::Message| {
                if let Some(user_data_body) = &message.data.as_ref().unwrap().body {
                    if let Body::UserDataBody(user_data) = user_data_body {
                        if user_data.r#type == user_data_type as i32 {
                            return true;
                        }
                    }
                }
                false
            }),
        );

        if result.is_ok() && result.as_ref().unwrap().messages.len() == 1 {
            let user_data_message = &result?.messages[0];
            Ok(user_data_message.clone())
        } else {
            Err(HubError {
                code: "not_found".to_string(),
                message: "user data not found".to_string(),
            })
        }
    }

    pub fn get_username_proof(
        store: &Store<UserDataStoreDef>,
        txn: &mut RocksDbTransactionBatch,
        name: &[u8],
    ) -> Result<Option<UserNameProof>, HubError> {
        get_username_proof(&store.db(), txn, name)
    }

    pub fn get_username_proof_by_fid(
        store: &Store<UserDataStoreDef>,
        fid: u64,
    ) -> Result<Option<UserNameProof>, HubError> {
        get_fname_proof_by_fid(&store.db(), fid)
    }

    pub fn merge_username_proof(
        store: &Store<UserDataStoreDef>,
        username_proof: &UserNameProof,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<HubEvent, HubError> {
        let existing_proof = get_username_proof(&store.db(), txn, &username_proof.name)?;
        let mut existing_fid: Option<u64> = None;

        if existing_proof.is_some() {
            let cmp =
                Self::username_proof_compare(existing_proof.as_ref().unwrap(), username_proof);

            if cmp == 0 {
                return Err(HubError {
                    code: "bad_request.duplicate".to_string(),
                    message: "username proof already exists".to_string(),
                });
            }
            if cmp > 0 {
                return Err(HubError {
                    code: "bad_request.conflict".to_string(),
                    message: "event conflicts with a more recent UserNameProof".to_string(),
                });
            }
            existing_fid = Some(existing_proof.as_ref().unwrap().fid);
        }

        if existing_proof.is_none() && username_proof.fid == 0 {
            return Err(HubError {
                code: "bad_request.conflict".to_string(),
                message: "proof does not exist".to_string(),
            });
        }

        if username_proof.fid == 0 {
            delete_username_proof_transaction(txn, username_proof, existing_fid);
        } else {
            put_username_proof_transaction(txn, username_proof);
        }

        let mut hub_event = HubEvent {
            r#type: HubEventType::MergeUsernameProof as i32,
            body: Some(proto::hub_event::Body::MergeUsernameProofBody(
                MergeUserNameProofBody {
                    username_proof: Some(username_proof.clone()),
                    deleted_username_proof: existing_proof,
                    username_proof_message: None,
                    deleted_username_proof_message: None,
                },
            )),
            id: 0,
        };
        let id = store
            .event_handler()
            .commit_transaction(txn, &mut hub_event)?;

        hub_event.id = id;
        Ok(hub_event)
    }

    fn username_proof_compare(a: &UserNameProof, b: &UserNameProof) -> i8 {
        if a.timestamp < b.timestamp {
            return -1;
        }
        if a.timestamp > b.timestamp {
            return 1;
        }

        bytes_compare(&a.signature, &b.signature)
    }
}
