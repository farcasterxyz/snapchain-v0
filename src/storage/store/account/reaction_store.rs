use super::{
    get_many_messages, make_cast_id_key, make_fid_key, make_message_primary_key, make_user_key,
    read_fid_key, read_ts_hash,
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler, PAGE_SIZE_MAX, TS_HASH_LENGTH,
};
use crate::{core::error::HubError, proto::SignatureScheme};
use crate::{proto::message_data::Body, storage::db::PageOptions};
use crate::{
    proto::MessageData,
    storage::constants::{RootPrefix, UserPostfix},
};
use crate::{
    proto::{reaction_body::Target, ReactionBody, ReactionType},
    storage::util::increment_vec_u8,
};
use crate::{
    proto::{Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::{borrow::Borrow, sync::Arc};

#[derive(Clone)]
pub struct ReactionStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for ReactionStoreDef {
    fn postfix(&self) -> u8 {
        UserPostfix::ReactionMessage.as_u8()
    }

    fn add_message_type(&self) -> u8 {
        MessageType::ReactionAdd as u8
    }

    fn remove_message_type(&self) -> u8 {
        MessageType::ReactionRemove as u8
    }

    fn is_add_type(&self, message: &Message) -> bool {
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::ReactionAdd as i32
            && message.data.as_ref().unwrap().body.is_some()
    }

    fn is_remove_type(&self, message: &Message) -> bool {
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && message.data.is_some()
            && message.data.as_ref().unwrap().r#type == MessageType::ReactionRemove as i32
            && message.data.as_ref().unwrap().body.is_some()
    }

    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    fn is_compact_state_type(&self, _message: &Message) -> bool {
        false
    }

    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let (by_target_key, rtype) = self.secondary_index_key(ts_hash, message)?;

        txn.put(by_target_key, vec![rtype]);

        Ok(())
    }

    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let (by_target_key, _) = self.secondary_index_key(ts_hash, message)?;

        txn.delete(by_target_key);

        Ok(())
    }

    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid reaction body".to_string(),
                })
            }
        };

        Self::make_reaction_adds_key(
            message.data.as_ref().unwrap().fid,
            reaction_body.r#type,
            reaction_body.target.as_ref(),
        )
    }

    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid reaction body".to_string(),
                })
            }
        };

        Self::make_reaction_removes_key(
            message.data.as_ref().unwrap().fid,
            reaction_body.r#type,
            reaction_body.target.as_ref(),
        )
    }

    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Reaction Store doesn't support compact state".to_string(),
        })
    }

    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Reaction Store doesn't support compact state".to_string(),
        })
    }

    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl ReactionStoreDef {
    fn secondary_index_key(
        &self,
        ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(Vec<u8>, u8), HubError> {
        // Make sure at least one of targetCastId or targetUrl is set
        let reaction_body = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::ReactionBody(reaction_body) => reaction_body,
            _ => Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "Invalid reaction body".to_string(),
            })?,
        };
        let target = reaction_body.target.as_ref().ok_or(HubError {
            code: "bad_request.validation_failure".to_string(),
            message: "Invalid reaction body".to_string(),
        })?;

        let by_target_key = ReactionStoreDef::make_reactions_by_target_key(
            target,
            message.data.as_ref().unwrap().fid,
            Some(ts_hash),
        );

        Ok((by_target_key, reaction_body.r#type as u8))
    }

    pub fn make_reactions_by_target_key(
        target: &Target,
        fid: u64,
        ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 28 + 24 + 4);

        key.push(RootPrefix::ReactionsByTarget as u8); // ReactionsByTarget prefix, 1 byte
        key.extend_from_slice(&Self::make_target_key(target));
        if ts_hash.is_some() && ts_hash.unwrap().len() == TS_HASH_LENGTH {
            key.extend_from_slice(ts_hash.unwrap());
        }
        if fid > 0 {
            key.extend_from_slice(&make_fid_key(fid));
        }

        key
    }

    pub fn make_target_key(target: &Target) -> Vec<u8> {
        match target {
            Target::TargetUrl(url) => url.as_bytes().to_vec(),
            Target::TargetCastId(cast_id) => make_cast_id_key(cast_id),
        }
    }

    pub fn make_reaction_adds_key(
        fid: u64,
        r#type: i32,
        target: Option<&Target>,
    ) -> Result<Vec<u8>, HubError> {
        if target.is_some() && r#type == 0 {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "targetId provided without type".to_string(),
            });
        }
        let mut key = Vec::with_capacity(33 + 1 + 1 + 28);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::ReactionAdds as u8); // ReactionAdds postfix, 1 byte
        if r#type > 0 {
            key.push(r#type as u8); // type, 1 byte
        }
        if target.is_some() {
            // target, 28 bytes
            key.extend_from_slice(&Self::make_target_key(target.unwrap()));
        }

        Ok(key)
    }

    pub fn make_reaction_removes_key(
        fid: u64,
        r#type: i32,
        target: Option<&Target>,
    ) -> Result<Vec<u8>, HubError> {
        if target.is_some() && r#type == 0 {
            return Err(HubError {
                code: "bad_request.validation_failure".to_string(),
                message: "targetId provided without type".to_string(),
            });
        }
        let mut key = Vec::with_capacity(33 + 1 + 1 + 28);

        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::ReactionRemoves as u8); // ReactionRemoves postfix, 1 byte
        if r#type > 0 {
            key.push(r#type as u8); // type, 1 byte
        }
        if target.is_some() {
            key.extend_from_slice(&Self::make_target_key(target.unwrap()));
            // target, 28 bytes
        }

        Ok(key)
    }
}

pub struct ReactionStore {}

impl ReactionStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<ReactionStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            ReactionStoreDef { prune_size_limit },
        )
    }

    pub fn get_reaction_add(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        r#type: i32,
        target: Option<Target>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::ReactionAdd.into(),
                body: Some(Body::ReactionBody(ReactionBody {
                    r#type,
                    target: target.clone(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_add(&partial_message)
    }

    pub fn get_reaction_remove(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        r#type: i32,
        target: Option<Target>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::ReactionRemove.into(),
                body: Some(Body::ReactionBody(ReactionBody {
                    r#type,
                    target: target.clone(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        let r = store.get_remove(&partial_message);
        // println!("got reaction remove: {:?}", r);

        r
    }

    pub fn get_reaction_adds_by_fid(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid(
            fid,
            page_options,
            Some(|message: &Message| {
                if let Some(reaction_body) = &message.data.as_ref().unwrap().body {
                    if let Body::ReactionBody(reaction_body) = reaction_body {
                        if reaction_type == 0 || reaction_body.r#type == reaction_type {
                            return true;
                        }
                    }
                }

                false
            }),
        )
    }

    pub fn get_reaction_removes_by_fid(
        store: &Store<ReactionStoreDef>,
        fid: u64,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid(
            fid,
            page_options,
            Some(|message: &Message| {
                if let Some(reaction_body) = &message.data.as_ref().unwrap().body {
                    if let Body::ReactionBody(reaction_body) = reaction_body {
                        if reaction_type == 0 || reaction_body.r#type == reaction_type {
                            return true;
                        }
                    }
                }

                false
            }),
        )
    }

    pub fn get_reactions_by_target(
        store: &Store<ReactionStoreDef>,
        target: &Target,
        reaction_type: i32,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        let start_prefix = ReactionStoreDef::make_reactions_by_target_key(target, 0, None);

        let mut message_keys = vec![];
        let mut last_key = vec![];

        store.db().for_each_iterator_by_prefix(
            Some(start_prefix.to_vec()),
            Some(increment_vec_u8(&start_prefix)),
            page_options,
            |key, value| {
                if reaction_type == ReactionType::None as i32
                    || (value.len() == 1 && value[0] == reaction_type as u8)
                {
                    let ts_hash_offset = start_prefix.len();
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
                }

                Ok(false) // Continue iterating
            },
        )?;

        let messages = get_many_messages(store.db().borrow(), message_keys)?;
        let next_page_token = if last_key.len() > 0 {
            Some(last_key.to_vec())
        } else {
            None
        };

        Ok(MessagesPage {
            messages,
            next_page_token,
        })
    }
}
