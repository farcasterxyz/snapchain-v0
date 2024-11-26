use crate::proto::msg::MessageType;
use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{CastStore, CastStoreDef, IntoU8, LinkStore, OnchainEventStorageError, OnchainEventStore, Store, StoreEventHandler};
use crate::storage::store::shard::ShardStore;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::TrieKey;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoresError {
    #[error(transparent)]
    OnchainEventError(#[from] OnchainEventStorageError),
}

#[derive(Clone)]
pub struct Stores {
    pub shard_store: ShardStore,
    pub cast_store: Store<CastStoreDef>,
    pub link_store: Store<LinkStore>,
    pub onchain_event_store: OnchainEventStore,
    db: Arc<RocksDB>,
    pub(crate) trie: merkle_trie::MerkleTrie,
    pub store_limits: StoreLimits,
}

#[derive(Clone)]
pub struct Limits {
    pub casts: u32,
    pub links: u32,
    pub reactions: u32,
    pub user_data: u32,
    pub user_name_proofs: u32,
    pub verifications: u32,
}

impl Limits {
    pub fn default() -> Limits {
        Limits {
            casts: 2000,
            links: 1000,
            reactions: 1000,
            user_data: 50,
            user_name_proofs: 5,
            verifications: 25,
        }
    }

    pub fn legacy() -> Limits {
        Limits {
            casts: 5000,
            links: 2500,
            reactions: 2500,
            user_data: 50,
            user_name_proofs: 5,
            verifications: 25,
        }
    }

    pub(crate) fn zero() -> Limits {
        Limits {
            casts: 0,
            links: 0,
            reactions: 0,
            user_data: 0,
            user_name_proofs: 0,
            verifications: 0,
        }
    }

    pub(crate) fn for_test() -> Limits {
        Limits {
            casts: 4,
            links: 4,
            reactions: 3,
            user_data: 4,
            user_name_proofs: 2,
            verifications: 2,
        }
    }

    fn for_message_type(&self, message_type: MessageType) -> u32 {
        match message_type {
            MessageType::CastAdd => self.casts,
            MessageType::CastRemove => self.casts,
            MessageType::ReactionAdd => self.reactions,
            MessageType::ReactionRemove => self.reactions,
            MessageType::LinkAdd => self.links,
            MessageType::LinkRemove => self.links,
            MessageType::LinkCompactState => self.links,
            MessageType::VerificationAddEthAddress => self.verifications,
            MessageType::VerificationRemove => self.verifications,
            MessageType::UserDataAdd => self.user_data,
            MessageType::UsernameProof => self.user_name_proofs,
            MessageType::FrameAction => 0,
            MessageType::None => 0,
        }
    }
}

#[derive(Clone)]
pub struct StoreLimits {
    pub limits: Limits,
    pub legacy_limits: Limits,
}

impl StoreLimits {
    pub fn max_messages(&self, units: u32, legacy_units: u32, message_type: MessageType) -> u32 {
        units * self.limits.for_message_type(message_type)
            + legacy_units * self.legacy_limits.for_message_type(message_type)
    }
}

impl StoreLimits {
    pub fn default() -> StoreLimits {
        StoreLimits {
            limits: Limits::default(),
            legacy_limits: Limits::legacy(),
        }
    }
}

impl Stores {
    pub fn new(db: Arc<RocksDB>, store_limits: StoreLimits) -> Stores {
        let mut trie = merkle_trie::MerkleTrie::new();
        trie.initialize(&db).unwrap();

        let event_handler = StoreEventHandler::new(None, None, None);
        let shard_store = ShardStore::new(db.clone());
        let cast_store = CastStore::new(db.clone(), event_handler.clone(), 100);
        let link_store = LinkStore::new(db.clone(), event_handler.clone(), 100);
        let onchain_event_store = OnchainEventStore::new(db.clone(), event_handler.clone());
        Stores {
            trie,
            shard_store,
            cast_store,
            link_store,
            onchain_event_store,
            db: db.clone(),
            store_limits,
        }
    }

    pub fn is_at_limit(
        &self,
        fid: u32,
        message_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<bool, StoresError> {
        let message_count = self.trie.get_count(
            &self.db,
            txn_batch,
            &TrieKey::for_message_type(fid, message_type.into_u8()),
        );
        let slot = self
            .onchain_event_store
            .get_storage_slot_for_fid(fid)
            .map_err(|e| StoresError::OnchainEventError(e))?;
        let max_messages =
            self.store_limits
                .max_messages(slot.units, slot.legacy_units, message_type);

        Ok(message_count >= max_messages as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = Limits::default();
        assert_eq!(limits.casts, 2000);
        assert_eq!(limits.links, 1000);
        assert_eq!(limits.reactions, 1000);
        assert_eq!(limits.user_data, 50);
        assert_eq!(limits.user_name_proofs, 5);
        assert_eq!(limits.verifications, 25);
    }

    #[test]
    fn test_legacy_limits() {
        let limits = Limits::legacy();
        assert_eq!(limits.casts, 5000);
        assert_eq!(limits.links, 2500);
        assert_eq!(limits.reactions, 2500);
        assert_eq!(limits.user_data, 50);
        assert_eq!(limits.user_name_proofs, 5);
        assert_eq!(limits.verifications, 25);
    }

    #[test]
    fn test_limit_for_message() {
        let limits = Limits::default();
        assert_eq!(limits.for_message_type(MessageType::CastAdd), 2000);
        assert_eq!(limits.for_message_type(MessageType::CastRemove), 2000);
        assert_eq!(limits.for_message_type(MessageType::ReactionAdd), 1000);
        assert_eq!(limits.for_message_type(MessageType::ReactionRemove), 1000);
        assert_eq!(limits.for_message_type(MessageType::LinkCompactState), 1000);
        assert_eq!(limits.for_message_type(MessageType::FrameAction), 0);
        assert_eq!(limits.for_message_type(MessageType::None), 0);
    }

    #[test]
    fn test_max_messages() {
        let store_limits = &StoreLimits::default();
        let legacy_limits = &store_limits.legacy_limits;
        let limits = &store_limits.limits;
        assert_eq!(
            store_limits.max_messages(1, 0, MessageType::CastAdd),
            limits.casts * 1
        );
        assert_eq!(
            store_limits.max_messages(0, 1, MessageType::CastAdd),
            legacy_limits.casts * 1
        );

        assert_eq!(
            store_limits.max_messages(3, 2, MessageType::ReactionRemove),
            (limits.links * 3) + (legacy_limits.links * 2)
        );

        assert_eq!(store_limits.max_messages(0, 0, MessageType::LinkAdd), 0);
    }
}
