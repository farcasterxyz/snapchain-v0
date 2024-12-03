use super::account::{IntoU8, OnchainEventStorageError};
use crate::core::error::HubError;
use crate::core::types::Height;
use crate::proto::hub_event::HubEvent;
use crate::proto::msg::Message;
use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
use crate::proto::{hub_event, msg as message, onchain_event, snapchain};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{CastStore, MessagesPage};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::store::BlockStore;
use crate::storage::trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use itertools::Itertools;
use merkle_trie::TrieKey;
use message::MessageType;
use snapchain::{Block, ShardChunk, Transaction};
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};
use trie::merkle_trie;

#[derive(Error, Debug)]
enum EngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error("store error")]
    StoreError {
        inner: HubError, // TODO: move away from HubError when we can
        hash: Vec<u8>,
    },

    #[error("unsupported message type")]
    UnsupportedMessageType(MessageType),

    #[error("unsupported event")]
    UnsupportedEvent,

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("message has no data")]
    NoMessageData,

    #[error("Unable to get usage count")]
    UsageCountError,

    #[error("invalid message type")]
    InvalidMessageType,

    #[error("missing fid")]
    MissingFid,

    #[error("missing signer")]
    MissingSigner,

    #[error("message receive error")]
    MessageReceiveError(#[from] mpsc::error::TryRecvError),

    #[error(transparent)]
    MergeOnchainEventError(#[from] OnchainEventStorageError),
}

impl EngineError {
    pub fn new_store_error(hash: Vec<u8>) -> impl FnOnce(HubError) -> Self {
        move |inner: HubError| EngineError::StoreError { inner, hash }
    }
}

#[derive(Clone)]
pub enum MempoolMessage {
    UserMessage(message::Message),
    ValidatorMessage(snapchain::ValidatorMessage),
}

impl MempoolMessage {
    pub fn fid(&self) -> u32 {
        match self {
            MempoolMessage::UserMessage(msg) => msg.fid(),
            MempoolMessage::ValidatorMessage(msg) => msg.fid(),
        }
    }
}

// Shard state root and the transactions
#[derive(Clone)]
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<Transaction>,
}

#[derive(Clone)]
pub struct Senders {
    pub messages_tx: mpsc::Sender<MempoolMessage>,
    pub events_tx: broadcast::Sender<HubEvent>,
}

impl Senders {
    pub fn new(messages_tx: mpsc::Sender<MempoolMessage>) -> Senders {
        let (events_tx, _events_rx) = broadcast::channel::<HubEvent>(100);
        Senders {
            events_tx,
            messages_tx,
        }
    }
}

struct TransactionCounts {
    transactions: u64,
    user_messages: u64,
    system_messages: u64,
}

pub struct ShardEngine {
    shard_id: u32,
    pub db: Arc<RocksDB>,
    senders: Senders,
    stores: Stores,
    messages_rx: mpsc::Receiver<MempoolMessage>,
    statsd_client: StatsdClientWrapper,
}

impl ShardEngine {
    pub fn new(
        db: Arc<RocksDB>,
        shard_id: u32,
        store_limits: StoreLimits,
        statsd_client: StatsdClientWrapper,
    ) -> ShardEngine {
        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.
        let (messages_tx, messages_rx) = mpsc::channel::<MempoolMessage>(10_000);
        ShardEngine {
            shard_id,
            stores: Stores::new(db.clone(), store_limits),
            senders: Senders::new(messages_tx),
            messages_rx,
            db,
            statsd_client,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<MempoolMessage> {
        self.senders.messages_tx.clone()
    }

    // statsd
    fn count(&self, key: &str, count: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .count_with_shard(self.shard_id, key.as_str(), count);
    }

    // statsd
    fn gauge(&self, key: &str, value: u64) {
        let key = format!("engine.{}", key);
        self.statsd_client
            .gauge_with_shard(self.shard_id, key.as_str(), value);
    }

    pub fn get_stores(&self) -> Stores {
        self.stores.clone()
    }

    pub fn get_senders(&self) -> Senders {
        self.senders.clone()
    }

    #[cfg(test)]
    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap()
    }

    fn prepare_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        shard_id: u32,
    ) -> Result<ShardStateChange, EngineError> {
        let mut messages = Vec::new();

        loop {
            match self.messages_rx.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(err) => return Err(EngineError::from(err)),
            }
        }

        self.count("prepare_proposal.recv_messages", messages.len() as u64);

        let mut snapchain_txns = self.create_transactions_from_mempool(messages)?;
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, _events) =
                self.replay_snapchain_txn(trie_ctx, &snapchain_txn, txn_batch)?;
            snapchain_txn.account_root = account_root;
        }

        let count = Self::txn_counts(&snapchain_txns);

        self.count("prepare_proposal.transactions", count.transactions);
        self.count("prepare_proposal.user_messages", count.user_messages);
        self.count("prepare_proposal.system_messages", count.system_messages);

        let new_root_hash = self.stores.trie.root_hash()?;
        let result = ShardStateChange {
            shard_id,
            new_state_root: new_root_hash.clone(),
            transactions: snapchain_txns,
        };

        Ok(result)
    }

    // Groups messages by fid and creates a transaction for each fid
    fn create_transactions_from_mempool(
        &mut self,
        messages: Vec<MempoolMessage>,
    ) -> Result<Vec<Transaction>, EngineError> {
        let mut transactions = vec![];

        let grouped_messages = messages.iter().into_group_map_by(|msg| msg.fid());
        let unique_fids = grouped_messages.keys().len();
        for (fid, messages) in grouped_messages {
            let mut transaction = Transaction {
                fid: fid as u64,
                account_root: vec![], // Starts empty, will be updated after replay
                system_messages: vec![],
                user_messages: vec![],
            };
            let storage_slot = self
                .stores
                .onchain_event_store
                .get_storage_slot_for_fid(fid)?;
            for msg in messages {
                match msg {
                    MempoolMessage::ValidatorMessage(msg) => {
                        transaction.system_messages.push(msg.clone());
                    }
                    MempoolMessage::UserMessage(msg) => {
                        // Only include messages for users that have storage
                        if storage_slot.is_active() {
                            transaction.user_messages.push(msg.clone());
                        }
                    }
                }
            }
            if !transaction.user_messages.is_empty() || !transaction.system_messages.is_empty() {
                transactions.push(transaction);
            }
        }
        info!(
            transactions = transactions.len(),
            messages = messages.len(),
            fids = unique_fids,
            "Created transactions from mempool"
        );
        Ok(transactions)
    }

    pub fn propose_state_change(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        shard: u32,
    ) -> ShardStateChange {
        let mut txn = RocksDbTransactionBatch::new();
        let result = self.prepare_proposal(trie_ctx, &mut txn, shard).unwrap(); //TODO: don't unwrap()

        // TODO: this should probably operate automatically via drop trait
        self.stores.trie.reload(&self.db).unwrap();

        self.count("propose.invoked", 1);
        result
    }

    fn replay_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
    ) -> Result<Vec<HubEvent>, EngineError> {
        let mut events = vec![];
        for snapchain_txn in transactions {
            let (account_root, txn_events) =
                self.replay_snapchain_txn(trie_ctx, snapchain_txn, txn_batch)?;
            // Reject early if account roots fail to match (shard roots will definitely fail)
            if &account_root != &snapchain_txn.account_root {
                warn!(
                    fid = snapchain_txn.fid,
                    new_account_root = hex::encode(&account_root),
                    tx_account_root = hex::encode(&snapchain_txn.account_root),
                    "Account root mismatch"
                );
                return Err(EngineError::HashMismatch);
            }
            events.extend(txn_events);
        }

        let root1 = self.stores.trie.root_hash()?;

        if &root1 != shard_root {
            warn!(
                shard_id = self.shard_id,
                new_shard_root = hex::encode(&root1),
                tx_shard_root = hex::encode(shard_root),
                "Shard root mismatch"
            );
            return Err(EngineError::HashMismatch);
        }

        Ok(events)
    }

    fn replay_snapchain_txn(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(Vec<u8>, Vec<HubEvent>), EngineError> {
        let total_user_messages = snapchain_txn.user_messages.len();
        let total_system_messages = snapchain_txn.system_messages.len();
        let mut user_messages_count = 0;
        let mut system_messages_count = 0;
        let mut onchain_events_count = 0;
        let mut merged_messages_count = 0;
        let mut pruned_messages_count = 0;
        let mut revoked_messages_count = 0;
        let mut events = vec![];
        let mut message_types = HashSet::new();
        let mut revoked_signers = HashSet::new();

        // System messages first, then user messages and finally prunes
        for msg in &snapchain_txn.system_messages {
            if let Some(onchain_event) = &msg.on_chain_event {
                let event = self
                    .stores
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch);

                match event {
                    Ok(hub_event) => {
                        onchain_events_count += 1;
                        self.update_trie(&merkle_trie::Context::new(), &hub_event, txn_batch)?;
                        events.push(hub_event.clone());
                        system_messages_count += 1;
                        match &onchain_event.body {
                            Some(onchain_event::on_chain_event::Body::SignerEventBody(
                                signer_event,
                            )) => {
                                if signer_event.event_type
                                    == onchain_event::SignerEventType::Remove as i32
                                {
                                    revoked_signers.insert(signer_event.key.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        warn!("Error merging onchain event: {:?}", err);
                    }
                }
            }
        }

        for key in revoked_signers {
            let result = self
                .stores
                .revoke_messages(snapchain_txn.fid as u32, &key, txn_batch);
            match result {
                Ok(revoke_events) => {
                    for event in revoke_events {
                        revoked_messages_count += 1;
                        self.update_trie(trie_ctx, &event, txn_batch)?;
                        events.push(event.clone());
                    }
                }
                Err(err) => {
                    warn!(
                        fid = snapchain_txn.fid,
                        key = hex::encode(key),
                        "Error revoking signer: {:?}",
                        err
                    );
                }
            }
        }

        for msg in &snapchain_txn.user_messages {
            // Errors are validated based on the shard root
            match self.validate_user_message(msg) {
                Ok(()) => {
                    let result = self.merge_message(msg, txn_batch);
                    match result {
                        Ok(event) => {
                            merged_messages_count += 1;
                            self.update_trie(trie_ctx, &event, txn_batch)?;
                            events.push(event.clone());
                            user_messages_count += 1;
                            message_types.insert(msg.msg_type());
                        }
                        Err(err) => {
                            warn!(
                                fid = msg.fid(),
                                hash = msg.hex_hash(),
                                "Error merging message: {:?}",
                                err
                            );
                        }
                    }
                }
                Err(err) => {
                    println!("Error validating user message {:#?}", err)
                }
            }
        }

        for msg_type in message_types {
            let fid = snapchain_txn.fid as u32;
            let result = self.prune_messages(fid, msg_type, txn_batch);
            match result {
                Ok(pruned_events) => {
                    for event in pruned_events {
                        pruned_messages_count += 1;
                        self.update_trie(trie_ctx, &event, txn_batch)?;
                        events.push(event.clone());
                    }
                }
                Err(err) => {
                    warn!(
                        fid = fid,
                        msg_type = msg_type.into_u8(),
                        "Error pruning messages: {:?}",
                        err
                    );
                }
            }
        }

        let account_root = self.stores.trie.get_hash(
            &self.db,
            txn_batch,
            &TrieKey::for_fid(snapchain_txn.fid as u32),
        );
        info!(
            fid = snapchain_txn.fid,
            num_user_messages = total_user_messages,
            num_system_messages = total_system_messages,
            user_messages_merged = user_messages_count,
            system_messages_merged = system_messages_count,
            onchain_events_merged = onchain_events_count,
            messages_merged = merged_messages_count,
            messages_pruned = pruned_messages_count,
            messages_revoked = revoked_messages_count,
            new_account_root = hex::encode(&account_root),
            tx_account_root = hex::encode(&snapchain_txn.account_root),
            "Replayed transaction"
        );

        // Return the new account root hash
        Ok((account_root, events))
    }

    fn merge_message(
        &mut self,
        msg: &message::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<hub_event::HubEvent, EngineError> {
        let data = msg.data.as_ref().ok_or(EngineError::NoMessageData)?;
        let mt = MessageType::try_from(data.r#type).or(Err(EngineError::InvalidMessageType))?;

        let event = match mt {
            MessageType::CastAdd | MessageType::CastRemove => self
                .stores
                .cast_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::LinkAdd | MessageType::LinkRemove | MessageType::LinkCompactState => self
                .stores
                .link_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::ReactionAdd | MessageType::ReactionRemove => self
                .stores
                .reaction_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::UserDataAdd => self
                .stores
                .user_data_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => self
                .stores
                .verification_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::UsernameProof => {
                let store = &self.stores.username_proof_store;
                let result = store.merge(msg, txn_batch);
                result.map_err(EngineError::new_store_error(msg.hash.clone()))
            }
            unhandled_type => {
                return Err(EngineError::UnsupportedMessageType(unhandled_type));
            }
        }?;

        Ok(event)
    }

    fn prune_messages(
        &mut self,
        fid: u32,
        msg_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, EngineError> {
        let (current_count, max_count) = self
            .stores
            .get_usage(fid, msg_type, txn_batch)
            .map_err(|_| EngineError::UsageCountError)?;
        println!(
            "Prune messages counts. Current count: {}, Max count: {}",
            current_count, max_count
        );

        let events = match msg_type {
            MessageType::CastAdd | MessageType::CastRemove => self
                .stores
                .cast_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(EngineError::new_store_error(vec![])),
            MessageType::LinkAdd | MessageType::LinkRemove | MessageType::LinkCompactState => self
                .stores
                .link_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(EngineError::new_store_error(vec![])),
            MessageType::ReactionAdd | MessageType::ReactionRemove => self
                .stores
                .reaction_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(EngineError::new_store_error(vec![])),
            MessageType::UserDataAdd => self
                .stores
                .user_data_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(EngineError::new_store_error(vec![])),
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => self
                .stores
                .verification_store
                .prune_messages(fid, current_count, max_count, txn_batch)
                .map_err(EngineError::new_store_error(vec![])),
            unhandled_type => {
                return Err(EngineError::UnsupportedMessageType(unhandled_type));
            }
        }?;

        info!(
            fid = fid,
            msg_type = msg_type.into_u8(),
            count = events.len(),
            "Pruned messages"
        );
        Ok(events)
    }

    fn update_trie(
        &mut self,
        ctx: &merkle_trie::Context,
        event: &hub_event::HubEvent,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), EngineError> {
        match &event.body {
            Some(hub_event::hub_event::Body::MergeMessageBody(merge)) => {
                if let Some(msg) = &merge.message {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
                for deleted_message in &merge.deleted_messages {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&deleted_message)],
                    )?;
                }
            }
            Some(hub_event::hub_event::Body::MergeOnChainEventBody(merge)) => {
                if let Some(onchain_event) = &merge.on_chain_event {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_onchain_event(&onchain_event)],
                    )?;
                }
            }
            Some(hub_event::hub_event::Body::PruneMessageBody(prune)) => {
                if let Some(msg) = &prune.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(hub_event::hub_event::Body::RevokeMessageBody(revoke)) => {
                if let Some(msg) = &revoke.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(hub_event::hub_event::Body::MergeUsernameProofBody(merge)) => {
                if let Some(msg) = &merge.username_proof_message {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
            }
            _ => {
                // TODO: This fallback case should not exist, every event should be handled
                return Err(EngineError::UnsupportedEvent);
            }
        }
        Ok(())
    }

    fn validate_user_message(&self, message: &message::Message) -> Result<(), EngineError> {
        // Ensure message data is present
        let message_data = message.data.as_ref().ok_or(EngineError::NoMessageData)?;

        // TODO(aditi): Check network

        // Check that the user has a custody address
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid as u32)?
            .ok_or(EngineError::MissingFid)?;

        // Check that signer is valid
        self.stores
            .onchain_event_store
            .get_active_signer(message_data.fid as u32, message.signer.clone())?
            .ok_or(EngineError::MissingSigner)?;

        Ok(())
    }

    pub fn validate_state_change(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        shard_state_change: &ShardStateChange,
    ) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let mut result = true;

        if let Err(err) = self.replay_proposal(trie_ctx, &mut txn, transactions, shard_root) {
            error!("State change validation failed: {}", err);
            result = false;
        }

        self.stores.trie.reload(&self.db).unwrap();

        if result {
            self.count("validate.true", 1);
            self.count("validate.false", 0);
        } else {
            self.count("validate.false", 1);
            self.count("validate.true", 0);
        }

        result
    }

    pub fn commit_and_emit_events(
        &mut self,
        shard_chunk: &ShardChunk,
        events: Vec<HubEvent>,
        txn: RocksDbTransactionBatch,
    ) {
        self.db.commit(txn).unwrap();
        for event in events {
            // An error here just means there are no active receivers, which is fine and will happen if there are no active subscribe rpcs
            let _ = self.senders.events_tx.send(event);
        }
        self.stores.trie.reload(&self.db).unwrap();

        _ = self.emit_commit_metrics(&shard_chunk);

        match self.stores.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
    }

    fn emit_commit_metrics(&mut self, shard_chunk: &&ShardChunk) -> Result<(), EngineError> {
        self.count("commit.invoked", 1);

        let block_number = &shard_chunk
            .header
            .as_ref()
            .unwrap()
            .height
            .unwrap()
            .block_number;

        self.gauge("block_height", *block_number);

        let trie_size = self.stores.trie.items()?;
        self.gauge("trie.num_items", trie_size as u64);

        let counts = Self::txn_counts(&shard_chunk.transactions);

        self.count("commit.transactions", counts.transactions);
        self.count("commit.user_messages", counts.user_messages);
        self.count("commit.system_messages", counts.system_messages);

        Ok(())
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: &ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        let trie_ctx = &merkle_trie::Context::new();
        match self.replay_proposal(trie_ctx, &mut txn, transactions, shard_root) {
            Err(err) => {
                error!("State change commit failed: {}", err);
                panic!("State change commit failed: {}", err);
            }
            Ok(events) => {
                self.commit_and_emit_events(shard_chunk, events, txn);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn trie_key_exists(
        &mut self,
        ctx: &merkle_trie::Context,
        sync_id: &Vec<u8>,
    ) -> bool {
        self.stores
            .trie
            .exists(ctx, &self.db, sync_id.as_ref())
            .unwrap_or_else(|err| {
                error!("Error checking if sync id exists: {:?}", err);
                false
            })
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.stores.shard_store.max_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            Err(_) => Height::new(self.shard_id, 0),
        }
    }

    pub fn get_last_shard_chunk(&self) -> Option<ShardChunk> {
        match self.stores.shard_store.get_last_shard_chunk() {
            Ok(shard_chunk) => shard_chunk,
            Err(err) => {
                error!("Unable to obtain last shard chunk {:#?}", err);
                None
            }
        }
    }

    pub fn get_casts_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        CastStore::get_cast_adds_by_fid(&self.stores.cast_store, fid, &PageOptions::default())
    }

    pub fn get_links_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_link_compact_state_messages_by_fid(
        &self,
        fid: u32,
    ) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_compact_state_messages_by_fid(fid, &PageOptions::default())
    }

    pub fn get_reactions_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        self.stores
            .reaction_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_user_data_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        self.stores
            .user_data_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_verifications_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        self.stores
            .verification_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_username_proofs_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        self.stores
            .username_proof_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u32,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        self.stores
            .onchain_event_store
            .get_onchain_events(event_type, fid)
    }

    fn txn_counts(txns: &[Transaction]) -> TransactionCounts {
        let (user_count, system_count) =
            txns.iter().fold((0, 0), |(user_count, system_count), tx| {
                (
                    user_count + tx.user_messages.len(),
                    system_count + tx.system_messages.len(),
                )
            });

        TransactionCounts {
            transactions: txns.len() as u64,
            user_messages: user_count as u64,
            system_messages: system_count as u64,
        }
    }
}

pub struct BlockEngine {
    block_store: BlockStore,
}

impl BlockEngine {
    pub fn new(block_store: BlockStore) -> Self {
        BlockEngine { block_store }
    }

    pub fn commit_block(&mut self, block: Block) {
        let result = self.block_store.put_block(block);
        if result.is_err() {
            error!("Failed to store block: {:?}", result.err());
        }
    }

    pub fn get_last_block(&self) -> Option<Block> {
        match self.block_store.get_last_block() {
            Ok(block) => block,
            Err(err) => {
                error!("Unable to obtain last block {:#?}", err);
                None
            }
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        match self.block_store.max_block_number() {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }
}
