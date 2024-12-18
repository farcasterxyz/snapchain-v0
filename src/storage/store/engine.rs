use super::account::{IntoU8, OnchainEventStorageError, UserDataStore};
use crate::core::error::HubError;
use crate::core::types::Height;
use crate::core::validations;
use crate::proto::HubEvent;
use crate::proto::Message;
use crate::proto::UserNameProof;
use crate::proto::{self, Block, MessageType, ShardChunk, Transaction};
use crate::proto::{OnChainEvent, OnChainEventType};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{CastStore, MessagesPage};
use crate::storage::store::stores::{StoreLimits, Stores};
use crate::storage::store::BlockStore;
use crate::storage::trie;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use itertools::Itertools;
use merkle_trie::TrieKey;
use std::collections::HashSet;
use std::str;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    TrieError(#[from] trie::errors::TrieError),

    #[error("store error")]
    StoreError {
        inner: HubError, // TODO: move away from HubError when we can
        hash: Vec<u8>,
    },

    #[error("unsupported message type")]
    UnsupportedMessageType(MessageType),

    #[error("merkle trie root hash mismatch")]
    HashMismatch,

    #[error("Unable to get usage count")]
    UsageCountError,

    #[error("message receive error")]
    MessageReceiveError(#[from] mpsc::error::TryRecvError),

    #[error(transparent)]
    MergeOnchainEventError(#[from] OnchainEventStorageError),

    #[error(transparent)]
    EngineMessageValidationError(#[from] MessageValidationError),
}

#[derive(Error, Debug, Clone)]
pub enum MessageValidationError {
    #[error("message has no data")]
    NoMessageData,

    #[error("missing fid")]
    MissingFid,

    #[error("missing signer")]
    MissingSigner,

    #[error(transparent)]
    MessageValidationError(#[from] validations::ValidationError),

    #[error("invalid message type")]
    InvalidMessageType(i32),

    #[error("store error")]
    StoreError { inner: HubError, hash: Vec<u8> },

    #[error("fname not registered for fid")]
    MissingFname,
}

impl MessageValidationError {
    pub fn new_store_error(hash: Vec<u8>) -> impl FnOnce(HubError) -> Self {
        move |inner: HubError| MessageValidationError::StoreError { inner, hash }
    }
}

impl EngineError {
    pub fn new_store_error(hash: Vec<u8>) -> impl FnOnce(HubError) -> Self {
        move |inner: HubError| EngineError::StoreError { inner, hash }
    }
}

#[derive(Clone)]
pub enum MempoolMessage {
    UserMessage(proto::Message),
    ValidatorMessage(proto::ValidatorMessage),
}

impl MempoolMessage {
    pub fn fid(&self) -> u64 {
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
    max_messages_per_block: u32,
}

impl ShardEngine {
    pub fn new(
        db: Arc<RocksDB>,
        trie: merkle_trie::MerkleTrie,
        shard_id: u32,
        store_limits: StoreLimits,
        statsd_client: StatsdClientWrapper,
        max_messages_per_block: u32,
        mempool_queue_size: u32,
    ) -> ShardEngine {
        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.
        let (messages_tx, messages_rx) =
            mpsc::channel::<MempoolMessage>(mempool_queue_size as usize);
        ShardEngine {
            shard_id,
            stores: Stores::new(db.clone(), trie, store_limits),
            senders: Senders::new(messages_tx),
            messages_rx,
            db,
            statsd_client,
            max_messages_per_block,
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

    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.stores.trie.root_hash().unwrap()
    }

    pub(crate) async fn pull_messages(
        &mut self,
        max_wait: Duration,
    ) -> Result<Vec<MempoolMessage>, EngineError> {
        let mut messages = Vec::new();
        let start_time = Instant::now();

        loop {
            if start_time.elapsed() >= max_wait {
                break;
            }

            while messages.len() < self.max_messages_per_block as usize {
                match self.messages_rx.try_recv() {
                    Ok(msg) => messages.push(msg),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(err) => return Err(EngineError::from(err)),
                }
            }

            if messages.len() >= self.max_messages_per_block as usize {
                break;
            }

            sleep(Duration::from_millis(5)).await;
        }

        Ok(messages)
    }

    fn prepare_proposal(
        &mut self,
        trie_ctx: &merkle_trie::Context,
        txn_batch: &mut RocksDbTransactionBatch,
        shard_id: u32,
        messages: Vec<MempoolMessage>,
    ) -> Result<ShardStateChange, EngineError> {
        self.count("prepare_proposal.recv_messages", messages.len() as u64);

        let mut snapchain_txns = self.create_transactions_from_mempool(messages)?;
        let mut validation_error_count = 0;
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, _events, validation_errors) = self.replay_snapchain_txn(
                trie_ctx,
                &snapchain_txn,
                txn_batch,
                "prepare_proposal".to_string(),
            )?;
            snapchain_txn.account_root = account_root;
            validation_error_count += validation_errors.len();
        }

        let count = Self::txn_counts(&snapchain_txns);

        self.count("prepare_proposal.transactions", count.transactions);
        self.count("prepare_proposal.user_messages", count.user_messages);
        self.count("prepare_proposal.system_messages", count.system_messages);
        self.count(
            "prepare_proposal.validation_errors",
            validation_error_count as u64,
        );

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
        shard: u32,
        messages: Vec<MempoolMessage>,
    ) -> ShardStateChange {
        let mut txn = RocksDbTransactionBatch::new();

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_propose", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_propose", read_count.1);
        };

        let result = self
            .prepare_proposal(
                &merkle_trie::Context::with_callback(count_callback),
                &mut txn,
                shard,
                messages,
            )
            .unwrap(); //TODO: don't unwrap()

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
        source: String,
    ) -> Result<Vec<HubEvent>, EngineError> {
        let mut events = vec![];
        for snapchain_txn in transactions {
            let (account_root, txn_events, _) =
                self.replay_snapchain_txn(trie_ctx, snapchain_txn, txn_batch, source.clone())?;
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
        source: String,
    ) -> Result<(Vec<u8>, Vec<HubEvent>, Vec<MessageValidationError>), EngineError> {
        let total_user_messages = snapchain_txn.user_messages.len();
        let total_system_messages = snapchain_txn.system_messages.len();
        let mut user_messages_count = 0;
        let mut system_messages_count = 0;
        let mut onchain_events_count = 0;
        let mut merged_fnames_count = 0;
        let mut merged_messages_count = 0;
        let mut pruned_messages_count = 0;
        let mut revoked_messages_count = 0;
        let mut events = vec![];
        let mut message_types = HashSet::new();
        let mut revoked_signers = HashSet::new();

        let mut validation_errors = vec![];

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
                        self.update_trie(trie_ctx, &hub_event, txn_batch)?;
                        events.push(hub_event.clone());
                        system_messages_count += 1;
                        match &onchain_event.body {
                            Some(proto::on_chain_event::Body::SignerEventBody(signer_event)) => {
                                if signer_event.event_type == proto::SignerEventType::Remove as i32
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
            if let Some(fname_transfer) = &msg.fname_transfer {
                if fname_transfer.proof.is_none() {
                    warn!(
                        fid = snapchain_txn.fid,
                        id = fname_transfer.id,
                        "Fname transfer has no proof"
                    );
                }

                match fname_transfer.verify_signature() {
                    Ok(_) => {}
                    Err(err) => {
                        warn!("Error validating fname transfer: {:?}", err);
                    }
                }

                let event = UserDataStore::merge_username_proof(
                    &self.stores.user_data_store,
                    fname_transfer.proof.as_ref().unwrap(),
                    txn_batch,
                );
                match event {
                    Ok(hub_event) => {
                        merged_fnames_count += 1;
                        self.update_trie(&merkle_trie::Context::new(), &hub_event, txn_batch)?;
                        events.push(hub_event.clone());
                        system_messages_count += 1;
                    }
                    Err(err) => {
                        warn!("Error merging fname transfer: {:?}", err);
                    }
                }
                // If the name was transfered from an existing fid, we need to make sure to revoke any existing username UserDataAdd
                if fname_transfer.from_fid > 0 {
                    let existing_username = self.get_user_data_by_fid_and_type(
                        fname_transfer.from_fid,
                        proto::UserDataType::Username,
                    );
                    if existing_username.is_ok() {
                        let existing_username = existing_username.unwrap();
                        let event = self
                            .stores
                            .user_data_store
                            .revoke(&existing_username, txn_batch);
                        match event {
                            Ok(hub_event) => {
                                revoked_messages_count += 1;
                                self.update_trie(
                                    &merkle_trie::Context::new(),
                                    &hub_event,
                                    txn_batch,
                                )?;
                                events.push(hub_event.clone());
                            }
                            Err(err) => {
                                warn!("Error revoking existing username: {:?}", err);
                            }
                        }
                    }
                }
            }
        }

        for key in revoked_signers {
            let result = self
                .stores
                .revoke_messages(snapchain_txn.fid, &key, txn_batch);
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
            match self.validate_user_message(msg, txn_batch) {
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
                    warn!(
                        fid = snapchain_txn.fid,
                        hash = msg.hex_hash(),
                        "Error validating user message: {:?}",
                        err
                    );
                    validation_errors.push(err);
                }
            }
        }

        for msg_type in message_types {
            let fid = snapchain_txn.fid;
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

        let account_root =
            self.stores
                .trie
                .get_hash(&self.db, txn_batch, &TrieKey::for_fid(snapchain_txn.fid));
        info!(
            fid = snapchain_txn.fid,
            num_user_messages = total_user_messages,
            num_system_messages = total_system_messages,
            user_messages_merged = user_messages_count,
            system_messages_merged = system_messages_count,
            onchain_events_merged = onchain_events_count,
            fnames_merged = merged_fnames_count,
            messages_merged = merged_messages_count,
            messages_pruned = pruned_messages_count,
            messages_revoked = revoked_messages_count,
            validation_errors = validation_errors.len(),
            new_account_root = hex::encode(&account_root),
            tx_account_root = hex::encode(&snapchain_txn.account_root),
            source,
            "Replayed transaction"
        );

        // Return the new account root hash
        Ok((account_root, events, validation_errors))
    }

    fn merge_message(
        &mut self,
        msg: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<proto::HubEvent, MessageValidationError> {
        let data = msg
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;
        let mt = MessageType::try_from(data.r#type)
            .or(Err(MessageValidationError::InvalidMessageType(data.r#type)))?;

        let event = match mt {
            MessageType::CastAdd | MessageType::CastRemove => self
                .stores
                .cast_store
                .merge(msg, txn_batch)
                .map_err(MessageValidationError::new_store_error(msg.hash.clone())),
            MessageType::LinkAdd | MessageType::LinkRemove | MessageType::LinkCompactState => self
                .stores
                .link_store
                .merge(msg, txn_batch)
                .map_err(MessageValidationError::new_store_error(msg.hash.clone())),
            MessageType::ReactionAdd | MessageType::ReactionRemove => self
                .stores
                .reaction_store
                .merge(msg, txn_batch)
                .map_err(MessageValidationError::new_store_error(msg.hash.clone())),
            MessageType::UserDataAdd => self
                .stores
                .user_data_store
                .merge(msg, txn_batch)
                .map_err(MessageValidationError::new_store_error(msg.hash.clone())),
            MessageType::VerificationAddEthAddress | MessageType::VerificationRemove => self
                .stores
                .verification_store
                .merge(msg, txn_batch)
                .map_err(MessageValidationError::new_store_error(msg.hash.clone())),
            MessageType::UsernameProof => {
                let store = &self.stores.username_proof_store;
                let result = store.merge(msg, txn_batch);
                result.map_err(MessageValidationError::new_store_error(msg.hash.clone()))
            }
            unhandled_type => {
                return Err(MessageValidationError::InvalidMessageType(
                    unhandled_type as i32,
                ));
            }
        }?;

        Ok(event)
    }

    fn prune_messages(
        &mut self,
        fid: u64,
        msg_type: MessageType,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<Vec<HubEvent>, EngineError> {
        let (current_count, max_count) = self
            .stores
            .get_usage(fid, msg_type, txn_batch)
            .map_err(|_| EngineError::UsageCountError)?;

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

        if !events.is_empty() {
            info!(
                fid = fid,
                msg_type = msg_type.into_u8(),
                count = events.len(),
                "Pruned messages"
            );
        }
        Ok(events)
    }

    fn update_trie(
        &mut self,
        ctx: &merkle_trie::Context,
        event: &proto::HubEvent,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), EngineError> {
        match &event.body {
            Some(proto::hub_event::Body::MergeMessageBody(merge)) => {
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
            Some(proto::hub_event::Body::MergeOnChainEventBody(merge)) => {
                if let Some(onchain_event) = &merge.on_chain_event {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_onchain_event(&onchain_event)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::PruneMessageBody(prune)) => {
                if let Some(msg) = &prune.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::RevokeMessageBody(revoke)) => {
                if let Some(msg) = &revoke.message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
            }
            Some(proto::hub_event::Body::MergeUsernameProofBody(merge)) => {
                if let Some(msg) = &merge.username_proof_message {
                    self.stores.trie.insert(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
                if let Some(msg) = &merge.deleted_username_proof_message {
                    self.stores.trie.delete(
                        ctx,
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&msg)],
                    )?;
                }
                if let Some(proof) = &merge.username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32 {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        self.stores.trie.insert(
                            ctx,
                            &self.db,
                            txn_batch,
                            vec![TrieKey::for_fname(proof.fid, &name)],
                        )?;
                    }
                }
                if let Some(proof) = &merge.deleted_username_proof {
                    if proof.r#type == proto::UserNameType::UsernameTypeFname as i32 {
                        let name = str::from_utf8(&proof.name).unwrap().to_string();
                        self.stores.trie.delete(
                            ctx,
                            &self.db,
                            txn_batch,
                            vec![TrieKey::for_fname(proof.fid, &name)],
                        )?;
                    }
                }
            }
            &None => {
                // This should never happen
                panic!("No body in event");
            }
        }
        Ok(())
    }

    pub(crate) fn validate_user_message(
        &self,
        message: &proto::Message,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        // Ensure message data is present
        let message_data = message
            .data
            .as_ref()
            .ok_or(MessageValidationError::NoMessageData)?;

        // TODO(aditi): Check network

        validations::validate_message(message)?;

        // Check that the user has a custody address
        self.stores
            .onchain_event_store
            .get_id_register_event_by_fid(message_data.fid)
            .map_err(|_| MessageValidationError::MissingFid)?
            .ok_or(MessageValidationError::MissingFid)?;

        // Check that signer is valid
        self.stores
            .onchain_event_store
            .get_active_signer(message_data.fid, message.signer.clone())
            .map_err(|_| MessageValidationError::MissingSigner)?
            .ok_or(MessageValidationError::MissingSigner)?;

        match &message_data.body {
            Some(proto::message_data::Body::UserDataBody(user_data)) => {
                if user_data.r#type == proto::UserDataType::Username as i32 {
                    self.validate_username(message_data.fid, &user_data.value, txn_batch)?;
                }
            }
            Some(proto::message_data::Body::UsernameProofBody(_)) => {
                // Validate ens
            }
            Some(proto::message_data::Body::VerificationAddAddressBody(__add)) => {
                // Validate verification
            }
            Some(proto::message_data::Body::LinkCompactStateBody(_)) => {
                // Validate link state length
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_username(
        &self,
        fid: u64,
        fname: &str,
        txn: &mut RocksDbTransactionBatch,
    ) -> Result<(), MessageValidationError> {
        if fname.is_empty() {
            // Setting an empty username is allowed, no need to validate the proof
            return Ok(());
        }
        let fname = fname.to_string();
        // TODO: validate fname string

        if fname.ends_with(".eth") {
            // TODO: Validate ens names
        } else {
            let proof = UserDataStore::get_username_proof(
                &self.stores.user_data_store,
                txn,
                fname.as_bytes(),
            )
            .map_err(|e| MessageValidationError::StoreError {
                inner: e,
                hash: vec![],
            })?;
            match proof {
                Some(proof) => {
                    if proof.fid != fid {
                        return Err(MessageValidationError::MissingFname);
                    }
                }
                None => {
                    return Err(MessageValidationError::MissingFname);
                }
            }
        }
        Ok(())
    }

    pub fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let mut result = true;

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_validate", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_validate", read_count.1);
        };

        if let Err(err) = self.replay_proposal(
            &merkle_trie::Context::with_callback(count_callback),
            &mut txn,
            transactions,
            shard_root,
            "validate_state_change".to_string(),
        ) {
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

        // useful to see on perf test dashboards
        self.gauge(
            "trie.branching_factor",
            self.stores.trie.branching_factor() as u64,
        );
        self.gauge("max_messages_per_block", self.max_messages_per_block as u64);

        Ok(())
    }

    pub fn make_count_fn(statsd_client: StatsdClientWrapper, shard_id: u32) -> impl Fn(&str, u64) {
        move |key: &str, count: u64| {
            let key = format!("engine.{}", key);
            statsd_client.count_with_shard(shard_id, &key, count);
        }
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: &ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        let count_fn = Self::make_count_fn(self.statsd_client.clone(), self.shard_id);
        let count_callback = move |read_count: (u64, u64)| {
            count_fn("trie.db_get_count.total", read_count.0);
            count_fn("trie.db_get_count.for_commit", read_count.0);
            count_fn("trie.mem_get_count.total", read_count.1);
            count_fn("trie.mem_get_count.for_commit", read_count.1);
        };
        let trie_ctx = &merkle_trie::Context::with_callback(count_callback);

        match self.replay_proposal(
            trie_ctx,
            &mut txn,
            transactions,
            shard_root,
            "commit".to_string(),
        ) {
            Err(err) => {
                error!("State change commit failed: {}", err);
                panic!("State change commit failed: {}", err);
            }
            Ok(events) => {
                self.commit_and_emit_events(shard_chunk, events, txn);
            }
        }
    }

    pub fn simulate_message(&mut self, message: &Message) -> Result<(), MessageValidationError> {
        let mut txn = RocksDbTransactionBatch::new();
        let snapchain_txn = Transaction {
            fid: message.fid() as u64,
            account_root: vec![],
            system_messages: vec![],
            user_messages: vec![message.clone()],
        };
        let result = self.replay_snapchain_txn(
            &merkle_trie::Context::new(),
            &snapchain_txn,
            &mut txn,
            "simulate_message".to_string(),
        );

        match result {
            Ok((_, _, errors)) => {
                self.stores.trie.reload(&self.db).map_err(|e| {
                    MessageValidationError::StoreError {
                        inner: HubError::invalid_internal_state(&*e.to_string()),
                        hash: vec![],
                    }
                })?;
                if !errors.is_empty() {
                    return Err(errors[0].clone());
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                error!("Error simulating message: {:?}", err);
                Err(MessageValidationError::StoreError {
                    inner: HubError::invalid_internal_state(&*err.to_string()),
                    hash: vec![],
                })
            }
        }
    }

    pub fn trie_key_exists(&mut self, ctx: &merkle_trie::Context, sync_id: &Vec<u8>) -> bool {
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

    pub fn get_casts_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        CastStore::get_cast_adds_by_fid(&self.stores.cast_store, fid, &PageOptions::default())
    }

    pub fn get_links_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_link_compact_state_messages_by_fid(
        &self,
        fid: u64,
    ) -> Result<MessagesPage, HubError> {
        self.stores
            .link_store
            .get_compact_state_messages_by_fid(fid, &PageOptions::default())
    }

    pub fn get_reactions_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .reaction_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_user_data_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .user_data_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_user_data_by_fid_and_type(
        &self,
        fid: u64,
        user_data_type: proto::UserDataType,
    ) -> Result<Message, HubError> {
        UserDataStore::get_user_data_by_fid_and_type(
            &self.stores.user_data_store,
            fid,
            user_data_type,
        )
    }

    pub fn get_verifications_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .verification_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_username_proofs_by_fid(&self, fid: u64) -> Result<MessagesPage, HubError> {
        self.stores
            .username_proof_store
            .get_adds_by_fid::<fn(&Message) -> bool>(fid, &PageOptions::default(), None)
    }

    pub fn get_fname_proof(&self, name: &String) -> Result<Option<UserNameProof>, HubError> {
        UserDataStore::get_username_proof(
            &self.stores.user_data_store,
            &mut RocksDbTransactionBatch::new(),
            name.as_bytes(),
        )
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u64,
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

    pub fn trie_num_items(&mut self) -> usize {
        self.stores.trie.items().unwrap()
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
