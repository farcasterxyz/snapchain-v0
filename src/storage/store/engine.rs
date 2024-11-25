use super::account::OnchainEventStorageError;
use super::shard::ShardStore;
use crate::core::error::HubError;
use crate::core::types::Height;
use crate::proto::hub_event::HubEvent;
use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
use crate::proto::{hub_event, msg as message, snapchain};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::store::account::{
    CastStore, MessagesPage, OnchainEventStore, Store, StoreEventHandler,
};
use crate::storage::store::BlockStore;
use crate::storage::trie;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::TrieKey;
use cadence::{Counted, CountedExt, Gauged, StatsdClient};
use itertools::Itertools;
use message::MessageType;
use snapchain::{Block, ShardChunk, Transaction};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

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

    #[error("invalid message type")]
    InvalidMessageType,

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

pub struct ShardEngine {
    shard_id: u32,
    shard_store: ShardStore,
    messages_rx: mpsc::Receiver<MempoolMessage>,
    messages_tx: mpsc::Sender<MempoolMessage>,
    events_tx: broadcast::Sender<HubEvent>,
    pub(crate) trie: merkle_trie::MerkleTrie,
    cast_store: Store,
    pub db: Arc<RocksDB>,
    onchain_event_store: OnchainEventStore,
    metrics_client: Arc<StatsdClient>,
}

fn encode_vec(data: &[Vec<u8>]) -> String {
    data.iter()
        .map(|vec| hex::encode(vec))
        .collect::<Vec<String>>()
        .join(", ")
}

impl ShardEngine {
    pub fn new(
        shard_id: u32,
        shard_store: ShardStore,
        metrics_client: Arc<StatsdClient>,
        events_tx: broadcast::Sender<HubEvent>,
    ) -> ShardEngine {
        let db = &*shard_store.db;

        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.
        let mut trie = merkle_trie::MerkleTrie::new();
        trie.initialize(db).unwrap();

        let event_handler = StoreEventHandler::new(None, None, None);
        let db = shard_store.db.clone();
        let cast_store = CastStore::new(shard_store.db.clone(), event_handler.clone(), 100);
        let onchain_event_store =
            OnchainEventStore::new(shard_store.db.clone(), event_handler.clone());

        let (messages_tx, messages_rx) = mpsc::channel::<MempoolMessage>(10_000);
        ShardEngine {
            shard_id,
            shard_store,
            messages_rx,
            messages_tx,
            events_tx,
            trie,
            cast_store,
            db,
            onchain_event_store,
            metrics_client,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<MempoolMessage> {
        self.messages_tx.clone()
    }

    pub(crate) fn trie_root_hash(&self) -> Vec<u8> {
        self.trie.root_hash().unwrap()
    }

    fn prepare_proposal(
        &mut self,
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

        let mut snapchain_txns = self.create_transactions_from_mempool(messages);
        for snapchain_txn in &mut snapchain_txns {
            let (account_root, _events) = self.replay_snapchain_txn(&snapchain_txn, txn_batch)?;
            snapchain_txn.account_root = account_root;
        }

        let new_root_hash = self.trie.root_hash()?;
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
        mut messages: Vec<MempoolMessage>,
    ) -> Vec<Transaction> {
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
            for msg in messages {
                match msg {
                    MempoolMessage::ValidatorMessage(msg) => {
                        transaction.system_messages.push(msg.clone());
                    }
                    MempoolMessage::UserMessage(msg) => {
                        transaction.user_messages.push(msg.clone());
                    }
                }
            }
            transactions.push(transaction);
        }
        info!(
            transactions = transactions.len(),
            messages = messages.len(),
            fids = unique_fids,
            "Created transactions from mempool"
        );
        transactions
    }

    fn incr(&self, key: &str) {
        let key = format!("shard{}.{}", self.shard_id, key);
        _ = self.metrics_client.incr(key.as_str())
    }

    fn count(&self, key: &str, value: u64) {
        let key = format!("shard{}.{}", self.shard_id, key);
        _ = self.metrics_client.count(key.as_str(), value)
    }

    fn gauge(&self, key: &str, value: u64) {
        let key = format!("shard{}.{}", self.shard_id, key);
        _ = self.metrics_client.gauge(key.as_str(), value)
    }

    pub fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
        let mut txn = RocksDbTransactionBatch::new();
        let result = self.prepare_proposal(&mut txn, shard).unwrap(); //TODO: don't unwrap()

        // TODO: this should probably operate automatically via drop trait
        self.trie.reload(&self.db).unwrap();

        self.incr("propose");

        result
    }

    fn replay_proposal(
        &mut self,
        txn_batch: &mut RocksDbTransactionBatch,
        transactions: &[Transaction],
        shard_root: &[u8],
    ) -> Result<Vec<HubEvent>, EngineError> {
        let mut events = vec![];
        for snapchain_txn in transactions {
            let (_, txn_events) = self.replay_snapchain_txn(snapchain_txn, txn_batch)?;
            events.extend(txn_events);
        }

        let root1 = self.trie.root_hash()?;

        if &root1 != shard_root {
            return Err(EngineError::HashMismatch);
        }

        Ok(events)
    }

    fn replay_snapchain_txn(
        &mut self,
        snapchain_txn: &Transaction,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(Vec<u8>, Vec<HubEvent>), EngineError> {
        let total_user_messages = snapchain_txn.user_messages.len();
        let total_system_messages = snapchain_txn.system_messages.len();
        let mut user_messages_count = 0;
        let mut system_messages_count = 0;
        let mut events = vec![];

        for msg in &snapchain_txn.user_messages {
            // Errors are validated based on the shard root
            let result = self.merge_message(msg, txn_batch);
            match result {
                Ok(event) => {
                    self.update_trie(&event, txn_batch)?;
                    events.push(event.clone());
                    user_messages_count += 1;
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

        for msg in &snapchain_txn.system_messages {
            if let Some(onchain_event) = &msg.on_chain_event {
                let event = self
                    .onchain_event_store
                    .merge_onchain_event(onchain_event.clone(), txn_batch);

                match event {
                    Ok(hub_event) => {
                        self.update_trie(&hub_event, txn_batch)?;
                        events.push(hub_event.clone());
                        system_messages_count += 1;
                    }
                    Err(err) => {
                        warn!("Error merging onchain event: {:?}", err);
                    }
                }
            }
        }

        let account_root = self.trie.get_hash(
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
            account_root = hex::encode(&account_root),
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
            MessageType::CastAdd => self
                .cast_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            MessageType::CastRemove => self
                .cast_store
                .merge(msg, txn_batch)
                .map_err(EngineError::new_store_error(msg.hash.clone())),
            unhandled_type => {
                return Err(EngineError::UnsupportedMessageType(unhandled_type));
            }
        }?;

        Ok(event)
    }

    fn update_trie(
        &mut self,
        event: &hub_event::HubEvent,
        txn_batch: &mut RocksDbTransactionBatch,
    ) -> Result<(), EngineError> {
        match &event.body {
            Some(hub_event::hub_event::Body::MergeMessageBody(merge)) => {
                if let Some(msg) = &merge.message {
                    self.trie
                        .insert(&self.db, txn_batch, vec![TrieKey::for_message(&msg)])?;
                }
                for deleted_message in &merge.deleted_messages {
                    self.trie.delete(
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_message(&deleted_message)],
                    )?;
                }
            }
            Some(hub_event::hub_event::Body::MergeOnChainEventBody(merge)) => {
                if let Some(onchain_event) = &merge.on_chain_event {
                    self.trie.insert(
                        &self.db,
                        txn_batch,
                        vec![TrieKey::for_onchain_event(&onchain_event)],
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

    pub fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool {
        let mut txn = RocksDbTransactionBatch::new();

        let transactions = &shard_state_change.transactions;
        let shard_root = &shard_state_change.new_state_root;

        let mut result = true;

        if let Err(err) = self.replay_proposal(&mut txn, transactions, shard_root) {
            error!("State change validation failed: {}", err);
            result = false;
        }

        self.trie.reload(&*self.shard_store.db).unwrap();

        if result {
            self.incr("validate.true");
            self.count("validate.false", 0)
        } else {
            self.incr("validate.false");
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
            self.events_tx.send(event);
        }
        self.trie.reload(&self.db).unwrap();
        self.incr("commit");

        let block_number = &shard_chunk
            .header
            .as_ref()
            .unwrap()
            .height
            .unwrap()
            .block_number;
        self.gauge("block_height", *block_number);
        match self.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: &ShardChunk) {
        let mut txn = RocksDbTransactionBatch::new();

        let shard_root = &shard_chunk.header.as_ref().unwrap().shard_root;
        let transactions = &shard_chunk.transactions;

        match self.replay_proposal(&mut txn, transactions, shard_root) {
            Err(err) => {
                error!("State change commit failed: {}", err);
                panic!("State change commit failed: {}", err);
            }
            Ok(events) => {
                self.commit_and_emit_events(shard_chunk, events, txn);
            }
        }
    }

    pub(crate) fn trie_key_exists(&mut self, sync_id: &Vec<u8>) -> bool {
        self.trie
            .exists(&self.db, sync_id.as_ref())
            .unwrap_or_else(|err| {
                error!("Error checking if sync id exists: {:?}", err);
                false
            })
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.shard_store.max_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            Err(_) => Height::new(self.shard_id, 0),
        }
    }

    pub fn get_last_shard_chunk(&self) -> Option<ShardChunk> {
        match self.shard_store.get_last_shard_chunk() {
            Ok(shard_chunk) => shard_chunk,
            Err(err) => {
                error!("Unable to obtain last shard chunk {:#?}", err);
                None
            }
        }
    }

    pub fn get_casts_by_fid(&self, fid: u32) -> Result<MessagesPage, HubError> {
        CastStore::get_cast_adds_by_fid(&self.cast_store, fid, &PageOptions::default())
    }

    pub fn get_onchain_events(
        &self,
        event_type: OnChainEventType,
        fid: u32,
    ) -> Result<Vec<OnChainEvent>, OnchainEventStorageError> {
        self.onchain_event_store.get_onchain_events(event_type, fid)
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
