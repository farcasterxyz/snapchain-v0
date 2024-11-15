use crate::core::types::{proto, Height};
use crate::proto::snapchain::Block;
use crate::proto::{message, snapchain};
use crate::storage::store::BlockStore;
use std::iter;
use tokio::sync::mpsc;
use tracing::error;

// Shard state root and the transactions
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<proto::Transaction>,
}

pub trait Engine {
    fn propose_state_change(&mut self, shard_id: u32) -> ShardStateChange;
    fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool;
    fn commit_state_change(&mut self, shard_state_change: ShardStateChange);

    fn commit_block(&mut self, block: proto::Block);

    fn get_confirmed_height(&self, shard_id: u32) -> Height;
}

pub struct SnapchainEngine {
    block_store: BlockStore,
    messages_rx: mpsc::Receiver<message::Message>,
    messages_tx: mpsc::Sender<message::Message>,
}

impl SnapchainEngine {
    pub fn new(block_store: BlockStore) -> Self {
        let (messages_tx, messages_rx) = mpsc::channel::<message::Message>(100);
        SnapchainEngine {
            block_store,
            messages_rx,
            messages_tx,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<message::Message> {
        self.messages_tx.clone()
    }
}

impl Engine for SnapchainEngine {
    fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
        let it = iter::from_fn(|| self.messages_rx.try_recv().ok());
        let user_messages: Vec<message::Message> = it.collect();

        let transactions = vec![snapchain::Transaction {
            fid: 1234,                      //TODO
            account_root: vec![5, 5, 6, 6], //TODO
            system_messages: vec![],        //TODO
            user_messages,
        }];

        // TODO:
        // Create a db transaction
        // For each user message, add to the store and merkle trie
        // Evict invalid messages from the mempool
        // Construct a ShardStateChange with the valid transactions and the new state root
        // Rollback the transaction
        // Return the state change

        ShardStateChange {
            shard_id: shard,
            new_state_root: vec![],
            transactions,
        }
    }

    fn validate_state_change(&mut self, _shard_state_change: &ShardStateChange) -> bool {
        // Create a db transaction
        // Replay the state change
        // If all messages merge successfully and the merkle trie root matches the stateroot in the state change, return true
        // Else return false
        // Rollback the transaction
        true
    }

    fn commit_state_change(&mut self, _shard_state_change: ShardStateChange) {
        // Create a db transaction
        // Replay the state change
        // If the state root does not match or any of the messages fail to merge, panic?
        // write the events to the db
        // Commit the transaction
        // Emit events
    }

    fn commit_block(&mut self, block: Block) {
        // Commit the individual shards state changes
        for shard_chunks in block.shard_chunks.clone() {
            let shard_index = shard_chunks
                .header
                .clone()
                .unwrap()
                .height
                .unwrap()
                .shard_index;
            self.commit_state_change(ShardStateChange {
                shard_id: shard_index,
                new_state_root: shard_chunks.header.unwrap().shard_root,
                transactions: shard_chunks.transactions,
            });
        }

        let result = self.block_store.put_block(block);
        if result.is_err() {
            error!("Failed to store block: {:?}", result.err());
        }
    }

    fn get_confirmed_height(&self, shard_id: u32) -> Height {
        match self.block_store.max_block_number(shard_id) {
            Ok(block_num) => Height::new(shard_id, block_num),
            Err(_) => Height::new(shard_id, 0),
        }
    }
}
