use crate::core::types::{proto, Height};
use crate::proto::snapchain::Block;
use crate::proto::{message, snapchain};
use crate::storage::db::RocksDB;
use crate::storage::store::BlockStore;
use std::collections::HashMap;
use std::iter;
use tokio::sync::mpsc;
use tracing::error;

// Shard state root and the transactions
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<proto::Transaction>,
}

// pub trait Engine {
//     fn propose_state_change(&mut self, shard_id: u32) -> ShardStateChange;
//     fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool;
//     fn commit_state_change(&mut self, shard_state_change: ShardStateChange);

//     fn commit_block(&mut self, block: proto::Block);

//     fn get_confirmed_height(&self, shard_id: u32) -> Height;
// }

pub struct ShardEngine {
    shard_id: u32,
    shard_store: RocksDB,
    messages_rx: mpsc::Receiver<message::Message>,
    messages_tx: mpsc::Sender<message::Message>,
}

impl ShardEngine {
    pub fn new(shard_id: u32, shard_store: RocksDB) -> ShardEngine {
        let (messages_tx, messages_rx) = mpsc::channel::<message::Message>(100);
        ShardEngine {
            shard_id,
            shard_store,
            messages_rx,
            messages_tx,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<message::Message> {
        self.messages_tx.clone()
    }

    pub fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
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

    pub fn validate_state_change(&mut self, _shard_state_change: &ShardStateChange) -> bool {
        // Create a db transaction
        // Replay the state change
        // If all messages merge successfully and the merkle trie root matches the stateroot in the state change, return true
        // Else return false
        // Rollback the transaction
        true
    }

    pub fn commit_state_change(&mut self, _shard_state_change: ShardStateChange) {
        // Create a db transaction
        // Replay the state change
        // If the state root does not match or any of the messages fail to merge, panic?
        // write the events to the db
        // Commit the transaction
        // Emit events
    }

    pub fn get_confirmed_height(&self) -> Height {
        // TODO: look up in db
        return Height::new(self.shard_id, 0);
    }
}

pub struct SnapchainEngine {
    block_store: BlockStore,
    block_shard: u32,
}

impl SnapchainEngine {
    pub fn new(block_shard: u32, block_store: BlockStore) -> Self {
        SnapchainEngine {
            block_shard,
            block_store,
        }
    }

    pub fn commit_block(&mut self, block: Block) {
        // Commit the individual shards state changes
        // for shard_chunks in block.shard_chunks.clone() {
        //     let shard_index = shard_chunks
        //         .header
        //         .clone()
        //         .unwrap()
        //         .height
        //         .unwrap()
        //         .shard_index;
        //     self.commit_state_change(ShardStateChange {
        //         shard_id: shard_index,
        //         new_state_root: shard_chunks.header.unwrap().shard_root,
        //         transactions: shard_chunks.transactions,
        //     });
        // }

        let result = self.block_store.put_block(block);
        if result.is_err() {
            error!("Failed to store block: {:?}", result.err());
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.block_store.max_block_number(self.block_shard) {
            Ok(block_num) => Height::new(self.block_shard, block_num),
            Err(_) => Height::new(self.block_shard, 0),
        }
    }
}
