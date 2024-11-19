use crate::core::types::{proto, Height};
use crate::proto::snapchain::{Block, ShardChunk};
use crate::proto::{message, snapchain};
use crate::storage::store::BlockStore;
use crate::storage::trie::merkle_trie;
use std::iter;
use tokio::sync::mpsc;
use tracing::{error, warn};

use super::shard::ShardStore;

// Shard state root and the transactions
pub struct ShardStateChange {
    pub shard_id: u32,
    pub new_state_root: Vec<u8>,
    pub transactions: Vec<proto::Transaction>,
}

pub struct ShardEngine {
    shard_id: u32,
    shard_store: ShardStore,
    messages_rx: mpsc::Receiver<message::Message>,
    messages_tx: mpsc::Sender<message::Message>,
    trie: merkle_trie::MerkleTrie,
}

fn encode_vec(data: &[Vec<u8>]) -> String {
    data.iter()
        .map(|vec| hex::encode(vec))
        .collect::<Vec<String>>()
        .join(", ")
}

impl ShardEngine {
    pub fn new(shard_id: u32, shard_store: ShardStore) -> ShardEngine {
        // TODO: adding the trie here introduces many calls that want to return errors. Rethink unwrap strategy.

        // TODO: refactor trie code to work with transactions only instead of having its own reference to the db (probably).
        let trie = merkle_trie::MerkleTrie::new_with_db(shard_store.db.clone()).unwrap();
        trie.initialize().unwrap();

        // TODO: The empty trie currently has some issues with the newly added commit/rollback code. Remove when we can.
        trie.insert(vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
            .unwrap();
        trie.commit().unwrap();
        trie.reload().unwrap();

        let (messages_tx, messages_rx) = mpsc::channel::<message::Message>(10_000);
        ShardEngine {
            shard_id,
            shard_store,
            messages_rx,
            messages_tx,
            trie,
        }
    }

    pub fn messages_tx(&self) -> mpsc::Sender<message::Message> {
        self.messages_tx.clone()
    }

    pub fn propose_state_change(&mut self, shard: u32) -> ShardStateChange {
        //TODO: return Result instead of .unwrap() ?
        let it = iter::from_fn(|| self.messages_rx.try_recv().ok());
        let user_messages: Vec<message::Message> = it.collect();

        let mut hashes: Vec<Vec<u8>> = vec![];
        for msg in &user_messages {
            hashes.push(msg.hash.clone());
        }

        let transactions = vec![snapchain::Transaction {
            fid: 1234,                      //TODO
            account_root: vec![5, 5, 6, 6], //TODO
            system_messages: vec![],        //TODO
            user_messages,
        }];

        warn!(
            shard,
            insert = encode_vec(&hashes.clone()),
            old_root_hash = hex::encode(self.trie.root_hash().unwrap()),
            "propose – before insert",
        );

        self.trie.insert(hashes);
        let new_root_hash = self.trie.root_hash().unwrap();
        let count = self.trie.items().unwrap();

        warn!(
            shard,
            new_state_root = hex::encode(&new_root_hash),
            count,
            "propose - after insert"
        );

        let result = ShardStateChange {
            shard_id: shard,
            new_state_root: new_root_hash.clone(),
            transactions,
        };

        // TODO: this should probably operate automatically via drop trait
        self.trie.reload().unwrap();

        warn!(
            shard,
            reloaded_root_hash = hex::encode(&self.trie.root_hash().unwrap()),
            count,
            "propose - reloaded"
        );

        result

        // TODO:
        // Create a db transaction
        // For each user message, add to the store and merkle trie
        // Evict invalid messages from the mempool
        // Construct a ShardStateChange with the valid transactions and the new state root
        // Rollback the transaction
        // Return the state change
    }

    pub fn validate_state_change(&mut self, shard_state_change: &ShardStateChange) -> bool {
        // TODO: actually validate

        // Create a db transaction
        // Replay the state change
        // If all messages merge successfully and the merkle trie root matches the stateroot in the state change, return true
        // Else return false
        // Rollback the transaction
        true // TODO
    }

    pub fn commit_shard_chunk(&mut self, shard_chunk: ShardChunk) {
        let shard_root = shard_chunk.clone().header.unwrap().shard_root; // TODO: without clone?

        let mut hashes: Vec<Vec<u8>> = vec![];
        // TODO! don't assume 1 transaction
        for msg in &shard_chunk.transactions[0].user_messages {
            hashes.push(msg.hash.clone());
        }

        let root0 = self.trie.root_hash().unwrap();

        warn!(
            state_root_0 = hex::encode(&root0),
            insert = encode_vec(&hashes),
            "commit insert"
        );

        self.trie.insert(hashes.clone()).unwrap();
        let root1 = self.trie.root_hash().unwrap();

        let hashes_match = &root1 == &shard_root;

        warn!(
            hashes_match,
            state_root_0 = hex::encode(&root0),
            state_root_1 = hex::encode(&root1),
            shard_root = hex::encode(shard_root.clone()),
            "commit 1"
        );

        if hashes_match {
            self.trie.commit().unwrap();
            let committed_root = self.trie.root_hash().unwrap();

            self.trie.reload().unwrap();
            let reloaded_root = self.trie.root_hash().unwrap();

            error!(
                committed_root = hex::encode(committed_root),
                reloaded_root = hex::encode(reloaded_root),
                "commit!"
            );
        } else {
            error!("panic!");
            panic!("hashes don't match")
        }

        // Create a db transaction
        // Replay the state change
        // If the state root does not match or any of the messages fail to merge, panic?
        // write the events to the db
        // Commit the transaction
        // Emit events
        match self.shard_store.put_shard_chunk(shard_chunk) {
            Err(err) => {
                error!("Unable to write shard chunk to store {}", err)
            }
            Ok(()) => {}
        }
    }

    pub fn get_confirmed_height(&self) -> Height {
        match self.shard_store.max_block_number() {
            Ok(block_num) => Height::new(self.shard_id, block_num),
            Err(_) => Height::new(self.shard_id, 0),
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

    pub fn get_confirmed_height(&self) -> Height {
        let shard_index = 0;
        // TODO(aditi): There's no reason we need to provide a shard id here anymore
        match self.block_store.max_block_number(shard_index) {
            Ok(block_num) => Height::new(shard_index, block_num),
            Err(_) => Height::new(shard_index, 0),
        }
    }
}
