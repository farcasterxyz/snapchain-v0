use super::super::db::{RocksDB, RocksDbTransactionBatch};
use super::errors::TrieError;
use super::trie_node::{TrieNode, TIMESTAMP_LENGTH};
use crate::proto::msg as message;
use crate::proto::onchain_event;
use crate::storage::store::account::IntoU8;
use std::collections::HashMap;
use tracing::info;

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";

pub struct TrieKey {}

impl TrieKey {
    pub fn for_message(msg: &message::Message) -> Vec<u8> {
        let mut key = Self::for_message_type(msg.fid(), msg.msg_type().into_u8());
        key.extend_from_slice(&msg.hash);
        key
    }

    pub fn for_message_type(fid: u32, msg_type: u8) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(fid));
        key.push(msg_type);
        key
    }

    pub fn for_onchain_event(event: &onchain_event::OnChainEvent) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(event.fid as u32));
        key.push(event.r#type as u8);
        key.extend_from_slice(&event.transaction_hash);
        key
    }

    pub fn for_fid(fid: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(4);
        key.extend_from_slice(&fid.to_be_bytes());
        key
    }
}

fn expand_nibbles_(input: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 2);
    for byte in input {
        let high_nibble = (byte >> 4) & 0x0F;
        let low_nibble = byte & 0x0F;
        result.push(high_nibble);
        result.push(low_nibble);
    }
    result
}

fn combine_nibbles_(input: Vec<u8>) -> Vec<u8> {
    assert!(input.len() % 2 == 0, "Input length must be even");
    let mut result = Vec::with_capacity(input.len() / 2);
    for chunk in input.chunks(2) {
        let high_nibble = chunk[0] << 4;
        let low_nibble = chunk[1] & 0x0F;
        result.push(high_nibble | low_nibble);
    }
    result
}

fn expand_quibbles(input: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 4);
    for byte in input {
        let q1 = (byte >> 6) & 0x03; // Top 2 bits
        let q2 = (byte >> 4) & 0x03; // Next 2 bits
        let q3 = (byte >> 2) & 0x03; // Next 2 bits
        let q4 = byte & 0x03; // Bottom 2 bits
        result.push(q1);
        result.push(q2);
        result.push(q3);
        result.push(q4);
    }
    result
}

fn combine_quibbles(input: Vec<u8>) -> Vec<u8> {
    assert!(input.len() % 4 == 0, "Input length must be a multiple of 4");
    let mut result = Vec::with_capacity(input.len() / 4);
    for chunk in input.chunks(4) {
        let q1 = (chunk[0] & 0x03) << 6; // Top 2 bits
        let q2 = (chunk[1] & 0x03) << 4; // Next 2 bits
        let q3 = (chunk[2] & 0x03) << 2; // Next 2 bits
        let q4 = chunk[3] & 0x03; // Bottom 2 bits
        result.push(q1 | q2 | q3 | q4);
    }
    result
}

#[derive(Debug)]
pub struct NodeMetadata {
    pub prefix: Vec<u8>,
    pub num_messages: usize,
    pub hash: String,
    pub children: HashMap<u8, NodeMetadata>,
}

pub struct TrieSnapshot {
    pub prefix: Vec<u8>,
    pub excluded_hashes: Vec<String>,
    pub num_messages: usize,
}

#[derive(Clone)]
pub struct MerkleTrie {
    root: Option<TrieNode>,
}

impl MerkleTrie {
    pub fn new() -> Self {
        MerkleTrie { root: None }
    }

    fn create_empty_root(&mut self, txn_batch: &mut RocksDbTransactionBatch) {
        let root_key = TrieNode::make_primary_key(&[], None);
        let empty = TrieNode::new();
        let serialized = TrieNode::serialize(&empty);

        // Write the empty root node to the DB
        txn_batch.put(root_key, serialized);
        self.root.replace(empty);
    }

    pub fn initialize(&mut self, db: &RocksDB) -> Result<(), TrieError> {
        // db must be "open" by now

        let loaded = self.load_root(db)?;
        if let Some(root_node) = loaded {
            self.root.replace(root_node);
        } else {
            info!("Initializing empty merkle trie root");
            let mut txn_batch = RocksDbTransactionBatch::new();
            self.create_empty_root(&mut txn_batch);
            db.commit(txn_batch).map_err(TrieError::wrap_database)?;
        }

        Ok(())
    }

    fn load_root(&self, db: &RocksDB) -> Result<Option<TrieNode>, TrieError> {
        let root_key = TrieNode::make_primary_key(&[], None);

        if let Some(root_bytes) = db.get(&root_key).map_err(TrieError::wrap_database)? {
            let root_node = TrieNode::deserialize(&root_bytes.as_slice())?;
            Ok(Some(root_node))
        } else {
            Ok(None)
        }
    }

    pub fn reload(&mut self, db: &RocksDB) -> Result<(), TrieError> {
        // Load the root node using the provided database reference
        let loaded = self.load_root(db)?;

        match loaded {
            Some(replacement_root) => {
                // Replace the root node with the loaded node
                self.root.replace(replacement_root);
                Ok(())
            }
            None => Err(TrieError::UnableToReloadRoot),
        }
    }

    pub fn insert(
        &mut self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys_: Vec<Vec<u8>>,
        load_count: &mut u64,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys_.into_iter().map(expand_quibbles).collect();

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < TIMESTAMP_LENGTH {
                return Err(TrieError::KeyLengthTooShort);
            }
        }

        if let Some(root) = self.root.as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            let results = root.insert(db, &mut txn, keys, 0, load_count)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn delete(
        &mut self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys_: Vec<Vec<u8>>,
        load_count: &mut u64,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys_.into_iter().map(expand_quibbles).collect();

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < TIMESTAMP_LENGTH {
                return Err(TrieError::KeyLengthTooShort);
            }
        }

        if let Some(root) = self.root.as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            let results = root.delete(db, &mut txn, keys, 0, load_count)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn exists(
        &mut self,
        db: &RocksDB,
        key_: &Vec<u8>,
        load_count: &mut u64,
    ) -> Result<bool, TrieError> {
        let key: Vec<u8> = expand_quibbles(key_.clone());

        if let Some(root) = self.root.as_mut() {
            root.exists(db, &key, 0, load_count)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn items(&self) -> Result<usize, TrieError> {
        if let Some(root) = self.root.as_ref() {
            Ok(root.items())
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn get_node(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Option<TrieNode> {
        let node_key = TrieNode::make_primary_key(prefix, None);

        // First, attempt to get it from the DB cache
        if let Some(Some(node_bytes)) = txn_batch.batch.get(&node_key) {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        // Else, get it directly from the DB
        if let Some(node_bytes) = db.get(&node_key).ok().flatten() {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        None
    }

    pub fn get_hash(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Vec<u8> {
        let expanded_prefix = expand_quibbles(prefix.to_vec());
        let hash_result = self
            .get_node(db, txn_batch, &expanded_prefix)
            .map(|node| node.hash())
            .unwrap_or(vec![]);
        combine_quibbles(hash_result)
    }

    pub fn get_count(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> u64 {
        self.get_node(db, txn_batch, prefix)
            .map(|node| node.items())
            .unwrap_or(0) as u64
    }

    pub fn root_hash(&self) -> Result<Vec<u8>, TrieError> {
        if let Some(root) = self.root.as_ref() {
            Ok(root.hash())
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn get_all_values(
        &mut self,
        db: &RocksDB,
        prefix: &[u8],
        load_count: &mut u64,
    ) -> Result<Vec<Vec<u8>>, TrieError> {
        if let Some(root) = self.root.as_mut() {
            if let Some(node) = root.get_node_from_trie(db, prefix, 0, load_count) {
                match node.get_all_values(db, prefix, load_count) {
                    Ok(values) => Ok(values.into_iter().map(expand_quibbles).collect()),
                    Err(e) => Err(e),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn get_snapshot(
        &mut self,
        db: &RocksDB,
        prefix: &[u8],
        load_count: &mut u64,
    ) -> Result<TrieSnapshot, TrieError> {
        if let Some(root) = self.root.as_mut() {
            root.get_snapshot(db, prefix, 0, load_count)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub(crate) fn print(&mut self) {
        self.root.as_ref().unwrap().print(0, 0).unwrap();
    }

    pub fn get_trie_node_metadata(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Result<NodeMetadata, TrieError> {
        if let Some(node) = self.get_node(db, txn_batch, prefix) {
            let mut children = HashMap::new();

            for char in node.children().keys() {
                let mut child_prefix = prefix.to_vec();
                child_prefix.push(*char);

                let child_node = self.get_node(db, txn_batch, &child_prefix).ok_or(
                    TrieError::ChildNotFound {
                        char: *char,
                        prefix: prefix.to_vec(),
                    },
                )?;

                children.insert(
                    *char,
                    NodeMetadata {
                        prefix: child_prefix,
                        num_messages: child_node.items(),
                        hash: hex::encode(&child_node.hash()),
                        children: HashMap::new(),
                    },
                );
            }

            Ok(NodeMetadata {
                prefix: prefix.to_vec(),
                num_messages: node.items(),
                hash: hex::encode(&node.hash()),
                children,
            })
        } else {
            Err(TrieError::NodeNotFound {
                prefix: prefix.to_vec(),
            })
        }
    }
}
