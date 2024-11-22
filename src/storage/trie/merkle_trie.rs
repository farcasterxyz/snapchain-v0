use super::super::db::{RocksDB, RocksDbTransactionBatch};
use super::errors::TrieError;
use super::trie_node::{TrieNode, TIMESTAMP_LENGTH};
use std::collections::HashMap;
use tracing::info;

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";

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
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, TrieError> {
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
            let results = root.insert(db, &mut txn, keys, 0)?;

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
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, TrieError> {
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
            let results = root.delete(db, &mut txn, keys, 0)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn exists(&mut self, db: &RocksDB, key: &Vec<u8>) -> Result<bool, TrieError> {
        if let Some(root) = self.root.as_mut() {
            root.exists(db, key, 0)
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
    ) -> Result<Vec<Vec<u8>>, TrieError> {
        if let Some(root) = self.root.as_mut() {
            if let Some(node) = root.get_node_from_trie(db, prefix, 0) {
                node.get_all_values(db, prefix)
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn get_snapshot(&mut self, db: &RocksDB, prefix: &[u8]) -> Result<TrieSnapshot, TrieError> {
        if let Some(root) = self.root.as_mut() {
            root.get_snapshot(db, prefix, 0)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
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
