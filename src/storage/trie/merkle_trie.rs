use super::super::{
    db::{RocksDB, RocksDbTransactionBatch},
    hub_error::HubError,
};
use super::trie_node::{TrieNode, TIMESTAMP_LENGTH};
use std::{
    collections::HashMap,
    path::Path,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

// Threadpool for use in the store
use once_cell::sync::Lazy;
use threadpool::ThreadPool;

pub static THREAD_POOL: Lazy<Mutex<ThreadPool>> = Lazy::new(|| Mutex::new(ThreadPool::new(4)));

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";
const TRIE_UNLOAD_THRESHOLD: usize = 10_000;

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
    root: RwLock<Option<TrieNode>>,
    db: Arc<RocksDB>,
    db_owned: AtomicBool,
    txn_batch: Mutex<RocksDbTransactionBatch>,
}

impl MerkleTrie {
    pub fn new(main_db_path: &str) -> Result<Self, HubError> {
        let path = Path::join(Path::new(main_db_path), Path::new(TRIE_DBPATH_PREFIX))
            .into_os_string()
            .into_string()
            .map_err(|os_str| {
                HubError::validation_failure(
                    format!("error with Merkle Trie path {:?}", os_str).as_str(),
                )
            })?;
        let db = Arc::new(RocksDB::new(path.as_str()));

        Ok(MerkleTrie {
            root: RwLock::new(None),
            db,
            db_owned: AtomicBool::new(true),
            txn_batch: Mutex::new(RocksDbTransactionBatch::new()),
        })
    }

    pub fn new_with_db(db: Arc<RocksDB>) -> Result<Self, HubError> {
        Ok(MerkleTrie {
            root: RwLock::new(None),
            db,
            db_owned: AtomicBool::new(false),
            txn_batch: Mutex::new(RocksDbTransactionBatch::new()),
        })
    }

    fn create_empty_root(&self) {
        let root_key = TrieNode::make_primary_key(&[], None);
        let empty = TrieNode::new();
        let serialized = TrieNode::serialize(&empty);

        // Write the empty root node to the DB
        self.txn_batch.lock().unwrap().put(root_key, serialized);
        self.root.write().unwrap().replace(empty);
    }

    pub fn initialize(&self) -> Result<(), HubError> {
        // First open the DB
        if self.db_owned.load(std::sync::atomic::Ordering::Relaxed) {
            self.db.open()?;
        }

        // Then load the root node
        let loaded = self.load_root()?;
        if let Some(root_node) = loaded {
            // Replace the root node
            self.root.write().unwrap().replace(root_node);
        } else {
            self.create_empty_root();
        }

        Ok(())
    }

    fn load_root(&self) -> Result<Option<TrieNode>, HubError> {
        let root_key = TrieNode::make_primary_key(&[], None);
        if let Some(root_bytes) = self.db.get(&root_key)? {
            let root_node = TrieNode::deserialize(&root_bytes.as_slice())?;
            Ok(Some(root_node))
        } else {
            Ok(None)
        }
    }

    pub fn db(&self) -> Arc<RocksDB> {
        self.db.clone()
    }

    pub fn clear(&self) -> Result<(), HubError> {
        self.txn_batch.lock().unwrap().batch.clear();
        self.db.clear()?;

        self.create_empty_root();

        Ok(())
    }

    pub fn stop(&self) -> Result<(), HubError> {
        // Close
        if self.db_owned.load(std::sync::atomic::Ordering::Relaxed) {
            self.db.close();
        }

        Ok(())
    }

    pub fn commit(&self) -> Result<(), HubError> {
        if let Some(root) = self.root.write().unwrap().as_mut() {
            self.unload_from_memory(root, true)
        } else {
            panic!("commit") // TODO
        }
    }

    pub fn reload(&self) -> Result<(), HubError> {
        let loaded = self.load_root()?;

        match loaded {
            Some(replacement_root) => {
                let mut root_guard = self.root.write().unwrap();
                root_guard.replace(replacement_root);

                let mut txn_batch = self.txn_batch.lock().unwrap();
                *txn_batch = RocksDbTransactionBatch::new();

                Ok(())
            }
            None => Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "unable to reload root".to_string(),
            }),
        }
    }

    /**
     *  Unload children from memory after every few ops, to prevent memory leaks.
     *  Note: We require a write-locked root node to perform this operation, which should
     *  be supplied by the caller.
     */
    fn unload_from_memory(&self, root: &mut TrieNode, force: bool) -> Result<(), HubError> {
        let mut txn_batch = self.txn_batch.lock().unwrap();
        if force || txn_batch.batch.len() > TRIE_UNLOAD_THRESHOLD {
            // Take the txn_batch out of the lock and replace it with a new one
            let pending_txn_batch =
                std::mem::replace(&mut *txn_batch, RocksDbTransactionBatch::new());

            // Commit the txn_batch
            self.db.commit(pending_txn_batch)?;

            root.unload_children();
        }
        Ok(())
    }

    pub fn insert(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, HubError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < TIMESTAMP_LENGTH {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "Key length is too short".to_string(),
                });
            }
        }

        if let Some(root) = self.root.write().unwrap().as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            let results = root.insert(&self.db, &mut txn, keys, 0)?;

            self.txn_batch.lock().unwrap().merge(txn);
            Ok(results)
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: format!("Merkle Trie not initialized for insert {:?}", keys),
            })
        }
    }

    pub fn delete(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, HubError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys.iter() {
            if key.len() < TIMESTAMP_LENGTH {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "Key length is too short".to_string(),
                });
            }
        }

        if let Some(root) = self.root.write().unwrap().as_mut() {
            let mut txn = RocksDbTransactionBatch::new();
            let results = root.delete(&self.db, &mut txn, keys, 0)?;

            self.txn_batch.lock().unwrap().merge(txn);
            Ok(results)
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for delete".to_string(),
            })
        }
    }

    pub fn exists(&self, key: &Vec<u8>) -> Result<bool, HubError> {
        if let Some(root) = self.root.write().unwrap().as_mut() {
            root.exists(&self.db, &key, 0)
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for exists".to_string(),
            })
        }
    }

    pub fn items(&self) -> Result<usize, HubError> {
        if let Some(root) = self.root.read().unwrap().as_ref() {
            Ok(root.items())
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for items".to_string(),
            })
        }
    }

    pub fn get_node(&self, prefix: &[u8]) -> Option<TrieNode> {
        let node_key = TrieNode::make_primary_key(prefix, None);

        // We will first attempt to get it from the DB cache
        if let Some(Some(node_bytes)) = self.txn_batch.lock().unwrap().batch.get(&node_key) {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        // Else, get it directly from the DB
        if let Some(node_bytes) = self.db.get(&node_key).ok().flatten() {
            if let Ok(node) = TrieNode::deserialize(&node_bytes) {
                return Some(node);
            }
        }

        None
    }

    pub fn root_hash(&self) -> Result<Vec<u8>, HubError> {
        if let Some(root) = self.root.read().unwrap().as_ref() {
            Ok(root.hash())
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for root_hash".to_string(),
            })
        }
    }

    pub fn get_all_values(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, HubError> {
        if let Some(root) = self.root.write().unwrap().as_mut() {
            if let Some(node) = root.get_node_from_trie(&self.db, prefix, 0) {
                node.get_all_values(&self.db, prefix)
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for get_all_values".to_string(),
            })
        }
    }

    pub fn get_snapshot(&self, prefix: &[u8]) -> Result<TrieSnapshot, HubError> {
        if let Some(root) = self.root.write().unwrap().as_mut() {
            let result = root.get_snapshot(&self.db, prefix, 0);

            result
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: "Merkle Trie not initialized for get_snapshot".to_string(),
            })
        }
    }

    pub fn get_trie_node_metadata(&self, prefix: &[u8]) -> Result<NodeMetadata, HubError> {
        if let Some(node) = self.get_node(prefix) {
            let mut children = HashMap::new();

            for char in node.children().keys() {
                let mut child_prefix = prefix.to_vec();
                child_prefix.push(*char);

                let child_node = self.get_node(&child_prefix).ok_or(HubError {
                    code: "bad_request.internal_error".to_string(),
                    message: "Child Node not found".to_string(),
                })?;

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
            Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "Node not found".to_string(),
            })
        }
    }
}
