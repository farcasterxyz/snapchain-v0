use super::super::db::{RocksDB, RocksDbTransactionBatch};
use super::errors::TrieError;
use super::trie_node::{TrieNode, TIMESTAMP_LENGTH};
use crate::proto;
use crate::storage::store::account::{make_fid_key, IntoU8};
use crate::storage::trie::{trie_node, util};
use std::collections::HashMap;
use tracing::info;
pub use trie_node::Context;

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";

pub struct TrieKey {}

impl TrieKey {
    pub fn for_message(msg: &proto::Message) -> Vec<u8> {
        let mut key = Self::for_message_type(msg.fid(), msg.msg_type().into_u8());
        key.extend_from_slice(&msg.hash);
        key
    }

    pub fn for_message_type(fid: u64, msg_type: u8) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(fid));
        // Left shift msg_ype by 3 bits so we don't collide with onchain event types.
        // Supports 8 reserved types (onchain events, fnames etc) and 32 message types
        key.push(msg_type << 3);
        key
    }

    pub fn for_onchain_event(event: &proto::OnChainEvent) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(event.fid));
        key.push(event.r#type as u8);
        key.extend_from_slice(&event.transaction_hash);
        key.extend_from_slice(&event.log_index.to_be_bytes());
        key
    }

    pub fn for_fname(fid: u64, name: &String) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&Self::for_fid(fid));
        key.push(7); // 1-6 is for onchain events, use 7 for fnames, and everything else for messages
        key.extend_from_slice(&name.as_bytes());
        key
    }

    pub fn for_fid(fid: u64) -> Vec<u8> {
        make_fid_key(fid)
    }
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
    branch_xform: util::BranchingFactorTransform,
    root: Option<TrieNode>,
    branching_factor: u32,
}

impl MerkleTrie {
    pub fn new(branching_factor: u32) -> Result<Self, TrieError> {
        let branch_xform = util::get_transform_functions(branching_factor)
            .ok_or(TrieError::UnknownBranchingFactor)?;

        Ok(MerkleTrie {
            root: None,
            branch_xform,
            branching_factor,
        })
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
        ctx: &Context,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(self.branch_xform.expand).collect();

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
            // root_stub_map: the hash of a node is stored/cached in the parent of that node (in a hashmap)
            // the root doesn't have a parent, but still needs a hashmap here to satisfy the API. Note also
            // that the root's hash will NOT be populated anywhere in this map. Use root.hash() for that.
            let mut root_stub_map = HashMap::new();
            let results = root.insert(ctx, &mut root_stub_map, db, &mut txn, keys, 0)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn delete(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, TrieError> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(self.branch_xform.expand).collect();

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
            // root_stub_map: see comment in insert()
            let root_stub_map = &mut HashMap::new();
            let results = root.delete(ctx, root_stub_map, db, &mut txn, keys, 0)?;

            txn_batch.merge(txn);
            Ok(results)
        } else {
            Err(TrieError::TrieNotInitialized)
        }
    }

    pub fn exists(
        &mut self,
        ctx: &Context,
        db: &RocksDB,
        key: &Vec<u8>,
    ) -> Result<bool, TrieError> {
        let key: Vec<u8> = (self.branch_xform.expand)(key.clone());

        if let Some(root) = self.root.as_mut() {
            root.exists(ctx, db, &key, 0)
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

    fn get_node(
        &self,
        db: &RocksDB,
        txn_batch: &mut RocksDbTransactionBatch,
        prefix: &[u8],
    ) -> Option<TrieNode> {
        let prefix = (self.branch_xform.expand)(prefix.to_vec());
        let node_key = TrieNode::make_primary_key(&prefix, None);

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
        self.get_node(db, txn_batch, prefix)
            .map(|node| node.hash())
            .unwrap_or(vec![])
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
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, TrieError> {
        let prefix = (self.branch_xform.expand)(prefix.to_vec());

        if let Some(root) = self.root.as_mut() {
            if let Some(node) = root.get_node_from_trie(ctx, db, &prefix, 0) {
                match node.get_all_values(ctx, db, &prefix) {
                    Ok(values) => Ok(values.into_iter().map(self.branch_xform.combine).collect()),
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
        ctx: &Context,
        db: &RocksDB,
        prefix: &[u8],
    ) -> Result<TrieSnapshot, TrieError> {
        if let Some(root) = self.root.as_mut() {
            root.get_snapshot(ctx, db, prefix, 0)
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

    pub fn branching_factor(&self) -> u32 {
        self.branching_factor
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::trie::errors::TrieError;
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie};

    #[test]
    fn test_merkle_trie_get_node() {
        let ctx = &Context::new();

        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        // TODO: this test needs to be able to work with different branching factors
        let mut trie = MerkleTrie::new(256).unwrap();
        trie.initialize(db).unwrap();
        let mut txn_batch = RocksDbTransactionBatch::new();

        let result = trie.insert(
            ctx,
            db,
            &mut txn_batch,
            vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9]],
        );
        assert!(result.is_err());
        if let Err(TrieError::KeyLengthTooShort) = result {
            //ok
        } else {
            panic!("Unexpected error type");
        }

        let key1: Vec<_> = "0000482712".bytes().collect();
        trie.insert(ctx, db, &mut txn_batch, vec![key1.clone()])
            .unwrap();

        let node = trie.get_node(db, &mut txn_batch, &key1).unwrap();
        assert_eq!(node.value().unwrap(), key1);

        // Add another key
        let key2: Vec<_> = "0000482713".bytes().collect();
        trie.insert(ctx, db, &mut txn_batch, vec![key2.clone()])
            .unwrap();

        // The get node should still work for both keys
        let node = trie.get_node(db, &mut txn_batch, &key1).unwrap();
        assert_eq!(node.value().unwrap(), key1);
        let node = trie.get_node(db, &mut txn_batch, &key2).unwrap();
        assert_eq!(node.value().unwrap(), key2);

        // Getting the node with first 9 bytes should return the node with key1
        let common_node = trie
            .get_node(db, &mut txn_batch, &key1[0..9].to_vec())
            .unwrap();
        assert_eq!(common_node.is_leaf(), false);
        assert_eq!(common_node.children().len(), 2);
        let mut children_keys: Vec<_> = common_node.children().keys().collect();
        children_keys.sort();

        assert_eq!(*children_keys[0], key1[9]);
        assert_eq!(*children_keys[1], key2[9]);

        // Get the metadata for the root node
        let root_metadata = trie
            .get_trie_node_metadata(db, &mut txn_batch, &key1[0..1])
            .unwrap();
        assert_eq!(root_metadata.prefix, "0".bytes().collect::<Vec<_>>());
        assert_eq!(root_metadata.num_messages, 2);
        assert_eq!(root_metadata.children.len(), 1);

        let metadata = trie
            .get_trie_node_metadata(db, &mut txn_batch, &key1[0..9])
            .unwrap();

        // Get the children
        let mut children = metadata
            .children
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect::<Vec<_>>();
        children.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(children[0].0, key1[9]);
        assert_eq!(children[0].1.prefix, key1);
        assert_eq!(children[0].1.num_messages, 1);

        assert_eq!(children[1].0, key2[9]);
        assert_eq!(children[1].1.prefix, key2);
        assert_eq!(children[1].1.num_messages, 1);

        db.close();

        // Clean up
        std::fs::remove_dir_all(&tmp_path).unwrap();
    }
}
