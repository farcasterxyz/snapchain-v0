#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::trie::errors::TrieError;
    use crate::storage::trie::merkle_trie::{MerkleTrie, TrieKey};
    use crate::utils::factory::{events_factory, messages_factory};

    fn random_hash() -> Vec<u8> {
        (0..32).map(|_| rand::random::<u8>()).collect()
    }

    #[test]
    fn test_merkle_trie_get_node() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        let mut trie = MerkleTrie::new();
        trie.initialize(db).unwrap();
        let mut txn_batch = RocksDbTransactionBatch::new();

        let result = trie.insert(db, &mut txn_batch, vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9]]);
        assert!(result.is_err());
        if let Err(TrieError::KeyLengthTooShort) = result {
            //ok
        } else {
            panic!("Unexpected error type");
        }

        let key1: Vec<_> = "0000482712".bytes().collect();
        println!("{:?}", key1);
        trie.insert(db, &mut txn_batch, vec![key1.clone()]).unwrap();

        let node = trie.get_node(db, &mut txn_batch, &key1).unwrap();
        assert_eq!(node.value().unwrap(), key1);

        // Add another key
        let key2: Vec<_> = "0000482713".bytes().collect();
        trie.insert(db, &mut txn_batch, vec![key2.clone()]).unwrap();

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

    #[test]
    fn test_reload_with_empty_root() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        let mut trie = MerkleTrie::new();
        trie.initialize(db).unwrap();

        let mut txn_batch = RocksDbTransactionBatch::new();
        let hash = random_hash();

        let res = trie.insert(db, &mut txn_batch, vec![hash.clone()]).unwrap();
        assert_eq!(res, vec![true]);

        let res = trie.exists(db, &hash).unwrap();
        assert_eq!(res, true);

        let res = trie.reload(db);
        assert!(res.is_ok());

        // Does not exist after reload
        let res = trie.exists(db, &hash).unwrap();
        assert_eq!(res, false);
    }

    #[test]
    fn test_reload_with_existing_data() {
        let tmp_path = tempfile::tempdir()
            .unwrap()
            .path()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        let db = &RocksDB::new(&tmp_path);
        db.open().unwrap();

        let mut trie = MerkleTrie::new();
        trie.initialize(db).unwrap();

        let mut first_txn = RocksDbTransactionBatch::new();
        let first_hash = random_hash();
        let second_hash = random_hash();

        trie.insert(db, &mut first_txn, vec![first_hash.clone()])
            .unwrap();
        db.commit(first_txn).unwrap();
        trie.reload(db).unwrap();

        let res = trie.exists(db, &first_hash).unwrap();
        assert_eq!(res, true);

        let mut second_txn = RocksDbTransactionBatch::new();
        trie.insert(db, &mut second_txn, vec![second_hash.clone()])
            .unwrap();

        trie.reload(db).unwrap();

        // First hash still exists, but not the second
        let res = trie.exists(db, &first_hash).unwrap();
        assert_eq!(res, true);
        let res = trie.exists(db, &second_hash).unwrap();
        assert_eq!(res, false);
    }

    #[test]
    fn test_trie_key() {
        let fid_key = TrieKey::for_fid(1234);
        assert_eq!(fid_key, (1234u32).to_be_bytes().to_vec());

        let message = messages_factory::casts::create_cast_add(1234, "test", None, None);
        let message_key = TrieKey::for_message(&message);
        assert_eq!(message_key[0..4], TrieKey::for_fid(1234));
        assert_eq!(message_key[4], message.msg_type() as u8);
        assert_eq!(message_key[5..], message.hash);

        let delete_message =
            messages_factory::casts::create_cast_remove(321456, &message.hash, None, None);
        let delete_message_key = TrieKey::for_message(&delete_message);
        assert_eq!(delete_message_key[0..4], TrieKey::for_fid(321456));
        assert_eq!(delete_message_key[4], delete_message.msg_type() as u8);
        assert_eq!(delete_message_key[5..], delete_message.hash);

        let event = events_factory::create_onchain_event(1234);
        let event_key = TrieKey::for_onchain_event(&event);
        assert_eq!(event_key[0..4], TrieKey::for_fid(1234));
        assert_eq!(event_key[4], event.r#type as u8);
        assert_eq!(event_key[5..], event.transaction_hash);
    }
}
