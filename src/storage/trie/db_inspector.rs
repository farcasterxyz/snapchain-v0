use crate::storage::trie::trie_node::TrieNode;
use crate::storage::{db, trie};
use hex;

pub fn inspect_root_node(db: &db::RocksDB) -> Result<(), trie::errors::TrieError> {
    // Construct the primary key for the root node.
    // The root node is stored at a key starting with RootPrefix::SyncMerkleTrieNode and an empty prefix.
    let root_key = TrieNode::make_primary_key(&[], None);

    // Attempt to load the root node from the database.
    let value = db
        .get(&root_key)
        .map_err(trie::errors::TrieError::wrap_database)?;

    if let Some(serialized_node) = value {
        // Deserialize the root node
        let root_node = TrieNode::deserialize(&serialized_node)?;

        // Print out some basic info
        println!("Root node loaded successfully:");
        println!("  Hash: {}", hex::encode(root_node.hash()));
        println!("  Items: {}", root_node.items());
        println!("  Is leaf: {}", root_node.is_leaf());
        println!("  Number of children: {}", root_node.children().len());
    } else {
        println!("No root node found in the database.");
    }

    Ok(())
}
