use crate::storage::trie::trie_node::TrieNode;
use crate::storage::util::blake3_20;
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

fn dfs_print_node(
    db: &db::RocksDB,
    prefix: &[u8],
    node: &TrieNode,
    depth: usize,
) -> Result<(), trie::errors::TrieError> {
    let indent = "  ".repeat(depth);
    let key_str = encode_with_spaces(node.key_ref().unwrap_or(&vec![]));
    let mut child_hashes_str = String::new();
    for (k, v) in node.child_hashes.iter() {
        child_hashes_str += &format!("{k}:{} ", hex::encode(v));
    }

    let last_char = prefix.last().copied().unwrap_or_default();
    let hash1 = compute_node_hash(node);

    println!(
        "{}d={} 0x{:02x} prefix=0x{} key=[{}] hash0={} hash1={} items={} child_hashes={}",
        indent,
        depth,
        last_char,
        hex::encode(prefix),
        key_str,
        hex::encode(node.hash()),
        hex::encode(hash1),
        node.items(),
        child_hashes_str,
    );

    // Sort child chars to have a stable traversal order
    let mut children_chars: Vec<u8> = node.children().keys().copied().collect();
    children_chars.sort();

    for c in children_chars {
        // Construct the child's prefix
        let mut child_prefix = prefix.to_vec();
        child_prefix.push(c);

        match node.children().get(&c).unwrap() {
            trie::trie_node::TrieNodeType::Node(child_node) => {
                panic!("shouldn't ever have a regular node");
            }
            trie::trie_node::TrieNodeType::Serialized(_) => {
                // We have a serialized child, need to load it from the database
                let child_key = TrieNode::make_primary_key(prefix, Some(c));
                if let Some(child_data) = db
                    .get(&child_key)
                    .map_err(trie::errors::TrieError::wrap_database)?
                {
                    let child_node = TrieNode::deserialize(&child_data)?;
                    dfs_print_node(db, &child_prefix, &child_node, depth + 1)?;
                } else {
                    println!(
                        "{}  Missing data for serialized child at prefix=0x{}",
                        indent,
                        hex::encode(&child_prefix)
                    );
                }
            }
        }
    }

    Ok(())
}

/// Loads the root node from the database and prints out the entire trie using DFS.
pub fn print_entire_trie_dfs(db: &db::RocksDB) -> Result<(), trie::errors::TrieError> {
    let root_key = TrieNode::make_primary_key(&[], None);
    let root_data = match db
        .get(&root_key)
        .map_err(trie::errors::TrieError::wrap_database)?
    {
        Some(data) => data,
        None => {
            println!("No root node found in the database.");
            return Ok(());
        }
    };

    let root_node = TrieNode::deserialize(&root_data)?;

    println!("Starting DFS print of the entire trie:");
    dfs_print_node(db, &[], &root_node, 0)
}

// Helper function to format bytes with spaces in hex
fn encode_with_spaces(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<_>>()
        .join(" ")
}

fn compute_node_hash(node: &TrieNode) -> Vec<u8> {
    if node.is_leaf() {
        return blake3_20(node.key_ref().unwrap_or(&[]));
    }

    let mut chars: Vec<u8> = node.child_hashes.keys().copied().collect();
    chars.sort();

    let mut concat_hashes = vec![];
    for c in chars {
        if let Some(child_hash) = node.child_hashes.get(&c) {
            concat_hashes.extend_from_slice(child_hash);
        }
    }

    blake3_20(&concat_hashes)
}
