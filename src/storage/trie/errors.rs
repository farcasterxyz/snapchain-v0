use crate::storage::db;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrieError {
    #[error("Database operation failed")]
    DatabaseError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Node not found for the given prefix")]
    NodeNotFound { prefix: Vec<u8> },

    #[error("Child not found")]
    ChildNotFound { char: u8, prefix: Vec<u8> },

    #[error("Child is not a node")]
    InvalidChildNode { child_char: u8 },

    #[error("Failed to deserialize trie node")]
    DeserializationError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Merkle Trie is not initialized")]
    TrieNotInitialized,

    #[error("No keys to insert")]
    NoKeysToInsert,

    #[error("Key length exceeded")]
    KeyLengthExceeded,

    #[error("Unable to reload root")]
    UnableToReloadRoot,

    #[error("Key length is too short")]
    KeyLengthTooShort,

    #[error("Unknown branching factor")]
    UnknownBranchingFactor,
}

impl TrieError {
    pub fn wrap_database(err: db::RocksdbError) -> TrieError {
        TrieError::DatabaseError {
            source: Box::new(err),
        }
    }

    pub fn wrap_deserialize(err: prost::DecodeError) -> TrieError {
        TrieError::DeserializationError {
            source: Box::new(err),
        }
    }
}
