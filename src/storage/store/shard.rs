use crate::proto::snapchain::{Block, ShardChunk, Transaction};
use crate::storage::db::{PageOptions, RocksDB, RocksdbError};
use crate::storage::store::block::RootPrefix;
use prost::Message;
use std::sync::Arc;
use thiserror::Error;

use super::utils::PAGE_SIZE_MAX;

// TODO(aditi): This code definitely needs unit tests
#[derive(Error, Debug)]
pub enum ShardStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error("Shard missing header")]
    ShardMissingHeader,

    #[error("Shard missing height")]
    ShardMissingHeight,

    #[error("Too many shards in result")]
    TooManyShardsInResult,
}

/** A page of messages returned from various APIs */
pub struct ShardPage {
    pub shard_chunks: Vec<ShardChunk>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_shard_key(block_number: u64) -> Vec<u8> {
    // Store the prefix in the first byte so there's no overlap across different stores
    let mut key = vec![RootPrefix::Shard as u8];

    // Store the block number in the next 8 bytes
    key.extend_from_slice(&block_number.to_be_bytes());

    key
}

fn get_shard_page_by_prefix(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Option<Vec<u8>>,
    stop_prefix: Option<Vec<u8>>,
) -> Result<ShardPage, ShardStorageError> {
    let mut shard_chunks = Vec::new();
    let mut last_key = vec![];

    db.for_each_iterator_by_prefix_paged(start_prefix, stop_prefix, page_options, |key, value| {
        let block = ShardChunk::decode(value)?;
        shard_chunks.push(block);

        if shard_chunks.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
            last_key = key.to_vec();
            return Ok(true); // Stop iterating
        }

        Ok(false) // Continue iterating
    })?;

    let next_page_token = if last_key.len() > 0 {
        Some(last_key)
    } else {
        None
    };

    Ok(ShardPage {
        shard_chunks,
        next_page_token,
    })
}

pub fn get_current_height(db: &RocksDB) -> Result<Option<u64>, ShardStorageError> {
    let start_block_key = make_shard_key(0);
    let shard_page = get_shard_page_by_prefix(
        db,
        &PageOptions {
            reverse: true,
            page_size: Some(1),
            page_token: None,
        },
        Some(start_block_key),
        None,
    )?;

    if shard_page.shard_chunks.len() > 1 {
        return Err(ShardStorageError::TooManyShardsInResult);
    }

    match shard_page.shard_chunks.get(0).cloned() {
        None => Ok(None),
        Some(shard_chunk) => match shard_chunk.header {
            None => Ok(None),
            Some(header) => match header.height {
                None => Ok(None),
                Some(height) => Ok(Some(height.block_number)),
            },
        },
    }
}

pub fn put_shard_chunk(db: &RocksDB, shard_chunk: ShardChunk) -> Result<(), ShardStorageError> {
    // TODO: We need to introduce a transaction model
    let mut txn = db.txn();
    let header = shard_chunk
        .header
        .as_ref()
        .ok_or(ShardStorageError::ShardMissingHeader)?;
    let height = header
        .height
        .as_ref()
        .ok_or(ShardStorageError::ShardMissingHeight)?;
    let primary_key = make_shard_key(height.block_number);
    txn.put(primary_key, shard_chunk.encode_to_vec());
    db.commit(txn)?;
    Ok(())
}

#[derive(Default)]
pub struct ShardStore {
    pub db: Arc<RocksDB>, // TODO: pub and Arc are temporary to allow trie to use
}

impl ShardStore {
    pub fn new(db: RocksDB) -> ShardStore {
        ShardStore { db: Arc::new(db) }
    }

    pub fn put_shard_chunk(&self, shard_chunk: ShardChunk) -> Result<(), ShardStorageError> {
        put_shard_chunk(&self.db, shard_chunk)
    }

    pub fn max_block_number(&self) -> Result<u64, ShardStorageError> {
        let current_height = get_current_height(&self.db)?;
        match current_height {
            None => Ok(0),
            Some(height) => Ok(height),
        }
    }
}
