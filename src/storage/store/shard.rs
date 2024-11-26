use super::super::constants::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::proto::snapchain::ShardChunk;
use crate::storage::constants::RootPrefix;
use crate::storage::db::{PageOptions, RocksDB, RocksdbError};
use prost::Message;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

static PAGE_SIZE: usize = 100;

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

    #[error("Hub error")]
    HubError,
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

    let start_prefix = match start_prefix {
        None => make_shard_key(0),
        Some(key) => key,
    };

    let stop_prefix = match stop_prefix {
        None => {
            // Covers everything up to the end of the shard keys
            vec![RootPrefix::Shard as u8 + 1]
        }
        Some(key) => key,
    };

    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let shard_chunk = ShardChunk::decode(value).map_err(|e| HubError::from(e))?;

            shard_chunks.push(shard_chunk);

            if shard_chunks.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|e| ShardStorageError::HubError)?; // TODO: Return the right error

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

pub fn get_last_shard_chunk(db: &RocksDB) -> Result<Option<ShardChunk>, ShardStorageError> {
    let start_shard_key = make_shard_key(0);
    let shard_page = get_shard_page_by_prefix(
        db,
        &PageOptions {
            reverse: true,
            page_size: Some(1),
            page_token: None,
        },
        Some(start_shard_key),
        None,
    )?;

    if shard_page.shard_chunks.len() > 1 {
        return Err(ShardStorageError::TooManyShardsInResult);
    }

    Ok(shard_page.shard_chunks.get(0).cloned())
}

pub fn get_current_height(db: &RocksDB) -> Result<Option<u64>, ShardStorageError> {
    let shard_chunk = get_last_shard_chunk(db)?;
    match shard_chunk {
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

pub fn put_shard_chunk(db: &RocksDB, shard_chunk: &ShardChunk) -> Result<(), ShardStorageError> {
    // TODO: We need to introduce a transaction model
    let header = shard_chunk
        .header
        .as_ref()
        .ok_or(ShardStorageError::ShardMissingHeader)?;
    let height = header
        .height
        .as_ref()
        .ok_or(ShardStorageError::ShardMissingHeight)?;
    let primary_key = make_shard_key(height.block_number);
    db.put(&primary_key, shard_chunk.encode_to_vec().as_slice())?;
    Ok(())
}

pub fn get_shard_chunks_in_range(
    db: &RocksDB,
    page_options: &PageOptions,
    start_block_number: u64,
    stop_block_number: Option<u64>,
) -> Result<ShardPage, ShardStorageError> {
    let start_primary_key = make_shard_key(start_block_number);
    let stop_prefix = stop_block_number.map(|block_number| make_shard_key(block_number));

    get_shard_page_by_prefix(db, page_options, Some(start_primary_key), stop_prefix)
}

#[derive(Default, Clone)]
pub struct ShardStore {
    pub db: Arc<RocksDB>, // TODO: pub and Arc are temporary to allow trie to use
}

impl ShardStore {
    pub fn new(db: Arc<RocksDB>) -> ShardStore {
        ShardStore { db }
    }

    pub fn put_shard_chunk(&self, shard_chunk: &ShardChunk) -> Result<(), ShardStorageError> {
        put_shard_chunk(&self.db, shard_chunk)
    }

    pub fn get_last_shard_chunk(&self) -> Result<Option<ShardChunk>, ShardStorageError> {
        get_last_shard_chunk(&self.db)
    }

    pub fn max_block_number(&self) -> Result<u64, ShardStorageError> {
        let current_height = get_current_height(&self.db)?;
        match current_height {
            None => Ok(0),
            Some(height) => Ok(height),
        }
    }

    pub fn get_shard_chunks(
        &self,
        start_block_number: u64,
        stop_block_number: Option<u64>,
    ) -> Result<Vec<ShardChunk>, ShardStorageError> {
        let mut shard_chunks = vec![];
        let mut next_page_token = None;
        loop {
            let shard_page = get_shard_chunks_in_range(
                &self.db,
                &PageOptions {
                    page_size: Some(PAGE_SIZE),
                    page_token: next_page_token,
                    reverse: false,
                },
                start_block_number,
                stop_block_number,
            )?;
            shard_chunks.extend(shard_page.shard_chunks);
            if shard_page.next_page_token.is_none() {
                break;
            } else {
                next_page_token = shard_page.next_page_token
            }
        }

        Ok(shard_chunks)
    }
}
