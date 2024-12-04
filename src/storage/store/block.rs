use super::super::constants::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::proto::Block;
use crate::storage::constants::RootPrefix;
use crate::storage::db::{PageOptions, RocksDB, RocksdbError};
use prost::Message;
use std::sync::Arc;
use thiserror::Error;

// TODO(aditi): This code definitely needs unit tests
#[derive(Error, Debug)]
pub enum BlockStorageError {
    #[error(transparent)]
    RocksdbError(#[from] RocksdbError),

    #[error("Block missing header")]
    BlockMissingHeader,

    #[error("Block missing height")]
    BlockMissingHeight,

    #[error("Too many blocks in result")]
    TooManyBlocksInResult,
}

/** A page of messages returned from various APIs */
pub struct BlockPage {
    pub blocks: Vec<Block>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_block_key(block_number: u64) -> Vec<u8> {
    // Store the prefix in the first byte so there's no overlap across different stores
    let mut key = vec![RootPrefix::Block as u8];
    // Store the block number in the next 8 bytes
    key.extend_from_slice(&block_number.to_be_bytes());

    key
}

fn get_block_page_by_prefix(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Option<Vec<u8>>,
    stop_prefix: Option<Vec<u8>>,
) -> Result<BlockPage, BlockStorageError> {
    let mut blocks = Vec::new();
    let mut last_key = vec![];

    let start_prefix = match start_prefix {
        None => make_block_key(0),
        Some(key) => key,
    };

    let stop_prefix = match stop_prefix {
        None => {
            // Covers everything up to the end of the shard keys
            vec![RootPrefix::Block as u8 + 1]
        }
        Some(key) => key,
    };

    db.for_each_iterator_by_prefix_paged(
        Some(start_prefix),
        Some(stop_prefix),
        page_options,
        |key, value| {
            let block = Block::decode(value).map_err(|e| HubError::from(e))?;
            blocks.push(block);

            if blocks.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                last_key = key.to_vec();
                return Ok(true); // Stop iterating
            }

            Ok(false) // Continue iterating
        },
    )
    .map_err(|_| BlockStorageError::TooManyBlocksInResult)?; // TODO: Return the right error

    let next_page_token = if last_key.len() > 0 {
        Some(last_key)
    } else {
        None
    };

    Ok(BlockPage {
        blocks,
        next_page_token,
    })
}

pub fn get_last_block(db: &RocksDB) -> Result<Option<Block>, BlockStorageError> {
    let start_block_key = make_block_key(0);
    let block_page = get_block_page_by_prefix(
        db,
        &PageOptions {
            reverse: true,
            page_size: Some(1),
            page_token: None,
        },
        Some(start_block_key),
        None,
    )?;

    if block_page.blocks.len() > 1 {
        return Err(BlockStorageError::TooManyBlocksInResult);
    }

    Ok(block_page.blocks.get(0).cloned())
}

pub fn get_current_height(db: &RocksDB) -> Result<Option<u64>, BlockStorageError> {
    let last_block = get_last_block(db)?;
    match last_block {
        None => Ok(None),
        Some(block) => match block.header {
            None => Ok(None),
            Some(header) => match header.height {
                None => Ok(None),
                Some(height) => Ok(Some(height.block_number)),
            },
        },
    }
}

pub fn get_blocks_in_range(
    db: &RocksDB,
    page_options: &PageOptions,
    start_block_number: u64,
    stop_block_number: Option<u64>,
) -> Result<BlockPage, BlockStorageError> {
    let start_primary_key = make_block_key(start_block_number);
    let stop_prefix = stop_block_number.map(|block_number| make_block_key(block_number));

    get_block_page_by_prefix(db, page_options, Some(start_primary_key), stop_prefix)
}

pub fn put_block(db: &RocksDB, block: Block) -> Result<(), BlockStorageError> {
    // TODO: We need to introduce a transaction model
    let mut txn = db.txn();
    let header = block
        .header
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeader)?;
    let height = header
        .height
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeight)?;
    let primary_key = make_block_key(height.block_number);
    txn.put(primary_key, block.encode_to_vec());
    db.commit(txn)?;
    Ok(())
}

#[derive(Default, Clone)]
pub struct BlockStore {
    pub db: Arc<RocksDB>,
}

impl BlockStore {
    pub fn new(db: Arc<RocksDB>) -> BlockStore {
        BlockStore { db }
    }

    pub fn put_block(&self, block: Block) -> Result<(), BlockStorageError> {
        put_block(&self.db, block)
    }

    pub fn get_last_block(&self) -> Result<Option<Block>, BlockStorageError> {
        get_last_block(&self.db)
    }

    pub fn max_block_number(&self) -> Result<u64, BlockStorageError> {
        let current_height = get_current_height(&self.db)?;
        match current_height {
            None => Ok(0),
            Some(height) => Ok(height),
        }
    }

    pub fn get_blocks(
        &self,
        start_block_number: u64,
        stop_block_number: Option<u64>,
        page_options: &PageOptions,
    ) -> Result<BlockPage, BlockStorageError> {
        get_blocks_in_range(
            &self.db,
            page_options,
            start_block_number,
            stop_block_number,
        )
    }
}
