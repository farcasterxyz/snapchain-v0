use crate::proto::snapchain::Block;
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch, RocksdbError};
use prost::Message;
use serde::Deserialize;
use thiserror::Error;

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

pub const PAGE_SIZE_MAX: usize = 10_000;
/** A page of messages returned from various APIs */
pub struct BlockPage {
    pub blocks: Vec<Block>,
    pub next_page_token: Option<Vec<u8>>,
}

fn make_primary_key(shard_index: u32, block_number: u64) -> Vec<u8> {
    let mut key = [0u8; 12];
    // Store the shard index as big-endian in the first 4 bytes
    key[0..4].copy_from_slice(&shard_index.to_be_bytes());
    // Store the block number in the remaining 8 bytes
    key[4..12].copy_from_slice(&block_number.to_be_bytes());

    key.to_vec()
}

fn get_block_page_by_prefix(
    db: &RocksDB,
    page_options: &PageOptions,
    start_prefix: Option<Vec<u8>>,
    stop_prefix: Option<Vec<u8>>,
) -> Result<BlockPage, BlockStorageError> {
    let mut blocks = Vec::new();
    let mut last_key = vec![];

    db.for_each_iterator_by_prefix(start_prefix, stop_prefix, page_options, |key, value| {
        let block = Block::decode(value)?;
        blocks.push(block);

        if blocks.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
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

    Ok(BlockPage {
        blocks,
        next_page_token,
    })
}

pub fn get_current_height(
    db: &RocksDB,
    shard_index: u32,
) -> Result<Option<u64>, BlockStorageError> {
    let start_primary_key = make_primary_key(shard_index, 0);
    let block_page = get_block_page_by_prefix(
        db,
        &PageOptions {
            reverse: true,
            page_size: Some(1),
            page_token: None,
        },
        Some(start_primary_key),
        None,
    )?;

    if block_page.blocks.len() > 1 {
        return Err(BlockStorageError::TooManyBlocksInResult);
    }

    match block_page.blocks.get(0).cloned() {
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
    shard_index: u32,
    start_block_number: u64,
    stop_block_number: Option<u64>,
) -> Result<BlockPage, BlockStorageError> {
    let start_primary_key = make_primary_key(shard_index, start_block_number);
    let stop_prefix =
        stop_block_number.map(|block_number| make_primary_key(shard_index, block_number));

    get_block_page_by_prefix(db, page_options, Some(start_primary_key), stop_prefix)
}

pub fn put_block(db: &RocksDB, block: Block) -> Result<(), BlockStorageError> {
    let mut txn = db.txn();
    let header = block
        .header
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeader)?;
    let height = header
        .height
        .as_ref()
        .ok_or(BlockStorageError::BlockMissingHeight)?;
    let primary_key = make_primary_key(height.shard_index, height.block_number);
    txn.put(primary_key, block.encode_to_vec());
    db.commit(txn)?;
    Ok(())
}
