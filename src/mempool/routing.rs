use crate::core::types::FidOnDisk;
use sha2::{Digest, Sha256};

pub trait MessageRouter: Send + Sync {
    fn route_message(&self, fid: u64, num_shards: u32) -> u32;
}

pub struct ShardRouter {}

impl MessageRouter for ShardRouter {
    fn route_message(&self, fid: u64, num_shards: u32) -> u32 {
        let hash = Sha256::digest((fid as FidOnDisk).to_be_bytes());
        let hash_u32 = u32::from_be_bytes(hash[..4].try_into().unwrap());
        (hash_u32 % num_shards) + 1
    }
}

// Meant only for tests
pub struct EvenOddRouterForTest {}
impl MessageRouter for EvenOddRouterForTest {
    fn route_message(&self, fid: u64, num_shards: u32) -> u32 {
        if num_shards > 2 {
            panic!("EvenOddRouterForTest only supports 2 shards");
        }
        // Event fids go to the even shard (2), and odd fids go to the odd shard (1)
        if fid % 2 == 0 {
            2
        } else {
            1
        }
    }
}
