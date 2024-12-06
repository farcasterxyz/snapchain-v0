use sha2::{Digest, Sha256};

// TODO: find a better place for this
pub fn route_message(fid: u32, num_shards: u32) -> u32 {
    let hash = Sha256::digest(fid.to_be_bytes());
    let hash_u32 = u32::from_be_bytes(hash[..4].try_into().unwrap());
    (hash_u32 % num_shards) + 1
}
