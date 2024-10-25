use malachite_common::Context;

pub trait ShardId
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    fn shard_id(&self) -> u8;
}

#[derive(Clone)]
pub struct SnapchainShard(u8);

impl ShardId for SnapchainShard {
    fn shard_id(&self) -> u8 {
        self.0
    }
}

pub trait ShardedContext {
    type ShardId: ShardId;
}

pub trait SnapchainContext: Context + ShardedContext {}
