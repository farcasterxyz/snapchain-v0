use crate::perf::gen_multi::MultiUser;
use crate::perf::gen_single::SingleUser;
use crate::proto;

pub enum NextMessage {
    OnChainEvent(proto::OnChainEvent),
    Message(proto::Message),
}

pub trait MessageGenerator: Send {
    fn next(&mut self, seq: u64) -> Vec<NextMessage>;
}

#[derive(Clone)]
pub enum GeneratorTypes {
    SingleUser,
    MultiUser,
}

pub struct Config {
    pub users_per_shard: u32,
}

pub fn new_generator(
    typ: GeneratorTypes,
    thread_id: u32,
    cfg: Config,
) -> Box<dyn MessageGenerator> {
    match typ {
        GeneratorTypes::SingleUser => Box::new(SingleUser::new(thread_id)),
        GeneratorTypes::MultiUser => Box::new(MultiUser::new(thread_id, cfg.users_per_shard)),
    }
}
