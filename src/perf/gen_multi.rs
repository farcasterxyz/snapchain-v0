use crate::perf::generate::{MessageGenerator, NextMessage};
use crate::proto;
use crate::storage::store::test_helper;
use crate::utils::cli;
use crate::utils::factory::events_factory;
use ed25519_dalek::SigningKey;
use rand::Rng;
use std::collections::HashSet;

pub struct MultiUser {
    initialized_fids: HashSet<u64>,
    private_key: SigningKey,
    thread_id: u32,
    users_per_shard: u32,
}

impl MultiUser {
    pub fn new(thread_id: u32, users_per_shard: u32) -> Self {
        Self {
            initialized_fids: HashSet::new(),
            private_key: test_helper::default_signer(),
            thread_id,
            users_per_shard,
        }
    }
}

impl MessageGenerator for MultiUser {
    fn next(&mut self, seq: u64) -> Vec<NextMessage> {
        let mut rng = rand::thread_rng();

        let fid: u64 =
            rng.gen_range(1..=self.users_per_shard as u64) + 1_000_000 * self.thread_id as u64;
        let mut messages = Vec::new();

        // If the FID has not been initialized, return initial messages
        if !self.initialized_fids.contains(&fid) {
            let private_key = self.private_key.clone();

            messages.push(NextMessage::OnChainEvent(cli::compose_rent_event(fid)));
            messages.push(NextMessage::OnChainEvent(
                events_factory::create_id_register_event(
                    fid,
                    proto::IdRegisterEventType::Register,
                    vec![],
                    None,
                ),
            ));
            messages.push(NextMessage::OnChainEvent(
                events_factory::create_signer_event(
                    fid,
                    private_key,
                    proto::SignerEventType::Add,
                    None,
                ),
            ));

            self.initialized_fids.insert(fid);
        }

        // Add a benchmarking message
        let text = format!("For benchmarking {}", seq);
        let msg = cli::compose_message(fid, &text, None, Some(&self.private_key));
        messages.push(NextMessage::Message(msg));

        messages
    }
}
