use crate::perf::generate::{MessageGenerator, NextMessage};
use crate::proto;
use crate::storage::store::test_helper;
use crate::utils::cli;
use crate::utils::factory::events_factory;
use ed25519_dalek::SigningKey;

pub struct SingleUser {
    private_key: SigningKey,
    initialized: bool,
    thread_id: u32,
}

impl SingleUser {
    pub fn new(thread_id: u32) -> Self {
        Self {
            initialized: false,
            private_key: test_helper::default_signer(),
            thread_id,
        }
    }
}

impl MessageGenerator for SingleUser {
    fn next(&mut self, seq: u64) -> Vec<NextMessage> {
        let mut messages = Vec::new();

        let fid = (self.thread_id * 1_000_000) as u64;

        if !self.initialized {
            self.initialized = true;
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
                    self.private_key.clone(),
                    proto::SignerEventType::Add,
                    None,
                ),
            ));
        }

        let text = format!("For benchmarking {}", seq);
        let msg = cli::compose_message(fid, &text, None, Some(&self.private_key));
        messages.push(NextMessage::Message(msg));

        messages
    }
}

pub fn assert_send<T: Send>() {}
