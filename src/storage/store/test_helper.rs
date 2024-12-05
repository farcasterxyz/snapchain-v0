use crate::storage::db;
use crate::storage::store::engine::ShardEngine;
use crate::storage::store::stores::StoreLimits;
use crate::storage::trie::merkle_trie;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use ed25519_dalek::{SecretKey, SigningKey};
use std::sync::Arc;
use tempfile;

use crate::proto::onchain_event;
use crate::proto::onchain_event::OnChainEvent;
use crate::proto::snapchain::{Height, ShardChunk, ShardHeader, Transaction};
use crate::storage::store::engine::{MempoolMessage, ShardStateChange};
use crate::utils::factory::{events_factory, username_factory};
use hex::FromHex;

pub const FID_FOR_TEST: u32 = 1234;

#[allow(dead_code)] // TODO
pub const FID2_FOR_TEST: u32 = 1235;

pub mod limits {
    use crate::storage::store::stores::Limits;

    pub fn zero() -> Limits {
        Limits {
            casts: 0,
            links: 0,
            reactions: 0,
            user_data: 0,
            user_name_proofs: 0,
            verifications: 0,
        }
    }

    pub fn test() -> Limits {
        Limits {
            casts: 4,
            links: 4,
            reactions: 3,
            user_data: 4,
            user_name_proofs: 2,
            verifications: 2,
        }
    }

    pub fn unlimited() -> Limits {
        Limits {
            casts: u32::MAX,
            links: u32::MAX,
            reactions: u32::MAX,
            user_data: u32::MAX,
            user_name_proofs: u32::MAX,
            verifications: u32::MAX,
        }
    }
}

pub struct EngineOptions {
    pub limits: Option<StoreLimits>,
}

pub fn new_engine_with_options(options: EngineOptions) -> (ShardEngine, tempfile::TempDir) {
    let statsd_client = StatsdClientWrapper::new(
        cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
        true,
    );
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("a.db");

    let db = db::RocksDB::new(db_path.to_str().unwrap());
    db.open().unwrap();

    let test_limits = options.limits.unwrap_or(StoreLimits {
        limits: limits::test(),
        legacy_limits: limits::zero(),
    });

    (
        ShardEngine::new(
            Arc::new(db),
            merkle_trie::MerkleTrie::new(16).unwrap(),
            1,
            test_limits,
            statsd_client,
            256,
        ),
        dir,
    )
}

#[allow(dead_code)] // TODO
pub fn new_engine() -> (ShardEngine, tempfile::TempDir) {
    new_engine_with_options(EngineOptions { limits: None })
}

pub async fn commit_event(engine: &mut ShardEngine, event: &OnChainEvent) -> ShardChunk {
    let messages_tx = engine.messages_tx();

    messages_tx
        .send(MempoolMessage::ValidatorMessage(
            crate::proto::snapchain::ValidatorMessage {
                on_chain_event: Some(event.clone()),
                fname_transfer: None,
            },
        ))
        .await
        .unwrap();
    let state_change = engine.propose_state_change(1);

    validate_and_commit_state_change(engine, &state_change)
}

pub fn default_shard_chunk() -> ShardChunk {
    ShardChunk {
        header: Some(ShardHeader::default()),
        // TODO: eventually we won't hardcode one transaction here
        transactions: vec![Transaction {
            user_messages: vec![],
            system_messages: vec![],
            fid: FID_FOR_TEST as u64,
            account_root: vec![5, 5, 6, 6], //TODO,
        }],
        hash: vec![],
        votes: None,
    }
}

pub fn state_change_to_shard_chunk(
    shard_index: u32,
    block_number: u64,
    change: &ShardStateChange,
) -> ShardChunk {
    let mut chunk = default_shard_chunk();

    chunk.header.as_mut().unwrap().shard_root = change.new_state_root.clone();
    chunk.header.as_mut().unwrap().height = Some(Height {
        shard_index,
        block_number,
    });
    chunk.transactions = change.transactions.clone();
    chunk
}

pub fn validate_and_commit_state_change(
    engine: &mut ShardEngine,
    state_change: &ShardStateChange,
) -> ShardChunk {
    let valid = engine.validate_state_change(state_change);
    assert!(valid);

    let height = engine.get_confirmed_height();
    let chunk = state_change_to_shard_chunk(1, height.block_number + 1, state_change);
    engine.commit_shard_chunk(&chunk);
    assert_eq!(state_change.new_state_root, engine.trie_root_hash());
    chunk
}

pub fn default_storage_event(fid: u32) -> OnChainEvent {
    events_factory::create_rent_event(fid, None, Some(1), false)
}

pub async fn register_user(fid: u32, signer: SigningKey, engine: &mut ShardEngine) {
    commit_event(engine, &default_storage_event(fid)).await;
    let id_register_event =
        events_factory::create_id_register_event(fid, onchain_event::IdRegisterEventType::Register);
    commit_event(engine, &id_register_event).await;
    let signer_event =
        events_factory::create_signer_event(fid, signer, onchain_event::SignerEventType::Add);
    commit_event(engine, &signer_event).await;
}

#[allow(dead_code)] // This is used by tests
pub async fn register_fname(fid: u32, username: &String, engine: &mut ShardEngine) {
    let messages_tx = engine.messages_tx();
    let fname_transfer = username_factory::create_transfer(fid, username);
    messages_tx
        .send(MempoolMessage::ValidatorMessage(
            crate::proto::snapchain::ValidatorMessage {
                on_chain_event: None,
                fname_transfer: Some(fname_transfer),
            },
        ))
        .await
        .unwrap();
    let state_change = engine.propose_state_change(1);

    validate_and_commit_state_change(engine, &state_change);
}

pub fn default_signer() -> SigningKey {
    SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    )
}
