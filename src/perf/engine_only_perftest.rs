use crate::proto::{Height, ShardChunk, ShardHeader};
use crate::storage::store::engine::{MempoolMessage, ShardStateChange};
use crate::storage::store::stores::StoreLimits;
use crate::storage::store::test_helper;
use crate::utils::cli::compose_message;
use std::error::Error;
use std::time::Duration;

fn state_change_to_shard_chunk(
    shard_index: u32,
    block_number: u64,
    change: &ShardStateChange,
) -> ShardChunk {
    ShardChunk {
        header: Some(ShardHeader {
            shard_root: change.new_state_root.clone(),
            height: Some(Height {
                shard_index,
                block_number,
            }),
            timestamp: 0,
            parent_hash: vec![], // TODO
        }),
        transactions: change.transactions.clone(),
        hash: vec![],
        votes: None,
    }
}

pub async fn run() -> Result<(), Box<dyn Error>> {
    let (mut engine, _tmpdir) = test_helper::new_engine_with_options(test_helper::EngineOptions {
        limits: Some(StoreLimits {
            limits: test_helper::limits::unlimited(),
            legacy_limits: test_helper::limits::unlimited(),
        }),
        db_name: None,
    });

    let mut i = 0;
    let messages_tx = engine.messages_tx();

    let fid = test_helper::FID_FOR_TEST;

    test_helper::register_user(
        fid,
        test_helper::default_signer(),
        test_helper::default_custody_address(),
        &mut engine,
    )
    .await;

    loop {
        for _ in 0..100 {
            let text = format!("For benchmarking {}", i);
            let msg = compose_message(fid, text.as_str(), None, None);

            messages_tx
                .send(MempoolMessage::UserMessage(msg.clone()))
                .await
                .unwrap();
            i += 1;
        }

        let messages = engine.pull_messages(Duration::from_millis(50)).await?;
        let state_change = engine.propose_state_change(1, messages);

        let valid = engine.validate_state_change(&state_change);
        assert!(valid);

        // TODO: need block height below
        let chunk = state_change_to_shard_chunk(1, 1, &state_change);
        engine.commit_shard_chunk(&chunk);

        println!("{}", engine.trie_num_items());
    }
}
