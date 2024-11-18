#[cfg(test)]
mod tests {
    use crate::proto::message::Message;
    use crate::proto::snapchain::{Height, ShardChunk, ShardHeader, Transaction};
    use crate::storage::db;
    use crate::storage::store::engine::{ShardEngine, ShardStateChange};
    use crate::storage::store::shard::ShardStore;
    use crate::utils::cli;
    use ed25519_dalek::{SecretKey, SigningKey};
    use hex::FromHex;
    use tracing_subscriber::EnvFilter;

    fn new_engine() -> ShardEngine {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        let shard_store = ShardStore::new(db);
        ShardEngine::new(1, shard_store)
    }

    fn enable_logging() {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .try_init();
    }

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    fn to_hex(b: &[u8]) -> String {
        hex::encode(b)
    }

    fn state_change_to_shard_chunk(
        shard_idx: u32,
        block_number: u64,
        change: ShardStateChange,
    ) -> ShardChunk {
        let mut chunk = default_shard_chunk();

        chunk.header.as_mut().unwrap().shard_root = change.new_state_root;
        chunk.header.as_mut().unwrap().height = Some(Height {
            shard_index: shard_idx,
            block_number: block_number,
        });

        //TODO: don't assume 1 transaction
        chunk.transactions[0]
            .user_messages
            .extend(change.transactions[0].user_messages.iter().cloned());

        chunk
    }

    fn default_shard_chunk() -> ShardChunk {
        ShardChunk {
            header: Some(ShardHeader::default()),
            // TODO: eventually we won't hardcode one transaction here
            transactions: vec![Transaction {
                user_messages: vec![],
                system_messages: vec![],
                fid: 1234,
                account_root: vec![5, 5, 6, 6], //TODO,
            }],
            hash: vec![],
            votes: None,
        }
    }

    fn default_message(text: &str) -> Message {
        let private_key = SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        );

        cli::compose_message(private_key, 1234, text, Some(0))
    }

    fn entities() -> (Message, Message) {
        let msg1 = default_message("msg1");
        let msg2 = default_message("msg2");

        assert_eq!(
            "eb1850b43b2dd25935222c9137f5fa71b02b9689",
            to_hex(&msg1.hash),
        );

        assert_eq!(
            "ee0fcb6344d22ea2af4f97859108eb5a3c6650fd",
            to_hex(&msg2.hash),
        );

        (msg1, msg2)
    }

    #[test]
    fn test_engine_basic_propose() {
        let mut engine = new_engine();
        let state_change = engine.propose_state_change(1);

        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(0, state_change.transactions[0].user_messages.len());
        assert_eq!(
            "237b11d0dd9e78994ef2f141c7f170d48bb51d34",
            to_hex(&state_change.new_state_root)
        );
        assert_eq!(
            "237b11d0dd9e78994ef2f141c7f170d48bb51d34",
            to_hex(&engine.trie_root_hash())
        );
    }

    #[test]
    #[should_panic(expected = "hashes don't match")]
    fn test_engine_commit_with_mismatched_hash() {
        let mut engine = new_engine();
        let state_change = engine.propose_state_change(1);

        let mut chunk = default_shard_chunk();
        chunk.header.as_mut().unwrap().shard_root =
            from_hex("ffffffffffffffffffffffffffffffffffffffff");
        engine.commit_shard_chunk(chunk);
    }

    #[test]
    // #[should_panic(expected = "abc123")]
    // which mismatched hash?
    fn test_engine_commit_no_messages_happy_path() {
        let mut engine = new_engine();
        let state_change = engine.propose_state_change(1);
        let expected_roots = vec!["237b11d0dd9e78994ef2f141c7f170d48bb51d34"];

        let chunk = state_change_to_shard_chunk(1, 1, state_change);
        engine.commit_shard_chunk(chunk);

        assert_eq!(expected_roots[0], to_hex(&engine.trie_root_hash()));
    }

    #[tokio::test]
    async fn test_engine_send_messages_one_by_one() {
        enable_logging();
        let (msg1, msg2) = entities();
        let mut engine = new_engine();
        let messages_tx = engine.messages_tx();
        let expected_roots = vec![
            "237b11d0dd9e78994ef2f141c7f170d48bb51d34",
            "8d566fb56cabed2665962a558dd2d4be0b0e4f6c",
            "215cee5fa4850848a9f9f06a93b0ba4da2ff52ef",
        ];

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);
        assert_eq!(height.block_number, 0);

        {
            messages_tx.send(msg1.clone()).await.unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg1.hash));

            assert_eq!(expected_roots[1], to_hex(&state_change.new_state_root));

            let chunk = state_change_to_shard_chunk(1, 1, state_change.clone());
            engine.commit_shard_chunk(chunk);

            assert_eq!(expected_roots[1], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }

        {
            messages_tx.send(msg2.clone()).await.unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg2.hash));

            assert_eq!(expected_roots[2], to_hex(&state_change.new_state_root));

            let chunk = state_change_to_shard_chunk(1, 2, state_change.clone());
            engine.commit_shard_chunk(chunk);

            assert_eq!(expected_roots[2], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 2); // TODO
        }
    }

    #[tokio::test]
    async fn test_engine_send_two_messages() {
        enable_logging();
        let (msg1, msg2) = entities();
        let mut engine = new_engine();
        let messages_tx = engine.messages_tx();
        let expected_roots = vec![
            "237b11d0dd9e78994ef2f141c7f170d48bb51d34",
            "215cee5fa4850848a9f9f06a93b0ba4da2ff52ef",
        ];

        {
            messages_tx.send(msg1.clone()).await.unwrap();
            messages_tx.send(msg2.clone()).await.unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(2, state_change.transactions[0].user_messages.len());

            let prop_msg_1 = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg_1.hash), to_hex(&msg1.hash));

            let prop_msg_2 = &state_change.transactions[0].user_messages[1];
            assert_eq!(to_hex(&prop_msg_2.hash), to_hex(&msg2.hash));

            assert_eq!(expected_roots[1], to_hex(&state_change.new_state_root));

            let chunk = state_change_to_shard_chunk(1, 1, state_change.clone());
            engine.commit_shard_chunk(chunk);

            assert_eq!(expected_roots[1], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }
    }
}
