#[cfg(test)]
mod tests {
    use crate::proto::hub_event::{HubEvent, MergeMessageBody};
    use crate::proto::msg as message;
    use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
    use crate::proto::snapchain::{Height, ShardChunk, ShardHeader, Transaction};
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::engine::{MempoolMessage, ShardEngine, ShardStateChange};
    use crate::storage::store::shard::ShardStore;
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::{events_factory, messages_factory};
    use prost::Message as _;
    use std::sync::Arc;
    use tempfile;
    use tracing_subscriber::EnvFilter;

    const FID_FOR_TEST: u32 = 1234;

    fn new_engine() -> (ShardEngine, tempfile::TempDir) {
        let metrics_client =
            Arc::new(cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build());

        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        let shard_store = ShardStore::new(db);
        (ShardEngine::new(1, shard_store, metrics_client), dir)
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

    fn default_shard_chunk() -> ShardChunk {
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

    fn default_message(text: &str) -> message::Message {
        messages_factory::casts::create_cast_add(FID_FOR_TEST, text, Some(0), None)
    }

    fn default_onchain_event() -> OnChainEvent {
        events_factory::create_onchain_event(FID_FOR_TEST)
    }

    fn entities() -> (message::Message, message::Message) {
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

    fn validate_and_commit_state_change(
        engine: &mut ShardEngine,
        state_change: &ShardStateChange,
    ) -> ShardChunk {
        let valid = engine.validate_state_change(state_change);
        assert!(valid);

        let height = engine.get_confirmed_height();
        let chunk = state_change_to_shard_chunk(1, height.block_number + 1, state_change);
        engine.commit_shard_chunk(&chunk);
        chunk
    }

    async fn commit_message(engine: &mut ShardEngine, msg: &message::Message) -> ShardChunk {
        let messages_tx = engine.messages_tx();

        messages_tx
            .send(MempoolMessage::UserMessage(msg.clone()))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);

        validate_and_commit_state_change(engine, &state_change)
    }

    #[tokio::test]
    async fn test_engine_basic_propose() {
        let (mut engine, _tmpdir) = new_engine();
        let (msg1, _) = entities();
        let messages_tx = engine.messages_tx();

        // State root starts empty
        assert_eq!("", to_hex(&engine.trie_root_hash()));

        // Propose empty transaction
        let state_change = engine.propose_state_change(1);
        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 0);
        // No messages so, new state root should be same as before
        assert_eq!("", to_hex(&state_change.new_state_root));
        // Root hash is not updated until commit
        assert_eq!("", to_hex(&engine.trie_root_hash()));

        // Propose a message
        messages_tx
            .send(MempoolMessage::UserMessage(msg1.clone()))
            .await
            .unwrap();

        let state_change = engine.propose_state_change(1);

        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(
            "9d9ff2bff951492db0c42511f23230d6e819ab15",
            to_hex(&state_change.new_state_root)
        );
        // Root hash is not updated until commit
        assert_eq!("", to_hex(&engine.trie_root_hash()));
    }

    #[test]
    #[should_panic(expected = "State change commit failed: merkle trie root hash mismatch")]
    fn test_engine_commit_with_mismatched_hash() {
        let (mut engine, _tmpdir) = new_engine();
        let mut state_change = engine.propose_state_change(1);
        let invalid_hash = from_hex("ffffffffffffffffffffffffffffffffffffffff");

        {
            let valid = engine.validate_state_change(&state_change);
            assert!(valid);
        }

        {
            state_change.new_state_root = invalid_hash.clone();
            let valid = engine.validate_state_change(&state_change);
            assert!(!valid);
        }

        let mut chunk = default_shard_chunk();

        chunk.header.as_mut().unwrap().shard_root = invalid_hash;

        engine.commit_shard_chunk(&chunk);
    }

    #[test]
    fn test_engine_commit_no_messages_happy_path() {
        let (mut engine, _tmpdir) = new_engine();
        let state_change = engine.propose_state_change(1);
        let expected_roots = vec![""];

        validate_and_commit_state_change(&mut engine, &state_change);

        assert_eq!(expected_roots[0], to_hex(&engine.trie_root_hash()));

        let valid = engine.validate_state_change(&state_change);
        assert!(valid);
    }

    #[tokio::test]
    async fn test_engine_commit_with_single_message() {
        // enable_logging();
        let (msg1, _) = entities();
        let (mut engine, _tmpdir) = new_engine();
        let messages_tx = engine.messages_tx();

        messages_tx
            .send(MempoolMessage::UserMessage(msg1.clone()))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);

        assert_eq!(1, state_change.transactions.len());
        assert_eq!(1, state_change.transactions[0].user_messages.len());

        // propose does not write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        assert_eq!(0, casts_result.unwrap().messages_bytes.len());

        // No events are generated either
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(0, events.events.len());

        // And it's not inserted into the trie
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&msg1)), false);

        let valid = engine.validate_state_change(&state_change);
        assert!(valid);

        // validate does not write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        assert_eq!(0, casts_result.unwrap().messages_bytes.len());

        validate_and_commit_state_change(&mut engine, &state_change);

        // commit does write to the store
        let casts_result = engine.get_casts_by_fid(msg1.fid());
        let messages = casts_result.unwrap().messages_bytes;
        assert_eq!(1, messages.len());
        let decoded = message::Message::decode(&*messages[0]).unwrap();
        assert_eq!(to_hex(&msg1.hash), to_hex(&decoded.hash));

        // And events are generated
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(1, events.events.len());
        let generated_event = match events.events[0].clone().body {
            Some(crate::proto::hub_event::hub_event::Body::MergeMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type"),
        };
        assert_eq!(
            to_hex(&msg1.hash),
            to_hex(&generated_event.message.unwrap().hash)
        );

        // The message exists in the trie
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&msg1)), true);
    }

    #[tokio::test]
    async fn test_engine_commit_delete_message() {
        let timestamp = messages_factory::farcaster_time();
        let cast =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", Some(timestamp), None);
        let (mut engine, _tmpdir) = new_engine();

        commit_message(&mut engine, &cast).await;

        // The cast is present in the store and the trie
        let casts_result = engine.get_casts_by_fid(cast.fid());
        let messages = casts_result.unwrap().messages_bytes;
        assert_eq!(1, messages.len());
        let decoded = message::Message::decode(&*messages[0]).unwrap();
        assert_eq!(to_hex(&cast.hash), to_hex(&decoded.hash));
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast)), true);

        // Delete the cast
        let delete_cast = messages_factory::casts::create_cast_remove(
            FID_FOR_TEST,
            &cast.hash,
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &delete_cast).await;

        // The cast is not present in the store
        let casts_result = engine.get_casts_by_fid(FID_FOR_TEST);
        let messages = casts_result.unwrap().messages_bytes;
        assert_eq!(0, messages.len());

        // The cast is not present in the trie, but the remove message is
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast)), false);
        assert_eq!(
            engine.trie_key_exists(&TrieKey::for_message(&delete_cast)),
            true
        );
    }

    #[tokio::test]
    async fn test_account_roots() {
        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", None, None);
        let (mut engine, _tmpdir) = new_engine();

        let txn = &mut RocksDbTransactionBatch::new();
        let account_root = engine
            .trie
            .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let shard_root = engine.trie.root_hash().unwrap();

        // Account root and shard root is empty initially
        assert_eq!(account_root.len(), 0);
        assert_eq!(shard_root.len(), 0);

        commit_message(&mut engine, &cast).await;

        let updated_account_root =
            engine
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let updated_shard_root = engine.trie.root_hash().unwrap();
        // Account root is not empty after a message is committed
        assert_eq!(updated_account_root.len() > 0, true);
        assert_ne!(updated_shard_root, shard_root);

        let another_fid_cast =
            messages_factory::casts::create_cast_add(FID_FOR_TEST + 1, "msg2", None, None);

        commit_message(&mut engine, &another_fid_cast).await;

        let account_root_another_fid =
            engine
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST + 1));
        let account_root_original_fid =
            engine
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let latest_shard_root = engine.trie.root_hash().unwrap();
        // Only the account root for the new fid and the shard root is updated, original fid account root remains the same
        assert_eq!(account_root_another_fid.len() > 0, true);
        assert_eq!(account_root_original_fid, updated_account_root);
        assert_ne!(latest_shard_root, updated_shard_root);
    }

    #[tokio::test]
    async fn test_engine_send_messages_one_by_one() {
        // enable_logging();
        let (msg1, msg2) = entities();
        let (mut engine, _tmpdir) = new_engine();
        let messages_tx = engine.messages_tx();
        let expected_roots = vec![
            "",
            "9d9ff2bff951492db0c42511f23230d6e819ab15",
            "89506154d345eaf8ef9d0bdf5756b08feb97411b",
        ];

        /* note: Hard-coded expected_roots is going to be fragile for some time.
        This is by design during initial development. Any time these hashes change,
        we'll want to investigate and verify that things are working as expected.
        At some point this will become unwieldy and we'll have confidence that our
        state changes are working as expected and at that point feel free to just check
        that the root hash in the merkle trie matches the hash in the state change struct,
        or otherwise refactor to taste.
        */

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);
        assert_eq!(height.block_number, 0);

        {
            messages_tx
                .send(MempoolMessage::UserMessage(msg1.clone()))
                .await
                .unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg1.hash));

            assert_eq!(expected_roots[1], to_hex(&state_change.new_state_root));

            validate_and_commit_state_change(&mut engine, &state_change);

            assert_eq!(expected_roots[1], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }

        {
            messages_tx
                .send(MempoolMessage::UserMessage(msg2.clone()))
                .await
                .unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(1, state_change.transactions[0].user_messages.len());

            let prop_msg = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg.hash), to_hex(&msg2.hash));

            assert_eq!(expected_roots[2], to_hex(&state_change.new_state_root));

            validate_and_commit_state_change(&mut engine, &state_change);

            assert_eq!(expected_roots[2], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 2); // TODO
        }
    }

    #[tokio::test]
    async fn test_engine_send_two_messages() {
        // enable_logging();
        let (msg1, msg2) = entities();
        let (mut engine, _tmpdir) = new_engine();
        let messages_tx = engine.messages_tx();
        let expected_roots = vec!["", "89506154d345eaf8ef9d0bdf5756b08feb97411b"];

        {
            messages_tx
                .send(MempoolMessage::UserMessage(msg1.clone()))
                .await
                .unwrap();
            messages_tx
                .send(MempoolMessage::UserMessage(msg2.clone()))
                .await
                .unwrap();
            let state_change = engine.propose_state_change(1);

            assert_eq!(1, state_change.shard_id);
            assert_eq!(state_change.transactions.len(), 1);
            assert_eq!(2, state_change.transactions[0].user_messages.len());

            let prop_msg_1 = &state_change.transactions[0].user_messages[0];
            assert_eq!(to_hex(&prop_msg_1.hash), to_hex(&msg1.hash));

            let prop_msg_2 = &state_change.transactions[0].user_messages[1];
            assert_eq!(to_hex(&prop_msg_2.hash), to_hex(&msg2.hash));

            assert_eq!(expected_roots[1], to_hex(&state_change.new_state_root));

            validate_and_commit_state_change(&mut engine, &state_change);

            assert_eq!(expected_roots[1], to_hex(&engine.trie_root_hash()));

            let height = engine.get_confirmed_height();
            assert_eq!(height.shard_index, 1);
            // assert_eq!(height.block_number, 1); // TODO
        }
    }

    #[tokio::test]
    async fn test_engine_send_onchain_event() {
        let onchain_event = default_onchain_event();
        let (mut engine, _tmpdir) = new_engine();
        let messages_tx = engine.messages_tx();
        messages_tx
            .send(MempoolMessage::ValidatorMessage(
                crate::proto::snapchain::ValidatorMessage {
                    on_chain_event: Some(onchain_event.clone()),
                    fname_transfer: None,
                },
            ))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);
        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(1, state_change.transactions[0].system_messages.len());

        // No hub events are generated
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(0, events.events.len());

        validate_and_commit_state_change(&mut engine, &state_change);

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);

        let stored_onchain_events = engine
            .get_onchain_events(OnChainEventType::EventTypeIdRegister, FID_FOR_TEST)
            .unwrap();
        assert_eq!(stored_onchain_events.len(), 1);

        // Hub events are generated
        let events = HubEvent::get_events(engine.db.clone(), 0, None, None).unwrap();
        assert_eq!(1, events.events.len());
        let generated_event = match events.events[0].clone().body {
            Some(crate::proto::hub_event::hub_event::Body::MergeOnChainEventBody(e)) => e,
            _ => panic!("Unexpected event type"),
        };
        assert_eq!(
            to_hex(&onchain_event.transaction_hash),
            to_hex(&generated_event.on_chain_event.unwrap().transaction_hash)
        );
    }
}
