#[cfg(test)]
mod tests {
    use crate::proto::hub_event::HubEvent;
    use crate::proto::msg::{self as message, ReactionType};
    use crate::proto::onchain_event;
    use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
    use crate::proto::snapchain::{Height, ShardChunk, ShardHeader, Transaction};
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::engine::{MempoolMessage, ShardEngine, ShardStateChange};
    use crate::storage::store::stores::{Limits, StoreLimits};
    use crate::storage::trie::merkle_trie::TrieKey;
    use crate::utils::factory::{self, events_factory, messages_factory};
    use crate::utils::statsd_wrapper::StatsdClientWrapper;
    use ed25519_dalek::{SecretKey, SigningKey};
    use hex::FromHex;
    use prost::Message as _;
    use std::sync::Arc;
    use tempfile;
    use tracing_subscriber::EnvFilter;

    const FID_FOR_TEST: u32 = 1234;
    const FID2_FOR_TEST: u32 = 1235;

    fn new_engine() -> (ShardEngine, tempfile::TempDir) {
        let statsd_client = StatsdClientWrapper::new(
            cadence::StatsdClient::builder("", cadence::NopMetricSink {}).build(),
            true,
        );
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        let test_limits = StoreLimits {
            limits: Limits::for_test(),
            legacy_limits: Limits::zero(),
        };

        (
            ShardEngine::new(Arc::new(db), 1, test_limits, statsd_client),
            dir,
        )
    }

    fn default_storage_event(fid: u32) -> OnChainEvent {
        events_factory::create_rent_event(fid, None, Some(1), false)
    }

    async fn register_user(fid: u32, signer: SigningKey, engine: &mut ShardEngine) {
        commit_event(engine, &default_storage_event(fid)).await;
        let id_register_event = events_factory::create_id_register_event(
            fid,
            onchain_event::IdRegisterEventType::Register,
        );
        commit_event(engine, &id_register_event).await;
        let signer_event =
            events_factory::create_signer_event(fid, signer, onchain_event::SignerEventType::Add);
        commit_event(engine, &signer_event).await;
    }

    #[allow(dead_code)]
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
        messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            text,
            Some(0),
            Some(&default_signer()),
        )
    }

    fn default_onchain_event() -> OnChainEvent {
        events_factory::create_onchain_event(FID_FOR_TEST)
    }

    fn default_signer() -> SigningKey {
        SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        )
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

    fn assert_merge_event(event: &HubEvent, merged_message: &message::Message) {
        let generated_event = match &event.body {
            Some(crate::proto::hub_event::hub_event::Body::MergeMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(
            to_hex(&merged_message.hash),
            to_hex(&generated_event.message.as_ref().unwrap().hash)
        );
    }

    fn assert_prune_event(event: &HubEvent, pruned_message: &message::Message) {
        let generated_event = match &event.body {
            Some(crate::proto::hub_event::hub_event::Body::PruneMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(
            to_hex(&pruned_message.hash),
            to_hex(&generated_event.message.as_ref().unwrap().hash)
        );
    }

    fn assert_revoke_event(event: &HubEvent, revoked_message: &message::Message) {
        let generated_event = match &event.body {
            Some(crate::proto::hub_event::hub_event::Body::RevokeMessageBody(msg)) => msg,
            _ => panic!("Unexpected event type: {:?}", event.body),
        };
        assert_eq!(
            to_hex(&revoked_message.hash),
            to_hex(&generated_event.message.as_ref().unwrap().hash)
        );
    }

    fn assert_onchain_hub_event(event: &HubEvent, onchain_event: &OnChainEvent) {
        let generated_event = match &event.body {
            Some(crate::proto::hub_event::hub_event::Body::MergeOnChainEventBody(onchain)) => {
                onchain
            }
            _ => panic!("Unexpected event type: {:?}", event.body),
        }
        .on_chain_event
        .as_ref()
        .unwrap();
        assert_eq!(
            to_hex(&onchain_event.transaction_hash),
            to_hex(&generated_event.transaction_hash)
        );
        assert_eq!(&onchain_event.r#type, &generated_event.r#type);
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
        assert_eq!(state_change.new_state_root, engine.trie_root_hash());
        chunk
    }

    async fn commit_message(
        engine: &mut ShardEngine,
        msg: &message::Message,
        assert_success: bool,
    ) -> ShardChunk {
        let messages_tx = engine.messages_tx();

        messages_tx
            .send(MempoolMessage::UserMessage(msg.clone()))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);

        if state_change.transactions.is_empty() {
            panic!("Failed to propose message");
        }

        let chunk = validate_and_commit_state_change(engine, &state_change);
        if assert_success {
            assert_eq!(
                state_change.new_state_root,
                chunk.header.as_ref().unwrap().shard_root
            );
            assert!(engine.trie_key_exists(&TrieKey::for_message(msg)));
        }
        chunk
    }

    async fn commit_event(engine: &mut ShardEngine, event: &OnChainEvent) -> ShardChunk {
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

    #[tokio::test]
    async fn test_engine_basic_propose() {
        let (mut engine, _tmpdir) = new_engine();
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

        // Propose a message that doesn't require storage
        messages_tx
            .send(MempoolMessage::ValidatorMessage(
                crate::proto::snapchain::ValidatorMessage {
                    on_chain_event: Some(events_factory::create_onchain_event(FID_FOR_TEST)),
                    fname_transfer: None,
                },
            ))
            .await
            .unwrap();

        let state_change = engine.propose_state_change(1);

        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(40, to_hex(&state_change.new_state_root).len());
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
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;
        let messages_tx = engine.messages_tx();
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        // Registering a user generates events
        let initial_events_count = HubEvent::get_events(engine.db.clone(), 0, None, None)
            .unwrap()
            .events
            .len();
        assert_eq!(3, initial_events_count);

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
        assert_eq!(initial_events_count, events.events.len());

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
        assert_eq!(initial_events_count + 1, events.events.len());
        let generated_event = event_rx.recv().await.unwrap();
        assert_eq!(generated_event, events.events[initial_events_count]);

        assert_merge_event(&generated_event, &msg1);

        // The message exists in the trie
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&msg1)), true);
    }

    #[tokio::test]
    async fn test_engine_commit_delete_message() {
        let timestamp = messages_factory::farcaster_time();
        let cast =
            messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", Some(timestamp), None);
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;

        commit_message(&mut engine, &cast, true).await;

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

        commit_message(&mut engine, &delete_cast, true).await;

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
    async fn test_commit_link_messages() {
        let timestamp = messages_factory::farcaster_time();
        let target_fid = 15;
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;

        let link_add = messages_factory::links::create_link_add(
            FID_FOR_TEST,
            "follow".to_string(),
            target_fid,
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &link_add, true).await;

        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(1, link_result.unwrap().messages_bytes.len());

        let link_remove = messages_factory::links::create_link_remove(
            FID_FOR_TEST,
            "follow".to_string(),
            target_fid,
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &link_remove, true).await;

        let link_result = engine.get_links_by_fid(FID_FOR_TEST);
        assert_eq!(0, link_result.unwrap().messages_bytes.len());

        let link_compact_state = messages_factory::links::create_link_compact_state(
            FID_FOR_TEST,
            "follow".to_string(),
            target_fid,
            Some(timestamp + 1),
            None,
        );

        commit_message(&mut engine, &link_compact_state, true).await;

        let link_result = engine.get_link_compact_state_messages_by_fid(FID_FOR_TEST);
        assert_eq!(1, link_result.unwrap().messages_bytes.len());
    }

    #[tokio::test]
    async fn test_commit_reaction_messages() {
        let timestamp = messages_factory::farcaster_time();
        let target_url = "exampleurl".to_string();
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;

        let reaction_add = messages_factory::reactions::create_reaction_add(
            FID_FOR_TEST,
            ReactionType::Like,
            target_url.clone(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &reaction_add, true).await;

        let reaction_result = engine.get_reactions_by_fid(FID_FOR_TEST);
        assert_eq!(1, reaction_result.unwrap().messages_bytes.len());

        let reaction_remove = messages_factory::reactions::create_reaction_remove(
            FID_FOR_TEST,
            ReactionType::Like,
            target_url.clone(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &reaction_remove, true).await;

        let reaction_result = engine.get_reactions_by_fid(FID_FOR_TEST);
        assert_eq!(0, reaction_result.unwrap().messages_bytes.len());
    }

    #[tokio::test]
    async fn test_commit_user_data_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;

        let user_data_add = messages_factory::user_data::create_user_data_add(
            FID_FOR_TEST,
            message::UserDataType::Bio,
            "Hi it's me".to_string(),
            Some(timestamp),
            Some(&default_signer()),
        );

        commit_message(&mut engine, &user_data_add, true).await;

        let user_data_result = engine.get_user_data_by_fid(FID_FOR_TEST);
        assert_eq!(1, user_data_result.unwrap().messages_bytes.len());
    }

    #[tokio::test]
    async fn test_commit_verification_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;
        let address = "address".to_string();

        let verification_add = messages_factory::verifications::create_verification_add(
            FID_FOR_TEST,
            0,
            address.clone(),
            "signature".to_string(),
            "hash".to_string(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &verification_add, true).await;

        let verification_result = engine.get_verifications_by_fid(FID_FOR_TEST);
        assert_eq!(1, verification_result.unwrap().messages_bytes.len());

        let verification_remove = messages_factory::verifications::create_verification_remove(
            FID_FOR_TEST,
            address.clone(),
            Some(timestamp),
            None,
        );

        commit_message(&mut engine, &verification_remove, true).await;

        let verification_result = engine.get_verifications_by_fid(FID_FOR_TEST);
        assert_eq!(0, verification_result.unwrap().messages_bytes.len());
    }

    #[tokio::test]
    async fn test_commit_username_proof_messages() {
        let timestamp = messages_factory::farcaster_time();
        let (mut engine, _tmpdir) = new_engine();
        let name = "username".to_string();
        let owner = "owner".to_string();
        let signature = "signature".to_string();
        let signer = default_signer();

        register_user(FID_FOR_TEST, signer.clone(), &mut engine).await;

        let username_proof_add = messages_factory::username_proof::create_username_proof(
            FID_FOR_TEST as u64,
            crate::proto::username_proof::UserNameType::UsernameTypeFname,
            name.clone(),
            owner.clone(),
            signature.clone(),
            timestamp as u64,
            Some(&signer),
        );

        commit_message(&mut engine, &username_proof_add, true).await;

        {
            let username_proof_result = engine.get_username_proofs_by_fid(FID2_FOR_TEST);
            assert!(username_proof_result.is_ok());

            let messages_bytes_len = username_proof_result.unwrap().messages_bytes.len();
            assert_eq!(0, messages_bytes_len);
        }
        {
            let username_proof_result = engine.get_username_proofs_by_fid(FID_FOR_TEST);
            assert!(username_proof_result.is_ok());

            let messages_bytes_len = username_proof_result.unwrap().messages_bytes.len();
            assert_eq!(1, messages_bytes_len);
        }

        // TODO: test get_username_proof (by name)
        // TODO: do we need a test for ENS name registration?
    }

    #[tokio::test]
    async fn test_account_roots() {
        let cast = messages_factory::casts::create_cast_add(FID_FOR_TEST, "msg1", None, None);
        let (mut engine, _tmpdir) = new_engine();

        let txn = &mut RocksDbTransactionBatch::new();
        let account_root =
            engine
                .get_stores()
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let shard_root = engine.get_stores().trie.root_hash().unwrap();

        // Account root and shard root is empty initially
        assert_eq!(account_root.len(), 0);
        assert_eq!(shard_root.len(), 0);

        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;
        commit_message(&mut engine, &cast, true).await;

        let updated_account_root =
            engine
                .get_stores()
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let updated_shard_root = engine.get_stores().trie.root_hash().unwrap();
        // Account root is not empty after a message is committed
        assert_eq!(updated_account_root.len() > 0, true);
        assert_ne!(updated_shard_root, shard_root);

        let another_fid_event = events_factory::create_onchain_event(FID_FOR_TEST + 1);
        commit_event(&mut engine, &another_fid_event).await;

        let account_root_another_fid =
            engine
                .get_stores()
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST + 1));
        let account_root_original_fid =
            engine
                .get_stores()
                .trie
                .get_hash(&engine.db, txn, &TrieKey::for_fid(FID_FOR_TEST));
        let latest_shard_root = engine.get_stores().trie.root_hash().unwrap();
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
        let mut previous_root = "".to_string();

        let height = engine.get_confirmed_height();
        assert_eq!(height.shard_index, 1);
        assert_eq!(height.block_number, 0);

        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;

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

            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            validate_and_commit_state_change(&mut engine, &state_change);

            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

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

            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            validate_and_commit_state_change(&mut engine, &state_change);

            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

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
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;
        let messages_tx = engine.messages_tx();
        let mut previous_root = "".to_string();

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

            // State root has changed
            assert_ne!(previous_root, to_hex(&state_change.new_state_root));
            previous_root = to_hex(&state_change.new_state_root);

            validate_and_commit_state_change(&mut engine, &state_change);

            // Committed state root is the same as what was proposed
            assert_eq!(previous_root, to_hex(&engine.trie_root_hash()));

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
        let mut event_rx = engine.get_senders().events_tx.subscribe();
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
        assert!(event_rx.try_recv().is_err());

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
        assert_eq!(event_rx.recv().await.unwrap(), events.events[0]);

        let generated_event = match events.events[0].clone().body {
            Some(crate::proto::hub_event::hub_event::Body::MergeOnChainEventBody(e)) => e,
            _ => panic!("Unexpected event type"),
        };
        assert_eq!(
            to_hex(&onchain_event.transaction_hash),
            to_hex(&generated_event.on_chain_event.unwrap().transaction_hash)
        );
    }

    #[tokio::test]
    async fn test_messages_not_merged_with_no_storage() {
        let (mut engine, _tmpdir) = new_engine();

        let cast_add =
            messages_factory::casts::create_cast_add(FID_FOR_TEST + 1, "no storage", None, None);

        assert_eq!("", to_hex(&engine.trie_root_hash()));
        let messages_tx = engine.messages_tx();

        messages_tx
            .send(MempoolMessage::UserMessage(cast_add.clone()))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);

        assert_eq!(0, state_change.transactions.len());
        assert_eq!("", to_hex(&state_change.new_state_root));
    }

    #[ignore]
    #[tokio::test]
    async fn test_messages_pruned_with_exceeded_storage() {
        let (mut engine, _tmpdir) = new_engine();
        register_user(FID_FOR_TEST, default_signer(), &mut engine).await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let current_time = factory::time::farcaster_time();
        let cast1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(current_time),
            None,
        );
        let cast2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(current_time + 1),
            None,
        );
        let cast3 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            Some(current_time + 2),
            None,
        );
        let cast4 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg4",
            Some(current_time + 3),
            None,
        );
        let cast5 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg5",
            Some(current_time + 4),
            None,
        );

        // Default size in tests is 4 casts, so first four messages should merge without issues
        commit_message(&mut engine, &cast1, true).await;
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast1);
        commit_message(&mut engine, &cast2, true).await;
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast2);
        commit_message(&mut engine, &cast3, true).await;
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast3);
        commit_message(&mut engine, &cast4, true).await;
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast4);

        // Fifth message should be merged, but should cause cast1 to be pruned
        commit_message(&mut engine, &cast5, true).await;
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast5);
        assert_prune_event(&event_rx.try_recv().unwrap(), &cast1);

        // Prunes are reflected in the trie
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast1)), false);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast2)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast3)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast4)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast5)), true);
    }

    #[ignore]
    #[tokio::test]
    async fn test_messages_partially_merged_with_insufficient_storage() {
        let (mut engine, _tmpdir) = new_engine();
        let signer = default_signer();
        register_user(FID_FOR_TEST, signer.clone(), &mut engine).await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();
        let current_time = factory::time::farcaster_time();
        let cast1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(current_time),
            None,
        );
        let cast2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(current_time + 1),
            Some(&signer),
        );
        let cast3 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            Some(current_time + 2),
            Some(&signer),
        );
        let cast4 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg4",
            Some(current_time + 3),
            Some(&signer),
        );
        let cast5 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg5",
            Some(current_time + 4),
            Some(&signer),
        );
        let cast6 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg6",
            Some(current_time + 5),
            Some(&signer),
        );

        let messages_tx = engine.messages_tx();

        // Send first three messages in one block, which should mean there is 1 message left in storage
        messages_tx
            .send(MempoolMessage::UserMessage(cast1.clone()))
            .await
            .unwrap();
        messages_tx
            .send(MempoolMessage::UserMessage(cast2.clone()))
            .await
            .unwrap();
        messages_tx
            .send(MempoolMessage::UserMessage(cast3.clone()))
            .await
            .unwrap();
        let state_change = engine.propose_state_change(1);
        validate_and_commit_state_change(&mut engine, &state_change);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast1);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast2);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast3);

        // Now send the last three messages, all of them should be merged, and the first two should be pruned
        messages_tx
            .send(MempoolMessage::UserMessage(cast4.clone()))
            .await
            .unwrap();
        messages_tx
            .send(MempoolMessage::UserMessage(cast5.clone()))
            .await
            .unwrap();
        messages_tx
            .send(MempoolMessage::UserMessage(cast6.clone()))
            .await
            .unwrap();

        let state_change = engine.propose_state_change(1);
        let chunk = validate_and_commit_state_change(&mut engine, &state_change);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast4);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast5);
        assert_merge_event(&event_rx.try_recv().unwrap(), &cast6);

        assert_prune_event(&event_rx.try_recv().unwrap(), &cast1);
        assert_prune_event(&event_rx.try_recv().unwrap(), &cast2);

        let user_messages = chunk.transactions[0]
            .user_messages
            .iter()
            .map(|m| to_hex(&m.hash))
            .collect::<Vec<String>>();
        assert_eq!(
            user_messages,
            vec![
                to_hex(&cast4.hash),
                to_hex(&cast5.hash),
                to_hex(&cast6.hash)
            ]
        );

        // Prunes are reflected in the trie
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast1)), false);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast2)), false);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast3)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast4)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast5)), true);
        assert_eq!(engine.trie_key_exists(&TrieKey::for_message(&cast6)), true);
    }

    #[ignore]
    #[tokio::test]
    async fn test_revoking_a_signer_deletes_all_messages_from_that_signer() {
        let (mut engine, _tmpdir) = new_engine();
        let signer = SigningKey::generate(&mut rand::rngs::OsRng);
        let another_signer = &SigningKey::generate(&mut rand::rngs::OsRng);
        let timestamp = factory::time::farcaster_time();
        let msg1 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg1",
            Some(timestamp),
            Some(&signer),
        );
        let msg2 = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg2",
            Some(timestamp + 1),
            Some(&signer),
        );
        let same_fid_different_signer = messages_factory::casts::create_cast_add(
            FID_FOR_TEST,
            "msg3",
            None,
            Some(another_signer),
        );
        let different_fid_same_signer =
            messages_factory::casts::create_cast_add(FID_FOR_TEST + 1, "msg4", None, Some(&signer));
        register_user(FID_FOR_TEST, signer.clone(), &mut engine).await;
        let another_signer_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            another_signer.clone(),
            onchain_event::SignerEventType::Add,
        );
        commit_event(&mut engine, &another_signer_event).await;
        register_user(FID_FOR_TEST + 1, signer.clone(), &mut engine).await;
        let mut event_rx = engine.get_senders().events_tx.subscribe();

        commit_message(&mut engine, &msg1, true).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &msg2, true).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &same_fid_different_signer, true).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event
        commit_message(&mut engine, &different_fid_same_signer, true).await;
        let _ = &event_rx.try_recv().unwrap(); // Ignore merge event

        // All 4 messages exist
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(3, messages.messages_bytes.len());
        let messages = engine.get_casts_by_fid(FID_FOR_TEST + 1).unwrap();
        assert_eq!(1, messages.messages_bytes.len());

        // Revoke a single signer
        let revoke_event = events_factory::create_signer_event(
            FID_FOR_TEST,
            signer.clone(),
            onchain_event::SignerEventType::Remove,
        );
        commit_event(&mut engine, &revoke_event).await;
        assert_onchain_hub_event(&event_rx.try_recv().unwrap(), &revoke_event);
        assert_revoke_event(&event_rx.try_recv().unwrap(), &msg1);
        assert_revoke_event(&event_rx.try_recv().unwrap(), &msg2);

        assert_eq!(event_rx.try_recv().is_err(), true); // No more events

        // Only the messages from the revoked signer are deleted
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(1, messages.messages_bytes.len());

        // Different Fid with the same signer is unaffected
        let messages = engine.get_casts_by_fid(FID_FOR_TEST + 1).unwrap();
        assert_eq!(1, messages.messages_bytes.len());
    }

    #[tokio::test]
    async fn test_missing_id_registration() {
        let (mut engine, _tmpdir) = new_engine();
        commit_event(&mut engine, &default_storage_event(FID_FOR_TEST)).await;
        commit_event(
            &mut engine,
            &factory::events_factory::create_signer_event(
                FID_FOR_TEST,
                default_signer(),
                onchain_event::SignerEventType::Add,
            ),
        )
        .await;
        commit_message(&mut engine, &default_message("msg1"), false).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(0, messages.messages_bytes.len());
        let id_register = events_factory::create_id_register_event(
            FID_FOR_TEST,
            onchain_event::IdRegisterEventType::Register,
        );
        commit_event(&mut engine, &id_register).await;
        commit_message(&mut engine, &default_message("msg1"), true).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(1, messages.messages_bytes.len());
    }

    #[tokio::test]
    async fn test_missing_signer() {
        let (mut engine, _tmpdir) = new_engine();
        commit_event(&mut engine, &default_storage_event(FID_FOR_TEST)).await;
        commit_event(
            &mut engine,
            &factory::events_factory::create_id_register_event(
                FID_FOR_TEST,
                onchain_event::IdRegisterEventType::Register,
            ),
        )
        .await;
        commit_message(&mut engine, &default_message("msg1"), false).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(0, messages.messages_bytes.len());
        commit_event(
            &mut engine,
            &factory::events_factory::create_signer_event(
                FID_FOR_TEST,
                default_signer(),
                onchain_event::SignerEventType::Add,
            ),
        )
        .await;
        commit_message(&mut engine, &default_message("msg1"), true).await;
        let messages = engine.get_casts_by_fid(FID_FOR_TEST).unwrap();
        assert_eq!(1, messages.messages_bytes.len());
    }
}
