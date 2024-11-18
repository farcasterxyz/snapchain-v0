#[cfg(test)]
mod tests {
    use crate::proto::message::Message;
    use crate::proto::snapchain::{ShardChunk, ShardHeader, Transaction};
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

    fn state_change_to_shard_chunk(change: ShardStateChange) -> ShardChunk {
        let mut chunk = default_shard_chunk();

        chunk.header.as_mut().unwrap().shard_root = change.new_state_root;

        //TODO: all messages
        chunk.transactions[0]
            .user_messages
            .push(change.transactions[0].user_messages[0].clone());

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

    fn default_message() -> Message {
        let private_key = SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        );

        cli::compose_message(private_key, 1234, "this is the note", Some(0))
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

        let mut chunk = default_shard_chunk();
        chunk.header.as_mut().unwrap().shard_root =
            from_hex("237b11d0dd9e78994ef2f141c7f170d48bb51d34");

        engine.commit_shard_chunk(chunk);

        assert_eq!(
            "237b11d0dd9e78994ef2f141c7f170d48bb51d34",
            to_hex(&engine.trie_root_hash())
        );
    }

    #[tokio::test]
    async fn test_engine_send_messages() {
        enable_logging();
        let mut engine = new_engine();
        let messages_tx = engine.messages_tx();
        let msg = default_message();

        assert_eq!(
            "dcf21a11dac4c3e7944f0d5254a2ebbf23c964df",
            to_hex(&msg.hash),
        );

        messages_tx.send(msg.clone()).await.unwrap();

        let state_change = engine.propose_state_change(1);

        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1);
        assert_eq!(1, state_change.transactions[0].user_messages.len());

        let msg0 = &state_change.transactions[0].user_messages[0];

        assert_eq!(
            "dcf21a11dac4c3e7944f0d5254a2ebbf23c964df",
            to_hex(&msg0.hash)
        );

        assert_eq!(
            "4ecbed7e119cf4999271780abb881dfaa579d85e",
            to_hex(&state_change.new_state_root)
        );

        let chunk = state_change_to_shard_chunk(state_change.clone());
        engine.commit_shard_chunk(chunk);

        assert_eq!(
            "4ecbed7e119cf4999271780abb881dfaa579d85e",
            to_hex(&engine.trie_root_hash())
        );
    }
}
