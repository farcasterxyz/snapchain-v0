#[cfg(test)]
mod tests {
    use crate::core::types::Height;
    use crate::proto::snapchain::{ShardChunk, ShardHeader, Transaction};
    use crate::storage::db;
    use crate::storage::store::engine::ShardEngine;
    use crate::storage::store::shard::ShardStore;

    fn new_engine() -> ShardEngine {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        let shard_store = ShardStore::new(db);
        ShardEngine::new(1, shard_store)
    }

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    fn to_hex(b: &[u8]) -> String {
        hex::encode(b)
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
}
