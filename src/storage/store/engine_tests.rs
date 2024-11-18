#[cfg(test)]
mod tests {
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

    #[test]
    fn test_engine_one() {
        let mut engine = new_engine();
        let state_change = engine.propose_state_change(1);
        assert_eq!(1, state_change.shard_id);
        assert_eq!(state_change.transactions.len(), 1); // TODO: will change
        state_change.transactions[]
    }
}
