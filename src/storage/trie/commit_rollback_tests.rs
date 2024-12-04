#[cfg(test)]
mod tests {
    use crate::storage::db::{RocksDB, RocksDbTransactionBatch};
    use crate::storage::trie::errors::TrieError;
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie};
    use hex;
    use tempfile::TempDir;

    fn generate_hashes(seed: Vec<u8>, chain_size: usize) -> Vec<Vec<u8>> {
        let mut hash_chain = Vec::new();
        let mut current_hash = seed;

        for _ in 0..chain_size {
            let hash = blake3::hash(&current_hash);
            hash_chain.push(hash.as_bytes()[..20].to_vec());
            current_hash = hash.as_bytes()[..20].to_vec();
        }

        hash_chain
    }

    #[test]
    fn test_merkle_trie_basic_operations() -> Result<(), TrieError> {
        let ctx = &Context::new();

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");
        let mut t = MerkleTrie::new(16).unwrap();
        let db = &RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        t.initialize(db)?;

        let mut txn_batch = RocksDbTransactionBatch::new();
        t.insert(
            ctx,
            db,
            &mut txn_batch,
            vec![
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ],
        )?;

        db.commit(txn_batch).unwrap();
        t.reload(db).unwrap();

        let mut txn_batch = RocksDbTransactionBatch::new();

        println!(
            "After commit: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(ctx, db, &[])?
        );

        t.insert(
            ctx,
            db,
            &mut txn_batch,
            vec![vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
        )?;
        println!(
            "After insert: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(ctx, db, &[])?
        );

        t.reload(db)?;
        println!(
            "After reload: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(ctx, db, &[])?
        );

        Ok(())
    }

    #[test]
    fn test_merkle_trie_with_large_data() -> Result<(), TrieError> {
        let ctx = &Context::new();

        let dir = TempDir::new().unwrap();
        let hashes1 = generate_hashes(vec![1], 10_000);

        {
            let db_path = dir.path().join("t1.db");
            let db = &RocksDB::new(db_path.to_str().unwrap());
            db.open().unwrap();

            let mut t1 = MerkleTrie::new(16).unwrap();
            t1.initialize(db)?;

            let mut txn_batch = RocksDbTransactionBatch::new();
            t1.insert(ctx, db, &mut txn_batch, hashes1.clone())?;
            let items = t1.items()?;
            println!(
                "t1: items = {:?}, root_hash = {}",
                items,
                hex::encode(t1.root_hash()?)
            );
        }

        {
            let db_path = dir.path().join("t2.db");
            let db = &RocksDB::new(db_path.to_str().unwrap());
            db.open().unwrap();

            let mut t2 = MerkleTrie::new(16).unwrap();
            t2.initialize(db)?;

            let mut txn_batch = RocksDbTransactionBatch::new();
            t2.insert(ctx, db, &mut txn_batch, hashes1)?;
            let items = t2.items()?;
            println!(
                "t2: items = {:?}, root_hash = {}",
                items,
                hex::encode(t2.root_hash()?)
            );
        }

        Ok(())
    }
}
