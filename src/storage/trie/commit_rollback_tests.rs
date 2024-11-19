#[cfg(test)]
mod tests {
    use crate::core::error::HubError;
    use crate::storage::trie::merkle_trie::MerkleTrie;
    use hex;
    use rand::{seq::SliceRandom, thread_rng};
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
    fn test_merkle_trie_basic_operations() -> Result<(), HubError> {
        let dir = TempDir::new()?;
        let db_path = dir.path().join("a.db");
        let mut t = MerkleTrie::new(db_path.to_str().unwrap())?;
        t.initialize()?;

        t.insert(vec![
            vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ])?;

        t.commit();

        println!(
            "After commit: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(&[])?
        );

        t.insert(vec![vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0]])?;
        println!(
            "After insert: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(&[])?
        );

        t.reload()?;
        println!(
            "After reload: root_hash = {}, values = {:?}",
            hex::encode(t.root_hash()?),
            t.get_all_values(&[])?
        );

        Ok(())
    }

    #[test]
    fn test_merkle_trie_with_large_data() -> Result<(), HubError> {
        let dir = TempDir::new()?;
        let hashes1 = generate_hashes(vec![1], 10_000);

        let hashes2 = {
            let mut rng = thread_rng();
            let mut hashes = hashes1.clone();
            hashes.shuffle(&mut rng);
            hashes
        };

        {
            let db_path = dir.path().join("t1.db");
            let t1 = MerkleTrie::new(db_path.to_str().unwrap())?;
            t1.initialize()?;
            t1.insert(hashes1.clone())?;
            let items = t1.items()?;
            println!(
                "t1: items = {:?}, root_hash = {}",
                items,
                hex::encode(t1.root_hash()?)
            );
        }

        {
            let db_path = dir.path().join("t2.db");
            let t2 = MerkleTrie::new(db_path.to_str().unwrap())?;
            t2.initialize()?;
            t2.insert(hashes1)?;
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
