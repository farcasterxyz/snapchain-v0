/**
 * The hashes in the sync trie are 20 bytes (160 bits) long, so we use the first 20 bytes of the blake3 hash
 */
const BLAKE3_HASH_LEN: usize = 20;
pub fn blake3_20(input: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(input);
    hasher.finalize().as_bytes()[..BLAKE3_HASH_LEN].to_vec()
}

pub fn bytes_compare(a: &[u8], b: &[u8]) -> i8 {
    let len = a.len().min(b.len());
    for i in 0..len {
        if a[i] < b[i] {
            return -1;
        }
        if a[i] > b[i] {
            return 1;
        }
    }
    if a.len() < b.len() {
        -1
    } else if a.len() > b.len() {
        1
    } else {
        0
    }
}

// TODO: find a better place for below
/** Copied from the JS code */
#[allow(dead_code)]
pub enum RootPrefix {
    /* Used for multiple purposes, starts with a 4-byte fid */
    User = 1,
    /* Used to index casts by parent */
    CastsByParent = 2,
    /* Used to index casts by mention */
    CastsByMention = 3,
    /* Used to index links by target */
    LinksByTarget = 4,
    /* Used to index reactions by target  */
    ReactionsByTarget = 5,
    /* Deprecated */
    // IdRegistryEvent = 6,
    // NameRegistryEvent = 7,
    // IdRegistryEventByCustodyAddress = 8,
    /* Used to store the state of the hub */
    HubState = 9,
    /* Revoke signer jobs */
    JobRevokeMessageBySigner = 10,
    /* Sync Merkle Trie Node */
    SyncMerkleTrieNode = 11,
    /* Deprecated */
    // JobUpdateNameExpiry = 12,
    // NameRegistryEventsByExpiry = 13,
    /* To check if the Hub was cleanly shutdown */
    HubCleanShutdown = 14,
    /* Event log */
    HubEvents = 15,
    /* The network ID that the rocksDB was created with */
    Network = 16,
    /* Used to store fname server name proofs */
    FNameUserNameProof = 17,
    /* Used to store gossip network metrics */
    // Deprecated, DO NOT USE
    // GossipMetrics = 18,

    /* Used to index user submitted username proofs */
    UserNameProofByName = 19,

    // Deprecated
    // RentRegistryEvent = 20,
    // RentRegistryEventsByExpiry = 21,
    // StorageAdminRegistryEvent = 22,

    /* Used to store on chain events */
    OnChainEvent = 23,

    /** DB Schema version */
    DBSchemaVersion = 24,

    /* Used to index verifications by address */
    VerificationByAddress = 25,

    /* Store the connected peers */
    ConnectedPeers = 26,

    /* Used to index fname username proofs by fid */
    FNameUserNameProofByFid = 27,
}
