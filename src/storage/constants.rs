pub const PAGE_SIZE_MAX: usize = 1_000;

#[allow(dead_code)]
pub enum RootPrefix {
    Block = 1,
    Shard = 2,
    /* Used for multiple purposes, starts with a 4-byte fid */
    User = 3,
    /* Used to index casts by parent */
    CastsByParent = 4,
    /* Used to index casts by mention */
    CastsByMention = 5,
    /* Used to index links by target */
    LinksByTarget = 6,
    /* Used to index reactions by target  */
    ReactionsByTarget = 7,

    /* Sync Merkle Trie Node */
    SyncMerkleTrieNode = 8,

    /* Event log */
    HubEvents = 9,
    // /* The network ID that the rocksDB was created with */
    // Network = 10,

    // /* Used to store fname server name proofs */
    // FNameUserNameProof = 11,

    // /* Used to store on chain events */
    OnChainEvent = 12,
    // /** DB Schema version */
    // DBSchemaVersion = 13,

    // /* Used to index verifications by address */
    // VerificationByAddress = 14,
}

/** Copied from the JS code */
#[repr(u8)]
pub enum UserPostfix {
    /* Message records (1-85) */
    CastMessage = 1,
    LinkMessage = 2,
    ReactionMessage = 3,
    VerificationMessage = 4,
    // Deprecated
    // SignerMessage = 5,
    UserDataMessage = 6,
    UsernameProofMessage = 7,

    // Add new message types here
    // NOTE: If you add a new message type, make sure that it is only used to store Message protobufs.
    // If you need to store an index, use one of the UserPostfix values below (>86).
    /** Index records (must be 86-255) */
    // Deprecated
    // BySigner = 86, // Index message by its signer

    /** CastStore add and remove sets */
    CastAdds = 87,
    CastRemoves = 88,

    /* LinkStore add and remove sets */
    LinkAdds = 89,
    LinkRemoves = 90,

    /** ReactionStore add and remove sets */
    ReactionAdds = 91,
    ReactionRemoves = 92,

    /** Verification add and remove sets */
    VerificationAdds = 93,
    VerificationRemoves = 94,

    /* Deprecated */
    // SignerAdds = 95,
    // SignerRemoves = 96,

    /* UserDataStore add set */
    UserDataAdds = 97,

    /* UserNameProof add set */
    UserNameProofAdds = 99,

    /* Link Compact State set */
    LinkCompactStateMessage = 100,
}

impl UserPostfix {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
pub enum OnChainEventPostfix {
    OnChainEvents = 1,

    // Secondary indexes
    SignerByFid = 51,
    IdRegisterByFid = 52,
    IdRegisterByCustodyAddress = 53,
}
