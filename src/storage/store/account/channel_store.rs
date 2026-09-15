use super::{
    get_from_db_or_txn, get_message, make_ts_hash, make_user_key, read_fid_key,
    require_page_token_in_prefix, FollowIndexGate, Store, StoreDef, StoreEventHandler,
    StoreOptions, TS_HASH_LENGTH,
};
use crate::core::error::HubError;
use crate::proto::{
    message_data::Body, CastingMode, ChannelMemberAction, ChannelModerateAction, ChannelPinBody,
    ChannelUpdateBody, HubEvent, MembershipMode, Message, MessageType, SignatureScheme,
};
use crate::storage::constants::{RootPrefix, UserPostfix, PAGE_SIZE_MAX};
use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
use crate::storage::util::increment_vec_u8;
use std::sync::Arc;
use tracing::warn;

// Proposed protocol values; these await protocol confirmation before activation. These bound
// TOTAL permanent slots per channel, not live states. In particular, OPEN self-joins spend the
// channel's member-slot budget: a bot swarm can exhaust it and brick future joins until a gated
// cap raise. Role flips, removals, bans, and re-adds remain row-neutral and cannot reclaim slots.
pub const CHANNEL_MEMBER_SLOT_CAP: u32 = 8_192;
pub const CHANNEL_MODERATE_SLOT_CAP: u32 = 16_384;

// CONSENSUS-CRITICAL: slot keys concatenate `channel_id` with a per-type suffix, so the
// keyspace is prefix-free ONLY while `channel_id` is fixed-width. With a variable-length
// channel_id, `(channel_id = C ++ x, suffix = s)` and `(channel_id = C, suffix = x ++ s)`
// build the identical slot key while charging DIFFERENT cap counters (the counter key has no
// suffix, so it stays injective). That splits the slot keyspace from the quantity meant to
// bound it: one channel could supersede another channel's slots, and rows could be minted
// into C's keyspace without ever charging C's cap. Both lengths are therefore enforced in the
// slot path itself rather than deferred to the validation increment. `TrieKey::for_fname`
// pads names for this same reason.
pub const CHANNEL_ID_LENGTH: usize = 32;
pub const CHANNEL_MODERATE_CAST_HASH_LENGTH: usize = 20;

#[repr(u8)]
#[derive(Clone, Copy)]
enum ChannelIndex {
    UpdateSlot = 1,
    MemberSlot = 2,
    PinSlot = 3,
    ModerateSlot = 4,
    MemberSlotCount = 5,
    ModerateSlotCount = 6,
    LiveModeratorCount = 7,
    MemberByFid = 8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelUpdateState {
    pub body: ChannelUpdateBody,
    pub casting_mode: CastingMode,
    pub membership_mode: MembershipMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelMemberState {
    Member,
    Moderator,
    Removed,
    Banned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelModerationState {
    Hidden,
    Visible,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelMemberEntry {
    pub fid: u64,
    pub state: ChannelMemberState,
    pub last_action_ts: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelMembershipEntry {
    pub channel_id: Vec<u8>,
    pub state: ChannelMemberState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelModerationEntry {
    pub cast_hash: Vec<u8>,
    pub action: ChannelModerateAction,
    pub author_fid: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelPinState {
    pub body: ChannelPinBody,
    pub author_fid: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelPage<T> {
    pub entries: Vec<T>,
    pub next_page_token: Option<Vec<u8>>,
}

/// Whether a merge may write the `ChannelMessages`-gated derived indices — today
/// only the member by-fid index, which lives outside the trie.
///
/// This is a required argument on every channel merge rather than a default,
/// because getting it wrong is invisible: a `Skip`ped merge still writes the slot,
/// updates the trie, and produces a matching state root, but the row never appears
/// in `memberships_by_fid`, so `GetChannelMembershipsByFid` reports "no memberships"
/// with no error anywhere. The stores whose defs have no gated index today still
/// take it, so adding one to them later is a compile error at each call site rather
/// than a silent skip on the replay path (see the topology note in version.rs —
/// catch-all match arms provide no compiler-enforced reminder).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedIndexGate {
    /// `ChannelMessages` is active for this merge's clock; write derived indices.
    Write,
    /// Feature inactive for this merge's clock; primary slot state only.
    Skip,
}

impl DerivedIndexGate {
    /// Lifts a caller's already-resolved `ChannelMessages` gate. Callers that hold
    /// the boolean should use this rather than picking a variant by hand, so the
    /// merge cannot disagree with the gate its own dispatch arm was chosen under.
    pub fn when_channel_messages_enabled(channel_messages_enabled: bool) -> Self {
        if channel_messages_enabled {
            Self::Write
        } else {
            Self::Skip
        }
    }

    fn writes_derived_indices(self) -> bool {
        self == Self::Write
    }
}

#[derive(Clone)]
pub struct ChannelUpdateStoreDef {
    prune_size_limit: u32,
    // Resolved slot cap; `None` for uncapped stores. See `define_channel_store!`.
    slot_cap: Option<u32>,
}

#[derive(Clone)]
pub struct ChannelMemberStoreDef {
    prune_size_limit: u32,
    slot_cap: Option<u32>,
}

#[derive(Clone)]
pub struct ChannelPinStoreDef {
    prune_size_limit: u32,
    slot_cap: Option<u32>,
}

#[derive(Clone)]
pub struct ChannelModerateStoreDef {
    prune_size_limit: u32,
    slot_cap: Option<u32>,
}

fn invalid_body(store_name: &str) -> HubError {
    HubError::validation_failure(&format!("invalid {store_name} body"))
}

fn unsupported(store_name: &str) -> HubError {
    HubError::invalid_parameter(&format!("{store_name} does not support this operation"))
}

fn is_channel_message(message: &Message, expected_type: MessageType) -> bool {
    let Some(data) = message.data.as_ref() else {
        return false;
    };
    if message.signature_scheme != SignatureScheme::Ed25519 as i32
        || data.r#type != expected_type as i32
    {
        return false;
    }

    matches!(
        (expected_type, data.body.as_ref()),
        (MessageType::ChannelUpdate, Some(Body::ChannelUpdateBody(_)))
            | (MessageType::ChannelMember, Some(Body::ChannelMemberBody(_)))
            | (MessageType::ChannelPin, Some(Body::ChannelPinBody(_)))
            | (
                MessageType::ChannelModerate,
                Some(Body::ChannelModerateBody(_))
            )
    )
}

fn author_slot_key(
    message: &Message,
    index_postfix: UserPostfix,
    slot_suffix: &[u8],
) -> Result<Vec<u8>, HubError> {
    let data = message
        .data
        .as_ref()
        .ok_or_else(|| HubError::validation_failure("message data is missing"))?;
    let mut key = make_user_key(data.fid);
    key.push(index_postfix.as_u8());
    key.extend_from_slice(slot_suffix);
    Ok(key)
}

fn update_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelUpdateBody(body)) => Ok(body.channel_id.clone()),
        _ => Err(invalid_body("ChannelUpdate")),
    }
}

fn member_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelMemberBody(body)) => {
            let mut suffix = body.channel_id.clone();
            let target_fid = u32::try_from(body.fid)
                .map_err(|_| HubError::invalid_parameter("channel member fid exceeds u32"))?;
            suffix.extend_from_slice(&target_fid.to_be_bytes());
            Ok(suffix)
        }
        _ => Err(invalid_body("ChannelMember")),
    }
}

fn pin_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelPinBody(body)) => Ok(body.channel_id.clone()),
        _ => Err(invalid_body("ChannelPin")),
    }
}

fn moderate_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelModerateBody(body)) => {
            let mut suffix = body.channel_id.clone();
            suffix.extend_from_slice(&body.cast_hash);
            Ok(suffix)
        }
        _ => Err(invalid_body("ChannelModerate")),
    }
}

fn channel_index_key(index: ChannelIndex, channel_id: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + channel_id.len() + suffix.len());
    key.push(RootPrefix::Channel as u8);
    key.push(index as u8);
    key.extend_from_slice(channel_id);
    key.extend_from_slice(suffix);
    key
}

/// Reads a big-endian `u32` counter, treating an absent key as zero.
///
/// A stored value of any other width is corruption in this node's own derived
/// state, not caller input, so it fails loudly rather than defaulting.
fn read_counter(db: &RocksDB, txn: &RocksDbTransactionBatch, key: &[u8]) -> Result<u32, HubError> {
    match get_from_db_or_txn(db, txn, key)? {
        None => Ok(0),
        Some(value) if value.len() == 4 => Ok(u32::from_be_bytes(value.try_into().unwrap())),
        Some(value) => {
            warn!(
                actual_length = value.len(),
                "Channel counter has invalid length"
            );
            Err(HubError::invalid_internal_state(
                "channel counter has invalid length",
            ))
        }
    }
}

fn encode_slot_pointer(
    message: &Message,
    ts_hash: &[u8; TS_HASH_LENGTH],
) -> Result<Vec<u8>, HubError> {
    let fid = u32::try_from(message.fid())
        .map_err(|_| HubError::invalid_parameter("channel message fid exceeds u32"))?;
    let mut pointer = Vec::with_capacity(4 + TS_HASH_LENGTH);
    pointer.extend_from_slice(&fid.to_be_bytes());
    pointer.extend_from_slice(ts_hash);
    Ok(pointer)
}

fn load_slot_message<T: ChannelSlotStoreDef + Clone>(
    store: &Store<T>,
    txn: &RocksDbTransactionBatch,
    slot_key: &[u8],
) -> Result<Option<Message>, HubError> {
    let Some(pointer) = get_from_db_or_txn(&store.db(), txn, slot_key)? else {
        return Ok(None);
    };
    message_for_slot_pointer(store, txn, &pointer).map(Some)
}

/// Resolves an already-read slot pointer to its primary message.
///
/// Split out from `load_slot_message` for the by-channel enumerators: their
/// iterator callback is handed the pointer as the row's value, so going back
/// through `load_slot_message` would re-fetch a key RocksDB just gave them —
/// one avoidable point-get per row, on reads that scan up to a channel's whole
/// slot cap when a state filter matches nothing.
fn message_for_slot_pointer<T: ChannelSlotStoreDef + Clone>(
    store: &Store<T>,
    txn: &RocksDbTransactionBatch,
    pointer: &[u8],
) -> Result<Message, HubError> {
    if pointer.len() != 4 + TS_HASH_LENGTH {
        warn!(
            actual_length = pointer.len(),
            expected_length = 4 + TS_HASH_LENGTH,
            "Channel slot pointer has invalid length"
        );
        return Err(HubError::invalid_internal_state(
            "channel slot pointer has invalid length",
        ));
    }
    let fid = read_fid_key(pointer, 0);
    let ts_hash: [u8; TS_HASH_LENGTH] = pointer[4..].try_into().unwrap();
    match get_message(&store.db(), txn, fid, store.store_def().postfix(), &ts_hash)? {
        Some(message) => Ok(message),
        None => {
            warn!(fid, "Channel slot points to a missing message");
            Err(HubError::invalid_internal_state(
                "channel slot points to a missing message",
            ))
        }
    }
}

trait ChannelSlotStoreDef: StoreDef {
    fn channel_id(&self, message: &Message) -> Result<Vec<u8>, HubError>;
    fn slot_key(&self, message: &Message) -> Result<Vec<u8>, HubError>;

    fn validate_slot_message(&self, _message: &Message) -> Result<(), HubError> {
        Ok(())
    }

    fn slot_cap(&self) -> Option<u32> {
        None
    }

    fn slot_count_key(&self, _channel_id: &[u8]) -> Option<Vec<u8>> {
        None
    }

    fn is_live_moderator(&self, _message: &Message) -> Result<Option<bool>, HubError> {
        Ok(None)
    }

    fn build_gated_secondary_indices(
        &self,
        _txn: &mut RocksDbTransactionBatch,
        _message: &Message,
        _gate: DerivedIndexGate,
    ) -> Result<(), HubError> {
        Ok(())
    }
}

/// The D3 fold for the two policy modes, shared by merge-time validation and the read fold so
/// they cannot drift. An unparseable mode is rejected at merge (the slot must never hold state
/// the fold cannot read). Both "absent" and the explicit zero variant mean UNSPECIFIED and fold
/// to the most restrictive value, so a cosmetic-only update closes permissions rather than
/// accidentally opening them — an explicit `Some(0)` must not be a way around that default.
fn fold_channel_modes(body: &ChannelUpdateBody) -> Result<(CastingMode, MembershipMode), HubError> {
    let casting_mode = body
        .casting_mode
        .map(CastingMode::try_from)
        .transpose()
        .map_err(|_| HubError::validation_failure("invalid channel casting mode"))?
        .filter(|mode| *mode != CastingMode::None)
        .unwrap_or(CastingMode::MembersOnly);
    let membership_mode = body
        .membership_mode
        .map(MembershipMode::try_from)
        .transpose()
        .map_err(|_| HubError::validation_failure("invalid channel membership mode"))?
        .filter(|mode| *mode != MembershipMode::None)
        .unwrap_or(MembershipMode::Approval);
    Ok((casting_mode, membership_mode))
}

fn member_state_for_message(message: &Message) -> Result<ChannelMemberState, HubError> {
    let action = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelMemberBody(body)) => ChannelMemberAction::try_from(body.action)
            .map_err(|_| HubError::validation_failure("invalid channel member action"))?,
        _ => return Err(invalid_body("ChannelMember")),
    };
    match action {
        ChannelMemberAction::AddMember | ChannelMemberAction::RemoveModerator => {
            Ok(ChannelMemberState::Member)
        }
        ChannelMemberAction::AddModerator => Ok(ChannelMemberState::Moderator),
        ChannelMemberAction::RemoveMember | ChannelMemberAction::Unban => {
            Ok(ChannelMemberState::Removed)
        }
        ChannelMemberAction::Ban => Ok(ChannelMemberState::Banned),
        ChannelMemberAction::None => Err(HubError::validation_failure(
            "invalid channel member action",
        )),
    }
}

fn stored_member_state_for_message(message: &Message) -> Result<ChannelMemberState, HubError> {
    member_state_for_message(message).map_err(|err| HubError::invalid_internal_state(&err.message))
}

fn moderation_state_for_message(message: &Message) -> Result<ChannelModerationState, HubError> {
    let action = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelModerateBody(body)) => ChannelModerateAction::try_from(body.action)
            .map_err(|_| HubError::validation_failure("invalid channel moderate action"))?,
        _ => return Err(invalid_body("ChannelModerate")),
    };
    match action {
        ChannelModerateAction::Hide => Ok(ChannelModerationState::Hidden),
        ChannelModerateAction::Unhide => Ok(ChannelModerationState::Visible),
        ChannelModerateAction::None => Err(HubError::validation_failure(
            "invalid channel moderate action",
        )),
    }
}

fn stored_moderation_state_for_message(
    message: &Message,
) -> Result<ChannelModerationState, HubError> {
    moderation_state_for_message(message)
        .map_err(|err| HubError::invalid_internal_state(&err.message))
}

fn merge_slot<T: ChannelSlotStoreDef + Clone>(
    store: &Store<T>,
    message: &Message,
    txn: &mut RocksDbTransactionBatch,
    gate: DerivedIndexGate,
) -> Result<HubEvent, HubError> {
    // DATA-SHARD ADMISSION PROVENANCE: a channel message reaches this shared slot merge on a data
    // shard only as (1) a ChannelMessages-gated BlockEvent that shard 0 minted after its authority
    // and policy checks succeeded, or (2) a replication row whose trie key is verified against a
    // decided state root. Direct ShardEngine admission rejects all four channel types. Therefore
    // authority and admission-only policy caps (notably the live-moderator cap) must not be
    // re-evaluated here: doing so would turn evaluator drift into a silently missing replica row.
    //
    // The slot-count checks below ARE the same caps shard 0 already applied, re-evaluated against
    // this store's own counter — so they are subject to exactly that failure mode, and are kept
    // only because the counter is a deterministic function of the identical ordered merge stream.
    // If it ever drifts, `merge_slot` fails, `handle_block_event` propagates, and the caller in
    // engine.rs logs "Error merging block event" and continues: the row is absent from this
    // replica for good, with no re-drive. `block_event.replay_failed` is the only signal. Do not
    // add further checks here on the reasoning that these ones are already present.
    let store_def = store.store_def();
    if !store_def.is_add_type(message) {
        return Err(HubError::validation_failure("invalid channel message type"));
    }
    // The key-shape invariant gates everything else: a wrong-width channel_id would make the
    // slot keyspace ambiguous (see CHANNEL_ID_LENGTH), so it is checked before any key is built.
    let channel_id = store_def.channel_id(message)?;
    if channel_id.len() != CHANNEL_ID_LENGTH {
        return Err(HubError::validation_failure("channel id must be 32 bytes"));
    }
    store_def.validate_slot_message(message)?;
    // CONSENSUS-CRITICAL DEVIATION: unlike data-shard stores, channel slots never compare
    // embedded timestamps or ts_hash ordering. Shard-0 consensus merge order is the total order;
    // the latest call reaching this function replaces the slot incumbent unconditionally.
    let data = message
        .data
        .as_ref()
        .ok_or_else(|| HubError::validation_failure("message data is missing"))?;
    let ts_hash = make_ts_hash(data.timestamp, &message.hash)?;
    let slot_key = store_def.slot_key(message)?;
    let incumbent = load_slot_message(store, txn, &slot_key)?;
    if incumbent
        .as_ref()
        .is_some_and(|current| current.hash == message.hash && current.fid() == message.fid())
    {
        return Err(HubError::duplicate("message has already been merged"));
    }

    let mut new_slot_count = None;
    if incumbent.is_none() {
        if let (Some(cap), Some(count_key)) =
            (store_def.slot_cap(), store_def.slot_count_key(&channel_id))
        {
            let count = read_counter(&store.db(), txn, &count_key)?;
            if count >= cap {
                return Err(HubError::validation_failure("channel slot cap exceeded"));
            }
            new_slot_count = Some((
                count_key,
                count.checked_add(1).ok_or_else(|| {
                    warn!("Channel slot count overflow");
                    HubError::invalid_internal_state("channel slot count overflow")
                })?,
            ));
        }
    }

    let old_is_moderator = incumbent
        .as_ref()
        .map(|current| store_def.is_live_moderator(current))
        .transpose()?
        .flatten()
        .unwrap_or(false);
    let new_is_moderator = store_def.is_live_moderator(message)?.unwrap_or(false);
    let new_live_moderator_count = if old_is_moderator != new_is_moderator {
        let key = channel_index_key(ChannelIndex::LiveModeratorCount, &channel_id, &[]);
        let count = read_counter(&store.db(), txn, &key)?;
        let next = if new_is_moderator {
            count.checked_add(1).ok_or_else(|| {
                warn!("Live moderator count overflow");
                HubError::invalid_internal_state("live moderator count overflow")
            })?
        } else {
            count.checked_sub(1).ok_or_else(|| {
                warn!("Live moderator count underflow");
                HubError::invalid_internal_state("live moderator count underflow")
            })?
        };
        Some((key, next))
    } else {
        None
    };

    // Stage every mutation separately and merge it into the caller only on the Ok arm. A failed
    // cap check, counter transition, delete, or put therefore cannot leak a counter delta.
    let mut slot_txn = RocksDbTransactionBatch::new();
    let deleted_messages: Vec<Message> = incumbent.into_iter().collect();
    store.delete_many_transaction(&mut slot_txn, &deleted_messages)?;
    slot_txn.put(slot_key, encode_slot_pointer(message, &ts_hash)?);
    if let Some((key, value)) = new_slot_count {
        slot_txn.put(key, value.to_be_bytes().to_vec());
    }
    if let Some((key, value)) = new_live_moderator_count {
        if value == 0 {
            slot_txn.delete(key);
        } else {
            slot_txn.put(key, value.to_be_bytes().to_vec());
        }
    }
    // Channel slot messages are never a channel follow — a follow is an ordinary
    // ReactionAdd on the author's own shard, and these four types are shard-0
    // authority state.
    let event = store.merge_add_with_conflicts(
        &ts_hash,
        message,
        &mut slot_txn,
        deleted_messages,
        FollowIndexGate::no_follow_carrier(),
    )?;
    store_def.build_gated_secondary_indices(&mut slot_txn, message, gate)?;
    txn.merge(slot_txn);
    Ok(event)
}

fn read_slot<T: ChannelSlotStoreDef + Clone>(
    store: &Store<T>,
    slot_key: Vec<u8>,
    maybe_txn: Option<&RocksDbTransactionBatch>,
) -> Result<Option<Message>, HubError> {
    let empty_txn = RocksDbTransactionBatch::new();
    load_slot_message(store, maybe_txn.unwrap_or(&empty_txn), &slot_key)
}

fn read_channel_counter<T: StoreDef + Clone>(
    store: &Store<T>,
    key: Vec<u8>,
    maybe_txn: Option<&RocksDbTransactionBatch>,
) -> Result<u32, HubError> {
    let empty_txn = RocksDbTransactionBatch::new();
    read_counter(&store.db(), maybe_txn.unwrap_or(&empty_txn), &key)
}

macro_rules! impl_channel_store_def {
    (
        $def:ty,
        $store_name:literal,
        $message_postfix:expr,
        $index_postfix:expr,
        $message_type:expr,
        $slot_suffix:ident
    ) => {
        impl StoreDef for $def {
            fn postfix(&self) -> u8 {
                $message_postfix.as_u8()
            }

            fn add_message_type(&self) -> u8 {
                $message_type as u8
            }

            fn remove_message_type(&self) -> u8 {
                MessageType::None as u8
            }

            fn compact_state_message_type(&self) -> u8 {
                MessageType::None as u8
            }

            fn is_add_type(&self, message: &Message) -> bool {
                is_channel_message(message, $message_type)
            }

            fn is_remove_type(&self, _message: &Message) -> bool {
                false
            }

            fn requires_consensus_order_slot_merge(&self) -> bool {
                true
            }

            fn is_compact_state_type(&self, _message: &Message) -> bool {
                false
            }

            fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
                author_slot_key(message, $index_postfix, &$slot_suffix(message)?)
            }

            fn make_remove_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn get_prune_size_limit(&self) -> u32 {
                self.prune_size_limit
            }
        }
    };
}

impl_channel_store_def!(
    ChannelUpdateStoreDef,
    "ChannelUpdateStore",
    UserPostfix::ChannelUpdateMessage,
    UserPostfix::ChannelUpdateAdds,
    MessageType::ChannelUpdate,
    update_slot_suffix
);
impl_channel_store_def!(
    ChannelMemberStoreDef,
    "ChannelMemberStore",
    UserPostfix::ChannelMemberMessage,
    UserPostfix::ChannelMemberAdds,
    MessageType::ChannelMember,
    member_slot_suffix
);
impl_channel_store_def!(
    ChannelPinStoreDef,
    "ChannelPinStore",
    UserPostfix::ChannelPinMessage,
    UserPostfix::ChannelPinAdds,
    MessageType::ChannelPin,
    pin_slot_suffix
);
impl_channel_store_def!(
    ChannelModerateStoreDef,
    "ChannelModerateStore",
    UserPostfix::ChannelModerateMessage,
    UserPostfix::ChannelModerateAdds,
    MessageType::ChannelModerate,
    moderate_slot_suffix
);

impl ChannelSlotStoreDef for ChannelUpdateStoreDef {
    fn channel_id(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        update_slot_suffix(message)
    }

    fn slot_cap(&self) -> Option<u32> {
        self.slot_cap
    }

    /// Without this the slot could accept a body whose modes `get_channel_update` cannot parse,
    /// leaving every read of the channel erroring until something supersedes it.
    fn validate_slot_message(&self, message: &Message) -> Result<(), HubError> {
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelUpdateBody(body)) => fold_channel_modes(body).map(|_| ()),
            _ => Err(invalid_body("ChannelUpdate")),
        }
    }

    fn slot_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        Ok(channel_index_key(
            ChannelIndex::UpdateSlot,
            &update_slot_suffix(message)?,
            &[],
        ))
    }
}

impl ChannelSlotStoreDef for ChannelMemberStoreDef {
    fn channel_id(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelMemberBody(body)) => Ok(body.channel_id.clone()),
            _ => Err(invalid_body("ChannelMember")),
        }
    }

    fn slot_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let body = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelMemberBody(body)) => body,
            _ => return Err(invalid_body("ChannelMember")),
        };
        let target_fid = u32::try_from(body.fid)
            .map_err(|_| HubError::invalid_parameter("channel member fid exceeds u32"))?;
        Ok(channel_index_key(
            ChannelIndex::MemberSlot,
            &body.channel_id,
            &target_fid.to_be_bytes(),
        ))
    }

    fn slot_cap(&self) -> Option<u32> {
        self.slot_cap
    }

    fn validate_slot_message(&self, message: &Message) -> Result<(), HubError> {
        member_state_for_message(message).map(|_| ())
    }

    fn slot_count_key(&self, channel_id: &[u8]) -> Option<Vec<u8>> {
        Some(channel_index_key(
            ChannelIndex::MemberSlotCount,
            channel_id,
            &[],
        ))
    }

    fn is_live_moderator(&self, message: &Message) -> Result<Option<bool>, HubError> {
        Ok(Some(
            member_state_for_message(message)? == ChannelMemberState::Moderator,
        ))
    }

    fn build_gated_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        message: &Message,
        gate: DerivedIndexGate,
    ) -> Result<(), HubError> {
        if !gate.writes_derived_indices() {
            return Ok(());
        }
        let body = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelMemberBody(body)) => body,
            _ => return Err(invalid_body("ChannelMember")),
        };
        txn.put(
            ChannelMemberStoreDef::make_member_by_fid_key(body.fid, &body.channel_id)?,
            Vec::new(),
        );
        Ok(())
    }
}

impl ChannelMemberStoreDef {
    pub fn make_member_by_fid_key(target_fid: u64, channel_id: &[u8]) -> Result<Vec<u8>, HubError> {
        let target_fid = u32::try_from(target_fid)
            .map_err(|_| HubError::invalid_parameter("channel member fid exceeds u32"))?;
        let mut key = Vec::with_capacity(2 + 4 + channel_id.len());
        key.push(RootPrefix::Channel as u8);
        key.push(ChannelIndex::MemberByFid as u8);
        key.extend_from_slice(&target_fid.to_be_bytes());
        key.extend_from_slice(channel_id);
        Ok(key)
    }
}

impl ChannelSlotStoreDef for ChannelPinStoreDef {
    fn channel_id(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        pin_slot_suffix(message)
    }

    fn validate_slot_message(&self, message: &Message) -> Result<(), HubError> {
        // Unlike the moderate slot, the pin key is `channel_id` alone — cast_hash is
        // payload, not key material — so an off-width hash cannot corrupt the
        // keyspace. Enforced anyway to match `validate_channel_pin_body`, because the
        // paths that skip stateless validation (snapshot bootstrap, replication
        // replay) reach this merge directly, and `GetChannelPin` echoes the bytes it
        // finds. Empty is legal: that is an unpin.
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelPinBody(body))
                if body.cast_hash.is_empty()
                    || body.cast_hash.len() == CHANNEL_MODERATE_CAST_HASH_LENGTH =>
            {
                Ok(())
            }
            Some(Body::ChannelPinBody(_)) => Err(HubError::validation_failure(
                "channel pin cast hash must be empty or 20 bytes",
            )),
            _ => Err(invalid_body("ChannelPin")),
        }
    }

    fn slot_cap(&self) -> Option<u32> {
        self.slot_cap
    }

    fn slot_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        Ok(channel_index_key(
            ChannelIndex::PinSlot,
            &pin_slot_suffix(message)?,
            &[],
        ))
    }
}

impl ChannelSlotStoreDef for ChannelModerateStoreDef {
    fn channel_id(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelModerateBody(body)) => Ok(body.channel_id.clone()),
            _ => Err(invalid_body("ChannelModerate")),
        }
    }

    fn slot_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let body = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelModerateBody(body)) => body,
            _ => return Err(invalid_body("ChannelModerate")),
        };
        Ok(channel_index_key(
            ChannelIndex::ModerateSlot,
            &body.channel_id,
            &body.cast_hash,
        ))
    }

    fn slot_cap(&self) -> Option<u32> {
        self.slot_cap
    }

    fn validate_slot_message(&self, message: &Message) -> Result<(), HubError> {
        moderation_state_for_message(message)?;
        // The moderate slot key is `channel_id ++ cast_hash`; a fixed-width channel_id already
        // restores injectivity, but pinning cast_hash too keeps the key bounded and the layout
        // self-describing rather than resting on the other field's width.
        match message.data.as_ref().and_then(|data| data.body.as_ref()) {
            Some(Body::ChannelModerateBody(body))
                if body.cast_hash.len() == CHANNEL_MODERATE_CAST_HASH_LENGTH =>
            {
                Ok(())
            }
            Some(Body::ChannelModerateBody(_)) => Err(HubError::validation_failure(
                "channel moderate cast hash must be 20 bytes",
            )),
            _ => Err(invalid_body("ChannelModerate")),
        }
    }

    fn slot_count_key(&self, channel_id: &[u8]) -> Option<Vec<u8>> {
        Some(channel_index_key(
            ChannelIndex::ModerateSlotCount,
            channel_id,
            &[],
        ))
    }
}

macro_rules! define_channel_store {
    // `$default_slot_cap` is the store's production slot cap as an `Option<u32>` (`None` for the
    // uncapped update/pin stores). `StoreOptions::channel_slot_cap_override` is a test-only knob
    // that replaces that cap with a small value so slot-boundary tests don't insert thousands of
    // rows; it only affects capped stores, and `None` leaves the production cap in place.
    ($store:ident, $def:ident, $default_slot_cap:expr) => {
        pub struct $store;

        impl $store {
            pub fn new(
                db: Arc<RocksDB>,
                store_event_handler: Arc<StoreEventHandler>,
                prune_size_limit: u32,
            ) -> Store<$def> {
                Self::new_with_opts(
                    db,
                    store_event_handler,
                    prune_size_limit,
                    StoreOptions::default(),
                )
            }

            pub fn new_with_opts(
                db: Arc<RocksDB>,
                store_event_handler: Arc<StoreEventHandler>,
                prune_size_limit: u32,
                store_opts: StoreOptions,
            ) -> Store<$def> {
                let slot_cap = $default_slot_cap
                    .map(|cap| store_opts.channel_slot_cap_override.unwrap_or(cap));
                Store::new_with_store_def_opts(
                    db,
                    store_event_handler,
                    $def {
                        prune_size_limit,
                        slot_cap,
                    },
                    store_opts,
                )
            }

            /// The slot cap this store was actually constructed with.
            ///
            /// Boundary behavior is exercised through the test-only override, which
            /// means nothing else proves the PRODUCTION constant is the one wired in
            /// here. This lets a test assert that separately from exercising it.
            pub fn slot_cap(store: &Store<$def>) -> Option<u32> {
                ChannelSlotStoreDef::slot_cap(&store.store_def())
            }
        }
    };
}

define_channel_store!(ChannelUpdateStore, ChannelUpdateStoreDef, None);
define_channel_store!(
    ChannelMemberStore,
    ChannelMemberStoreDef,
    Some(CHANNEL_MEMBER_SLOT_CAP)
);
define_channel_store!(ChannelPinStore, ChannelPinStoreDef, None);
define_channel_store!(
    ChannelModerateStore,
    ChannelModerateStoreDef,
    Some(CHANNEL_MODERATE_SLOT_CAP)
);

impl ChannelUpdateStore {
    pub fn merge(
        store: &Store<ChannelUpdateStoreDef>,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
        gate: DerivedIndexGate,
    ) -> Result<HubEvent, HubError> {
        merge_slot(store, message, txn, gate)
    }

    pub fn slot_key(channel_id: &[u8]) -> Vec<u8> {
        channel_index_key(ChannelIndex::UpdateSlot, channel_id, &[])
    }

    /// Effective modes for a registered channel that has no ChannelUpdate yet.
    ///
    /// Derived from `fold_channel_modes` rather than restated, so the read path's
    /// "no update" branch cannot drift from the policy admission actually applies.
    /// An unconfigured channel and one whose update omitted both modes must report
    /// the same thing, because to the fold they ARE the same thing.
    pub fn default_channel_modes() -> (CastingMode, MembershipMode) {
        fold_channel_modes(&ChannelUpdateBody::default())
            .expect("an empty channel update body folds to the restrictive defaults")
    }

    pub fn get_channel_update(
        store: &Store<ChannelUpdateStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelUpdateState>, HubError> {
        let Some(message) = read_slot(store, Self::slot_key(channel_id), maybe_txn)? else {
            return Ok(None);
        };
        let body = match message.data.and_then(|data| data.body) {
            Some(Body::ChannelUpdateBody(body)) => body,
            _ => return Err(invalid_body("ChannelUpdate")),
        };
        // ChannelUpdate is a whole-replace fold: absent fields are unset, never inherited from
        // the superseded message. `fold_channel_modes` resolves the modes restrictively.
        let (casting_mode, membership_mode) = fold_channel_modes(&body)?;
        Ok(Some(ChannelUpdateState {
            body,
            casting_mode,
            membership_mode,
        }))
    }
}

impl ChannelMemberStore {
    pub fn merge(
        store: &Store<ChannelMemberStoreDef>,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
        gate: DerivedIndexGate,
    ) -> Result<HubEvent, HubError> {
        merge_slot(store, message, txn, gate)
    }

    pub fn slot_key(channel_id: &[u8], target_fid: u64) -> Result<Vec<u8>, HubError> {
        let target_fid = u32::try_from(target_fid)
            .map_err(|_| HubError::invalid_parameter("channel member fid exceeds u32"))?;
        Ok(channel_index_key(
            ChannelIndex::MemberSlot,
            channel_id,
            &target_fid.to_be_bytes(),
        ))
    }

    pub fn member_state(
        store: &Store<ChannelMemberStoreDef>,
        channel_id: &[u8],
        target_fid: u64,
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelMemberState>, HubError> {
        read_slot(store, Self::slot_key(channel_id, target_fid)?, maybe_txn)?
            .map(|message| stored_member_state_for_message(&message))
            .transpose()
    }

    pub fn member(
        store: &Store<ChannelMemberStoreDef>,
        channel_id: &[u8],
        target_fid: u64,
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelMemberEntry>, HubError> {
        read_slot(store, Self::slot_key(channel_id, target_fid)?, maybe_txn)?
            .map(|message| {
                let last_action_ts = message
                    .data
                    .as_ref()
                    .ok_or_else(|| HubError::invalid_internal_state("channel member missing data"))?
                    .timestamp;
                Ok(ChannelMemberEntry {
                    fid: target_fid,
                    state: stored_member_state_for_message(&message)?,
                    last_action_ts,
                })
            })
            .transpose()
    }

    pub fn members_by_channel(
        store: &Store<ChannelMemberStoreDef>,
        channel_id: &[u8],
        state_filter: Option<ChannelMemberState>,
        page_options: &PageOptions,
    ) -> Result<ChannelPage<ChannelMemberEntry>, HubError> {
        let prefix = channel_index_key(ChannelIndex::MemberSlot, channel_id, &[]);
        require_page_token_in_prefix(&prefix, page_options)?;
        let empty_txn = RocksDbTransactionBatch::new();
        let page_size = page_options.page_size.unwrap_or(PAGE_SIZE_MAX);
        let mut entries = Vec::new();
        let mut last_key = None;
        let all_done = store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, pointer| {
                if key.len() != prefix.len() + 4 {
                    return Err(HubError::invalid_internal_state(
                        "channel member slot key has invalid length",
                    ));
                }
                let fid = read_fid_key(key, prefix.len());
                // `pointer` is the row's own value, so this resolves the primary
                // message without re-reading the slot key.
                let message =
                    message_for_slot_pointer(store, &empty_txn, pointer).map_err(|err| {
                        warn!(
                            channel_id = hex::encode(channel_id),
                            fid, "channel member slot could not be resolved from its pointer",
                        );
                        err
                    })?;
                let state = stored_member_state_for_message(&message)?;
                if state_filter.is_none_or(|filter| filter == state) {
                    let last_action_ts = message
                        .data
                        .as_ref()
                        .ok_or_else(|| {
                            HubError::invalid_internal_state("channel member missing data")
                        })?
                        .timestamp;
                    entries.push(ChannelMemberEntry {
                        fid,
                        state,
                        last_action_ts,
                    });
                    last_key = Some(key.to_vec());
                    if entries.len() >= page_size {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
        )?;
        Ok(ChannelPage {
            entries,
            next_page_token: (!all_done).then_some(last_key).flatten(),
        })
    }

    pub fn memberships_by_fid(
        store: &Store<ChannelMemberStoreDef>,
        target_fid: u64,
        page_options: &PageOptions,
    ) -> Result<ChannelPage<ChannelMembershipEntry>, HubError> {
        let prefix = ChannelMemberStoreDef::make_member_by_fid_key(target_fid, &[])?;
        require_page_token_in_prefix(&prefix, page_options)?;
        let page_size = page_options.page_size.unwrap_or(PAGE_SIZE_MAX);
        let mut entries = Vec::new();
        let mut last_key = None;
        let all_done = store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, _| {
                if key.len() != prefix.len() + CHANNEL_ID_LENGTH {
                    return Err(HubError::invalid_internal_state(
                        "channel member by-fid key has invalid length",
                    ));
                }
                let channel_id = key[prefix.len()..].to_vec();
                let state =
                    Self::member_state(store, &channel_id, target_fid, None)?.ok_or_else(|| {
                        warn!(
                            target_fid,
                            channel_id = hex::encode(&channel_id),
                            "channel member by-fid index points to a missing slot",
                        );
                        HubError::invalid_internal_state(
                            "channel member by-fid index points to a missing slot",
                        )
                    })?;
                entries.push(ChannelMembershipEntry { channel_id, state });
                last_key = Some(key.to_vec());
                Ok(entries.len() >= page_size)
            },
        )?;
        Ok(ChannelPage {
            entries,
            next_page_token: (!all_done).then_some(last_key).flatten(),
        })
    }

    pub fn slot_count(
        store: &Store<ChannelMemberStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<u32, HubError> {
        read_channel_counter(
            store,
            channel_index_key(ChannelIndex::MemberSlotCount, channel_id, &[]),
            maybe_txn,
        )
    }

    pub fn live_moderator_count(
        store: &Store<ChannelMemberStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<u32, HubError> {
        read_channel_counter(
            store,
            channel_index_key(ChannelIndex::LiveModeratorCount, channel_id, &[]),
            maybe_txn,
        )
    }
}

impl ChannelPinStore {
    pub fn merge(
        store: &Store<ChannelPinStoreDef>,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
        gate: DerivedIndexGate,
    ) -> Result<HubEvent, HubError> {
        merge_slot(store, message, txn, gate)
    }

    pub fn slot_key(channel_id: &[u8]) -> Vec<u8> {
        channel_index_key(ChannelIndex::PinSlot, channel_id, &[])
    }

    pub fn get_channel_pin(
        store: &Store<ChannelPinStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelPinBody>, HubError> {
        read_slot(store, Self::slot_key(channel_id), maybe_txn)?
            .map(|message| match message.data.and_then(|data| data.body) {
                Some(Body::ChannelPinBody(body)) => Ok(body),
                _ => Err(invalid_body("ChannelPin")),
            })
            .transpose()
    }

    pub fn get_channel_pin_state(
        store: &Store<ChannelPinStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelPinState>, HubError> {
        read_slot(store, Self::slot_key(channel_id), maybe_txn)?
            .map(|message| {
                let author_fid = message.fid();
                match message.data.and_then(|data| data.body) {
                    Some(Body::ChannelPinBody(body)) => Ok(ChannelPinState { body, author_fid }),
                    _ => Err(invalid_body("ChannelPin")),
                }
            })
            .transpose()
    }
}

impl ChannelModerateStore {
    pub fn merge(
        store: &Store<ChannelModerateStoreDef>,
        message: &Message,
        txn: &mut RocksDbTransactionBatch,
        gate: DerivedIndexGate,
    ) -> Result<HubEvent, HubError> {
        merge_slot(store, message, txn, gate)
    }

    pub fn slot_key(channel_id: &[u8], cast_hash: &[u8]) -> Vec<u8> {
        channel_index_key(ChannelIndex::ModerateSlot, channel_id, cast_hash)
    }

    pub fn moderation_state(
        store: &Store<ChannelModerateStoreDef>,
        channel_id: &[u8],
        cast_hash: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<ChannelModerationState>, HubError> {
        read_slot(store, Self::slot_key(channel_id, cast_hash), maybe_txn)?
            .map(|message| stored_moderation_state_for_message(&message))
            .transpose()
    }

    pub fn slot_count(
        store: &Store<ChannelModerateStoreDef>,
        channel_id: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<u32, HubError> {
        read_channel_counter(
            store,
            channel_index_key(ChannelIndex::ModerateSlotCount, channel_id, &[]),
            maybe_txn,
        )
    }

    pub fn moderations_by_channel(
        store: &Store<ChannelModerateStoreDef>,
        channel_id: &[u8],
        page_options: &PageOptions,
    ) -> Result<ChannelPage<ChannelModerationEntry>, HubError> {
        let prefix = channel_index_key(ChannelIndex::ModerateSlot, channel_id, &[]);
        require_page_token_in_prefix(&prefix, page_options)?;
        let empty_txn = RocksDbTransactionBatch::new();
        let page_size = page_options.page_size.unwrap_or(PAGE_SIZE_MAX);
        let mut entries = Vec::new();
        let mut last_key = None;
        let all_done = store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(increment_vec_u8(&prefix)),
            page_options,
            |key, pointer| {
                if key.len() != prefix.len() + CHANNEL_MODERATE_CAST_HASH_LENGTH {
                    return Err(HubError::invalid_internal_state(
                        "channel moderate slot key has invalid length",
                    ));
                }
                // `pointer` is the row's own value, so this resolves the primary
                // message without re-reading the slot key.
                let message =
                    message_for_slot_pointer(store, &empty_txn, pointer).map_err(|err| {
                        warn!(
                            channel_id = hex::encode(channel_id),
                            "channel moderate slot could not be resolved from its pointer",
                        );
                        err
                    })?;
                let body = match message.data.as_ref().and_then(|data| data.body.as_ref()) {
                    Some(Body::ChannelModerateBody(body)) => body,
                    _ => {
                        return Err(HubError::invalid_internal_state(
                            "invalid ChannelModerate body",
                        ))
                    }
                };
                let action = ChannelModerateAction::try_from(body.action).map_err(|_| {
                    HubError::invalid_internal_state("invalid channel moderate action")
                })?;
                entries.push(ChannelModerationEntry {
                    cast_hash: body.cast_hash.clone(),
                    action,
                    author_fid: message.fid(),
                });
                last_key = Some(key.to_vec());
                Ok(entries.len() >= page_size)
            },
        )?;
        Ok(ChannelPage {
            entries,
            next_page_token: (!all_done).then_some(last_key).flatten(),
        })
    }
}
