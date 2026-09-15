use super::rpc_extensions::{
    authenticate_request, AsMessagesResponse, AsSingleMessageResponse, FidRequestExt,
    FidTimestampRequestExt, LinksByFidRequestExt, ReactionsByFidRequestExt,
};
use crate::connectors::fname::FnameTransferLookup;
use crate::connectors::onchain_events::{Chain, ChainClients};
use crate::core::error::HubError;
use crate::core::types::SnapchainValidatorContext;
use crate::core::util::{get_farcaster_time, FarcasterTime};
use crate::core::validations;
use crate::core::validations::verification::VerificationAddressClaim;
use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::mempool::routing;
use crate::network::gossip::GossipEvent;
use crate::network::mesh::cache::MeshCache;
use crate::network::mesh::crawl::crawl_mesh;
use crate::network::mesh::view::{build_validator_peer_ids, classify_mesh_view, ValidatorPeerIds};
use crate::proto::hub_service_server::HubService;
use crate::proto::{
    self, cast_add_body, casts_by_parent_request, link_body, links_by_target_request, message_data,
    on_chain_event::Body, reaction_body, reactions_by_target_request, Block, BlocksRequest, CastId,
    CastsByParentRequest, ChannelFollow, ChannelFollower, ChannelFollowerCountRequest,
    ChannelFollowerCountResponse, ChannelFollowersRequest, ChannelFollowersResponse,
    ChannelFollowsRequest, ChannelFollowsResponse, ChannelInfo, ChannelMember,
    ChannelMemberRequest, ChannelMemberResponse, ChannelMembersRequest, ChannelMembersResponse,
    ChannelMembership, ChannelMembershipsByFidRequest, ChannelMembershipsResponse,
    ChannelMetadataResponse, ChannelModeration, ChannelModerationsRequest,
    ChannelModerationsResponse, ChannelOwnerRequest, ChannelOwnerResponse, ChannelPin,
    ChannelPinResponse, ChannelRequest, ChannelsByAddressRequest, ChannelsByFidRequest,
    ChannelsResponse, DbStats, EventRequest, EventsRequest, EventsResponse, FidAddressTypeRequest,
    FidAddressTypeResponse, FidRequest, FidTimestampRequest, FidsRequest, FidsResponse,
    GetConnectedPeersRequest, GetConnectedPeersResponse, GetInfoRequest, GetInfoResponse,
    GetMeshViewRequest, Height, HubEvent, IdRegistryEventByAddressRequest,
    IsFollowingChannelRequest, IsFollowingChannelResponse, LinkRequest, LinksByFidRequest,
    LinksByTargetRequest, MeshTopology, MeshView, Message, MessageType, MessagesResponse,
    OnChainEvent, OnChainEventRequest, OnChainEventResponse, ReactionRequest, ReactionType,
    ReactionsByFidRequest, ReactionsByTargetRequest, ShardChunk, ShardChunksRequest,
    ShardChunksResponse, Signer, SignerEventType, SignerRequest, SignerResponse, SignerSource,
    SignersByFidRequest, SignersByFidResponse, StorageLimitsResponse, SubscribeRequest,
    TrieNodeMetadataRequest, TrieNodeMetadataResponse, UserDataRequest, UserNameProof,
    UserNameType, UsernameProofRequest, UsernameProofsResponse, ValidationResponse,
    VerificationAddAddressBody, VerificationRequest,
};
use crate::storage::constants::OnChainEventPostfix;
use crate::storage::constants::RootPrefix;
use crate::storage::constants::PAGE_SIZE_MAX;
use crate::storage::db::PageOptions;
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::account::MessagesPage;
use crate::storage::store::account::UsernameProofStore;
use crate::storage::store::account::{
    get_app_nonce, get_gasless_key_count, get_gasless_key_record, get_last_used_at, get_user_nonce,
    list_gasless_keys_by_fid, CastStore, ChannelMemberState as StoredChannelMemberState,
    ChannelMemberStore, ChannelModerateStore, ChannelPinStore, ChannelUpdateStore,
    GaslessKeyRecord, LinkStore, OnchainEventStorageError, ReactionStore, UserDataStore,
    VerificationStore, CHANNEL_ID_LENGTH,
};
use crate::storage::store::account::{
    get_channel_keys_by_owner_address, get_channel_keys_for_owner_addresses,
};
use crate::storage::store::account::{message_bytes_decode, IntoI32};
use crate::storage::store::account::{EventsPage, HubEventIdGenerator};
use crate::storage::store::block_engine::{self, BlockStores};
use crate::storage::store::engine::{self, Senders, ShardEngine};
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::stores::Stores;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use crate::version::version::{EngineVersion, ProtocolFeature};
use hex::ToHex;
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataValue;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

pub const MEMPOOL_ADD_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(100);

// Time budget for recovering from a `MissingFname` validation failure on UserDataAdd
// Username messages: fetch the transfer from the fname registry, push it to the
// mempool, then poll for the proof to land in the local store. Block production cycles
// in seconds, so 8s gives ~4 block opportunities before we give up and return the
// original error to the client.
const MISSING_FNAME_RECOVERY_BUDGET: Duration = Duration::from_secs(8);
const MISSING_FNAME_POLL_INTERVAL: Duration = Duration::from_millis(250);
// Cap the synchronous fname-registry lookup so a slow/hung registry can't stall
// the gRPC request beyond the recovery budget.
const MISSING_FNAME_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Convert a typed engine validation error back into the HubError shape that the
/// gRPC layer expects. `StoreError` is forwarded as-is so we don't double-wrap;
/// every other variant becomes a generic validation_failure.
fn simulate_error_to_hub_error(err: engine::MessageValidationError) -> HubError {
    match err {
        engine::MessageValidationError::StoreError(hub_error) => hub_error,
        _ => HubError::validation_failure(&err.to_string()),
    }
}

/// Returns the fname that should be looked up against the fname registry to
/// recover from a `MissingFname` validation failure, or `None` if the message
/// isn't an fname-eligible UserDataAdd Username (.eth/empty values are out of
/// scope — they go through ENS / are valid no-ops).
fn username_for_fname_recovery(message: &proto::Message) -> Option<String> {
    let data = message.data.as_ref()?;
    let user_data = match data.body.as_ref()? {
        proto::message_data::Body::UserDataBody(body) => body,
        _ => return None,
    };
    if user_data.r#type() != proto::UserDataType::Username {
        return None;
    }
    if user_data.value.is_empty() || user_data.value.ends_with(".eth") {
        return None;
    }
    Some(user_data.value.clone())
}

/// Translate a HubError raised by the gasless / signer stores into a gRPC
/// `Status`. `bad_request.*` codes (validation_failure, invalid_param, …) come
/// from caller-supplied input — typically a malformed public key — so they
/// surface as `invalid_argument` rather than 500. Everything else is a true
/// storage failure and stays as `internal`.
fn signer_store_error_to_status(err: HubError) -> Status {
    if err.code.starts_with("bad_request") {
        Status::invalid_argument(err.to_string())
    } else {
        Status::internal(format!("Store error: {:?}", err))
    }
}

fn onchain_event_storage_error_to_status(err: OnchainEventStorageError) -> Status {
    match err {
        OnchainEventStorageError::HubError(hub_error)
            if hub_error.code.starts_with("bad_request") =>
        {
            Status::invalid_argument(hub_error.to_string())
        }
        other => Status::internal(format!("Store error: {:?}", other)),
    }
}

fn channel_member_state_to_proto(state: StoredChannelMemberState) -> proto::ChannelMemberState {
    match state {
        StoredChannelMemberState::Member => proto::ChannelMemberState::Member,
        StoredChannelMemberState::Moderator => proto::ChannelMemberState::Moderator,
        StoredChannelMemberState::Removed => proto::ChannelMemberState::Removed,
        StoredChannelMemberState::Banned => proto::ChannelMemberState::Banned,
    }
}

fn channel_member_state_from_proto(
    state: proto::ChannelMemberState,
) -> Option<StoredChannelMemberState> {
    match state {
        proto::ChannelMemberState::None => None,
        proto::ChannelMemberState::Member => Some(StoredChannelMemberState::Member),
        proto::ChannelMemberState::Moderator => Some(StoredChannelMemberState::Moderator),
        proto::ChannelMemberState::Removed => Some(StoredChannelMemberState::Removed),
        proto::ChannelMemberState::Banned => Some(StoredChannelMemberState::Banned),
    }
}

fn channel_page_options(
    page_size: Option<u32>,
    page_token: Option<Vec<u8>>,
    reverse: Option<bool>,
) -> PageOptions {
    PageOptions {
        page_size: Some(
            page_size
                .map(|size| size as usize)
                .unwrap_or(PAGE_SIZE_MAX)
                .min(PAGE_SIZE_MAX),
        ),
        // `optional bytes` lets a client send an explicitly empty token, which
        // generated clients do when they echo back an absent `next_page_token`.
        // Treat it as "first page" the way `page_options` in rpc_extensions does,
        // rather than letting it reach the store as an out-of-prefix cursor.
        page_token: page_token.filter(|token| !token.is_empty()),
        reverse: reverse.unwrap_or(false),
    }
}

/// Rejects `page_size: 0` on the paginated channel reads.
///
/// Zero is the natural default an unset integer takes in generated clients, and
/// serving it as an empty page with no `next_page_token` would assert "this
/// channel has no members, enumeration complete" — indistinguishable from the
/// truth. Callers wanting the server default must omit the field.
fn require_nonzero_page_size(page_size: Option<u32>) -> Result<(), Status> {
    if page_size == Some(0) {
        return Err(Status::invalid_argument(
            "page_size must be greater than zero; omit it for the server default",
        ));
    }
    Ok(())
}

/// Validates the width of a caller-supplied channel id and returns it as a
/// fixed-width array.
///
/// Split out of `require_registered_channel` so the follow reads can share the
/// width check without the registration check. Follows are answered from data
/// shards and deliberately do not require a registered channel — see the contract
/// on `GetChannelFollowers` in rpc.proto.
fn require_channel_id_width(channel_id: &[u8]) -> Result<[u8; CHANNEL_ID_LENGTH], Status> {
    channel_id
        .try_into()
        .map_err(|_| Status::invalid_argument("channel_id must be 32 bytes"))
}

/// Where one shard's scan should pick up.
///
/// Three states rather than an `Option`, because `Exhausted` and `Fresh` are not
/// the same instruction: handing an exhausted shard "start from the beginning"
/// restarts it, re-emitting its rows on every subsequent page and never
/// terminating.
///
/// This distinction is spelled out ON THE WIRE — the enum is what gets
/// serialized — rather than reconstructed by a decoder from `Option::None`. With
/// `Option<Vec<u8>>` on the wire, serde's missing-field-means-`None` rule made a
/// truncated token like `{"shard_id":1}` decode as `Exhausted`, so a proxy that
/// strips null fields would turn every shard "complete" and the read would answer
/// `followers: []` with no next token — "this channel has no followers", as a
/// success.
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
enum ShardScan {
    Fresh,
    Resume(Vec<u8>),
    Exhausted,
}

/// One shard's position in a fan-out enumeration.
///
/// Carries the shard id rather than relying on position, so a cursor cannot be
/// silently applied to the wrong shard if the hosted set changes between pages —
/// the failure `get_casts_by_parent` has, zipping tokens against `HashMap` order.
///
/// `deny_unknown_fields` because this is pagination state, not a forward-
/// compatible API: silently ignoring a field a newer node meant something by is
/// how a cursor quietly pages the wrong range.
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ShardCursor {
    shard_id: u32,
    scan: ShardScan,
}

/// Decodes a fan-out `page_token` into one scan position per hosted shard.
///
/// A present token must name exactly the node's shard set, each shard once: a
/// mismatch means it was minted against a different topology, and applying it
/// would page the wrong shards.
fn decode_shard_cursors(
    page_token: Option<Vec<u8>>,
    shards: &[(u32, &Stores)],
) -> Result<HashMap<u32, ShardScan>, Status> {
    let Some(bytes) = page_token.filter(|token| !token.is_empty()) else {
        return Ok(shards
            .iter()
            .map(|(id, _)| (*id, ShardScan::Fresh))
            .collect());
    };
    let cursors: Vec<ShardCursor> = serde_json::from_slice(&bytes)
        .map_err(|err| Status::invalid_argument(format!("invalid page token: {err}")))?;

    // Built with an explicit loop rather than `collect()`: a `HashMap` silently
    // keeps the last of a duplicated key, and a length check afterwards only
    // notices when the collapse drops below the shard count. A token listing one
    // shard twice plus every other shard once would otherwise pass, with one
    // cursor discarded.
    let mut scans: HashMap<u32, ShardScan> = HashMap::with_capacity(cursors.len());
    for cursor in cursors {
        let shard_id = cursor.shard_id;
        if scans.insert(shard_id, cursor.scan).is_some() {
            return Err(Status::invalid_argument(format!(
                "page token names shard {shard_id} more than once"
            )));
        }
    }
    if scans.len() != shards.len() || shards.iter().any(|(id, _)| !scans.contains_key(id)) {
        return Err(Status::invalid_argument(
            "page token does not match this node's shards",
        ));
    }
    Ok(scans)
}

/// Re-encodes the per-shard cursors, or `None` once every shard is exhausted.
///
/// Returning `None` at the end is what lets a client terminate; a token that is
/// always `Some` makes the loop unbounded.
fn encode_shard_cursors(cursors: Vec<ShardCursor>) -> Result<Option<Vec<u8>>, Status> {
    if cursors
        .iter()
        .all(|cursor| cursor.scan == ShardScan::Exhausted)
    {
        return Ok(None);
    }
    serde_json::to_vec(&cursors)
        .map(Some)
        .map_err(|err| Status::internal(format!("failed to serialize next_page_token: {err}")))
}

/// Translate a HubError raised by the channel stores into a gRPC `Status`.
///
/// Channel stores keep caller errors under `bad_request.*` and report state this
/// node stored but can no longer interpret as `invalid_internal_state`, so this
/// boundary can classify by provenance instead of maintaining an allowlist of
/// caller-error codes.
pub(crate) fn channel_store_error_to_status(err: HubError) -> Status {
    if err.code.starts_with("bad_request.") {
        Status::invalid_argument(err.to_string())
    } else {
        Status::internal(format!("Store error: {err:?}"))
    }
}

/// Resolves `(owner_address, channel_key)` index entries into [`ChannelInfo`]s,
/// stamping `fid` onto each. Lapsed records (expiry in the past) are included:
/// release state is not computable from chain events, so callers interpret
/// `expiry` themselves (see GetChannelOwner in rpc.proto). An index entry whose
/// primary `ChannelOwner` disagrees on the owner address is dropped with a
/// `warn!`, since the fold writes both sides in one transaction so a mismatch
/// signals index corruption.
fn channel_infos_for_index_keys(
    block_stores: &BlockStores,
    index_keys: impl IntoIterator<Item = (Vec<u8>, String)>,
    fid: u64,
) -> Result<Vec<ChannelInfo>, Status> {
    let mut channels = Vec::new();

    for (owner_address, channel_key) in index_keys {
        let Some(channel_owner) = block_stores
            .onchain_event_store
            .get_channel_owner(&channel_key, None)
            .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?
        else {
            continue;
        };

        if channel_owner.owner_address != owner_address {
            warn!(
                channel_key,
                indexed_owner_address = hex::encode(&owner_address),
                primary_owner_address = hex::encode(&channel_owner.owner_address),
                "channel list skipped an owner-address index entry that disagrees with its primary record",
            );
            continue;
        }

        channels.push(ChannelInfo {
            channel_key,
            fid,
            owner_address: channel_owner.owner_address,
            expiry: channel_owner.expiry,
        });
    }

    Ok(channels)
}

fn channel_infos_by_owner_address(
    block_stores: &BlockStores,
    owner_address: &[u8],
    fid: u64,
    page_options: &PageOptions,
) -> Result<(Vec<ChannelInfo>, Option<Vec<u8>>), Status> {
    let (channel_keys, next_page_token) =
        get_channel_keys_by_owner_address(&block_stores.db, owner_address, page_options)
            .map_err(onchain_event_storage_error_to_status)?;
    let index_keys = channel_keys
        .into_iter()
        .map(|channel_key| (owner_address.to_vec(), channel_key));
    let channels = channel_infos_for_index_keys(block_stores, index_keys, fid)?;

    Ok((channels, next_page_token))
}

/// Build a unified `Signer` record from an on-chain `OnChainEvent` whose body is a
/// `SignerEventBody`. Off-chain–only fields (scopes, ttl, last_used_at, expires_at,
/// nonce, request_fid) are intentionally left at their proto defaults; the
/// originating event is attached so callers that need raw on-chain payload still
/// get it. `added_at` is the block timestamp (Unix epoch seconds).
fn signer_from_onchain_event(event: &OnChainEvent) -> Signer {
    let (key, key_type) = match &event.body {
        Some(Body::SignerEventBody(body)) => (body.key.clone(), body.key_type),
        _ => (Vec::new(), 0),
    };
    Signer {
        source: SignerSource::Onchain as i32,
        key,
        key_type,
        fid: event.fid,
        added_at: Some(event.block_timestamp),
        last_used_at: None,
        ttl: None,
        expires_at: None,
        scopes: Vec::new(),
        request_fid: None,
        nonce: None,
        onchain_event: Some(event.clone()),
    }
}

/// Build a unified `Signer` record from a stored `GaslessKeyRecord`, joining in
/// `last_used_at` from the sibling store. Returns `None` if the embedded
/// KEY_ADD message is malformed (missing `data.body.key_add_body`) — by
/// construction this can't happen for records that successfully merged, but
/// guarding here keeps the RPC path defensive against future schema changes.
fn signer_from_gasless_record(
    record: &GaslessKeyRecord,
    public_key: &[u8],
    fid: u64,
    last_used_at: Option<u64>,
) -> Option<Signer> {
    let message = record.message.as_ref()?;
    let data = message.data.as_ref()?;
    let key_add = match data.body.as_ref()? {
        message_data::Body::KeyAddBody(body) => body,
        _ => return None,
    };
    let ttl = key_add.ttl;
    // Both `data.timestamp` and the gasless last-used store are Farcaster-time
    // seconds; the unified API normalizes everything to Unix seconds so a single
    // time base flows out across on-chain and off-chain sources.
    let added_at_unix = FarcasterTime::new(data.timestamp as u64).to_unix_seconds();
    let last_used_at_unix = last_used_at.map(|t| FarcasterTime::new(t).to_unix_seconds());
    let expires_at = match (last_used_at_unix, ttl) {
        (Some(used), t) if t > 0 => Some(used + t as u64),
        _ => None,
    };
    Some(Signer {
        source: SignerSource::Offchain as i32,
        key: public_key.to_vec(),
        key_type: key_add.key_type,
        fid,
        added_at: Some(added_at_unix),
        last_used_at: last_used_at_unix,
        ttl: Some(ttl),
        expires_at,
        scopes: key_add.scopes.clone(),
        request_fid: Some(record.request_fid),
        nonce: Some(key_add.nonce),
        onchain_event: None,
    })
}

/// Look up `(fid, signer)` across both signer indexes, on-chain first then off-chain,
/// matching the read order in `active_key::get_active_key`. Returns `None` if the
/// key is not active on either side.
fn resolve_signer(stores: &Stores, fid: u64, public_key: &[u8]) -> Result<Option<Signer>, Status> {
    if let Some(event) = stores
        .onchain_event_store
        .get_active_signer(fid, public_key.to_vec(), None)
        .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?
    {
        return Ok(Some(signer_from_onchain_event(&event)));
    }

    let txn = RocksDbTransactionBatch::new();
    let Some(record) = get_gasless_key_record(&stores.db, &txn, fid, public_key)
        .map_err(signer_store_error_to_status)?
    else {
        return Ok(None);
    };

    let last_used_at = get_last_used_at(&stores.db, &txn, fid, public_key)
        .map_err(signer_store_error_to_status)?
        .map(|t| t as u64);

    Ok(signer_from_gasless_record(
        &record,
        public_key,
        fid,
        last_used_at,
    ))
}

/// Result of merging on-chain + off-chain signer pages for a single FID.
struct UnifiedSignersPage {
    signers: Vec<Signer>,
    next_page_token: Option<Vec<u8>>,
    /// Total active gasless (off-chain) keys for the FID, sourced from the O(1)
    /// per-FID counter. Populated regardless of pagination state so callers
    /// always see the FID-wide total.
    gasless_signer_count: u32,
}

/// Composite cursor for `list_signers_for_fid`. Carries one cursor per
/// underlying store so each side can advance independently, plus an explicit
/// `*_exhausted` flag per side. The flags are necessary because a `None`
/// cursor is ambiguous: it could mean "start from the beginning" *or* "this
/// side has been fully drained." Without the flag, a request that drains
/// gasless before on-chain would re-scan gasless on every subsequent page and
/// duplicate every gasless row in the response. Encoded as JSON for parity
/// with the existing per-shard composite tokens used in
/// `get_reactions_by_target`.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct UnifiedSignerPageToken {
    onchain: Option<Vec<u8>>,
    gasless: Option<Vec<u8>>,
    #[serde(default)]
    onchain_exhausted: bool,
    #[serde(default)]
    gasless_exhausted: bool,
}

/// Drain both signer indexes for `fid` and return the merged page. Pagination
/// preserves per-store cursors via a JSON composite token; ordering between the
/// two sides is "on-chain first, then gasless," matching the lookup priority in
/// `active_key::get_active_key`.
///
/// `page_options.page_size` is treated as a **global** cap on the merged
/// response — on-chain rows are drained first up to the cap, then gasless rows
/// fill any remaining slots. This keeps response sizes predictable for
/// callers and consistent with the rest of the RPC surface.
fn list_signers_for_fid(
    stores: &Stores,
    fid: u64,
    page_options: &PageOptions,
) -> Result<UnifiedSignersPage, Status> {
    let cursor: UnifiedSignerPageToken = match &page_options.page_token {
        None => UnifiedSignerPageToken::default(),
        Some(bytes) => serde_json::from_slice(bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid signers page token: {}", e)))?,
    };

    // None means "no client cap" — defer to the underlying store defaults.
    let global_limit = page_options.page_size;

    let mut signers: Vec<Signer> = Vec::new();
    let mut next_onchain_token: Option<Vec<u8>> = None;
    let mut next_gasless_token: Option<Vec<u8>> = None;
    let mut onchain_exhausted = cursor.onchain_exhausted;
    let mut gasless_exhausted = cursor.gasless_exhausted;

    if !cursor.onchain_exhausted {
        let onchain_options = PageOptions {
            page_size: global_limit,
            page_token: cursor.onchain,
            reverse: page_options.reverse,
        };
        let onchain_page = stores
            .onchain_event_store
            .get_signers(Some(fid), &onchain_options)
            .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;
        signers.extend(
            onchain_page
                .onchain_events
                .iter()
                .map(signer_from_onchain_event),
        );
        if onchain_page.next_page_token.is_none() {
            onchain_exhausted = true;
        } else {
            next_onchain_token = onchain_page.next_page_token;
        }
    }

    let remaining = global_limit.map(|cap| cap.saturating_sub(signers.len()));
    let should_scan_gasless = !cursor.gasless_exhausted && remaining.map_or(true, |r| r > 0);

    let txn = RocksDbTransactionBatch::new();

    if should_scan_gasless {
        let gasless_options = PageOptions {
            page_size: remaining,
            page_token: cursor.gasless,
            reverse: page_options.reverse,
        };
        let gasless_page = list_gasless_keys_by_fid(&stores.db, fid, &gasless_options)
            .map_err(signer_store_error_to_status)?;
        for (public_key, record) in &gasless_page.records {
            let last_used_at = get_last_used_at(&stores.db, &txn, fid, public_key)
                .map_err(signer_store_error_to_status)?
                .map(|t| t as u64);
            if let Some(s) = signer_from_gasless_record(record, public_key, fid, last_used_at) {
                signers.push(s);
            }
        }
        if gasless_page.next_page_token.is_none() {
            gasless_exhausted = true;
        } else {
            next_gasless_token = gasless_page.next_page_token;
        }
    }

    let next_page_token = if onchain_exhausted && gasless_exhausted {
        None
    } else {
        let token = UnifiedSignerPageToken {
            onchain: next_onchain_token,
            gasless: next_gasless_token,
            onchain_exhausted,
            gasless_exhausted,
        };
        Some(serde_json::to_vec(&token).map_err(|e| {
            Status::internal(format!("failed to serialize signers page token: {}", e))
        })?)
    };

    let gasless_signer_count =
        get_gasless_key_count(&stores.db, &txn, fid).map_err(signer_store_error_to_status)?;

    Ok(UnifiedSignersPage {
        signers,
        next_page_token,
        gasless_signer_count,
    })
}

pub struct MyHubService {
    allowed_users: HashMap<String, String>,
    /// Admin credentials (from `admin_rpc_auth`) gating diagnostic endpoints
    /// like the mesh view. Empty ⇒ open (matches admin-server semantics).
    admin_allowed_users: HashMap<String, String>,
    /// Validator `PeerId` index, derived from the configured validator set's
    /// public keys. Used to classify connected peers in the mesh view.
    validator_peer_ids: ValidatorPeerIds,
    block_stores: BlockStores,
    shard_stores: HashMap<u32, Stores>,
    shard_senders: HashMap<u32, Senders>,
    num_shards: u32,
    message_router: Box<dyn routing::MessageRouter>,
    statsd_client: StatsdClientWrapper,
    chain_clients: ChainClients,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    network: proto::FarcasterNetwork,
    version: String,
    peer_id: String,
    id_registry_cache: Cache<Vec<u8>, OnChainEvent>,
    /// Short-TTL cache for the admin mesh view / topology responses. Consulted
    /// only *after* the admin auth check in each handler.
    mesh_cache: MeshCache,
    // Synchronous lookup against the fname registry. Used to recover from the
    // race condition where a client submits a UserDataAdd for a username before
    // the background fname connector has polled the corresponding transfer. None
    // disables on-demand recovery (e.g. when fnames are configured off).
    fname_lookup: Option<Arc<dyn FnameTransferLookup>>,
}

impl MyHubService {
    pub fn new(
        rpc_auth: String,
        admin_rpc_auth: String,
        validator_hex_keys: Vec<String>,
        block_stores: BlockStores,
        shard_stores: HashMap<u32, Stores>,
        shard_senders: HashMap<u32, Senders>,
        statsd_client: StatsdClientWrapper,
        num_shards: u32,
        network: proto::FarcasterNetwork,
        message_router: Box<dyn routing::MessageRouter>,
        mempool_tx: mpsc::Sender<MempoolRequest>,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        chain_clients: ChainClients,
        version: String,
        peer_id: String,
        fname_lookup: Option<Arc<dyn FnameTransferLookup>>,
        mesh_config: crate::network::mesh::config::Config,
    ) -> Self {
        let parse_auth = |auth_str: &str| {
            let mut users = HashMap::new();
            for auth in auth_str.split(",") {
                let parts: Vec<&str> = auth.split(":").collect();
                if parts.len() == 2 {
                    users.insert(parts[0].to_string(), parts[1].to_string());
                }
            }
            users
        };
        let allowed_users = parse_auth(&rpc_auth);
        let admin_allowed_users = parse_auth(&admin_rpc_auth);

        if allowed_users.is_empty() {
            info!("RPC server auth disabled");
        } else {
            info!("RPC server auth enabled with {} users", allowed_users.len());
        }
        if admin_allowed_users.is_empty() {
            info!("Admin/diagnostic RPC auth disabled (mesh view is open)");
        } else {
            info!(
                "Admin/diagnostic RPC auth enabled with {} users",
                admin_allowed_users.len()
            );
        }

        let validator_peer_ids = build_validator_peer_ids(validator_hex_keys.iter());
        info!(
            "Mesh view: indexed {} validator peer ids",
            validator_peer_ids.len()
        );

        let id_registry_cache = CacheBuilder::new(2_000_000)
            .time_to_idle(Duration::from_secs(60 * 60))
            .eviction_policy(EvictionPolicy::lru())
            .build();

        let mesh_cache = MeshCache::new(Duration::from_secs(mesh_config.cache_ttl_secs));
        if mesh_config.cache_ttl_secs == 0 {
            info!("Mesh view/topology cache disabled (cache_ttl_secs = 0)");
        } else {
            info!(
                "Mesh view/topology cache enabled with {}s TTL",
                mesh_config.cache_ttl_secs
            );
        }

        let service = Self {
            allowed_users,
            admin_allowed_users,
            validator_peer_ids,
            network,
            block_stores,
            shard_senders,
            shard_stores,
            statsd_client,
            message_router,
            num_shards,
            chain_clients,
            mempool_tx,
            gossip_tx,
            version,
            peer_id,
            id_registry_cache,
            mesh_cache,
            fname_lookup,
        };
        service
    }

    #[cfg(test)]
    pub fn set_fname_lookup_for_test(&mut self, lookup: Arc<dyn FnameTransferLookup>) {
        self.fname_lookup = Some(lookup);
    }

    async fn submit_message_internal(
        &self,
        message: proto::Message,
    ) -> Result<proto::Message, HubError> {
        let fid = message.fid();
        if fid == 0 {
            return Err(HubError::invalid_parameter("fid cannot be 0"));
        }

        let version = EngineVersion::current(self.network);
        let dst_shard =
            routing::route_message(&self.message_router, &message, self.num_shards, version);

        match self
            .simulate_message_for_shard_typed(&message, dst_shard)
            .await
        {
            Ok(()) => {}
            Err(engine::MessageValidationError::MissingFname) => {
                if let Some(fname) = username_for_fname_recovery(&message) {
                    self.recover_missing_fname(fid, &fname, &message, dst_shard)
                        .await?;
                } else {
                    return Err(HubError::validation_failure(
                        &engine::MessageValidationError::MissingFname.to_string(),
                    ));
                }
            }
            Err(err) => return Err(simulate_error_to_hub_error(err)),
        }

        // Process the submitted message
        self.submit_message_to_mempool(message).await
    }

    /// On-demand recovery for the fname-not-yet-propagated race (issue #456): query
    /// the fname registry directly, push any matching transfer through the mempool,
    /// then poll for the proof to land in our local store before re-running
    /// validation. If the registry has no transfer for `fid`, or the transfer fails
    /// to land within the budget, we surface the original `MissingFname` error.
    async fn recover_missing_fname(
        &self,
        fid: u64,
        fname: &str,
        message: &proto::Message,
        dst_shard: u32,
    ) -> Result<(), HubError> {
        let lookup = match &self.fname_lookup {
            Some(lookup) => lookup,
            None => {
                return Err(HubError::validation_failure(
                    &engine::MessageValidationError::MissingFname.to_string(),
                ));
            }
        };

        self.statsd_client.count(
            "rpc.submit_message.missing_fname_recovery_attempted",
            1,
            vec![],
        );

        // Bound the registry call: if it stalls or fails, surface the original
        // `MissingFname` error to the client rather than a new "registry
        // unavailable" failure mode. Recovery is best-effort — a broken registry
        // shouldn't change the error contract for clients that already handle
        // `MissingFname` retries.
        let transfers =
            match timeout(MISSING_FNAME_LOOKUP_TIMEOUT, lookup.lookup_fname(fname)).await {
                Ok(Ok(transfers)) => transfers,
                Ok(Err(err)) => {
                    error!(
                        fid,
                        fname,
                        err = err.to_string(),
                        "fname registry lookup failed during missing-fname recovery"
                    );
                    self.statsd_client.count(
                        "rpc.submit_message.missing_fname_recovery_lookup_error",
                        1,
                        vec![],
                    );
                    return Err(HubError::validation_failure(
                        &engine::MessageValidationError::MissingFname.to_string(),
                    ));
                }
                Err(_) => {
                    error!(
                        fid,
                        fname, "fname registry lookup timed out during missing-fname recovery"
                    );
                    self.statsd_client.count(
                        "rpc.submit_message.missing_fname_recovery_lookup_timeout",
                        1,
                        vec![],
                    );
                    return Err(HubError::validation_failure(
                        &engine::MessageValidationError::MissingFname.to_string(),
                    ));
                }
            };

        // Only forward transfers whose latest target matches the requesting fid —
        // otherwise the proof we land won't satisfy the pending UserDataAdd anyway.
        let mut submitted_any = false;
        for transfer in transfers {
            let target_fid = transfer.proof.as_ref().map(|p| p.fid).unwrap_or(0);
            if target_fid != fid {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            if let Err(err) = self.mempool_tx.try_send(MempoolRequest::AddMessage(
                MempoolMessage::FnameTransfer(transfer),
                MempoolSource::RPC,
                Some(tx),
            )) {
                error!(
                    fid,
                    fname,
                    err = err.to_string(),
                    "failed to enqueue fname transfer for missing-fname recovery"
                );
                continue;
            }
            // Mempool ack means "queued for inclusion", not "applied". The
            // simulate-poll loop below is what blocks on the proof actually landing.
            // A duplicate-message ack is benign — the transfer is already in flight.
            match timeout(MEMPOOL_ADD_REQUEST_TIMEOUT, rx).await {
                Ok(Ok(Ok(()))) | Ok(Ok(Err(_))) => submitted_any = true,
                Ok(Err(_)) | Err(_) => {
                    // Channel closed or timed out — keep going; the proof might
                    // already be in flight from a concurrent path.
                    submitted_any = true;
                }
            }
        }

        if !submitted_any {
            self.statsd_client.count(
                "rpc.submit_message.missing_fname_recovery_no_transfer",
                1,
                vec![],
            );
            return Err(HubError::validation_failure(
                &engine::MessageValidationError::MissingFname.to_string(),
            ));
        }

        // Poll for the fname transfer to reach the persisted store. Re-run the
        // engine simulation on each tick — this is the same check the real
        // validation will perform, so it implicitly covers the proof-store lookup
        // and any other state-dependent prerequisites. We simulate once up-front
        // before sleeping so we don't add a needless poll-interval of latency
        // when the proof has already landed (e.g. via a concurrent recovery or
        // the background fetcher catching up while we awaited the lookup).
        let deadline = std::time::Instant::now() + MISSING_FNAME_RECOVERY_BUDGET;
        loop {
            match self
                .simulate_message_for_shard_typed(message, dst_shard)
                .await
            {
                Ok(()) => {
                    self.statsd_client.count(
                        "rpc.submit_message.missing_fname_recovery_success",
                        1,
                        vec![],
                    );
                    return Ok(());
                }
                Err(engine::MessageValidationError::MissingFname) => {
                    if std::time::Instant::now() >= deadline {
                        self.statsd_client.count(
                            "rpc.submit_message.missing_fname_recovery_timeout",
                            1,
                            vec![],
                        );
                        return Err(HubError::validation_failure(
                            &engine::MessageValidationError::MissingFname.to_string(),
                        ));
                    }
                    sleep(MISSING_FNAME_POLL_INTERVAL).await;
                }
                Err(err) => return Err(simulate_error_to_hub_error(err)),
            }
        }
    }

    async fn submit_message_to_mempool(
        &self,
        message: proto::Message,
    ) -> Result<proto::Message, HubError> {
        let fid = message.fid();

        // We're doing the ens and address validations here for now because we don't want L1 interactions to be on the consensus critical path.
        // Eventually this will move to the fname server.
        if let Some(message_data) = &message.data {
            match &message_data.body {
                Some(proto::message_data::Body::UserDataBody(user_data)) => {
                    if user_data.r#type() == proto::UserDataType::Username {
                        if user_data.value.ends_with(".eth") {
                            self.validate_ens_username(fid, user_data.value.to_string())
                                .await?;
                        }
                    };
                }
                Some(proto::message_data::Body::UsernameProofBody(proof)) => {
                    self.validate_ens_username_proof(fid, &proof).await?;
                }
                Some(proto::message_data::Body::VerificationAddAddressBody(body)) => {
                    if body.verification_type == 1 {
                        let claim_result =
                            validations::verification::make_verification_address_claim(
                                message_data.fid,
                                &body.address,
                                self.network,
                                &body.block_hash,
                                proto::Protocol::Ethereum,
                            );
                        match claim_result {
                            Ok(claim) => {
                                self.validate_contract_signature(claim, body).await?;
                            }
                            Err(err) => {
                                return Err(HubError::validation_failure(
                                    format!(
                                        "could not create verification address claim: {}",
                                        err.to_string()
                                    )
                                    .as_str(),
                                ))
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        let (tx, rx) = oneshot::channel();

        match self.mempool_tx.try_send(MempoolRequest::AddMessage(
            MempoolMessage::UserMessage(message.clone()),
            MempoolSource::RPC,
            Some(tx),
        )) {
            Ok(_) => {
                self.statsd_client
                    .count("rpc.submit_message.success", 1, vec![]);
                debug!("successfully submitted message");
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.statsd_client
                    .count("rpc.submit_message.channel_full", 1, vec![]);
                return Err(HubError::unavailable("mempool channel is full"));
            }
            Err(e) => {
                error!(
                    "Error sending message to mempool channel: {:?}",
                    e.to_string()
                );
                return Err(HubError::unavailable("mempool channel send error"));
            }
        }

        let result = match timeout(MEMPOOL_ADD_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                self.statsd_client
                    .count("rpc.mempool_submit_error", 1, vec![]);
                error!(
                    "Error receiving message from mempool channel: {:?}",
                    err.to_string()
                );
                return Err(HubError::unavailable("Error adding to mempool"));
            }
            Err(_) => {
                self.statsd_client
                    .count("rpc.mempool_submit_timeout", 1, vec![]);
                error!("Timeout receiving message from mempool channel",);
                return Err(HubError::unavailable("Error adding to mempool"));
            }
        };

        return match result {
            Ok(_) => Ok(message),
            Err(hub_error) => Err(hub_error),
        };
    }

    /// Every data shard, ascending by shard id, or an error if this node does not
    /// host all of them.
    ///
    /// Fan-out reads must not answer from a subset. An empty follower list from a
    /// node hosting one shard of four is indistinguishable from a channel nobody
    /// follows, so an incomplete node has to refuse rather than under-report.
    ///
    /// Sorted, and carrying the shard id, because `shard_stores` is a `HashMap`:
    /// `get_casts_by_parent` zips per-shard page tokens against its unspecified
    /// iteration order, which means a token slot is not stably bound to any shard.
    /// Do not reproduce that here.
    fn all_shard_stores(&self) -> Result<Vec<(u32, &Stores)>, Status> {
        // Data shards are 1..=num_shards; `route_fid` returns `(hash % n) + 1`.
        (1..=self.num_shards)
            .map(|shard_id| {
                self.shard_stores
                    .get(&shard_id)
                    .map(|stores| (shard_id, stores))
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "node hosts {} of {} shards; cross-shard channel follow reads are unavailable here",
                            self.shard_stores.len(),
                            self.num_shards
                        ))
                    })
            })
            .collect()
    }

    fn get_stores_for_shard(&self, shard_id: u32) -> Result<&Stores, Status> {
        match self.shard_stores.get(&shard_id) {
            Some(store) => Ok(store),
            None => Err(Status::invalid_argument(
                "no shard store for fid".to_string(),
            )),
        }
    }

    fn get_stores_for(&self, fid: u64) -> Result<&Stores, Status> {
        let shard_id = self.message_router.route_fid(fid, self.num_shards);
        self.get_stores_for_shard(shard_id)
    }

    /// Validates the width and registration of a caller-supplied channel id, and
    /// returns the id as a fixed-width array.
    ///
    /// Returning the validated value rather than `()` is deliberate: the store keys
    /// are built by concatenation and do not re-check width (see CHANNEL_ID_LENGTH),
    /// so passing the raw request slice onward would leave width safety resting on
    /// every handler remembering to call this first. Threading the array makes
    /// "validated" and "used" the same value.
    fn require_registered_channel(
        &self,
        channel_id: &[u8],
    ) -> Result<[u8; CHANNEL_ID_LENGTH], Status> {
        let channel_id = require_channel_id_width(channel_id)?;
        let channel_key = self
            .block_stores
            .onchain_event_store
            .get_channel_key_by_label(&channel_id, None)
            .map_err(|err| Status::internal(format!("Store error: {err:?}")))?
            .ok_or_else(|| Status::not_found("channel not registered"))?;
        self.block_stores
            .onchain_event_store
            .get_channel_owner(&channel_key, None)
            .map_err(|err| Status::internal(format!("Store error: {err:?}")))?
            .ok_or_else(|| Status::not_found("channel not registered"))?;
        Ok(channel_id)
    }

    /// Replays `message` against a read-only engine for `shard_id` and returns the
    /// typed [`engine::MessageValidationError`] so callers can branch on specific
    /// variants — for example, the `MissingFname` recovery path.
    async fn simulate_message_for_shard_typed(
        &self,
        message: &proto::Message,
        shard_id: u32,
    ) -> Result<(), engine::MessageValidationError> {
        if shard_id == 0 {
            // Handle shard 0 (block engine) specially
            let mut block_engine = block_engine::BlockEngine::new(
                self.block_stores.trie.clone(),
                self.statsd_client.clone(),
                self.block_stores.db.clone(),
                100,
                None,
                self.network,
            );

            block_engine.simulate_message(message).map_err(|e| match e {
                block_engine::MessageValidationError::HubError(hub_error) => {
                    engine::MessageValidationError::StoreError(hub_error)
                }
                other => engine::MessageValidationError::StoreError(HubError::validation_failure(
                    &other.to_string(),
                )),
            })
        } else {
            let stores = match self.shard_stores.get(&shard_id) {
                Some(store) => store,
                None => {
                    return Err(engine::MessageValidationError::StoreError(
                        HubError::invalid_parameter("shard not found for fid"),
                    ));
                }
            };

            // TODO: This is a hack to get around the fact that self cannot be made mutable
            let mut readonly_engine = ShardEngine::new(
                stores.db.clone(),
                self.network,
                stores.trie.clone(),
                1,
                stores.store_limits.clone(),
                self.statsd_client.clone(),
                100,
                None,
                None,
                None,
            )
            .await
            .map_err(engine::MessageValidationError::StoreError)?;

            readonly_engine.simulate_message(message)
        }
    }

    async fn simulate_bulk_messages_for_shard(
        &self,
        messages: &[proto::Message],
        shard_id: u32,
    ) -> Vec<Result<(), HubError>> {
        if shard_id == 0 {
            messages
                .iter()
                .map(|_| {
                    Err(HubError::validation_failure(
                        "submit bulk messages not supported for shard 0",
                    ))
                })
                .collect()
        } else {
            let stores = match self.shard_stores.get(&shard_id) {
                Some(store) => store,
                None => {
                    let error = HubError::invalid_parameter("shard not found for fid");
                    return messages.iter().map(|_| Err(error.clone())).collect();
                }
            };

            // Create shard engine for bulk simulation
            let mut readonly_engine = match ShardEngine::new(
                stores.db.clone(),
                self.network,
                stores.trie.clone(),
                shard_id,
                stores.store_limits.clone(),
                self.statsd_client.clone(),
                100,
                None,
                None,
                None,
            )
            .await
            {
                Ok(engine) => engine,
                Err(err) => {
                    let hub_error = HubError::invalid_internal_state(&err.to_string());
                    return messages.iter().map(|_| Err(hub_error.clone())).collect();
                }
            };

            readonly_engine
                .simulate_bulk_messages(messages)
                .into_iter()
                .map(|result| {
                    result.map_err(|err| match err {
                        engine::MessageValidationError::StoreError(hub_error) => {
                            // Forward hub errors as is, otherwise we end up wrapping them
                            hub_error
                        }
                        _ => HubError::validation_failure(&err.to_string()),
                    })
                })
                .collect()
        }
    }

    pub async fn validate_contract_signature(
        &self,
        claim: VerificationAddressClaim,
        body: &VerificationAddAddressBody,
    ) -> Result<(), HubError> {
        let chain = Chain::from_chain_id(body.chain_id)
            .ok_or(HubError::validation_failure("invalid chain id"))?;
        let client = &self.chain_clients.for_chain(chain)?;
        client
            .verify_contract_signature(claim, body)
            .await
            .or_else(|e| {
                Err(HubError::validation_failure(
                    format!("could not verify contract signature: {}", e.to_string()).as_str(),
                ))
            })
    }

    pub async fn validate_ens_username_proof(
        &self,
        fid: u64,
        proof: &UserNameProof,
    ) -> Result<(), HubError> {
        let resolved_ens_address = self.resolve_ens_address(proof).await?;
        if resolved_ens_address != proof.owner {
            return Err(HubError::validation_failure(
                "invalid ens name, resolved address doesn't match proof owner address",
            ));
        }

        let stores = self
            .get_stores_for(fid)
            .map_err(|_| HubError::internal_db_error("stores not found for fid"))?;

        let id_register = stores
            .onchain_event_store
            .get_id_register_event_by_fid(fid, None)
            .map_err(|_| HubError::internal_db_error("Could not fetch id registration"))?;

        match id_register {
            None => return Err(HubError::validation_failure("missing fid registration")),
            Some(id_register) => {
                match id_register.body {
                    Some(Body::IdRegisterEventBody(id_register)) => {
                        // Check verified addresses if the resolved address doesn't match the custody address
                        if id_register.to != resolved_ens_address {
                            let verification = VerificationStore::get_verification_add(
                                &stores.verification_store,
                                fid,
                                &resolved_ens_address,
                                None,
                            )?;

                            match verification {
                                None => Err(HubError::validation_failure("invalid ens proof, no matching custody address or verified addresses")),
                                Some(_) => Ok(()),
                            }
                        } else {
                            Ok(())
                        }
                    }
                    _ => return Err(HubError::validation_failure("missing fid registration")),
                }
            }
        }
    }

    async fn resolve_ens_address(&self, proof: &UserNameProof) -> Result<Vec<u8>, HubError> {
        let name = std::str::from_utf8(&proof.name)
            .map_err(|_| HubError::validation_failure("ENS name is not utf8"))?;

        let chain_api = match UserNameType::try_from(proof.r#type) {
            Ok(UserNameType::UsernameTypeEnsL1) => {
                if !name.ends_with(".eth") {
                    return Err(HubError::validation_failure(
                        "ENS name does not end with .eth",
                    ));
                }
                self.chain_clients.for_chain(Chain::EthMainnet)?
            }
            Ok(UserNameType::UsernameTypeBasename) => {
                if !name.ends_with(".base.eth") {
                    return Err(HubError::validation_failure(
                        "Basename does not end with base.eth",
                    ));
                }
                self.chain_clients.for_chain(Chain::BaseMainnet)?
            }
            _ => {
                return Err(HubError::validation_failure(
                    format!(
                        "unsupported username type: {} for name: {}",
                        proof.r#type, name,
                    )
                    .as_str(),
                ))
            }
        };

        let resolved_ens_address = chain_api
            .resolve_ens_name(name.to_string())
            .await
            .map_err(|err| {
                HubError::validation_failure(
                    format!("ENS resolution error: {}", err.to_string()).as_str(),
                )
            })?
            .to_vec();

        Ok(resolved_ens_address)
    }

    async fn validate_ens_username(&self, fid: u64, name: String) -> Result<(), HubError> {
        let stores = self
            .get_stores_for(fid)
            .map_err(|_| HubError::invalid_parameter("stores not found for fid"))?;
        let proof_message = UsernameProofStore::get_username_proof(
            &stores.username_proof_store,
            &name.as_bytes().to_vec(),
            &mut RocksDbTransactionBatch::new(),
        )?;
        match proof_message {
            Some(message) => match message.data {
                None => Err(HubError::validation_failure("username proof missing data")),
                Some(message_data) => match message_data.body {
                    Some(body) => match body {
                        proto::message_data::Body::UsernameProofBody(proof) => {
                            self.validate_ens_username_proof(fid, &proof).await
                        }
                        _ => Err(HubError::validation_failure(
                            "username proof has wrong type",
                        )),
                    },
                    None => Err(HubError::validation_failure("username proof missing body")),
                },
            },
            None => Err(HubError::validation_failure("username proof missing proof")),
        }
    }

    fn rewrite_hub_event(
        mut hub_event: HubEvent,
        shard_index: u32,
        timestamp: Option<u64>,
    ) -> HubEvent {
        let (block_number, _) = HubEventIdGenerator::extract_height_and_seq(hub_event.id);
        hub_event.block_number = block_number;
        hub_event.shard_index = shard_index;
        if let Some(timestamp) = timestamp {
            hub_event.timestamp = timestamp;
        }

        match &mut hub_event.body {
            Some(body) => {
                match body {
                    proto::hub_event::Body::MergeMessageBody(merge_message_body) => {
                        match &merge_message_body.message {
                            None => {}
                            Some(message) => {
                                if message.msg_type() == MessageType::LinkCompactState {
                                    // In the case of merging compact state, we omit the deleted messages as this would
                                    // result in an unbounded message size:
                                    merge_message_body.deleted_messages = vec![]
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            None => {}
        };
        hub_event
    }

    fn get_events_from_store(
        stores: &Stores,
        start_id: u64,
        stop_id: Option<u64>,
        page_options: Option<PageOptions>,
        last_chunk: Option<ShardChunk>,
    ) -> (EventsPage, Option<ShardChunk>) {
        let mut events = vec![];
        let old_events = stores.get_events(start_id, stop_id, page_options).unwrap();
        let mut last_chunk = last_chunk;

        for event in old_events.events {
            let (block_number, _) = HubEventIdGenerator::extract_height_and_seq(event.id);
            if last_chunk
                .as_ref()
                .map(|chunk| {
                    return block_number
                        != chunk.header.as_ref().unwrap().height.unwrap().block_number;
                })
                .unwrap_or(true)
            {
                let chunk = stores.shard_store.get_chunk_by_height(
                    Height {
                        shard_index: stores.shard_id,
                        block_number,
                    }
                    .as_u64(),
                );
                last_chunk = chunk.unwrap_or(None);
            }
            let event = Self::rewrite_hub_event(
                event,
                stores.shard_id,
                last_chunk
                    .as_ref()
                    .map(|chunk| chunk.header.as_ref().unwrap().timestamp),
            );
            events.push(event)
        }
        (
            EventsPage {
                events,
                next_page_token: old_events.next_page_token,
            },
            last_chunk,
        )
    }
}

#[tonic::async_trait]
impl HubService for MyHubService {
    async fn submit_message(
        &self,
        request: Request<proto::Message>,
    ) -> Result<Response<proto::Message>, Status> {
        self.statsd_client
            .count("rpc.submit_message_in_flight", 1, vec![]);
        let start_time = std::time::Instant::now();

        authenticate_request(&request, &self.allowed_users).map_err(|err| {
            self.statsd_client
                .count("rpc.submit_message_in_flight", -1, vec![]);
            err
        })?;

        let hash = request.get_ref().hash.encode_hex::<String>();
        debug!(hash, "Received call to [submit_message] RPC");

        let mut message = request.into_inner();
        message_bytes_decode(&mut message);
        let fid = message.fid();
        let msg_type = message.msg_type().into_i32();
        let result = self.submit_message_internal(message).await;

        self.statsd_client.time(
            "rpc.submit_message.duration",
            start_time.elapsed().as_millis() as u64,
        );

        match result {
            Ok(message) => {
                self.statsd_client
                    .count("rpc.submit_message.success", 1, vec![]);
                self.statsd_client
                    .count("rpc.submit_message_in_flight", -1, vec![]);
                Ok(Response::new(message))
            }
            Err(err) => {
                self.statsd_client
                    .count("rpc.submit_message.failure", 1, vec![]);
                info!(
                    hash = hash,
                    fid = fid,
                    errCode = err.code,
                    msgType = msg_type,
                    "submit_message failed: {}",
                    err
                );
                let err_code = err.code.as_str();
                let mut status = if err_code.starts_with("bad_request") {
                    Status::invalid_argument(err.to_string())
                } else if err_code == "not_found" {
                    Status::not_found(err.to_string())
                } else if err_code.starts_with("db") || err_code.starts_with("internal") {
                    Status::internal(err.to_string())
                } else if err_code.starts_with("unavailable") {
                    Status::unavailable(err.to_string())
                } else {
                    Status::unknown(err.to_string())
                };
                if let Ok(err_str) = AsciiMetadataValue::from_str(&err_code) {
                    status.metadata_mut().insert("x-err-code", err_str);
                }
                self.statsd_client
                    .count("rpc.submit_message_in_flight", -1, vec![]);
                Err(status)
            }
        }
    }

    // Submit multiple messages in a single RPC call
    async fn submit_bulk_messages(
        &self,
        request: Request<proto::SubmitBulkMessagesRequest>,
    ) -> Result<Response<proto::SubmitBulkMessagesResponse>, Status> {
        let version = EngineVersion::current(self.network);
        if !version.is_enabled(ProtocolFeature::DependentMessagesInBulkSubmit) {
            return Err(Status::invalid_argument(
                "Dependent messages are not supported in this version",
            ));
        }

        authenticate_request(&request, &self.allowed_users)?;

        let mut messages = request.into_inner().messages;
        let num_messages = messages.len();
        debug!(
            "Received call to [submit_bulk_messages] RPC with {} messages",
            num_messages
        );

        // Helper to create error responses
        fn create_error_response(hash: Vec<u8>, err: HubError) -> proto::BulkMessageResponse {
            proto::BulkMessageResponse {
                response: Some(proto::bulk_message_response::Response::MessageError(
                    proto::MessageError {
                        hash,
                        err_code: err.code,
                        message: err.message,
                    },
                )),
            }
        }

        // Decode all message data_bytes fields first
        for msg in &mut messages {
            message_bytes_decode(msg);
        }

        // 1. Group messages by their destination shard
        let mut messages_by_shard: HashMap<u32, Vec<proto::Message>> = HashMap::new();
        for msg in messages {
            let shard_id =
                routing::route_message(&self.message_router, &msg, self.num_shards, version);
            messages_by_shard.entry(shard_id).or_default().push(msg);
        }

        let mut results = Vec::with_capacity(num_messages);

        // 2. Process each shard's batch transactionally for validation
        for (shard_id, batch) in messages_by_shard {
            self.statsd_client
                .count("rpc.submit_message_in_flight", batch.len() as i64, vec![]);

            // 3. Simulate the entire batch for the shard using our helper
            let sim_results = self
                .simulate_bulk_messages_for_shard(&batch, shard_id)
                .await;

            // 4. Process simulation results
            for (sim_result, msg) in sim_results.into_iter().zip(batch.into_iter()) {
                match sim_result {
                    Ok(()) => {
                        // 4a. If simulation succeeds, submit the message to the mempool
                        let message_hash_for_error = msg.hash.clone();
                        let result = self.submit_message_to_mempool(msg).await;
                        results.push(match result {
                            Ok(message) => {
                                self.statsd_client
                                    .count("rpc.submit_message.success", 1, vec![]);
                                self.statsd_client.count(
                                    "rpc.submit_message_in_flight",
                                    -1,
                                    vec![],
                                );
                                proto::BulkMessageResponse {
                                    response: Some(
                                        proto::bulk_message_response::Response::Message(message),
                                    ),
                                }
                            }
                            Err(err) => {
                                self.statsd_client
                                    .count("rpc.submit_message.failure", 1, vec![]);
                                self.statsd_client.count(
                                    "rpc.submit_message_in_flight",
                                    -1,
                                    vec![],
                                );
                                create_error_response(message_hash_for_error, err)
                            }
                        });
                    }
                    Err(hub_error) => {
                        // 4b. If simulation fails, create an error response for the message
                        results.push(create_error_response(msg.hash, hub_error));
                    }
                }
            }
        }

        Ok(Response::new(proto::SubmitBulkMessagesResponse {
            messages: results,
        }))
    }

    type GetBlocksStream = ReceiverStream<Result<Block, Status>>;

    async fn get_blocks(
        &self,
        request: Request<BlocksRequest>,
    ) -> Result<Response<Self::GetBlocksStream>, Status> {
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;
        // TODO(aditi): Rethink the channel size
        let (server_tx, client_rx) = mpsc::channel::<Result<Block, Status>>(100);

        info!( {start_block_number, stop_block_number}, "Received call to [get_blocks] RPC");

        let block_store = self.block_stores.block_store.clone();

        tokio::spawn(async move {
            let mut next_page_token = None;
            loop {
                match block_store.get_blocks(
                    start_block_number,
                    stop_block_number,
                    &PageOptions {
                        page_size: Some(100),
                        page_token: next_page_token,
                        reverse: false,
                    },
                ) {
                    Err(err) => {
                        _ = server_tx.send(Err(Status::from_error(Box::new(err)))).await;
                        break;
                    }
                    Ok(block_page) => {
                        for block in block_page.blocks {
                            if let Err(_) = server_tx.send(Ok(block)).await {
                                break;
                            }
                        }

                        if block_page.next_page_token.is_none() {
                            break;
                        } else {
                            next_page_token = block_page.next_page_token;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }

    async fn get_shard_chunks(
        &self,
        request: Request<ShardChunksRequest>,
    ) -> Result<Response<ShardChunksResponse>, Status> {
        // TODO(aditi): Write unit tests for these functions.
        let shard_index = request.get_ref().shard_id;
        let start_block_number = request.get_ref().start_block_number;
        let stop_block_number = request.get_ref().stop_block_number;

        info!( {shard_index, start_block_number, stop_block_number},
            "Received call to [get_shard_chunks] RPC");

        let stores = self.shard_stores.get(&shard_index);
        match stores {
            None => Err(Status::from_error(Box::new(
                HubError::invalid_internal_state("Missing shard store"),
            ))),
            Some(stores) => {
                match stores
                    .shard_store
                    .get_shard_chunks(start_block_number, stop_block_number)
                {
                    Err(err) => Err(Status::from_error(Box::new(err))),
                    Ok(shard_chunks) => {
                        let response = Response::new(ShardChunksResponse { shard_chunks });
                        Ok(response)
                    }
                }
            }
        }
    }

    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        let mut total_fid_registrations = 0;
        let mut total_approx_size = 0;
        let mut total_num_messages = 0;
        let mut shard_infos = Vec::new();

        let (size_req, size_res) = oneshot::channel();
        let _ = self
            .mempool_tx
            .send(MempoolRequest::GetSize(size_req))
            .await
            .map_err(|err| {
                error!(
                    { err = err.to_string() },
                    "[get_info] error sending mempool size request"
                );
            });

        let current_time = get_farcaster_time().unwrap_or(0);
        let block_info = proto::ShardInfo {
            shard_id: 0,
            max_height: self
                .block_stores
                .block_store
                .max_block_number()
                .unwrap_or(0),
            num_messages: self
                .block_stores
                .trie
                .get_count(
                    &self.block_stores.db,
                    &mut RocksDbTransactionBatch::new(),
                    &[],
                )
                .map_err(|err| Status::internal(err.to_string()))?,
            num_onchain_events: 0,
            // TODO(aditi): [num_onchain_events] is making the endpoint really slow, enable once there's a faster implementation
            // num_onchain_events: self
            //     .block_stores
            //     .db
            //     .count_keys_at_prefix(vec![
            //         RootPrefix::OnChainEvent as u8,
            //         OnChainEventPostfix::OnChainEvents as u8,
            //     ])
            //     .map_err(|err| Status::from_error(Box::new(err)))?
            //     as u64,
            num_fid_registrations: 0,
            approx_size: self.block_stores.block_store.db.approximate_size(),
            block_delay: current_time
                - self
                    .block_stores
                    .block_store
                    .max_block_timestamp()
                    .unwrap_or(0),
            mempool_size: 0,
        };
        shard_infos.push(block_info);

        let mempool_size = match timeout(DEFAULT_REQUEST_TIMEOUT, size_res).await {
            Ok(Ok(size)) => size,
            Ok(Err(err)) => {
                error!(
                    { err = err.to_string() },
                    "[get_info] error receiving mempool size response"
                );
                HashMap::new()
            }
            Err(_) => {
                error!("[get_info] timeout receiving mempool size response");
                HashMap::new()
            }
        };

        for (shard_index, shard_store) in self.shard_stores.iter() {
            let shard_approx_size = shard_store.db.approximate_size();
            let shard_num_messages = shard_store
                .trie
                .get_count(&shard_store.db, &mut RocksDbTransactionBatch::new(), &[])
                .map_err(|err| Status::internal(err.to_string()))?;
            let shard_fid_registrations = shard_store
                .db
                .count_keys_at_prefix(vec![
                    RootPrefix::OnChainEvent as u8,
                    OnChainEventPostfix::IdRegisterByFid as u8,
                ])
                .map_err(|err| Status::from_error(Box::new(err)))?
                as u64;

            let max_block_time = shard_store.shard_store.max_block_timestamp().unwrap_or(0);

            let info = proto::ShardInfo {
                shard_id: *shard_index,
                max_height: shard_store.shard_store.max_block_number().unwrap_or(0),
                num_messages: shard_num_messages,
                num_onchain_events: 0, // TODO(aditi): Populating this is making the endpoint slow, enable once there's a faster implementation
                num_fid_registrations: shard_fid_registrations,
                approx_size: shard_approx_size,
                block_delay: current_time.saturating_sub(max_block_time),
                // If there is no value in the map, it likely means we could not communicate with the mempool
                // Returning 0 would mean the clients would think the mempool is empty
                // So, return a high value
                mempool_size: *mempool_size.get(shard_index).unwrap_or(&(u32::MAX as u64)),
            };
            shard_infos.push(info);
            total_num_messages += shard_num_messages;
            total_fid_registrations += shard_fid_registrations;
            total_approx_size += shard_approx_size;
        }

        let current_farcaster_time = FarcasterTime::new(current_time);
        let next_engine_version_timestamp =
            EngineVersion::next_version_timestamp_for(&current_farcaster_time, self.network)
                .unwrap_or(0);

        Ok(Response::new(GetInfoResponse {
            db_stats: Some(DbStats {
                num_fid_registrations: total_fid_registrations,
                num_messages: total_num_messages,
                approx_size: total_approx_size,
            }),
            shard_infos,
            num_shards: self.num_shards,
            version: self.version.clone(),
            peer_id: self.peer_id.clone(),
            next_engine_version_timestamp,
        }))
    }

    async fn get_fids(
        &self,
        request: Request<FidsRequest>,
    ) -> Result<Response<proto::FidsResponse>, Status> {
        let inner_request = request.into_inner();

        let stores = self.get_stores_for_shard(inner_request.shard_id)?;

        let page_options = PageOptions {
            page_size: inner_request.page_size.map(|s| s as usize),
            page_token: inner_request.page_token,
            reverse: inner_request.reverse.unwrap_or(false),
        };

        let (fids, next_page_token) = stores
            .onchain_event_store
            .get_fids(&page_options)
            .unwrap_or((vec![], None));

        Ok(Response::new(FidsResponse {
            fids,
            next_page_token,
        }))
    }

    type SubscribeStream = ReceiverStream<Result<HubEvent, Status>>;
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        info!(
            "Received call to [subscribe] RPC for events: {:?} from: {:?} with shard: {:?}",
            request.get_ref().event_types,
            request.get_ref().from_id,
            request.get_ref().shard_index
        );
        let (server_tx, client_rx) = mpsc::channel::<Result<HubEvent, Status>>(100);
        let events_txs = match request.get_ref().shard_index {
            Some(shard_id) => match self.shard_senders.get(&(shard_id)) {
                None => {
                    return Err(Status::from_error(Box::new(
                        HubError::invalid_internal_state("Invalid shard id"),
                    )))
                }
                Some(senders) => vec![(shard_id, senders.events_tx.clone())],
            },
            None => self
                .shard_senders
                .iter()
                .map(|(shard_id, senders)| (*shard_id, senders.events_tx.clone()))
                .collect(),
        };

        let shard_stores = match request.get_ref().shard_index {
            Some(shard_id) => {
                vec![self.shard_stores.get(&shard_id).cloned().unwrap()]
            }
            None => self.shard_stores.values().cloned().collect(),
        };

        let request = request.into_inner();
        let events = request.event_types;
        let mut inner_events: Vec<i32> = Vec::new();
        inner_events.resize(events.len(), 0);
        inner_events.copy_from_slice(events.as_slice());
        let from_id = request.from_id;

        tokio::spawn(async move {
            let event_types = inner_events;
            let mut event_types_filter = Vec::new();
            event_types_filter.resize(event_types.len(), 0);
            event_types_filter.copy_from_slice(event_types.as_slice());

            // If [from_id] is not specified, start from the latest events
            if let Some(start_id) = from_id {
                let mut page_token = None;
                for store in shard_stores {
                    info!(
                        "[subscribe] Replaying old events for shard {}",
                        store.shard_id
                    );
                    let mut last_chunk: Option<ShardChunk> = None;
                    loop {
                        let (old_events, chunk) = Self::get_events_from_store(
                            &store,
                            start_id,
                            None,
                            Some(PageOptions {
                                page_token: page_token.clone(),
                                page_size: None,
                                reverse: false,
                            }),
                            last_chunk,
                        );

                        last_chunk = chunk;

                        for event in old_events.events {
                            if event_types.contains(&event.r#type) {
                                if let Err(_) = server_tx.send(Ok(event)).await {
                                    return;
                                }
                            }
                        }

                        page_token = old_events.next_page_token;
                        if page_token.is_none() {
                            break;
                        }
                    }
                }
            }

            info!(
                "[subscribe] Streaming live events from {} shards",
                events_txs.len()
            );

            // TODO(aditi): It's possible that events show up between when we finish reading from the db and the subscription starts. We don't handle this case in the current hub code, but we may want to down the line.
            for (shard_id, event_tx) in events_txs {
                let mut inner_events: Vec<i32> = Vec::new();
                inner_events.resize(event_types_filter.len(), 0);
                inner_events.copy_from_slice(event_types_filter.as_slice());
                let tx = server_tx.clone();
                tokio::spawn(async move {
                    let filtered_events = inner_events.clone();
                    let mut event_rx = event_tx.subscribe();
                    loop {
                        match event_rx.recv().await {
                            Ok(hub_event) => {
                                if filtered_events.contains(&hub_event.r#type) {
                                    let hub_event =
                                        Self::rewrite_hub_event(hub_event, shard_id, None);
                                    match tx.send(Ok(hub_event)).await {
                                        Ok(_) => {}
                                        Err(_) => {
                                            // This means the client hung up
                                            info!("[subscribe] Client hung up on RPC, stopping event stream");
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                error!(
                                    { err = err.to_string() },
                                    "[subscribe] error receiving from event stream"
                                )
                            }
                        }
                    }
                });
            }
        });

        Ok(Response::new(ReceiverStream::new(client_rx)))
    }

    async fn get_event(
        &self,
        request: Request<EventRequest>,
    ) -> Result<Response<HubEvent>, Status> {
        let request = request.into_inner();
        // Not sure this is the correct way to be handling the shard
        let stores = self.get_stores_for_shard(request.shard_index)?;
        let hub_event_result = stores.get_event(request.id);

        match hub_event_result {
            Ok(hub_event) => {
                let (block_number, _) = HubEventIdGenerator::extract_height_and_seq(hub_event.id);
                let chunk = stores.shard_store.get_chunk_by_height(
                    Height {
                        shard_index: stores.shard_id,
                        block_number,
                    }
                    .as_u64(),
                );
                let hub_event = Self::rewrite_hub_event(
                    hub_event,
                    stores.shard_id,
                    chunk
                        .unwrap_or(None)
                        .as_ref()
                        .map(|chunk| chunk.header.as_ref().unwrap().timestamp),
                );

                Ok(Response::new(hub_event))
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn get_events(
        &self,
        request: Request<EventsRequest>,
    ) -> Result<Response<EventsResponse>, Status> {
        let req = request.into_inner();

        let num_shards;
        let shard_stores;
        match req.shard_index {
            None => {
                num_shards = self.num_shards;
                shard_stores = self.shard_stores.values().collect::<Vec<_>>();
            }
            Some(index) => {
                num_shards = 1;
                shard_stores = match self.shard_stores.get(&index) {
                    Some(store) => {
                        vec![store]
                    }
                    None => return Err(Status::invalid_argument("Shard not found".to_string())),
                };
            }
        }
        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards as usize]
        };
        if per_shard_tokens.len() != num_shards as usize {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }
        let pages: Vec<EventsPage> = shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(store, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };
                let (events, _) = Self::get_events_from_store(
                    store,
                    req.start_id,
                    req.stop_id,
                    Some(page_options),
                    None,
                );
                events
            })
            .collect();
        let combined_events: Vec<HubEvent> =
            pages.iter().flat_map(|page| page.events.clone()).collect();
        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();
        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;
        let response = EventsResponse {
            events: combined_events,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_cast(&self, request: Request<CastId>) -> Result<Response<proto::Message>, Status> {
        let cast_id = request.into_inner();
        let stores = self.get_stores_for(cast_id.fid)?;
        CastStore::get_cast_add(&stores.cast_store, cast_id.fid, cast_id.hash).as_response()
    }

    async fn get_casts_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        CastStore::get_cast_adds_by_fid(&stores.cast_store, request.fid, &options).as_response()
    }

    async fn get_all_cast_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .cast_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_reaction(
        &self,
        request: Request<ReactionRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::reaction_request::Target::TargetCastId(cast_id)) => {
                Some(proto::reaction_body::Target::TargetCastId(cast_id))
            }
            Some(proto::reaction_request::Target::TargetUrl(url)) => {
                Some(proto::reaction_body::Target::TargetUrl(url))
            }
            None => None,
        };
        ReactionStore::get_reaction_add(
            &stores.reaction_store,
            request.fid,
            request.reaction_type,
            target,
        )
        .as_response()
    }

    async fn get_reactions_by_fid(
        &self,
        request: Request<ReactionsByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        ReactionStore::get_reaction_adds_by_fid(
            &stores.reaction_store,
            request.fid,
            request.reaction_type.unwrap_or(0),
            &options,
        )
        .as_response()
    }

    async fn get_all_reaction_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<proto::MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .reaction_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link(&self, request: Request<LinkRequest>) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let target = match request.target {
            Some(proto::link_request::Target::TargetFid(fid)) => {
                Some(proto::link_body::Target::TargetFid(fid))
            }
            None => None,
        };
        LinkStore::get_link_add(&stores.link_store, request.fid, request.link_type, target)
            .as_response()
    }

    async fn get_links_by_fid(
        &self,
        request: Request<LinksByFidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_adds_by_fid(
            &stores.link_store,
            request.fid,
            request.link_type.unwrap_or("".to_string()),
            &options,
        )
        .as_response()
    }

    async fn get_all_link_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .link_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_user_data(
        &self,
        request: Request<UserDataRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let user_data_type = proto::UserDataType::try_from(request.user_data_type)
            .map_err(|_| Status::invalid_argument("Invalid user data type"))?;
        UserDataStore::get_user_data_by_fid_and_type(
            &stores.user_data_store,
            request.fid,
            user_data_type,
        )
        .as_response()
    }

    async fn get_user_data_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        UserDataStore::get_user_data_adds_by_fid(
            &stores.user_data_store,
            request.fid,
            &options,
            None,
            None,
        )
        .as_response()
    }

    async fn get_all_user_data_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .user_data_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn validate_message(
        &self,
        request: Request<Message>,
    ) -> Result<Response<ValidationResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid())?;
        let is_pro_user = stores
            .is_pro_user(request.fid(), &FarcasterTime::current())
            .map_err(|err| Status::from_error(Box::new(err)))?;
        let result = validations::message::validate_message(
            &request,
            self.network,
            is_pro_user,
            &FarcasterTime::current(),
            EngineVersion::current(self.network),
        )
        .map_or_else(|_| false, |_| true);

        Ok(Response::new(ValidationResponse {
            valid: result,
            message: Some(request),
        }))
    }

    async fn get_verification(
        &self,
        request: Request<VerificationRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        VerificationStore::get_verification_add(
            &stores.verification_store,
            request.fid,
            &request.address,
            None,
        )
        .as_response()
    }

    async fn get_verifications_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        VerificationStore::get_verification_adds_by_fid(
            &stores.verification_store,
            request.fid,
            &options,
        )
        .as_response()
    }

    async fn get_all_verification_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let (start_ts, stop_ts) = request.timestamps();
        stores
            .verification_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_all_lend_storage_messages_by_fid(
        &self,
        request: Request<FidTimestampRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let (start_ts, stop_ts) = request.timestamps();
        // These messages are stored on all shards. Query them from the block shard because this is the source of truth.
        self.block_stores
            .storage_lend_store
            .get_all_messages_by_fid(request.fid, start_ts, stop_ts, &request.page_options())
            .as_response()
    }

    async fn get_link_compact_state_message_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let options = request.page_options();
        LinkStore::get_link_compact_state_message_by_fid(&stores.link_store, request.fid, &options)
            .as_response()
    }

    async fn get_current_storage_limits_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<StorageLimitsResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for(request.fid)?;
        let limits = stores
            .get_storage_limits(request.fid)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(limits))
    }

    async fn get_casts_by_parent(
        &self,
        request: Request<CastsByParentRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();
        let parent = match req.parent {
            Some(casts_by_parent_request::Parent::ParentCastId(cast_id)) => {
                cast_add_body::Parent::ParentCastId(cast_id)
            }
            Some(casts_by_parent_request::Parent::ParentUrl(url)) => {
                cast_add_body::Parent::ParentUrl(url)
            }
            None => return Err(Status::not_found("Parent not specified".to_string())),
        };
        let num_shards = self.shard_stores.len();
        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };
        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }
        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };
                let cast_store = &shard_entry.1.cast_store;
                return CastStore::get_casts_by_parent(cast_store, &parent, &page_options)
                    .unwrap_or(MessagesPage {
                        messages: vec![],
                        next_page_token: None,
                    });
            })
            .collect();
        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();
        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();
        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;
        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_casts_by_mention(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();
        let mention = req.fid;

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> =
            self.shard_stores
                .iter()
                .zip(per_shard_tokens.into_iter())
                .map(|(shard_entry, shard_token)| {
                    let page_options = PageOptions {
                        page_size: req.page_size.map(|s| s as usize),
                        page_token: shard_token,
                        reverse: req.reverse.unwrap_or(false),
                    };

                    let store = &shard_entry.1.cast_store;
                    return CastStore::get_casts_by_mention(store, mention, &page_options)
                        .unwrap_or(MessagesPage {
                            messages: vec![],
                            next_page_token: None,
                        });
                })
                .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_reactions_by_cast(
        &self,
        request: Request<ReactionsByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        let reaction_type = req
            .reaction_type
            .ok_or_else(|| Status::invalid_argument("reaction_type is required".to_string()))?;

        let target = match req.target {
            Some(reactions_by_target_request::Target::TargetCastId(cast_id)) => {
                reaction_body::Target::TargetCastId(cast_id)
            }
            // Enforce compatibility, disallow url target
            _ => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.reaction_store;

                return ReactionStore::get_reactions_by_target(
                    store,
                    &target,
                    reaction_type,
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                });
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_reactions_by_target(
        &self,
        request: Request<ReactionsByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        let reaction_type = req.reaction_type.unwrap_or(ReactionType::None.into()); // Use enum vs 0?

        let target = match req.target {
            Some(reactions_by_target_request::Target::TargetCastId(cast_id)) => {
                reaction_body::Target::TargetCastId(cast_id)
            }
            Some(reactions_by_target_request::Target::TargetUrl(url)) => {
                reaction_body::Target::TargetUrl(url)
            }
            None => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.reaction_store;

                return ReactionStore::get_reactions_by_target(
                    store,
                    &target,
                    reaction_type,
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                });
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = if next_page_tokens.iter().any(|token| token.is_some()) {
            Some(serde_json::to_vec(&next_page_tokens).map_err(|e| {
                Status::internal(format!("Failed to serialize next_page_token: {}", e))
            })?)
        } else {
            None // Return None if no subsequent page exists
        };

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: new_page_token,
        };

        Ok(Response::new(response))
    }

    async fn get_username_proof(
        &self,
        request: Request<UsernameProofRequest>,
    ) -> Result<Response<UserNameProof>, Status> {
        let req = request.into_inner();
        let name_str = std::str::from_utf8(&req.name).unwrap_or("");

        // Check if this is an .eth name (look in username_proof_store) or fname (look in user_data_store)
        if name_str.ends_with(".eth") {
            // Look for ENS username proofs in the username_proof_store
            let proof_opt = self.shard_stores.iter().find_map(|(_shard_entry, stores)| {
                match UsernameProofStore::get_username_proof(
                    &stores.username_proof_store,
                    &req.name,
                    &mut RocksDbTransactionBatch::new(),
                ) {
                    Ok(Some(message)) => message.data.and_then(|data| {
                        if let Some(message_data::Body::UsernameProofBody(user_name_proof)) =
                            data.body
                        {
                            Some(user_name_proof)
                        } else {
                            None
                        }
                    }),
                    _ => None,
                }
            });

            if let Some(proof_message) = proof_opt {
                Ok(Response::new(proof_message))
            } else {
                Err(Status::not_found(
                    "ENS username proof not found".to_string(),
                ))
            }
        } else {
            // Look for fname proofs in the user_data_store
            let proof_opt = self.shard_stores.iter().find_map(|(_shard_entry, stores)| {
                match UserDataStore::get_username_proof(
                    &stores.user_data_store,
                    &mut RocksDbTransactionBatch::new(),
                    &req.name,
                ) {
                    Ok(Some(user_name_proof)) => Some(user_name_proof),
                    _ => None,
                }
            });

            if let Some(proof_message) = proof_opt {
                Ok(Response::new(proof_message))
            } else {
                Err(Status::not_found("Username proof not found".to_string()))
            }
        }
    }

    async fn get_user_name_proofs_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<UsernameProofsResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let mut combined_proofs = Vec::new();

        // First, get proofs from username_proof_store (for ENS names)
        let ens_shard_results: Vec<Result<Vec<UserNameProof>, Status>> = self
            .shard_stores
            .iter()
            .map(|(_shard_id, stores)| {
                let mut all_proofs = Vec::new();
                let mut token: Option<Vec<u8>> = None;

                loop {
                    let page_options = PageOptions {
                        page_size: None,
                        page_token: token.clone(),
                        reverse: false,
                    };

                    let page = UsernameProofStore::get_username_proofs_by_fid(
                        &stores.username_proof_store,
                        fid,
                        &page_options,
                    )
                    .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;

                    all_proofs.extend(page.messages.into_iter().filter_map(|message| {
                        message.data.and_then(|data| {
                            if let Some(message_data::Body::UsernameProofBody(user_name_proof)) =
                                data.body
                            {
                                Some(user_name_proof)
                            } else {
                                None
                            }
                        })
                    }));

                    if page.next_page_token.is_none() {
                        break;
                    }

                    token = page.next_page_token;
                }

                Ok(all_proofs)
            })
            .collect();

        // Aggregate ENS proofs
        for shard_result in ens_shard_results {
            let proofs = shard_result?;
            combined_proofs.extend(proofs);
        }

        // Now get proofs from user_data_store (for fnames)
        for (_shard_id, stores) in &self.shard_stores {
            match UserDataStore::get_username_proof_by_fid(&stores.user_data_store, fid) {
                Ok(Some(proof)) => {
                    combined_proofs.push(proof);
                }
                Ok(None) => {}
                Err(e) => {
                    // Log the error but continue, to try to get all proofs we can
                    error!("Error getting username proof from user_data_store: {:?}", e);
                }
            }
        }

        let response = UsernameProofsResponse {
            proofs: combined_proofs,
        };

        Ok(Response::new(response))
    }

    async fn get_on_chain_signer(
        &self,
        request: Request<SignerRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let signer = req.signer;

        let maybe_event = self.shard_stores.iter().find_map(|(_shard_id, stores)| {
            match stores
                .onchain_event_store
                .get_active_signer(fid, signer.clone(), None)
            {
                Ok(Some(event)) => Some(Ok(event)),
                Ok(None) => None,
                Err(e) => Some(Err(Status::internal(format!("Store error: {:?}", e)))),
            }
        });

        let event = match maybe_event {
            Some(Ok(event)) => event,
            Some(Err(e)) => return Err(e),
            None => return Err(Status::not_found("Active signer not found".to_string())),
        };

        Ok(Response::new(event))
    }

    async fn get_on_chain_signers_by_fid(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<OnChainEventResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let stores = self.get_stores_for(fid)?;
        let events_page = stores
            .onchain_event_store
            .get_signers(
                Some(fid),
                &PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: req.page_token.clone(),
                    reverse: req.reverse.unwrap_or(false),
                },
            )
            .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;

        let response = OnChainEventResponse {
            events: events_page.onchain_events,
            next_page_token: events_page.next_page_token,
        };
        Ok(Response::new(response))
    }

    async fn get_signer(
        &self,
        request: Request<SignerRequest>,
    ) -> Result<Response<SignerResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let stores = self.get_stores_for(fid)?;

        let resolved = resolve_signer(stores, fid, &req.signer)?
            .ok_or_else(|| Status::not_found("Active signer not found".to_string()))?;

        Ok(Response::new(SignerResponse {
            signer: Some(resolved),
        }))
    }

    async fn get_signers_by_fid(
        &self,
        request: Request<SignersByFidRequest>,
    ) -> Result<Response<SignersByFidResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let stores = self.get_stores_for(fid)?;

        let page_options = req.page_options();
        let page = list_signers_for_fid(stores, fid, &page_options)?;

        // Read nonces from the gasless-key counter store. A missing entry means
        // no gasless activity has occurred yet for that namespace, which we
        // surface as 0 — matching the merge-time validation that treats stored
        // = 0 when the key is absent (key_nonce_store::check_and_set_nonce).
        let nonce_txn = RocksDbTransactionBatch::new();
        let current_user_nonce = get_user_nonce(&stores.db, &nonce_txn, fid)
            .map_err(signer_store_error_to_status)?
            .unwrap_or(0);
        // One map entry per requested FID. Missing counters surface as 0,
        // mirroring the merge-time validation rule (stored = 0 when absent).
        let mut requester_fid_nonces: HashMap<u64, u32> =
            HashMap::with_capacity(req.requester_fids.len());
        for requester_fid in &req.requester_fids {
            let nonce = get_app_nonce(&stores.db, &nonce_txn, *requester_fid)
                .map_err(signer_store_error_to_status)?
                .unwrap_or(0);
            requester_fid_nonces.insert(*requester_fid, nonce);
        }

        Ok(Response::new(SignersByFidResponse {
            signers: page.signers,
            next_page_token: page.next_page_token,
            gasless_signer_count: page.gasless_signer_count,
            gasless_signer_limit: crate::core::validations::key::MAX_GASLESS_KEYS_PER_FID,
            current_user_nonce,
            requester_fid_nonces,
        }))
    }

    async fn get_on_chain_events(
        &self,
        request: Request<OnChainEventRequest>,
    ) -> Result<Response<OnChainEventResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let event_type = proto::OnChainEventType::try_from(req.event_type)
            .map_err(|_| Status::invalid_argument("Invalid event type"))?;

        let mut combined_events = Vec::new();
        for (_shard_id, stores) in &self.shard_stores {
            let events = stores
                .onchain_event_store
                .get_onchain_events(event_type, Some(fid))
                .map_err(|e| Status::internal(format!("Store error: {:?}", e)))?;
            combined_events.extend(events);
        }

        let response = OnChainEventResponse {
            events: combined_events,
            next_page_token: None,
        };
        Ok(Response::new(response))
    }

    async fn get_channel_owner(
        &self,
        request: Request<ChannelOwnerRequest>,
    ) -> Result<Response<ChannelOwnerResponse>, Status> {
        let req = request.into_inner();
        let channel_owner = self
            .block_stores
            .onchain_event_store
            .get_channel_owner(&req.channel_key, None)
            .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?
            .ok_or_else(|| Status::not_found("channel not registered"))?;

        // 0 is a real answer, not a swallowed error: the owner's verification has
        // not landed on shard 0 yet. See ChannelOwnerResponse.fid.
        let fid = self
            .block_stores
            .resolve_channel_owner_fid(&channel_owner.owner_address, None)
            .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?
            .unwrap_or(0);

        Ok(Response::new(ChannelOwnerResponse {
            fid,
            owner_address: channel_owner.owner_address,
            expiry: channel_owner.expiry,
        }))
    }

    async fn get_channels_by_address(
        &self,
        request: Request<ChannelsByAddressRequest>,
    ) -> Result<Response<ChannelsResponse>, Status> {
        let req = request.into_inner();
        // Match GetChannelsByFid's page-size contract on this public read: 0
        // returns an empty page, and an omitted or oversized value is clamped
        // to the server-side maximum so it can't trigger an unbounded scan.
        let mut page_options = req.page_options();
        page_options.page_size = match page_options.page_size {
            Some(0) => {
                return Ok(Response::new(ChannelsResponse {
                    channels: vec![],
                    next_page_token: None,
                }));
            }
            Some(size) => Some(size.min(PAGE_SIZE_MAX)),
            None => Some(PAGE_SIZE_MAX),
        };
        let (mut channels, next_page_token) = channel_infos_by_owner_address(
            &self.block_stores,
            &req.owner_address,
            0,
            &page_options,
        )?;

        if !channels.is_empty() {
            // Same contract as GetChannelOwner: 0 means the owner's verification
            // has not landed on shard 0 yet, and is stamped onto every channel in
            // this page. See ChannelOwnerResponse.fid.
            let fid = self
                .block_stores
                .resolve_channel_owner_fid(&req.owner_address, None)
                .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?
                .unwrap_or(0);
            for channel in &mut channels {
                channel.fid = fid;
            }
        }

        Ok(Response::new(ChannelsResponse {
            channels,
            next_page_token,
        }))
    }

    async fn get_channels_by_fid(
        &self,
        request: Request<ChannelsByFidRequest>,
    ) -> Result<Response<ChannelsResponse>, Status> {
        let req = request.into_inner();
        let stores = self.get_stores_for(req.fid)?;

        // Clamp the page size to a server-side maximum so an omitted or oversized
        // request can't trigger an unbounded scan on this public read.
        let page_size = match req.page_size {
            Some(0) => {
                return Ok(Response::new(ChannelsResponse {
                    channels: vec![],
                    next_page_token: None,
                }));
            }
            Some(size) => (size as usize).min(PAGE_SIZE_MAX),
            None => PAGE_SIZE_MAX,
        };

        // Collect the fid's verified Ethereum addresses (deduped), then sort them
        // ascending so the by-owner-address index key is a globally ordered
        // composite cursor across the whole join.
        let mut verification_page_token = None;
        let mut owner_addresses = Vec::new();
        let mut seen_owner_addresses = HashSet::new();

        loop {
            let verification_page_options = PageOptions {
                page_size: None,
                page_token: verification_page_token.clone(),
                reverse: false,
            };
            let page = VerificationStore::get_verification_adds_by_fid(
                &stores.verification_store,
                req.fid,
                &verification_page_options,
            )
            .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?;

            for message in page.messages {
                let message_hash = message.hash.clone();
                let Some(data) = message.data else {
                    // A merged message always carries `data`; a missing one is a
                    // storage anomaly, not a benign state. Skip it (one bad record
                    // must not fail the read) but log so it stays observable.
                    warn!(
                        fid = req.fid,
                        message_hash = hex::encode(&message_hash),
                        "channels-by-fid skipped a VerificationAdd with no data",
                    );
                    continue;
                };
                let Some(proto::message_data::Body::VerificationAddAddressBody(body)) = data.body
                else {
                    continue;
                };
                if body.protocol != proto::Protocol::Ethereum as i32 || body.address.len() != 20 {
                    continue;
                }
                if seen_owner_addresses.insert(body.address.clone()) {
                    owner_addresses.push(body.address);
                }
            }

            let Some(next_page_token) = page.next_page_token else {
                break;
            };
            verification_page_token = Some(next_page_token);
        }
        owner_addresses.sort();

        // Keep only the addresses this fid currently wins under the shard-0
        // winner rule, so a channel appears here only if GetChannelOwner
        // resolves it to `req.fid`. The converse does not quite hold: shard-0
        // replica rows are permanent, but the home-shard rows enumerated above
        // prune to the fid's local storage cap, so a winning verification the
        // home shard has pruned keeps the owner resolvable without the channel
        // being listed here.
        let mut winning_addresses = Vec::new();
        for owner_address in owner_addresses {
            if self
                .block_stores
                .resolve_channel_owner_fid(&owner_address, None)
                .map_err(|err| Status::internal(format!("Store error: {:?}", err)))?
                == Some(req.fid)
            {
                winning_addresses.push(owner_address);
            }
        }

        // Page across the winning addresses using the composite cursor.
        let (index_keys, next_page_token) = get_channel_keys_for_owner_addresses(
            &self.block_stores.db,
            &winning_addresses,
            req.page_token.as_deref(),
            page_size,
        )
        .map_err(onchain_event_storage_error_to_status)?;
        let channels = channel_infos_for_index_keys(&self.block_stores, index_keys, req.fid)?;

        Ok(Response::new(ChannelsResponse {
            channels,
            next_page_token,
        }))
    }

    async fn get_channel_member(
        &self,
        request: Request<ChannelMemberRequest>,
    ) -> Result<Response<ChannelMemberResponse>, Status> {
        let req = request.into_inner();
        let channel_id = self.require_registered_channel(&req.channel_id)?;
        if req.fid == 0 || u32::try_from(req.fid).is_err() {
            return Err(Status::invalid_argument("fid must fit in a non-zero u32"));
        }
        let member = ChannelMemberStore::member(
            &self.block_stores.channel_member_store,
            &channel_id,
            req.fid,
            None,
        )
        .map_err(|err| Status::internal(format!("Store error: {err:?}")))?;
        Ok(Response::new(match member {
            Some(member) => ChannelMemberResponse {
                state: channel_member_state_to_proto(member.state) as i32,
                last_action_ts: Some(member.last_action_ts),
            },
            None => ChannelMemberResponse {
                state: proto::ChannelMemberState::None as i32,
                last_action_ts: None,
            },
        }))
    }

    async fn get_channel_members(
        &self,
        request: Request<ChannelMembersRequest>,
    ) -> Result<Response<ChannelMembersResponse>, Status> {
        let req = request.into_inner();
        let channel_id = self.require_registered_channel(&req.channel_id)?;
        require_nonzero_page_size(req.page_size)?;
        let state_filter = req
            .state_filter
            .map(|value| {
                proto::ChannelMemberState::try_from(value)
                    .map_err(|_| Status::invalid_argument("invalid channel member state"))
            })
            .transpose()?
            .and_then(channel_member_state_from_proto);
        let page = ChannelMemberStore::members_by_channel(
            &self.block_stores.channel_member_store,
            &channel_id,
            state_filter,
            &channel_page_options(req.page_size, req.page_token, req.reverse),
        )
        .map_err(channel_store_error_to_status)?;
        Ok(Response::new(ChannelMembersResponse {
            members: page
                .entries
                .into_iter()
                .map(|member| ChannelMember {
                    fid: member.fid,
                    state: channel_member_state_to_proto(member.state) as i32,
                })
                .collect(),
            next_page_token: page.next_page_token,
        }))
    }

    async fn get_channel_pin(
        &self,
        request: Request<ChannelRequest>,
    ) -> Result<Response<ChannelPinResponse>, Status> {
        let req = request.into_inner();
        let channel_id = self.require_registered_channel(&req.channel_id)?;
        let pin = ChannelPinStore::get_channel_pin_state(
            &self.block_stores.channel_pin_store,
            &channel_id,
            None,
        )
        .map_err(|err| Status::internal(format!("Store error: {err:?}")))?;
        // An unpin (empty cast_hash, permitted by validate_channel_pin_body) and a
        // channel that was never pinned intentionally read identically as "no pin".
        Ok(Response::new(ChannelPinResponse {
            pin: pin
                .filter(|pin| !pin.body.cast_hash.is_empty())
                .map(|pin| ChannelPin {
                    cast_hash: pin.body.cast_hash,
                    author_fid: pin.author_fid,
                }),
        }))
    }

    async fn get_channel_moderations(
        &self,
        request: Request<ChannelModerationsRequest>,
    ) -> Result<Response<ChannelModerationsResponse>, Status> {
        let req = request.into_inner();
        let channel_id = self.require_registered_channel(&req.channel_id)?;
        require_nonzero_page_size(req.page_size)?;
        let page = ChannelModerateStore::moderations_by_channel(
            &self.block_stores.channel_moderate_store,
            &channel_id,
            &channel_page_options(req.page_size, req.page_token, req.reverse),
        )
        .map_err(channel_store_error_to_status)?;
        Ok(Response::new(ChannelModerationsResponse {
            moderations: page
                .entries
                .into_iter()
                .map(|moderation| ChannelModeration {
                    cast_hash: moderation.cast_hash,
                    action: moderation.action as i32,
                    author_fid: moderation.author_fid,
                })
                .collect(),
            next_page_token: page.next_page_token,
        }))
    }

    async fn get_channel_metadata(
        &self,
        request: Request<ChannelRequest>,
    ) -> Result<Response<ChannelMetadataResponse>, Status> {
        let req = request.into_inner();
        let channel_id = self.require_registered_channel(&req.channel_id)?;
        let update = ChannelUpdateStore::get_channel_update(
            &self.block_stores.channel_update_store,
            &channel_id,
            None,
        )
        .map_err(|err| Status::internal(format!("Store error: {err:?}")))?;
        Ok(Response::new(match update {
            Some(update) => ChannelMetadataResponse {
                name: update.body.name,
                description: update.body.description,
                image_url: update.body.image_url,
                header: update.body.header,
                rules: update.body.rules,
                casting_mode: update.casting_mode as i32,
                membership_mode: update.membership_mode as i32,
            },
            None => {
                // Taken from the fold rather than restated, so this branch cannot
                // drift from the policy admission applies to an unconfigured channel.
                let (casting_mode, membership_mode) = ChannelUpdateStore::default_channel_modes();
                ChannelMetadataResponse {
                    name: None,
                    description: None,
                    image_url: None,
                    header: None,
                    rules: None,
                    casting_mode: casting_mode as i32,
                    membership_mode: membership_mode as i32,
                }
            }
        }))
    }

    async fn get_channel_memberships_by_fid(
        &self,
        request: Request<ChannelMembershipsByFidRequest>,
    ) -> Result<Response<ChannelMembershipsResponse>, Status> {
        let req = request.into_inner();
        if req.fid == 0 || u32::try_from(req.fid).is_err() {
            return Err(Status::invalid_argument("fid must fit in a non-zero u32"));
        }
        require_nonzero_page_size(req.page_size)?;
        let page = ChannelMemberStore::memberships_by_fid(
            &self.block_stores.channel_member_store,
            req.fid,
            &channel_page_options(req.page_size, req.page_token, req.reverse),
        )
        .map_err(channel_store_error_to_status)?;
        Ok(Response::new(ChannelMembershipsResponse {
            memberships: page
                .entries
                .into_iter()
                .map(|membership| ChannelMembership {
                    channel_id: membership.channel_id,
                    state: channel_member_state_to_proto(membership.state) as i32,
                })
                .collect(),
            next_page_token: page.next_page_token,
        }))
    }

    async fn get_channel_followers(
        &self,
        request: Request<ChannelFollowersRequest>,
    ) -> Result<Response<ChannelFollowersResponse>, Status> {
        let req = request.into_inner();
        // Width only: follows live on data shards and carry no registration check.
        let channel_id = require_channel_id_width(&req.channel_id)?;
        require_nonzero_page_size(req.page_size)?;

        let shards = self.all_shard_stores()?;
        let mut cursors = decode_shard_cursors(req.page_token, &shards)?;

        let mut followers = Vec::new();
        let mut next_cursors = Vec::with_capacity(shards.len());
        for (shard_id, stores) in &shards {
            let token = match cursors.remove(shard_id) {
                Some(ShardScan::Fresh) => None,
                Some(ShardScan::Resume(token)) => Some(token),
                // Already read to the end on an earlier page. Skipping it is the
                // whole point of the three-state cursor: re-scanning with `None`
                // would re-emit its rows forever.
                Some(ShardScan::Exhausted) => {
                    next_cursors.push(ShardCursor {
                        shard_id: *shard_id,
                        scan: ShardScan::Exhausted,
                    });
                    continue;
                }
                // `decode_shard_cursors` guarantees every hosted shard is present,
                // so this is unreachable. Refuse rather than defaulting to a fresh
                // scan: if that guarantee is ever loosened, defaulting would
                // silently restart a shard and page forever.
                None => {
                    return Err(Status::internal(format!(
                        "no cursor for shard {shard_id} after validation"
                    )))
                }
            };
            let page = ReactionStore::followers_by_channel(
                &stores.reaction_store,
                &channel_id,
                &channel_page_options(req.page_size, token, req.reverse),
            )
            .map_err(channel_store_error_to_status)?;
            followers.extend(page.entries.into_iter().map(|entry| ChannelFollower {
                fid: entry.fid,
                followed_at: entry.followed_at,
            }));
            next_cursors.push(ShardCursor {
                shard_id: *shard_id,
                scan: match page.next_page_token {
                    Some(token) => ShardScan::Resume(token),
                    None => ShardScan::Exhausted,
                },
            });
        }

        Ok(Response::new(ChannelFollowersResponse {
            followers,
            next_page_token: encode_shard_cursors(next_cursors)?,
        }))
    }

    async fn get_channel_follower_count(
        &self,
        request: Request<ChannelFollowerCountRequest>,
    ) -> Result<Response<ChannelFollowerCountResponse>, Status> {
        let req = request.into_inner();
        let channel_id = require_channel_id_width(&req.channel_id)?;

        // Summed as u64: each shard's counter is a u32, and their total need not be.
        let mut count: u64 = 0;
        for (_shard_id, stores) in self.all_shard_stores()? {
            count += ReactionStore::follower_count(&stores.reaction_store, &channel_id)
                .map_err(channel_store_error_to_status)?;
        }
        Ok(Response::new(ChannelFollowerCountResponse { count }))
    }

    async fn get_channel_follows(
        &self,
        request: Request<ChannelFollowsRequest>,
    ) -> Result<Response<ChannelFollowsResponse>, Status> {
        let req = request.into_inner();
        if req.fid == 0 || u32::try_from(req.fid).is_err() {
            return Err(Status::invalid_argument("fid must fit in a non-zero u32"));
        }
        require_nonzero_page_size(req.page_size)?;
        // Keyed by fid, so this is a single-shard read with none of the fan-out
        // caveats.
        let stores = self.get_stores_for(req.fid)?;
        let page = ReactionStore::follows_by_fid(
            &stores.reaction_store,
            req.fid,
            &channel_page_options(req.page_size, req.page_token, req.reverse),
        )
        .map_err(channel_store_error_to_status)?;
        Ok(Response::new(ChannelFollowsResponse {
            follows: page
                .entries
                .into_iter()
                .map(|entry| ChannelFollow {
                    channel_id: entry.channel_id.to_vec(),
                    followed_at: entry.followed_at,
                })
                .collect(),
            next_page_token: page.next_page_token,
        }))
    }

    async fn is_following_channel(
        &self,
        request: Request<IsFollowingChannelRequest>,
    ) -> Result<Response<IsFollowingChannelResponse>, Status> {
        let req = request.into_inner();
        if req.fid == 0 || u32::try_from(req.fid).is_err() {
            return Err(Status::invalid_argument("fid must fit in a non-zero u32"));
        }
        let channel_id = require_channel_id_width(&req.channel_id)?;
        let stores = self.get_stores_for(req.fid)?;
        let followed_at = ReactionStore::is_following(&stores.reaction_store, req.fid, &channel_id)
            .map_err(channel_store_error_to_status)?;
        Ok(Response::new(IsFollowingChannelResponse {
            following: followed_at.is_some(),
            followed_at,
        }))
    }

    async fn get_id_registry_on_chain_event(
        &self,
        request: Request<FidRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        let req = request.into_inner();
        let fid = req.fid;

        let maybe_event = self.shard_stores.iter().find_map(|(_shard_id, stores)| {
            match stores
                .onchain_event_store
                .get_id_register_event_by_fid(fid, None)
            {
                Ok(Some(event)) => Some(Ok(event)),
                Ok(None) => None,
                Err(e) => Some(Err(Status::internal(format!("Store error: {:?}", e)))),
            }
        });

        let event = match maybe_event {
            Some(Ok(event)) => event,
            Some(Err(e)) => return Err(e),
            None => return Err(Status::not_found("ID registry event not found".to_string())),
        };

        Ok(Response::new(event))
    }

    async fn get_id_registry_on_chain_event_by_address(
        &self,
        request: Request<IdRegistryEventByAddressRequest>,
    ) -> Result<Response<OnChainEvent>, Status> {
        let address = request.into_inner().address;

        if let Some(evt) = self.id_registry_cache.get(&address) {
            return Ok(Response::new(evt.clone()));
        }

        for store in self.shard_stores.values() {
            let events = store
                .onchain_event_store
                .get_onchain_events(proto::OnChainEventType::EventTypeIdRegister, None)
                .map_err(|_| {
                    Status::internal("on chain event store iterator not found for EventType")
                    // Is this the correct error and hows the string look?
                })?;

            for evt in events {
                if let Some(Body::IdRegisterEventBody(body)) = &evt.body {
                    let key = &body.to;
                    self.id_registry_cache.insert(key.clone(), evt.clone());
                    // return here so we don't have to iterate through everything
                    if *key == address {
                        return Ok(Response::new(evt.clone()));
                    }
                }
            }
        }
        // If we reach here, we didn't find the event so error out
        Err(Status::not_found("no id-registry event for address"))
    }

    async fn get_fid_address_type(
        &self,
        request: Request<FidAddressTypeRequest>,
    ) -> Result<Response<FidAddressTypeResponse>, Status> {
        let req = request.into_inner();
        let fid = req.fid;
        let address = req.address;

        let mut is_custody = false;
        let mut is_auth = false;
        let mut is_verified = false;

        // Check if the address is a custody address (from IdRegistry)
        for store in self.shard_stores.values() {
            // Check IdRegistry for custody address
            if let Ok(Some(id_event)) = store
                .onchain_event_store
                .get_id_register_event_by_fid(fid, None)
            {
                if let Some(Body::IdRegisterEventBody(body)) = &id_event.body {
                    if body.to == address {
                        is_custody = true;
                    }
                }
            }

            // Check KeyRegistry for auth address (keyType=2)
            // We need to get all signer events, not just the filtered ones
            if let Ok(events) = store
                .onchain_event_store
                .get_onchain_events(proto::OnChainEventType::EventTypeSigner, Some(fid))
            {
                for signer_event in events {
                    if let Some(Body::SignerEventBody(signer_body)) = &signer_event.body {
                        // Check if this is an auth key (keyType=2) and matches the address
                        if signer_body.key_type == 2
                            && signer_body.key == address
                            && signer_body.event_type() == SignerEventType::Add
                        {
                            is_auth = true;
                        }
                    }
                }
            }

            // Check verified addresses
            if let Ok(Some(_verification)) = VerificationStore::get_verification_add(
                &store.verification_store,
                fid,
                &address,
                None,
            ) {
                is_verified = true;
            }

            // If we found results in this shard, no need to check others
            if is_custody || is_auth || is_verified {
                break;
            }
        }

        Ok(Response::new(FidAddressTypeResponse {
            is_custody,
            is_auth,
            is_verified,
        }))
    }

    async fn get_links_by_target(
        &self,
        request: Request<LinksByTargetRequest>,
    ) -> Result<Response<MessagesResponse>, Status> {
        let req = request.into_inner();

        if req.link_type.clone().is_none() {
            return Err(Status::invalid_argument(
                "link_type is required".to_string(),
            ));
        }

        let target = match req.target {
            Some(links_by_target_request::Target::TargetFid(fid)) => {
                link_body::Target::TargetFid(fid)
            }
            None => return Err(Status::not_found("Target not specified".to_string())),
        };

        let num_shards = self.shard_stores.len();

        let per_shard_tokens: Vec<Option<Vec<u8>>> = if let Some(token_bytes) = req.page_token {
            serde_json::from_slice(&token_bytes)
                .map_err(|e| Status::invalid_argument(format!("Invalid page token: {}", e)))?
        } else {
            vec![None; num_shards]
        };

        if per_shard_tokens.len() != num_shards {
            return Err(Status::invalid_argument(
                "Page token does not match number of shards".to_string(),
            ));
        }

        let pages: Vec<MessagesPage> = self
            .shard_stores
            .iter()
            .zip(per_shard_tokens.into_iter())
            .map(|(shard_entry, shard_token)| {
                let page_options = PageOptions {
                    page_size: req.page_size.map(|s| s as usize),
                    page_token: shard_token,
                    reverse: req.reverse.unwrap_or(false),
                };

                let store = &shard_entry.1.link_store;
                LinkStore::get_links_by_target(
                    store,
                    &target,
                    req.link_type.clone().unwrap(),
                    &page_options,
                )
                .unwrap_or(MessagesPage {
                    messages: vec![],
                    next_page_token: None,
                })
            })
            .collect();

        let combined_messages: Vec<Message> = pages
            .iter()
            .flat_map(|page| page.messages.clone())
            .collect();

        let next_page_tokens: Vec<Option<Vec<u8>>> =
            pages.into_iter().map(|page| page.next_page_token).collect();

        let new_page_token = serde_json::to_vec(&next_page_tokens)
            .map_err(|e| Status::internal(format!("Failed to serialize next_page_token: {}", e)))?;

        let response = MessagesResponse {
            messages: combined_messages,
            next_page_token: Some(new_page_token),
        };

        Ok(Response::new(response))
    }

    async fn get_trie_metadata_by_prefix(
        &self,
        request: Request<TrieNodeMetadataRequest>,
    ) -> Result<Response<TrieNodeMetadataResponse>, Status> {
        let request = request.into_inner();
        let stores = self.get_stores_for_shard(request.shard_id)?;
        let trie_node = stores
            .trie
            .get_trie_node_metadata(
                &stores.db,
                &mut RocksDbTransactionBatch::new(),
                &request.prefix,
            )
            .map_err(|err| Status::internal(err.to_string()))?;
        let children = trie_node
            .children
            .values()
            .map(|child_node| TrieNodeMetadataResponse {
                prefix: child_node.prefix.clone(),
                num_messages: child_node.num_messages as u64,
                hash: child_node.hash.clone(),
                children: vec![],
            })
            .collect();
        Ok(Response::new(TrieNodeMetadataResponse {
            prefix: trie_node.prefix,
            num_messages: trie_node.num_messages as u64,
            hash: trie_node.hash,
            children,
        }))
    }

    async fn get_connected_peers(
        &self,
        _request: Request<GetConnectedPeersRequest>,
    ) -> Result<Response<GetConnectedPeersResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .gossip_tx
            .send(GossipEvent::GetConnectedPeers(tx))
            .await
            .map_err(|err| {
                error!(
                    { err = err.to_string() },
                    "[get_connected_peers] error sending connected peers request"
                );
            });

        match timeout(DEFAULT_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(peers)) => {
                // `contacts` stays back-compatible: COLLECTED entries only. The new
                // `peers` list adds source-tagged DERIVED entries (connected peers
                // with no collected contact info, e.g. validators).
                let contacts = peers
                    .iter()
                    .filter(|p| p.source == proto::ContactSource::Collected as i32)
                    .filter_map(|p| p.contact_info.clone())
                    .collect();
                Ok(Response::new(GetConnectedPeersResponse { contacts, peers }))
            }
            Ok(Err(err)) => {
                error!(
                    { err = err.to_string() },
                    "[get_connected_peers] error receiving connected peers response"
                );
                Err(Status::internal("Unable to retrieve connected peers."))
            }
            Err(_) => {
                error!("[get_connected_peers] timeout receiving connected peers response");
                Err(Status::internal("Unable to retrieve connected peers."))
            }
        }
    }

    async fn get_mesh_view(
        &self,
        request: Request<GetMeshViewRequest>,
    ) -> Result<Response<MeshView>, Status> {
        // Admin-gated diagnostic endpoint. Authenticate BEFORE touching the
        // cache — a cached view must never be served to an unauthenticated
        // caller.
        authenticate_request(&request, &self.admin_allowed_users)?;
        let validators_only = request.into_inner().validators_only;

        self.mesh_cache
            .view(validators_only, || async {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .gossip_tx
                    .send(GossipEvent::GetMeshView(tx))
                    .await
                    .map_err(|err| {
                        error!(
                            { err = err.to_string() },
                            "[get_mesh_view] error sending mesh view request"
                        );
                    });

                match timeout(DEFAULT_REQUEST_TIMEOUT, rx).await {
                    Ok(Ok(view)) => {
                        // Classify peers against the validator set effective at the
                        // current block height (this is the only place public-key ->
                        // validator-set mapping happens).
                        let current_height = self
                            .block_stores
                            .block_store
                            .max_block_number()
                            .unwrap_or(0);
                        let view = classify_mesh_view(
                            view,
                            &self.validator_peer_ids,
                            current_height,
                            validators_only,
                        );
                        Ok(view)
                    }
                    Ok(Err(err)) => {
                        error!(
                            { err = err.to_string() },
                            "[get_mesh_view] error receiving mesh view response"
                        );
                        Err(Status::internal("Unable to retrieve mesh view."))
                    }
                    Err(_) => {
                        error!("[get_mesh_view] timeout receiving mesh view response");
                        Err(Status::internal("Unable to retrieve mesh view."))
                    }
                }
            })
            .await
            .map(Response::new)
    }

    async fn get_mesh_topology(
        &self,
        request: Request<GetMeshViewRequest>,
    ) -> Result<Response<MeshTopology>, Status> {
        // Admin-gated diagnostic endpoint. Authenticate BEFORE touching the
        // cache — a cached topology must never be served to an unauthenticated
        // caller. The cache also single-flights concurrent misses, so a burst of
        // requests triggers only one (expensive) crawl.
        authenticate_request(&request, &self.admin_allowed_users)?;
        let validators_only = request.into_inner().validators_only;

        self.mesh_cache
            .topology(validators_only, || async {
                let current_height = self
                    .block_stores
                    .block_store
                    .max_block_number()
                    .unwrap_or(0);
                let generated_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);

                crawl_mesh(
                    &self.gossip_tx,
                    &self.validator_peer_ids,
                    current_height,
                    validators_only,
                    generated_at,
                )
                .await
                .map_err(|err| {
                    error!({ err = err }, "[get_mesh_topology] crawl failed");
                    Status::internal("Unable to crawl mesh topology.")
                })
            })
            .await
            .map(Response::new)
    }
}
