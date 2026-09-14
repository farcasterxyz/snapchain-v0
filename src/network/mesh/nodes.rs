//! Known-node registry: maps a node's consensus public key (or, as a fallback,
//! its libp2p peer id) to human-readable metadata — pretty name, operator, role,
//! and a public HTTP API base URL the browser UI can hit for `/v1/info`.
//!
//! The compiled-in [`BUILTIN`] table is transcribed from Neynar's internal
//! "Snapchain" runbook (Notion). Operators can extend or override it via
//! `[[mesh.nodes]]` config entries (see [`crate::network::mesh::config::Config`]).
//!
//! Internal-only addresses (10.x / 172.31.x) are recorded in comments for the
//! human reader but never as `http_api_url` — the browser can't reach them.

use crate::cfg::DEFAULT_HTTP_PORT;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operator {
    Neynar,
    Merkle,
    Community,
    Unknown,
}

impl Operator {
    fn unknown() -> Self {
        Operator::Unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    MainnetValidator,
    MainnetReader,
    TestnetValidator,
    TestnetReader,
    Unknown,
}

impl NodeRole {
    fn unknown() -> Self {
        NodeRole::Unknown
    }
}

/// Metadata for a single known node. Config entries deserialize into this shape;
/// a config entry is matched to a builtin by `consensus_public_key` (preferred)
/// or `peer_id`, and a match replaces the whole builtin entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnownNode {
    pub name: String,
    #[serde(default = "Operator::unknown")]
    pub operator: Operator,
    #[serde(default = "NodeRole::unknown")]
    pub role: NodeRole,
    /// Public HTTP API base URL (no trailing `/v1/...`), e.g.
    /// `http://107.20.169.236:3381`. `None` when the node has no
    /// browser-reachable address.
    #[serde(default)]
    pub http_api_url: Option<String>,
    /// Lowercase hex ed25519 consensus public key. Primary lookup key.
    #[serde(default)]
    pub consensus_public_key: Option<String>,
    /// base58 libp2p peer id. Fallback lookup key.
    #[serde(default)]
    pub peer_id: Option<String>,
    /// Node is known to be offline; suppress the `http_api_url` in output.
    #[serde(default)]
    pub offline: bool,
    /// Free-form operator note (e.g. "demoted to reader 2026-06-23").
    #[serde(default)]
    pub note: Option<String>,
}

/// Which source a resolved HTTP API URL came from (for debuggability in the UI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlSource {
    /// The known-node table.
    Known,
    /// Derived from the observed (live-connection) address.
    Observed,
}

impl UrlSource {
    pub fn as_str(self) -> &'static str {
        match self {
            UrlSource::Known => "known",
            UrlSource::Observed => "observed",
        }
    }
}

/// Compiled-in defaults, mirroring [`KnownNode`] with `&'static str` fields.
struct BuiltinNode {
    name: &'static str,
    operator: Operator,
    role: NodeRole,
    http_api_url: Option<&'static str>,
    consensus_public_key: Option<&'static str>,
    peer_id: Option<&'static str>,
    offline: bool,
    note: Option<&'static str>,
}

impl BuiltinNode {
    fn to_known(&self) -> KnownNode {
        KnownNode {
            name: self.name.to_string(),
            operator: self.operator,
            role: self.role,
            http_api_url: self.http_api_url.map(str::to_string),
            consensus_public_key: self.consensus_public_key.map(str::to_string),
            peer_id: self.peer_id.map(str::to_string),
            offline: self.offline,
            note: self.note.map(str::to_string),
        }
    }
}

/// Known snapchain fleet (mainnet + testnet). Source: internal Notion runbook.
/// Testnet readers carry a node-identity key, not a validator signing key, so
/// they are intentionally peer-id-only (no `consensus_public_key`).
const BUILTIN: &[BuiltinNode] = &[
    // ---- Mainnet ----
    BuiltinNode {
        name: "mordor",
        operator: Operator::Neynar,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("http://107.20.169.236:3381"), // internal 172.31.82.100
        consensus_public_key: Some(
            "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
        ),
        peer_id: Some("12D3KooWCc28TYrrXFivwUshyZ8R5HqPMgx4f7AP54iCDLYr7kFR"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "gondor",
        operator: Operator::Neynar,
        role: NodeRole::MainnetReader,
        http_api_url: Some("http://54.161.182.145:3381"), // internal 172.31.81.161
        consensus_public_key: None,
        peer_id: Some("12D3KooWSFyadP8BZkjhKGMcWVZrvVSxVfXEYip7R8jqeVjherRk"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "rohan",
        operator: Operator::Neynar,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("http://54.157.62.17:3381"), // internal 172.31.85.226
        consensus_public_key: Some(
            "db65769be751f402fe9ea2fdf21679a870ea0e088454bbc47e02c4cc6c258081",
        ),
        peer_id: Some("12D3KooWQaoBw2gvdmfGdXjepEQU9i47FXxvsCZ6wu8Vn4gwvHm2"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "erebor",
        operator: Operator::Neynar,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("http://108.132.114.186:3381"), // eu-west-1; internal 10.54.12.187
        consensus_public_key: Some(
            "81032ecefa4260e5a63424f5a4b8b18b52d717a52583b3ffe22c4a7b084911b8",
        ),
        peer_id: Some("12D3KooWJVyaQRovV1rjV8TzkN3cRiysACyey86kXDLdvf6JRq5Z"),
        offline: false,
        note: Some(
            "eu-west-1; promoted to active validator 2026-06-23 (carries the validator key moved from pop)",
        ),
    },
    BuiltinNode {
        name: "snap",
        operator: Operator::Merkle,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("https://snap.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "6bc2d8901443de856d2670b0c2ea12b6727132fa830f9030d3a44ac5da9b1a72",
        ),
        peer_id: Some("12D3KooWH527dqZTziqzzuXunismeJ3iFVnUxV5spv6VkS7U7zL1"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "crackle",
        operator: Operator::Merkle,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("https://crackle.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "67474a42e0c6507198b73373b0558dfc94616b976ecfdf5c45fae11e2bee7102",
        ),
        peer_id: Some("12D3KooWGmXDC2SfjSG7h7DchyVJHMB4GpA8JYpHf9iwz8L8BFqB"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "pop",
        operator: Operator::Merkle,
        role: NodeRole::MainnetReader,
        http_api_url: Some("https://pop.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "2c8d84020bdf7534551b043549b8b9f1e13fa9189f78fd0c6049c8176599f402",
        ),
        peer_id: Some("12D3KooWCpHGJd3WYMY4cnbP8wQk4AK99J8YQfczLTsa7w4HkGDP"),
        offline: false,
        note: Some(
            "demoted to read node 2026-06-23; validator key moved to erebor, pop reissued with a new node key",
        ),
    },
    BuiltinNode {
        name: "pow",
        operator: Operator::Merkle,
        role: NodeRole::MainnetValidator,
        http_api_url: None,
        consensus_public_key: Some(
            "2c0f58a364b7959c85e49b5a50d14d220c16f8bd7879b0d5d3f68b32de83ecb8",
        ),
        peer_id: Some("12D3KooWCnMgP8BYvwGG5eBfU3gLRnk1bA8BwCkrC3xMtHYp3dRR"),
        offline: true,
        note: Some("offline"),
    },
    BuiltinNode {
        name: "uno",
        operator: Operator::Community,
        role: NodeRole::MainnetValidator,
        http_api_url: Some("https://snap.uno.fun:3381"),
        consensus_public_key: Some(
            "80d7800b45db3ec6d6e4be4d278db1aea1c7a77206941ec976a8680ecbe56860",
        ),
        peer_id: Some("12D3KooWN1tfvjuYaMdkMbtfed6o5TmAnNw1szjaZowmhet8Y4uF"),
        offline: false,
        note: Some("community validator"),
    },
    // ---- Testnet validators (weight 1 each, quorum 4/5) ----
    BuiltinNode {
        name: "iris",
        operator: Operator::Merkle,
        role: NodeRole::TestnetValidator,
        http_api_url: Some("https://iris.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "719a2a8331e05a3c5e2f4689fc71e7eabfea96d79c69df773a6fc8d8962dfda4",
        ),
        peer_id: Some("12D3KooWHTpapWmaNYxPcaWn9Uhoh7TZ67LZcM9dCUMMEwebCe7V"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "juno",
        operator: Operator::Merkle,
        role: NodeRole::TestnetValidator,
        http_api_url: Some("https://juno.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "e89dda4bff3ed5f75f56656a661f9f3e972b7206852dee7bfa65c6cee341e7ae",
        ),
        peer_id: Some("12D3KooWRUQMrmZGXKQvafnqZQBPuAoV1mEFLXvkZwWtgiojhoXB"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "vega",
        operator: Operator::Merkle,
        role: NodeRole::TestnetValidator,
        http_api_url: Some("https://vega.farcaster.xyz:3381"),
        consensus_public_key: Some(
            "5b5eb128729aedd86b626f0d60267f770025a551989c422a8f6959ce0bcf24de",
        ),
        peer_id: Some("12D3KooWFy31r6kuGGuxtAcfPzePpUTtoDo4f5gCJb3A17ximJYd"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "merry",
        operator: Operator::Merkle,
        role: NodeRole::TestnetValidator,
        http_api_url: Some("http://52.71.77.227:3381"),
        consensus_public_key: Some(
            "1694afcc51709e4e2cb94e20bc99f9ea75f5d7ae7eeae66ffc6a350ff1cfd815",
        ),
        peer_id: Some("12D3KooWBLWdvcKWCUyuFtaoFWnNWZXbRNVtF629fLHUrRJboghS"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "gloin",
        operator: Operator::Neynar,
        role: NodeRole::TestnetValidator,
        http_api_url: Some("http://13.205.232.10:3381"), // internal 10.53.5.11
        consensus_public_key: Some(
            "d1facefc03296a24d0d0b1c474e72ca2a84a48199c972d6f37d4722de450a056",
        ),
        peer_id: Some("12D3KooWPx3CCoZR7Fwg5foTkhiZEYpTdYJVbqgmbpDgzarEoKG9"),
        offline: false,
        note: None,
    },
    // ---- Testnet readers (node-identity key, not a validator signing key) ----
    BuiltinNode {
        name: "tau",
        operator: Operator::Merkle,
        role: NodeRole::TestnetReader,
        http_api_url: Some("https://tau.farcaster.xyz:3381"),
        consensus_public_key: None,
        peer_id: Some("12D3KooWP4s56tqsCmaqjuuuS1QA5nvavR9DFrLwNeLzER32NbYA"),
        offline: false,
        note: None,
    },
    BuiltinNode {
        name: "misty",
        operator: Operator::Merkle,
        role: NodeRole::TestnetReader,
        http_api_url: Some("http://32.198.222.236:3381"),
        consensus_public_key: None,
        peer_id: Some("12D3KooWQFn5HaLXuqy5wdsrBy5oeVZT4WVWH8h4rY4XdMhiLf29"),
        offline: false,
        note: None,
    },
];

/// Indexed known-node lookup. Cheap to share behind an `Arc`.
pub struct NodeRegistry {
    by_pubkey: HashMap<String, Arc<KnownNode>>,
    by_peer_id: HashMap<String, Arc<KnownNode>>,
}

impl NodeRegistry {
    /// The compiled-in table only.
    pub fn builtin() -> Self {
        let mut registry = NodeRegistry {
            by_pubkey: HashMap::new(),
            by_peer_id: HashMap::new(),
        };
        for builtin in BUILTIN {
            registry.insert(builtin.to_known());
        }
        registry
    }

    /// The compiled-in table, with config `nodes` merged on top. Each config
    /// entry replaces a builtin matched by pubkey (preferred) or peer id, else
    /// it is appended. Entries with neither key are skipped with a warning.
    pub fn from_config(cfg: &crate::network::mesh::config::Config) -> Self {
        let mut registry = Self::builtin();
        for node in &cfg.nodes {
            if node.consensus_public_key.is_none() && node.peer_id.is_none() {
                warn!(
                    name = node.name,
                    "[mesh] ignoring [[mesh.nodes]] entry with neither consensus_public_key nor peer_id"
                );
                continue;
            }
            registry.remove_existing(node);
            registry.insert(node.clone());
        }
        registry
    }

    /// Look up a node by consensus public key (preferred) or peer id.
    /// `consensus_public_key` is matched case-insensitively.
    pub fn lookup(&self, peer_id: &str, consensus_public_key: Option<&str>) -> Option<&KnownNode> {
        if let Some(pubkey) = consensus_public_key {
            if !pubkey.is_empty() {
                if let Some(node) = self.by_pubkey.get(&pubkey.to_ascii_lowercase()) {
                    return Some(node);
                }
            }
        }
        self.by_peer_id.get(peer_id).map(|n| n.as_ref())
    }

    pub fn len(&self) -> usize {
        // Every entry is indexed by at least one key; peer id is the more common.
        self.by_peer_id
            .values()
            .chain(self.by_pubkey.values())
            .map(|n| Arc::as_ptr(n) as usize)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn insert(&mut self, node: KnownNode) {
        let node = Arc::new(node);
        if let Some(pubkey) = &node.consensus_public_key {
            if !pubkey.is_empty() {
                self.by_pubkey
                    .insert(pubkey.to_ascii_lowercase(), node.clone());
            }
        }
        if let Some(peer_id) = &node.peer_id {
            if !peer_id.is_empty() {
                self.by_peer_id.insert(peer_id.clone(), node.clone());
            }
        }
    }

    /// Drop any existing entry the incoming config node matches, so a whole
    /// entry (both index keys) is replaced rather than partially merged.
    ///
    fn remove_existing(&mut self, node: &KnownNode) {
        let pubkey_match = node
            .consensus_public_key
            .as_ref()
            .filter(|k| !k.is_empty())
            .and_then(|k| self.by_pubkey.get(&k.to_ascii_lowercase()).cloned());
        let peer_id_match = node
            .peer_id
            .as_ref()
            .filter(|p| !p.is_empty())
            .and_then(|p| self.by_peer_id.get(p).cloned());

        for existing in [pubkey_match, peer_id_match].into_iter().flatten() {
            if let Some(pubkey) = &existing.consensus_public_key {
                self.by_pubkey.remove(&pubkey.to_ascii_lowercase());
            }
            if let Some(peer_id) = &existing.peer_id {
                self.by_peer_id.remove(peer_id);
            }
        }
    }
}

/// Resolve a browser-reachable HTTP API base URL for a node, or `None` if we
/// have no address a browser could reach. Precedence:
///   1. the known-node table's `http_api_url` (`None` if the node is offline);
///   2. a URL derived from the observed (live-connection) multiaddr, if public.
///
/// The node's `announce_rpc_address` is intentionally NOT consulted: that field
/// is canonically the gRPC/RPC address (its name, its auto-detected default of
/// `DEFAULT_RPC_PORT`, and `peer_discovery`'s use of it all agree), so it is not
/// a valid source for an HTTP `/v1/info` URL. The known-node table is where a
/// node's real public HTTP URL belongs.
pub fn resolve_http_api_url(
    known: Option<&KnownNode>,
    observed_address: &str,
) -> Option<(String, UrlSource)> {
    if let Some(known) = known {
        if known.offline {
            return None;
        }
        // A non-empty table URL is authoritative; an empty string is treated as
        // "no URL" so we still fall through to the observed-address derivation.
        if let Some(url) = &known.http_api_url {
            if !url.is_empty() {
                return Some((url.clone(), UrlSource::Known));
            }
        }
    }

    if let Some(host) = host_from_multiaddr(observed_address) {
        if !is_private_host(&host) {
            let url = format!("http://{}:{}", bracket_if_ipv6(&host), DEFAULT_HTTP_PORT);
            return Some((url, UrlSource::Observed));
        }
    }

    None
}

/// Extract the host (IP or DNS name) from a libp2p multiaddr string.
fn host_from_multiaddr(addr: &str) -> Option<String> {
    let multiaddr: Multiaddr = addr.parse().ok()?;
    for protocol in multiaddr.iter() {
        use libp2p::multiaddr::Protocol;
        match protocol {
            Protocol::Ip4(ip) => return Some(ip.to_string()),
            Protocol::Ip6(ip) => return Some(ip.to_string()),
            Protocol::Dns(h) | Protocol::Dns4(h) | Protocol::Dns6(h) | Protocol::Dnsaddr(h) => {
                return Some(h.to_string())
            }
            _ => {}
        }
    }
    None
}

/// True if `host` is an IP a browser on the public internet can't reach. DNS
/// names are treated as public (we can't resolve them here).
fn is_private_host(host: &str) -> bool {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => is_private_v4(ip),
        Ok(IpAddr::V6(ip)) => {
            // Dual-stack listeners report IPv4 peers as IPv4-mapped IPv6
            // (`::ffff:a.b.c.d`); classify those by the embedded IPv4 so a
            // private/loopback v4 doesn't slip through as a "public" v6.
            if let Some(v4) = ip.to_ipv4_mapped() {
                return is_private_v4(v4);
            }
            // fc00::/7 unique-local and fe80::/10 link-local (both `is_*` helpers
            // are unstable in std, so match the prefixes directly).
            let is_unique_local = (ip.octets()[0] & 0xfe) == 0xfc;
            let is_link_local = (ip.segments()[0] & 0xffc0) == 0xfe80;
            ip.is_loopback() || ip.is_unspecified() || is_unique_local || is_link_local
        }
        // Not an IP literal — assume it's a resolvable public DNS name.
        Err(_) => false,
    }
}

fn is_private_v4(ip: std::net::Ipv4Addr) -> bool {
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
}

fn bracket_if_ipv6(host: &str) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{}]", host),
        _ => host.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::mesh::config::Config;
    use libp2p::PeerId;
    use std::str::FromStr;

    #[test]
    fn builtin_table_has_unique_keys() {
        let mut pubkeys = std::collections::HashSet::new();
        let mut peer_ids = std::collections::HashSet::new();
        for node in BUILTIN {
            if let Some(pk) = node.consensus_public_key {
                assert!(
                    pubkeys.insert(pk),
                    "duplicate consensus_public_key in BUILTIN: {pk} ({})",
                    node.name
                );
            }
            if let Some(pid) = node.peer_id {
                assert!(
                    peer_ids.insert(pid),
                    "duplicate peer_id in BUILTIN: {pid} ({})",
                    node.name
                );
            }
        }
    }

    #[test]
    fn builtin_pubkeys_are_32_byte_hex() {
        for node in BUILTIN {
            if let Some(pk) = node.consensus_public_key {
                assert_eq!(pk.len(), 64, "{}: pubkey not 64 hex chars", node.name);
                assert!(
                    pk.chars()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                    "{}: pubkey not lowercase hex",
                    node.name
                );
            }
        }
    }

    #[test]
    fn builtin_peer_ids_parse() {
        for node in BUILTIN {
            let pid = node.peer_id.expect("every builtin has a peer id");
            assert!(PeerId::from_str(pid).is_ok(), "{}: bad peer id", node.name);
        }
    }

    #[test]
    fn lookup_prefers_pubkey_over_peer_id() {
        // mordor's pubkey but gondor's peer id — pubkey must win.
        let node = NodeRegistry::builtin()
            .lookup(
                "12D3KooWSFyadP8BZkjhKGMcWVZrvVSxVfXEYip7R8jqeVjherRk", // gondor
                Some("29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970"), // mordor
            )
            .cloned();
        assert_eq!(node.unwrap().name, "mordor");
    }

    #[test]
    fn lookup_is_case_insensitive_on_pubkey() {
        let node = NodeRegistry::builtin()
            .lookup(
                "unknown-peer",
                Some("29696EB40EB900A329A8D2542EDEF15D552C9BA6DED7882276BE1E9ECA090970"),
            )
            .cloned();
        assert_eq!(node.unwrap().name, "mordor");
    }

    #[test]
    fn lookup_falls_back_to_peer_id() {
        // gondor has no pubkey; resolves by peer id only.
        let node = NodeRegistry::builtin()
            .lookup("12D3KooWSFyadP8BZkjhKGMcWVZrvVSxVfXEYip7R8jqeVjherRk", None)
            .cloned();
        assert_eq!(node.unwrap().name, "gondor");
    }

    #[test]
    fn lookup_unknown_returns_none() {
        assert!(NodeRegistry::builtin()
            .lookup("12D3KooWunknown", Some("deadbeef"))
            .is_none());
    }

    fn cfg_with(nodes: Vec<KnownNode>) -> Config {
        Config {
            cache_ttl_secs: 5,
            nodes,
        }
    }

    #[test]
    fn config_override_replaces_matching_entry() {
        let cfg = cfg_with(vec![KnownNode {
            name: "mordor-renamed".to_string(),
            operator: Operator::Neynar,
            role: NodeRole::MainnetValidator,
            http_api_url: Some("http://10.0.0.9:3381".to_string()),
            consensus_public_key: Some(
                "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970".to_string(),
            ),
            peer_id: None,
            offline: false,
            note: None,
        }]);
        let registry = NodeRegistry::from_config(&cfg);
        let node = registry
            .lookup(
                "",
                Some("29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970"),
            )
            .unwrap();
        assert_eq!(node.name, "mordor-renamed");
        // The builtin peer-id index entry for mordor is gone (whole-entry replace).
        assert!(registry
            .lookup("12D3KooWCc28TYrrXFivwUshyZ8R5HqPMgx4f7AP54iCDLYr7kFR", None)
            .is_none());
    }

    #[test]
    fn config_override_removes_distinct_pubkey_and_peer_id_matches() {
        let cfg = cfg_with(vec![KnownNode {
            name: "combined".to_string(),
            operator: Operator::Community,
            role: NodeRole::MainnetValidator,
            http_api_url: None,
            // Mordor's public key and Rohan's peer id intentionally match two
            // different builtin entries.
            consensus_public_key: Some(
                "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970".to_string(),
            ),
            peer_id: Some(
                "12D3KooWQaoBw2gvdmfGdXjepEQU9i47FXxvsCZ6wu8Vn4gwvHm2".to_string(),
            ),
            offline: false,
            note: None,
        }]);

        let registry = NodeRegistry::from_config(&cfg);

        assert_eq!(registry.len(), BUILTIN.len() - 1);
        assert_eq!(
            registry
                .lookup(
                    "",
                    Some(
                        "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970"
                    )
                )
                .unwrap()
                .name,
            "combined"
        );
        assert_eq!(
            registry
                .lookup(
                    "12D3KooWQaoBw2gvdmfGdXjepEQU9i47FXxvsCZ6wu8Vn4gwvHm2",
                    None
                )
                .unwrap()
                .name,
            "combined"
        );
        // Both of the original nodes' other index keys must be gone.
        assert!(registry
            .lookup("12D3KooWCc28TYrrXFivwUshyZ8R5HqPMgx4f7AP54iCDLYr7kFR", None)
            .is_none());
        assert!(registry
            .lookup(
                "",
                Some("db65769be751f402fe9ea2fdf21679a870ea0e088454bbc47e02c4cc6c258081")
            )
            .is_none());
    }

    #[test]
    fn config_override_appends_unknown_entry() {
        let cfg = cfg_with(vec![KnownNode {
            name: "newcomer".to_string(),
            operator: Operator::Community,
            role: NodeRole::MainnetValidator,
            http_api_url: None,
            consensus_public_key: Some("a".repeat(64)),
            peer_id: Some("12D3KooWNewcomerabcdefghijklmnopqrstuvwxyz00000".to_string()),
            offline: false,
            note: None,
        }]);
        let registry = NodeRegistry::from_config(&cfg);
        assert_eq!(registry.len(), BUILTIN.len() + 1);
        assert_eq!(
            registry.lookup("", Some(&"a".repeat(64))).unwrap().name,
            "newcomer"
        );
    }

    #[test]
    fn config_entry_without_keys_is_ignored() {
        let cfg = cfg_with(vec![KnownNode {
            name: "keyless".to_string(),
            operator: Operator::Unknown,
            role: NodeRole::Unknown,
            http_api_url: None,
            consensus_public_key: None,
            peer_id: None,
            offline: false,
            note: None,
        }]);
        let registry = NodeRegistry::from_config(&cfg);
        assert_eq!(registry.len(), BUILTIN.len());
    }

    #[test]
    fn resolve_url_prefers_known_table() {
        let known = NodeRegistry::builtin()
            .lookup(
                "",
                Some("29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970"),
            )
            .cloned()
            .unwrap();
        // Known table wins over any observed-address derivation.
        let (url, source) = resolve_http_api_url(Some(&known), "/ip4/5.6.7.8/tcp/3382").unwrap();
        assert_eq!(url, "http://107.20.169.236:3381");
        assert_eq!(source, UrlSource::Known);
    }

    #[test]
    fn resolve_url_offline_known_node_is_none() {
        let known = NodeRegistry::builtin()
            .lookup(
                "",
                Some("2c0f58a364b7959c85e49b5a50d14d220c16f8bd7879b0d5d3f68b32de83ecb8"),
            ) // pow
            .cloned()
            .unwrap();
        assert!(resolve_http_api_url(Some(&known), "/ip4/9.9.9.9/tcp/3382").is_none());
    }

    #[test]
    fn resolve_url_empty_known_url_falls_through_to_observed() {
        // A table entry with an empty http_api_url is not authoritative; the
        // observed public address should still yield a derived URL.
        let known = KnownNode {
            name: "blank".to_string(),
            operator: Operator::Unknown,
            role: NodeRole::Unknown,
            http_api_url: Some(String::new()),
            consensus_public_key: None,
            peer_id: None,
            offline: false,
            note: None,
        };
        let (url, source) =
            resolve_http_api_url(Some(&known), "/ip4/198.51.100.7/tcp/3382").unwrap();
        assert_eq!(url, "http://198.51.100.7:3381");
        assert_eq!(source, UrlSource::Observed);
    }

    #[test]
    fn resolve_url_derives_from_public_observed_address() {
        // Best-effort: assumes the default HTTP port for an unknown public node.
        let (url, source) = resolve_http_api_url(None, "/ip4/198.51.100.7/tcp/3382").unwrap();
        assert_eq!(url, "http://198.51.100.7:3381");
        assert_eq!(source, UrlSource::Observed);
    }

    #[test]
    fn resolve_url_omits_private_observed_address() {
        assert!(resolve_http_api_url(None, "/ip4/172.31.82.100/tcp/3382").is_none());
        assert!(resolve_http_api_url(None, "/ip4/10.54.12.187/tcp/3382").is_none());
    }

    #[test]
    fn resolve_url_brackets_ipv6() {
        let (url, _) = resolve_http_api_url(None, "/ip6/2001:db8::1/tcp/3382").unwrap();
        assert_eq!(url, "http://[2001:db8::1]:3381");
    }

    #[test]
    fn is_private_host_classifies_ranges() {
        // Private / unreachable.
        for h in [
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "127.0.0.1",
            "169.254.1.1",
            "0.0.0.0",
            "::1",
            "fc00::1",
            "fd12::1",
            "fe80::1",           // IPv6 link-local
            "febf::1",           // top of fe80::/10
            "::ffff:10.0.0.1",   // IPv4-mapped private
            "::ffff:127.0.0.1",  // IPv4-mapped loopback
            "::ffff:172.31.0.1", // IPv4-mapped private
        ] {
            assert!(is_private_host(h), "{h} should be private");
        }
        // Public — note the /12 boundary counterexamples.
        for h in [
            "8.8.8.8",
            "172.15.0.1",
            "172.32.0.1",
            "203.0.113.1",
            "2001:db8::1",
            "fec0::1",        // deprecated site-local, just past fe80::/10 — not link-local
            "::ffff:8.8.8.8", // IPv4-mapped public
            "snap.farcaster.xyz",
        ] {
            assert!(!is_private_host(h), "{h} should be public");
        }
    }
}
