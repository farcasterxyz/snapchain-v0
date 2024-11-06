use std::collections::HashMap;

use alloy::{
    primitives::{address, Address, Bytes, FixedBytes, Uint},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
    transports::http::{Client, Http},
};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error};

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    StorageRegistryAbi,
    "src/connectors/onchain_events/storage_registry_abi.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    IdRegistryAbi,
    "src/connectors/onchain_events/id_registry_abi.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    KeyRegistryAbi,
    "src/connectors/onchain_events/key_registry_abi.json"
);

static STORAGE_REGISTRY: Address = address!("00000000fcce7f938e7ae6d3c335bd6a1a7c593d");

static KEY_REGISTRY: Address = address!("00000000Fc1237824fb747aBDE0FF18990E59b7e");

static ID_REGISTRY: Address = address!("00000000Fc6c5F01Fc30151999387Bb99A9f489b");

static CHAIN_ID: u64 = 10;
const RENT_EXPIRY_IN_SECONDS: u64 = 365 * 24 * 60 * 60; // One year

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub disable: bool,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: "must specify rpc url in config".to_string(),
            disable: false,
        };
    }
}

#[derive(Error, Debug)]
pub enum SubscribeError {
    #[error(transparent)]
    UnableToSubscribe(#[from] alloy_transport::TransportError),

    #[error(transparent)]
    UnableToParseUrl(#[from] url::ParseError),

    #[error(transparent)]
    UnableToParseLog(#[from] alloy_sol_types::Error),

    #[error("Log missing block hash")]
    LogMissingBlockHash,

    #[error("Log missing log index")]
    LogMissingLogIndex,

    #[error("Log missing block number")]
    LogMissingBlockNumber,

    #[error("Log missing tx index")]
    LogMissingTxIndex,

    #[error("Unable to find block by hash")]
    UnableToFindBlockByHash,
}

#[derive(Debug)]
pub enum SignerEvent {
    Add {
        key: Bytes,
        key_type: u32,
        metadata: Bytes,
        metadata_type: u8,
    },
    Remove {
        key: Bytes,
    },
    AdminReset {
        key: Bytes,
    },
}

#[derive(Debug)]
pub struct SignerMigratedEvent {
    migrated_at: u64,
}

#[derive(Debug)]
pub enum IdRegisterEvent {
    Register {
        to: Address,
        recovery_address: Address,
    },
    Transfer {
        from: Address,
        to: Address,
    },
    ChangeRecovery {
        recovery_address: Address,
    },
}

#[derive(Debug)]
pub struct StorageRentEvent {
    payer: Address,
    units: u64,
    expiry: u64,
}

#[derive(Debug)]
pub enum EventType {
    Signer(SignerEvent),
    SignerMigrated { migrated_at: u64 },
    IdRegister(IdRegisterEvent),
    StorageRent(StorageRentEvent),
}

#[derive(Debug)]
pub struct Event {
    chain_id: u64,
    block_number: u64,
    block_hash: FixedBytes<32>,
    block_timestamp: u64,
    log_index: u64,
    fid: u64,
    tx_index: u64,
    version: u64,
    event_type: EventType,
}

pub struct Subscriber {
    provider: RootProvider<Http<Client>>,
    onchain_events_by_block: HashMap<u64, Vec<Event>>,
}

// TODO(aditi): Wait for 1 confirmation before "committing" an onchain event.
impl Subscriber {
    pub fn new(config: Config) -> Result<Subscriber, SubscribeError> {
        let url = config.rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        return Ok(Subscriber {
            provider,
            onchain_events_by_block: HashMap::new(),
        });
    }

    pub fn add_onchain_event(
        &mut self,
        fid: u64,
        block_number: u64,
        block_hash: FixedBytes<32>,
        block_timestamp: u64,
        log_index: u64,
        tx_index: u64,
        event_type: EventType,
    ) {
        let event = Event {
            fid,
            block_number,
            block_hash,
            block_timestamp,
            log_index,
            tx_index,
            event_type,
            chain_id: CHAIN_ID,
            version: 0,
        };
        let events = self.onchain_events_by_block.get_mut(&block_number);
        match events {
            None => {
                self.onchain_events_by_block
                    .insert(block_number, vec![event]);
            }
            Some(events) => events.push(event),
        }
    }

    pub async fn get_block_timestamp(
        &self,
        block_hash: FixedBytes<32>,
    ) -> Result<u64, SubscribeError> {
        let block = self
            .provider
            .get_block_by_hash(block_hash, alloy::rpc::types::BlockTransactionsKind::Hashes)
            .await?
            .ok_or(SubscribeError::UnableToFindBlockByHash)?;
        return Ok(block.header.timestamp);
    }

    pub async fn process_log(&mut self, event: &Log) -> Result<(), SubscribeError> {
        let block_hash = event
            .block_hash
            .ok_or(SubscribeError::LogMissingBlockHash)?;
        let log_index = event.log_index.ok_or(SubscribeError::LogMissingLogIndex)?;
        let block_number = event
            .block_number
            .ok_or(SubscribeError::LogMissingBlockNumber)?;
        let tx_index = event
            .transaction_index
            .ok_or(SubscribeError::LogMissingTxIndex)?;
        // TODO(aditi): Cache these queries for timestamp to optimize rpc calls.
        let block_timestamp = self.get_block_timestamp(block_hash).await?;
        let mut add_event = |fid, event_type| {
            self.add_onchain_event(
                fid,
                block_number,
                block_hash,
                block_timestamp,
                log_index,
                tx_index,
                event_type,
            );
        };
        match event.topic0() {
            Some(&StorageRegistryAbi::Rent::SIGNATURE_HASH) => {
                let StorageRegistryAbi::Rent { payer, fid, units } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&fid);
                add_event(
                    fid,
                    EventType::StorageRent(StorageRentEvent {
                        payer,
                        units: Uint::to::<u64>(&units),
                        expiry: block_timestamp + RENT_EXPIRY_IN_SECONDS,
                    }),
                );
                return Ok(());
            }
            Some(&IdRegistryAbi::Register::SIGNATURE_HASH) => {
                let IdRegistryAbi::Register { to, id, recovery } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    EventType::IdRegister(IdRegisterEvent::Register {
                        to,
                        recovery_address: recovery,
                    }),
                );
                return Ok(());
            }
            Some(&IdRegistryAbi::Transfer::SIGNATURE_HASH) => {
                let IdRegistryAbi::Transfer { from, to, id } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    EventType::IdRegister(IdRegisterEvent::Transfer { to, from }),
                );
                return Ok(());
            }
            Some(&IdRegistryAbi::ChangeRecoveryAddress::SIGNATURE_HASH) => {
                let IdRegistryAbi::ChangeRecoveryAddress { id, recovery } =
                    event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    EventType::IdRegister(IdRegisterEvent::ChangeRecovery {
                        recovery_address: recovery,
                    }),
                );
                return Ok(());
            }
            Some(&KeyRegistryAbi::Add::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Add {
                    fid,
                    key: _,
                    keytype,
                    keyBytes,
                    metadatatype,
                    metadata,
                } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&fid);
                add_event(
                    fid,
                    EventType::Signer(SignerEvent::Add {
                        key: keyBytes,
                        key_type: keytype,
                        metadata,
                        metadata_type: metadatatype,
                    }),
                );
                return Ok(());
            }
            Some(&KeyRegistryAbi::Remove::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Remove {
                    fid,
                    key: _,
                    keyBytes,
                } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&fid);
                add_event(
                    fid,
                    EventType::Signer(SignerEvent::Remove { key: keyBytes }),
                );
                return Ok(());
            }
            Some(&KeyRegistryAbi::AdminReset::SIGNATURE_HASH) => {
                let KeyRegistryAbi::AdminReset {
                    fid,
                    key: _,
                    keyBytes,
                } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&fid);
                add_event(
                    fid,
                    EventType::Signer(SignerEvent::AdminReset { key: keyBytes }),
                );
                return Ok(());
            }
            Some(&KeyRegistryAbi::Migrated::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Migrated { keysMigratedAt } = event.log_decode()?.inner.data;
                let migrated_at = Uint::to::<u64>(&keysMigratedAt);
                add_event(0, EventType::SignerMigrated { migrated_at });
                return Ok(());
            }
            _ => return Ok(()),
        }
    }

    pub async fn run(&mut self) -> Result<(), SubscribeError> {
        // Subscribe to new events starting from now.
        let filter = Filter::new().address(vec![STORAGE_REGISTRY, KEY_REGISTRY, ID_REGISTRY]);
        let subscription = self.provider.watch_logs(&filter).await?;
        let mut stream = subscription.into_stream();
        while let Some(events) = stream.next().await {
            for event in events {
                let result = self.process_log(&event).await;
                if result.is_err() {
                    error!(
                        "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                        result.err(),
                        event,
                    )
                } else {
                    debug!("Processed onchain event {:#?}", event);
                }
            }
        }
        return Ok(());
    }
}
