use std::collections::HashMap;

use alloy_primitives::{address, Address, FixedBytes, Uint};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use alloy_sol_types::{sol, SolEvent};
use alloy_transport_http::{Client, Http};
use async_trait::async_trait;
use foundry_common::ens::EnsError;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info};

use crate::{
    mempool::routing::MessageRouter,
    proto::{
        on_chain_event, IdRegisterEventBody, IdRegisterEventType, OnChainEvent, OnChainEventType,
        SignerEventBody, SignerEventType, SignerMigratedEventBody, StorageRentEventBody,
        ValidatorMessage,
    },
    storage::store::engine::{MempoolMessage, Senders},
};

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

static CHAIN_ID: u32 = 10; // OP mainnet
const RENT_EXPIRY_IN_SECONDS: u64 = 365 * 24 * 60 * 60; // One year
const FIRST_BLOCK: u64 = 108864739;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub start_block_number: u64,
    pub stop_block_number: u64,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            rpc_url: String::new(),
            start_block_number: FIRST_BLOCK,
            stop_block_number: FIRST_BLOCK,
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

    #[error("Empty rpc url")]
    EmptyRpcUrl,

    #[error("Log missing block hash")]
    LogMissingBlockHash,

    #[error("Log missing log index")]
    LogMissingLogIndex,

    #[error("Log missing block number")]
    LogMissingBlockNumber,

    #[error("Log missing tx index")]
    LogMissingTxIndex,

    #[error("Log missing tx hash")]
    LogMissingTransactionHash,

    #[error("Unable to find block by hash")]
    UnableToFindBlockByHash,
}

#[async_trait]
pub trait L1Client: Send + Sync {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError>;
}

pub struct RealL1Client {
    provider: RootProvider<Http<Client>>,
}

impl RealL1Client {
    pub fn new(rpc_url: String) -> Result<RealL1Client, SubscribeError> {
        if rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        Ok(RealL1Client { provider })
    }
}

#[async_trait]
impl L1Client for RealL1Client {
    async fn resolve_ens_name(&self, name: String) -> Result<Address, EnsError> {
        foundry_common::ens::NameOrAddress::Name(name)
            .resolve(&self.provider)
            .await
    }
}

pub struct Subscriber {
    provider: RootProvider<Http<Client>>,
    onchain_events_by_block: HashMap<u32, Vec<OnChainEvent>>,
    shard_senders: HashMap<u32, Senders>,
    message_router: Box<dyn MessageRouter>,
    num_shards: u32,
    start_block_number: u64,
    stop_block_number: u64,
}

// TODO(aditi): Wait for 1 confirmation before "committing" an onchain event.
impl Subscriber {
    pub fn new(
        config: Config,
        shard_senders: HashMap<u32, Senders>,
        num_shards: u32,
        message_router: Box<dyn MessageRouter>,
    ) -> Result<Subscriber, SubscribeError> {
        if config.rpc_url.is_empty() {
            return Err(SubscribeError::EmptyRpcUrl);
        }
        let url = config.rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        Ok(Subscriber {
            provider,
            onchain_events_by_block: HashMap::new(),
            shard_senders,
            num_shards,
            message_router,
            start_block_number: config.start_block_number,
            stop_block_number: config.stop_block_number,
        })
    }

    async fn add_onchain_event(
        &mut self,
        fid: u64,
        block_number: u32,
        block_hash: FixedBytes<32>,
        block_timestamp: u64,
        log_index: u32,
        tx_index: u32,
        transaction_hash: FixedBytes<32>,
        event_type: OnChainEventType,
        event_body: on_chain_event::Body,
    ) {
        let event = OnChainEvent {
            fid,
            block_number,
            block_hash: block_hash.to_vec(),
            block_timestamp,
            log_index,
            tx_index,
            r#type: event_type as i32,
            chain_id: CHAIN_ID,
            version: 0,
            body: Some(event_body),
            transaction_hash: transaction_hash.to_vec(),
        };
        info!(
            fid,
            event_type = event_type.as_str_name(),
            block_number = event.block_number,
            block_timestamp = event.block_timestamp,
            tx_hash = hex::encode(&event.transaction_hash),
            log_index = event.log_index,
            "Processed onchain event"
        );
        let events = self.onchain_events_by_block.get_mut(&block_number);
        match events {
            None => {
                self.onchain_events_by_block
                    .insert(block_number, vec![event.clone()]);
            }
            Some(events) => events.push(event.clone()),
        }
        let shard = self.message_router.route_message(fid, self.num_shards);
        let senders = self.shard_senders.get(&shard);
        match senders {
            None => {
                error!(
                    block_number = event.block_number,
                    tx_hash = hex::encode(&event.transaction_hash),
                    log_index = event.log_index,
                    "Unable to find shard to send onchain event to"
                )
            }
            Some(senders) => {
                if let Err(err) = senders
                    .messages_tx
                    .send(MempoolMessage::ValidatorMessage(ValidatorMessage {
                        on_chain_event: Some(event.clone()),
                        fname_transfer: None,
                    }))
                    .await
                {
                    error!(
                        block_number = event.block_number,
                        tx_hash = hex::encode(&event.transaction_hash),
                        log_index = event.log_index,
                        err = err.to_string(),
                        "Unable to send onchain event to mempool"
                    )
                }
            }
        }
    }

    async fn get_block_timestamp(&self, block_hash: FixedBytes<32>) -> Result<u64, SubscribeError> {
        let block = self
            .provider
            .get_block_by_hash(block_hash, alloy_rpc_types::BlockTransactionsKind::Hashes)
            .await?
            .ok_or(SubscribeError::UnableToFindBlockByHash)?;
        Ok(block.header.timestamp)
    }

    async fn process_log(&mut self, event: &Log) -> Result<(), SubscribeError> {
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
        let transaction_hash = event
            .transaction_hash
            .ok_or(SubscribeError::LogMissingTransactionHash)?;
        // TODO(aditi): Cache these queries for timestamp to optimize rpc calls.
        // [block_timestamp] exists on [Log], however it's never populated in practice.
        let block_timestamp = self.get_block_timestamp(block_hash).await?;
        let add_event = |fid, event_type, event_body| async move {
            self.add_onchain_event(
                fid,
                block_number as u32,
                block_hash,
                block_timestamp,
                log_index as u32,
                tx_index as u32,
                transaction_hash,
                event_type,
                event_body,
            )
            .await;
        };
        match event.topic0() {
            Some(&StorageRegistryAbi::Rent::SIGNATURE_HASH) => {
                let StorageRegistryAbi::Rent { payer, fid, units } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&fid);
                add_event(
                    fid,
                    OnChainEventType::EventTypeStorageRent,
                    on_chain_event::Body::StorageRentEventBody(StorageRentEventBody {
                        payer: payer.to_vec(),
                        units: Uint::to::<u32>(&units),
                        expiry: (block_timestamp + RENT_EXPIRY_IN_SECONDS) as u32,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&IdRegistryAbi::Register::SIGNATURE_HASH) => {
                let IdRegistryAbi::Register { to, id, recovery } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    OnChainEventType::EventTypeIdRegister,
                    on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                        event_type: IdRegisterEventType::Register as i32,
                        to: to.to_vec(),
                        recovery_address: recovery.to_vec(),
                        from: vec![], // TODO(aditi) : What to do about this?
                    }),
                )
                .await;
                Ok(())
            }
            Some(&IdRegistryAbi::Transfer::SIGNATURE_HASH) => {
                let IdRegistryAbi::Transfer { from, to, id } = event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    OnChainEventType::EventTypeIdRegister,
                    on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                        event_type: IdRegisterEventType::Transfer as i32,
                        to: to.to_vec(),
                        from: from.to_vec(),
                        recovery_address: vec![],
                    }),
                )
                .await;
                Ok(())
            }
            Some(&IdRegistryAbi::ChangeRecoveryAddress::SIGNATURE_HASH) => {
                let IdRegistryAbi::ChangeRecoveryAddress { id, recovery } =
                    event.log_decode()?.inner.data;
                let fid = Uint::to::<u64>(&id);
                add_event(
                    fid,
                    OnChainEventType::EventTypeIdRegister,
                    on_chain_event::Body::IdRegisterEventBody(IdRegisterEventBody {
                        event_type: IdRegisterEventType::ChangeRecovery as i32,
                        to: vec![],
                        from: vec![],
                        recovery_address: recovery.to_vec(),
                    }),
                )
                .await;
                Ok(())
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
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: keytype,
                        event_type: SignerEventType::Add as i32,
                        metadata: metadata.to_vec(),
                        metadata_type: metadatatype as u32,
                    }),
                )
                .await;
                Ok(())
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
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: 0,
                        event_type: SignerEventType::Remove as i32,
                        metadata: vec![],
                        metadata_type: 0,
                    }),
                )
                .await;
                Ok(())
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
                    OnChainEventType::EventTypeSigner,
                    on_chain_event::Body::SignerEventBody(SignerEventBody {
                        key: keyBytes.to_vec(),
                        key_type: 0,
                        event_type: SignerEventType::AdminReset as i32,
                        metadata: vec![],
                        metadata_type: 0,
                    }),
                )
                .await;
                Ok(())
            }
            Some(&KeyRegistryAbi::Migrated::SIGNATURE_HASH) => {
                let KeyRegistryAbi::Migrated { keysMigratedAt } = event.log_decode()?.inner.data;
                let migrated_at = Uint::to::<u32>(&keysMigratedAt);
                add_event(
                    0,
                    OnChainEventType::EventTypeSignerMigrated,
                    on_chain_event::Body::SignerMigratedEventBody(SignerMigratedEventBody {
                        migrated_at,
                    }),
                )
                .await;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub async fn sync_historical_events(&mut self, address: Address) -> Result<(), SubscribeError> {
        let batch_size = 1000;
        let mut start_block = self.start_block_number;
        loop {
            let stop_block = self.stop_block_number.min(start_block + batch_size);
            let filter = Filter::new()
                .address(address)
                .from_block(start_block)
                .to_block(stop_block);
            info!(
                start_block,
                stop_block, "Syncing historical events in range"
            );
            let events = self.provider.get_logs(&filter).await?;
            for event in events {
                let result = self.process_log(&event).await;
                match result {
                    Err(err) => {
                        error!(
                            "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                            err, event,
                        )
                    }
                    Ok(_) => {}
                }
            }
            start_block += batch_size;
            if start_block > self.stop_block_number {
                info!(
                    start_block,
                    stop_block = self.stop_block_number,
                    "Stopping onchain events sync"
                );
                return Ok(());
            }
        }
    }

    pub async fn run(&mut self, sync_live_events: bool) -> Result<(), SubscribeError> {
        info!(
            start_block_number = self.start_block_number,
            stop_block_numer = self.stop_block_number,
            "Starting l2 events subscription"
        );
        self.sync_historical_events(STORAGE_REGISTRY).await?;
        self.sync_historical_events(ID_REGISTRY).await?;
        self.sync_historical_events(KEY_REGISTRY).await?;
        // TODO (aditi): [sync_live_events] should go away. We should automatically do live sync and figure out where to stop historical sync
        if sync_live_events {
            // Subscribe to new events starting from now.
            let filter = Filter::new()
                .address(vec![STORAGE_REGISTRY, KEY_REGISTRY, ID_REGISTRY])
                .from_block(self.start_block_number);
            let subscription = self.provider.watch_logs(&filter).await?;
            let mut stream = subscription.into_stream();
            while let Some(events) = stream.next().await {
                for event in events {
                    let result = self.process_log(&event).await;
                    match result {
                        Err(err) => {
                            error!(
                                "Error processing onchain event. Error: {:#?}. Event: {:#?}",
                                err, event,
                            )
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
        Ok(())
    }
}
