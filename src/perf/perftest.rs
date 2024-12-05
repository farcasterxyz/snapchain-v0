use crate::proto::onchain_event;
use crate::proto::rpc::hub_service_client::HubServiceClient;
use crate::proto::snapchain::Block;
use crate::storage::store::test_helper;
use crate::utils::cli::{compose_message, follow_blocks, send_message};
use crate::utils::factory::events_factory;
use crate::{
    consensus::proposer::current_time, proto::admin_rpc::admin_service_client::AdminServiceClient,
    utils::cli::compose_rent_event,
};
use crate::{proto::msg as message, utils::cli::send_on_chain_event};
use clap::Parser;
use ed25519_dalek::{SecretKey, SigningKey};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use hex;
use hex::FromHex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;
use std::sync::{atomic, Arc};
use std::time::Duration;
use std::{env, panic, process};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{select, time};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubmitMessageConfig {
    pub rpc_addrs: Vec<String>,

    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FollowBlocksConfig {
    pub rpc_addr: String,
    pub enable: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub submit_message: SubmitMessageConfig,
    pub follow_blocks: FollowBlocksConfig,

    #[serde(with = "humantime_serde")]
    pub stats_calculation_interval: Duration,

    pub num_iterations: u64,
}

#[derive(Parser)]
pub struct CliArgs {
    #[arg(long, help = "Path to the configuration file")]
    config_path: String,
}

pub fn load_and_merge_config(config_path: &str) -> Result<Config, Box<dyn Error>> {
    if !Path::new(config_path).exists() {
        return Err(format!("Config file not found: {}", config_path).into());
    }

    let figment = Figment::new()
        .merge(Toml::file(config_path))
        .merge(Env::prefixed("PERFTEST_").split("__"));

    let config: Config = figment.extract()?;
    Ok(config)
}

fn start_submit_messages(
    messages_tx: mpsc::Sender<message::Message>,
    config: Config,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut submit_message_handles = vec![];
    const FID: u32 = 6833;

    let i = Arc::new(atomic::AtomicU64::new(1));

    for rpc_addr in config.submit_message.rpc_addrs {
        let messages_tx = messages_tx.clone();
        let private_key = SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        );

        let i = i.clone();

        let submit_message_handle = tokio::spawn(async move {
            let mut submit_message_timer = time::interval(config.submit_message.interval);

            println!("connecting to {}", &rpc_addr);
            let mut client = match HubServiceClient::connect(rpc_addr.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    panic!("Error connecting to {}: {}", &rpc_addr, e);
                }
            };

            let mut admin_client = match AdminServiceClient::connect(rpc_addr.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    panic!("Error connecting to {}: {}", &rpc_addr, e);
                }
            };

            let rent_event = compose_rent_event(FID);
            send_on_chain_event(&mut admin_client, rent_event)
                .await
                .unwrap();

            let id_register_event = events_factory::create_id_register_event(
                FID,
                onchain_event::IdRegisterEventType::Register,
            );

            send_on_chain_event(&mut admin_client, id_register_event)
                .await
                .unwrap();

            let signer_event = events_factory::create_signer_event(
                FID,
                test_helper::default_signer(),
                onchain_event::SignerEventType::Add,
            );

            send_on_chain_event(&mut admin_client, signer_event)
                .await
                .unwrap();

            loop {
                submit_message_timer.tick().await;

                let current_i = i.fetch_add(1, atomic::Ordering::SeqCst);
                let text = format!("For benchmarking {}", current_i);

                let msg = compose_message(FID, text.as_str(), None, Some(&private_key));
                let message = send_message(&mut client, &msg).await.unwrap();
                messages_tx.send(message).await.unwrap();
            }
        });

        submit_message_handles.push(submit_message_handle);
    }

    submit_message_handles
}

pub async fn run() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_BACKTRACE", "1");
    panic::set_hook(Box::new(|panic_info| {
        eprintln!("Panic occurred: {}", panic_info);
        let backtrace = std::backtrace::Backtrace::capture();
        eprintln!("Stack trace:\n{}", backtrace);
        process::exit(1);
    }));

    let cli_args = CliArgs::parse();
    let cfg = load_and_merge_config(&cli_args.config_path)?;

    println!("Starting scenario {:#?}", cfg);
    let (blocks_tx, mut blocks_rx) = mpsc::channel::<Block>(10_000_000);

    let (messages_tx, mut messages_rx) = mpsc::channel::<message::Message>(10_000_000);
    let submit_message_handles = start_submit_messages(messages_tx, cfg.clone());

    let follow_blocks_handle = tokio::spawn(async move {
        if !cfg.follow_blocks.enable {
            return;
        }
        let addr = cfg.follow_blocks.rpc_addr.clone();
        follow_blocks(addr, blocks_tx).await.unwrap()
    });

    let start = Instant::now();
    let mut stats_calculation_timer = time::interval(cfg.stats_calculation_interval);
    let start_time = current_time();
    let mut block_count = 0;
    let mut num_messages_confirmed = 0;
    let mut num_messages_submitted = 0;
    let mut pending_messages = HashSet::new();
    let mut time_to_confirmation = vec![];
    let mut block_times = vec![];
    let mut last_block_time = start_time;
    let mut iteration = 0;
    loop {
        select! {
            Some(message) = messages_rx.recv() => {
                num_messages_submitted += 1;
                pending_messages.insert(hex::encode(message.hash));
            },
            Some(block) = blocks_rx.recv() => {
                let block_timestamp = block.header.as_ref().unwrap().timestamp;
                if block_timestamp >= start_time {
                    block_count += 1;
                    block_times.push(block_timestamp - last_block_time);
                    last_block_time = block_timestamp;
                    for chunk in &block.shard_chunks {
                        for tx in &chunk.transactions {
                            for msg in &tx.user_messages {
                                let msg_timestamp = msg.data.as_ref().unwrap().timestamp;
                                time_to_confirmation.push(block_timestamp  - msg_timestamp as u64);
                                num_messages_confirmed += 1;
                                pending_messages.remove(&hex::encode(msg.hash.clone()));
                            }
                        }
                    }
                }
            }
            time = stats_calculation_timer.tick() => {
                let avg_time_to_confirmation = if time_to_confirmation.len() > 0 {time_to_confirmation.iter().sum::<u64>() as f64 / time_to_confirmation.len() as f64} else {0f64};
                let max_time_to_confirmation = if time_to_confirmation.len() > 0 {time_to_confirmation.clone().into_iter().max().unwrap()} else {0};
                let avg_block_time = if block_times.len() > 0 {block_times.iter().sum::<u64>() as f64 / block_times.len() as f64} else {0f64};
                let max_block_time = if block_times.len() > 0 {block_times.clone().into_iter().max().unwrap()} else {0};
                let time_elapsed = time.duration_since(start).as_secs();
                let confirmed_msgs_per_sec = num_messages_confirmed as f64 / cfg.stats_calculation_interval.as_secs_f64();
                let submitted_msgs_per_sec = num_messages_submitted as f64 / cfg.stats_calculation_interval.as_secs_f64();
                println!("{:#?} (secs): Blocks produced: {:#?}, Msgs submitted/sec: {:#?}, Msgs confirmed/sec: {:#?}, Avg time to confirmation (secs): {:#?}, Max time to confirmation (secs): {:#?}, Avg block time (secs): {:#?}, Max block time (secs): {:#?}", time_elapsed, block_count, submitted_msgs_per_sec, confirmed_msgs_per_sec, avg_time_to_confirmation, max_time_to_confirmation, avg_block_time, max_block_time);
                block_count = 0;
                num_messages_confirmed= 0;
                num_messages_submitted = 0;
                time_to_confirmation.clear();
                block_times.clear();
                iteration += 1;

                // 0 iterations means go forever
                if cfg.num_iterations > 0 && iteration > cfg.num_iterations {
                    for submit_message_handle in submit_message_handles {
                        submit_message_handle.abort();
                    }
                    follow_blocks_handle.abort();
                    break Ok(());
                }
            }
        }
    }
}
