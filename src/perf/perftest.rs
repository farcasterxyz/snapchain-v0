use crate::perf::gen_single::SingleUser;
use crate::perf::generate::{new_generator, GeneratorTypes};
use crate::perf::{gen_single, generate};
use crate::proto;
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::Block;
use crate::utils::cli::follow_blocks;
use crate::utils::cli::send_on_chain_event;
use crate::{consensus::proposer::current_time, proto::admin_service_client::AdminServiceClient};
use clap::Parser;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use hex;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
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
    messages_tx: mpsc::Sender<proto::Message>,
    config: Config,
    gen_type: GeneratorTypes,
) -> Vec<tokio::task::JoinHandle<()>> {
    gen_single::assert_send::<SingleUser>();

    let mut submit_message_handles = vec![];

    let seq = Arc::new(atomic::AtomicU64::new(0));

    for (index, rpc_addr) in config.submit_message.rpc_addrs.into_iter().enumerate() {
        let thread_id = (index + 1) as u32;
        let messages_tx = messages_tx.clone();
        let seq = seq.clone();
        let gen_type = gen_type.clone();

        submit_message_handles.push(tokio::spawn(async move {
            let mut generator = new_generator(
                gen_type,
                thread_id,
                generate::Config {
                    users_per_shard: 5000,
                },
            );

            let mut submit_message_timer = time::interval(config.submit_message.interval);

            println!("connecting to {}", &rpc_addr);
            let mut client = HubServiceClient::connect(rpc_addr.clone())
                .await
                .unwrap_or_else(|e| panic!("Error connecting to {}: {}", &rpc_addr, e));

            let mut admin_client = AdminServiceClient::connect(rpc_addr.clone())
                .await
                .unwrap_or_else(|e| panic!("Error connecting to {}: {}", &rpc_addr, e));

            let mut message_queue: VecDeque<generate::NextMessage> = VecDeque::new();
            loop {
                submit_message_timer.tick().await;
                let seq = seq.fetch_add(1, atomic::Ordering::SeqCst);

                if message_queue.is_empty() {
                    let msgs = generator.next(seq);
                    message_queue.extend(msgs);
                }

                let msg = message_queue.front().expect("message queue was empty");

                match msg {
                    generate::NextMessage::Message(message) => {
                        let response = client
                            .submit_message_with_options(proto::SubmitMessageRequest {
                                message: Some(message.clone()),
                                bypass_validation: Some(true),
                            })
                            .await;

                        match response {
                            Ok(resp) => {
                                let sent = resp.into_inner().message.unwrap();
                                messages_tx.send(sent).await.unwrap();
                                message_queue.pop_front(); // Remove message only if successfully sent
                            }
                            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                                // TODO: emit metrics
                            }
                            Err(e) => {
                                panic!("Unexpected error: {:?}", e); // Handle other errors as needed
                            }
                        }
                    }
                    generate::NextMessage::OnChainEvent(event) => {
                        if let Err(e) = send_on_chain_event(&mut admin_client, &event).await {
                            panic!("Failed to send on-chain event: {:?}", e);
                        } else {
                            message_queue.pop_front(); // Remove event if successfully sent
                        }
                    }
                }
            }
        }));
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

    let gen_type = GeneratorTypes::MultiUser;

    let cli_args = CliArgs::parse();
    let cfg = load_and_merge_config(&cli_args.config_path)?;

    println!("Starting scenario {:#?}", cfg);
    let (blocks_tx, mut blocks_rx) = mpsc::channel::<Block>(10_000_000);

    let (messages_tx, mut messages_rx) = mpsc::channel::<proto::Message>(10_000_000);

    let submit_message_handles = start_submit_messages(messages_tx, cfg.clone(), gen_type);

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
