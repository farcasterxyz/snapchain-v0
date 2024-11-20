use ed25519_dalek::{SecretKey, SigningKey};
use hex::{self, FromHex};
use message::MessageData;
use prost::Message;
use snapchain::consensus::proposer::current_time;
use snapchain::proto::message;
use snapchain::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use snapchain::proto::snapchain::Block;
use snapchain::utils::cli::{compose_message, follow_blocks, send_message};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tokio::select;

const STATS_CALCULATION_INTERVAL: Duration = Duration::from_secs(10);
const NUM_ITERATIONS: u64 = 5;

#[derive(Debug, Clone)]
struct Scenario {
    submit_message_rpc_addrs: Vec<String>,
    follow_blocks_rpc_addr: String,
    submit_message_interval: Option<Duration>,
}

fn create_signing_key() -> Result<SigningKey, String> {
    let secret_key_bytes = hex::decode(
        "1000000000000000000000000000000000000000000000000000000000000000",
    ).map_err(|e| format!("Failed to decode secret key: {}", e))?;
    
    let secret_key = SecretKey::from_bytes(&secret_key_bytes)
        .map_err(|e| format!("Failed to create secret key: {}", e))?;
    
    SigningKey::from_bytes(&secret_key.to_bytes())
        .map_err(|e| format!("Failed to create signing key: {}", e))
}

async fn submit_messages(
    messages_tx: mpsc::Sender<message::Message>,
    scenario: Scenario,
    private_key: SigningKey,
) -> Result<u64, String> {
    let mut submit_message_timer = scenario.submit_message_interval.map(time::interval);
    let mut client = SnapchainServiceClient::connect(scenario.submit_message_rpc_addrs[0].clone())
        .await
        .map_err(|e| format!("Failed to connect to Snapchain service: {}", e))?;

    let mut i = 1;
    let mut num_messages_submitted = 0;
    
    loop {
        if let Some(timer) = &mut submit_message_timer {
            timer.tick().await;
        }

        let message = send_message(
            &mut client,
            &compose_message(
                6833,
                format!("For benchmarking {}", i).as_str(),
                None,
                Some(private_key.clone()),
            ),
        )
        .await
        .map_err(|e| format!("Failed to send message: {}", e))?;

        messages_tx.send(message).await.map_err(|e| format!("Failed to send message to channel: {}", e))?;
        num_messages_submitted += 1;
        i += 1;

        if num_messages_submitted >= NUM_ITERATIONS {
            break; // Stop the loop after sending the required number of messages
        }
    }

    Ok(num_messages_submitted)
}

async fn handle_blocks(
    blocks_rx: mpsc::Receiver<Block>,
    start_farcaster_time: u64,
) -> (u64, u64, HashSet<String>, Vec<u64>, Vec<u64>) {
    let mut block_count = 0;
    let mut num_messages_confirmed = 0;
    let mut pending_messages = HashSet::new();
    let mut time_to_confirmation = vec![];
    let mut block_times = vec![];
    let mut last_block_time = start_farcaster_time;

    while let Some(block) = blocks_rx.recv().await {
        let block_timestamp = block.header.as_ref().unwrap().timestamp;
        if block_timestamp > start_farcaster_time {
            block_count += 1;
            block_times.push(block_timestamp - last_block_time);
            last_block_time = block_timestamp;
            for chunk in &block.shard_chunks {
                for tx in &chunk.transactions {
                    for msg in &tx.user_messages {
                        if let Some(data_bytes) = &msg.data_bytes {
                            if let Ok(msg_data) = MessageData::decode(data_bytes.as_slice()) {
                                let msg_timestamp = msg_data.timestamp;
                                time_to_confirmation.push(block_timestamp - msg_timestamp as u64);
                                num_messages_confirmed += 1;
                                pending_messages.remove(&hex::encode(msg.hash.clone()));
                            }
                        }
                    }
                }
            }
        }
    }

    (block_count, num_messages_confirmed, pending_messages, time_to_confirmation, block_times)
}

async fn calculate_stats(
    start: Instant,
    block_count: u64,
    num_messages_submitted: u64,
    num_messages_confirmed: u64,
    time_to_confirmation: Vec<u64>,
    block_times: Vec<u64>,
    iteration: u64,
) {
    let avg_time_to_confirmation = if !time_to_confirmation.is_empty() {
        time_to_confirmation.iter().sum::<u64>() as f64 / time_to_confirmation.len() as f64
    } else {
        0f64
    };

    let max_time_to_confirmation = time_to_confirmation.iter().max().copied().unwrap_or(0);
    let avg_block_time = if !block_times.is_empty() {
        block_times.iter().sum::<u64>() as f64 / block_times.len() as f64
    } else {
        0f64
    };

    let max_block_time = block_times.iter().max().copied().unwrap_or(0);
    let time_elapsed = start.elapsed().as_secs();
    let submitted_msgs_per_sec = num_messages_submitted as f64 / STATS_CALCULATION_INTERVAL.as_secs_f64();
    let confirmed_msgs_per_sec = num_messages_confirmed as f64 / STATS_CALCULATION_INTERVAL.as_secs_f64();

    println!("{:#?} (secs): Blocks produced: {:#?}, Msgs submitted/sec: {:#?}, Msgs confirmed/sec: {:#?}, Avg time to confirmation (secs): {:#?}, Max time to confirmation (secs): {:#?}, Avg block time (secs): {:#?}, Max block time (secs): {:#?}",
        time_elapsed, block_count, submitted_msgs_per_sec, confirmed_msgs_per_sec,
        avg_time_to_confirmation, max_time_to_confirmation, avg_block_time, max_block_time);

    if iteration >= NUM_ITERATIONS {
        println!("Benchmark completed.");
    }
}

#[tokio::main]
async fn main() {
    let scenarios = vec![
        Scenario {
            submit_message_rpc_addrs: vec![
                "http://127.0.0.1:3383".to_string(),
                "http://127.0.0.1:3384".to_string(),
                "http://127.0.0.1:3385".to_string(),
            ],
            follow_blocks_rpc_addr: "http://127.0.0.1:3383".to_string(),
            submit_message_interval: Some(Duration::from_micros(500)),
        },
    ];

    for scenario in scenarios {
        println!("Starting scenario {:#?}", scenario);

        let (blocks_tx, blocks_rx) = mpsc::channel::<Block>(10_000_000);
        let (messages_tx, messages_rx) = mpsc::channel::<message::Message>(10_000_000);

        // Create private key once
        let private_key = match create_signing_key() {
            Ok(key) => key,
            Err(e) => {
                eprintln!("{}", e);
                return;
            }
        };

        let submit_message_handles = tokio::spawn(async move {
            submit_messages(messages_tx, scenario.clone(), private_key).await.unwrap();
        });

        let follow_blocks_handle = tokio::spawn(async move {
            follow_blocks(scenario.follow_blocks_rpc_addr.clone(), blocks_tx)
                .await
                .unwrap();
        });

        let start = Instant::now();
        let start_farcaster_time = current_time();
        let mut stats_calculation_timer = time::interval(STATS_CALCULATION_INTERVAL);
        let mut iteration = 0;

        loop {
            select! {
                _ = stats_calculation_timer.tick() => {
                    let (block_count, num_messages_confirmed, pending_messages, time_to_confirmation, block_times) =
                        handle_blocks(blocks_rx.clone(), start_farcaster_time).await;

                    calculate_stats(
                        start,
                        block_count,
                        num_messages_submitted,
                        num_messages_confirmed,
                        time_to_confirmation,
                        block_times,
                        iteration,
                    ).await;

                    iteration += 1;
                    if iteration >= NUM_ITERATIONS {
                        break; // Exit after NUM_ITERATIONS
                    }
                }
            }
        }
    }
}
