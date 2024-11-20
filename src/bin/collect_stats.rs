use ed25519_dalek::{SecretKey, SigningKey};
use hex;
use hex::FromHex;
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
use tokio::time::Instant;
use tokio::{select, time};
const STATS_CALCULATION_INTERVAL: Duration = Duration::from_secs(10);
const NUM_ITERATIONS: u64 = 5;

#[derive(Debug, Clone)]
struct Scenario {
    submit_message_rpc_addrs: Vec<String>,
    follow_blocks_rpc_addr: String,
    submit_message_interval: Option<Duration>,
}

fn start_submit_messages(
    messages_tx: tokio::sync::mpsc::Sender<message::Message>,
    scenario: Scenario,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut submit_message_handles = vec![];

    for rpc_addr in scenario.submit_message_rpc_addrs {
        let messages_tx = messages_tx.clone();
        let private_key = SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        );
        let submit_message_handle = tokio::spawn(async move {
            let mut submit_message_timer = match scenario.submit_message_interval {
                None => None,
                Some(interval) => Some(time::interval(interval)),
            };
            let mut client = SnapchainServiceClient::connect(rpc_addr.clone())
                .await
                .unwrap();

            let mut i = 1;
            loop {
                match &mut submit_message_timer {
                    None => (),
                    Some(timer) => {
                        timer.tick().await;
                    }
                };
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
                .unwrap();
                messages_tx.send(message).await.unwrap();
                i += 1;
            }
        });
        submit_message_handles.push(submit_message_handle);
    }

    submit_message_handles
}

#[tokio::main]
async fn main() {
    let scenarios = [
        // Scenario {
        //     submit_message_rpc_addrs: vec![
        //         "http://127.0.0.1:3383".to_string(),
        //         "http://127.0.0.1:3384".to_string(),
        //         "http://127.0.0.1:3385".to_string(),
        //     ],
        //     follow_blocks_rpc_addr: "http://127.0.0.1:3383".to_string(),
        //     submit_message_interval: None, // Tight loop for submitting messages, no delay,
        // },
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
        let (blocks_tx, mut blocks_rx) = mpsc::channel::<Block>(10_000_000);

        let (messages_tx, mut messages_rx) = mpsc::channel::<message::Message>(10_000_000);
        let submit_message_handles = start_submit_messages(messages_tx, scenario.clone());

        let follow_blocks_handle = tokio::spawn(async move {
            let addr = scenario.follow_blocks_rpc_addr;
            follow_blocks(addr, blocks_tx).await.unwrap()
        });

        let start = Instant::now();
        let start_farcaster_time = current_time();
        let mut stats_calculation_timer = time::interval(STATS_CALCULATION_INTERVAL);
        let mut block_count = 0;
        let mut num_messages_confirmed = 0;
        let mut num_messages_submitted = 0;
        let mut pending_messages = HashSet::new();
        let mut time_to_confirmation = vec![];
        let mut block_times = vec![];
        let mut last_block_time = start_farcaster_time;
        let mut iteration = 0;
        loop {
            select! {
                Some(message) = messages_rx.recv() => {
                    num_messages_submitted += 1;
                    pending_messages.insert(hex::encode(message.hash));
                },
                Some(block) = blocks_rx.recv() => {
                    let block_timestamp = block.header.as_ref().unwrap().timestamp;
                    if block_timestamp > start_farcaster_time {
                        block_count += 1;
                        block_times.push(block_timestamp - last_block_time);
                        last_block_time = block_timestamp;
                        for chunk in &block.shard_chunks {
                            for tx in &chunk.transactions {
                                for msg in &tx.user_messages {
                                    let msg_data = MessageData::decode(msg.data_bytes.as_ref().unwrap().as_slice()).unwrap();
                                    let msg_timestamp = msg_data.timestamp;
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
                    let confirmed_msgs_per_sec = num_messages_confirmed as f64 / STATS_CALCULATION_INTERVAL.as_secs_f64();
                    let submitted_msgs_per_sec = num_messages_submitted as f64 / STATS_CALCULATION_INTERVAL.as_secs_f64();
                    println!("{:#?} (secs): Blocks produced: {:#?}, Msgs submitted/sec: {:#?}, Msgs confirmed/sec: {:#?}, Avg time to confirmation (secs): {:#?}, Max time to confirmation (secs): {:#?}, Avg block time (secs): {:#?}, Max block time (secs): {:#?}", time_elapsed, block_count, submitted_msgs_per_sec, confirmed_msgs_per_sec, avg_time_to_confirmation, max_time_to_confirmation, avg_block_time, max_block_time);
                    block_count = 0;
                    num_messages_confirmed= 0;
                    num_messages_submitted = 0;
                    time_to_confirmation.clear();
                    block_times.clear();
                    iteration += 1;
                    if iteration > NUM_ITERATIONS {
                        for submit_message_handle in submit_message_handles {
                            submit_message_handle.abort();
                        }
                        follow_blocks_handle.abort();
                        break;
                    }
                }
            }
        }
    }
}
