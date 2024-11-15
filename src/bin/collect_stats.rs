use alloy::signers::k256::elliptic_curve::group::ScalarMulOwned;
use ed25519_dalek::{SecretKey, SigningKey};
use hex;
use snapchain::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use snapchain::proto::snapchain::Block;
use snapchain::utils::cli::{compose_message, follow_blocks};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{select, time};
mod submit_message;
use hex::FromHex;
use message::MessageData;
use prost::Message;
use snapchain::proto::message;
const STATS_CALCULATION_INTERVAL: Duration = Duration::from_secs(10);
const NUM_ITERATIONS: u64 = 10;

#[derive(Debug)]
struct Scenario {
    submit_message_rpc_addr: String,
    follow_blocks_rpc_addr: String,
    submit_message_interval: Duration,
}

#[tokio::main]
async fn main() {
    let scenarios = [
        Scenario {
            submit_message_rpc_addr: "http://127.0.0.1:3383".to_string(),
            follow_blocks_rpc_addr: "http://127.0.0.1:3383".to_string(),
            submit_message_interval: Duration::from_millis(1),
        },
        Scenario {
            submit_message_rpc_addr: "http://127.0.0.1:3383".to_string(),
            follow_blocks_rpc_addr: "http://127.0.0.1:3383".to_string(),
            submit_message_interval: Duration::from_millis(100),
        },
        Scenario {
            submit_message_rpc_addr: "http://127.0.0.1:3383".to_string(),
            follow_blocks_rpc_addr: "http://127.0.0.1:3384".to_string(),
            submit_message_interval: Duration::from_millis(100),
        },
        Scenario {
            submit_message_rpc_addr: "http://127.0.0.1:3383".to_string(),
            follow_blocks_rpc_addr: "http://127.0.0.1:3384".to_string(),
            submit_message_interval: Duration::from_millis(10),
        },
    ];

    for scenario in scenarios {
        println!("Starting scenario {:#?}", scenario);
        let (blocks_tx, mut blocks_rx) = mpsc::channel::<Block>(100_000);
        let (messages_tx, mut messages_rx) = mpsc::channel::<message::Message>(100_000);
        let submit_message_handle = tokio::spawn(async move {
            let private_key = SigningKey::from_bytes(
                &SecretKey::from_hex(
                    "1000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap(),
            );

            let mut submit_message_timer = time::interval(scenario.submit_message_interval);
            let mut client =
                SnapchainServiceClient::connect(scenario.submit_message_rpc_addr.clone())
                    .await
                    .unwrap();

            let mut i = 1;
            loop {
                let _ = submit_message_timer.tick().await;
                let message = compose_message(
                    &mut client,
                    private_key.clone(),
                    6833,
                    format!("For benchmarking {}", i).as_str(),
                )
                .await
                .unwrap();
                messages_tx.send(message).await.unwrap();
                i += 1;
            }
        });

        let follow_blocks_handle = tokio::spawn(async move {
            let addr = scenario.follow_blocks_rpc_addr;
            follow_blocks(addr, blocks_tx).await.unwrap()
        });

        let start = Instant::now();
        let mut stats_calculation_timer = time::interval(STATS_CALCULATION_INTERVAL);
        let mut block_count = 0;
        let mut num_messages_confirmed = 0;
        let mut num_messages_submitted = 0;
        let mut pending_messages = HashSet::new();
        let mut time_to_confirmation = vec![];
        let mut iteration = 0;
        loop {
            select! {
                Some(message) = messages_rx.recv() => {
                    num_messages_submitted += 1;
                    pending_messages.insert(hex::encode(message.hash));
                },
                Some(block) = blocks_rx.recv() => {
                    block_count += 1;
                    for chunk in &block.shard_chunks {
                        for tx in &chunk.transactions {
                            for msg in &tx.user_messages {
                                let msg_data = MessageData::decode(msg.data_bytes.as_ref().unwrap().as_slice()).unwrap();
                                let msg_timestamp = msg_data.timestamp;
                                let block_timestamp = block.header.as_ref().unwrap().timestamp;
                                time_to_confirmation.push(block_timestamp  - msg_timestamp as u64);
                                num_messages_confirmed += 1;
                                pending_messages.remove(&hex::encode(msg.hash.clone()));
                            }
                        }
                    }
                }
                time = stats_calculation_timer.tick() => {
                    let avg_time_to_confirmation = if time_to_confirmation.len() > 0 {time_to_confirmation.iter().sum::<u64>() as f64 / time_to_confirmation.len() as f64} else {0f64};
                    let max_time_to_confirmation = if time_to_confirmation.len() > 0 {time_to_confirmation.clone().into_iter().max().unwrap()} else {0};
                    let time_elapsed = time.duration_since(start).as_secs();
                    println!("{:#?} (secs): Blocks produced: {:#?}, Messages submitted: {:#?}, Messages confirmed: {:#?}, Pending messages: {:#?}, Avg time to confirmation (secs): {:#?}, Max time to confirmation (secs): {:#?}", time_elapsed, block_count, num_messages_submitted, num_messages_confirmed, pending_messages.len(), avg_time_to_confirmation, max_time_to_confirmation);
                    block_count = 0;
                    num_messages_confirmed= 0;
                    num_messages_submitted = 0;
                    time_to_confirmation.clear();
                    iteration += 1;
                    if iteration > NUM_ITERATIONS {
                        submit_message_handle.abort();
                        follow_blocks_handle.abort();
                        break;
                    }
                }
            }
        }
    }
}
