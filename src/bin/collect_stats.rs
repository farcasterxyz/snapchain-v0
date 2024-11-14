use ed25519_dalek::{SecretKey, SigningKey};
use hex;
use snapchain::proto::snapchain::Block;
use snapchain::utils::cli::{compose_message, follow_blocks};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, time};
mod submit_message;
use hex::FromHex;
use message::MessageData;
use prost::Message;
use snapchain::proto::message;
// Sumbit x messages per second
// Read blocks and collect stats
const SUBMIT_MESSAGE_INTERVAL: Duration = Duration::from_millis(100);
const STATS_CALCULATION_INTERVAL: Duration = Duration::from_secs(1);
const RPC_ADDR: &str = "http://127.0.0.1:3383";

#[tokio::main]
async fn main() {
    let (blocks_tx, mut blocks_rx) = mpsc::channel::<Block>(1000);
    let (messages_tx, mut messages_rx) = mpsc::channel::<message::Message>(1000);
    tokio::spawn(async move {
        let private_key = SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        );
        // Create a timer for block creation
        let mut submit_message_timer = time::interval(SUBMIT_MESSAGE_INTERVAL);

        let mut i = 1;
        loop {
            let _ = submit_message_timer.tick().await;
            let message = compose_message(
                private_key.clone(),
                6833,
                RPC_ADDR.to_string(),
                format!("For benchmarking {}", i).as_str(),
            )
            .await
            .unwrap();
            messages_tx.send(message).await.unwrap();
            i += 1;
        }
    });

    tokio::spawn(async move {
        let addr = RPC_ADDR.to_string();
        follow_blocks(addr, blocks_tx).await.unwrap()
    });

    let mut stats_calculation_timer = time::interval(STATS_CALCULATION_INTERVAL);
    let mut block_count = 0;
    let mut num_messages_processed = 0;
    let mut num_messages_submitted = 0;
    let mut time_to_confirmation = vec![];
    loop {
        select! {
            _ = stats_calculation_timer.tick() => {
                let avg_time_to_confirmation = if time_to_confirmation.len() > 0 {time_to_confirmation.iter().sum::<f64>() / time_to_confirmation.len() as f64 } else {0f64};
                println!("Blocks produced : {:#?}, Messages submitted: {:#?}, Messages processed: {:#?}, Avg time to confirmation (secs): {:#?}", block_count, num_messages_submitted, num_messages_processed, avg_time_to_confirmation);
                block_count = 0;
                num_messages_processed= 0;
                num_messages_submitted = 0;
                time_to_confirmation.clear();
            }
            Some(block) = blocks_rx.recv() => {
                block_count += 1;
                for chunk in &block.shard_chunks {
                    for tx in &chunk.transactions {
                        for msg in &tx.user_messages {
                            let msg_data = MessageData::decode(msg.data_bytes.as_ref().unwrap().as_slice()).unwrap();
                            let msg_timestamp = msg_data.timestamp;
                            let block_timestamp = block.header.as_ref().unwrap().timestamp;
                            time_to_confirmation.push(block_timestamp as f64 - (msg_timestamp as f64));
                            num_messages_processed += 1;
                        }
                    }
        }
            }
            Some(_) = messages_rx.recv() => {
                num_messages_submitted += 1;
            },
        }
    }
}
